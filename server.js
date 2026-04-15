const path = require("path");
const express = require("express");
const multer = require("multer");
const dotenv = require("dotenv");
const { Readable } = require("stream");
const { pipeline } = require("stream/promises");
const crypto = require("crypto");

dotenv.config({ path: path.join(__dirname, ".env") });
dotenv.config({ path: path.join(__dirname, ".env.local") });
dotenv.config({ path: path.join(process.cwd(), ".env.local") });
dotenv.config({ path: path.join(__dirname, "..", ".env") });
dotenv.config({ path: path.join(__dirname, "..", ".env.local") });

function toPositiveNumber(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function normalizeBaseUrl(rawValue, fallback) {
  const value =
    typeof rawValue === "string" && rawValue.trim()
      ? rawValue.trim()
      : fallback;
  try {
    const url = new URL(value);
    return url.toString();
  } catch {
    return fallback;
  }
}

const FALLBACK_API_KEY =
  process.env.NEXT_PUBLIC_API_KEY ||
  process.env.NEXT_PUBLIC_PHONT_X_API_KEY ||
  "";
const DEFAULT_PLUGIN_API_KEY =
  "5e5a8fdf643a.WtFibU9r8wSxdNSvyAms-v2ggjsiXNeiMDC6x7SeoLY";

const CONFIG = {
  port: Math.round(toPositiveNumber(process.env.PORT, 5055)),
  nextExportBaseUrl: normalizeBaseUrl(
    process.env.NEXT_EXPORT_BASE_URL,
    "http://4.210.217.8/",
  ),
  subtitleSourceBaseUrl: normalizeBaseUrl(
    process.env.SUBTITLE_SOURCE_API_BASE_URL ||
      process.env.NEXT_PUBLIC_API_SUB_BASE_URL,
    "https://demo-api-dev.phont.ai",
  ),
  uploadApiBaseUrl: normalizeBaseUrl(
    process.env.UPLOAD_API_BASE_URL,
    "https://upload.demo-api-dev.phont.ai",
  ),
  subtitleSourceApiKey:
    process.env.SUBTITLE_SOURCE_X_API_KEY || FALLBACK_API_KEY,
  subtitleSourceBearerToken:
    process.env.SUBTITLE_SOURCE_BEARER_TOKEN || FALLBACK_API_KEY,
  subtitleSourceSessionId: process.env.SUBTITLE_SOURCE_SESSION_ID || "",
  sourceAutoLoginUsername:
    process.env.SUBTITLE_SOURCE_AUTO_LOGIN_USERNAME ||
    process.env.NEXT_PUBLIC_AUTO_LOGIN_USERNAME ||
    "",
  sourceAutoLoginPassword:
    process.env.SUBTITLE_SOURCE_AUTO_LOGIN_PASSWORD ||
    process.env.NEXT_PUBLIC_AUTO_LOGIN_PASSWORD ||
    "",
  pluginApiKey:
    process.env.PLUGIN_X_API_KEY ||
    process.env.UPLOAD_API_KEY ||
    process.env.SUBTITLE_SOURCE_X_API_KEY ||
    DEFAULT_PLUGIN_API_KEY,
  exportWidth: Math.max(
    2,
    Math.round(toPositiveNumber(process.env.EXPORT_WIDTH, 1920)),
  ),
  exportHeight: Math.max(
    2,
    Math.round(toPositiveNumber(process.env.EXPORT_HEIGHT, 1080)),
  ),
  exportFps: Math.max(
    60,
    Math.round(toPositiveNumber(process.env.EXPORT_FPS, 60)),
  ),
  exportFormat:
    process.env.EXPORT_FORMAT === "vp9-alpha-webm"
      ? "vp9-alpha-webm"
      : "prores4444-mov",
  exportPollIntervalMs: Math.max(
    500,
    Math.round(toPositiveNumber(process.env.EXPORT_POLL_INTERVAL_MS, 1200)),
  ),
  exportPollTimeoutMs: Math.max(
    10_000,
    Math.round(toPositiveNumber(process.env.EXPORT_POLL_TIMEOUT_MS, 900_000)),
  ),
  httpTimeoutMs: Math.max(
    2_000,
    Math.round(toPositiveNumber(process.env.HTTP_TIMEOUT_MS, 30_000)),
  ),
  uploadTimeoutMs: Math.max(
    30_000,
    Math.round(toPositiveNumber(process.env.UPLOAD_TIMEOUT_MS, 900_000)),
  ),
  pluginPollIntervalMs: Math.max(
    10_000,
    Math.round(toPositiveNumber(process.env.PLUGIN_POLL_INTERVAL_MS, 10_000)),
  ),
  pluginPollTimeoutMs: Math.max(
    30_000,
    Math.round(toPositiveNumber(process.env.PLUGIN_POLL_TIMEOUT_MS, 3_600_000)),
  ),
  uploadMaxFileBytes: Math.max(
    1_048_576,
    Math.round(
      toPositiveNumber(process.env.UPLOAD_MAX_FILE_BYTES, 524_288_000),
    ),
  ),
};

const SOURCE_AUTH_STATE = {
  authToken:
    typeof CONFIG.subtitleSourceBearerToken === "string" &&
    CONFIG.subtitleSourceBearerToken.trim()
      ? CONFIG.subtitleSourceBearerToken.trim()
      : null,
  sessionId:
    typeof CONFIG.subtitleSourceSessionId === "string" &&
    CONFIG.subtitleSourceSessionId.trim()
      ? CONFIG.subtitleSourceSessionId.trim()
      : null,
  loginPromise: null,
  lastLoginAt: null,
};

class HttpError extends Error {
  constructor(status, message, details = null) {
    super(message);
    this.name = "HttpError";
    this.status = status;
    this.details = details;
  }
}

function parseBoolean(value, fallback = false) {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true" || normalized === "1" || normalized === "yes")
      return true;
    if (normalized === "false" || normalized === "0" || normalized === "no")
      return false;
  }
  return fallback;
}

function buildSourceHeaders() {
  const headers = {
    Accept: "application/json",
    "Content-Type": "application/json",
  };

  if (CONFIG.subtitleSourceApiKey) {
    headers["x-api-key"] = CONFIG.subtitleSourceApiKey;
  }
  if (SOURCE_AUTH_STATE.authToken) {
    headers.Authorization = `Bearer ${SOURCE_AUTH_STATE.authToken}`;
  }
  if (SOURCE_AUTH_STATE.sessionId) {
    headers["X-Session-ID"] = SOURCE_AUTH_STATE.sessionId;
  }

  return headers;
}

function buildPluginApiHeaders() {
  const headers = {
    Accept: "application/json",
  };
  if (CONFIG.pluginApiKey) {
    headers["X-API-Key"] = CONFIG.pluginApiKey;
  }
  return headers;
}

function hasSourceAutoLoginCredentials() {
  return (
    typeof CONFIG.sourceAutoLoginUsername === "string" &&
    CONFIG.sourceAutoLoginUsername.trim() !== "" &&
    typeof CONFIG.sourceAutoLoginPassword === "string" &&
    CONFIG.sourceAutoLoginPassword.trim() !== ""
  );
}

function createAbortSignal(timeoutMs, parentSignal) {
  const controller = new AbortController();
  const onParentAbort = () =>
    controller.abort(parentSignal.reason || new Error("Request aborted."));

  if (parentSignal) {
    if (parentSignal.aborted) {
      onParentAbort();
    } else {
      parentSignal.addEventListener("abort", onParentAbort, { once: true });
    }
  }

  const timer =
    Number.isFinite(timeoutMs) && timeoutMs > 0
      ? setTimeout(
          () =>
            controller.abort(
              new Error(`Request timed out after ${timeoutMs}ms.`),
            ),
          timeoutMs,
        )
      : null;

  return {
    signal: controller.signal,
    cleanup: () => {
      if (timer) clearTimeout(timer);
      if (parentSignal) {
        parentSignal.removeEventListener("abort", onParentAbort);
      }
    },
  };
}

async function fetchWithTimeout(url, options = {}) {
  const {
    timeoutMs = CONFIG.httpTimeoutMs,
    signal: parentSignal,
    ...fetchOptions
  } = options;

  const { signal, cleanup } = createAbortSignal(timeoutMs, parentSignal);
  try {
    return await fetch(url, { ...fetchOptions, signal });
  } catch (error) {
    if (error?.name === "AbortError") {
      if (parentSignal?.aborted && parentSignal.reason instanceof HttpError) {
        throw parentSignal.reason;
      }
      throw new HttpError(
        504,
        `Upstream request timed out or was aborted: ${url}`,
      );
    }
    throw error;
  } finally {
    cleanup();
  }
}

async function fetchJson(url, options = {}) {
  const response = await fetchWithTimeout(url, options);
  const raw = await response.text();
  let json;

  if (raw) {
    try {
      json = JSON.parse(raw);
    } catch {
      json = { raw };
    }
  } else {
    json = {};
  }

  if (!response.ok) {
    const message =
      json?.error ||
      json?.message ||
      `Upstream request failed with status ${response.status}.`;
    throw new HttpError(response.status, message, {
      url,
      status: response.status,
      response: json,
    });
  }

  return json;
}

async function parseJsonResponse(response) {
  const raw = await response.text();
  if (!raw) return {};

  try {
    return JSON.parse(raw);
  } catch {
    return { raw };
  }
}

function buildUrl(baseUrl, pathname, query = null) {
  const url = new URL(pathname, baseUrl);
  if (query && typeof query === "object") {
    for (const [key, value] of Object.entries(query)) {
      if (value === undefined || value === null || value === "") continue;
      url.searchParams.set(key, String(value));
    }
  }
  return url.toString();
}

async function loginToSourceApi(signal) {
  if (SOURCE_AUTH_STATE.loginPromise) {
    return SOURCE_AUTH_STATE.loginPromise;
  }

  if (!hasSourceAutoLoginCredentials()) {
    throw new HttpError(
      401,
      "Source API requires authentication and auto-login credentials are missing.",
      {
        requiredEnv: [
          "SUBTITLE_SOURCE_AUTO_LOGIN_USERNAME",
          "SUBTITLE_SOURCE_AUTO_LOGIN_PASSWORD",
        ],
      },
    );
  }

  SOURCE_AUTH_STATE.loginPromise = (async () => {
    const loginUrl = buildUrl(CONFIG.subtitleSourceBaseUrl, "/login");
    const loginResponse = await fetchWithTimeout(loginUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify({
        identifier: CONFIG.sourceAutoLoginUsername,
        password: CONFIG.sourceAutoLoginPassword,
      }),
      timeoutMs: CONFIG.httpTimeoutMs,
      signal,
    });

    const loginJson = await parseJsonResponse(loginResponse);
    if (!loginResponse.ok) {
      throw new HttpError(
        loginResponse.status,
        loginJson?.message ||
          loginJson?.detail ||
          `Source auto-login failed with status ${loginResponse.status}.`,
        {
          url: loginUrl,
          status: loginResponse.status,
          response: loginJson,
        },
      );
    }

    const authToken = loginJson?.auth_token;
    const sessionId = loginJson?.session_id;
    if (!authToken || !sessionId) {
      throw new HttpError(
        502,
        "Source auto-login returned an invalid payload (missing auth_token/session_id).",
        { response: loginJson },
      );
    }

    SOURCE_AUTH_STATE.authToken = authToken;
    SOURCE_AUTH_STATE.sessionId = sessionId;
    SOURCE_AUTH_STATE.lastLoginAt = Date.now();
    return { authToken, sessionId };
  })();

  try {
    return await SOURCE_AUTH_STATE.loginPromise;
  } finally {
    SOURCE_AUTH_STATE.loginPromise = null;
  }
}

async function fetchSourceJson(pathname, query, signal) {
  const url = buildUrl(CONFIG.subtitleSourceBaseUrl, pathname, query);
  let response = await fetchWithTimeout(url, {
    method: "GET",
    headers: buildSourceHeaders(),
    timeoutMs: CONFIG.httpTimeoutMs,
    signal,
  });

  if (response.status === 401 && hasSourceAutoLoginCredentials()) {
    await loginToSourceApi(signal);
    response = await fetchWithTimeout(url, {
      method: "GET",
      headers: buildSourceHeaders(),
      timeoutMs: CONFIG.httpTimeoutMs,
      signal,
    });
  }

  const json = await parseJsonResponse(response);
  if (!response.ok) {
    throw new HttpError(
      response.status,
      json?.error ||
        json?.message ||
        json?.detail ||
        `Upstream request failed with status ${response.status}.`,
      {
        url,
        status: response.status,
        response: json,
      },
    );
  }

  return json;
}

function normalizeSubtitles(payload) {
  if (Array.isArray(payload)) {
    if (
      payload.length > 0 &&
      !Array.isArray(payload[0]) &&
      typeof payload[0] === "object"
    ) {
      return [payload];
    }
    return payload;
  }

  if (payload && Array.isArray(payload.subtitles)) {
    if (
      payload.subtitles.length > 0 &&
      !Array.isArray(payload.subtitles[0]) &&
      typeof payload.subtitles[0] === "object"
    ) {
      return [payload.subtitles];
    }
    return payload.subtitles;
  }

  return [];
}

function hasAnimationData(segment) {
  if (!segment || typeof segment !== "object") return false;
  const hasEmotion =
    typeof segment.emotion === "string" && segment.emotion.trim() !== "";
  const hasAnimationUrl =
    typeof segment.animation_url === "string" &&
    segment.animation_url.trim() !== "";
  const hasAnimations =
    segment.animations &&
    typeof segment.animations === "object" &&
    Object.keys(segment.animations).length > 0;

  return Boolean(hasEmotion || hasAnimationUrl || hasAnimations);
}

function dedupeSubtitles(subtitles) {
  const map = new Map();

  for (const segment of subtitles) {
    const start = Number(segment?.start_time);
    const end = Number(segment?.end_time);
    const key =
      Number.isFinite(start) && Number.isFinite(end)
        ? `${start}-${end}`
        : JSON.stringify(segment);

    if (!map.has(key)) {
      map.set(key, segment);
      continue;
    }

    const current = map.get(key);
    if (hasAnimationData(segment) && !hasAnimationData(current)) {
      map.set(key, segment);
    }
  }

  return Array.from(map.values());
}

function maxSubtitleEnd(subtitles) {
  return subtitles.reduce((max, segment) => {
    const endTime = Number(segment?.end_time);
    return Number.isFinite(endTime) ? Math.max(max, endTime) : max;
  }, 0);
}

function normalizeDurationValue(value, subtitleMaxEnd) {
  const raw = Number(value);
  if (!Number.isFinite(raw) || raw <= 0) return null;

  if (raw > 1000 && subtitleMaxEnd > 0) {
    const millisecondsCandidate = raw / 1000;
    const lower = subtitleMaxEnd * 0.5;
    const upper = subtitleMaxEnd * 3;
    if (millisecondsCandidate >= lower && millisecondsCandidate <= upper) {
      return millisecondsCandidate;
    }
  }

  if (raw > 43_200 && raw / 1000 <= 43_200) {
    return raw / 1000;
  }

  return raw;
}

function readPath(source, pathParts) {
  let value = source;
  for (const key of pathParts) {
    if (!value || typeof value !== "object") return undefined;
    value = value[key];
  }
  return value;
}

function extractDurationFromMetadata(metadata, subtitleMaxEnd) {
  if (!metadata || typeof metadata !== "object") return null;

  const candidatePaths = [
    ["duration"],
    ["duration_seconds"],
    ["video_duration"],
    ["runtime"],
    ["length"],
    ["metadata", "duration"],
    ["metadata", "duration_seconds"],
    ["media", "duration"],
    ["video", "duration"],
    ["asset", "duration"],
  ];

  for (const pathParts of candidatePaths) {
    const candidate = readPath(metadata, pathParts);
    const seconds = normalizeDurationValue(candidate, subtitleMaxEnd);
    if (seconds && seconds > 0) {
      return seconds;
    }
  }

  return null;
}

function extractManifestUrl(metadata) {
  if (!metadata || typeof metadata !== "object") return null;

  const candidatePaths = [
    ["manifest"],
    ["manifest_url"],
    ["hls"],
    ["hls_url"],
    ["playback", "manifest"],
    ["playback", "hls"],
    ["stream", "manifest"],
    ["stream", "hls"],
  ];

  for (const pathParts of candidatePaths) {
    const value = readPath(metadata, pathParts);
    if (typeof value === "string" && value.trim()) {
      try {
        return new URL(value).toString();
      } catch {
        // Ignore invalid URLs
      }
    }
  }

  return null;
}

function sumExtInfDurations(m3u8Text) {
  const regex = /#EXTINF:([0-9.]+)/g;
  let total = 0;
  let match;

  while ((match = regex.exec(m3u8Text)) !== null) {
    const duration = Number(match[1]);
    if (Number.isFinite(duration) && duration > 0) {
      total += duration;
    }
  }

  return total > 0 ? total : null;
}

function selectBestVariantUrl(masterPlaylistText, masterUrl) {
  const lines = masterPlaylistText.split(/\r?\n/);
  const variants = [];

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (!line.startsWith("#EXT-X-STREAM-INF")) continue;

    const bandwidthMatch = line.match(/BANDWIDTH=(\d+)/i);
    const bandwidth = bandwidthMatch ? Number(bandwidthMatch[1]) : 0;

    let relativeUri = null;
    for (let j = i + 1; j < lines.length; j += 1) {
      const nextLine = lines[j].trim();
      if (!nextLine || nextLine.startsWith("#")) continue;
      relativeUri = nextLine;
      break;
    }

    if (!relativeUri) continue;
    try {
      variants.push({
        bandwidth: Number.isFinite(bandwidth) ? bandwidth : 0,
        url: new URL(relativeUri, masterUrl).toString(),
      });
    } catch {
      // Ignore malformed variant URL
    }
  }

  if (variants.length === 0) return null;
  variants.sort((a, b) => b.bandwidth - a.bandwidth);
  return variants[0].url;
}

async function fetchManifestDurationSeconds(manifestUrl, signal) {
  const manifestResponse = await fetchWithTimeout(manifestUrl, {
    method: "GET",
    timeoutMs: CONFIG.httpTimeoutMs,
    signal,
  });

  if (!manifestResponse.ok) {
    throw new HttpError(
      manifestResponse.status,
      `Failed to load manifest (${manifestResponse.status}).`,
    );
  }

  const manifestText = await manifestResponse.text();
  const directDuration = sumExtInfDurations(manifestText);
  if (directDuration && directDuration > 0) {
    return directDuration;
  }

  const variantUrl = selectBestVariantUrl(manifestText, manifestUrl);
  if (!variantUrl) {
    return null;
  }

  const variantResponse = await fetchWithTimeout(variantUrl, {
    method: "GET",
    timeoutMs: CONFIG.httpTimeoutMs,
    signal,
  });
  if (!variantResponse.ok) {
    throw new HttpError(
      variantResponse.status,
      `Failed to load variant playlist (${variantResponse.status}).`,
    );
  }

  const variantText = await variantResponse.text();
  return sumExtInfDurations(variantText);
}

function wait(ms, signal) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      if (signal) {
        signal.removeEventListener("abort", onAbort);
      }
      resolve();
    }, ms);

    const onAbort = () => {
      clearTimeout(timer);
      reject(
        new HttpError(499, "Client disconnected while export was processing."),
      );
    };

    if (!signal) return;
    if (signal.aborted) {
      onAbort();
      return;
    }
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

async function fetchSourceSubtitles(mediaId, language, model, signal) {
  return fetchSourceJson(
    `/vod-subtitles/${encodeURIComponent(mediaId)}`,
    { lang: language || undefined, model: model || undefined },
    signal,
  );
}

async function fetchSourceMetadata(mediaId, signal) {
  return fetchSourceJson(`/vods/${encodeURIComponent(mediaId)}`, null, signal);
}

async function uploadFileToPhontBackend({
  buffer,
  mimeType,
  originalName,
  source,
  pluginVodId,
  title,
  signal,
}) {
  const uploadUrl = buildUrl(CONFIG.uploadApiBaseUrl, "/api/upload", {
    auto_queue: "true",
  });

  const formData = new FormData();
  const normalizedFileName =
    sanitizeOptionalString(originalName) || "upload.bin";
  const normalizedMimeType =
    sanitizeOptionalString(mimeType) || "application/octet-stream";
  const fileBlob = new Blob([buffer], { type: normalizedMimeType });

  // Backend expects field name "video" for file uploads (same as extension behavior).
  formData.append("video", fileBlob, normalizedFileName);
  formData.append("chrome_plugin_source", source);
  formData.append("chrome_plugin_vod_id", pluginVodId);
  formData.append("name", title || normalizedFileName);

  const response = await fetchWithTimeout(uploadUrl, {
    method: "POST",
    headers: {
      "X-API-Key": CONFIG.pluginApiKey,
    },
    body: formData,
    timeoutMs: CONFIG.uploadTimeoutMs,
    signal,
  });
  const json = await parseJsonResponse(response);

  if (!response.ok) {
    throw new HttpError(
      response.status,
      json?.error ||
        json?.message ||
        json?.detail ||
        `Upload failed with status ${response.status}.`,
      { url: uploadUrl, status: response.status, response: json },
    );
  }

  return json;
}

async function pollPluginVodStatusByContext({ source, pluginVodId, signal }) {
  const statusUrl = buildUrl(CONFIG.subtitleSourceBaseUrl, "/status/vods", {
    chrome_plugin_source: source,
    chrome_plugin_vod_id: pluginVodId,
  });
  const deadline = Date.now() + CONFIG.pluginPollTimeoutMs;

  while (Date.now() < deadline) {
    const response = await fetchWithTimeout(statusUrl, {
      method: "GET",
      headers: buildPluginApiHeaders(),
      timeoutMs: CONFIG.httpTimeoutMs,
      signal,
    });

    if (response.status === 404) {
      await wait(CONFIG.pluginPollIntervalMs, signal);
      continue;
    }

    const json = await parseJsonResponse(response);
    if (!response.ok) {
      throw new HttpError(
        response.status,
        json?.error ||
          json?.message ||
          json?.detail ||
          `Status check failed with status ${response.status}.`,
        { url: statusUrl, status: response.status, response: json },
      );
    }

    const status = String(json?.status || "").toUpperCase();
    if (status === "COMPLETED") {
      return json;
    }
    if (status === "FAILED" || status === "ERROR" || status === "CANCELLED") {
      throw new HttpError(
        502,
        `Processing failed on Phont backend with status "${status}".`,
        json,
      );
    }

    await wait(CONFIG.pluginPollIntervalMs, signal);
  }

  throw new HttpError(
    504,
    `Processing did not reach COMPLETED within ${Math.round(
      CONFIG.pluginPollTimeoutMs / 1000,
    )} seconds.`,
    {
      source,
      pluginVodId,
    },
  );
}

async function fetchVodSubtitlesByPluginContext({
  source,
  pluginVodId,
  signal,
}) {
  const url = buildUrl(CONFIG.subtitleSourceBaseUrl, "/vod-subtitles", {
    chrome_plugin_source: source,
    chrome_plugin_vod_id: pluginVodId,
  });
  const response = await fetchWithTimeout(url, {
    method: "GET",
    headers: buildPluginApiHeaders(),
    timeoutMs: CONFIG.httpTimeoutMs,
    signal,
  });
  const json = await parseJsonResponse(response);

  if (!response.ok) {
    throw new HttpError(
      response.status,
      json?.error ||
        json?.message ||
        json?.detail ||
        `Subtitle fetch failed with status ${response.status}.`,
      { url, status: response.status, response: json },
    );
  }
  console.log(json, " -> JSON Response For Subtitles");
  return json;
}

async function startNextSubtitleExport(payload, signal) {
  const url = buildUrl(CONFIG.nextExportBaseUrl, "/api/subtitle-export");
  return fetchJson(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify(payload),
    timeoutMs: CONFIG.httpTimeoutMs,
    signal,
  });
}

async function pollNextExportJob(jobId, signal) {
  const statusUrl = buildUrl(
    CONFIG.nextExportBaseUrl,
    `/api/subtitle-export/${encodeURIComponent(jobId)}`,
  );
  const deadline = Date.now() + CONFIG.exportPollTimeoutMs;

  while (Date.now() < deadline) {
    const status = await fetchJson(statusUrl, {
      method: "GET",
      headers: { Accept: "application/json" },
      timeoutMs: CONFIG.httpTimeoutMs,
      signal,
    });

    const currentStatus = String(status?.status || "").toLowerCase();
    if (currentStatus === "completed") {
      return status;
    }
    if (currentStatus === "failed") {
      throw new HttpError(
        502,
        status?.error || "Subtitle export job failed.",
        status,
      );
    }

    await wait(CONFIG.exportPollIntervalMs, signal);
  }

  throw new HttpError(
    504,
    `Subtitle export job timed out after ${Math.round(CONFIG.exportPollTimeoutMs / 1000)} seconds.`,
    { jobId },
  );
}

async function generateAndStreamSubtitleOverlay({
  res,
  subtitles,
  durationSec,
  stylesheet,
  maxExpression,
  threshold,
  isRTL,
  fps,
  format,
  fileBaseName,
  signal,
}) {
  const exportPayload = buildExportPayload({
    subtitles,
    durationSec,
    stylesheet,
    maxExpression,
    threshold,
    isRTL,
    fps,
    format,
  });

  const startedJob = await startNextSubtitleExport(exportPayload, signal);
  if (!startedJob?.jobId) {
    throw new HttpError(
      502,
      "Next export API did not return a jobId.",
      startedJob,
    );
  }

  const finalStatus = await pollNextExportJob(startedJob.jobId, signal);
  const downloadPath =
    finalStatus?.downloadUrl ||
    `/api/subtitle-export/${encodeURIComponent(startedJob.jobId)}/file`;
  const downloadUrl = buildUrl(CONFIG.nextExportBaseUrl, downloadPath);

  const fileResponse = await fetchWithTimeout(downloadUrl, {
    method: "GET",
    headers: { Accept: "*/*" },
    timeoutMs: CONFIG.httpTimeoutMs,
    signal,
  });
  if (!fileResponse.ok) {
    const text = await fileResponse.text().catch(() => "");
    throw new HttpError(
      502,
      `Failed to download rendered subtitle file (${fileResponse.status}).`,
      text ? { responseBody: text } : null,
    );
  }

  const extension = format === "vp9-alpha-webm" ? "webm" : "mov";
  const safeBaseName =
    sanitizeOptionalString(fileBaseName) || "subtitles-overlay";
  const contentType =
    fileResponse.headers.get("content-type") ||
    (format === "vp9-alpha-webm" ? "video/webm" : "video/quicktime");
  const contentDisposition =
    fileResponse.headers.get("content-disposition") ||
    `attachment; filename="${safeBaseName}-${fps}fps.${extension}"`;

  res.status(200);
  res.setHeader("Content-Type", contentType);
  res.setHeader("Content-Disposition", contentDisposition);
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("X-Subtitle-Export-Job-Id", startedJob.jobId);
  res.setHeader("X-Subtitle-Export-Fps", String(fps));
  res.setHeader("X-Subtitle-Export-Duration-Sec", String(durationSec));

  if (fileResponse.body) {
    await pipeline(Readable.fromWeb(fileResponse.body), res);
  } else {
    const bytes = Buffer.from(await fileResponse.arrayBuffer());
    res.end(bytes);
  }
}

function sanitizeOptionalString(value) {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed ? trimmed : null;
}

function pickUploadedFile(req) {
  if (req?.file) return req.file;
  if (Array.isArray(req?.files) && req.files.length > 0) {
    const preferred = req.files.find((item) =>
      ["file", "video", "audio", "media"].includes(
        String(item?.fieldname || "").toLowerCase(),
      ),
    );
    return preferred || req.files[0];
  }
  return null;
}

function buildExportPayload({
  subtitles,
  durationSec,
  stylesheet,
  maxExpression,
  threshold,
  isRTL,
  fps,
  format,
}) {
  return {
    subtitles,
    width: CONFIG.exportWidth,
    height: CONFIG.exportHeight,
    fps,
    startTimeSec: 0,
    endTimeSec: durationSec,
    duration: durationSec,
    maxExpression,
    threshold,
    isRTL: Boolean(isRTL),
    forceMobile: false,
    stylesheet: stylesheet || null,
    format,
  };
}

const app = express();
app.use(express.json({ limit: "1mb" }));
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: CONFIG.uploadMaxFileBytes,
  },
});

function sendErrorResponse(res, error) {
  if (res.headersSent) {
    if (!res.writableEnded) res.end();
    return;
  }

  if (error instanceof HttpError) {
    const body = { error: error.message };
    if (error.details) body.details = error.details;
    res.status(error.status || 500).json(body);
    return;
  }

  res.status(500).json({
    error:
      error instanceof Error ? error.message : "Unexpected export failure.",
  });
}

app.get("/health", (_req, res) => {
  res.json({
    status: "ok",
    service: "subtitle-export-express-wrapper",
    nextExportBaseUrl: CONFIG.nextExportBaseUrl,
    subtitleSourceBaseUrl: CONFIG.subtitleSourceBaseUrl,
    sourceAuth: {
      hasApiKey: Boolean(CONFIG.subtitleSourceApiKey),
      hasSessionAuth: Boolean(
        SOURCE_AUTH_STATE.authToken && SOURCE_AUTH_STATE.sessionId,
      ),
      autoLoginEnabled: hasSourceAutoLoginCredentials(),
      lastLoginAt: SOURCE_AUTH_STATE.lastLoginAt,
    },
    pluginFlow: {
      uploadApiBaseUrl: CONFIG.uploadApiBaseUrl,
      hasPluginApiKey: Boolean(CONFIG.pluginApiKey),
      statusPollIntervalMs: CONFIG.pluginPollIntervalMs,
      statusPollTimeoutMs: CONFIG.pluginPollTimeoutMs,
    },
  });
});

async function handleExportRequest(req, res) {
  const mediaIdRaw = req.params.mediaId || req.body?.mediaId;
  const mediaId = typeof mediaIdRaw === "string" ? mediaIdRaw.trim() : "";
  if (!mediaId) {
    throw new HttpError(400, "mediaId is required.");
  }

  const language = sanitizeOptionalString(req.body?.language);
  const model = sanitizeOptionalString(req.body?.model);

  const explicitDurationSec = toPositiveNumber(req.body?.duration, 0);
  const fps = Math.max(
    60,
    Math.round(toPositiveNumber(req.body?.fps, CONFIG.exportFps)),
  );
  const format =
    req.body?.format === "vp9-alpha-webm"
      ? "vp9-alpha-webm"
      : CONFIG.exportFormat;
  const maxExpression = toPositiveNumber(req.body?.maxExpression, 1.0);
  const threshold = toPositiveNumber(req.body?.threshold, 0.2);
  const isRTL = parseBoolean(req.body?.isRTL, false);

  const clientAbortController = new AbortController();
  let responseFinished = false;

  const onResponseFinish = () => {
    responseFinished = true;
  };

  const onClientDisconnect = () => {
    if (responseFinished) return;
    if (!clientAbortController.signal.aborted) {
      clientAbortController.abort(
        new HttpError(499, "Client closed the connection."),
      );
    }
  };

  req.on("aborted", onClientDisconnect);
  res.on("finish", onResponseFinish);
  res.on("close", onClientDisconnect);

  try {
    const sourcePayload = await fetchSourceSubtitles(
      mediaId,
      language,
      model,
      clientAbortController.signal,
    );

    const sequenceNumber =
      sourcePayload?.sequence_number || sourcePayload?.vod_id || mediaId;
    const subtitles = dedupeSubtitles(
      normalizeSubtitles(sourcePayload)
        .flat()
        .map((segment) => ({ ...segment, sequence_number: sequenceNumber })),
    );

    if (!subtitles.length) {
      throw new HttpError(404, `No subtitles found for mediaId "${mediaId}".`);
    }

    const subtitleMaxEnd = maxSubtitleEnd(subtitles);
    let durationSec = explicitDurationSec > 0 ? explicitDurationSec : null;
    let metadata = null;

    if (!durationSec) {
      try {
        metadata = await fetchSourceMetadata(
          mediaId,
          clientAbortController.signal,
        );
        durationSec = extractDurationFromMetadata(metadata, subtitleMaxEnd);
      } catch {
        metadata = null;
      }
    }

    if (!durationSec && metadata) {
      const manifestUrl = extractManifestUrl(metadata);
      if (manifestUrl) {
        try {
          durationSec = await fetchManifestDurationSeconds(
            manifestUrl,
            clientAbortController.signal,
          );
        } catch {
          durationSec = null;
        }
      }
    }

    if (!durationSec || durationSec <= 0) {
      durationSec = subtitleMaxEnd;
    }

    if (!durationSec || durationSec <= 0) {
      throw new HttpError(
        422,
        `Could not determine video duration for mediaId "${mediaId}".`,
      );
    }

    await generateAndStreamSubtitleOverlay({
      res,
      subtitles,
      durationSec,
      stylesheet: sourcePayload?.stylesheet || null,
      maxExpression,
      threshold,
      isRTL,
      fps,
      format,
      fileBaseName: `subtitles-${mediaId}`,
      signal: clientAbortController.signal,
    });
  } finally {
    req.off("aborted", onClientDisconnect);
    res.off("finish", onResponseFinish);
    res.off("close", onClientDisconnect);
  }
}

async function handleUploadAndExportRequest(req, res) {
  const uploadedFile = pickUploadedFile(req);
  if (!uploadedFile?.buffer?.length) {
    throw new HttpError(
      400,
      'A file is required. Send multipart/form-data with a file field like "file" (or "video"/"audio").',
    );
  }

  const source = sanitizeOptionalString(req.body?.source) || "postman-upload";
  const pluginVodId =
    sanitizeOptionalString(req.body?.pluginVodId) ||
    sanitizeOptionalString(req.body?.chrome_plugin_vod_id) ||
    crypto.randomUUID();
  const title =
    sanitizeOptionalString(req.body?.name) ||
    sanitizeOptionalString(req.body?.title) ||
    sanitizeOptionalString(uploadedFile.originalname) ||
    `upload-${pluginVodId}`;

  const explicitDurationSec = toPositiveNumber(req.body?.duration, 0);
  const fps = Math.max(
    60,
    Math.round(toPositiveNumber(req.body?.fps, CONFIG.exportFps)),
  );
  const format =
    req.body?.format === "vp9-alpha-webm"
      ? "vp9-alpha-webm"
      : CONFIG.exportFormat;
  const maxExpression = toPositiveNumber(req.body?.maxExpression, 1.0);
  const threshold = toPositiveNumber(req.body?.threshold, 0.2);
  const isRTL = parseBoolean(req.body?.isRTL, false);

  const clientAbortController = new AbortController();
  let responseFinished = false;

  const onResponseFinish = () => {
    responseFinished = true;
  };

  const onClientDisconnect = () => {
    if (responseFinished) return;
    if (!clientAbortController.signal.aborted) {
      clientAbortController.abort(
        new HttpError(499, "Client closed the connection."),
      );
    }
  };

  req.on("aborted", onClientDisconnect);
  res.on("finish", onResponseFinish);
  res.on("close", onClientDisconnect);

  try {
    await uploadFileToPhontBackend({
      buffer: uploadedFile.buffer,
      mimeType: uploadedFile.mimetype,
      originalName: uploadedFile.originalname,
      source,
      pluginVodId,
      title,
      signal: clientAbortController.signal,
    });

    const statusData = await pollPluginVodStatusByContext({
      source,
      pluginVodId,
      signal: clientAbortController.signal,
    });

    const subtitlesPayload = await fetchVodSubtitlesByPluginContext({
      source,
      pluginVodId,
      signal: clientAbortController.signal,
    });

    const sequenceNumber =
      statusData?.vod_id ||
      subtitlesPayload?.vod_id ||
      subtitlesPayload?.sequence_number ||
      pluginVodId;
    const subtitles = dedupeSubtitles(
      normalizeSubtitles(subtitlesPayload)
        .flat()
        .map((segment) => ({ ...segment, sequence_number: sequenceNumber })),
    );
    if (!subtitles.length) {
      throw new HttpError(
        404,
        `No subtitles were returned for source "${source}" and plugin VOD "${pluginVodId}".`,
      );
    }

    const subtitleMaxEnd = maxSubtitleEnd(subtitles);
    let durationSec = explicitDurationSec > 0 ? explicitDurationSec : null;
    let metadata = null;

    if (!durationSec) {
      durationSec = extractDurationFromMetadata(
        subtitlesPayload,
        subtitleMaxEnd,
      );
    }
    if (!durationSec) {
      durationSec = extractDurationFromMetadata(statusData, subtitleMaxEnd);
    }

    const resolvedVodId =
      sanitizeOptionalString(statusData?.vod_id) ||
      sanitizeOptionalString(subtitlesPayload?.vod_id) ||
      null;

    if (!durationSec && resolvedVodId) {
      try {
        metadata = await fetchSourceMetadata(
          resolvedVodId,
          clientAbortController.signal,
        );
        durationSec = extractDurationFromMetadata(metadata, subtitleMaxEnd);
      } catch {
        metadata = null;
      }
    }

    if (!durationSec && metadata) {
      const manifestUrl = extractManifestUrl(metadata);
      if (manifestUrl) {
        try {
          durationSec = await fetchManifestDurationSeconds(
            manifestUrl,
            clientAbortController.signal,
          );
        } catch {
          durationSec = null;
        }
      }
    }

    if (!durationSec || durationSec <= 0) {
      durationSec = subtitleMaxEnd;
    }
    if (!durationSec || durationSec <= 0) {
      throw new HttpError(
        422,
        "Could not determine subtitle/video duration from processing output.",
      );
    }

    res.setHeader("X-Phont-Plugin-Source", source);
    res.setHeader("X-Phont-Plugin-Vod-Id", pluginVodId);
    if (resolvedVodId) {
      res.setHeader("X-Phont-Vod-Id", resolvedVodId);
    }

    await generateAndStreamSubtitleOverlay({
      res,
      subtitles,
      durationSec,
      stylesheet: subtitlesPayload?.stylesheet || null,
      maxExpression,
      threshold,
      isRTL,
      fps,
      format,
      fileBaseName:
        sanitizeOptionalString(req.body?.outputName) ||
        `subtitles-${source}-${pluginVodId}`,
      signal: clientAbortController.signal,
    });
  } finally {
    req.off("aborted", onClientDisconnect);
    res.off("finish", onResponseFinish);
    res.off("close", onClientDisconnect);
  }
}

app.post("/api/subtitle-overlay/export", async (req, res) => {
  try {
    await handleExportRequest(req, res);
  } catch (error) {
    sendErrorResponse(res, error);
  }
});

app.post("/api/subtitle-overlay/export/:mediaId", async (req, res) => {
  try {
    req.body = { ...(req.body || {}), mediaId: req.params.mediaId };
    await handleExportRequest(req, res);
  } catch (error) {
    sendErrorResponse(res, error);
  }
});

app.post(
  "/api/subtitle-overlay/export-upload",
  upload.any(),
  async (req, res) => {
    try {
      await handleUploadAndExportRequest(req, res);
    } catch (error) {
      sendErrorResponse(res, error);
    }
  },
);

app.post(
  "/api/subtitle-overlay/export-upload/:source/:pluginVodId",
  upload.any(),
  async (req, res) => {
    try {
      req.body = {
        ...(req.body || {}),
        source: req.params.source,
        pluginVodId: req.params.pluginVodId,
      };
      await handleUploadAndExportRequest(req, res);
    } catch (error) {
      sendErrorResponse(res, error);
    }
  },
);

app.use((error, _req, res, next) => {
  if (!error) {
    next();
    return;
  }

  if (error?.name === "MulterError") {
    if (error.code === "LIMIT_FILE_SIZE") {
      sendErrorResponse(
        res,
        new HttpError(
          413,
          `Uploaded file is too large. Max allowed is ${CONFIG.uploadMaxFileBytes} bytes.`,
        ),
      );
      return;
    }
    sendErrorResponse(
      res,
      new HttpError(400, error.message || "Invalid upload payload."),
    );
    return;
  }

  sendErrorResponse(res, error);
});

app.use((_, res) => {
  res.status(404).json({ error: "Not found." });
});

const server = app.listen(CONFIG.port, () => {
  console.log(`[subtitle-export-api] Listening on port ${CONFIG.port}`);
  console.log(
    `[subtitle-export-api] Next export base: ${CONFIG.nextExportBaseUrl}`,
  );
  console.log(
    `[subtitle-export-api] Subtitle source base: ${CONFIG.subtitleSourceBaseUrl}`,
  );
  console.log(
    `[subtitle-export-api] Upload API base: ${CONFIG.uploadApiBaseUrl}`,
  );
  console.log(
    `[subtitle-export-api] Source auto-login: ${hasSourceAutoLoginCredentials() ? "enabled" : "disabled"}`,
  );
  console.log(
    `[subtitle-export-api] Plugin status polling: every ${CONFIG.pluginPollIntervalMs}ms (timeout ${CONFIG.pluginPollTimeoutMs}ms)`,
  );
});

const socketTimeoutMs = CONFIG.exportPollTimeoutMs + 120_000;
server.setTimeout(socketTimeoutMs);
server.requestTimeout = socketTimeoutMs;
