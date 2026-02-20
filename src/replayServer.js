import fs from "node:fs";
import path from "node:path";
import http from "node:http";
import readline from "node:readline";
import crypto from "node:crypto";
import { URL, fileURLToPath } from "node:url";
import {
  analyzeWindows,
  DEFAULT_PATTERN_CONFIG,
  deriveWindowPatternInfo,
  normalizePatternConfig,
  PATTERN_PRIORITY,
  stableStringify,
  toPrice as toPatternPrice,
  toUpdownPointTimeMs as toPatternTimeMs
} from "./scripts/patterns5mCore.js";

const PORT = Math.max(1, Number(process.env.REPLAY_PORT || 8787));
const HOST = process.env.REPLAY_HOST || "0.0.0.0";
const LOG_ROOT = path.resolve(process.cwd(), process.env.COLLECTOR_OUTPUT_DIR || "./logs/raw");
const DERIVED_PATTERN_ROOT = path.resolve(
  process.cwd(),
  process.env.REPLAY_PATTERN_DERIVED_DIR || "./logs/derived/patterns"
);
const PATTERN_CONFIG_PATH = path.resolve(
  process.cwd(),
  process.env.REPLAY_PATTERN_CONFIG_PATH || "./config/patterns5m.json"
);
const DOC_ROOT = path.resolve(process.cwd(), "./docs");
const BUCKET_MS = 5 * 60 * 1000;
const MARKET_WINDOW_MS = 5 * 60 * 1000;
const COMPLETE_MIN_MS = 4 * 60 * 1000;
const PATTERN_STORE_SCHEMA_VERSION = 1;
const PATTERN_CACHE = new Map();
const PATTERN_CONFIG_CACHE = {
  signature: "",
  value: normalizePatternConfig(DEFAULT_PATTERN_CONFIG),
  hash: crypto.createHash("sha256").update(
    stableStringify(normalizePatternConfig(DEFAULT_PATTERN_CONFIG))
  ).digest("hex"),
  patternSetVersion: normalizePatternConfig(DEFAULT_PATTERN_CONFIG).patternSetVersion || "1",
  sourcePath: PATTERN_CONFIG_PATH
};

function isFiniteNumber(x) {
  const n = typeof x === "string" ? Number(x) : x;
  return Number.isFinite(n) ? n : null;
}

function toTimeMs(row) {
  return isFiniteNumber(row?.event_time_ms) ?? isFiniteNumber(row?.receive_time_ms);
}

function toUpdownPointTimeMs(row) {
  return isFiniteNumber(row?.bucket_end_ms) ??
    isFiniteNumber(row?.last_event_time_ms) ??
    isFiniteNumber(row?.event_time_ms) ??
    isFiniteNumber(row?.receive_time_ms);
}

function datePartUtc(ms) {
  const d = new Date(ms);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function isDateDirName(name) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(name || ""));
}

function toBoolParam(raw, defaultValue = false) {
  if (raw === null || raw === undefined || raw === "") return defaultValue;
  const s = String(raw).trim().toLowerCase();
  if (["1", "true", "yes", "y", "on"].includes(s)) return true;
  if (["0", "false", "no", "n", "off"].includes(s)) return false;
  return defaultValue;
}

function listDateDirs() {
  if (!fs.existsSync(LOG_ROOT)) return [];
  const out = [];
  for (const entry of fs.readdirSync(LOG_ROOT, { withFileTypes: true })) {
    if (entry.isDirectory() && isDateDirName(entry.name)) out.push(entry.name);
  }
  out.sort();
  return out;
}

async function eachJsonl(filePath, onRow) {
  if (!fs.existsSync(filePath)) return;
  const stream = fs.createReadStream(filePath, { encoding: "utf8" });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  for await (const line of rl) {
    if (!line || !line.trim()) continue;
    let row = null;
    try {
      row = JSON.parse(line);
    } catch {
      continue;
    }
    await onRow(row);
  }
}

function bucketStartMs(t, bucketMs = BUCKET_MS) {
  return Math.floor(t / bucketMs) * bucketMs;
}

function parseMarketStartMsFromSlug(slug) {
  const s = String(slug || "");
  const m = s.match(/btc-updown-(\d+)m-(\d{10})$/);
  if (!m) return null;
  const sec = Number(m[2]);
  if (!Number.isFinite(sec)) return null;
  return sec * 1000;
}

function parseMarketWindowMsFromSlug(slug) {
  const s = String(slug || "");
  const m = s.match(/btc-updown-(\d+)m-(\d{10})$/);
  if (!m) return null;
  const mins = Number(m[1]);
  if (!Number.isFinite(mins) || mins <= 0) return null;
  return mins * 60 * 1000;
}

function listWindowIdsForDate(date) {
  const dayDir = path.join(LOG_ROOT, date);
  if (!fs.existsSync(dayDir)) return [];
  const out = [];
  for (const entry of fs.readdirSync(dayDir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    if (entry.name.startsWith(".")) continue;
    out.push(entry.name);
  }
  out.sort();
  return out;
}

function statSignature(filePath) {
  if (!fs.existsSync(filePath)) return `${filePath}:missing`;
  const st = fs.statSync(filePath);
  return `${filePath}:${st.size}:${Math.floor(st.mtimeMs)}`;
}

function ensurePatternDir(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function fileStatSignature(filePath) {
  if (!fs.existsSync(filePath)) return `${filePath}:missing`;
  const st = fs.statSync(filePath);
  return `${filePath}:${st.size}:${Math.floor(st.mtimeMs)}`;
}

function normalizePatternIdList(list) {
  if (!Array.isArray(list)) return [];
  const seen = new Set();
  const out = [];
  for (const x of list) {
    const key = String(x || "");
    if (!PATTERN_PRIORITY.includes(key)) continue;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(key);
  }
  return out;
}

function emptyPatternSideHits() {
  return Object.fromEntries(PATTERN_PRIORITY.map((k) => [k, []]));
}

function emptyPatternInfo() {
  return {
    patterns: [],
    patternPrimary: null,
    patternSideHits: emptyPatternSideHits()
  };
}

function normalizePatternInfo(raw) {
  if (!raw || typeof raw !== "object") return emptyPatternInfo();
  const patternSideHits = emptyPatternSideHits();
  for (const k of PATTERN_PRIORITY) {
    const arr = Array.isArray(raw?.patternSideHits?.[k]) ? raw.patternSideHits[k] : [];
    patternSideHits[k] = arr.map((item) => {
      const side = String(item?.side || "").toLowerCase();
      return {
        side: side === "down" ? "down" : "up",
        metrics: item?.metrics && typeof item.metrics === "object" ? item.metrics : null
      };
    });
  }
  const patterns = normalizePatternIdList(raw.patterns);
  const primary = patterns.includes(raw.patternPrimary) ? raw.patternPrimary : (patterns[0] || null);
  return {
    patterns,
    patternPrimary: primary,
    patternSideHits
  };
}

function sanitizeStorePart(value, fallback = "na") {
  const out = String(value || "").trim().replace(/[^a-zA-Z0-9._-]+/g, "_");
  return out || fallback;
}

function resolvePatternConfigState() {
  const signature = fileStatSignature(PATTERN_CONFIG_PATH);
  if (PATTERN_CONFIG_CACHE.signature === signature && PATTERN_CONFIG_CACHE.hash) {
    return {
      value: PATTERN_CONFIG_CACHE.value,
      hash: PATTERN_CONFIG_CACHE.hash,
      patternSetVersion: PATTERN_CONFIG_CACHE.patternSetVersion,
      sourcePath: PATTERN_CONFIG_CACHE.sourcePath
    };
  }

  let raw = DEFAULT_PATTERN_CONFIG;
  if (fs.existsSync(PATTERN_CONFIG_PATH)) {
    try {
      raw = JSON.parse(fs.readFileSync(PATTERN_CONFIG_PATH, "utf8"));
    } catch {
      raw = DEFAULT_PATTERN_CONFIG;
    }
  }
  const value = normalizePatternConfig(raw);
  const hash = crypto.createHash("sha256").update(stableStringify(value)).digest("hex");
  const patternSetVersion = String(value?.patternSetVersion || "1");

  PATTERN_CONFIG_CACHE.signature = signature;
  PATTERN_CONFIG_CACHE.value = value;
  PATTERN_CONFIG_CACHE.hash = hash;
  PATTERN_CONFIG_CACHE.patternSetVersion = patternSetVersion;
  PATTERN_CONFIG_CACHE.sourcePath = PATTERN_CONFIG_PATH;

  return {
    value,
    hash,
    patternSetVersion,
    sourcePath: PATTERN_CONFIG_PATH
  };
}

function getIntervalPatternKey(interval) {
  const raw = String(interval?.windowId || interval?.marketSlug || "").trim();
  if (raw) return raw;
  const startMs = Number(isFiniteNumber(interval?.startMs) ?? -1);
  const endMs = Number(isFiniteNumber(interval?.endMs) ?? -1);
  return `window-${startMs}-${endMs}`;
}

function getPatternStorePath(date, includeIncomplete, patternSetVersion, paramsHash) {
  const datePart = sanitizeStorePart(`date=${date}`);
  const includePart = includeIncomplete ? "1" : "0";
  const setPart = sanitizeStorePart(patternSetVersion, "1");
  const hashPart = sanitizeStorePart(paramsHash, "hash");
  return path.join(
    DERIVED_PATTERN_ROOT,
    datePart,
    `patterns_inc-${includePart}_set-${setPart}_hash-${hashPart}.json`
  );
}

function readPatternStore(filePath) {
  if (!fs.existsSync(filePath)) return null;
  try {
    const parsed = JSON.parse(fs.readFileSync(filePath, "utf8"));
    if (!parsed || typeof parsed !== "object") return null;
    if (Number(parsed.schemaVersion) !== PATTERN_STORE_SCHEMA_VERSION) return null;
    return parsed;
  } catch {
    return null;
  }
}

function writePatternStore(filePath, payload) {
  ensurePatternDir(path.dirname(filePath));
  const tmpPath = `${filePath}.tmp-${process.pid}-${Date.now()}`;
  fs.writeFileSync(tmpPath, JSON.stringify(payload, null, 2));
  fs.renameSync(tmpPath, filePath);
}

function buildPatternSummary(intervals, byWindowId, includeIncomplete) {
  const summary = Object.fromEntries(PATTERN_PRIORITY.map((k) => [k, { windowCount: 0 }]));
  let countedWindows = 0;

  for (const interval of intervals) {
    if (!includeIncomplete && !interval.isComplete) continue;
    countedWindows += 1;
    const key = getIntervalPatternKey(interval);
    const info = byWindowId.get(key) || emptyPatternInfo();
    for (const patternId of info.patterns || []) {
      if (!summary[patternId]) continue;
      summary[patternId].windowCount += 1;
    }
  }

  return {
    ...summary,
    countedWindows,
    includeIncomplete
  };
}

function computeDayPatternSignature(date) {
  const dayDir = path.join(LOG_ROOT, date);
  if (!fs.existsSync(dayDir)) return `${date}:missing`;
  const parts = [statSignature(dayDir)];
  const partitioned = hasPartitionedWindowDirs(date);

  if (partitioned) {
    const windows = listWindowIdsForDate(date);
    for (const windowId of windows) {
      const { updownPath, btcPath } = windowFilesForDate(date, windowId);
      parts.push(statSignature(path.join(dayDir, windowId)));
      parts.push(statSignature(updownPath));
      parts.push(statSignature(btcPath));
    }
  } else {
    const { updownPath, btcPath } = windowFilesForDate(date, "");
    parts.push(statSignature(updownPath));
    parts.push(statSignature(btcPath));
  }
  return crypto.createHash("sha256").update(parts.join("|")).digest("hex");
}

function hasPartitionedWindowDirs(date) {
  const dayDir = path.join(LOG_ROOT, date);
  if (!fs.existsSync(dayDir)) return false;
  for (const windowId of listWindowIdsForDate(date)) {
    const winDir = path.join(dayDir, windowId);
    const hasUpdown = fs.existsSync(path.join(winDir, "updown_state.jsonl"));
    const hasBtc = fs.existsSync(path.join(winDir, "btc_reference.jsonl"));
    if (hasUpdown || hasBtc) return true;
  }
  return false;
}

function windowFilesForDate(date, windowId = "") {
  const dayDir = path.join(LOG_ROOT, date);
  if (windowId) {
    const winDir = path.join(dayDir, windowId);
    return {
      updownPath: path.join(winDir, "updown_state.jsonl"),
      btcPath: path.join(winDir, "btc_reference.jsonl"),
      ptbPath: path.join(winDir, "ptb_reference.jsonl")
    };
  }
  return {
    updownPath: path.join(dayDir, "updown_state.jsonl"),
    btcPath: path.join(dayDir, "btc_reference.jsonl"),
    ptbPath: path.join(dayDir, "ptb_reference.jsonl")
  };
}

function formatIntervalLabel(startMs, endMs) {
  const start = new Date(startMs);
  const end = new Date(endMs);
  const date = `${start.getUTCFullYear()}-${String(start.getUTCMonth() + 1).padStart(2, "0")}-${String(start.getUTCDate()).padStart(2, "0")}`;
  const hh1 = String(start.getUTCHours()).padStart(2, "0");
  const mm1 = String(start.getUTCMinutes()).padStart(2, "0");
  const hh2 = String(end.getUTCHours()).padStart(2, "0");
  const mm2 = String(end.getUTCMinutes()).padStart(2, "0");
  return `${date} ${hh1}:${mm1} - ${hh2}:${mm2}`;
}

async function buildIntervals(date, bucketMs = BUCKET_MS) {
  if (hasPartitionedWindowDirs(date)) {
    return buildIntervalsFromWindowDirs(date);
  }
  return buildIntervalsLegacy(date, bucketMs);
}

async function buildIntervalsFromWindowDirs(date) {
  const intervals = [];
  for (const windowId of listWindowIdsForDate(date)) {
    const { updownPath, btcPath } = windowFilesForDate(date, windowId);
    if (!fs.existsSync(updownPath) && !fs.existsSync(btcPath)) continue;

    let marketSlug = windowId === "_unassigned" ? "" : windowId;
    const parsedStartMs = parseMarketStartMsFromSlug(windowId);
    const parsedWindowMs = parseMarketWindowMsFromSlug(windowId) ?? MARKET_WINDOW_MS;
    let startMs = parsedStartMs;
    let endMs = parsedStartMs !== null ? parsedStartMs + parsedWindowMs : null;

    let btcPoints = 0;
    let upPoints = 0;
    let downPoints = 0;
    let upSampleCount = 0;
    let downSampleCount = 0;
    let btcMinMs = null;
    let btcMaxMs = null;
    let upMinMs = null;
    let upMaxMs = null;
    let downMinMs = null;
    let downMaxMs = null;

    await eachJsonl(updownPath, (row) => {
      const ts = toUpdownPointTimeMs(row);
      const side = String(row?.side || "").toLowerCase();
      const odds = isFiniteNumber(row?.mid) ?? isFiniteNumber(row?.last_trade_price);
      const sampleCount = Math.max(1, Number(isFiniteNumber(row?.sample_count) ?? 1));
      if (ts === null || odds === null) return;
      const slug = String(row?.market_slug || "");
      if (!marketSlug && slug) marketSlug = slug;
      if (side === "up") upPoints += 1;
      if (side === "down") downPoints += 1;
      if (side === "up") upSampleCount += sampleCount;
      if (side === "down") downSampleCount += sampleCount;
      if (side === "up") {
        upMinMs = upMinMs === null ? ts : Math.min(upMinMs, ts);
        upMaxMs = upMaxMs === null ? ts : Math.max(upMaxMs, ts);
      }
      if (side === "down") {
        downMinMs = downMinMs === null ? ts : Math.min(downMinMs, ts);
        downMaxMs = downMaxMs === null ? ts : Math.max(downMaxMs, ts);
      }
    });

    await eachJsonl(btcPath, (row) => {
      const ts = toTimeMs(row);
      const price = isFiniteNumber(row?.price);
      if (ts === null || price === null) return;
      btcPoints += 1;
      btcMinMs = btcMinMs === null ? ts : Math.min(btcMinMs, ts);
      btcMaxMs = btcMaxMs === null ? ts : Math.max(btcMaxMs, ts);
    });

    const derivedMin = [btcMinMs, upMinMs, downMinMs].filter((x) => x !== null);
    const derivedMax = [btcMaxMs, upMaxMs, downMaxMs].filter((x) => x !== null);
    if (startMs === null && derivedMin.length > 0) startMs = Math.min(...derivedMin);
    if (endMs === null && derivedMax.length > 0) endMs = Math.max(...derivedMax);
    if (startMs === null || endMs === null) continue;
    if (endMs <= startMs) endMs = startMs + (parseMarketWindowMsFromSlug(marketSlug) ?? MARKET_WINDOW_MS);

    const hasOdds = upPoints > 0 || downPoints > 0;
    if (!(btcPoints > 0 && hasOdds)) continue;

    const btcCoverageMs = btcMinMs !== null && btcMaxMs !== null ? (btcMaxMs - btcMinMs) : 0;
    const upCoverageMs = upMinMs !== null && upMaxMs !== null ? (upMaxMs - upMinMs) : 0;
    const downCoverageMs = downMinMs !== null && downMaxMs !== null ? (downMaxMs - downMinMs) : 0;
    const oddsCoverageMs = Math.max(upCoverageMs, downCoverageMs);
    const isComplete = btcCoverageMs >= COMPLETE_MIN_MS && oddsCoverageMs >= COMPLETE_MIN_MS;

    intervals.push({
      windowId,
      marketSlug,
      startMs,
      endMs,
      btcPoints,
      upPoints,
      downPoints,
      upSampleCount,
      downSampleCount,
      hasBtc: btcPoints > 0,
      hasOdds,
      btcCoverageMs,
      upCoverageMs,
      downCoverageMs,
      oddsCoverageMs,
      isComplete,
      label: formatIntervalLabel(startMs, endMs)
    });
  }

  intervals.sort((a, b) => a.startMs - b.startMs);
  return intervals;
}

async function buildIntervalsLegacy(date, bucketMs = BUCKET_MS) {
  const dayDir = path.join(LOG_ROOT, date);
  const btcPath = path.join(dayDir, "btc_reference.jsonl");
  const updownPath = path.join(dayDir, "updown_state.jsonl");
  const buckets = new Map();

  const ensureBucket = (startMs, marketSlug = "") => {
    let item = buckets.get(startMs);
    if (!item) {
      item = {
        startMs,
        endMs: startMs + MARKET_WINDOW_MS,
        marketSlug: marketSlug || "",
        btcPoints: 0,
        upPoints: 0,
        downPoints: 0,
        upSampleCount: 0,
        downSampleCount: 0,
        btcMinMs: null,
        btcMaxMs: null,
        upMinMs: null,
        upMaxMs: null,
        downMinMs: null,
        downMaxMs: null
      };
      buckets.set(startMs, item);
    } else if (!item.marketSlug && marketSlug) {
      item.marketSlug = marketSlug;
    }
    return item;
  };

  await eachJsonl(updownPath, (row) => {
    const ts = toUpdownPointTimeMs(row);
    const side = String(row?.side || "").toLowerCase();
    const odds = isFiniteNumber(row?.mid) ?? isFiniteNumber(row?.last_trade_price);
    const sampleCount = Math.max(1, Number(isFiniteNumber(row?.sample_count) ?? 1));
    if (ts === null || odds === null) return;
    const slug = String(row?.market_slug || "");
    const startMsFromSlug = parseMarketStartMsFromSlug(slug);
    const startMs = startMsFromSlug ?? bucketStartMs(ts, bucketMs);
    const bucket = ensureBucket(startMs, slug);
    if (side === "up") bucket.upPoints += 1;
    if (side === "down") bucket.downPoints += 1;
    if (side === "up") bucket.upSampleCount += sampleCount;
    if (side === "down") bucket.downSampleCount += sampleCount;
    if (side === "up") {
      bucket.upMinMs = bucket.upMinMs === null ? ts : Math.min(bucket.upMinMs, ts);
      bucket.upMaxMs = bucket.upMaxMs === null ? ts : Math.max(bucket.upMaxMs, ts);
    }
    if (side === "down") {
      bucket.downMinMs = bucket.downMinMs === null ? ts : Math.min(bucket.downMinMs, ts);
      bucket.downMaxMs = bucket.downMaxMs === null ? ts : Math.max(bucket.downMaxMs, ts);
    }
  });

  await eachJsonl(btcPath, (row) => {
    const ts = toTimeMs(row);
    const price = isFiniteNumber(row?.price);
    if (ts === null || price === null) return;
    const startMs = bucketStartMs(ts, bucketMs);
    const bucket = ensureBucket(startMs);
    bucket.btcPoints += 1;
    bucket.btcMinMs = bucket.btcMinMs === null ? ts : Math.min(bucket.btcMinMs, ts);
    bucket.btcMaxMs = bucket.btcMaxMs === null ? ts : Math.max(bucket.btcMaxMs, ts);
  });

  return Array.from(buckets.values())
    .filter((x) => x.btcPoints > 0 && (x.upPoints > 0 || x.downPoints > 0))
    .sort((a, b) => a.startMs - b.startMs)
    .map((x) => {
      const btcCoverageMs = x.btcMinMs !== null && x.btcMaxMs !== null ? (x.btcMaxMs - x.btcMinMs) : 0;
      const upCoverageMs = x.upMinMs !== null && x.upMaxMs !== null ? (x.upMaxMs - x.upMinMs) : 0;
      const downCoverageMs = x.downMinMs !== null && x.downMaxMs !== null ? (x.downMaxMs - x.downMinMs) : 0;
      const oddsCoverageMs = Math.max(upCoverageMs, downCoverageMs);
      const isComplete = btcCoverageMs >= COMPLETE_MIN_MS && oddsCoverageMs >= COMPLETE_MIN_MS;
      return {
        windowId: x.marketSlug || "",
        ...x,
        hasBtc: x.btcPoints > 0,
        hasOdds: x.upPoints > 0 || x.downPoints > 0,
        btcCoverageMs,
        upCoverageMs,
        downCoverageMs,
        oddsCoverageMs,
        isComplete,
        label: formatIntervalLabel(x.startMs, x.endMs)
      };
    });
}

function getIntervalSourceSignature(date, interval, partitioned) {
  const key = getIntervalPatternKey(interval);
  const parts = [
    key,
    String(isFiniteNumber(interval.startMs) ?? ""),
    String(isFiniteNumber(interval.endMs) ?? ""),
    String(Boolean(interval.isComplete)),
    String(isFiniteNumber(interval.btcCoverageMs) ?? 0),
    String(isFiniteNumber(interval.oddsCoverageMs) ?? 0),
    String(isFiniteNumber(interval.upCoverageMs) ?? 0),
    String(isFiniteNumber(interval.downCoverageMs) ?? 0)
  ];
  if (partitioned) {
    const { updownPath, btcPath } = windowFilesForDate(date, String(interval.windowId || key));
    parts.push(statSignature(updownPath));
    parts.push(statSignature(btcPath));
  } else {
    const { updownPath, btcPath } = windowFilesForDate(date, "");
    parts.push(statSignature(updownPath));
    parts.push(statSignature(btcPath));
  }
  return parts.join("|");
}

async function loadSidePointsForInterval(date, interval, partitioned) {
  const sidePoints = { up: [], down: [] };
  if (partitioned) {
    const key = String(interval.windowId || getIntervalPatternKey(interval));
    const { updownPath } = windowFilesForDate(date, key);
    await eachJsonl(updownPath, (row) => {
      const side = String(row?.side || "").toLowerCase();
      if (side !== "up" && side !== "down") return;
      const ts = toPatternTimeMs(row);
      const price = toPatternPrice(row);
      if (ts === null || price === null) return;
      sidePoints[side].push({ ts, price });
    });
    return sidePoints;
  }

  const { updownPath } = windowFilesForDate(date, "");
  const slugFilter = String(interval.marketSlug || "").trim();
  const startMs = isFiniteNumber(interval.startMs);
  const endMs = isFiniteNumber(interval.endMs);
  await eachJsonl(updownPath, (row) => {
    const side = String(row?.side || "").toLowerCase();
    if (side !== "up" && side !== "down") return;
    const ts = toPatternTimeMs(row);
    const price = toPatternPrice(row);
    if (ts === null || price === null) return;
    const slug = String(row?.market_slug || "").trim();
    if (slugFilter) {
      if (slug !== slugFilter) return;
    } else if (startMs !== null && endMs !== null && (ts < startMs || ts > endMs)) {
      return;
    }
    sidePoints[side].push({ ts, price });
  });
  return sidePoints;
}

async function computePatternInfoForInterval(
  date,
  interval,
  includeIncomplete,
  patternConfig,
  partitioned
) {
  if (!includeIncomplete && !interval.isComplete) return emptyPatternInfo();
  const key = getIntervalPatternKey(interval);
  const sidePoints = await loadSidePointsForInterval(date, interval, partitioned);
  const window = {
    date,
    windowId: key,
    marketSlug: String(interval.marketSlug || key),
    startMs: interval.startMs,
    endMs: interval.endMs,
    sidePoints,
    btcPoints: [],
    coverage: {
      upCoverageMs: interval.upCoverageMs || 0,
      downCoverageMs: interval.downCoverageMs || 0,
      oddsCoverageMs: interval.oddsCoverageMs || 0,
      btcCoverageMs: interval.btcCoverageMs || 0
    },
    isComplete: Boolean(interval.isComplete)
  };
  const analyzed = analyzeWindows([window], true, null, patternConfig);
  return normalizePatternInfo(deriveWindowPatternInfo(window, analyzed.hits));
}

function evictPatternCacheIfNeeded(maxSize = 32) {
  while (PATTERN_CACHE.size > maxSize) {
    const firstKey = PATTERN_CACHE.keys().next().value;
    if (!firstKey) break;
    PATTERN_CACHE.delete(firstKey);
  }
}

async function buildPatternIndexByScan(date, intervals, includeIncomplete, patternConfig) {
  const partitioned = hasPartitionedWindowDirs(date);
  const sidePointsByWindowId = new Map();

  if (partitioned) {
    for (const interval of intervals) {
      const key = getIntervalPatternKey(interval);
      const { updownPath } = windowFilesForDate(date, String(interval.windowId || key));
      const sidePoints = { up: [], down: [] };
      await eachJsonl(updownPath, (row) => {
        const side = String(row?.side || "").toLowerCase();
        if (side !== "up" && side !== "down") return;
        const ts = toPatternTimeMs(row);
        const price = toPatternPrice(row);
        if (ts === null || price === null) return;
        sidePoints[side].push({ ts, price });
      });
      sidePointsByWindowId.set(key, sidePoints);
    }
  } else {
    const { updownPath } = windowFilesForDate(date, "");
    const sidePointsBySlug = new Map();
    await eachJsonl(updownPath, (row) => {
      const slug = String(row?.market_slug || "").trim();
      if (!slug) return;
      const side = String(row?.side || "").toLowerCase();
      if (side !== "up" && side !== "down") return;
      const ts = toPatternTimeMs(row);
      const price = toPatternPrice(row);
      if (ts === null || price === null) return;
      let sidePoints = sidePointsBySlug.get(slug);
      if (!sidePoints) {
        sidePoints = { up: [], down: [] };
        sidePointsBySlug.set(slug, sidePoints);
      }
      sidePoints[side].push({ ts, price });
    });
    for (const interval of intervals) {
      const key = getIntervalPatternKey(interval);
      sidePointsByWindowId.set(key, sidePointsBySlug.get(String(interval.marketSlug || "")) || { up: [], down: [] });
    }
  }

  const windows = intervals.map((interval) => {
    const key = getIntervalPatternKey(interval);
    const sidePoints = sidePointsByWindowId.get(key) || { up: [], down: [] };
    return {
      date,
      windowId: key,
      marketSlug: String(interval.marketSlug || key),
      startMs: interval.startMs,
      endMs: interval.endMs,
      sidePoints,
      btcPoints: [],
      coverage: {
        upCoverageMs: interval.upCoverageMs || 0,
        downCoverageMs: interval.downCoverageMs || 0,
        oddsCoverageMs: interval.oddsCoverageMs || 0,
        btcCoverageMs: interval.btcCoverageMs || 0
      },
      isComplete: Boolean(interval.isComplete)
    };
  });

  const analyzed = analyzeWindows(windows, includeIncomplete, null, patternConfig);
  const byWindowId = new Map();
  for (const window of windows) {
    byWindowId.set(window.windowId || window.marketSlug, normalizePatternInfo(deriveWindowPatternInfo(window, analyzed.hits)));
  }
  return {
    byWindowId,
    summary: buildPatternSummary(intervals, byWindowId, includeIncomplete)
  };
}

async function buildPatternIndexForDate(date, intervals, includeIncomplete = false) {
  const daySignature = computeDayPatternSignature(date);
  const patternConfigState = resolvePatternConfigState();
  const cacheKey = `${date}|${includeIncomplete ? "1" : "0"}|${patternConfigState.hash}|${daySignature}`;
  const cached = PATTERN_CACHE.get(cacheKey);
  if (cached) return cached;

  const partitioned = hasPartitionedWindowDirs(date);
  if (!partitioned) {
    const fallback = await buildPatternIndexByScan(
      date,
      intervals,
      includeIncomplete,
      patternConfigState.value
    );
    PATTERN_CACHE.set(cacheKey, fallback);
    evictPatternCacheIfNeeded();
    return fallback;
  }

  const byWindowId = new Map();
  const storePath = getPatternStorePath(
    date,
    includeIncomplete,
    patternConfigState.patternSetVersion,
    patternConfigState.hash
  );
  const existingStore = readPatternStore(storePath);
  const existingWindows = existingStore?.windows && typeof existingStore.windows === "object"
    ? existingStore.windows
    : {};
  const nextWindows = {};
  let storeChanged = !existingStore;

  for (const interval of intervals) {
    const key = getIntervalPatternKey(interval);
    const sourceSignature = getIntervalSourceSignature(date, interval, true);
    const prev = existingWindows[key];
    const canReuse = Boolean(
      prev
      && prev.sourceSignature === sourceSignature
      && prev.paramsHash === patternConfigState.hash
      && prev.patternSetVersion === patternConfigState.patternSetVersion
      && Boolean(prev.includeIncomplete) === includeIncomplete
    );

    let record = null;
    if (canReuse) {
      record = prev;
    } else {
      const info = await computePatternInfoForInterval(
        date,
        interval,
        includeIncomplete,
        patternConfigState.value,
        true
      );
      record = {
        windowId: key,
        marketSlug: String(interval.marketSlug || key),
        windowStartMs: interval.startMs,
        windowEndMs: interval.endMs,
        isComplete: Boolean(interval.isComplete),
        includeIncomplete,
        sourceSignature,
        patternSetVersion: patternConfigState.patternSetVersion,
        paramsHash: patternConfigState.hash,
        computedAtMs: Date.now(),
        patterns: info.patterns,
        patternPrimary: info.patternPrimary,
        patternSideHits: info.patternSideHits
      };
      storeChanged = true;
    }

    nextWindows[key] = record;
    byWindowId.set(key, normalizePatternInfo(record));
  }

  if (!storeChanged) {
    const existingKeys = Object.keys(existingWindows).sort();
    const nextKeys = Object.keys(nextWindows).sort();
    if (existingKeys.length !== nextKeys.length) {
      storeChanged = true;
    } else {
      for (let i = 0; i < existingKeys.length; i += 1) {
        if (existingKeys[i] !== nextKeys[i]) {
          storeChanged = true;
          break;
        }
      }
    }
    if (
      !storeChanged
      && (
        existingStore?.date !== date
        || Boolean(existingStore?.includeIncomplete) !== includeIncomplete
        || existingStore?.patternSetVersion !== patternConfigState.patternSetVersion
        || existingStore?.paramsHash !== patternConfigState.hash
        || existingStore?.daySignature !== daySignature
      )
    ) {
      storeChanged = true;
    }
  }

  if (storeChanged) {
    const payload = {
      schemaVersion: PATTERN_STORE_SCHEMA_VERSION,
      date,
      includeIncomplete,
      patternSetVersion: patternConfigState.patternSetVersion,
      paramsHash: patternConfigState.hash,
      configPath: patternConfigState.sourcePath,
      daySignature,
      updatedAtMs: Date.now(),
      windows: nextWindows
    };
    writePatternStore(storePath, payload);
  }

  const value = {
    summary: buildPatternSummary(intervals, byWindowId, includeIncomplete),
    byWindowId
  };
  PATTERN_CACHE.set(cacheKey, value);
  evictPatternCacheIfNeeded();
  return value;
}

async function buildIntervalsWithPatterns(date, includeIncomplete = false) {
  const intervals = await buildIntervals(date, BUCKET_MS);
  const patternIndex = await buildPatternIndexForDate(date, intervals, includeIncomplete);
  const intervalsWithPatterns = intervals.map((interval) => {
    const key = getIntervalPatternKey(interval);
    const patternInfo = patternIndex.byWindowId.get(key) || emptyPatternInfo();
    return {
      ...interval,
      patterns: patternInfo.patterns,
      patternPrimary: patternInfo.patternPrimary,
      patternSideHits: patternInfo.patternSideHits
    };
  });
  return {
    date,
    intervals: intervalsWithPatterns,
    patternSummary: patternIndex.summary
  };
}

function dayRange(startMs, endMs) {
  const out = [];
  const startDay = new Date(Date.UTC(
    new Date(startMs).getUTCFullYear(),
    new Date(startMs).getUTCMonth(),
    new Date(startMs).getUTCDate()
  )).getTime();
  const endDay = new Date(Date.UTC(
    new Date(endMs).getUTCFullYear(),
    new Date(endMs).getUTCMonth(),
    new Date(endMs).getUTCDate()
  )).getTime();
  for (let t = startDay; t <= endDay; t += 24 * 60 * 60 * 1000) {
    out.push(datePartUtc(t));
  }
  return out;
}

async function buildSeries(startMs, endMs, marketSlug = "", windowId = "", dateHint = "") {
  const dateFromStart = isDateDirName(dateHint) ? dateHint : datePartUtc(startMs);
  const partitioned = hasPartitionedWindowDirs(dateFromStart);
  if (partitioned) {
    const resolvedWindowId = String(windowId || marketSlug || "");
    return buildSeriesFromWindowDir(dateFromStart, resolvedWindowId, startMs, endMs);
  }
  return buildSeriesLegacy(startMs, endMs, marketSlug);
}

async function buildSeriesFromWindowDir(date, windowId, startMs, endMs) {
  const { updownPath, btcPath, ptbPath } = windowFilesForDate(date, windowId);
  const btc = [];
  const ptb = [];
  const up = [];
  const down = [];

  await eachJsonl(btcPath, (row) => {
    const ts = toTimeMs(row);
    const price = isFiniteNumber(row?.price);
    if (ts === null || price === null) return;
    if (ts < startMs || ts > endMs) return;
    btc.push([ts, price]);
  });

  await eachJsonl(updownPath, (row) => {
    const ts = toUpdownPointTimeMs(row);
    const side = String(row?.side || "").toLowerCase();
    const odds = isFiniteNumber(row?.mid) ?? isFiniteNumber(row?.last_trade_price);
    const slug = String(row?.market_slug || "");
    const sampleCount = Math.max(1, Number(isFiniteNumber(row?.sample_count) ?? 1));
    const eventType = String(row?.event_type || "");
    if (ts === null || odds === null) return;
    if (ts < startMs || ts > endMs) return;
    const point = [ts, odds, slug || windowId, sampleCount, eventType];
    if (side === "up") up.push(point);
    if (side === "down") down.push(point);
  });

  await eachJsonl(ptbPath, (row) => {
    const ts = isFiniteNumber(row?.tick_ts_ms) ??
      isFiniteNumber(row?.boundary_ms) ??
      isFiniteNumber(row?.window_start_ms) ??
      isFiniteNumber(row?.receive_time_ms);
    const price = isFiniteNumber(row?.ptb_price);
    if (ts === null || price === null) return;
    if (ts < startMs || ts > endMs) return;
    ptb.push([
      ts,
      price,
      String(row?.ptb_method || ""),
      isFiniteNumber(row?.window_start_ms),
      isFiniteNumber(row?.window_end_ms)
    ]);
  });

  btc.sort((a, b) => a[0] - b[0]);
  ptb.sort((a, b) => a[0] - b[0]);
  up.sort((a, b) => a[0] - b[0]);
  down.sort((a, b) => a[0] - b[0]);
  return { btc, ptb, up, down };
}

async function buildSeriesLegacy(startMs, endMs, marketSlug = "") {
  const btc = [];
  const ptb = [];
  const up = [];
  const down = [];

  for (const date of dayRange(startMs, endMs)) {
    const dayDir = path.join(LOG_ROOT, date);
    const btcPath = path.join(dayDir, "btc_reference.jsonl");
    const updownPath = path.join(dayDir, "updown_state.jsonl");
    const ptbPath = path.join(dayDir, "ptb_reference.jsonl");

    await eachJsonl(btcPath, (row) => {
      const ts = toTimeMs(row);
      const price = isFiniteNumber(row?.price);
      if (ts === null || price === null) return;
      if (ts < startMs || ts > endMs) return;
      btc.push([ts, price]);
    });

    await eachJsonl(updownPath, (row) => {
      const ts = toUpdownPointTimeMs(row);
      const side = String(row?.side || "").toLowerCase();
      const odds = isFiniteNumber(row?.mid) ?? isFiniteNumber(row?.last_trade_price);
      const slug = String(row?.market_slug || "");
      const sampleCount = Math.max(1, Number(isFiniteNumber(row?.sample_count) ?? 1));
      const eventType = String(row?.event_type || "");
      if (ts === null || odds === null) return;
      if (ts < startMs || ts > endMs) return;
      if (marketSlug && slug !== marketSlug) return;
      const point = [ts, odds, slug, sampleCount, eventType];
      if (side === "up") up.push(point);
      if (side === "down") down.push(point);
    });

    await eachJsonl(ptbPath, (row) => {
      const ts = isFiniteNumber(row?.tick_ts_ms) ??
        isFiniteNumber(row?.boundary_ms) ??
        isFiniteNumber(row?.window_start_ms) ??
        isFiniteNumber(row?.receive_time_ms);
      const price = isFiniteNumber(row?.ptb_price);
      const slug = String(row?.market_slug || "");
      if (ts === null || price === null) return;
      if (ts < startMs || ts > endMs) return;
      if (marketSlug && slug && slug !== marketSlug) return;
      ptb.push([
        ts,
        price,
        String(row?.ptb_method || ""),
        isFiniteNumber(row?.window_start_ms),
        isFiniteNumber(row?.window_end_ms)
      ]);
    });
  }

  btc.sort((a, b) => a[0] - b[0]);
  ptb.sort((a, b) => a[0] - b[0]);
  up.sort((a, b) => a[0] - b[0]);
  down.sort((a, b) => a[0] - b[0]);

  return { btc, ptb, up, down };
}

function writeJson(res, code, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(code, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
    "content-length": Buffer.byteLength(body)
  });
  res.end(body);
}

function mimeType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === ".html") return "text/html; charset=utf-8";
  if (ext === ".css") return "text/css; charset=utf-8";
  if (ext === ".js") return "text/javascript; charset=utf-8";
  if (ext === ".json") return "application/json; charset=utf-8";
  if (ext === ".svg") return "image/svg+xml";
  return "application/octet-stream";
}

function resolveStaticPath(urlPathname) {
  const normalized = urlPathname === "/" || urlPathname === "/replay"
    ? "/replay.html"
    : urlPathname;
  const unsafe = path.normalize(normalized).replace(/^(\.\.[/\\])+/, "");
  const fullPath = path.resolve(DOC_ROOT, `.${unsafe}`);
  if (!fullPath.startsWith(DOC_ROOT)) return null;
  return fullPath;
}

function createReplayServer() {
  return http.createServer(async (req, res) => {
    const reqUrl = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);

    try {
      if (reqUrl.pathname === "/api/dates") {
        const dates = listDateDirs().sort((a, b) => b.localeCompare(a));
        writeJson(res, 200, { dates });
        return;
      }

      if (reqUrl.pathname === "/api/intervals") {
        const requestedDate = String(reqUrl.searchParams.get("date") || "");
        const includeIncomplete = toBoolParam(reqUrl.searchParams.get("includeIncomplete"), false);
        const dates = listDateDirs();
        const date = isDateDirName(requestedDate) ? requestedDate : dates.at(-1);
        if (!date) {
          writeJson(res, 200, {
            date: null,
            intervals: [],
            patternSummary: buildPatternSummary([], new Map(), includeIncomplete)
          });
          return;
        }
        const payload = await buildIntervalsWithPatterns(date, includeIncomplete);
        writeJson(res, 200, payload);
        return;
      }

      if (reqUrl.pathname === "/api/series") {
        const startMs = isFiniteNumber(reqUrl.searchParams.get("startMs"));
        const endMs = isFiniteNumber(reqUrl.searchParams.get("endMs"));
        const marketSlug = String(reqUrl.searchParams.get("marketSlug") || "");
        const windowId = String(reqUrl.searchParams.get("windowId") || "");
        const date = String(reqUrl.searchParams.get("date") || "");
        if (startMs === null || endMs === null || endMs <= startMs) {
          writeJson(res, 400, { error: "invalid startMs/endMs" });
          return;
        }
        const series = await buildSeries(startMs, endMs, marketSlug, windowId, date);
        writeJson(res, 200, {
          startMs,
          endMs,
          date: isDateDirName(date) ? date : null,
          windowId: windowId || null,
          marketSlug: marketSlug || null,
          ...series
        });
        return;
      }

      const filePath = resolveStaticPath(reqUrl.pathname);
      if (!filePath || !fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) {
        writeJson(res, 404, { error: "not found" });
        return;
      }

      const body = fs.readFileSync(filePath);
      res.writeHead(200, {
        "content-type": mimeType(filePath),
        "cache-control": "no-store",
        "content-length": body.length
      });
      res.end(body);
    } catch (err) {
      writeJson(res, 500, { error: err?.message || String(err) });
    }
  });
}

function startReplayServer() {
  const server = createReplayServer();
  server.listen(PORT, HOST, () => {
    // eslint-disable-next-line no-console
    console.log(`[replay] listening on http://${HOST}:${PORT}`);
    // eslint-disable-next-line no-console
    console.log(`[replay] log root: ${LOG_ROOT}`);
  });
  return server;
}

const thisFilePath = fileURLToPath(import.meta.url);
const isEntrypoint = process.argv[1] && path.resolve(process.argv[1]) === thisFilePath;

if (isEntrypoint) {
  startReplayServer();
}

export {
  LOG_ROOT,
  DOC_ROOT,
  BUCKET_MS,
  MARKET_WINDOW_MS,
  COMPLETE_MIN_MS,
  listDateDirs,
  buildIntervals,
  buildIntervalsWithPatterns,
  buildSeries,
  createReplayServer,
  startReplayServer
};
