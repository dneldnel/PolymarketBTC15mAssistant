import fs from "node:fs";
import path from "node:path";
import http from "node:http";
import readline from "node:readline";
import { URL, fileURLToPath } from "node:url";

const PORT = Math.max(1, Number(process.env.REPLAY_PORT || 8787));
const HOST = process.env.REPLAY_HOST || "0.0.0.0";
const LOG_ROOT = path.resolve(process.cwd(), process.env.COLLECTOR_OUTPUT_DIR || "./logs/raw");
const DOC_ROOT = path.resolve(process.cwd(), "./docs");
const BUCKET_MS = 5 * 60 * 1000;
const MARKET_WINDOW_MS = 5 * 60 * 1000;
const COMPLETE_MIN_MS = 4 * 60 * 1000;

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
  const date = `${start.getFullYear()}-${String(start.getMonth() + 1).padStart(2, "0")}-${String(start.getDate()).padStart(2, "0")}`;
  const hh1 = String(start.getHours()).padStart(2, "0");
  const mm1 = String(start.getMinutes()).padStart(2, "0");
  const hh2 = String(end.getHours()).padStart(2, "0");
  const mm2 = String(end.getMinutes()).padStart(2, "0");
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

    if (!(btcPoints > 0 && (upPoints > 0 || downPoints > 0))) continue;

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
        btcCoverageMs,
        upCoverageMs,
        downCoverageMs,
        oddsCoverageMs,
        isComplete,
        label: formatIntervalLabel(x.startMs, x.endMs)
      };
    });
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
        const dates = listDateDirs();
        const date = isDateDirName(requestedDate) ? requestedDate : dates.at(-1);
        if (!date) {
          writeJson(res, 200, { date: null, intervals: [] });
          return;
        }
        const intervals = await buildIntervals(date, BUCKET_MS);
        writeJson(res, 200, { date, intervals });
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
  buildSeries,
  createReplayServer,
  startReplayServer
};
