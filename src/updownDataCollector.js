import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import WebSocket from "ws";
import { CONFIG } from "./config.js";
import {
  fetchMarketBySlug,
  fetchLiveEventsBySeriesId,
  flattenEventMarkets,
  pickLatestLiveMarket
} from "./data/polymarket.js";
import { applyGlobalProxyFromEnv, wsAgentForUrl } from "./net/proxy.js";

function safeJsonParse(s) {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

function toFiniteNumber(x) {
  const n = typeof x === "string" ? Number(x) : typeof x === "number" ? x : NaN;
  return Number.isFinite(n) ? n : null;
}

function formatErr(err) {
  if (!err) return "unknown";
  if (err instanceof Error) return err.stack || err.message || String(err);
  return String(err);
}

function formatCloseReason(reason) {
  if (!reason) return "";
  if (typeof reason === "string") return reason;
  if (Buffer.isBuffer(reason)) return reason.toString("utf8");
  return String(reason);
}

function firstString(...values) {
  for (const v of values) {
    if (typeof v === "string" && v.trim()) return v.trim();
  }
  return null;
}

function normalizePayload(payload) {
  if (!payload) return null;
  if (typeof payload === "object") return payload;
  if (typeof payload === "string") return safeJsonParse(payload);
  return null;
}

function isLikelyBtcSymbol(raw) {
  if (!raw) return true;
  const s = String(raw).toLowerCase();
  return s.includes("btc") || s.includes("xbt");
}

function epochToMs(x) {
  if (x === null || x === undefined) return null;
  if (x instanceof Date) {
    const t = x.getTime();
    return Number.isFinite(t) ? t : null;
  }
  if (typeof x === "string") {
    const trimmed = x.trim();
    if (!trimmed) return null;
    const asNum = Number(trimmed);
    if (Number.isFinite(asNum)) return epochToMs(asNum);
    const t = new Date(trimmed).getTime();
    return Number.isFinite(t) ? t : null;
  }
  if (typeof x === "number") {
    if (!Number.isFinite(x)) return null;
    if (x >= 50_000_000_000) return Math.floor(x);
    return Math.floor(x * 1000);
  }
  return null;
}

function utcDatePart(ms = Date.now()) {
  const d = new Date(ms);
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, "0");
  const day = String(d.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

const TRACKED_FILE_NAMES = [
  "clob_market_ws.jsonl",
  "updown_state.jsonl",
  "btc_reference.jsonl",
  "market_lifecycle.jsonl",
  "collector_heartbeat.jsonl"
];

const lineCountByFilePath = new Map();

function toBoolEnv(value, fallback = false) {
  if (value === undefined || value === null) return fallback;
  const v = String(value).trim().toLowerCase();
  if (!v) return fallback;
  if (["1", "true", "yes", "y", "on"].includes(v)) return true;
  if (["0", "false", "no", "n", "off"].includes(v)) return false;
  return fallback;
}

function countLinesInText(s) {
  if (!s) return 0;
  let count = 0;
  for (let i = 0; i < s.length; i += 1) {
    if (s[i] === "\n") count += 1;
  }
  return count;
}

function ensureLineCountInitialized(filePath) {
  if (lineCountByFilePath.has(filePath)) return;
  if (!fs.existsSync(filePath)) {
    lineCountByFilePath.set(filePath, 0);
    return;
  }
  const content = fs.readFileSync(filePath, "utf8");
  lineCountByFilePath.set(filePath, countLinesInText(content));
}

function appendJsonl(filePath, row) {
  ensureDir(path.dirname(filePath));
  ensureLineCountInitialized(filePath);
  fs.appendFileSync(filePath, `${JSON.stringify(row)}\n`, "utf8");
  lineCountByFilePath.set(filePath, (lineCountByFilePath.get(filePath) || 0) + 1);
}

function writeDailyJsonl(baseDir, fileName, row, atMs = Date.now()) {
  const day = utcDatePart(atMs);
  const filePath = path.join(baseDir, day, fileName);
  appendJsonl(filePath, row);
  return filePath;
}

function normalizeDirName(x) {
  const raw = String(x ?? "").trim();
  if (!raw) return "";
  return raw.replace(/[\\/:*?"<>|]+/g, "-").replace(/\s+/g, "-").slice(0, 160);
}

function padRight(s, width) {
  const text = String(s ?? "");
  if (text.length >= width) return text;
  return text + " ".repeat(width - text.length);
}

function renderScreen(text) {
  try {
    readline.cursorTo(process.stdout, 0, 0);
    readline.clearScreenDown(process.stdout);
    process.stdout.write(text);
  } catch {
    // ignore
  }
}

function buildStatusScreen({
  outputRoot,
  outputWindowDir,
  windowId,
  activeMarket,
  heartbeatMs,
  marketRefreshMs,
  aggregateMs,
  saveClobRaw,
  lastPolymarketPrice,
  liveWsConnected,
  liveWsMode,
  liveWsLastError,
  pendingAggregateBuckets
}) {
  const now = Date.now();
  const lines = [];

  lines.push("Polymarket Up/Down Data Collector");
  lines.push("------------------------------------------------------------");
  lines.push(`utc_now:            ${new Date(now).toISOString()}`);
  lines.push(`output_root:        ${outputRoot}`);
  lines.push(`window_id:          ${windowId || "-"}`);
  lines.push(`output_window_dir:  ${outputWindowDir}`);
  lines.push(`market_refresh_ms:  ${marketRefreshMs}`);
  lines.push(`heartbeat_ms:       ${heartbeatMs}`);
  lines.push(`aggregate_ms:       ${aggregateMs}`);
  lines.push(`save_clob_raw:      ${saveClobRaw}`);
  lines.push(`pending_buckets:    ${pendingAggregateBuckets}`);
  lines.push(`live_ws_status:     ${liveWsConnected ? "CONNECTED" : "RECONNECTING"}`);
  lines.push(`live_ws_mode:       ${liveWsMode || "-"}`);
  lines.push(`live_ws_error:      ${liveWsLastError || "-"}`);
  lines.push(`market_slug:        ${activeMarket.slug || "-"}`);
  lines.push(`market_id:          ${activeMarket.id || "-"}`);
  lines.push(`up_token_id:        ${activeMarket.upTokenId || "-"}`);
  lines.push(`down_token_id:      ${activeMarket.downTokenId || "-"}`);
  lines.push(`btc_polymarket:     ${lastPolymarketPrice === null ? "-" : String(lastPolymarketPrice)}`);
  lines.push("");
  lines.push("jsonl record counts");
  lines.push("------------------------------------------------------------");

  for (const fileName of TRACKED_FILE_NAMES) {
    const filePath = path.join(outputWindowDir, fileName);
    ensureLineCountInitialized(filePath);
    const count = lineCountByFilePath.get(filePath) || 0;
    lines.push(`${padRight(fileName, 26)} ${count}`);
  }

  lines.push("");
  lines.push("refresh_interval: 1000ms");
  lines.push("press Ctrl+C to stop");
  return `${lines.join("\n")}\n`;
}

function startPolymarketBtcRtdsStream({
  wsUrl = CONFIG.polymarket.liveDataWsUrl,
  symbolIncludes = (process.env.POLYMARKET_LIVE_SYMBOL || "btc").toLowerCase(),
  onUpdate,
  onStatus
} = {}) {
  let ws = null;
  let closed = false;
  let reconnectMs = 500;
  let reconnectTimer = null;
  let useProxy = true;

  let lastPrice = null;
  let lastUpdatedAt = null;
  let connected = false;
  let lastError = "";

  const setStatus = (patch = {}) => {
    connected = patch.connected ?? connected;
    lastError = patch.lastError ?? lastError;
    const mode = patch.mode ?? (useProxy ? "proxy" : "direct");
    if (typeof onStatus === "function") {
      onStatus({ connected, mode, lastError });
    }
  };

  const scheduleReconnect = (mode, reason = "") => {
    if (closed) return;
    if (reconnectTimer) return;

    setStatus({ connected: false, lastError: reason, mode });
    try {
      ws?.terminate();
    } catch {
      // ignore
    }
    ws = null;
    const wait = reconnectMs;
    reconnectMs = Math.min(10_000, Math.floor(reconnectMs * 1.5));
    useProxy = !useProxy;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connect();
    }, wait);
  };

  const connect = () => {
    if (closed) return;

    const mode = useProxy ? "proxy" : "direct";
    const agent = useProxy ? wsAgentForUrl(wsUrl) : undefined;
    const socket = new WebSocket(wsUrl, {
      handshakeTimeout: 10_000,
      ...(agent ? { agent } : {})
    });
    ws = socket;
    setStatus({ connected: false, mode });

    socket.on("open", () => {
      if (closed || ws !== socket) return;
      reconnectMs = 500;
      setStatus({ connected: true, lastError: "", mode });
      try {
        socket.send(
          JSON.stringify({
            action: "subscribe",
            subscriptions: [{ topic: "crypto_prices_chainlink", type: "*", filters: "" }]
          })
        );
      } catch (err) {
        scheduleReconnect(mode, `subscribe_failed: ${err?.message ?? String(err)}`);
      }
    });

    socket.on("message", (buf) => {
      const msg = typeof buf === "string" ? buf : buf?.toString?.() ?? "";
      if (!msg || !msg.trim()) return;

      const data = safeJsonParse(msg);
      if (!data || data.topic !== "crypto_prices_chainlink") return;

      const payload = normalizePayload(data.payload) || {};
      const symbolRaw = String(payload.symbol || payload.pair || payload.ticker || "").trim();
      const symbolLower = symbolRaw.toLowerCase();

      if (symbolIncludes) {
        const accepted = symbolLower
          ? symbolLower.includes(symbolIncludes) || isLikelyBtcSymbol(symbolLower)
          : true;
        if (!accepted) return;
      }

      const price = toFiniteNumber(payload.value ?? payload.price ?? payload.current ?? payload.data);
      if (price === null) return;

      const updatedAtMs = epochToMs(payload.timestamp ?? payload.updatedAt ?? data.timestamp ?? Date.now());
      lastPrice = price;
      lastUpdatedAt = updatedAtMs ?? Date.now();

      if (typeof onUpdate === "function") {
        onUpdate({
          price: lastPrice,
          updatedAt: lastUpdatedAt,
          symbol: symbolRaw || "BTC/USD",
          source: "polymarket_ws"
        });
      }
    });

    socket.on("close", (code, reason) => {
      if (ws !== socket) return;
      const reasonText = formatCloseReason(reason);
      scheduleReconnect(mode, `close_${code}${reasonText ? `:${reasonText}` : ""}`);
    });
    socket.on("error", (err) => {
      if (ws !== socket) return;
      scheduleReconnect(mode, `error: ${err?.message ?? String(err)}`);
    });
  };

  connect();

  return {
    getLast() {
      return { price: lastPrice, updatedAt: lastUpdatedAt, source: "polymarket_ws", connected, lastError };
    },
    close() {
      closed = true;
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      try {
        ws?.terminate();
      } catch {
        // ignore
      }
      ws = null;
      setStatus({ connected: false, lastError: "closed" });
    }
  };
}

function parseOutcomes(market) {
  const outcomes = Array.isArray(market?.outcomes)
    ? market.outcomes
    : (typeof market?.outcomes === "string" ? safeJsonParse(market.outcomes) : []);
  const clobTokenIds = Array.isArray(market?.clobTokenIds)
    ? market.clobTokenIds
    : (typeof market?.clobTokenIds === "string" ? safeJsonParse(market.clobTokenIds) : []);

  let upTokenId = null;
  let downTokenId = null;
  for (let i = 0; i < outcomes.length; i += 1) {
    const label = String(outcomes[i] ?? "");
    const tokenId = clobTokenIds[i] ? String(clobTokenIds[i]) : null;
    if (!tokenId) continue;
    if (label.toLowerCase() === CONFIG.polymarket.upOutcomeLabel.toLowerCase()) upTokenId = tokenId;
    if (label.toLowerCase() === CONFIG.polymarket.downOutcomeLabel.toLowerCase()) downTokenId = tokenId;
  }

  return { upTokenId, downTokenId };
}

async function resolveCurrentBtc15mMarket() {
  if (CONFIG.polymarket.marketSlug) {
    const market = await fetchMarketBySlug(CONFIG.polymarket.marketSlug);
    if (!market) return null;
    const tokens = parseOutcomes(market);
    return { market, ...tokens };
  }

  const events = await fetchLiveEventsBySeriesId({
    seriesId: CONFIG.polymarket.seriesId,
    limit: 25
  });
  const markets = flattenEventMarkets(events);
  const market = pickLatestLiveMarket(markets);
  if (!market) return null;
  const tokens = parseOutcomes(market);
  return { market, ...tokens };
}

function bestPriceFromLevels(levels, side) {
  const list = Array.isArray(levels) ? levels : [];
  let best = null;
  for (const level of list) {
    let p = null;
    if (Array.isArray(level)) p = toFiniteNumber(level[0]);
    else p = toFiniteNumber(level?.price ?? level?.p);
    if (p === null) continue;
    if (best === null) best = p;
    else best = side === "bid" ? Math.max(best, p) : Math.min(best, p);
  }
  return best;
}

function getEventType(msg) {
  return String(msg?.event_type ?? msg?.eventType ?? msg?.type ?? msg?.channel ?? msg?.event ?? "unknown");
}

function extractPayloadCandidates(msg) {
  const out = [];
  if (Array.isArray(msg?.price_changes)) {
    for (const x of msg.price_changes) out.push({ payload: x, eventType: "price_change" });
  }
  if (Array.isArray(msg?.changes)) {
    for (const x of msg.changes) out.push({ payload: x, eventType: "price_change" });
  }
  if (msg?.price_change && typeof msg.price_change === "object") {
    out.push({ payload: msg.price_change, eventType: "price_change" });
  }
  if (msg?.book && typeof msg.book === "object") {
    out.push({ payload: msg.book, eventType: "book" });
  }
  if (msg?.data && typeof msg.data === "object") {
    out.push({ payload: msg.data, eventType: getEventType(msg) });
  }
  if (out.length === 0) out.push({ payload: msg, eventType: getEventType(msg) });
  return out;
}

function isSameAssetSet(a, b) {
  if (a.size !== b.size) return false;
  for (const x of a) {
    if (!b.has(x)) return false;
  }
  return true;
}

function stateChanged(prev, next) {
  if (!prev) return true;
  return prev.bestBid !== next.bestBid ||
    prev.bestAsk !== next.bestAsk ||
    prev.mid !== next.mid ||
    prev.spread !== next.spread ||
    prev.lastTradePrice !== next.lastTradePrice ||
    prev.updatedAtMs !== next.updatedAtMs;
}

function buildStateFromPayload(prev, payload, parent, receiveAtMs) {
  const merged = { ...(prev || {}) };
  const p = payload || {};
  const m = parent || {};

  const assetId = firstString(
    p.asset_id,
    p.assetId,
    p.token_id,
    p.tokenId,
    m.asset_id,
    m.assetId,
    m.token_id,
    m.tokenId
  );
  if (!assetId) return null;

  const bids = p.bids ?? p.buy ?? m.bids ?? m.buy ?? null;
  const asks = p.asks ?? p.sell ?? m.asks ?? m.sell ?? null;

  const bestBid = toFiniteNumber(p.best_bid ?? p.bestBid ?? p.bid ?? p.bbo_bid ?? p.bboBid) ?? bestPriceFromLevels(bids, "bid");
  const bestAsk = toFiniteNumber(p.best_ask ?? p.bestAsk ?? p.ask ?? p.bbo_ask ?? p.bboAsk) ?? bestPriceFromLevels(asks, "ask");
  const lastTradePrice = toFiniteNumber(
    p.last_trade_price ??
      p.lastTradePrice ??
      p.trade_price ??
      p.tradePrice ??
      p.price ??
      m.last_trade_price ??
      m.lastTradePrice
  );

  if (bestBid !== null) merged.bestBid = bestBid;
  if (bestAsk !== null) merged.bestAsk = bestAsk;
  if (lastTradePrice !== null) merged.lastTradePrice = lastTradePrice;

  if (Number.isFinite(merged.bestBid) && Number.isFinite(merged.bestAsk)) {
    merged.mid = (merged.bestBid + merged.bestAsk) / 2;
    merged.spread = merged.bestAsk - merged.bestBid;
  } else {
    merged.mid = null;
    merged.spread = null;
  }

  const eventAtMs = epochToMs(
    p.timestamp ??
      p.ts ??
      p.updated_at ??
      p.updatedAt ??
      m.timestamp ??
      m.ts ??
      m.updated_at ??
      m.updatedAt
  );
  merged.updatedAtMs = eventAtMs ?? receiveAtMs;

  return { assetId, state: merged, eventAtMs: merged.updatedAtMs };
}

function startPolymarketClobMarketStream({
  wsUrl = process.env.POLYMARKET_CLOB_WS_URL || "wss://ws-subscriptions-clob.polymarket.com/ws/market",
  onRawMessage,
  onUpdate
} = {}) {
  let ws = null;
  let closed = false;
  let reconnectMs = 500;
  let reconnectTimer = null;
  let subscribedAssets = new Set();

  const stateByAssetId = new Map();

  const sendSubscription = () => {
    if (!ws || ws.readyState !== WebSocket.OPEN || subscribedAssets.size === 0) return;
    ws.send(
      JSON.stringify({
        type: "market",
        assets_ids: Array.from(subscribedAssets)
      })
    );
  };

  const processMessageObject = (data, receiveAtMs) => {
    if (!data || typeof data !== "object") return;

    const candidates = extractPayloadCandidates(data);
    for (const { payload, eventType } of candidates) {
      const prevKey = firstString(
        payload?.asset_id,
        payload?.assetId,
        payload?.token_id,
        payload?.tokenId,
        data?.asset_id,
        data?.assetId,
        data?.token_id,
        data?.tokenId
      );
      const parsed = buildStateFromPayload(prevKey ? stateByAssetId.get(prevKey) : null, payload, data, receiveAtMs);
      if (!parsed) continue;
      if (subscribedAssets.size > 0 && !subscribedAssets.has(parsed.assetId)) continue;

      const prev = stateByAssetId.get(parsed.assetId) || null;
      if (!stateChanged(prev, parsed.state)) continue;

      stateByAssetId.set(parsed.assetId, parsed.state);

      if (typeof onUpdate === "function") {
        onUpdate({
          eventType,
          assetId: parsed.assetId,
          state: { ...parsed.state },
          eventAtMs: parsed.eventAtMs,
          receiveAtMs
        });
      }
    }
  };

  const scheduleReconnect = () => {
    if (closed) return;
    if (reconnectTimer) return;
    try {
      ws?.terminate();
    } catch {
      // ignore
    }
    ws = null;
    const wait = reconnectMs;
    reconnectMs = Math.min(10_000, Math.floor(reconnectMs * 1.5));
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connect();
    }, wait);
  };

  const connect = () => {
    if (closed) return;

    const socket = new WebSocket(wsUrl, {
      handshakeTimeout: 10_000,
      agent: wsAgentForUrl(wsUrl)
    });
    ws = socket;

    socket.on("open", () => {
      if (closed || ws !== socket) return;
      reconnectMs = 500;
      try {
        sendSubscription();
      } catch {
        scheduleReconnect();
      }
    });

    socket.on("message", (buf) => {
      const receiveAtMs = Date.now();
      const text = typeof buf === "string" ? buf : buf?.toString?.() ?? "";
      if (!text || !text.trim()) return;

      const data = safeJsonParse(text);
      if (!data) return;

      if (typeof onRawMessage === "function") {
        onRawMessage({
          receiveAtMs,
          eventType: getEventType(data),
          message: data
        });
      }

      if (Array.isArray(data)) {
        for (const item of data) processMessageObject(item, receiveAtMs);
      } else {
        processMessageObject(data, receiveAtMs);
      }
    });

    socket.on("close", () => {
      if (ws !== socket) return;
      scheduleReconnect();
    });
    socket.on("error", () => {
      if (ws !== socket) return;
      scheduleReconnect();
    });
  };

  connect();

  return {
    subscribeAssets(assetIds, { resetState = false } = {}) {
      const next = new Set(
        (Array.isArray(assetIds) ? assetIds : [])
          .map((x) => String(x || "").trim())
          .filter(Boolean)
      );
      const changed = !isSameAssetSet(subscribedAssets, next);
      subscribedAssets = next;
      if (resetState) stateByAssetId.clear();
      if (changed) sendSubscription();
      return changed;
    },
    getLastByAssetId(assetId) {
      const key = String(assetId || "").trim();
      return key ? (stateByAssetId.get(key) || null) : null;
    },
    clearState() {
      stateByAssetId.clear();
    },
    close() {
      closed = true;
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      try {
        ws?.terminate();
      } catch {
        // ignore
      }
      ws = null;
    }
  };
}

async function main() {
  applyGlobalProxyFromEnv();

  const marketRefreshMs = Math.max(500, Number(process.env.COLLECTOR_MARKET_REFRESH_MS || 1000));
  const heartbeatMs = Math.max(1000, Number(process.env.COLLECTOR_HEARTBEAT_MS || 5000));
  const outputRoot = process.env.COLLECTOR_OUTPUT_DIR || "./logs/raw";
  const partitionBy = (process.env.COLLECTOR_PARTITION_BY || "market_slug").trim().toLowerCase();
  const unassignedDir = normalizeDirName(process.env.COLLECTOR_UNASSIGNED_DIR || "_unassigned") || "_unassigned";
  const screenRefreshMs = 1000;
  const flushIntervalMs = 100;
  const flushGraceMs = 50;
  const aggregateMsRaw = Number(process.env.COLLECTOR_AGGREGATE_MS ?? 500);
  const aggregateMs = Number.isFinite(aggregateMsRaw) && aggregateMsRaw > 0 ? Math.floor(aggregateMsRaw) : 0;
  const saveClobRaw = toBoolEnv(process.env.COLLECTOR_SAVE_CLOB_RAW, false);

  let activeMarket = {
    slug: null,
    id: null,
    upTokenId: null,
    downTokenId: null
  };
  let lastPolymarketPrice = null;
  let liveWsConnected = false;
  let liveWsMode = "proxy";
  let liveWsLastError = "";
  let shutdownStarted = false;
  const aggregateBuckets = new Map();

  const resolveWindowId = (fallbackMs = Date.now()) => {
    if (partitionBy === "market_slug") {
      const slug = normalizeDirName(activeMarket.slug);
      if (slug) return slug;
      return unassignedDir;
    }
    const d = new Date(fallbackMs);
    const h = String(d.getUTCHours()).padStart(2, "0");
    const m = String(Math.floor(d.getUTCMinutes() / 5) * 5).padStart(2, "0");
    return `${h}${m}`;
  };

  const resolveWindowDir = (atMs = Date.now(), explicitWindowId = null) => {
    const day = utcDatePart(atMs);
    const windowId = explicitWindowId || resolveWindowId(atMs);
    return path.join(outputRoot, day, windowId);
  };

  const writeWindowedJsonl = (fileName, row, atMs = Date.now(), explicitWindowId = null) => {
    const windowDir = resolveWindowDir(atMs, explicitWindowId);
    const filePath = path.join(windowDir, fileName);
    appendJsonl(filePath, row);
    return filePath;
  };

  const writeUpdownSnapshot = ({
    marketSlug,
    marketId,
    assetId,
    side,
    eventType,
    bestBid,
    bestAsk,
    mid,
    spread,
    lastTradePrice,
    bucketStartMs,
    bucketEndMs,
    sampleCount,
    lastEventTimeMs,
    receiveTimeMs
  }) => {
    const windowId = normalizeDirName(marketSlug) || unassignedDir;
    writeWindowedJsonl("updown_state.jsonl", {
      event_type: eventType,
      market_slug: marketSlug,
      market_id: marketId,
      asset_id: assetId,
      side,
      event_time_ms: lastEventTimeMs,
      receive_time_ms: receiveTimeMs,
      best_bid: bestBid,
      best_ask: bestAsk,
      mid,
      spread,
      last_trade_price: lastTradePrice,
      bucket_start_ms: bucketStartMs,
      bucket_end_ms: bucketEndMs,
      sample_count: sampleCount,
      last_event_time_ms: lastEventTimeMs
    }, receiveTimeMs, windowId);
  };

  const flushAggregation = (nowMs = Date.now(), forceAll = false) => {
    if (aggregateBuckets.size === 0) return 0;
    const keysToFlush = [];
    for (const [k, bucket] of aggregateBuckets) {
      if (forceAll || bucket.bucketEndMs <= nowMs - flushGraceMs) {
        keysToFlush.push(k);
      }
    }
    if (keysToFlush.length === 0) return 0;

    keysToFlush.sort((a, b) => {
      const ba = aggregateBuckets.get(a);
      const bb = aggregateBuckets.get(b);
      if (!ba || !bb) return 0;
      if (ba.bucketStartMs !== bb.bucketStartMs) return ba.bucketStartMs - bb.bucketStartMs;
      return String(ba.assetId).localeCompare(String(bb.assetId));
    });

    for (const k of keysToFlush) {
      const bucket = aggregateBuckets.get(k);
      if (!bucket) continue;
      writeUpdownSnapshot({
        marketSlug: bucket.marketSlug,
        marketId: bucket.marketId,
        assetId: bucket.assetId,
        side: bucket.side,
        eventType: bucket.eventType,
        bestBid: bucket.latestState.bestBid ?? null,
        bestAsk: bucket.latestState.bestAsk ?? null,
        mid: bucket.latestState.mid ?? null,
        spread: bucket.latestState.spread ?? null,
        lastTradePrice: bucket.latestState.lastTradePrice ?? null,
        bucketStartMs: bucket.bucketStartMs,
        bucketEndMs: bucket.bucketEndMs,
        sampleCount: bucket.sampleCount,
        lastEventTimeMs: bucket.lastEventTimeMs,
        receiveTimeMs: bucket.lastReceiveTimeMs
      });
      aggregateBuckets.delete(k);
    }
    return keysToFlush.length;
  };

  const persistState = (evt) => {
    const marketSlug = activeMarket.slug;
    const marketId = activeMarket.id;
    if (!marketSlug || !evt?.assetId) return;

    const side = evt.assetId === activeMarket.upTokenId
      ? "up"
      : evt.assetId === activeMarket.downTokenId
        ? "down"
        : "other";

    const eventAtMs = Number.isFinite(Number(evt.eventAtMs)) ? Number(evt.eventAtMs) : Number(evt.receiveAtMs);
    const receiveAtMs = Number.isFinite(Number(evt.receiveAtMs)) ? Number(evt.receiveAtMs) : Date.now();

    if (aggregateMs <= 0) {
      writeUpdownSnapshot({
        marketSlug,
        marketId,
        assetId: evt.assetId,
        side,
        eventType: evt.eventType,
        bestBid: evt.state.bestBid ?? null,
        bestAsk: evt.state.bestAsk ?? null,
        mid: evt.state.mid ?? null,
        spread: evt.state.spread ?? null,
        lastTradePrice: evt.state.lastTradePrice ?? null,
        bucketStartMs: eventAtMs,
        bucketEndMs: eventAtMs,
        sampleCount: 1,
        lastEventTimeMs: eventAtMs,
        receiveTimeMs: receiveAtMs
      });
      return;
    }

    const bucketStartMs = Math.floor(eventAtMs / aggregateMs) * aggregateMs;
    const bucketEndMs = bucketStartMs + aggregateMs;
    const key = `${marketSlug}|${evt.assetId}|${bucketStartMs}`;
    const prev = aggregateBuckets.get(key);

    if (!prev) {
      aggregateBuckets.set(key, {
        marketSlug,
        marketId,
        assetId: evt.assetId,
        side,
        eventType: evt.eventType,
        bucketStartMs,
        bucketEndMs,
        sampleCount: 1,
        lastEventTimeMs: eventAtMs,
        lastReceiveTimeMs: receiveAtMs,
        latestState: { ...evt.state }
      });
      return;
    }

    prev.marketId = marketId;
    prev.side = side;
    prev.eventType = evt.eventType;
    prev.sampleCount += 1;
    prev.lastEventTimeMs = eventAtMs;
    prev.lastReceiveTimeMs = receiveAtMs;
    prev.latestState = { ...evt.state };
  };

  const clobStream = startPolymarketClobMarketStream({
    wsUrl: process.env.POLYMARKET_CLOB_WS_URL || "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    onRawMessage(evt) {
      if (!saveClobRaw) return;
      writeWindowedJsonl("clob_market_ws.jsonl", {
        receive_time_ms: evt.receiveAtMs,
        event_type: evt.eventType,
        message: evt.message
      }, evt.receiveAtMs);
    },
    onUpdate: persistState
  });

  const polymarketPriceStream = startPolymarketBtcRtdsStream({
    onUpdate(tick) {
      const now = Date.now();
      lastPolymarketPrice = Number.isFinite(Number(tick.price)) ? Number(tick.price) : lastPolymarketPrice;
      writeWindowedJsonl("btc_reference.jsonl", {
        event_type: "btc_tick",
        source: "polymarket_ws",
        event_time_ms: tick.updatedAt ?? now,
        receive_time_ms: now,
        price: tick.price
      }, now);
    },
    onStatus(status) {
      liveWsConnected = Boolean(status?.connected);
      liveWsMode = status?.mode || liveWsMode;
      liveWsLastError = status?.lastError || "";
    }
  });

  let marketRefreshInFlight = false;

  const writeLifecycle = (name, extra = {}, windowIdOverride = null) => {
    const now = Date.now();
    writeWindowedJsonl("market_lifecycle.jsonl", {
      event_type: name,
      receive_time_ms: now,
      market_slug: activeMarket.slug,
      market_id: activeMarket.id,
      up_token_id: activeMarket.upTokenId,
      down_token_id: activeMarket.downTokenId,
      ...extra
    }, now, windowIdOverride);
  };

  const refreshMarket = async () => {
    if (marketRefreshInFlight) return;
    marketRefreshInFlight = true;
    try {
      const resolved = await resolveCurrentBtc15mMarket();
      if (!resolved?.market) return;

      const slug = String(resolved.market.slug ?? "");
      const id = String(resolved.market.id ?? "");
      const upTokenId = resolved.upTokenId ? String(resolved.upTokenId) : null;
      const downTokenId = resolved.downTokenId ? String(resolved.downTokenId) : null;

      const changed = slug !== activeMarket.slug || upTokenId !== activeMarket.upTokenId || downTokenId !== activeMarket.downTokenId;
      if (!changed) return;

      const prevMarket = { ...activeMarket };
      const prevWindowId = normalizeDirName(prevMarket.slug) || unassignedDir;
      flushAggregation(Date.now(), true);
      activeMarket = { slug, id, upTokenId, downTokenId };
      clobStream.clearState();
      clobStream.subscribeAssets([upTokenId, downTokenId], { resetState: true });

      writeLifecycle("market_switch", {
        prev_market_slug: prevMarket.slug,
        prev_market_id: prevMarket.id,
        prev_up_token_id: prevMarket.upTokenId,
        prev_down_token_id: prevMarket.downTokenId,
        next_market_slug: slug,
        next_market_id: id,
        next_up_token_id: upTokenId,
        next_down_token_id: downTokenId
      }, prevWindowId);
    } catch (err) {
      writeLifecycle("market_refresh_error", { error: err?.message ?? String(err) });
    } finally {
      marketRefreshInFlight = false;
    }
  };

  const syncSubscriptions = () => {
    const assets = [activeMarket.upTokenId, activeMarket.downTokenId].filter(Boolean);
    clobStream.subscribeAssets(assets);
  };

  const timerMarket = setInterval(async () => {
    await refreshMarket();
    syncSubscriptions();
  }, marketRefreshMs);

  const timerHeartbeat = setInterval(() => {
    const now = Date.now();
    writeWindowedJsonl("collector_heartbeat.jsonl", {
      event_type: "heartbeat",
      receive_time_ms: now,
      market_slug: activeMarket.slug,
      market_id: activeMarket.id,
      up_token_id: activeMarket.upTokenId,
      down_token_id: activeMarket.downTokenId,
      btc_polymarket_last: polymarketPriceStream.getLast()?.price ?? null
    }, now);
  }, heartbeatMs);

  const timerFlush = setInterval(() => {
    flushAggregation(Date.now(), false);
  }, flushIntervalMs);

  const timerScreen = setInterval(() => {
    if (!process.stdout?.isTTY) return;
    const now = Date.now();
    const windowId = resolveWindowId(now);
    const outputWindowDir = resolveWindowDir(now, windowId);
    const text = buildStatusScreen({
      outputRoot,
      outputWindowDir,
      windowId,
      activeMarket,
      heartbeatMs,
      marketRefreshMs,
      aggregateMs,
      saveClobRaw,
      lastPolymarketPrice,
      liveWsConnected,
      liveWsMode,
      liveWsLastError,
      pendingAggregateBuckets: aggregateBuckets.size
    });
    renderScreen(text);
  }, screenRefreshMs);

  await refreshMarket();
  syncSubscriptions();
  writeLifecycle("collector_started", { market_refresh_ms: marketRefreshMs, heartbeat_ms: heartbeatMs });
  if (process.stdout?.isTTY) {
    const now = Date.now();
    const windowId = resolveWindowId(now);
    const outputWindowDir = resolveWindowDir(now, windowId);
    renderScreen(
      buildStatusScreen({
        outputRoot,
        outputWindowDir,
        windowId,
        activeMarket,
        heartbeatMs,
        marketRefreshMs,
        aggregateMs,
        saveClobRaw,
        lastPolymarketPrice,
        liveWsConnected,
        liveWsMode,
        liveWsLastError,
        pendingAggregateBuckets: aggregateBuckets.size
      })
    );
  }

  const shutdown = (reason, exitCode = 0) => {
    if (shutdownStarted) return;
    shutdownStarted = true;
    clearInterval(timerMarket);
    clearInterval(timerHeartbeat);
    clearInterval(timerFlush);
    clearInterval(timerScreen);
    flushAggregation(Date.now(), true);
    writeLifecycle("collector_stopped", { reason, exit_code: exitCode });
    try {
      clobStream.close();
    } catch {
      // ignore
    }
    try {
      polymarketPriceStream.close();
    } catch {
      // ignore
    }
    process.exit(exitCode);
  };

  process.once("SIGINT", () => shutdown("SIGINT"));
  process.once("SIGTERM", () => shutdown("SIGTERM"));
  process.once("SIGHUP", () => shutdown("SIGHUP"));

  process.on("uncaughtException", (err) => {
    shutdown(`uncaughtException: ${formatErr(err)}`, 1);
  });

  process.on("unhandledRejection", (reason) => {
    shutdown(`unhandledRejection: ${formatErr(reason)}`, 1);
  });

  process.stdout?.on("error", (err) => {
    shutdown(`stdout_error: ${formatErr(err)}`, 1);
  });
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(err);
  process.exit(1);
});
