import WebSocket from "ws";
import { CONFIG } from "./src/config.js";
import { applyGlobalProxyFromEnv, wsAgentForUrl } from "./src/net/proxy.js";

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

function epochToMs(x) {
  const n = toFiniteNumber(x);
  if (n === null) return null;
  // Heuristic: seconds are ~1e9; milliseconds are ~1e12.
  if (n >= 50_000_000_000) return Math.floor(n);
  return Math.floor(n * 1000);
}

const BUCKET_MS = 5 * 60_000;
function floorToBucket(ms) {
  return Math.floor(ms / BUCKET_MS) * BUCKET_MS;
}

function nextBucketBoundary(ms) {
  return floorToBucket(ms) + BUCKET_MS;
}

function formatTimestampMs(ms) {
  if (!ms || !Number.isFinite(ms)) return "-";
  const d = new Date(ms);
  const y = d.getFullYear();
  const mo = String(d.getMonth() + 1).padStart(2, "0");
  const da = String(d.getDate()).padStart(2, "0");
  const h = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  const s = String(d.getSeconds()).padStart(2, "0");
  const ms3 = String(d.getMilliseconds()).padStart(3, "0");
  return `${y}-${mo}-${da} ${h}:${mi}:${s}.${ms3}`;
}

function formatPriceUsd(x) {
  const n = typeof x === "string" ? Number(x) : typeof x === "number" ? x : NaN;
  if (!Number.isFinite(n)) return "-";
  return n.toFixed(2);
}

applyGlobalProxyFromEnv();

const args = process.argv.slice(2);
const wsUrl = args[0] || CONFIG.polymarket.liveDataWsUrl;
const symbolIncludes = String(args[1] || "btc").toLowerCase();

if (!wsUrl) {
  console.error("Missing wsUrl. Set POLYMARKET_LIVE_WS_URL or pass it as the first arg.");
  process.exitCode = 1;
} else {
  console.log(`Connecting: ${wsUrl}`);
  console.log(`Topic: crypto_prices_chainlink (symbol includes: "${symbolIncludes || "*"}")`);
  console.log("Press Ctrl+C to stop.\n");

  let ws = null;
  let closed = false;
  let reconnectMs = 500;
  let lastPrintedAt = 0;
  let reconnectTimer = null;

  let prevTick = null; // { tsMs, price, symbol }
  let nextBucketMs = null;

  const connect = () => {
    if (closed) return;

    ws = new WebSocket(wsUrl, {
      handshakeTimeout: 10_000,
      agent: wsAgentForUrl(wsUrl)
    });

    const scheduleReconnect = (reason = "reconnect") => {
      if (closed) return;
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
      const wait = reconnectMs;
      reconnectMs = Math.min(10_000, Math.floor(reconnectMs * 1.5));
      const now = Date.now();
      if (now - lastPrintedAt > 500) {
        lastPrintedAt = now;
        console.log(`[${formatTimestampMs(now)}] ${reason} in ${wait}ms...`);
      }
      reconnectTimer = setTimeout(connect, wait);
      reconnectTimer?.unref?.();
    };

    ws.on("open", () => {
      reconnectMs = 500;
      try {
        ws.send(
          JSON.stringify({
            action: "subscribe",
            subscriptions: [{ topic: "crypto_prices_chainlink", type: "*", filters: "" }]
          })
        );
      } catch {
        scheduleReconnect("subscribe failed");
      }
    });

    ws.on("message", (buf) => {
      const receivedAtMs = Date.now();
      const msg = typeof buf === "string" ? buf : buf?.toString?.() ?? "";
      if (!msg || !msg.trim()) return;

      const data = safeJsonParse(msg);
      if (!data || data.topic !== "crypto_prices_chainlink") return;

      const payload = typeof data.payload === "string" ? safeJsonParse(data.payload) : data.payload;
      if (!payload || typeof payload !== "object") return;

      const symbol = String(payload.symbol || payload.pair || payload.ticker || "").toLowerCase();
      if (symbolIncludes && !symbol.includes(symbolIncludes)) return;

      const price = toFiniteNumber(payload.value ?? payload.price ?? payload.current ?? payload.data);
      if (price === null) return;

      const updatedAt = epochToMs(payload.updatedAt ?? null);
      const timestamp = epochToMs(payload.timestamp ?? null);

      const wsLineParts = [
        `receivedAt: ${formatTimestampMs(receivedAtMs)}`,
        `updatedAt: ${formatTimestampMs(updatedAt)}`
      ];
      if (timestamp !== null && (updatedAt === null || updatedAt !== timestamp)) {
        wsLineParts.push(`timestamp: ${formatTimestampMs(timestamp)}`);
      }
      wsLineParts.push(`btc/usd: ${formatPriceUsd(price)}`);

      console.log(
        wsLineParts.join(" ")
      );

      if (timestamp === null) return;

      const tick = {
        tsMs: timestamp,
        price,
        symbol: symbol || null
      };

      if (!prevTick) {
        prevTick = tick;
        nextBucketMs = nextBucketBoundary(tick.tsMs);
        return;
      }

      if (tick.tsMs < prevTick.tsMs) {
        // Out-of-order tick: keep raw print for debugging, but don't use for minute aggregation.
        return;
      }

      if (nextBucketMs === null) nextBucketMs = nextBucketBoundary(prevTick.tsMs);

      while (nextBucketMs <= tick.tsMs) {
        let method = null;
        let bucketPrice = null;
        let before = null;
        let after = null;

        if (tick.tsMs === nextBucketMs) {
          method = "exact";
          bucketPrice = tick.price;
          before = prevTick.tsMs < nextBucketMs ? prevTick : null;
          after = tick;
        } else if (prevTick.tsMs < nextBucketMs && tick.tsMs > nextBucketMs) {
          method = "avg";
          bucketPrice = (prevTick.price + tick.price) / 2;
          before = prevTick;
          after = tick;
        }

        if (bucketPrice !== null) {
          console.log(`${method} btc/usd: ${formatPriceUsd(bucketPrice)} dt: ${formatTimestampMs(nextBucketMs)}`);
        }

        nextBucketMs += BUCKET_MS;
      }

      prevTick = tick;
    });

    ws.on("close", () => scheduleReconnect("ws closed"));
    ws.on("error", () => scheduleReconnect("ws error"));
  };

  connect();

  const shutdown = (reason = "shutdown") => {
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
    console.log(`[${formatTimestampMs(Date.now())}] ${reason}`);
    process.exit(0);
  };

  process.once("SIGINT", () => shutdown("SIGINT"));
  process.once("SIGTERM", () => shutdown("SIGTERM"));
}
