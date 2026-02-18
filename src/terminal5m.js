import WebSocket from "ws";
import readline from "node:readline";
import { CONFIG } from "./config.js";
import { clamp } from "./utils.js";
import { applyGlobalProxyFromEnv, wsAgentForUrl } from "./net/proxy.js";

function safeJsonParse(s) {
  try {
    return JSON.parse(s);
  } catch {
    return null;
  }
}

function normalizePayload(payload) {
  if (!payload) return null;
  if (typeof payload === "object") return payload;
  if (typeof payload === "string") return safeJsonParse(payload);
  return null;
}

function toFiniteNumber(x) {
  const n = typeof x === "string" ? Number(x) : typeof x === "number" ? x : NaN;
  return Number.isFinite(n) ? n : null;
}

function epochToMs(x) {
  const n = toFiniteNumber(x);
  if (n === null) return null;
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

function formatLocalTsMs(ms) {
  if (ms === null || ms === undefined || !Number.isFinite(ms)) return "-";
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

function formatLocalTsMin(ms) {
  if (ms === null || ms === undefined || !Number.isFinite(ms)) return "-";
  const d = new Date(ms);
  const y = d.getFullYear();
  const mo = String(d.getMonth() + 1).padStart(2, "0");
  const da = String(d.getDate()).padStart(2, "0");
  const h = String(d.getHours()).padStart(2, "0");
  const mi = String(d.getMinutes()).padStart(2, "0");
  return `${y}-${mo}-${da} ${h}:${mi}`;
}

function formatMmSs(ms) {
  if (ms === null || ms === undefined || !Number.isFinite(ms)) return "--:--";
  const totalSeconds = Math.max(0, Math.floor(ms / 1000));
  const m = Math.floor(totalSeconds / 60);
  const s = totalSeconds % 60;
  return `${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

function formatUsd(x, decimals = 2) {
  if (x === null || x === undefined || !Number.isFinite(Number(x))) return "-";
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals
  }).format(Number(x));
}

function formatSignedUsd(x, decimals = 2) {
  if (x === null || x === undefined || !Number.isFinite(Number(x))) return "-";
  const n = Number(x);
  const sign = n > 0 ? "+" : n < 0 ? "-" : "";
  return `${sign}$${formatUsd(Math.abs(n), decimals)}`;
}

function formatSignedPct(x, decimals = 2) {
  if (x === null || x === undefined || !Number.isFinite(Number(x))) return "-";
  const n = Number(x);
  const sign = n > 0 ? "+" : n < 0 ? "-" : "";
  return `${sign}${(Math.abs(n) * 100).toFixed(decimals)}%`;
}

const ANSI = {
  reset: "\x1b[0m",
  red: "\x1b[31m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  gray: "\x1b[90m",
  white: "\x1b[97m",
  dim: "\x1b[2m",
  hideCursor: "\x1b[?25l",
  showCursor: "\x1b[?25h"
};

function stripAnsi(s) {
  return String(s).replace(/\x1b\[[0-9;]*m/g, "");
}

function screenWidth() {
  const w = Number(process.stdout?.columns);
  return Number.isFinite(w) && w >= 60 ? w : 80;
}

function sepLine(ch = "─") {
  return `${ANSI.white}${ch.repeat(screenWidth())}${ANSI.reset}`;
}

function renderScreen(text) {
  try {
    readline.cursorTo(process.stdout, 0, 0);
    readline.clearScreenDown(process.stdout);
  } catch {
    // ignore
  }
  process.stdout.write(text);
}

function padRightVisible(s, width) {
  const visible = stripAnsi(s).length;
  if (visible >= width) return s;
  return s + " ".repeat(width - visible);
}

function centerText(text) {
  const w = screenWidth();
  const visible = stripAnsi(text).length;
  if (visible >= w) return text;
  const left = Math.floor((w - visible) / 2);
  const right = w - visible - left;
  return " ".repeat(left) + text + " ".repeat(right);
}

const LABEL_W = 14;
function kv(label, value) {
  return `${padRightVisible(String(label), LABEL_W)}${value}`;
}

function colorDelta(x) {
  if (x === null || x === undefined || !Number.isFinite(Number(x)) || Number(x) === 0) return ANSI.gray;
  return Number(x) > 0 ? ANSI.green : ANSI.red;
}

function colorTimeLeft(ms) {
  if (ms === null || ms === undefined || !Number.isFinite(ms)) return ANSI.gray;
  if (ms <= 15_000) return ANSI.red;
  if (ms <= 60_000) return ANSI.yellow;
  return ANSI.white;
}

function parseCliArgs(argv) {
  const args = argv.slice(2);
  const help = args.includes("-h") || args.includes("--help");
  const wsUrl = args[0] && !args[0].startsWith("-") ? String(args[0]).trim() : "";
  const symbolIncludes = args[1] && !args[1].startsWith("-") ? String(args[1]).trim() : "";
  return { help, wsUrl, symbolIncludes };
}

class PtbCalculator {
  constructor({ bucketMs }) {
    this.bucketMs = bucketMs;
    this.prevTick = null; // { tsMs, price }
    this.nextBoundaryMs = null;
    this.currentWindow = null; // { startMs, endMs, ptbPrice, ptbMethod }
  }

  resetWithTick(tick) {
    this.prevTick = tick;
    this.nextBoundaryMs = nextBucketBoundary(tick.tsMs);
    this.currentWindow = null;
  }

  onTick(tick) {
    if (!this.prevTick) {
      this.resetWithTick(tick);
      return null;
    }

    if (tick.tsMs < this.prevTick.tsMs) {
      return null;
    }

    if (tick.tsMs - this.prevTick.tsMs > this.bucketMs * 2) {
      this.resetWithTick(tick);
      return null;
    }

    if (this.nextBoundaryMs === null) this.nextBoundaryMs = nextBucketBoundary(this.prevTick.tsMs);

    let lastBoundaryEvent = null;

    while (this.nextBoundaryMs <= tick.tsMs) {
      const boundaryMs = this.nextBoundaryMs;
      let method = null;
      let price = null;

      if (tick.tsMs === boundaryMs) {
        method = "exact";
        price = tick.price;
      } else if (this.prevTick.tsMs < boundaryMs && tick.tsMs > boundaryMs) {
        method = "avg";
        price = (this.prevTick.price + tick.price) / 2;
      }

      if (price !== null) {
        lastBoundaryEvent = { startMs: boundaryMs, price, method };
        this.currentWindow = {
          startMs: boundaryMs,
          endMs: boundaryMs + this.bucketMs,
          ptbPrice: price,
          ptbMethod: method
        };
      }

      this.nextBoundaryMs += this.bucketMs;
    }

    this.prevTick = tick;
    return lastBoundaryEvent;
  }
}

applyGlobalProxyFromEnv();

const { help, wsUrl: cliWsUrl, symbolIncludes: cliSymbolIncludes } = parseCliArgs(process.argv);

if (help) {
  // eslint-disable-next-line no-console
  console.log("Usage:");
  // eslint-disable-next-line no-console
  console.log("  npm run terminal:5m");
  // eslint-disable-next-line no-console
  console.log("  node src/terminal5m.js [wsUrl] [symbolIncludes]");
  process.exit(0);
}

const wsUrl = cliWsUrl || CONFIG.polymarket.liveDataWsUrl;
const symbolIncludes = (cliSymbolIncludes || "btc").toLowerCase();

if (!wsUrl) {
  // eslint-disable-next-line no-console
  console.error("Missing wsUrl. Set POLYMARKET_LIVE_WS_URL or pass it as the first arg.");
  process.exit(1);
}

let ws = null;
let closed = false;
let connected = false;
let reconnectMs = 500;
let reconnectTimer = null;
let lastReconnectReason = "";

const ptb = new PtbCalculator({ bucketMs: BUCKET_MS });

let lastTick = null; // { tsMs, receivedAtMs, price, symbol, updatedAtMs }
let prevWsPrice = null;
let lastWsArrow = "";

function scheduleReconnect(reason) {
  if (closed) return;
  connected = false;
  lastReconnectReason = reason || "reconnect";

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
  reconnectTimer = setTimeout(connect, wait);
  reconnectTimer?.unref?.();
}

function connect() {
  if (closed) return;

  ws = new WebSocket(wsUrl, {
    handshakeTimeout: 10_000,
    agent: wsAgentForUrl(wsUrl)
  });

  ws.on("open", () => {
    connected = true;
    reconnectMs = 500;
    lastReconnectReason = "";
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

    const payload = normalizePayload(data.payload) || {};
    const symbolRaw = String(payload.symbol || payload.pair || payload.ticker || "");
    const symbolLower = symbolRaw.toLowerCase();
    if (symbolIncludes && !symbolLower.includes(symbolIncludes)) return;

    const price = toFiniteNumber(payload.value ?? payload.price ?? payload.current ?? payload.data);
    if (price === null) return;

    const timestampMs = epochToMs(payload.timestamp ?? null);
    if (timestampMs === null) return;

    const updatedAtMs = epochToMs(payload.updatedAt ?? null);

    const displaySymbol = symbolRaw ? symbolRaw.toUpperCase().replace(/\s+/g, "") : "BTC/USD";

    if (prevWsPrice !== null && Number.isFinite(prevWsPrice) && price !== prevWsPrice) {
      lastWsArrow = price > prevWsPrice ? "↑" : "↓";
    } else {
      lastWsArrow = "";
    }
    prevWsPrice = price;

    lastTick = { tsMs: timestampMs, receivedAtMs, price, symbol: displaySymbol, updatedAtMs };
    ptb.onTick({ tsMs: timestampMs, price });
  });

  ws.on("close", () => scheduleReconnect("ws closed"));
  ws.on("error", () => scheduleReconnect("ws error"));
}

function computeNowMs() {
  if (!lastTick) return Date.now();
  const sinceReceive = Date.now() - lastTick.receivedAtMs;
  const est = lastTick.tsMs + Math.max(0, sinceReceive);
  return Math.max(est, lastTick.tsMs);
}

function buildScreen() {
  const nowMs = computeNowMs();
  const w = ptb.currentWindow;

  const timeLeftMs = w ? clamp(w.endMs - nowMs, 0, BUCKET_MS) : null;
  const timeLeft = formatMmSs(timeLeftMs);
  const timeLeftColor = colorTimeLeft(timeLeftMs);

  const wsStatus = connected ? `${ANSI.green}CONNECTED${ANSI.reset}` : `${ANSI.yellow}RECONNECTING${ANSI.reset}`;

  const lastTickTime = lastTick ? formatLocalTsMs(lastTick.tsMs) : "-";
  const tickAgeMs = lastTick?.updatedAtMs ? Math.abs(lastTick.tsMs - lastTick.updatedAtMs) : null;
  const tickAge = tickAgeMs === null ? "-" : `${Math.floor(tickAgeMs)}ms`;
  const symbol = lastTick?.symbol || "BTC/USD";

  const wsLine = [
    `WS: ${wsStatus}`,
    `last tick: ${ANSI.white}${lastTickTime}${ANSI.reset}`,
    `age: ${ANSI.white}${tickAge}${ANSI.reset}`,
    `symbol: ${ANSI.white}${symbol}${ANSI.reset}`
  ].join(" | ");

  const reconnectLine = !connected && lastReconnectReason
    ? `${ANSI.dim}${ANSI.gray}${lastReconnectReason}${ANSI.reset}`
    : null;

  const windowLine = w
    ? `${formatLocalTsMin(w.startMs)}  ->  ${formatLocalTsMin(w.endMs)}`
    : `${ANSI.gray}waiting boundary...${ANSI.reset}`;

  const ptbLine = w
    ? `${ANSI.white}$${formatUsd(w.ptbPrice, 2)}${ANSI.reset}  (${ANSI.white}${w.ptbMethod}${ANSI.reset})  @  ${formatLocalTsMin(w.startMs)}`
    : `${ANSI.gray}-${ANSI.reset}`;

  const curPrice = lastTick?.price ?? null;
  const curPriceLine = curPrice === null
    ? `${ANSI.gray}-${ANSI.reset}`
    : `${ANSI.white}$${formatUsd(curPrice, 2)}${ANSI.reset}${lastWsArrow ? ` ${ANSI.dim}${ANSI.gray}${lastWsArrow}${ANSI.reset}` : ""}`;

  const delta = w && curPrice !== null ? curPrice - w.ptbPrice : null;
  const deltaPct = w && curPrice !== null && w.ptbPrice ? delta / w.ptbPrice : null;
  const deltaColor = colorDelta(delta);
  const deltaLine = delta === null
    ? `${ANSI.gray}-${ANSI.reset}`
    : `${deltaColor}${formatSignedUsd(delta, 2)}${ANSI.reset}  ${deltaColor}(${formatSignedPct(deltaPct, 2)})${ANSI.reset}`;

  const title = centerText(`${ANSI.white}POLYMARKET BTC/USD 5m TERMINAL${ANSI.reset}`);

  const lines = [
    sepLine(),
    title,
    sepLine(),
    wsLine,
    reconnectLine,
    "",
    `${ANSI.white}TIME LEFT${ANSI.reset}`,
    `${timeLeftColor}${centerText(timeLeft)}${ANSI.reset}`,
    "",
    sepLine(),
    kv("WINDOW:", windowLine),
    kv("PTB:", ptbLine),
    "",
    sepLine(),
    kv("BTC/USD:", curPriceLine),
    kv("Δ vs PTB:", deltaLine),
    sepLine(),
    centerText(`${ANSI.dim}${ANSI.gray}Ctrl+C to exit${ANSI.reset}`)
  ].filter((x) => x !== null);

  return lines.join("\n") + "\n";
}

function hideCursor() {
  try {
    process.stdout.write(ANSI.hideCursor);
  } catch {
    // ignore
  }
}

function showCursor() {
  try {
    process.stdout.write(ANSI.showCursor);
  } catch {
    // ignore
  }
}

hideCursor();
process.on("exit", () => showCursor());

connect();
const drawTimer = setInterval(() => renderScreen(buildScreen()), 200);
drawTimer?.unref?.();

function shutdown(reason = "shutdown") {
  closed = true;
  connected = false;

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

  try {
    clearInterval(drawTimer);
  } catch {
    // ignore
  }

  showCursor();
  renderScreen(`${ANSI.reset}\n[${formatLocalTsMs(Date.now())}] ${reason}\n`);
  process.exit(0);
}

process.once("SIGINT", () => shutdown("SIGINT"));
process.once("SIGTERM", () => shutdown("SIGTERM"));
