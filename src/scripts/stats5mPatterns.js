import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

const FIVE_MIN_MS = 5 * 60 * 1000;
const LAST_TWO_MIN_MS = 2 * 60 * 1000;
const COMPLETE_MIN_MS = 4 * 60 * 1000;

function usage() {
  return [
    "Usage:",
    "  node src/scripts/stats5mPatterns.js [--root <path>] [--date <YYYY-MM-DD>] [--include-incomplete] [--json]",
    "",
    "Options:",
    "  --root <path>           Log root (default: ./logs/raw)",
    "  --date <YYYY-MM-DD>     Limit to one date; repeatable",
    "  --include-incomplete    Include incomplete windows",
    "  --json                  Output JSON"
  ].join("\n");
}

function toFiniteNumber(x) {
  const n = typeof x === "string" ? Number(x) : x;
  return Number.isFinite(n) ? n : null;
}

function isDateDirName(name) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(name || ""));
}

function parseArgs(argv) {
  const args = {
    root: "./logs/raw",
    dates: [],
    includeIncomplete: false,
    json: false
  };

  for (let i = 0; i < argv.length; i += 1) {
    const cur = argv[i];
    if (cur === "--root") {
      const next = argv[i + 1];
      if (!next) throw new Error("--root requires a value");
      args.root = next;
      i += 1;
      continue;
    }
    if (cur === "--date") {
      const next = argv[i + 1];
      if (!next) throw new Error("--date requires a value");
      if (!isDateDirName(next)) {
        throw new Error(`Invalid --date value: ${next}`);
      }
      args.dates.push(next);
      i += 1;
      continue;
    }
    if (cur === "--include-incomplete") {
      args.includeIncomplete = true;
      continue;
    }
    if (cur === "--json") {
      args.json = true;
      continue;
    }
    if (cur === "--help" || cur === "-h") {
      process.stdout.write(`${usage()}\n`);
      process.exit(0);
    }
    throw new Error(`Unknown argument: ${cur}`);
  }

  args.dates = Array.from(new Set(args.dates)).sort();
  return args;
}

function parseMarketMeta(value) {
  const s = String(value || "");
  const m = s.match(/btc-updown-(\d+)m-(\d{10})$/);
  if (!m) return null;
  const minutes = Number(m[1]);
  const startSec = Number(m[2]);
  if (!Number.isFinite(minutes) || !Number.isFinite(startSec) || minutes <= 0) return null;
  const startMs = startSec * 1000;
  return {
    minutes,
    windowMs: minutes * 60 * 1000,
    startMs,
    endMs: startMs + minutes * 60 * 1000
  };
}

function toUpdownPointTimeMs(row) {
  return toFiniteNumber(row?.bucket_end_ms)
    ?? toFiniteNumber(row?.last_event_time_ms)
    ?? toFiniteNumber(row?.event_time_ms)
    ?? toFiniteNumber(row?.receive_time_ms);
}

function toBtcTimeMs(row) {
  return toFiniteNumber(row?.event_time_ms) ?? toFiniteNumber(row?.receive_time_ms);
}

function toPrice(row) {
  return toFiniteNumber(row?.last_trade_price) ?? toFiniteNumber(row?.mid);
}

function floorToBucket(ms, bucketMs) {
  return Math.floor(ms / bucketMs) * bucketMs;
}

function sortPoints(points) {
  points.sort((a, b) => a.ts - b.ts);
}

function computeCoverageMs(points) {
  if (points.length <= 1) return 0;
  return points[points.length - 1].ts - points[0].ts;
}

function evaluateLateVolatility(points) {
  let highSeen = false;
  for (const p of points) {
    if (p.price >= 0.8) highSeen = true;
    if (highSeen && p.price < 0.4) return true;
  }
  return false;
}

function computeMaxDrawdownAbs(points) {
  let runningHigh = -Infinity;
  let maxDd = 0;
  for (const p of points) {
    runningHigh = Math.max(runningHigh, p.price);
    maxDd = Math.max(maxDd, runningHigh - p.price);
  }
  return maxDd;
}

function evaluateSidePatterns(points, windowStartMs, windowEndMs) {
  const inWindow = points.filter((p) => p.ts >= windowStartMs && p.ts <= windowEndMs);
  if (inWindow.length === 0) {
    return {
      hasData: false,
      extremeReversal: false,
      lateVolatility: false,
      peacefulFinish: false,
      metrics: null
    };
  }

  sortPoints(inWindow);
  const finalPrice = inWindow[inWindow.length - 1].price;
  let fullMax = -Infinity;
  for (const p of inWindow) fullMax = Math.max(fullMax, p.price);

  const last2mStart = windowEndMs - LAST_TWO_MIN_MS;
  const last2m = inWindow.filter((p) => p.ts >= last2mStart);
  const last2mHigh = last2m.length > 0 ? Math.max(...last2m.map((x) => x.price)) : null;
  const last2mLow = last2m.length > 0 ? Math.min(...last2m.map((x) => x.price)) : null;

  const extremeReversal = fullMax >= 0.98 && finalPrice <= 0.01;
  const lateVolatility = last2m.length > 0 ? evaluateLateVolatility(last2m) : false;
  const maxDrawdownAbs = last2m.length > 0 ? computeMaxDrawdownAbs(last2m) : null;
  const peacefulFinish = (
    last2m.length > 0
    && finalPrice >= 0.99
    && maxDrawdownAbs !== null
    && maxDrawdownAbs <= 0.1
    && !lateVolatility
  );

  return {
    hasData: true,
    extremeReversal,
    lateVolatility,
    peacefulFinish,
    metrics: {
      maxPrice: fullMax,
      finalPrice,
      last2mHigh,
      last2mLow,
      maxDrawdownAbs
    }
  };
}

function newWindow(date, windowId, marketSlug, meta) {
  return {
    date,
    windowId,
    marketSlug,
    startMs: meta.startMs,
    endMs: meta.endMs,
    sidePoints: {
      up: [],
      down: []
    },
    btcPoints: [],
    coverage: {
      upCoverageMs: 0,
      downCoverageMs: 0,
      oddsCoverageMs: 0,
      btcCoverageMs: 0
    },
    isComplete: false
  };
}

function finalizeWindow(window) {
  sortPoints(window.sidePoints.up);
  sortPoints(window.sidePoints.down);
  sortPoints(window.btcPoints);

  const upCoverageMs = computeCoverageMs(window.sidePoints.up);
  const downCoverageMs = computeCoverageMs(window.sidePoints.down);
  const oddsCoverageMs = Math.max(upCoverageMs, downCoverageMs);
  const btcCoverageMs = computeCoverageMs(window.btcPoints);
  const isComplete = btcCoverageMs >= COMPLETE_MIN_MS && oddsCoverageMs >= COMPLETE_MIN_MS;
  window.coverage = {
    upCoverageMs,
    downCoverageMs,
    oddsCoverageMs,
    btcCoverageMs
  };
  window.isComplete = isComplete;
}

function createWarningTracker() {
  const counts = new Map();
  const samples = [];
  return {
    add(code, message) {
      counts.set(code, (counts.get(code) || 0) + 1);
      if (samples.length < 25) samples.push({ code, message });
    },
    toObject() {
      return {
        total: Array.from(counts.values()).reduce((a, b) => a + b, 0),
        byCode: Object.fromEntries(Array.from(counts.entries()).sort((a, b) => a[0].localeCompare(b[0]))),
        samples
      };
    }
  };
}

async function eachJsonl(filePath, warnings, onRow) {
  if (!fs.existsSync(filePath)) {
    warnings.add("missing_file", `missing: ${filePath}`);
    return;
  }
  const stream = fs.createReadStream(filePath, { encoding: "utf8" });
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
  let lineNo = 0;

  for await (const line of rl) {
    lineNo += 1;
    if (!line || !line.trim()) continue;
    let row = null;
    try {
      row = JSON.parse(line);
    } catch {
      warnings.add("bad_json_line", `${filePath}:${lineNo}`);
      continue;
    }
    await onRow(row);
  }
}

function listDateDirs(root) {
  if (!fs.existsSync(root)) return [];
  const out = [];
  for (const entry of fs.readdirSync(root, { withFileTypes: true })) {
    if (entry.isDirectory() && isDateDirName(entry.name)) out.push(entry.name);
  }
  out.sort();
  return out;
}

function hasPartitionedWindowDirs(dayDir) {
  if (!fs.existsSync(dayDir)) return false;
  for (const entry of fs.readdirSync(dayDir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    if (entry.name.startsWith(".")) continue;
    const winDir = path.join(dayDir, entry.name);
    const hasUpdown = fs.existsSync(path.join(winDir, "updown_state.jsonl"));
    const hasBtc = fs.existsSync(path.join(winDir, "btc_reference.jsonl"));
    if (hasUpdown || hasBtc) return true;
  }
  return false;
}

async function loadPartitionedWindows(root, date, warnings, counters) {
  const dayDir = path.join(root, date);
  const windows = [];
  for (const entry of fs.readdirSync(dayDir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    if (entry.name.startsWith(".")) continue;
    if (entry.name === "_unassigned") continue;

    counters.scannedWindows += 1;
    const winDir = path.join(dayDir, entry.name);
    const updownPath = path.join(winDir, "updown_state.jsonl");
    const btcPath = path.join(winDir, "btc_reference.jsonl");
    const parsedById = parseMarketMeta(entry.name);
    let parsed = parsedById;
    let marketSlug = entry.name;

    let window = parsed
      ? newWindow(date, entry.name, marketSlug, parsed)
      : null;

    await eachJsonl(updownPath, warnings, (row) => {
      const side = String(row?.side || "").toLowerCase();
      if (side !== "up" && side !== "down") return;
      const ts = toUpdownPointTimeMs(row);
      const price = toPrice(row);
      if (ts === null || price === null) return;

      const slug = String(row?.market_slug || "").trim();
      if (slug) marketSlug = slug;
      if (!parsed && slug) {
        parsed = parseMarketMeta(slug);
      }
      if (!parsed) return;
      if (parsed.minutes !== 5) return;

      if (!window) {
        window = newWindow(date, entry.name, marketSlug, parsed);
      }
      window.marketSlug = marketSlug;
      window.sidePoints[side].push({ ts, price });
    });

    if (!parsed || parsed.minutes !== 5 || !window) continue;
    counters.valid5mWindows += 1;

    await eachJsonl(btcPath, warnings, (row) => {
      const ts = toBtcTimeMs(row);
      const price = toFiniteNumber(row?.price);
      if (ts === null || price === null) return;
      window.btcPoints.push({ ts, price });
    });

    finalizeWindow(window);
    if (window.isComplete) counters.completeWindows += 1;
    windows.push(window);
  }
  return windows;
}

async function loadLegacyWindows(root, date, warnings, counters) {
  const dayDir = path.join(root, date);
  const updownPath = path.join(dayDir, "updown_state.jsonl");
  const btcPath = path.join(dayDir, "btc_reference.jsonl");

  const allSlugCandidates = new Set();
  const windowsBySlug = new Map();

  await eachJsonl(updownPath, warnings, (row) => {
    const slug = String(row?.market_slug || "").trim();
    if (!slug) return;
    allSlugCandidates.add(slug);

    const parsed = parseMarketMeta(slug);
    if (!parsed || parsed.minutes !== 5) return;
    let w = windowsBySlug.get(slug);
    if (!w) {
      w = newWindow(date, slug, slug, parsed);
      windowsBySlug.set(slug, w);
    }

    const side = String(row?.side || "").toLowerCase();
    if (side !== "up" && side !== "down") return;
    const ts = toUpdownPointTimeMs(row);
    const price = toPrice(row);
    if (ts === null || price === null) return;
    w.sidePoints[side].push({ ts, price });
  });

  counters.scannedWindows += allSlugCandidates.size;
  counters.valid5mWindows += windowsBySlug.size;

  await eachJsonl(btcPath, warnings, (row) => {
    const ts = toBtcTimeMs(row);
    const price = toFiniteNumber(row?.price);
    if (ts === null || price === null) return;
    const startMs = floorToBucket(ts, FIVE_MIN_MS);
    for (const w of windowsBySlug.values()) {
      if (w.startMs === startMs) {
        w.btcPoints.push({ ts, price });
      }
    }
  });

  const out = Array.from(windowsBySlug.values());
  for (const window of out) {
    finalizeWindow(window);
    if (window.isComplete) counters.completeWindows += 1;
  }
  return out;
}

function fmt(n, digits = 4) {
  return Number.isFinite(n) ? n.toFixed(digits) : "-";
}

function formatUtcWindowRange(startMs, endMs) {
  const a = new Date(startMs);
  const b = new Date(endMs);
  const y = a.getUTCFullYear();
  const m = String(a.getUTCMonth() + 1).padStart(2, "0");
  const d = String(a.getUTCDate()).padStart(2, "0");
  const hh1 = String(a.getUTCHours()).padStart(2, "0");
  const mm1 = String(a.getUTCMinutes()).padStart(2, "0");
  const ss1 = String(a.getUTCSeconds()).padStart(2, "0");
  const hh2 = String(b.getUTCHours()).padStart(2, "0");
  const mm2 = String(b.getUTCMinutes()).padStart(2, "0");
  const ss2 = String(b.getUTCSeconds()).padStart(2, "0");
  return `${y}-${m}-${d} ${hh1}:${mm1}:${ss1} - ${hh2}:${mm2}:${ss2} UTC`;
}

function pushPatternHit(target, window, side, metrics) {
  target.push({
    date: window.date,
    market_slug: window.marketSlug || window.windowId,
    window_id: window.windowId,
    window_start_ms: window.startMs,
    window_end_ms: window.endMs,
    side,
    metrics
  });
}

function sortHits(hits) {
  hits.sort((a, b) => {
    if (a.window_start_ms !== b.window_start_ms) return a.window_start_ms - b.window_start_ms;
    if (a.market_slug !== b.market_slug) return a.market_slug.localeCompare(b.market_slug);
    return a.side.localeCompare(b.side);
  });
}

function analyzeWindows(windows, includeIncomplete, counters) {
  const hits = {
    extremeReversal: [],
    lateVolatility: [],
    peacefulFinish: []
  };

  let extremeWindowCount = 0;
  let lateWindowCount = 0;
  let peaceWindowCount = 0;

  for (const window of windows) {
    if (!includeIncomplete && !window.isComplete) continue;
    counters.countedWindows += 1;

    const sideResults = {
      up: evaluateSidePatterns(window.sidePoints.up, window.startMs, window.endMs),
      down: evaluateSidePatterns(window.sidePoints.down, window.startMs, window.endMs)
    };

    let hitExtreme = false;
    let hitLate = false;
    let hitPeace = false;

    for (const side of ["up", "down"]) {
      const result = sideResults[side];
      if (!result.hasData || !result.metrics) continue;
      if (result.extremeReversal) {
        hitExtreme = true;
        pushPatternHit(hits.extremeReversal, window, side, {
          max_price: result.metrics.maxPrice,
          final_price: result.metrics.finalPrice
        });
      }
      if (result.lateVolatility) {
        hitLate = true;
        pushPatternHit(hits.lateVolatility, window, side, {
          last2m_high: result.metrics.last2mHigh,
          last2m_low: result.metrics.last2mLow,
          final_price: result.metrics.finalPrice
        });
      }
      if (result.peacefulFinish) {
        hitPeace = true;
        pushPatternHit(hits.peacefulFinish, window, side, {
          final_price: result.metrics.finalPrice,
          last2m_high: result.metrics.last2mHigh,
          last2m_low: result.metrics.last2mLow,
          max_drawdown_abs: result.metrics.maxDrawdownAbs
        });
      }
    }

    if (hitExtreme) extremeWindowCount += 1;
    if (hitLate) lateWindowCount += 1;
    if (hitPeace) peaceWindowCount += 1;
  }

  sortHits(hits.extremeReversal);
  sortHits(hits.lateVolatility);
  sortHits(hits.peacefulFinish);

  return {
    hits,
    counts: {
      extremeReversal: extremeWindowCount,
      lateVolatility: lateWindowCount,
      peacefulFinish: peaceWindowCount
    }
  };
}

function printConsoleResult(result) {
  const { root, dates, includeIncomplete } = result.config;
  const c = result.counters;
  process.stdout.write("=== 5m Pattern Stats ===\n");
  process.stdout.write(`root: ${root}\n`);
  process.stdout.write(`dates: ${dates.join(", ") || "-"}\n`);
  process.stdout.write(`includeIncomplete: ${String(includeIncomplete)}\n`);
  process.stdout.write("\n");
  process.stdout.write(`scannedWindows: ${c.scannedWindows}\n`);
  process.stdout.write(`valid5mWindows: ${c.valid5mWindows}\n`);
  process.stdout.write(`completeWindows: ${c.completeWindows}\n`);
  process.stdout.write(`countedWindows: ${c.countedWindows}\n`);
  process.stdout.write("\n");
  process.stdout.write(`extremeReversal: ${result.patterns.extremeReversal.windowCount}\n`);
  process.stdout.write(`lateVolatility: ${result.patterns.lateVolatility.windowCount}\n`);
  process.stdout.write(`peacefulFinish: ${result.patterns.peacefulFinish.windowCount}\n`);
  process.stdout.write("\n");

  const printHitList = (name, hits) => {
    process.stdout.write(`[${name}] side hits: ${hits.length}\n`);
    if (hits.length === 0) {
      process.stdout.write("- (none)\n\n");
      return;
    }
    for (const hit of hits) {
      const range = formatUtcWindowRange(hit.window_start_ms, hit.window_end_ms);
      const base = `${range} | side=${hit.side}`;
      if (name === "extremeReversal") {
        process.stdout.write(
          `- ${base} | max=${fmt(hit.metrics.max_price)} | final=${fmt(hit.metrics.final_price)}\n`
        );
      } else if (name === "lateVolatility") {
        process.stdout.write(
          `- ${base} | last2m_high=${fmt(hit.metrics.last2m_high)} | last2m_low=${fmt(hit.metrics.last2m_low)} | final=${fmt(hit.metrics.final_price)}\n`
        );
      } else {
        process.stdout.write(
          `- ${base} | final=${fmt(hit.metrics.final_price)} | last2m_high=${fmt(hit.metrics.last2m_high)} | last2m_low=${fmt(hit.metrics.last2m_low)} | max_drawdown_abs=${fmt(hit.metrics.max_drawdown_abs)}\n`
        );
      }
    }
    process.stdout.write("\n");
  };

  printHitList("extremeReversal", result.patterns.extremeReversal.hits);
  printHitList("lateVolatility", result.patterns.lateVolatility.hits);
  printHitList("peacefulFinish", result.patterns.peacefulFinish.hits);

  if (result.warnings.total > 0) {
    process.stdout.write("warnings:\n");
    for (const [k, v] of Object.entries(result.warnings.byCode)) {
      process.stdout.write(`- ${k}: ${v}\n`);
    }
    if (result.warnings.samples.length > 0) {
      process.stdout.write("warning samples:\n");
      for (const s of result.warnings.samples) {
        process.stdout.write(`- [${s.code}] ${s.message}\n`);
      }
    }
  }
}

async function main() {
  let args;
  try {
    args = parseArgs(process.argv.slice(2));
  } catch (err) {
    process.stderr.write(`Error: ${err.message}\n\n${usage()}\n`);
    process.exit(1);
  }

  const root = path.resolve(process.cwd(), args.root);
  if (!fs.existsSync(root)) {
    process.stderr.write(`Error: root does not exist: ${root}\n`);
    process.exit(1);
  }

  const warnings = createWarningTracker();
  const counters = {
    scannedWindows: 0,
    valid5mWindows: 0,
    completeWindows: 0,
    countedWindows: 0
  };

  let dates = args.dates;
  if (dates.length === 0) dates = listDateDirs(root);
  if (dates.length === 0) {
    const emptyResult = {
      config: { root, dates: [], includeIncomplete: args.includeIncomplete },
      counters,
      patterns: {
        extremeReversal: { windowCount: 0, sideHitCount: 0, hits: [] },
        lateVolatility: { windowCount: 0, sideHitCount: 0, hits: [] },
        peacefulFinish: { windowCount: 0, sideHitCount: 0, hits: [] }
      },
      warnings: warnings.toObject()
    };
    if (args.json) {
      process.stdout.write(`${JSON.stringify(emptyResult, null, 2)}\n`);
    } else {
      printConsoleResult(emptyResult);
    }
    return;
  }

  const allWindows = [];
  for (const date of dates) {
    const dayDir = path.join(root, date);
    if (!fs.existsSync(dayDir)) {
      warnings.add("missing_date_dir", dayDir);
      continue;
    }
    const partitioned = hasPartitionedWindowDirs(dayDir);
    if (partitioned) {
      const windows = await loadPartitionedWindows(root, date, warnings, counters);
      allWindows.push(...windows);
    } else {
      const windows = await loadLegacyWindows(root, date, warnings, counters);
      allWindows.push(...windows);
    }
  }

  const analyzed = analyzeWindows(allWindows, args.includeIncomplete, counters);
  const warningObj = warnings.toObject();
  const result = {
    config: {
      root,
      dates,
      includeIncomplete: args.includeIncomplete
    },
    counters,
    patterns: {
      extremeReversal: {
        windowCount: analyzed.counts.extremeReversal,
        sideHitCount: analyzed.hits.extremeReversal.length,
        hits: analyzed.hits.extremeReversal
      },
      lateVolatility: {
        windowCount: analyzed.counts.lateVolatility,
        sideHitCount: analyzed.hits.lateVolatility.length,
        hits: analyzed.hits.lateVolatility
      },
      peacefulFinish: {
        windowCount: analyzed.counts.peacefulFinish,
        sideHitCount: analyzed.hits.peacefulFinish.length,
        hits: analyzed.hits.peacefulFinish
      }
    },
    warnings: warningObj
  };

  if (args.json) {
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    return;
  }
  printConsoleResult(result);
}

main().catch((err) => {
  process.stderr.write(`Fatal: ${err?.stack || err?.message || String(err)}\n`);
  process.exit(1);
});
