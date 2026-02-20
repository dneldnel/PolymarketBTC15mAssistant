import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";
import crypto from "node:crypto";
import {
  DEFAULT_PATTERN_CONFIG,
  FIVE_MIN_MS,
  analyzeWindows,
  finalizeWindow,
  floorToBucket,
  newWindow,
  normalizePatternConfig,
  parseMarketMeta,
  stableStringify,
  toBtcTimeMs,
  toFiniteNumber,
  toPrice,
  toUpdownPointTimeMs
} from "./patterns5mCore.js";

function usage() {
  return [
    "Usage:",
    "  node src/scripts/stats5mPatterns.js [--root <path>] [--date <YYYY-MM-DD>] [--pattern-config <path>] [--include-incomplete] [--json]",
    "",
    "Options:",
    "  --root <path>           Log root (default: ./logs/raw)",
    "  --date <YYYY-MM-DD>     Limit to one date; repeatable",
    "  --pattern-config <path> Pattern config JSON (default: ./config/patterns5m.json)",
    "  --include-incomplete    Include incomplete windows",
    "  --json                  Output JSON"
  ].join("\n");
}

function isDateDirName(name) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(name || ""));
}

function parseArgs(argv) {
  const args = {
    root: "./logs/raw",
    dates: [],
    patternConfigPath: "./config/patterns5m.json",
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
    if (cur === "--pattern-config") {
      const next = argv[i + 1];
      if (!next) throw new Error("--pattern-config requires a value");
      args.patternConfigPath = next;
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

function loadPatternConfig(patternConfigPath, warnings) {
  const resolvedPath = path.resolve(process.cwd(), patternConfigPath || "./config/patterns5m.json");
  let raw = DEFAULT_PATTERN_CONFIG;
  let source = "default";
  if (fs.existsSync(resolvedPath)) {
    source = resolvedPath;
    try {
      raw = JSON.parse(fs.readFileSync(resolvedPath, "utf8"));
    } catch {
      warnings.add("bad_pattern_config_json", resolvedPath);
      raw = DEFAULT_PATTERN_CONFIG;
      source = "default";
    }
  }
  const config = normalizePatternConfig(raw);
  const hash = crypto.createHash("sha256").update(stableStringify(config)).digest("hex");
  return {
    source,
    path: source === "default" ? null : resolvedPath,
    hash,
    config
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

function printConsoleResult(result) {
  const {
    root,
    dates,
    includeIncomplete,
    patternConfigSource,
    patternSetVersion,
    patternParamsHash
  } = result.config;
  const c = result.counters;
  process.stdout.write("=== 5m Pattern Stats ===\n");
  process.stdout.write(`root: ${root}\n`);
  process.stdout.write(`dates: ${dates.join(", ") || "-"}\n`);
  process.stdout.write(`includeIncomplete: ${String(includeIncomplete)}\n`);
  process.stdout.write(`patternConfig: ${patternConfigSource || "default"}\n`);
  process.stdout.write(`patternSetVersion: ${patternSetVersion || "1"}\n`);
  process.stdout.write(`patternParamsHash: ${patternParamsHash || "-"}\n`);
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
  const patternConfig = loadPatternConfig(args.patternConfigPath, warnings);
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
      config: {
        root,
        dates: [],
        includeIncomplete: args.includeIncomplete,
        patternConfigSource: patternConfig.source,
        patternSetVersion: patternConfig.config.patternSetVersion,
        patternParamsHash: patternConfig.hash
      },
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

  const analyzed = analyzeWindows(
    allWindows,
    args.includeIncomplete,
    counters,
    patternConfig.config
  );
  const warningObj = warnings.toObject();
  const result = {
    config: {
      root,
      dates,
      includeIncomplete: args.includeIncomplete,
      patternConfigSource: patternConfig.source,
      patternSetVersion: patternConfig.config.patternSetVersion,
      patternParamsHash: patternConfig.hash
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
