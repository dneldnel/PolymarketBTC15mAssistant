export const FIVE_MIN_MS = 5 * 60 * 1000;
export const LAST_TWO_MIN_MS = 2 * 60 * 1000;
export const COMPLETE_MIN_MS = 4 * 60 * 1000;
export const PATTERN_PRIORITY = ["extremeReversal", "lateVolatility", "peacefulFinish"];
export const DEFAULT_PATTERN_SET_VERSION = "1";

export const DEFAULT_PATTERN_CONFIG = {
  patternSetVersion: DEFAULT_PATTERN_SET_VERSION,
  patterns: {
    extremeReversal: {
      enabled: true,
      params: {
        maxPriceThreshold: 0.98,
        finalPriceThreshold: 0.01
      }
    },
    lateVolatility: {
      enabled: true,
      params: {
        highThreshold: 0.8,
        lowThreshold: 0.4
      }
    },
    peacefulFinish: {
      enabled: true,
      params: {
        finalPriceThreshold: 0.99,
        maxDrawdownAbsThreshold: 0.1
      }
    }
  }
};

export function toFiniteNumber(x) {
  const n = typeof x === "string" ? Number(x) : x;
  return Number.isFinite(n) ? n : null;
}

export function parseMarketMeta(value) {
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

export function toUpdownPointTimeMs(row) {
  return toFiniteNumber(row?.bucket_end_ms)
    ?? toFiniteNumber(row?.last_event_time_ms)
    ?? toFiniteNumber(row?.event_time_ms)
    ?? toFiniteNumber(row?.receive_time_ms);
}

export function toBtcTimeMs(row) {
  return toFiniteNumber(row?.event_time_ms) ?? toFiniteNumber(row?.receive_time_ms);
}

export function toPrice(row) {
  const lastTradePrice = toFiniteNumber(row?.last_trade_price);
  const mid = toFiniteNumber(row?.mid);
  const bestBid = toFiniteNumber(row?.best_bid);
  const bestAsk = toFiniteNumber(row?.best_ask);
  const eps = 1e-9;

  // Prefer last_trade_price only when it is consistent with visible BBO.
  if (lastTradePrice !== null) {
    if (bestBid !== null && bestAsk !== null) {
      const low = Math.min(bestBid, bestAsk) - eps;
      const high = Math.max(bestBid, bestAsk) + eps;
      if (lastTradePrice >= low && lastTradePrice <= high) return lastTradePrice;
      return mid ?? null;
    }
    return lastTradePrice;
  }

  return mid;
}

export function floorToBucket(ms, bucketMs) {
  return Math.floor(ms / bucketMs) * bucketMs;
}

export function sortPoints(points) {
  points.sort((a, b) => a.ts - b.ts);
}

export function computeCoverageMs(points) {
  if (points.length <= 1) return 0;
  return points[points.length - 1].ts - points[0].ts;
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

function deepClone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

export function normalizePatternConfig(raw = {}) {
  const cfg = deepClone(DEFAULT_PATTERN_CONFIG);
  if (typeof raw?.patternSetVersion === "string" && raw.patternSetVersion.trim()) {
    cfg.patternSetVersion = raw.patternSetVersion.trim();
  }
  for (const id of PATTERN_PRIORITY) {
    const src = raw?.patterns?.[id];
    if (!src || typeof src !== "object") continue;
    if (typeof src.enabled === "boolean") cfg.patterns[id].enabled = src.enabled;
    if (src.params && typeof src.params === "object") {
      cfg.patterns[id].params = { ...cfg.patterns[id].params, ...src.params };
    }
  }
  return cfg;
}

export function stableStringify(value) {
  if (Array.isArray(value)) {
    return `[${value.map((x) => stableStringify(x)).join(",")}]`;
  }
  if (value && typeof value === "object") {
    const keys = Object.keys(value).sort();
    const body = keys.map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`).join(",");
    return `{${body}}`;
  }
  return JSON.stringify(value);
}

const PATTERN_EVALUATORS = {
  extremeReversal(ctx, params) {
    const maxPriceThreshold = Number(toFiniteNumber(params?.maxPriceThreshold) ?? 0.98);
    const finalPriceThreshold = Number(toFiniteNumber(params?.finalPriceThreshold) ?? 0.01);
    const hit = ctx.fullMax >= maxPriceThreshold && ctx.finalPrice <= finalPriceThreshold;
    return {
      hit,
      metrics: {
        max_price: ctx.fullMax,
        final_price: ctx.finalPrice
      }
    };
  },
  lateVolatility(ctx, params) {
    const highThreshold = Number(toFiniteNumber(params?.highThreshold) ?? 0.8);
    const lowThreshold = Number(toFiniteNumber(params?.lowThreshold) ?? 0.4);
    let highSeen = false;
    let hit = false;
    for (const p of ctx.last2m) {
      if (p.price >= highThreshold) highSeen = true;
      if (highSeen && p.price < lowThreshold) {
        hit = true;
        break;
      }
    }
    return {
      hit,
      metrics: {
        last2m_high: ctx.last2mHigh,
        last2m_low: ctx.last2mLow,
        final_price: ctx.finalPrice
      }
    };
  },
  peacefulFinish(ctx, params, state) {
    const finalPriceThreshold = Number(toFiniteNumber(params?.finalPriceThreshold) ?? 0.99);
    const maxDrawdownAbsThreshold = Number(toFiniteNumber(params?.maxDrawdownAbsThreshold) ?? 0.1);
    const lateVolatilityHit = Boolean(state?.hits?.lateVolatility);
    const hit = (
      ctx.last2m.length > 0
      && ctx.finalPrice >= finalPriceThreshold
      && ctx.maxDrawdownAbs !== null
      && ctx.maxDrawdownAbs <= maxDrawdownAbsThreshold
      && !lateVolatilityHit
    );
    return {
      hit,
      metrics: {
        final_price: ctx.finalPrice,
        last2m_high: ctx.last2mHigh,
        last2m_low: ctx.last2mLow,
        max_drawdown_abs: ctx.maxDrawdownAbs
      }
    };
  }
};

export function evaluateSidePatterns(points, windowStartMs, windowEndMs, patternConfigInput = null) {
  const patternConfig = normalizePatternConfig(patternConfigInput || DEFAULT_PATTERN_CONFIG);
  const enabledPatternIds = PATTERN_PRIORITY.filter((id) => patternConfig?.patterns?.[id]?.enabled);
  const inWindow = points.filter((p) => p.ts >= windowStartMs && p.ts <= windowEndMs);
  const empty = {
    hasData: false,
    hits: Object.fromEntries(PATTERN_PRIORITY.map((id) => [id, false])),
    metricsByPattern: Object.fromEntries(PATTERN_PRIORITY.map((id) => [id, null])),
    sharedMetrics: null
  };
  if (inWindow.length === 0) return empty;

  sortPoints(inWindow);
  const finalPrice = inWindow[inWindow.length - 1].price;
  let fullMax = -Infinity;
  for (const p of inWindow) fullMax = Math.max(fullMax, p.price);

  const last2mStart = windowEndMs - LAST_TWO_MIN_MS;
  const last2m = inWindow.filter((p) => p.ts >= last2mStart);
  const last2mHigh = last2m.length > 0 ? Math.max(...last2m.map((x) => x.price)) : null;
  const last2mLow = last2m.length > 0 ? Math.min(...last2m.map((x) => x.price)) : null;
  const maxDrawdownAbs = last2m.length > 0 ? computeMaxDrawdownAbs(last2m) : null;
  const ctx = {
    inWindow,
    last2m,
    finalPrice,
    fullMax,
    last2mHigh,
    last2mLow,
    maxDrawdownAbs
  };

  const hits = Object.fromEntries(PATTERN_PRIORITY.map((id) => [id, false]));
  const metricsByPattern = Object.fromEntries(PATTERN_PRIORITY.map((id) => [id, null]));
  const state = {
    hits: Object.fromEntries(PATTERN_PRIORITY.map((id) => [id, false]))
  };
  for (const id of enabledPatternIds) {
    const evaluator = PATTERN_EVALUATORS[id];
    if (!evaluator) continue;
    const params = patternConfig.patterns[id]?.params || {};
    const result = evaluator(ctx, params, state);
    hits[id] = Boolean(result?.hit);
    state.hits[id] = hits[id];
    metricsByPattern[id] = result?.metrics || null;
  }

  return {
    hasData: true,
    hits,
    metricsByPattern,
    sharedMetrics: {
      max_price: fullMax,
      final_price: finalPrice,
      last2m_high: last2mHigh,
      last2m_low: last2mLow,
      max_drawdown_abs: maxDrawdownAbs
    }
  };
}

export function newWindow(date, windowId, marketSlug, meta) {
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

export function finalizeWindow(window) {
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

export function analyzeWindows(windows, includeIncomplete, counters = null, patternConfigInput = null) {
  const patternConfig = normalizePatternConfig(patternConfigInput || DEFAULT_PATTERN_CONFIG);
  const enabledPatternIds = PATTERN_PRIORITY.filter((id) => patternConfig?.patterns?.[id]?.enabled);
  const hits = Object.fromEntries(PATTERN_PRIORITY.map((id) => [id, []]));
  const counts = Object.fromEntries(PATTERN_PRIORITY.map((id) => [id, 0]));

  for (const window of windows) {
    if (!includeIncomplete && !window.isComplete) continue;
    if (counters) counters.countedWindows += 1;

    const sideResults = {
      up: evaluateSidePatterns(window.sidePoints.up, window.startMs, window.endMs, patternConfig),
      down: evaluateSidePatterns(window.sidePoints.down, window.startMs, window.endMs, patternConfig)
    };
    const windowHitByPattern = Object.fromEntries(PATTERN_PRIORITY.map((id) => [id, false]));

    for (const side of ["up", "down"]) {
      const result = sideResults[side];
      if (!result.hasData) continue;
      for (const patternId of enabledPatternIds) {
        if (!result.hits[patternId]) continue;
        windowHitByPattern[patternId] = true;
        pushPatternHit(hits[patternId], window, side, result.metricsByPattern[patternId]);
      }
    }

    for (const patternId of enabledPatternIds) {
      if (windowHitByPattern[patternId]) counts[patternId] += 1;
    }
  }

  for (const k of PATTERN_PRIORITY) sortHits(hits[k]);

  return { hits, counts, patternConfig };
}

export function deriveWindowPatternInfo(window, analyzedHits, patternPriority = PATTERN_PRIORITY) {
  const sideHits = Object.fromEntries(PATTERN_PRIORITY.map((id) => [id, []]));
  const windowId = window.windowId || window.marketSlug || "";

  for (const k of patternPriority) {
    for (const h of analyzedHits?.[k] || []) {
      if ((h.window_id || h.market_slug) !== windowId) continue;
      sideHits[k].push({
        side: h.side,
        metrics: h.metrics
      });
    }
  }

  const patterns = patternPriority.filter((k) => sideHits[k].length > 0);
  const patternPrimary = patterns.length > 0 ? patterns[0] : null;
  return {
    patterns,
    patternPrimary,
    patternSideHits: sideHits
  };
}
