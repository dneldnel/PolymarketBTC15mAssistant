#!/usr/bin/env node
/**
 * Test script to parse "PRICE TO BEAT" from Polymarket market objects
 *
 * Usage:
 *   node test_price_to_beat.js <event-slug>
 *   node test_price_to_beat.js btc-updown-15m-1771172100
 *
 * The script will:
 * 1. Fetch the market data from Polymarket Gamma API
 * 2. Parse the "price to beat" using multiple methods
 * 3. Return the found value
 */

import { ProxyAgent, setGlobalDispatcher } from "undici";
import fs from "fs";

const GAMMA_BASE_URL = "https://gamma-api.polymarket.com";

// ============================================
// Proxy Setup
// ============================================

function setupProxy() {
  const readEnv = (k) => process.env[k] ?? "";
  const all = readEnv("ALL_PROXY") || readEnv("all_proxy");
  const https = readEnv("HTTPS_PROXY") || readEnv("https_proxy");
  const http = readEnv("HTTP_PROXY") || readEnv("http_proxy");
  const proxyUrl = all || https || http;

  if (!proxyUrl) return { success: false, message: "No proxy configured" };

  const isSocks = proxyUrl.toLowerCase().startsWith("socks");
  if (isSocks) {
    return {
      success: false,
      message: `SOCKS proxy detected (not supported by undici fetch)`,
      hint: `Set HTTP_PROXY instead (e.g., export HTTP_PROXY=http://127.0.0.1:7890)`
    };
  }

  try {
    setGlobalDispatcher(new ProxyAgent(proxyUrl));
    return {
      success: true,
      message: `Using HTTP proxy: ${proxyUrl.replace(/:\/[^@]+@/, "://***@")}`
    };
  } catch (e) {
    return {
      success: false,
      message: `Proxy setup failed: ${e.message}`
    };
  }
}

// ============================================
// Parsing Functions
// ============================================

function parsePriceToBeatFromText(market) {
  const text = String(market?.question ?? market?.title ?? "");
  if (!text) return null;

  // Match "price to beat: $XXX" pattern in the text
  const m = text.match(/price\s*to\s*beat[^\d$]*\$?\s*([0-9][0-9,]*(?:\.[0-9]+)?)/i);
  if (!m) return null;

  const raw = m[1].replace(/,/g, "");
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

function parsePriceToBeatFromMetadata(market) {
  const directKeys = [
    "priceToBeat",
    "price_to_beat",
    "strikePrice",
    "strike_price",
    "strike",
    "threshold",
    "thresholdPrice",
    "threshold_price",
    "targetPrice",
    "target_price",
    "referencePrice",
    "reference_price"
  ];

  for (const k of directKeys) {
    const v = market?.[k];
    const n = typeof v === "string" ? Number(v) : typeof v === "number" ? v : NaN;
    if (Number.isFinite(n)) return { value: n, key: k };
  }

  return null;
}

function deepSearchPriceToBeat(market, maxDepth = 6) {
  const seen = new Set();
  const stack = [{ obj: market, depth: 0 }];
  const targetPatterns = [
    /^priceToBeat$/i,
    /^price_to_beat$/i,
    /^strikePrice$/i,
    /^strike_price$/i,
    /^strike$/i,
    /^threshold$/i,
    /^thresholdPrice$/i,
    /^threshold_price$/i,
    /^targetPrice$/i,
    /^target_price$/i,
    /^referencePrice$/i,
    /^reference_price$/i
  ];

  while (stack.length) {
    const { obj, depth } = stack.pop();
    if (!obj || typeof obj !== "object") continue;
    if (seen.has(obj) || depth > maxDepth) continue;
    seen.add(obj);

    const entries = Array.isArray(obj) ? obj.entries() : Object.entries(obj);
    for (const [key, value] of entries) {
      for (const pattern of targetPatterns) {
        if (pattern.test(key)) {
          const n = typeof value === "string" ? Number(value) : typeof value === "number" ? value : NaN;
          if (Number.isFinite(n)) {
            return { value: n, key, path: `[depth ${depth}] ${key}` };
          }
        }
      }
      if (value && typeof value === "object") {
        stack.push({ obj: value, depth: depth + 1 });
      }
    }
  }

  return null;
}

function parsePriceToBeat(market) {
  const results = [];

  // Method 1: Regex from question/title text
  const textResult = parsePriceToBeatFromText(market);
  if (textResult !== null) {
    results.push({ method: "regex", value: textResult, source: "question text" });
  }

  // Method 2: Direct metadata keys
  const metaResult = parsePriceToBeatFromMetadata(market);
  if (metaResult !== null) {
    results.push({ method: "metadata", value: metaResult.value, source: `key: ${metaResult.key}` });
  }

  // Method 3: Deep search
  const deepResult = deepSearchPriceToBeat(market);
  if (deepResult !== null) {
    results.push({ method: "deep_search", value: deepResult.value, source: deepResult.path });
  }

  return results;
}

// ============================================
// API Functions
// ============================================

async function fetchMarketBySlug(slug) {
  const url = new URL("/markets", GAMMA_BASE_URL);
  url.searchParams.set("slug", slug);

  const res = await fetch(url);
  if (!res.ok) {
    throw new Error(`Gamma API error: ${res.status} ${await res.text()}`);
  }

  const data = await res.json();
  return Array.isArray(data) ? data[0] : data;
}

// ============================================
// Main Function
// ============================================

async function main() {
  const args = process.argv.slice(2);

  if (args.length === 0 || args[0] === "--help" || args[0] === "-h") {
    console.log(`
Usage: node test_price_to_beat.js <event-slug>

Example:
  node test_price_to_beat.js btc-updown-15m-1771172100

Options:
  --file <path.json>    Load market data from a JSON file instead of fetching
  --help, -h            Show this help message

Environment:
  HTTP_PROXY            HTTP proxy URL (e.g., http://127.0.0.1:7890)
  HTTPS_PROXY           HTTPS proxy URL (takes precedence over HTTP_PROXY)
  ALL_PROXY             Fallback proxy URL

Note: SOCKS proxies are not supported for fetch. Use HTTP proxy instead.
`);
    process.exit(0);
  }

  // Check for --file option
  if (args[0] === "--file" && args[1]) {
    const filePath = args[1];
    console.log(`📂 Loading market data from: ${filePath}\n`);
    try {
      const json = fs.readFileSync(filePath, "utf-8");
      const market = JSON.parse(json);
      return { market, source: "file" };
    } catch (e) {
      console.error(`❌ Failed to load file: ${e.message}`);
      process.exit(1);
    }
  }

  const eventSlug = args[0];
  console.log(`📡 Fetching market data for: ${eventSlug}\n`);

  // Setup proxy
  const proxyResult = setupProxy();
  if (proxyResult.success) {
    console.log(`🔧 ${proxyResult.message}\n`);
  } else {
    console.log(`⚠️  ${proxyResult.message}`);
    if (proxyResult.hint) {
      console.log(`💡 ${proxyResult.hint}`);
    }
    console.log();
  }

  try {
    const market = await fetchMarketBySlug(eventSlug);
    return { market, source: "api" };
  } catch (e) {
    console.error(`❌ Failed to fetch market: ${e.message}`);
    console.log(`
If you're in China or behind a firewall, you may need a proxy:

1. Set up an HTTP proxy (e.g., Clash, V2Ray)
2. Export the proxy variable:
   export HTTP_PROXY=http://127.0.0.1:7890
   # or on Windows PowerShell:
   # $env:HTTP_PROXY = "http://127.0.0.1:7890"

3. Run the script again:
   node test_price_to_beat.js ${eventSlug}

Or use a saved JSON file:
   node test_price_to_beat.js --file <path-to-market.json>
`);
    process.exit(1);
  }
}

// ============================================
// Run
// ============================================

main().then(({ market, source }) => {
  console.log("=" .repeat(60));
  console.log("MARKET INFO");
  console.log("=".repeat(60));
  console.log(`ID:        ${market.id}`);
  console.log(`Slug:      ${market.slug}`);
  console.log(`Question:  ${market.question?.substring(0, 100)}${market.question?.length > 100 ? "..." : ""}`);
  console.log(`End Date:  ${market.endDate}`);
  console.log();

  console.log("=".repeat(60));
  console.log("PARSING 'PRICE TO BEAT'");
  console.log("=".repeat(60));
  console.log();

  const results = parsePriceToBeat(market);

  if (results.length === 0) {
    console.log("❌ Could not parse 'price to beat' from market object");
    console.log();
    console.log("This is expected behavior - Polymarket market objects don't");
    console.log("typically contain the 'price to beat' value. In the actual");
    console.log("application, this value is latched from the Chainlink BTC/USD");
    console.log("price feed when the market starts.");
    process.exit(0);
  }

  // Display all results
  results.forEach((r, i) => {
    console.log(`${i + 1}. Method: ${r.method.toUpperCase()}`);
    console.log(`   Value:  $${r.value.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`);
    console.log(`   Source: ${r.source}`);
    console.log();
  });

  // Return the first found value (priority: regex > metadata > deep search)
  const firstResult = results[0];
  console.log("=".repeat(60));
  console.log("RESULT");
  console.log("=".repeat(60));
  console.log(`Price to beat: $${firstResult.value.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`);
  console.log();

}).catch(err => {
  console.error(err);
  process.exit(1);
});
