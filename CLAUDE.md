# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Real-time console trading assistant for Polymarket "Bitcoin Up or Down" 15-minute markets. Combines multiple data sources (Polymarket, Binance, Chainlink), technical indicators, and a probability engine to display live market analysis with trading recommendations.

## Commands

```bash
# Run the assistant
npm start

# Install dependencies
npm install

# Update to latest version
git pull && npm install
```

## Architecture

### Data Layer (`src/data/`)

- **binance.js**: Binance spot price and klines (candlestick data) via REST API
- **binanceWs.js**: Binance WebSocket for real-time trade stream
- **polymarket.js**: Polymarket Gamma API for markets, events, CLOB prices/orderbook
- **polymarketLiveWs.js**: Polymarket live WebSocket for Chainlink BTC/USD feed
- **chainlink.js**: On-chain Chainlink BTC/USD aggregator via Polygon RPC (fallback)
- **chainlinkWs.js**: WebSocket subscription to Polygon RPC for real-time on-chain price

Price source priority: Polymarket WS → Chainlink WS → Chainlink HTTP RPC → Binance (reference)

### Indicators (`src/indicators/`)

- **vwap.js**: Volume-weighted average price (session and series)
- **rsi.js**: Relative Strength Index with SMA and slope
- **macd.js**: MACD (Moving Average Convergence Divergence)
- **heikenAshi.js**: Heiken Ashi candles with consecutive color counting

### Engines (`src/engines/`)

- **regime.js**: Market regime detection (TREND_UP, TREND_DOWN, RANGE, CHOP)
- **probality.js**: Probability scoring from indicators + time-aware decay
- **edge.js**: Edge calculation (model vs market) and trade decision logic

### Entry Point (`src/index.js`)

Main loop:
1. Fetch/subscribe to all data sources in parallel
2. Compute technical indicators
3. Score direction → apply time decay → compute edge → decide trade
4. Render console UI with `readline.cursorTo` + `clearScreenDown`
5. Log signals to `./logs/signals.csv`

### Proxy Support (`src/net/proxy.js`)

Global proxy agent for `fetch` (undici) and WebSocket agents. Reads from `HTTPS_PROXY`, `HTTP_PROXY`, or `ALL_PROXY` environment variables.

## Configuration

All configuration via environment variables (see `src/config.js`):

**Polymarket:**
- `POLYMARKET_AUTO_SELECT_LATEST` (default: `true`)
- `POLYMARKET_SERIES_ID`, `POLYMARKET_SERIES_SLUG`
- `POLYMARKET_SLUG` (pin specific market)
- `POLYMARKET_LIVE_WS_URL`

**Chainlink on Polygon (fallback):**
- `POLYGON_RPC_URL`, `POLYGON_RPC_URLS`
- `POLYGON_WSS_URL`, `POLYGON_WSS_URLS`
- `CHAINLINK_BTC_USD_AGGREGATOR`

**Indicators (CONFIG object):**
- `candleWindowMinutes`: 15
- `vwapSlopeLookbackMinutes`: 5
- `rsiPeriod`: 14, `rsiMaPeriod`: 14
- `macdFast`: 12, `macdSlow`: 26, `macdSignal`: 9

## Module System

- Uses ES modules (`"type": "module"` in package.json)
- Imports use `.js` extensions (required for ESM)
- No build step - runs directly with Node.js 18+

## Key Patterns

- **Stream managers**: WebSocket modules export `start*Stream()` functions with `getLast()` for current state
- **Null safety**: All indicator functions return `null` for insufficient data
- **Time windowing**: Main loop aligns to 15-minute candle windows
- **Price latching**: "PRICE TO BEAT" is latched once per market from the first available Chainlink price after market start
