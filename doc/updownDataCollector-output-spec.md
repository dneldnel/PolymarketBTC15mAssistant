# updownDataCollector.js 输出文件与数据格式说明

本文档说明 `src/updownDataCollector.js` 运行后会产生哪些文件、目录分区规则，以及每个 JSONL 文件的数据结构。

## 1. 输出目录结构

默认输出根目录由环境变量控制：

- `COLLECTOR_OUTPUT_DIR`（默认值：`./logs/raw`）

当前目录分区规则：**按 UTC 日期 + market 窗口子目录**。

```text
logs/raw/
  YYYY-MM-DD/
    <window_id>/
      clob_market_ws.jsonl
      updown_state.jsonl
      btc_reference.jsonl
      market_lifecycle.jsonl
      collector_heartbeat.jsonl
```

其中：

- `window_id` 默认使用当前 `market_slug`（例如 `btc-updown-5m-1771427100`）。
- 当 market 尚未解析（启动早期或 refresh error）时，写入：
  - `logs/raw/YYYY-MM-DD/_unassigned/`

> 所有文件均为 **JSONL** 格式（一行一个 JSON 对象）。

## 2. 聚合与落盘策略

### 2.1 `updown_state.jsonl` 聚合

- 默认按 `500ms` 聚合（`COLLECTOR_AGGREGATE_MS=500`）
- 每个 `market_slug + asset_id + bucket` 最多写 1 条（桶内最后快照）
- 关键聚合字段：
  - `bucket_start_ms`
  - `bucket_end_ms`
  - `sample_count`
  - `last_event_time_ms`

可回退为事件级写入：

- 设置 `COLLECTOR_AGGREGATE_MS=0`

### 2.2 `clob_market_ws.jsonl` 原始消息

- 默认关闭（`COLLECTOR_SAVE_CLOB_RAW=false`）
- 仅当 `COLLECTOR_SAVE_CLOB_RAW=true` 时写入

### 2.3 窗口切换

- 5 分钟市场切换时，采集器会先 flush 聚合桶，再切到新 `market_slug` 子目录。
- 因此同一 market 的数据会集中在同一子目录，避免跨目录污染。

## 3. 文件说明与字段格式

## 3.1 `clob_market_ws.jsonl`

用途：保存 CLOB websocket 原始消息（可审计/回放）。

字段：

- `receive_time_ms` `number`：本机接收时间（毫秒）
- `event_type` `string`：消息类型（如 `price_change` / `book` / `last_trade_price`）
- `message` `object`：原始消息对象（完整 payload）

示例：

```json
{"receive_time_ms":1771426530674,"event_type":"price_change","message":{"market":"0x...","price_changes":[{"asset_id":"...","price":"0.54","best_bid":"0.54","best_ask":"0.55"}],"timestamp":"1771426530832","event_type":"price_change"}}
```

## 3.2 `updown_state.jsonl`

用途：保存 up/down 状态快照（聚合后），用于策略/特征分析。

字段：

- `event_type` `string`：状态来源事件类型（如 `price_change` / `book`）
- `market_slug` `string`：当前市场 slug
- `market_id` `string`：当前市场 id
- `asset_id` `string`：token id
- `side` `string`：`up` / `down` / `other`
- `event_time_ms` `number`：桶内最后事件时间（毫秒）
- `receive_time_ms` `number`：桶内最后接收时间（毫秒）
- `best_bid` `number|null`
- `best_ask` `number|null`
- `mid` `number|null`
- `spread` `number|null`
- `last_trade_price` `number|null`
- `bucket_start_ms` `number`：聚合桶起始时间（毫秒）
- `bucket_end_ms` `number`：聚合桶结束时间（毫秒）
- `sample_count` `number`：该桶合并的事件数
- `last_event_time_ms` `number`：该桶最后事件时间（毫秒）

示例：

```json
{"event_type":"price_change","market_slug":"btc-updown-5m-1771427100","market_id":"1389001","asset_id":"5486...","side":"up","event_time_ms":1771427108450,"receive_time_ms":1771427108523,"best_bid":0.54,"best_ask":0.55,"mid":0.545,"spread":0.010000000000000009,"last_trade_price":0.54,"bucket_start_ms":1771427108000,"bucket_end_ms":1771427108500,"sample_count":23,"last_event_time_ms":1771427108450}
```

## 3.3 `btc_reference.jsonl`

用途：保存 Polymarket live ws 的 BTC 参考价。

字段：

- `event_type` `string`：固定为 `btc_tick`
- `source` `string`：固定为 `polymarket_ws`
- `event_time_ms` `number`：行情事件时间（毫秒）
- `receive_time_ms` `number`：本机接收时间（毫秒）
- `price` `number`：BTC 价格

示例：

```json
{"event_type":"btc_tick","source":"polymarket_ws","event_time_ms":1771427109000,"receive_time_ms":1771427109841,"price":67447.23261210248}
```

## 3.4 `market_lifecycle.jsonl`

用途：记录采集器生命周期与市场切换事件。

常见 `event_type`：

- `collector_started`
- `collector_stopped`
- `market_switch`
- `market_refresh_error`

公共字段：

- `event_type` `string`
- `receive_time_ms` `number`
- `market_slug` `string|null`
- `market_id` `string|null`
- `up_token_id` `string|null`
- `down_token_id` `string|null`
- 其他扩展字段（按事件类型）

示例（market_switch）：

```json
{"event_type":"market_switch","receive_time_ms":1771426522099,"market_slug":"btc-updown-5m-1771426500","market_id":"1389012","up_token_id":"5486...","down_token_id":"9953...","prev_market_slug":"btc-updown-5m-1771426200","prev_market_id":"1389009","prev_up_token_id":"4938...","prev_down_token_id":"4648...","next_market_slug":"btc-updown-5m-1771426500","next_market_id":"1389012","next_up_token_id":"5486...","next_down_token_id":"9953..."}
```

## 3.5 `collector_heartbeat.jsonl`

用途：周期性心跳（默认每 5 秒）。

字段：

- `event_type` `string`：固定为 `heartbeat`
- `receive_time_ms` `number`
- `market_slug` `string|null`
- `market_id` `string|null`
- `up_token_id` `string|null`
- `down_token_id` `string|null`
- `btc_polymarket_last` `number|null`

示例：

```json
{"event_type":"heartbeat","receive_time_ms":1771426526621,"market_slug":"btc-updown-5m-1771426500","market_id":"1389012","up_token_id":"5486...","down_token_id":"9953...","btc_polymarket_last":67447.23261210248}
```

## 4. 关键环境变量

- `COLLECTOR_OUTPUT_DIR`：输出根目录（默认 `./logs/raw`）
- `COLLECTOR_PARTITION_BY`：分区策略（默认 `market_slug`）
- `COLLECTOR_UNASSIGNED_DIR`：market 未解析时的子目录名（默认 `_unassigned`）
- `COLLECTOR_MARKET_REFRESH_MS`：市场刷新周期 ms（默认 `1000`）
- `COLLECTOR_HEARTBEAT_MS`：心跳周期 ms（默认 `5000`）
- `COLLECTOR_AGGREGATE_MS`：`updown_state` 聚合窗口 ms（默认 `500`）
- `COLLECTOR_SAVE_CLOB_RAW`：是否保存 `clob_market_ws.jsonl`（默认 `false`）
- `POLYMARKET_CLOB_WS_URL`：CLOB WS 地址（默认 `wss://ws-subscriptions-clob.polymarket.com/ws/market`）

## 5. 时间与分桶口径

- 所有 `*_time_ms` 均为 Unix epoch 毫秒。
- 日期分区目录按 UTC 计算。
- `updown_state` 分桶规则：
  - `bucket_start_ms = floor(event_time_ms / AGG_MS) * AGG_MS`
  - `bucket_end_ms = bucket_start_ms + AGG_MS`

