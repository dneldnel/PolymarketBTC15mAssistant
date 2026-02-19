# Polymarket 下单与回传机制实操说明（API + WS）

更新时间：2026-02-19

本文面向你当前项目（`polymarketBTC15mAssistant`）使用的 Polymarket CLOB 接口，整理下单方法与下单后“服务器回传信息机制”（下单成功、是否成交、资金相关）。

## 1. 先区分两类文档域名（避免混淆）

- `docs.polymarket.com`：当前主站开发文档（CLOB 客户端、REST、WS）。
- `docs.polymarket.us`：美国站文档，接口栈与主站不完全一致。

本文件以下内容以 `docs.polymarket.com` 为主。

## 2. 交易链路全景（你要实现的服务端视角）

1. 准备账户认证参数（L1 签名、L2 Header）。
2. 先构造订单（`POST /order`，Create Order）拿到签名订单对象。
3. 提交订单（`POST /order`，Place Order）拿到服务端接收结果（如 `SUCCESS`）。
4. 订阅用户 WS（User Channel）接收订单状态/成交推送（低延迟）。
5. 用 REST 补全与对账（`GET /data/order/{id}`、`GET /data/orders`、`GET /data/trades`）。
6. 资金侧通过余额与授权相关接口做预检查与事后核对（如 `getBalanceAllowance` / `updateBalanceAllowance`）。

建议：WS 负责“快”，REST 负责“准”（补偿、幂等、重放）。

## 3. 下单相关 REST：你关心的“成功/失败”定义

## 3.1 构造订单（Create Order）

官方客户端文档里先调用 `createOrder(...)`，返回可提交的签名订单对象，再调用 `postOrder(...)`。

关键字段（示例）：
- `tokenID`
- `price`
- `size`
- `side`（`BUY` / `SELL`）

## 3.2 提交订单（Place Order）

文档示例：`POST /order`（不同步骤都叫 `/order`，但语义分为“构造/提交”）。

你需要在服务端记录以下内容：
- 提交请求的本地 `request_id`（你自己生成，幂等用）
- 返回的 `order_id`（或等价标识）
- 提交时刻（毫秒）
- 原始返回体（用于审计）

“下单成功”在工程上应定义为两层：
- 层 A：HTTP 成功 + 返回 `SUCCESS`（订单被撮合系统接受）
- 层 B：订单进入可追踪状态（WS 或 REST 可查到该订单）

只有层 A 不代表已成交。

## 3.3 批量下单返回中的状态语义（非常关键）

Polymarket 文档对批量下单结果给出了状态枚举：
- `matched`：完全成交
- `live`：已挂单（未完全成交）
- `delayed`：延迟状态
- `unmatched`：未匹配

即便不是批量接口，这组语义也可以直接作为你的通用订单状态字典，统一内部状态机。

## 4. 下单后的“服务器回传”：WS 与 REST 如何配合

## 4.1 WS 入口与鉴权

- 市场频道（Market Channel）WS：
  - `wss://ws-subscriptions-clob.polymarket.com/ws/market`
- 用户频道（User Channel）WS：
  - `wss://ws-subscriptions-clob.polymarket.com/ws/user`
- 鉴权：连接时通过 URL 参数传入 `auth`（文档写明 `wss-authentication` 机制）。

## 4.2 User Channel（你最需要）

User Channel 订阅请求示例中核心字段：
- `type`: `"USER"`
- `markets`: `["<condition_id>", ...]`
- `asset_ids`: `["<token_id>", ...]`
- `auth`: `{ apiKey, secret, passphrase }`

说明：
- 用户频道会推送该用户在订阅市场/资产上的订单与交易相关事件。
- 这是“订单是否成交、是否还在挂单”的实时第一信号。

## 4.3 Market Channel（辅助）

Market Channel 订阅请求核心字段：
- `type`: `"MARKET"`
- `assets_ids`: `["token_id_1", "token_id_2"]`

它主要给你盘口/市场维度更新，不是账户级成交确认主通道。

## 4.4 推荐的事件处理顺序

1. 先写本地 `PENDING_SUBMIT`。
2. `POST /order` 返回成功后改为 `SUBMITTED`。
3. 收到 User WS 的订单/成交事件后更新为：
   - `LIVE`（挂单中）
   - `PARTIALLY_FILLED`（部分成交）
   - `FILLED`（完全成交）
   - `CANCELED`（已撤）
4. 周期性用 REST 纠偏（防 WS 丢包）。

## 5. “成交与否”应该怎么判定

不要用单一信号判定，建议三信号合并：

- 信号 1：User WS 的订单状态变化（实时）
- 信号 2：`GET /data/order/{id}` 查询订单当前状态（权威）
- 信号 3：`GET /data/trades` 是否出现该订单对应成交记录（结算依据）

工程判定建议：
- `FILLED`：订单剩余数量为 0，且/或 trades 显示全部数量成交。
- `PARTIAL`：有 trades，但订单仍在 `live`。
- `NOT_FILLED`：订单存在但无成交记录且最终取消或过期。

## 6. 资金信息（余额、可用额度、授权）怎么接

Polymarket CLOB 客户端文档提供资金相关方法（示例）：
- `getBalanceAllowance(...)`
- `updateBalanceAllowance(...)`

实践中应维护三类资金值（即使接口字段命名可能不同）：
- `total_balance`：总余额
- `available_balance`：可用余额
- `locked_balance`：挂单占用/冻结

下单前检查：
1. 可用余额是否足够（含手续费/滑点缓冲）。
2. 对应 token 的 allowance/授权是否到位。

下单后检查：
1. 若状态 `live`，冻结余额应上升。
2. 若成交，持仓与现金余额应同步变化。
3. 若取消/过期，冻结余额应释放。

## 7. 推荐的本地订单状态机（可直接实现）

建议统一状态枚举：
- `PENDING_SUBMIT`
- `SUBMITTED`
- `LIVE`
- `PARTIALLY_FILLED`
- `FILLED`
- `CANCELED`
- `REJECTED`
- `EXPIRED`

状态迁移（简化）：
- `PENDING_SUBMIT -> SUBMITTED -> LIVE`
- `LIVE -> PARTIALLY_FILLED -> FILLED`
- `LIVE/PARTIALLY_FILLED -> CANCELED`
- `SUBMITTED -> REJECTED`

额外字段建议：
- `client_order_id`（本地幂等键）
- `exchange_order_id`
- `submitted_at`, `last_event_at`
- `filled_size`, `remaining_size`, `avg_fill_price`
- `raw_last_ws_event`, `raw_last_rest_snapshot`

## 8. 你这个项目的落地建议

结合现有代码（你已接了市场和价格流）：

- 在 `src/data/` 增加一个 `polymarketUserWs.js`：
  - 专门订阅 User Channel。
  - 统一把事件落地到内存状态 + 可选日志文件。
- 在下单模块加“回查任务”：
  - 下单后 `T+1s/T+3s/T+10s` 调 `GET /data/order/{id}`。
  - 若 WS 迟到或丢失，用 REST 状态覆盖。
- 资金模块独立：
  - 下单前调用余额/授权接口预检。
  - 每笔订单结束后做一次资金对账记录。

## 9. 关键风险与规避

- 仅靠 HTTP `SUCCESS` 就当作成交：错误。
- 仅靠 WS 不做 REST 补偿：有丢包风险。
- 不做幂等键：重试时可能重复下单。
- 不记录原始回包：排障困难。

## 10. 参考链接（官方/一手）

- Polymarket 主文档入口  
  https://docs.polymarket.com/
- CLOB API Quickstart（含下单示例）  
  https://docs.polymarket.com/developers/CLOB/quickstart
- CLOB 客户端（含 `createOrder` / `postOrder` / 资金方法示例）  
  https://docs.polymarket.com/developers/CLOB/clients
- CLOB WS 订阅总览  
  https://docs.polymarket.com/developers/CLOB/websocket
- CLOB User Channel（用户事件）  
  https://docs.polymarket.com/developers/CLOB/websocket/user-channel
- CLOB Market Channel（市场事件）  
  https://docs.polymarket.com/developers/CLOB/websocket/market-channel
- CLOB WS 鉴权  
  https://docs.polymarket.com/developers/CLOB/websocket/wss-authentication
- API Reference: Create Order  
  https://docs.polymarket.com/developers/CLOB/orders/create-order
- API Reference: Place Order  
  https://docs.polymarket.com/developers/CLOB/orders/place-order
- API Reference: Get Active Orders  
  https://docs.polymarket.com/developers/CLOB/orders/get-active-order
- API Reference: Get Order  
  https://docs.polymarket.com/developers/CLOB/orders/get-order
- API Reference: Get Trades  
  https://docs.polymarket.com/developers/CLOB/orders/get-trades
- API Reference: Place Multiple Orders（状态枚举）  
  https://docs.polymarket.com/developers/CLOB/orders/place-multiple-orders

