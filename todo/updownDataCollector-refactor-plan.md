# `updownDataCollector.js` 低风险渐进式重构方案（不改功能、提升性能）

## Summary
目标文件是 `src/updownDataCollector.js`（你说的 `updatedata`）。  
本方案采用“两阶段（低风险）”路线：先做**行为等价**的性能重构，再做结构拆分。  
核心结论：当前最主要性能瓶颈是高频路径上的同步磁盘写入（`appendFileSync`），其次是主流程耦合度高（`main()` 过大，WS/聚合/落盘/面板混在一起）。

## Success Criteria（验收标准）
1. 所有现有功能保持一致：
1. 5m/15m 市场自动切换逻辑不变。
2. CLOB 订阅与修复机制不变（`no_data_after_switch`、`stale_stream`、重连计数/原因）。
3. 现有 JSONL 文件字段与语义不变（除非新增可选内部诊断字段，不影响下游）。
2. 性能改进可量化：
1. 高频消息路径不再调用同步文件写。
2. 10 分钟运行中，前台刷新无明显卡顿（事件循环阻塞显著下降）。
3. 同时段记录量与旧版本同量级（不出现异常丢记录）。

## Important Changes / Interfaces
1. 对外行为：
1. 默认输出目录结构、文件名、JSON 字段全部保持兼容。
2. 入口命令保持不变（仍运行同一个 collector 脚本）。
2. 新增可选环境变量（仅用于性能调优/回滚，不影响默认行为）：
1. `COLLECTOR_IO_FLUSH_MS`，默认 `50`。
2. `COLLECTOR_IO_MAX_BATCH_BYTES`，默认 `262144`。
3. `COLLECTOR_IO_MAX_PENDING_LINES`，默认 `50000`。
4. `COLLECTOR_IO_FORCE_SYNC`，默认 `false`（设为 `true` 时回退旧同步写法，便于紧急止损）。
3. 内部模块拆分（第二阶段）：
1. `src/updownCollector/io/jsonlWriteQueue.js`
2. `src/updownCollector/io/windowPartition.js`
3. `src/updownCollector/aggregation/updownAggregator.js`
4. `src/updownCollector/ui/statusScreen.js`
5. `src/updownDataCollector.js` 保留为编排入口。

## Phase 1（先做，低风险性能改造）
1. 写盘层替换为异步批量队列（行为等价）：
1. 用 `WriteStream` + 内存队列替代 `appendFileSync`。
2. 保证“同一文件内顺序不变”。
3. 每 `flush_ms` 批量写入，减少 syscall 频率。
4. 进程退出、市场切换前执行 `flushAll()`，避免尾部丢数据。
2. 计数与前台显示保持一致：
1. 行计数在入队成功时递增，面板仍每秒显示各文件计数。
2. 首次遇到历史文件时只初始化一次计数，避免反复全量读。
3. 回滚保险：
1. 写队列异常时写 `market_lifecycle` 事件（例如 `io_writer_error`）。
2. 自动切 `COLLECTOR_IO_FORCE_SYNC` 路径继续写，优先保证不丢数据。
4. 清理无用代码：
1. 删除未使用函数 `writeDailyJsonl`（仅内部清理，不影响行为）。

## Phase 2（结构简化，保持逻辑不变）
1. 拆分职责但不改算法：
1. `windowPartition` 负责 `day/window` 路径解析与 `_unassigned` 兜底。
2. `updownAggregator` 负责 `persistState`、`flushAggregation`、500ms 分桶逻辑。
3. `statusScreen` 负责面板文案和渲染输入整形。
2. `main()` 只做编排：
1. 初始化配置与状态。
2. 连接 BTC WS + CLOB WS。
3. 启动 `market/heartbeat/flush/screen` 定时器。
4. 管理 shutdown 顺序（先停 timer，再 flush，再 close stream）。
3. 严格保持关键时序不变：
1. `market_switch` 前先 `flushAggregation(forceAll=true)`。
2. 切换后 `clearState + subscribeAssets(..., forceSend:true)`。
3. `maybeRepairClobStream` 判定条件与触发频率保持一致。

## Test Cases / Scenarios
1. 功能回归：
1. 启动后 1-3 秒内 `btc_reference.jsonl` 持续增长。
2. `updown_state.jsonl` 继续按聚合桶落盘（`sample_count`、`bucket_*` 正常）。
3. `ptb_reference.jsonl` 边界更新正常，面板字段正常刷新。
2. 切换场景：
1. 跨下一个 5 分钟窗口后，`up/down` 持续有数据，不断流。
2. 旧窗口聚合桶不写入新窗口目录。
3. 可靠性场景：
1. 主动断网/断 WS 后自动重连，重连计数和最近原因更新。
2. `no_data_after_switch` 与 `stale_stream` 修复机制仍触发。
4. 性能场景：
1. 10 分钟运行期间，前台刷新平滑，无明显阻塞。
2. 同时段总记录量与旧版本接近（允许小幅自然波动）。
3. 观察队列 backlog 不持续攀升（无内存泄漏迹象）。

## Assumptions / Defaults
1. “不影响现有功能”定义为：输出文件结构、字段、切换与修复逻辑、面板关键状态保持兼容。
2. 允许写盘从“即时同步”改为“几十毫秒级异步批量”，以换取性能提升。
3. 默认启用异步批量写；若线上异常，可通过 `COLLECTOR_IO_FORCE_SYNC=true` 一键回退。
4. 本次不改 `src/index.js`，只聚焦 collector 侧。
