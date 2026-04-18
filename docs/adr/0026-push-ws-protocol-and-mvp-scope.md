# ADR-0026: Push WS 协议与 MVP-7 单实例范围

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0004, 0021, 0022, 0025

## 背景 (Context)

ADR-0022 定义了 Push 的长期架构（10 实例 sticky by user_id hash、LB hash 对齐
partition、客户端重连拉快照补齐、coalescing、rate limit 等）。

MVP-7 先把单实例 push 跑起来 —— WS gateway + 两个 Kafka consumer +
per-connection fan-out —— 为后续多实例扩展留好接口。此 ADR 记录：

1. MVP-7 实际做了什么、有意没做什么；
2. WS 协议（客户端订阅/退订/心跳、服务端 data/ack/error 帧）；
3. stream key 命名规则；
4. 认证方案；
5. 消息丢弃策略。

## 决策 (Decision)

### 1. MVP-7 范围

**做**：

- 单 `push` 进程：HTTP server + `/ws` handler + `/healthz`。
- `internal/hub`：per-connection / per-user / per-stream 三张索引，并发安全。
- `internal/ws`：Conn 独立 read goroutine + write goroutine，基于
  [coder/websocket](https://github.com/coder/websocket)。
- `internal/consumer/MarketDataConsumer`：消费 `market-data` 全量，按
  payload 派生 stream key，`hub.BroadcastStream` 发送。
- `internal/consumer/PrivateConsumer`：消费 `counter-journal` 全量，按事件里
  的 `user_id` 字段 `hub.SendUser` 发送。
- X-User-Id 头认证（和 BFF 同款弱 auth）。
- 每连接建立时 **自动订阅** `user` 流（私有事件）。

**不做**（推迟 MVP-7b / 后续）：

- 多实例 sticky LB 路由、partition 子集订阅。MVP-7 单实例消费全量
  counter-journal；多实例时只需改 consumer 配置 + LB hash 对齐。
- 重连状态快照补齐（客户端重连后拉 BFF 补齐，push 只负责"连上之后"的流）。
- K 线/深度 coalescing。
- rate limit / per-connection / per-user 细颗粒度限额（除了 send queue 长度
  这个天然背压）。
- 客户端订阅数量上限、连接总数上限告警。

### 2. WS 协议

**客户端 → 服务端**（JSON text frame）：

```json
{"op":"subscribe",   "streams":["trade@BTC-USDT","depth@BTC-USDT"]}
{"op":"unsubscribe", "streams":["depth@BTC-USDT"]}
{"op":"ping"}
```

**服务端 → 客户端**（JSON text frame）：

```json
// 控制 / ack
{"op":"ack",   "streams":["trade@BTC-USDT"]}
{"op":"pong"}
{"op":"error", "message":"invalid json"}

// 数据帧
{"stream":"trade@BTC-USDT", "data":{ ... protojson ... }}
{"stream":"user",           "data":{ ... protojson ... }}
```

控制帧必有 `op`；数据帧必有 `stream` + `data`。两者互斥。

### 3. stream key 命名

```
trade@<SYMBOL>              公开逐笔
depth@<SYMBOL>              公开 depth 增量
depth.snapshot@<SYMBOL>     公开 depth 全量（周期）
kline@<SYMBOL>:<INTERVAL>   公开 K 线（<INTERVAL>: 1m | 5m | 15m | 1h | 1d）
user                        登录连接私有流（counter-journal 全量）
```

- 大小写：symbol / interval 保持 Match 内部形式；客户端按需 normalize。
- 扩展规则：新公开流走 `<kind>@<symbol>[:<variant>]` 形式。

### 4. 认证

- HTTP upgrade 时读 `X-User-Id` 头。
- 空值 → 接受为匿名连接（能订阅公开流，不订阅 `user` 流）。
- **信任 LB / BFF 认证层**：与 `bff/internal/auth.Middleware` 对齐（MVP 文档注
  明，生产切 JWT 再改）。
- coder/websocket 的 `AcceptOptions.InsecureSkipVerify = true`：MVP 跳过 Origin
  校验，上游 LB 做 TLS 终止和过滤。

### 5. 消息丢弃 vs 关连接

- 每连接有 `SendBuffer=256`（默认）的 outbound queue。
- `hub.BroadcastStream` / `hub.SendUser` 用 **非阻塞 `TrySend`**：队列满则丢
  消息 + `WARN` log，不关连接。
- 和 ADR-0022 §广播队列 对齐：慢消费者不能阻塞其它订阅者。
- 未来若"丢消息导致客户端视图不一致"成为痛点，切换为"满即断开，让客户端重连 +
  补快照"。MVP 保留丢。

### 6. Kafka consumer 语义

- 两个 consumer 独立 group：`push-md-<instance>` / `push-priv-<instance>`。
- `ConsumeResetOffset = AtEnd`：重启后从 topic 尾开始（不回放历史 —— push 无
  状态，历史补齐由客户端拉 BFF 完成）。
- 不提交 offset：重启等价于"从 now 开始订阅"；group lag 指标与 quote 一样需要
  特例处理（监控系统需标注）。

## 备选方案 (Alternatives Considered)

### A. 用 gorilla/websocket

- 优点：生态熟。
- 缺点：API 偏旧、Conn 使用 deadline 而非 context，和我们现有 ctx-first 风格不
  匹配；coder/websocket 更贴近 Go context 模型。
- 结论：选 coder/websocket。

### B. 满队列时关连接（不丢消息）

- 优点：客户端不会看到"视图缺页"。
- 缺点：慢客户端频繁重连，放大后端压力；需要客户端立即有重连 + 快照补齐能力。
- 结论：和 ADR-0022 §广播队列 一致，MVP 先丢；后续按需切。

### C. 私有事件不走 user 流，每个连接自己 subscribe 一次

- 优点：协议更一致（"全部靠 subscribe"）。
- 缺点：每连接都要多一次 subscribe round-trip；私有数据必须交付，不能依赖
  "忘了订阅"的客户端。
- 结论：私有事件对一个用户永远需要送达，隐式订阅更安全。

### D. 每次启动 unique group id（避开多实例 kafka group rebalance）

- 优点：解耦实例间协调。
- 缺点：counter-journal 一条消息会被每个 push 实例各收一次（和 market-data 一
  样），放大私有流量。
- 结论：MVP-7 单实例不考虑；后续多实例走 ADR-0022 方案（按 instance_id 订阅特
  定 partition），群 id 稳定。

## 理由 (Rationale)

- **协议够简单**：JSON + 3 op + 1 broadcast envelope，对客户端零学习成本。
- **stream key 字符串化** 让下游消息可直接路由，不用在 hub 里保存 typed enum。
- **自动订阅 user** 消除用户"忘了 subscribe"导致私有事件漏推的陷阱。
- **丢消息 + log** 优先保护整体稳定性，MVP 不引入复杂的 back-pressure 合约。

## 影响 (Consequences)

### 正面

- 代码量可控：hub（~200 行）+ ws（~200 行）+ 两个 consumer（各 ~100 行）+ main。
- 扩多实例时只需改 `NewPrivate` 的 partition 选择 + 与 LB hash 对齐，不触碰
  hub / ws 协议。
- 端到端测试（[push/internal/ws/ws_test.go](../../push/internal/ws/ws_test.go)）验证
  了 handshake + subscribe + broadcast + 用户自动订阅。

### 负面 / 代价

- 单实例容量上限未压测；预估单机 10 万 WS 够 MVP 用。
- 消息可能丢：**客户端必须能容忍**且在重连后拉快照（MVP 假设客户端做，代码里
  没实现 server-side 重传）。
- market-data group lag 指标无意义（和 quote 一样），监控需特例。

### 中性

- 协议是 JSON，二进制化（msgpack / protobuf-over-ws）作为未来优化点。

## 实施约束 (Implementation Notes)

- hub 用两层 map + `sync.RWMutex`。broadcast 时 `RLock` 只持锁到收集
  target 列表，发送用非阻塞 channel send 在锁外。
- Conn 的 read/write 在两个 goroutine，通过 `send chan []byte` 解耦；
  `TrySend` 是唯一入口，`closeOnce` 保证清理幂等。
- HTTP handler 同步 `c.Start(ctx)`（跑在 handler goroutine 里），net/http
  hijack 语义要求不能提前返回。
- 优雅关停：root ctx cancel → `srv.Shutdown` 给活跃连接 5s grace →
  两个 consumer Close → 等 goroutine 退出。

## 参考 (References)

- ADR-0022: Push 分片与 sticky WS 路由
- ADR-0021 / ADR-0025: Quote 与 market-data
- ADR-0004: counter-journal topic
- 实现：[push/internal/hub/hub.go](../../push/internal/hub/hub.go)、
  [push/internal/ws/](../../push/internal/ws/)、
  [push/internal/consumer/](../../push/internal/consumer/)、
  [push/cmd/push/main.go](../../push/cmd/push/main.go)
