# 术语表 / Glossary

项目里高频出现、容易混淆、或对 onboarding 不友好的术语。按类别组织。条目格式：**术语** — 定义 + 出处（文件 / ADR）。

## 服务

- **Counter** — 账户真值服务（账户余额 / 订单状态机 / 资金冻结）。按 `user_id` 分 shard。gRPC API：`PlaceOrder` / `CancelOrder` / `QueryOrder` / `Transfer` / `QueryBalance` / `Reserve` / `ReleaseReservation`。
- **Match** — 撮合引擎。按 symbol 分 worker（[ADR-0050](./adr/0050-match-input-topic-per-symbol.md) 后 input topic 也 per-symbol）。消费 `order-event-*`，emit `trade-event`。
- **BFF** — REST + WS 网关。客户端唯一入口，向后转发到 Counter / Trigger gRPC 和 Push WS。包含 market-data cache（[ADR-0038](./adr/0038-bff-reconnect-snapshot.md)）。
- **Push** — 私有 + 公共 WS 推送服务。消费 `counter-journal`（用户私有流）+ `market-data`（公共流），fanout 到连接。
- **Quote** — 市场数据投影。消费 `trade-event`，产出 `PublicTrade` / `Kline` / `DepthSnapshot` / `DepthUpdate`。
- **trade-dump** — MySQL 旁路持久化。消费 `counter-journal` + `trade-event`，落 `accounts` / `orders` / `trades` / `account_logs` 表。
- **history** — 只读 gRPC，按 cursor 分页查 trade-dump 的 MySQL projection。
- **trigger** — 触发单服务（止损 / 止盈 / OCO / Trailing）。订阅 `market-data` 的 PublicTrade，触发时调 `Counter.PlaceOrder`。

## 标识符 / ID

- **`client_order_id`** — 客户端提供的订单幂等键。Counter 按它 dedup PlaceOrder。
- **`order_id`** — 服务端分配的 64-bit Snowflake ID。全局唯一。
- **`trade_id`** — 成交唯一标识（格式 `<symbol>:<seq>`）。
- **`transfer_id`** — 客户端提供的 Transfer 幂等键。Counter 按 per-user ring 去重（现已替代全局 `dedup.Table`）。
- **`reservation_id`** — 资金预留的幂等键。trigger 触发时 PlaceOrder 带它原子消费。
- **`shard_id`** — Counter 分片编号，`[0, total_shards)`。由 `pkg/shard.Index(user_id, totalShards)` 决定。

## Sequence ID（[ADR-0051](./adr/0051-typed-producer-sequence-naming.md)）

按 producer 命名的单调序号。每个 producer 自己持久化 + restart 不回退（[ADR-0048](./adr/0048-snapshot-offset-atomicity.md)）。

- **`counter_seq_id`** — Counter UserSequencer 分配。shard 级，不跨 shard。出现在 `CounterJournalEvent` / `OrderEvent`。
- **`match_seq_id`** — Match SymbolWorker 分配。per-symbol 级，不跨 symbol。出现在 `TradeEvent`。
- **`quote_seq_id`** — Quote 实例分配。出现在 `MarketDataEvent`。
- **`trigger_seq_id`** — trigger 实例分配。出现在 `TriggerUpdate`。
- **`LastMatchSeq`** — Counter 里 per-(user, symbol) 的 match_seq 水位。业务层幂等 guard（[ADR-0048](./adr/0048-snapshot-offset-atomicity.md) §4）。

## 订单状态

8 态内部状态机（[ADR-0020](./adr/0020-order-state-machine.md)），BFF 向外折叠成 6 态。

- **`PENDING_NEW`** — BFF 接受后、Match 未 ack 前
- **`NEW`** — Match 接受后
- **`PARTIALLY_FILLED`** — 部分成交
- **`FILLED`** — 全部成交（终态）
- **`PENDING_CANCEL`** — Counter 发出 cancel 请求、Match 未 ack 前（内部态，BFF 默认映射成 `new`，但 `/v1/order/{id}` 的 `internal_status` 字段会暴露原始值）
- **`CANCELED`** — 取消确认（终态）
- **`REJECTED`** — 拒单（终态）
- **`EXPIRED`** — 过期（终态；TIF=IOC 部成剩余、触发单 TTL 到期等）

**`internal_status` vs `status`**：前者是服务端 8 态原始值（`/v1/order/{id}` 暴露），后者是 BFF 对外折叠的 6 态字符串。

## 撮合概念

- **Maker / Taker** — 挂单方 / 吃单方。
- **TIF（Time In Force）** — `GTC`（挂单至撤）/ `IOC`（立即成交，剩余撤单）/ `FOK`（全成或全撤）。
- **Self-trade / STP** — 同一用户的买卖碰撞。`STPMode`：`None`（允许，默认）/ `RejectTaker`（撤 taker）。自成交结算走 Counter `applySelfTrade` 合并路径（[bugs.md](./bugs.md) 4ccaf23）。
- **OCO（One-Cancels-the-Other）** — N 腿触发单，任一腿 terminal 自动 CANCEL 兄弟。[ADR-0044](./adr/0044-trigger-oco.md)。
- **Trailing stop** — 追踪止损，bps 回撤触发，引擎维护 watermark。[ADR-0045](./adr/0045-trigger-trailing-stop.md)。

## 一致性 / 持久化

- **UserSequencer** — Counter 里 per-user 的 FIFO 执行器（`counter/internal/sequencer`）。同一用户所有写串行化；不同用户并行。
- **SymbolWorker** — Match 里 per-symbol 的 actor（`match/internal/sequencer`）。
- **Sequencer（trigger）** — trigger 里 per-trigger-order 的 FIFO 队列。
- **Snapshot** — 服务内存态的原子落盘。Counter / Match / Quote / Trigger 都有。含 per-partition Kafka offset（[ADR-0048](./adr/0048-snapshot-offset-atomicity.md)）。格式默认 proto（[ADR-0049](./adr/0049-snapshot-protobuf-with-json-debug.md)），可选 JSON debug。
- **Reservation** — 触发前的资金冻结。`Reserve` / `ReleaseReservation` RPC + `PlaceOrder(reservation_id)` 原子消费。只进 snapshot，不进 journal。[ADR-0041](./adr/0041-counter-reservations.md)。
- **Transfer ring** — `engine.Account` 内置 256 位定长环，per-user dedup transfer_id（替代旧的全局 `dedup.Table`）。随 snapshot 持久化。

## 可用性

- **Primary / Standby** — HA 角色。primary 唯一服务请求，standby 等选主（etcd lease）。[ADR-0031](./adr/0031-counter-ha.md)。
- **Cold-standby** — standby 平时不 tail journal；failover 时加载 snapshot + seek Kafka offset 恢复。
- **EOS（Exactly-Once Semantics）** — 通过"transactional producer + snapshot 原子绑 offset + 下游业务层幂等"三件套达成。[ADR-0032](./adr/0032-match-transactional-producer.md) / [ADR-0048](./adr/0048-snapshot-offset-atomicity.md)。
- **Output flush barrier** — snapshot 前先 `FlushAndWait` producer，保证 snapshot 里 offset X+1 == "≤X 的 output 都已 commit 到下游 Kafka"。[ADR-0048](./adr/0048-snapshot-offset-atomicity.md)。

## pkg 常用

- **`pkg/shard.Index(userID, total)`** — xxhash 一致性分片。
- **`pkg/shard.OwnsUser(shardID, total, userID)`** — 分片归属判定。
- **`pkg/election`** — etcd leader election。`Campaign` / `Resign` / `LostCh` / `Observe` / `Leader`。
- **`pkg/dec`** — decimal 算术（`big.Int` 定点）。禁用 float。

---

**增补**：碰到新的易混淆术语就回来加条目。旧条目过时就改，不要留陈旧描述。
