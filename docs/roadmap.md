# OpenTrade MVP Roadmap

> 任务进度表，和 [architecture.md](./architecture.md)（长期架构）+ [adr/](./adr/README.md)（决策记录）互补。
> architecture.md 描述"应该长什么样"，ADR 描述"为什么这么做"，本表描述"做到了哪一步、下一步做什么"。
>
> 每个 MVP 的完成标志：**单元测试 + （必要时）集成测试 + commit + ADR**。

## 一览

| MVP | 主题 | 状态 | 说明 |
|---|---|---|---|
| [MVP-0](#mvp-0-scaffold) | Scaffold | ✅ | monorepo / go.work / 22 个 ADR |
| [MVP-1](#mvp-1-match) | Match 撮合 | ✅ | orderbook / engine / sequencer / kafka / snapshot |
| [MVP-2](#mvp-2-counter) | Counter 账户 | ✅ | accounts / transfer / journal / dedup / snapshot |
| [MVP-3](#mvp-3-counter-match) | Counter↔Match | ✅ | 订单、Kafka 事务双写、成交清算 |
| [MVP-4](#mvp-4-bff-rest) | BFF REST | ✅ | REST gateway / auth / rate-limit |
| [MVP-5](#mvp-5-trade-dump) | trade-dump（trades 单表） | ✅ | Kafka → MySQL 幂等落库 |
| [MVP-6](#mvp-6-quote) | Quote 行情 | ✅ | PublicTrade / Kline / Depth → market-data |
| [MVP-7](#mvp-7-push) | Push WS 单实例 | ✅ | WS gateway + market-data / counter-journal fan-out |
| [MVP-8](#mvp-8-counter-sharding) | Counter 10-shard | ✅ | `pkg/shard` + BFF 路由 + ownership guard |
| [MVP-9](#mvp-9-trade-dump-projections) | trade-dump 其它表 | ✅ | orders / account_logs / accounts projection |
| [MVP-10](#mvp-10-bff-ws) | BFF WebSocket 网关 | ✅ | 客户端走 BFF 而非直连 push |
| [MVP-11](#mvp-11-match-sharding) | Match 多实例 + symbol 迁移 | ✅ | etcd 配置驱动 + 热加减 symbol |
| [MVP-12](#mvp-12-ha) | Counter/Match HA | ✅ | etcd lease 选主 + cold standby |
| [MVP-13](#mvp-13-push-sharding) | Push 多实例 sticky | ⏳ 计划中 | LB hash 与 partition 订阅对齐 |

> 顺序原则：**最小依赖先行**。HA（12）晚于 sharding（8/11），因为 HA 实现依赖多实例拓扑成型。Sharding（8）早于 BFF WS（10），因为 BFF WS 本质上是"把 push 那套协议代理一遍"，在 push 协议稳定后做更省力。

## 填坑 / Backlog

从已合 MVP 里明确推迟的项，没有单独升到 MVP 级，但都要做：

- **K 线 gap 填充** — 两笔 trade 跨多个 interval 时填 empty bar（[ADR-0025 未来工作](./adr/0025-quote-engine-state-and-offset-strategy.md)）
- **Push coalescing / rate-limit** — K 线 / 深度合并、连接级消息速率限制（[ADR-0022 实施约束](./adr/0022-push-sharding-sticky-routing.md) / [ADR-0026](./adr/0026-push-ws-protocol-and-mvp-scope.md)）
- **Push 重连快照补齐** — 客户端重连后拉 BFF 补齐期间遗漏（[ADR-0026](./adr/0026-push-ws-protocol-and-mvp-scope.md)）
- **Counter 事务双写 review** — 过一遍 `counter/internal/journal/txn_producer.go` 确认真的是 Kafka transactional（[ADR-0005](./adr/0005-kafka-transactions-for-dual-writes.md)）
- **集成冒烟脚本** — `docker-compose up` 起全部依赖 + 下一单端到端 smoke（[architecture.md §18.2](./architecture.md)）
- **Counter 对账** — 每小时余额对比内存 vs MySQL 聚合（[ADR-0008 实施约束](./adr/0008-sidecar-persistence-trade-dump.md)）
- **Quote state snapshot** — 重启冷启动改从 snapshot + 增量（[ADR-0025 未来工作](./adr/0025-quote-engine-state-and-offset-strategy.md)）
- **Counter re-shard 工具** — 未来扩容 10→20 shard 用（[ADR-0010](./adr/0010-counter-sharding-by-userid.md)、[ADR-0027](./adr/0027-counter-sharding-rollout.md)）
- **trade-event consumer 的显式 shard filter** — 目前靠 `Orders().Get` miss 隐式过滤（[ADR-0027 备选方案 D](./adr/0027-counter-sharding-rollout.md)）
- **BFF auth 升级到 JWT / API-Key** — 目前 `X-User-Id` 弱 auth（[bff/internal/auth](../bff/internal/auth)）
- **Match / Counter 延迟 + 吞吐 benchmark** — 验证是否接近 20w TPS / 10ms P99（[architecture.md §18.3](./architecture.md)）

## 已完成

### MVP-0  Scaffold  {#mvp-0-scaffold}

- **commit** [`5efe906`](../) — scaffold monorepo
- monorepo + `go.work` 管 8 个 module；proto 定义 + 生成；docker-compose 本地依赖（kafka / etcd / mysql / minio）
- 22 个初始 ADR

### MVP-1  Match  {#mvp-1-match}

- **commit** [`8279f51`](../)
- per-symbol 单线程 worker（[ADR-0016](./adr/0016-per-symbol-single-thread-matching.md) / [ADR-0019](./adr/0019-match-sequencer-per-symbol-actor.md)）
- orderbook（btree 价格档位）+ engine + snapshot
- Kafka producer / consumer 骨架

### MVP-2  Counter（独立账户）  {#mvp-2-counter}

- **commit** [`8382a59`](../)
- accounts / transfer / journal / dedup / per-user sequencer（[ADR-0018](./adr/0018-counter-sequencer-fifo.md)）
- snapshot + 重启恢复

### MVP-3  Counter ↔ Match  {#mvp-3-counter-match}

- **commit** [`4a2d129`](../)
- PlaceOrder / CancelOrder（Kafka 事务写 counter-journal + order-event，[ADR-0005](./adr/0005-kafka-transactions-for-dual-writes.md)）
- 消费 trade-event → settlement → 写 journal

### MVP-4  BFF REST  {#mvp-4-bff-rest}

- **commit** [`5a73211`](../)
- REST：`/v1/order`（POST/DELETE/GET）、`/v1/transfer`、`/v1/account`、`/healthz`
- 滑动窗口限流（按 user / IP）
- `X-User-Id` 弱 auth middleware

### MVP-5  trade-dump（trades 单表）  {#mvp-5-trade-dump}

- **commit** [`63515de`](../) · **ADR** [0023](./adr/0023-trade-dump-batching-and-commit-order.md)
- 消费 `trade-event` → 投影 `trades` 表
- 单 tx 多行 `INSERT ... ON DUPLICATE KEY UPDATE`；commit 顺序：MySQL 成功后再 commit Kafka offset
- `orders` / `account_logs` / `accounts` 表留给 MVP-9

### MVP-6  Quote  {#mvp-6-quote}

- **commit** [`ccac3cc`](../) · **ADR** [0024](./adr/0024-trade-event-order-accepted-extension.md)、[0025](./adr/0025-quote-engine-state-and-offset-strategy.md)
- `trade-event.OrderAccepted` 扩展 `side / price / remaining_qty`（wire-compat）
- `market-data` 新 topic：`PublicTrade` / `KlineUpdate` / `KlineClosed` / `DepthUpdate` / `DepthSnapshot`
- per-symbol orderbook projection + 5 interval OHLCV 聚合
- 重启从 trade-event earliest 重扫，不提交 offset（内存态纯函数派生）

### MVP-7  Push WS 单实例  {#mvp-7-push}

- **commit** [`836fd3c`](../) · **ADR** [0026](./adr/0026-push-ws-protocol-and-mvp-scope.md)
- `coder/websocket` 单进程 WS gateway
- hub（per-conn / per-user / per-stream 三张索引），慢连接丢消息 + log
- 两个 consumer：market-data 公开 fan-out + counter-journal 按 user_id 私发
- 隐式订阅 `user` 流；JSON 协议 `{"op":"subscribe","streams":[...]}`

### MVP-8  Counter 10-shard  {#mvp-8-counter-sharding}

- **commit** [`fb127ef`](../) · **ADR** [0027](./adr/0027-counter-sharding-rollout.md)
- 新 `pkg/shard`（xxhash64，稳定冻结值）
- BFF `ShardedCounter` 实现 `Counter` 接口，按 `user_id` 路由到 N 个 shard client
- Counter `service.Config.TotalShards` + `OwnsUser` guard；`ErrWrongShard` → gRPC `FailedPrecondition`

### MVP-9  trade-dump 其它表  {#mvp-9-trade-dump-projections}

- **commit** pending · **ADR** [0028](./adr/0028-trade-dump-journal-projection.md)
- 新增 journal consumer（与 trade-event consumer 并行）消费 `counter-journal`
- 投影到 `orders` / `accounts` / `account_logs`；单 tx 三表写入；`accounts` 带 seq_id guard 防乱序
- schema 调整：`account_logs` PK `(shard_id, seq_id, asset)`（SettlementEvent 一事件两 asset 行）
- shard_id 从 `EventMeta.producer_id`（`counter-shard-N-main`）parse 而来

### MVP-10  BFF WebSocket 网关  {#mvp-10-bff-ws}

- **commit** pending · **ADR** [0029](./adr/0029-bff-ws-reverse-proxy.md)
- BFF `/ws` 接受客户端连接 → 对上游 push 发起独立 WS，双向透传帧
- auth 在 handshake 层（`bff/internal/auth.Middleware` 读 `X-User-Id`），转发时 re-inject 到上游
- 单 upstream（`--push-ws=ws://push:8081/ws`）；多实例 sticky 留给 MVP-13
- BFF 不解析载荷，对 push 协议变化免疫

## 计划中

### MVP-11  Match 多实例 + symbol 迁移  {#mvp-11-match-sharding}

- **commit** pending · **ADR** [0030](./adr/0030-match-etcd-sharding-rollout.md)
- 新 `pkg/etcdcfg`：`Source` 接口 + `EtcdSource`（clientv3）+ `MemorySource`（测试）；List → revision → Watch
- 新 `match/internal/registry`：线程安全的 symbol 生命周期管理（Factory + Restore + Snapshot 回调 + Dispatcher 注册/注销）
- match main 重写：etcd 启动 List + 后续 Watch 驱动 AddSymbol/RemoveSymbol；`--etcd` 空时 fallback 到 `--symbols` 静态模式
- ADR-0009 里的"trading:false 在线撤单"简化为"drain + final snapshot"；operator 配合先停新单

### MVP-12  Counter / Match HA  {#mvp-12-ha}

- **commit** pending · **ADR** [0031](./adr/0031-ha-cold-standby-rollout.md)
- 新 `pkg/election`：etcd `concurrency.Election` 封装（Campaign / Resign / LostCh / Observe）
- Counter / Match main 分出 `runPrimary`，外层 `runElectionLoop` cold-standby
- `--ha-mode=auto` 走选举；`disabled` 保留单机模式（向后兼容）
- Counter fencing 自然走 Kafka `TransactionalID`（ADR-0017）；Match 暂依赖 etcd lease + 自觉退出（split-brain 窗口 10s，明确留 MVP-12b）
- backup 不消费 Kafka、不打快照；snapshot 共享目录 MVP 假设本地 EFS/NFS mount

### MVP-13  Push 多实例 sticky  {#mvp-13-push-sharding}

- **范围**：
  - LB 按 `user_id` hash sticky（复用 `pkg/shard`）
  - `counter-journal` consumer 按 `instance_id` 订阅子集 partition（[ADR-0022](./adr/0022-push-sharding-sticky-routing.md) §私有数据订阅）
  - `market-data` 每实例独立 group 消费全量
- **依赖**：MVP-7 + MVP-8 的 `pkg/shard`
- **预期 ADR**：0033

---

## 更新约定

- 完成一个 MVP → 勾掉 ✅ + 填 commit hash + ADR 链接
- 新发现的大块工作 → 加 MVP-N 条目或填到 Backlog（取决于体量）
- 范围变化 → 在对应条目里改 "范围" 行，不要删 history
- 顺序调整 → 改 "一览" 表的 MVP 编号不是强制的；保留编号不连续，commit log 才能和编号对齐
