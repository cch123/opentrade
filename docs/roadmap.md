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
| MVP-12b | Match transactional producer (fencing) | ✅ | 消除 split-brain 写 trade-event 窗口 |
| [MVP-13](#mvp-13-push-sharding) | Push 多实例 sticky | ✅ | user 过滤 + handshake 检查(partition 严格对齐留 MVP-13b) |
| [MVP-14a](#mvp-14a-conditional) | 条件单（stop-loss / take-profit） | ✅ | 独立 `conditional` 服务订 market-data，触发时通过 Counter.PlaceOrder 下真实单；无资金预留，MVP-14b 补 |
| [MVP-14b](#mvp-14b-reservations) | 条件单资金预留 | ✅ | Counter 新 `Reserve` / `ReleaseReservation` + `PlaceOrder(reservation_id)`；conditional 下发即锁资金，触发原子消费 |
| [MVP-14c](#mvp-14c-conditional-ha) | 条件单 HA（cold standby） | ✅ | etcd 选主复刻 ADR-0031；`--ha-mode=auto` 双实例，primary crash 10~15s 内新 primary 接管 |
| [MVP-14d](#mvp-14d-conditional-expiry) | 条件单过期（TTL） | ✅ | `expires_at_unix_ms` + `EXPIRED` 状态 + primary 侧 sweeper；到期自动释放 reservation |
| [MVP-14e](#mvp-14e-conditional-oco) | 条件单 OCO（One-Cancels-Other） | ✅ | `PlaceOCO` 接 N 腿；任一腿到 terminal → 兄弟 CANCELED；client_oco_id 幂等 |
| [MVP-14f](#mvp-14f-conditional-trailing) | 条件单 Trailing Stop | ✅ | `TRAILING_STOP_LOSS` + `trailing_delta_bps` + 可选 `activation_price`；引擎实时追 watermark |
| [MVP-15](#mvp-15-history) | history / query 服务 | ✅ | 独立只读服务读 trade-dump 的 MySQL 投影（orders / trades / account_logs），BFF 历史接口改走它 |
| [MVP-16](#mvp-16-conditional-history) | 条件单长期历史 | ✅ | `conditional-event` topic + trade-dump 投影 `conditionals`；history 新增 `ListConditionals` / `GetConditional` |

> 顺序原则：**最小依赖先行**。HA（12）晚于 sharding（8/11），因为 HA 实现依赖多实例拓扑成型。Sharding（8）早于 BFF WS（10），因为 BFF WS 本质上是"把 push 那套协议代理一遍"，在 push 协议稳定后做更省力。

## 填坑 / Backlog

从已合 MVP 里明确推迟的项，没有单独升到 MVP 级，但都要做：

- ~~**Counter 周期性 snapshot**~~ — ✅ MVP-12 落地（`--snapshot-interval=60s` 默认，可调）
- ~~**K 线 gap 填充**~~ — ✅ 跨 bucket 的 trade 会 emit 中间 empty bar（O=H=L=C=上一收盘，volume=0，count=0）；Kline 流保持致密（[ADR-0025 未来工作](./adr/0025-quote-engine-state-and-offset-strategy.md)）
- ~~**Push coalescing / rate-limit**~~ — ✅ KlineUpdate 走可替代 coalesce 通道（latest-wins），每连接 token bucket 限速（默认 2000/s, burst 4000），depth/trade 继续走原队列（[ADR-0037](./adr/0037-push-coalesce-rate-limit.md)）
- ~~**Push 重连快照补齐**~~ — ✅ BFF `GET /v1/depth/{symbol}` + `GET /v1/klines/{symbol}?interval=...` 从自带 market-data cache 返回最新 DepthSnapshot + 最近 N 条 KlineClosed（[ADR-0038](./adr/0038-bff-reconnect-snapshot.md)）；`--market-brokers ""` 默认禁用
- ~~**Counter 事务双写 review**~~ — ✅ review 发现 Transfer/Settlement 走非事务 producer（HA 下 fencing 失效），已合并到 TxnProducer（见 ADR-0031 §2 补充）
- ~~**集成冒烟脚本**~~ — ✅ [docs/smoke.md](./smoke.md) + [deploy/scripts/smoke.sh](../deploy/scripts/smoke.sh)
- ~~**市价单 MARKET（服务端原生）**~~ — ✅ 两条路径落地（[ADR-0035](./adr/0035-market-orders-native-server-side.md)）：(A) 服务端原生 `type=market` + `qty`(sell) / `quote_qty`(buy)；(B) BFF 可选滑点保护：`type=market + slippage_bps + last_price` → 翻译成 LIMIT+IOC。客户端用法文档见 [docs/market-orders.md](./market-orders.md)
- ~~**Counter 对账**~~ — ✅ `counter/internal/reconcile` 每小时对比内存 vs `accounts` 表，差异日志 + 汇总（[ADR-0008 §对账](./adr/0008-sidecar-persistence-trade-dump.md)）；`--mysql-dsn` 空时禁用
- ~~**Quote state snapshot**~~ — ✅ 本地 JSON snapshot + 每 partition offset 随状态原子推进，重启热恢复（[ADR-0036](./adr/0036-quote-state-snapshot.md)）
- ~~**Counter re-shard 工具**~~ — ✅ `counter/cmd/counter-reshard`：读 N 份旧 snapshot，按新 hash 写 M 份新 snapshot；账户/订单按 `shard.Index(user, M)` 路由，dedup 丢弃，ShardSeq 取 max（runbook: [docs/counter-reshard.md](./counter-reshard.md)）
- ~~**trade-event consumer 的显式 shard filter**~~ — ✅ Counter 每个 trade-event handler 在进 sequencer 之前 `OwnsUser` 判定，非 owned 走 debug 日志 skip（[ADR-0027 备选方案 D](./adr/0027-counter-sharding-rollout.md)）
- ~~**BFF auth 升级到 JWT / API-Key**~~ — ✅ `--auth-mode=header|jwt|api-key|mixed`；HS256 JWT + BN 风格 HMAC-SHA256 API-Key，无外部依赖（[ADR-0039](./adr/0039-bff-auth-jwt-apikey.md)）
- **Match / Counter 延迟 + 吞吐 benchmark** — 验证是否接近 20w TPS / 10ms P99（[architecture.md §18.3](./architecture.md)）
- ~~**MVP-14b 条件单资金预留**~~ — ✅ Counter `Reserve` / `ReleaseReservation` RPC + `PlaceOrder(reservation_id)` 原子消费；snapshot 持久化；触发不再因余额不足 reject（[ADR-0041](./adr/0041-counter-reservations.md)）
- ~~**MVP-14c 条件单 HA**~~ — ✅ `pkg/election` cold standby 复刻 ADR-0031；`--ha-mode=auto` + `--etcd` 双实例（[ADR-0042](./adr/0042-conditional-ha.md)）
- ~~**MVP-14d 条件单过期**~~ — ✅ `expires_at_unix_ms` 字段 + EXPIRED 终态 + primary 侧 5s sweeper；到期释放 reservation（[ADR-0043](./adr/0043-conditional-expiry.md)）
- ~~**MVP-14e OCO**~~ — ✅ `PlaceOCO` N 腿原子下单 + 级联取消；`client_oco_id` 幂等；任一腿 terminal 自动 CANCEL 兄弟（[ADR-0044](./adr/0044-conditional-oco.md)）
- ~~**MVP-14f Trailing stop**~~ — ✅ `TRAILING_STOP_LOSS` 类型 + bps retracement + 可选 activation_price；引擎维护 watermark（[ADR-0045](./adr/0045-conditional-trailing-stop.md)）

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

- **commit** [`45d2c22`](../) · **ADR** [0028](./adr/0028-trade-dump-journal-projection.md)
- 新增 journal consumer（与 trade-event consumer 并行）消费 `counter-journal`
- 投影到 `orders` / `accounts` / `account_logs`；单 tx 三表写入；`accounts` 带 seq_id guard 防乱序
- schema 调整：`account_logs` PK `(shard_id, seq_id, asset)`（SettlementEvent 一事件两 asset 行）
- shard_id 从 `EventMeta.producer_id`（`counter-shard-N-main`）parse 而来

### MVP-10  BFF WebSocket 网关  {#mvp-10-bff-ws}

- **commit** [`1e40e9f`](../) · **ADR** [0029](./adr/0029-bff-ws-reverse-proxy.md)
- BFF `/ws` 接受客户端连接 → 对上游 push 发起独立 WS，双向透传帧
- auth 在 handshake 层（`bff/internal/auth.Middleware` 读 `X-User-Id`），转发时 re-inject 到上游
- 单 upstream（`--push-ws=ws://push:8081/ws`）；多实例 sticky 留给 MVP-13
- BFF 不解析载荷，对 push 协议变化免疫

### MVP-11  Match 多实例 + symbol 迁移  {#mvp-11-match-sharding}

- **commit** [`8fb001a`](../) · **ADR** [0030](./adr/0030-match-etcd-sharding-rollout.md)
- 新 `pkg/etcdcfg`：`Source` 接口 + `EtcdSource`（clientv3）+ `MemorySource`（测试）；List → revision → Watch
- 新 `match/internal/registry`：线程安全的 symbol 生命周期管理（Factory + Restore + Snapshot 回调 + Dispatcher 注册/注销）
- match main 重写：etcd 启动 List + 后续 Watch 驱动 AddSymbol/RemoveSymbol；`--etcd` 空时 fallback 到 `--symbols` 静态模式
- ADR-0009 里的"trading:false 在线撤单"简化为"drain + final snapshot"；operator 配合先停新单

### MVP-12  Counter / Match HA  {#mvp-12-ha}

- **commit** [`7355304`](../) · **ADR** [0031](./adr/0031-ha-cold-standby-rollout.md)
- 新 `pkg/election`：etcd `concurrency.Election` 封装（Campaign / Resign / LostCh / Observe）
- Counter / Match main 分出 `runPrimary`，外层 `runElectionLoop` cold-standby
- `--ha-mode=auto` 走选举；`disabled` 保留单机模式（向后兼容）
- Counter fencing 自然走 Kafka `TransactionalID`（ADR-0017）；Match MVP-12b 起同款 fencing（[ADR-0032](./adr/0032-match-transactional-producer.md)）
- backup 不消费 Kafka、不打快照；snapshot 共享目录 MVP 假设本地 EFS/NFS mount

**MVP-12b**（commit [`7f53424`](../)）：**ADR** [0032](./adr/0032-match-transactional-producer.md)
- `TradeProducer` 增加 `TransactionalID`；`Pump` 按 `BatchSize=32 / FlushInterval=10ms` 攒批写单事务
- `--ha-mode=auto` 时启用，`disabled` 仍走 idempotent
- 关闭 ADR-0031 遗留的 Match split-brain 窗口；Counter 和 Match 达成对等 fencing 模型

### MVP-13  Push 多实例 sticky  {#mvp-13-push-sharding}

- **commit** [`2480005`](../) · **ADR** [0033](./adr/0033-push-sticky-user-filter.md)
- 新 flag `--instance-ordinal` / `--total-instances`；`TotalInstances=1` 默认（单实例模式）
- `PrivateConsumer` 按 `shard.Index(userID, total)` 过滤非 owned 事件
- WS handshake 非 owner 的 `X-User-Id` 返回 `403` + `X-Correct-Instance`；匿名连接 bypass
- Counter 仍用默认 partitioner（全量消费 + user 过滤，MVP 接受 N 倍流量）；严格 partition 对齐留 **MVP-13b**

### MVP-14a  条件单（stop-loss / take-profit）  {#mvp-14a-conditional}

- **commit** [`773569d`](../) · **ADR** [0040](./adr/0040-conditional-order-service.md)
- 新 `conditional/` 模块 + `ConditionalService` gRPC（Place / Cancel / Query / List）
- 订 Quote `market-data`（PublicTrade）；触发按 side × type 矩阵 —— sell/buy × STOP_LOSS/TAKE_PROFIT（±_LIMIT）
- 触发时调 Counter `PlaceOrder`（`client_order_id = "cond-<id>"` 天然 dedup + replay 幂等）
- 本地 JSON snapshot + per-partition offset（照搬 ADR-0036 模式）；单实例，HA 留 MVP-14c
- **无资金预留**：触发瞬间 Counter 余额不足 → REJECTED + reject_reason；MVP-14b 补 Reserve/Release

### MVP-14b  条件单资金预留  {#mvp-14b-reservations}

- **commit** [`c2c52e6`](../) · **ADR** [0041](./adr/0041-counter-reservations.md)
- Counter 新 RPC：`Reserve` / `ReleaseReservation`；`PlaceOrderRequest` 加 `reservation_id` 字段
- Counter engine：`reservations[ref_id] → {user, asset, amount}` 副表，Available/Frozen 移动通过 `CreateReservation` / `ReleaseReservationByRef` / `ConsumeReservationForOrder` 操作
- Snapshot 持久化 reservations（graceful restart 恢复）；非 graceful crash 下 reservation + balance 一起回到 pre-Reserve 状态，语义一致
- 不发 counter-journal 事件：trade-dump projection 在持有期间 stale，consume/release 时收敛，可接受
- conditional 使用：Place → Reserve（失败不存储），Cancel / REJECTED → best-effort Release；Trigger PlaceOrder 带 `reservation_id` 原子消费
- Reserve vs `Transfer(FREEZE)` 的边界（权限 / 原子消费 / journal / 副表）详见 ADR-0041

### MVP-14c  条件单 HA（cold standby）  {#mvp-14c-conditional-ha}

- **commit** [`9ee1473`](../) · **ADR** [0042](./adr/0042-conditional-ha.md)
- `conditional/cmd/conditional` 拆分 `runPrimary` + `runElectionLoop`，复刻 ADR-0031 Counter/Match 的 cold-standby 模型
- `--ha-mode=auto` + `--etcd`：双实例共享 `/cex/conditional/leader` etcd key + `--snapshot-dir` 共享 mount
- Split-brain 失败半径退化为"少量重复 PlaceOrder"：Counter 的 `reservation_id` / `client_order_id` dedup 吸收，无业务错误
- `--ha-mode=disabled` 保留 MVP-14a/b 单机行为，CI / 本地开发零 etcd 依赖

### MVP-14d  条件单过期（TTL）  {#mvp-14d-conditional-expiry}

- **commit** [`7e6827a`](../) · **ADR** [0043](./adr/0043-conditional-expiry.md)
- proto: `PlaceConditionalRequest.expires_at_unix_ms` + `Conditional.expires_at_unix_ms`，`ConditionalStatus.EXPIRED = 5`
- engine: `SweepExpired(ctx) int` 扫 pending 里到期的条目 → EXPIRED + best-effort Release reservation
- main: `--expiry-sweep=5s` 起 sweeper goroutine（primary only，HA 切换后 backup 接管）
- BFF REST: `expires_at_unix_ms` 透传；状态 label 增加 `"expired"`；`0 = never` 保持向后兼容

### MVP-14e  条件单 OCO  {#mvp-14e-conditional-oco}

- **commit** [`64ceec8`](../) · **ADR** [0044](./adr/0044-conditional-oco.md)
- proto: 新 RPC `PlaceOCO(PlaceOCORequest)`；`Conditional.oco_group_id = 18`；N ≥ 2 腿共享 user/symbol/side
- engine: `PlaceOCO` Reserve 每腿失败回滚；`cascadeOCOCancelLocked` 统一级联，在 Cancel / tryFire / SweepExpired 后触发
- `ocoByClient map[string]string` 做 group-level 幂等；snapshot 持久化 + restore
- BFF REST: `POST /v1/conditional/oco`；每腿 body 复用既有 `placeConditionalBody` 结构

### MVP-14f  条件单 Trailing Stop  {#mvp-14f-conditional-trailing}

- **commit** [`fb7f6b6`](../) · **ADR** [0045](./adr/0045-conditional-trailing-stop.md)
- proto: `CONDITIONAL_TYPE_TRAILING_STOP_LOSS = 5`；`trailing_delta_bps` + `activation_price` 入参；observable 字段 `trailing_watermark` / `trailing_active`
- engine: `updateTrailingLocked(c, price)` 在 handleLocked 内分叉：activation gate → watermark 推进 → effective_stop 比较；mutating 写进 Conditional 由引擎锁保护，snapshot 捕获
- Reservations 走既有 MARKET 逻辑（Reserve 按 qty/quote_qty 一次算好，不依赖触发价）
- BFF REST: 类型字符串 `"trailing_stop_loss"`；conditionalToJSON 暴露 watermark + active

### MVP-15  history / query 服务  {#mvp-15-history}

- **commit** [`ea224a0`](../) · **ADR** [0046](./adr/0046-history-service.md)
- 新 `history/` 模块 + `HistoryService` gRPC：`GetOrder` / `ListOrders` / `ListTrades` / `ListAccountLogs`；数据源为 trade-dump 已有的 MySQL 投影（ADR-0023 / 0028），纯只读、无 Kafka
- `history/internal/cursor` opaque cursor（base64(JSON)）；orders 按 `(created_at, order_id)` 严格 < 游标，trades 按 `(ts, trade_id)`，account_logs 按 `(ts, shard_id, seq_id, asset)`
- `history/internal/mysqlstore` 存储层；`ListOrders` 接收 `scope=OPEN|TERMINAL|ALL`（fallback，`statuses` 非空时后者胜），trades 表用 maker/taker 两 index UNION ALL 合流；limit 默认 100 / clamp 500
- `history/cmd/history` main：`--grpc :8085` / `--mysql-dsn ...` / `--query-timeout 2s`；启动 pings MySQL，失败即退出
- BFF：新增 `client.History` 接口 + `DialHistory`；`--history` flag 空时 `/v1/orders` / `/v1/trades` / `/v1/account-logs` 返回 503；`GET /v1/order/:id` 保持 Counter hot path 不变（单笔终态查询改走列表）
- `orderToJSON` 加 `source=conditional|user` 标记（基于 `client_order_id` 是否以 `cond-` 开头，ADR-0040 条件单触发单识别）
- 测试：cursor 编解码 + boundary、sqlmock 驱动的 GetOrder / ListOrders 分页 / NotFound / scope → statuses 展开 / invalid cursor 错误映射；BFF 既有测试全绿

### MVP-16  条件单长期历史  {#mvp-16-conditional-history}

- **commit** [`e5133de`](../) · **ADR** [0047](./adr/0047-conditional-long-term-history.md)
- 新 proto `api/event/conditional_event.proto` + `ConditionalUpdate`（post-change full snapshot + `meta.ts_unix_ms` 守卫），`ConditionalEventType` / `ConditionalEventStatus` 与 rpc/conditional 枚举同值
- 新 Kafka topic `conditional-event`（默认），key=user_id；conditional 服务 `internal/journal/journal.go` 起非事务 producer + 4096 槽 buffer，drain goroutine async 落盘；队列满 WARN 丢弃
- engine `JournalSink` 接口 + `SetJournal`；Place / PlaceOCO / Cancel / tryFire / SweepExpired / cascadeOCOCancelLocked 全部在锁里 capture snapshot，锁外 emit
- `conditionals` MySQL 表（TINYINT side/type/status/tif, DECIMAL(36,18) prices/qty, BIGINT last_update_ms 守卫）+ idx_user_ctime / idx_user_symbol_ctime / idx_oco_group
- trade-dump 新 consumer `conditional-event`（`--conditional-topic` 空禁用）+ `ApplyConditionalBatch`：每列 `IF(VALUES(last_update_ms) >= last_update_ms, ...)` 保证 last-write-wins
- history 新 RPC `GetConditional` / `ListConditionals`（复用 cursor 模式，scope=ACTIVE/TERMINAL/ALL），proto 复用 condrpc ConditionalType/ConditionalStatus
- BFF `GET /v1/conditional?scope=active|terminal|all`：active → conditional gRPC；terminal/all → history gRPC；`include_inactive=true` 保留 MVP-14 兼容（history 未部署时回落 conditional.IncludeInactive）
- `GET /v1/conditional/:id`：Counter-first + history-fallback on NotFound
- history JSON 响应打 `source="history"` 标记；字段名与 conditional 服务一致（`placed_order_id` 等）

---

## 更新约定

- 完成一个 MVP → 勾掉 ✅ + 填 commit hash + ADR 链接
- 新发现的大块工作 → 加 MVP-N 条目或填到 Backlog（取决于体量）
- 范围变化 → 在对应条目里改 "范围" 行，不要删 history
- 顺序调整 → 改 "一览" 表的 MVP 编号不是强制的；保留编号不连续，commit log 才能和编号对齐
