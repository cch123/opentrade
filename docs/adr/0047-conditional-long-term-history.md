# ADR-0047: 条件单长期历史（conditional-event + history projection）

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0001, 0008, 0023, 0028, 0040, 0042, 0046

## 背景 (Context)

MVP-14a~14f 把条件单服务跑起来了，但它只在内存里保留 `TerminalHistoryLimit`
轮终态（默认几百个），重启 / 滚动升级后就清零。客户端查"三个月前那笔
stop-loss 最终怎么了"这条需求没有权威来源。

ADR-0046 起了 `history` 服务做只读查询，但它读的是 trade-dump 投影的
`orders` / `trades` / `account_logs` 三张表，条件单完全没上车 —
因为 conditional 服务根本不写 Kafka（ADR-0040）。

可选路线：
- **A. conditional 每次 Query/List 回源 MySQL**：让 conditional 服务
  自己读/写 MySQL。这会把 conditional 变成又是热路径状态机，又是读
  服务，违反 ADR-0046 的分层。
- **B. 把条件单快照周期写 MySQL**：conditional 定期把全量状态 dump
  到 MySQL。延迟粗，写量大（全量 overwrite），且 restart 后要恢复。
- **C. 新 Kafka topic + trade-dump 投影**：conditional 把状态转换
  事件发到新 topic，trade-dump 消费并投影到 MySQL，history 读表。
  和现有架构（ADR-0008 / 0023 / 0028）一致。

## 决策 (Decision)

采纳 **方案 C**：
- 新 Kafka topic `conditional-event`（key=`user_id`）
- conditional 服务在每次状态转换（PENDING → TRIGGERED / CANCELED /
  REJECTED / EXPIRED）时发 `ConditionalUpdate` 事件，事件带
  **完整 post-change 快照**
- trade-dump 新增 consumer 投影到 `conditionals` MySQL 表，按 `id`
  upsert，用 `last_update_ms` 做"last-write-wins"守卫
- history 服务新增 `GetConditional` / `ListConditionals` RPC 读这张表
- BFF `GET /v1/conditional?scope=active|terminal|all` 三档路由：
  - `active` 走 conditional gRPC（热路径、无历史延迟）
  - `terminal` / `all` 走 history gRPC
  - `GET /v1/conditional/:id` 先试 conditional；NotFound 时 fall back 到 history
- `include_inactive=true` 兼容 MVP-14 行为：history 未部署时回落 conditional `IncludeInactive`

### Producer 事务性 — MVP 不做

conditional 的 journal producer **不走 Kafka 事务**。理由：

1. 每个事件都是 full snapshot（不是 diff），consumer 靠 `last_update_ms`
   做守卫，重复 / 乱序都能收敛到最新状态
2. HA 切换期间偶发重复发送的失败半径：MySQL 里某行短暂重复 UPSERT，
   等新 primary 的后续 emit 追上后收敛
3. conditional 服务的 source of truth 仍是本地 JSON snapshot（ADR-0036
   模式），Kafka 出岔子不影响业务；MVP-14c 的 HA 依赖 etcd lease + 本地
   snapshot，跟 Counter/Match 的 EOS 模型（ADR-0032）不同

未来若要把 conditional 也升级到 EOS：换 `kgo.DisableIdempotentWrite()`
为 `kgo.RequireStableFetchOffsets()` + 加 transactional.id + 调整
trade-dump 消费的 `FetchIsolationLevel`。代价：producer 吞吐下降、
P99 延迟敏感。本 ADR 不强求。

### Guard 字段 — 为什么用 ts_unix_ms 而不是 seq_id

conditional 服务启动时 `atomic.Uint64` 从 0 开始，重启后也是 0。若用
它做 MySQL upsert 守卫，第一轮新事件的 seq_id 可能小于 MySQL 里已有
行的 seq_id，导致新状态被错误丢弃。

改用 `meta.ts_unix_ms`（wall-clock ms）：跨 restart 单调、跨 HA
primary 切换也单调（只要时钟没倒流）。MySQL 表用 `last_update_ms
BIGINT NOT NULL` 承载，upsert 子句形如
`col = IF(VALUES(last_update_ms) >= last_update_ms, VALUES(col), col)`。

时钟回拨是已知风险：MVP 接受；生产环境 NTP 已经解决 ±ms 级别问题。

## 备选方案 (Alternatives Considered)

### 方案 A: conditional 直连 MySQL

违反 ADR-0046 分层（条件单服务是写 / 触发热路径，混入读职责会增加
复杂度）。也使得 conditional 服务部署要带 MySQL 驱动 + 连接池。
拒绝。

### 方案 B: 周期性全量快照写 MySQL

写量大（每周期全表 overwrite），延迟粗（≥ 快照周期，默认 60s）。
"三个月前那笔条件单" 的需求要求秒级延迟。拒绝。

### 方案 D: EOS / transactional producer

带来正确性提升（消除 HA 重复），但代价是：producer 吞吐下降，
需要 transactional.id 命名规划（ADR-0017），consumer 改 read-committed
（ADR-0023 trade-dump 已经是了，OK）。MVP-16 先用 non-transactional
+ last_update_ms 守卫；future work 可以无痛升级（wire 协议兼容）。

### 方案 E: 只有 terminal 事件才发，PENDING 不发

省 Kafka 流量（PENDING 单未被触发则无 event；绝大多数条件单最终被
cancel/expire 而不是触发）。代价是 client 看不到刚下的条件单（下单
后先查 history 会返回空，得重试）。产品侧明确希望"挂单出现在列表里"
必须立刻可见。拒绝。

## 理由 (Rationale)

- 和 ADR-0028 counter-journal 投影完全同构：trade-dump 是唯一 MySQL 投影
  入口，history 是唯一读端，职责分离
- Full-snapshot 事件 + last_update_ms 守卫让 HA 重复不影响正确性，
  producer 不必 EOS
- `scope` 三档分流保持和 MVP-15 `ListOrders` 一致的 UI 语义
- `GET /v1/conditional/:id` 的 Counter-first + history-fallback 模型
  复用了 ADR-0046 里"活跃走 hot path、终态走 history" 的套路

## 影响 (Consequences)

### 正面

- 条件单终态永久可查；`conditional` 服务的 `TerminalHistoryLimit` 不再决定
  历史可见窗口
- BFF `/v1/conditional` 三档 scope 语义和订单列表统一
- 单条 `GET /v1/conditional/:id` 对已 TRIGGERED / CANCELED 等终态同
  样 200，解决 MVP-14 的 "活跃单命中、终态 NotFound" 不一致

### 负面 / 代价

- 多一个 Kafka topic；每次状态转换额外一次 produce（async，非阻塞）
- trade-dump 多一个 consumer goroutine + `conditional_events` 表
- HA 切换 / restart 偶发重复 UPSERT；MySQL 写放大 < 2x，acceptable

### 中性

- Producer / consumer 的 wire 协议预留 `meta.seq_id` 字段未用（供未来
  切 EOS 时单调排序），值永远是 process-local 的 atomic 计数
- `ConditionalEventType` / `ConditionalEventStatus` 两枚举与 rpc 层
  的同名枚举值相等，在 trade-dump / history 内做 int8 存/读

## 实施约束 (Implementation Notes)

- 新 proto `api/event/conditional_event.proto`，`ConditionalUpdate`
  携带完整状态；enums 与 rpc/conditional 同值
- conditional service: `internal/journal/journal.go` 起 kgo 客户端 +
  buffered queue + 单独 goroutine drain；队列满打 WARN 丢弃（非阻塞）
- engine 新加 `JournalSink` 接口 + `SetJournal` 方法；
  Place / PlaceOCO / Cancel / tryFire / SweepExpired /
  cascadeOCOCancelLocked 在状态变更点捕获 snapshot 后解锁并 emit
- trade-dump: `internal/writer/projection_cond.go` + `mysql_cond.go`
  + `internal/consumer/conditional.go`；`--conditional-topic` 空时禁
  用投影
- history: proto 新 `GetConditional` / `ListConditionals` + cursor +
  scope=ACTIVE/TERMINAL/ALL；store/server/enums 按 MVP-15 模式镜像
- BFF: `GET /v1/conditional?scope=active|terminal|all`；
  `include_inactive=true` 兼容 MVP-14；Query 单笔单元时 Counter-first
  + history-fallback on NotFound

## 参考 (References)

- [ADR-0001](0001-kafka-as-source-of-truth.md) — 权威事件源
- [ADR-0008](0008-sidecar-persistence-trade-dump.md) — trade-dump 旁路持久化
- [ADR-0023](0023-trade-dump-batching-and-commit-order.md) — MySQL 优先 / Kafka offset 后的语义
- [ADR-0028](0028-trade-dump-journal-projection.md) — orders / accounts / account_logs 同构投影
- [ADR-0040](0040-conditional-order-service.md) — 条件单独立服务
- [ADR-0042](0042-conditional-ha.md) — conditional HA cold standby
- [ADR-0046](0046-history-service.md) — history 只读查询聚合
- [roadmap MVP-16](../roadmap.md#mvp-16-conditional-history)
