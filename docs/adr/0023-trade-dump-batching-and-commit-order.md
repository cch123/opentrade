# ADR-0023: trade-dump 批量写入与 offset 提交顺序

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0001, 0005, 0008

## 背景 (Context)

ADR-0008 规定 Counter/Match 不直接写 MySQL，改由 `trade-dump` 旁路消费 Kafka 写 MySQL。
MVP-5 落地 `trade-event → trades` 单表投影，需要确定：

1. Kafka poll 到的一批 record 如何转成 MySQL 写入。
2. MySQL 写入与 Kafka offset commit 的先后顺序（崩溃一致性）。
3. 出错时的处理方式（重试、退出、DLQ）。

约束：

- `trade-event` 包含多种 payload（Trade / OrderAccepted / OrderRejected /
  OrderCancelled / OrderExpired），MVP-5 只落 `Trade`，其他 payload 仍然在同一
  topic 同一 offset 流上。
- MySQL 8 `max_allowed_packet` 默认 64 MiB，但锁时长与 binlog 大小随 batch 线性增长。
- franz-go `PollFetches` 一次可能返回跨多 partition 的 record，client 内部维护
  "committed 高水位"。未手动 commit 的 offset 在同一 client 生命周期内不会被再次
  poll 出来，只有重启后才能从上一次 committed offset 重放。

## 决策 (Decision)

**单事务、多行 INSERT、先 MySQL 后 Kafka 的 commit 顺序；失败即退出 Run，由 supervisor 重启。**

具体：

1. **批的定义**：每次 `PollFetches` 拿到的所有 record 作为一个逻辑批次。
2. **解码阶段**：逐条反序列化 `TradeEvent`；丢弃非 `Trade` payload 和解码失败的
   record（记 log），它们的 offset 仍然进入本次提交。
3. **写 MySQL**：`Trade` 投影成 `TradeRow`，在单个 MySQL transaction 里执行。
   大批拆成 `ChunkSize=500`（默认）的多行 `INSERT ... VALUES (...),(...),...
   ON DUPLICATE KEY UPDATE trade_id = trade_id` —— `trade_id` 是 PK，重复写入
   无副作用（幂等）。
4. **提交顺序**：`tx.Commit()` 成功 **之后** 才 `CommitUncommittedOffsets`。
5. **错误处理**：任意步骤（MySQL exec / commit / Kafka offset commit）失败即
   `Run` 返回 error，外层 supervisor / systemd 重启；重启后从 Kafka broker 的
   committed offset 开始，重放已经写入 MySQL 的 row 会在 ON DUPLICATE KEY
   UPDATE 分支上变成 no-op。

## 备选方案 (Alternatives Considered)

### A. 先 commit Kafka 再写 MySQL
- 优点：poll 链路不会卡住。
- 缺点：MySQL 未写入时 Kafka 已推进，崩溃 → **数据丢失**。不可接受。

### B. 两阶段提交（XA / 事务性 outbox）
- 优点：崩溃安全 + 精确一次。
- 缺点：运维复杂度高，MySQL 8 XA 性能差；与 ADR-0008"MySQL 故障不影响交易"相悖；
  MVP 不值得。

### C. Per-partition 并行 writer
- 优点：吞吐更高。
- 缺点：`trade-event` 按 symbol partition，热门 symbol 本身就是写放大的热点；
  per-partition 一个 tx 反而失去多 partition 合并写的批量效率。MVP-5 先单线程，
  后续 benchmark 驱动再拆。

### D. 遇错 retry 循环而非退出
- 优点：少一次重启开销。
- 缺点：热点 loop 可能 spin；掩盖连接池耗尽 / schema 变更等结构性问题；
  supervisor 统一管理重启更简洁。

### E. 把非 Trade payload 路由到 DLQ
- 优点：可审计。
- 缺点：MVP-5 根本不关心它们，DLQ 会堆积噪声；未来 MVP 把 order / account_log
  投影补齐后，这些 payload 会在新 consumer group 被处理。

## 理由 (Rationale)

- **先 MySQL 后 Kafka 的提交顺序** 是 at-least-once + 幂等写 MySQL 这一模型下唯一
  正确的方向。ADR-0008 明确要求它。
- **单事务 + 多行 INSERT** 把 fsync / group commit 的代价摊到整个批上，对应
  Kafka 一次 poll 的天然批。
- **遇错退出 + 幂等重放** 把"脏状态清理"下放给重启，避免 in-process 恢复逻辑。
  幂等靠 `trades.trade_id` 主键 + `uk_symbol_seq` 唯一索引保证。
- **非 Trade payload 静默跳过** 保持 MVP 范围干净；offset 仍然推进，避免 topic
  滞后被误读成消费能力不足。

## 影响 (Consequences)

### 正面

- 崩溃安全：MySQL 已写 → 即使 Kafka offset 没 commit，重放幂等无副作用。
- 交易链路解耦：MySQL 故障只会让 trade-dump 退出重启 + Kafka 堆积，不影响 Match/Counter。
- 代码简单：没有 retry、inflight tracking、DLQ。

### 负面 / 代价

- 单 partition 顺序处理，单实例吞吐上限受 MySQL 写入速度 + 单线程反序列化制约。
  压测出现瓶颈后再拆（方案 C）。
- 反复失败会导致 restart loop → supervisor 需要有 backoff 或告警；MVP 不自己实现。
- 整批失败会整批重放（幂等，但会有写放大）；未来若单行失败频繁，考虑 row-level
  skip + DLQ。

### 中性

- 非 Trade payload 的 offset 被推进。后续若新增 trade-dump consumer group 投影
  order / account_log，会独立消费、不受影响（每个 projection 一个 group_id）。

## 实施约束 (Implementation Notes)

- 幂等键：`trade_id`（PK）+ `(symbol, symbol_seq_id)` 唯一键。两者都在 MVP-1 的
  `match/internal/journal/convert.go` 里由 match 以 `symbol:seq_id` 格式生成。
- 反序列化失败记 `error` 级别 log 并包含 `topic / partition / offset`，便于事后回溯。
- MySQL connection pool：`MaxOpenConns=16 / MaxIdleConns=4 / ConnMaxLifetime=30m`
  作为默认，后续按真实 QPS 调整。
- 没有显式 flush timer —— franz-go 自己有 `FetchMaxWait`（默认 5s）保证低流量
  下也会周期性返回空 poll；空 poll 跳过整个处理块。
- Shutdown：`ctx.Done()` 时当前批若已 `PollFetches` 返回，会尝试走完整条
  写-提交链路；若其中 `InsertTrades(ctx)` 因 ctx 取消失败，退出即可，重启后重放。

## 参考 (References)

- ADR-0001: Kafka as Source of Truth
- ADR-0005: Kafka Transactions for Dual Writes（producer 侧 EOS；与本 ADR 无直接
  耦合，但 ReadCommitted 消费依赖它）
- ADR-0008: Sidecar Persistence（trade-dump）
- 实现：[trade-dump/internal/consumer/trade.go](../../trade-dump/internal/consumer/trade.go)、
  [trade-dump/internal/writer/mysql.go](../../trade-dump/internal/writer/mysql.go)
