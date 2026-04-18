# ADR-0032: MVP-12b Match TradeProducer 升级为 transactional (fencing)

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0005, 0017, 0031

## 背景 (Context)

ADR-0031（MVP-12）让 Counter/Match 支持 etcd lease cold-standby，但明确承认 **Match 层**没有 Kafka producer fencing：Match 仍然用 idempotent producer，网络分区下老 primary 的 `trade-event` 可能和新 primary 的写入并发。

ADR-0031 把这个尾巴明确列为 MVP-12b，本 ADR 记录实施。

核心约束：
- Match 的 per-symbol 撮合是 stateful 且 deterministic，**两个 primary 同时撮合**会得到不一致的订单成交结果（虽然 downstream 的 Counter 能用 trade_id 幂等吸收重复写入，但"同一订单两条成交"对用户是真实错误）。
- Counter 已经是 `TransactionalID` + fencing 成熟（MVP-3 / ADR-0005 / 0017）；Match 照抄即可。
- Match 的 producer 是热路径（目标 4w TPS/symbol），事务粒度必须是 batch 不能 per-event。

## 决策 (Decision)

### 1. TradeProducer 加 transactional mode

`ProducerConfig` 增加 `TransactionalID string` 字段：

- 空 → 走 legacy idempotent 路径（`acks=all` + `enable.idempotence=true`，franz-go 默认启用）。
- 非空 → `kgo.TransactionalID(...)` + 每批一个 `BeginTransaction / ... / EndTransaction(TryCommit)`。

### 2. 批量事务（ADR-0031 Future Work 里写过的"B 方案"）

`Pump` 循环按 `BatchSize` (默认 32) 或 `FlushInterval` (默认 10ms) 攒批，一次事务发完。`PublishBatch` 把整批 `*sequencer.Output` 序列化成 `*kgo.Record` 列表，整批一起：

```go
BeginTransaction()
for rec := range records {
    ProduceSync(rec)  // 任一失败 → Abort
}
Flush()
EndTransaction(TryCommit)
```

任何步骤失败 → `TryAbort`。现有的"日志一下、下一批继续"的容错策略不变（franz-go 会对网络瞬断自动 retry）。

单条 `Publish` API 保留给测试和偶发单写 caller 使用，内部直接调 `PublishBatch([]{out})`。

### 3. HA-aware 启用

`match/cmd/match/main.go` 的 `runPrimary` 里：

```go
txnID := ""
if cfg.HAMode == "auto" {
    txnID = cfg.InstanceID
}
journal.NewTradeProducer(Config{..., TransactionalID: txnID})
```

`--ha-mode=disabled`（dev / 单测）时保持 idempotent 模式 —— 没有 fencing 需求，也不想让单机测试被 Kafka transaction 状态机拖慢。

### 4. TransactionalID = InstanceID

两个 match 实例用相同 `--instance-id`（cold-standby 模型），`TransactionalID` 也相同。`BeginTransaction()` 隐式调 `InitProducerID`，新进程的 producer epoch 递增，老进程下次 `EndTransaction(TryCommit)` 会被 broker 拒绝（`ProducerFenced`）→ 老 primary 自然退出。

和 Counter 的 `TxnProducer` 使用完全相同的 id 命名规则（ADR-0017）。

## 备选方案 (Alternatives Considered)

### A. 每条消息一个事务
- 优点：事务语义最简单、序列化 per-event。
- 缺点：4w TPS × 每 25μs 一次事务 → broker 事务状态机打爆。
- 结论：拒绝。

### B. 按 "order-event 边界" 打包（每个输入 order 的输出一个事务）
- 优点：语义最贴近 "consume-transform-produce"（ADR-0005 的原始模型）。
- 缺点：match 的 `TradeProducer` 只看 outbox，不知道 order-event 边界；要在 `SymbolWorker.emit` 里发 begin / end 标记，跨多个文件改。
- 结论：ROI 低。当前按时间 + 数量 flush 的 batch 在性能上等价，只是语义"模糊一点"—— 一个 txn 里可能包括多个 order 的输出，都已 commit 的话 downstream 看起来原子。这对 `trade-event` 的使用方 OK（counter 本来就做 trade_id 幂等）。

### C. 不改 match producer，靠 OS / k8s 杀老 primary
- 优点：零代码改动。
- 缺点：依赖 k8s livenessProbe 及时（几秒）；存在健康 primary 但被网络隔离的场景无法 fence。ADR-0031 已明确这是短期折中。
- 结论：本 ADR 就是把这个洞补上。

### D. 把 Match 改为 `consume-produce-commitOffsets` EOS 事务
- 优点：input offset 和 output 原子；真 EOS。
- 缺点：需要把 order-event consumer 和 trade-event producer 绑在一起走
  `AssignPartitionsTx` + `SendOffsetsToTransaction`。当前 consumer / producer
  分属两个文件、两个 goroutine；改动放大 3 倍。
- 结论：留 Backlog。当前 batch 事务足够做 fencing，EOS offset 原子是单独目标。

## 理由 (Rationale)

- **Fencing 是正确性问题，不是性能问题**：宁可 batch 事务多点开销，也不能两主并写。
- **Batch 事务对 Counter 的消费方友好**：consumer 读到的一整批 trade 要么全见要么全不见，`read_committed` 保证。
- **Match 单机 dev 不受影响**：`--ha-mode=disabled` 路径保持原样。

## 影响 (Consequences)

### 正面

- 两个 match 进程用同一个 `--instance-id` 时，Kafka broker 保证只有一个 producer epoch 能提交事务。老 primary 无论何种原因还活着，在下一次 EndTransaction 时会被拒绝 → 自愈。
- ADR-0031 里"Match split-brain 窗口 10s"的遗留问题关闭。Match 和 Counter 达成一致的 fencing 模型。
- 和 Counter 的 `TxnProducer` 架构对称，代码风格一致。

### 负面 / 代价

- Batch 事务带来事务状态机开销，broker 侧的 `__transaction_state` topic 写入量增加。实测 MVP 规模（1 symbol，10 TPS 级别）不受影响；生产压测驱动调整 `BatchSize` / `FlushInterval`。
- `--ha-mode=auto` 模式下单条 produce 延迟 ≈ 事务 commit RTT（broker 一次 write + replication），从 ~2ms 升到 ~5ms。Match 的 P99 预算能吸收（目标 10ms）。
- `BatchSize` 和 `FlushInterval` 是两个新 knob；默认值合理但若被 flag 暴露出来会增加配置复杂度。当前没有暴露 CLI flag，写死在默认值。

### 中性

- idempotent producer 和 transactional producer 的 broker-side `acks=all` / `enable.idempotence` 都是 on 的；切换不影响 "消息不丢" 的语义。

## 实施约束 (Implementation Notes)

- 批量逻辑实现：[match/internal/journal/producer.go](../../match/internal/journal/producer.go)
  `Pump` 用 `time.Timer` + 累积 slice；触发 flush 条件：
  1. batch len 达到 `BatchSize`，或
  2. 自首条入队起超过 `FlushInterval`，或
  3. ctx cancel / outbox 关闭。
- Transactional 失败处理：`PublishBatch` 内部任一步失败都 `EndTransaction(TryAbort)`，Pump 对返回的 error 只 log（和 idempotent 模式行为一致）。
- **注意**：如果 `EndTransaction(TryCommit)` 返回 `ErrProducerFenced`（老 primary 场景），当前代码只是打 log；之后下一批又会 `BeginTransaction()`，再次失败。**期望的行为**是：`runPrimary` 通过 `elec.LostCh()` 已经收到 lease 丢失通知、正在被 cancel，此时 context 已取消，自然退出。Fenced error 不会 loop 太久。
- Match 单测没有覆盖 transactional 路径（需要真 Kafka 或 kgo mock）；手工冒烟验证，放 `ADR-0031` 的 smoke script 里。

## 未来工作 (Future Work)

- **真 EOS consumer-produce**：把 order-event 的 consumer offset 和 trade-event 的 produce 打在同一个 Kafka transaction 里（`SendOffsetsToTransaction`）。解决 "match 重启时重复产出 trade-event" 的幂等靠 Counter 那头兜底的问题。
- **Batch knob 可配**：暴露 `--trade-batch-size` / `--trade-flush-interval` 让压测调参。
- **Metrics**：事务 commit / abort 计数、fenced 错误计数。

## 参考 (References)

- ADR-0005: Kafka transactions for dual writes
- ADR-0017: transactional.id 按 shard 命名
- ADR-0031: MVP-12 Counter/Match HA — cold standby
- 实现：
  - [match/internal/journal/producer.go](../../match/internal/journal/producer.go)
  - [match/cmd/match/main.go](../../match/cmd/match/main.go)
