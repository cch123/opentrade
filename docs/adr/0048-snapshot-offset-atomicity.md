# ADR-0048: Snapshot 与 Kafka offset 原子绑定 + output flush barrier（counter / match）

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0036（quote 同款策略）, 0032（match transactional producer）, 0006（快照未来工作）, 0031（HA cold standby）, 0005（Kafka 事务双写）

## 背景 (Context)

目前四个持久化内存态服务的快照/恢复策略不一致：

| 服务 | snapshot 含 offset | 启动 seek 用 snapshot offset |
|---|---|---|
| quote | ✅ ([ADR-0036](./0036-quote-state-snapshot.md)) | ✅ |
| conditional | ✅ | ✅ |
| **match** | ⚠️ 字段有 (`snapshot.KafkaOffset`) 但启动不使用 | ❌ 靠 Kafka consumer group |
| **counter** | ❌ 字段都没有 | ❌ 靠 Kafka consumer group |

counter 和 match 现在的恢复路径是："primary 重启 → 加载 snapshot 恢复内存态 → Kafka client 以 consumer group 身份 resume，从 broker 上 committed offset 续消费"。

问题：**consumer group offset commit 和 snapshot 文件落盘不原子**。典型的失败窗口：

1. consumer poll 到 record at offset X
2. handler 处理完，内存状态前进到 X+1
3. Kafka `CommitUncommittedOffsets` 把 X+1 回刷到 broker ✅
4. snapshot 还没 tick 到、或者 tick 了但 `rename` 失败
5. 进程 crash
6. 重启：snapshot 里是某个更老的 Y < X；Kafka offset 是 X+1 → consumer 从 X+1 开始 → **[Y+1, X] 段事件永远丢失**

counter 的 `dedup.Table` 只覆盖用户发起的 Transfer（按 `transfer_id`），撮合回来的 trade-event 没有任何幂等/水位防护；match 的 sequencer 只管生成 out seq，不做 in 端 dedup。所以"丢事件"会直接变成"账户余额错误 / orderbook 状态残缺"。

ADR-0036 已经为 quote 解决了同类问题（quote 的错误表现是 depth / kline 错位）。本 ADR 把同一模式推广到 counter 和 match，让"snapshot 是位点的唯一权威"成为全项目的统一范式，也为 [ADR-0006](./0006-snapshots-by-backup-node.md) 的 backup-node 快照 / S3 上传铺路。

### 结合分布式快照理论：两类 in-flight 消息

本项目的快照范式不是 Chandy-Lamport 式的 global snapshot（无 marker、无协同切面），而是 **log-based checkpoint**：Kafka 持久 log 充当 durable channel，每个服务独立打 `(local state, input offsets)`，恢复时 seek offset → Kafka 自动"重放 channel state"。

这个范式对 **input in-flight**（上游已发、本服务未消费）是完备的。但它默认忽略了 **output in-flight**（本地 state 已推进、output event 还没成功 commit 到下游 Kafka）。后者在 Chandy-Lamport 里对应 channel state 捕获，在 log-based checkpoint 里必须靠额外机制补齐。

match / counter 都有这种 gap：

```
T1: handler 处理 input offset X → state 推进、seq++、emit output event Y 到内部 buffer
T2: snapshot ticker 触发，Capture offsets[p]=X+1 + 新 state → Save 成功
T3: Pump / TxnProducer 才开始攒批、开事务、commit 事件 Y 到 Kafka broker
T4: CRASH（事务尚未 commit）
---
T5: 重启 → load snapshot → seek 到 X+1
T6: Y 永远不会被重新 emit → 下游永远收不到 → 状态链路错账
```

这直接影响到 [ADR-0032](./0032-match-transactional-producer.md) 的假设。ADR-0032 让 match 的 transactional producer 把 **consumer offset commit 捎带进 Kafka 事务**，完成 read-process-write EOS 闭环。本 ADR-0048 把 offset 权威从 broker 搬到 snapshot 文件，等于单方面废掉了这个闭环 —— 必须在 Capture 前显式排空 output buffer / 确认事务 commit，才能让 "snapshot 里的 offset X+1" 重新等价于 "≤X 的所有 output 都已到达下游 Kafka"。

## 决策 (Decision)

核心原则三条（前两条已写入 claude memory `feedback_snapshot_offset.md`，作为项目工程约束）：

1. **Capture 原子性**：snapshot 必须在同一把锁 / 同一次原子落盘里，同时写入业务状态 + per-partition `{topic, partition, offset}`
2. **Restore 权威性**：启动时用 snapshot 里的 offsets 调 `AdjustFetchOffsetsFn` 精确 seek；consumer 不再调 `CommitUncommittedOffsets`，Kafka broker 上的 consumer group offset 仅作监控参考，不作恢复权威
3. **Output flush barrier**：Capture 前必须同步 flush 所有 output producer / Pump 的内部 buffer，等 Kafka 端确认事务 commit 完成，再读 state + offsets。**这条保证 snapshot 里的 offset X+1 等价于"≤X 的 input 对应的所有 output 都已到达下游 Kafka"**，恢复 ADR-0032 事务性 EOS 的语义闭环

### 1. match 改动

snapshot struct 保留既有 `Offsets []KafkaOffset` 字段（已存在）；实际填充与使用落实到位：

- `sequencer.SymbolWorker` 新增 `offsets map[int32]int64`（worker goroutine 单线程访问，无锁）
- `handle(evt)` 完成后 `offsets[evt.Source.Partition] = evt.Source.Offset + 1`
- `SymbolWorker.Offsets() map[int32]int64`、`SetOffsets(map[int32]int64)` 对 snapshot 包暴露
- `snapshot.Capture(w, _, ts)` 第二参数 `offsets` 改为从 `w.Offsets()` 读取（调用方 match main 不再需要手搓 `[]KafkaOffset`）
- `snapshot.Restore(w, snap)` 把 `snap.Offsets` 灌回 `w.SetOffsets(...)`
- `journal.OrderConsumer` 新增 `InitialOffsets map[int32]int64` + `AdjustFetchOffsetsFn`；移除 `CommitUncommittedOffsets` 调用
- `match/cmd/match/main.go` 启动时：`registry.Restore` 把每个 symbol 的 snapshot offsets 回灌 worker；**跨所有 symbol worker 取每 partition 的 MIN offset** 作为 consumer `InitialOffsets`；没有 snapshot 的 partition 回落到 `AtStart`
- **Output flush barrier**：`journal.TradeProducer.Pump` 暴露 `FlushAndWait(ctx) error`，强制攒批 + 结束当前事务 + 等所有 record 被 broker 确认；`registry.SnapshotAll()` 调用前先跑 `dispatcher.FlushProducer(ctx)`。周期 ticker 和 graceful shutdown 都走这条路径

### 2. counter 改动

snapshot struct `ShardSnapshot` 新增 `Offsets []KafkaOffset`（结构与 match 一致）；Version bump 到 2。

- `service.Service` 新增 shard 级 `offsets map[int32]int64`（`offsetsMu sync.Mutex` 保护；读频率低，锁竞争小）
- `HandleTradeEvent` 签名保持不变，新增兄弟方法 `HandleTradeRecord(ctx, evt, partition, offset) error`：在 `HandleTradeEvent` 成功返回后原子推进 `offsets[partition] = offset + 1`
- `journal.TradeConsumer` 改为调 `HandleTradeRecord`；新增 `InitialOffsets` + `AdjustFetchOffsetsFn`；移除 `CommitUncommittedOffsets` 调用
- `snapshot.Capture/Restore` 持久化 offsets；Restore 把 offsets 回灌 Service
- `counter/cmd/counter/main.go` 把 Restore 返回的 offsets 作为 consumer `InitialOffsets`；cold start（无 snapshot）继续 `AtStart`，和老行为一致
- **Output flush barrier**：Counter 的 `TxnProducer` 已经按 journal 粒度打事务（每个 Transfer / Settlement 一个 tx），但 `Publish` 不一定 sync 等 broker ack。Capture 前显式调 `TxnProducer.Flush(ctx)` → `kgo.Client.Flush` + `EndTransaction(TryCommit)` 语义的强 barrier，确保所有已返回 `HandleTradeRecord` 成功的 event 都已 Kafka 事务 commit
- Snapshot Version 迁移：Load 遇到 Version=1（老快照）时，依然可以加载业务状态，但 `Offsets` 为空 → consumer 会回落 `AtStart`，等价于一次全量重扫；重扫依赖 Kafka retention 能覆盖，MVP 默认保留 7 天足够。Operator 升级路径：停旧 → 起新（一次性重扫）→ 第一次 tick 后写出 Version=2 snapshot → 后续正常热重启

### 3. Capture 流程的新顺序

所有三个服务（本 ADR 改 match/counter；quote / conditional 将后续对齐）的 Capture 调用点必须严格遵守：

```
1. FlushOutput(ctx)          // 阻塞直到所有 in-flight output 事件 Kafka 已 ack
2. Capture(state, offsets)   // 同一把 lock 下读快照
3. Save(path, snap)          // atomic tmp + fsync + rename
```

三步之间不能有事件被处理（Capture 的锁本身会阻塞 handler）。

失败处理：FlushOutput 超时（默认 3s）→ 记 ERROR 日志、跳过本轮 snapshot，下一轮再试；**不能**在 flush 未完成时打快照（否则就是本 ADR 要根治的 race）。graceful shutdown 路径给一个更长的 timeout（默认 10s），必要时放弃 final snapshot，走 cold start fallback。

### 4. match 多-symbol-同-partition 的 min 取法

match 的 order-event topic 按 user_id (或 symbol) 分区，一个 partition 内可能携带多个 symbol 的事件。不同 symbol 消费进度不同步（每个 worker 独立 goroutine），于是同一 partition 在不同 worker 的 `offsets[p]` 值会不同。

恢复点必须是"任何 worker 都不会漏事件"的位置 → 跨所有 owned symbol 取 `min(offsets[p])`。代价是：进度最慢的 worker 会拖慢其他 worker，重启后会重放 `[min, max]` 段。重放经 `book.Has(o.ID)` / `orderbook.Cancel` not-found 等既有防御自然幂等（match 侧防御纵深）。

## 备选方案 (Alternatives Considered)

### A. 仅引入 seq_id / event_id 幂等守卫，不存 offset

- 在 trade-event handler 入口加 "if evt.SeqID <= lastSeqID { skip }"
- 优点：改动最小
- 缺点：**只能保证"重放安全"，不能保证"不丢事件"**。crash 窗口里 Kafka commit 领先 snapshot 的根因没解决，事件可能永远消费不到（consumer 从更晚位置开始）
- 结论：拒。offset 是 Kafka 层位点，必须在 Kafka 层解决

### B. Kafka exactly-once（transactional consumer 的 read-process-write 闭环）

- 把 snapshot 写入当作"写"，与 consumer offset commit 绑同一事务
- 缺点：snapshot 是本地文件，不是 Kafka topic；要不把 snapshot 落到 Kafka（违反 ADR-0036 JSON 本地方向），要不引入 2PC（复杂度远超收益）
- 结论：拒，保留 consumer 层至少一次 + 本地 snapshot 作为 offset 权威

### C. 让 backup 节点承担打快照（ADR-0006 目标态）

- 和本 ADR 正交：不管是 primary 还是 backup 打，"snapshot 要含 offset、restore 要用 offset"这条都成立
- backup 打的方案还涉及 Kafka 双消费、状态复刻、切主交接，工程量大
- 结论：本 ADR 只把 primary-only 模式做对，backup 打快照延后

### D. 只在 graceful shutdown 打快照，避免周期 tick 的 output race

- 优点：shutdown 时 Pump 有充足时间 drain，不需要 flush barrier 也能对
- 缺点：失去 ADR-0036 / 0042 的热重启收益；kill -9 / OOM / 硬件故障路径无 snapshot 保护；和项目范式背道而驰
- 结论：拒。周期 snapshot 是热重启的前提，flush barrier 是可接受的小成本

### E. 接受 output in-flight 丢失，靠下游对账修补

- conditional 走这条（[ADR-0047](./0047-conditional-long-term-history.md) 非事务 producer + 队列满即丢 + WARN 日志）
- conditional 的输出是审计用的 journal，业务链路不依赖其完整性
- counter / match 的输出是账户 / 撮合主链路，丢一条就是错账 / 残单 → 不可接受
- 结论：conditional 保持现状；counter / match 必须走 flush barrier

### F. snapshot 按 partition 拆成多文件

- 每个 partition 一份 offset + 对应 symbol/user 子集
- 优点：partition 扩容时不用 min 合并
- 缺点：match 每 symbol 快照模型已在，partition 粒度要再加一层；收益不对称
- 结论：保留现有粒度（match per-symbol、counter per-shard）

## 理由 (Rationale)

- **和 quote/conditional 对齐**：四个状态服务用同一恢复范式，新人一次学清
- **独立于 HA 演进**：cold standby（ADR-0031）下 primary-only；未来 backup 上线后也无需重写恢复逻辑，只是打快照的主体从 primary 改为 backup
- **迁移代价可控**：match snapshot 格式已有字段，counter 靠 Version bump + 一次重扫即可过渡；无需停机、无需数据迁移脚本
- **防御纵深依然保留**：match 的 `book.Has(id)` 拒重、counter 的 `dedup.Table` 按 `transfer_id` 去重、account_logs PK 去重都不动 —— offset seek 是第一道防线，业务幂等是第二道

## 影响 (Consequences)

### 正面

- 消除 "Kafka commit 超前 snapshot" 的丢事件窗口
- snapshot 成为恢复点的**唯一**权威，排查故障只看一个文件
- 为 ADR-0006 backup-node snapshot / S3 上传提供统一 schema

### 负面 / 代价

- counter snapshot schema 升版（Version 1 → 2），一次重扫迁移
- match 重启后可能重放 `[min, max]` 段（进度最慢 worker 拖累），但经幂等防御无副作用
- Kafka consumer group offset 在 broker 上不再推进，`kafka-consumer-groups.sh --describe` 的 lag 指标失真 —— 替代观测：snapshot 里的 offsets vs topic HWM
- **snapshot tick 被 producer flush 阻塞**：match 按 ADR-0032 的 `FlushInterval=10ms` / `BatchSize=32` 计算，最坏阻塞 ~10ms；counter TxnProducer 一般 p99 <5ms。对 60s / 30s snapshot 周期可忽略
- **flush 失败会跳过 snapshot**：下一轮再试；如果连续多轮失败代表 producer 链路挂了（不是 snapshot 问题），需要单独告警
- 代码改动：新增 consumer 配置字段、worker/service 加 offsets 字段、snapshot 包增字段 + 版本迁移、Producer/Pump 增 FlushAndWait；合计预计 ~450 LOC + 测试

### 中性

- snapshot JSON 每个分区多几十字节（按 32 partition 算 < 1KB/文件），可忽略

## 实施约束 (Implementation Notes)

### 迁移步骤

1. 改 snapshot / consumer / main，保留 Version=1 的 Load 容错分支
2. 部署新版：primary 启动时 Load 老 snapshot → business state 恢复，offsets 为空 → consumer fallback `AtStart` 一次性重扫
3. 第一次周期 tick 写出 Version=2 snapshot
4. 后续重启都走 snapshot offset seek
5. 下一版 release 可以删 Version=1 容错分支（或保留作防御）

### Flag

不新增 flag。既有：
- counter: `--snapshot-interval=60s`（已有）、`--snapshot-dir`（已有）
- match: `--snapshot-interval=60s`（已有）、`--snapshot-dir`（已有）

### 测试

- 单元：SymbolWorker offsets 推进、跨重启 Capture/Restore roundtrip
- 集成：模拟 commit 早于 snapshot 的 crash → 新实现重启后事件不丢
- 兼容：Version=1 老 snapshot load 成功、offsets 为空 → consumer fallback AtStart

### 失败场景

| 情况 | 结果 |
|---|---|
| snapshot 不存在 | cold start，consumer `AtStart`（与现状一致） |
| snapshot Version=1（老） | 业务状态 load，offsets 为空 → `AtStart` 重扫，重扫后下一次 tick 升级到 Version=2 |
| snapshot JSON 损坏 | 启动失败（和 counter/match 现状一致，operator 清理） |
| 新增 partition | `AdjustFetchOffsetsFn` 默认分支 → `AtStart`，自动 backfill |
| snapshot offsets > HWM | consumer 会等 HWM 追上，不是错误（broker 侧反压） |
| Flush 超时 | 跳过本轮 snapshot，ERROR 日志；下轮 tick 再试；连续 N 轮失败触发 producer 健康告警（N 留给实现） |
| graceful shutdown flush 超时 | 放弃 final snapshot；下次启动等价于 cold start（Kafka 重扫）；日志 WARN |

## 未来工作 (Future Work)

- **account-level version**：在 `Balance` / `Account` 加 version 字段做更细粒度幂等，配合 trade-event seq_id guard。与 offset seek 正交、可叠加
- **backup 节点打快照**（[ADR-0006](./0006-snapshots-by-backup-node.md)）：primary fencing + backup 消费到最新 → backup 打 snapshot 上传 S3 → primary 重启直接拉最新
- **Protobuf 化**：全部四个服务 snapshot 从 JSON 换 protobuf；和 format 可选化一并考虑

## 参考 (References)

- [ADR-0036](./0036-quote-state-snapshot.md) — quote 同款策略
- [ADR-0006](./0006-snapshots-by-backup-node.md) — backup-node snapshot 方向
- [ADR-0031](./0031-ha-cold-standby-rollout.md) — HA cold standby
- [ADR-0015](./0015-idempotency-at-counter.md) — counter 侧幂等（match 侧 `book.Has(id)` 为防御纵深）
- 实现：
  - [match/internal/snapshot/snapshot.go](../../match/internal/snapshot/snapshot.go)
  - [match/internal/sequencer/worker.go](../../match/internal/sequencer/worker.go)
  - [match/internal/journal/consumer.go](../../match/internal/journal/consumer.go)
  - [counter/snapshot/snapshot.go](../../counter/snapshot/snapshot.go)
  - [counter/internal/service/trade.go](../../counter/internal/service/trade.go)
  - [counter/internal/journal/trade_consumer.go](../../counter/internal/journal/trade_consumer.go)
