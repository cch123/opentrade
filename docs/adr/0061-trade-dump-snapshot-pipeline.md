# ADR-0061: Trade-Dump Snapshot Pipeline（Shadow Replay + Counter Recovery 供给）

- 状态: Accepted（M1–M4 + M7 + M8 落地，直接跳 Phase A 并行对比，进入 Phase B：Counter 不自产 snapshot，由 trade-dump 独占）
- 日期: 2026-04-22
- 决策者: xargin, Claude
- 相关 ADR: 0001（Kafka SoT）、0017（Kafka transactional.id）、0018（per-user sequencer + counterSeq）、0023（trade-dump MySQL 投影）、0048（snapshot + offset 原子绑定，本 ADR 承接 recovery 角色）、0049（snapshot protobuf/json）、0057（Asset service + Transfer Saga）、0058（Counter 虚拟分片 + 实例锁）、0059（Kafka 集群扩容长期调研 §A.5.5 本 ADR 是其落地）、0060（Counter 消费异步化，提供 TECheckpoint event + recovery 协议契约）、0062（订单终态 evict + OrderEvicted event）

## 术语 (Glossary)

| 本 ADR 字段 | 含义 | 业界对标 |
|---|---|---|
| `trade-dump snapshot pipeline` | trade-dump 服务内部新增的 pipeline，和现有 MySQL 投影 pipeline 并列 | ADR-0059 §A.5.5 推荐架构 |
| `shadow engine` | snapshot pipeline 内部维护的一份 Counter 状态复制品，按 counter-journal 事件 apply | Kafka Streams 的 state store |
| `shadow state` | shadow engine 的内存状态，per-vshard 一份（256 份），结构与 Counter `engine.ShardState` 同构 | — |
| `te_watermark` | snapshot pipeline 维护的每 vshard 的 trade-event offset 水位，由消费到 `TECheckpointEvent` 推进 | ADR-0060 定义 |
| `journal_offset` | snapshot pipeline 记录的 counter-journal partition 的下次消费位置，写进 snapshot 供 Counter recovery catch-up 起点 | — |
| `snapshot_counter_seq` | snapshot 内记录的 `s.counterSeq` 最大值（从 apply 过的 journal event 中推出） | ADR-0018 |
| `cold start 依赖` | Counter 启动依赖 trade-dump 已至少产出一份 snapshot；未上线前或灾难恢复需走冷 rebuild 工具 | — |
| `snapshot 生产者切换` | Counter 从"自产 snapshot"切到"读取 trade-dump 产出的 snapshot"，配套撤销 ADR-0060 M7 的 snapshotMu | — |

## 背景 (Context)

### ADR-0060 暴露的根本问题

ADR-0060 把 Counter 消费循环异步化了，但 Counter 自产 snapshot 的 cross-account 不一致问题（`sync.Map.Range` 边遍历边写）依然存在 —— ADR-0060 的 M7 只是加一把 `snapshotMu` 临时修复，没从架构上解。

根本解：**Counter 不再自产 snapshot**，改由独立 consumer 从 counter-journal replay 生成 snapshot。因为 Counter 状态本质 = `apply(counter-journal events from offset 0 to N)`，snapshot 可以从 journal 重建，不必读主内存。

### 为什么放在 trade-dump 内做，而不是新起服务

详见 ADR-0059 §A.5.5 的决策：

- trade-dump 已有"消费 Kafka → 聚合状态 → 持久化" 的基础设施（现有 MySQL pipeline）
- snapshot pipeline 的骨架和 MySQL pipeline **同构**，只是 sink 从 MySQL 换成 S3/EFS
- 共享 Kafka consumer / proto / monitoring 基础设施
- 新起独立服务成本高（CI / 部署 / 监控 / 值班各一套），Bybit 拆独立 trading-dump 服务是因为其团队规模支撑得起

### 硬约束（来自 ADR-0059 §A.5.5）

- 独立 consumer group（`trade-dump-snapshot`）
- 独立 offset 管理
- 独立故障域（snapshot pipeline 挂不影响 MySQL pipeline，反之亦然）
- 独立资源隔离
- **严禁"一次消费双写 MySQL + S3/EFS"**

### 为什么现在做

- ADR-0060 的 M7 是显式标记"过渡方案"，本 ADR 是其归宿
- ~~ADR-0062 的 `OrderEvictedEvent` 需要 shadow engine apply，协议在本 ADR 定义~~ —— ADR-0063 已移除 `OrderEvictedEvent`，shadow engine 通过 `OrderStatusEvent` 的终态分支隐式删 byID
- 未上线，breaking change 无顾虑（MEMORY）

## 决策 (Decision)

### 1. 部署形态：trade-dump 内部新 pipeline

```
┌──────────────────────────────────────────────────────┐
│                   trade-dump binary                  │
│                                                      │
│  ┌────────────────────┐  ┌─────────────────────────┐ │
│  │ MySQL pipeline     │  │ Snapshot pipeline       │ │
│  │ (ADR-0023 已有)    │  │ (本 ADR 新增)           │ │
│  │ group:             │  │ group:                  │ │
│  │   td-journal       │  │   td-snapshot           │ │
│  │   td-trade         │  │ 消费:                   │ │
│  │   td-asset         │  │   counter-journal (256) │ │
│  │   td-cond          │  │ 状态:                   │ │
│  │ sink: MySQL        │  │   shadow engine × 256   │ │
│  │                    │  │ sink: S3/EFS (per vshard)│ │
│  └────────────────────┘  └─────────────────────────┘ │
│            │                        │                │
│            └──── 共享基础设施 ───────┘                │
└──────────────────────────────────────────────────────┘

部署方式:
  --pipelines=sql,snap    (同进程双 pipeline, 初期默认)
  --pipelines=sql          (仅 MySQL)
  --pipelines=snap         (仅 snapshot, 独立扩容时使用)
```

### 2. Shadow Engine 架构

```go
// trade-dump/internal/snapshot/shadow/engine.go (新建)
type ShadowEngine struct {
    vshardID   int
    state      *engine.ShardState   // 复用 counter 的 engine 包
    counterSeq uint64                // 等同 Counter 的 s.counterSeq, 从 apply 过的 event 推出
    teWatermark map[int32]int64      // 每 te_partition 的水位 (per-vshard 只有一个 partition, 退化为单值)
    journalOffset int64              // 本 vshard 的 counter-journal partition 的下次消费位置
    lastAppliedEventOffset int64     // 最近一次成功 apply 的 journal record offset (== journalOffset - 1)
}

// Apply 对应 Counter Service 里的同名逻辑 (幂等)
func (e *ShadowEngine) Apply(evt *CounterJournalEvent, kafkaOffset int64) error {
    switch body := evt.Body.(type) {
    case *SettleEvent:      return e.applySettle(body)
    case *FreezeEvent:      return e.applyFreeze(body)
    case *TransferEvent:    return e.applyTransfer(body)
    case *OrderStatusEvent: return e.applyOrderStatus(body)  // 终态时同步 Delete byID（ADR-0063）
    case *TECheckpointEvent: return e.applyCheckpoint(body)  // 推进 teWatermark
    ...
    }
    e.counterSeq = max(e.counterSeq, evt.CounterSeqId)
    e.journalOffset = kafkaOffset + 1
}
```

**重用 `counter/engine` 包**：Counter 和 shadow engine 共用 `engine.ShardState` / `engine.Account` / `engine.OrderStore` 等核心数据结构。apply 函数（`applySettle` / `applyFreeze` / ...）以纯函数形式暴露（不依赖 Kafka producer / gRPC server），两边各自调用。

**不共享**：网络 I/O、sequencer（shadow engine 单线程 apply，不需要 per-user sequencer）。

### 3. Apply 协议（和 ADR-0060 的 event 对齐）

| Journal Event | Shadow Apply 动作 | 幂等机制 |
|---|---|---|
| `SettleEvent` | 更新 balance + order.filledQty + match_seq | `(user, symbol, match_seq)` guard（ADR-0018）|
| `FreezeEvent` | 更新 balance.available/frozen + insert Order | client_order_id + counter_seq |
| `TransferEvent` | 更新 balance + `RememberTransfer(transfer_id)` | transfer_id ring（ADR-0057）|
| `OrderStatusEvent` | 更新 Order.Status；终态转换时同步 `state.Orders().Delete(order_id)`（ADR-0063） | order_id + status terminal check；Delete 是 ErrOrderNotFound-safe |
| `TECheckpointEvent` (ADR-0060) | 推进 `teWatermark[te_partition] = te_offset + 1` | 单调推进，低于当前水位的 checkpoint 忽略 |
| ~~`OrderEvictedEvent` (ADR-0062)~~ | 已由 ADR-0063 移除 —— 终态由 `OrderStatusEvent` 隐式承担 | — |

**所有 apply 严格幂等**：重启时 shadow engine 从上次 commit 的 offset 开始，重复 apply 不改变最终状态。

### 4. Snapshot 生成协议

#### 4.1 触发条件

每 vshard 独立，按以下组合触发：

- **时间窗**：距上次 snapshot ≥ `SnapshotInterval`（默认 10 秒）
- **事件窗**：距上次 snapshot 已 apply ≥ `SnapshotEventCount` 条 journal event（默认 10000）
- 哪个先触发就打

#### 4.2 触发时点的原子性

Snapshot pipeline 内部 **shadow engine 是单线程 apply**（一个消费 goroutine per vshard），**snapshot 生成和 apply 天然串行**：

```go
// snapshot pipeline 的 per-vshard 主循环
for record := range consumer.recordsForVshard(vshardID):
    shadow.Apply(record)
    if shouldSnapshot(shadow):
        snap := shadow.Capture()  // 同步 Capture, 此刻无并发 apply
        go asyncSave(snap)         // 序列化 + S3/EFS 上传丢后台
```

**关键性质**：Capture 期间 shadow state 无并发写（单线程 apply 暂停），产出的 snapshot 对应 `journal_offset` 之前的所有 event 的一致状态。这是 ADR-0060 M7 snapshotMu 想达到但做得不干净的效果，在这里天然免费。

#### 4.3 Snapshot 内容

```protobuf
message ShardSnapshot {
  // 现有字段 (accounts / orders / seq / ...)
  
  // ADR-0060 扩展字段
  uint64 snapshot_counter_seq = ??;
  int64  journal_offset       = ??;
  int64  te_watermark         = ??;  // 本 vshard 对应 te_partition 的 watermark
  
  // ADR-0062 扩展字段
  // accounts 里的 AccountSnapshot 已含 recent_terminated_orders
}
```

#### 4.4 持久化路径

- 存储介质：**S3 / MinIO**（沿用 ADR-0058 现有基础设施）
- Key 格式：`{prefix}vshard-{000..255}.pb`（和当前 Counter 自产 snapshot 同名，便于平滑切换）
- 原子写：使用 S3 multipart upload + 完成后 copy 到正式 key（避免半写入被 Counter 读到）
- 保留：每次覆盖，另加定期 archive copy（`{prefix}archive/vshard-NNN-{timestamp}.pb`，7 天保留）

### 5. Counter 侧的 Snapshot 生产者切换

本 ADR 落地分两阶段：

#### Phase A：并行运行

- trade-dump snapshot pipeline 启动，开始产出 snapshot 到 S3
- Counter 继续自产 snapshot（ADR-0060 M7 的 snapshotMu 仍生效）
- 双写：两份 snapshot 文件 key 不同（`vshard-NNN.pb` Counter 自产 + `td-vshard-NNN.pb` trade-dump 产）
- 在灰度环境对比两份 snapshot 的一致性：
  - 相同 te_watermark 下的 state 是否一致
  - 资产守恒断言（所有 user 的 balance 总和守恒）
- 运行 1-2 周，灰度充分

#### Phase B：切换

- Counter recovery 改为从 trade-dump 产出的 snapshot 读取（key 改名为 `vshard-NNN.pb`，Counter 自产改名为 `legacy-vshard-NNN.pb`）
- Counter 停止自产 snapshot：`SnapshotInterval = 0`
- **撤销 ADR-0060 M7 的 `snapshotMu`**（不再需要）
- Counter self-restore 路径仍保留（作为灾难兜底：trade-dump snapshot 全丢时，Counter 可以从自己最后一次 snapshot + journal catch-up 手动 rebuild）

Phase B 完成即本 ADR 的核心目标达成。

### 6. Consumer Group 与 Offset

- Snapshot pipeline 用独立 consumer group `trade-dump-snapshot`
- 消费 counter-journal（256 partition）
- 每 vshard 一个独立消费逻辑（kgo 的 `ConsumePartitions` assign 模式或 group auto-assign 均可，倾向 assign 模式以保证每 partition 独立 goroutine + 独立 snapshot 节奏）
- Offset commit 策略：**只在 snapshot.Save 成功后** commit 到 `journal_offset`
  - 保证重启后 shadow state（由 snapshot restore）和 consumer offset 对齐
  - 避免"offset committed 但 snapshot 未 save" 的窗口漏事件

### 7. 健康度与监控

- `trade_dump_snapshot_age_seconds{vshard}` — 最近一份 snapshot 的时间距今
  - 超过阈值（默认 5 分钟）告警
  - ADR-0062 的 evictor 健康检查读此指标（超 2h 不更新时熔断 evict）
- `trade_dump_snapshot_consumer_lag{vshard, partition}` — 消费 lag
- `trade_dump_snapshot_capture_duration_ms{vshard}` p99 — Capture 耗时
- `trade_dump_snapshot_save_duration_ms{vshard}` p99 — S3 上传耗时
- `trade_dump_snapshot_apply_error_total{vshard}` — apply 失败计数

### 8. Cold Start 约束

- Counter 必须先有至少一份 trade-dump snapshot 才能启动正常路径
- 部署顺序：**trade-dump 先上线产出 snapshot → Counter 启动**
- 首次集群启动（还没 snapshot）：
  - Counter 允许 fallback 从 trade-event offset 0 开始 replay（`ConsumePartitions` 的 `AtStart`）
  - 时间长（GB 级 trade-event），作为一次性冷启动可以接受
  - 启动时间预算写进部署 runbook
- 灾难恢复（snapshot 全丢）：走冷 rebuild 工具（单独实现），不走正常启动流程

## 备选方案 (Alternatives Considered)

### 备选 1：Counter 进程内起 shadow goroutine

shadow engine 和主 engine 同进程，独立 consumer group 消费 counter-journal。

**拒绝（ADR-0059 §A.5.5 已讨论）**：
- 内存翻倍（Counter 进程同时持两份完整 state）
- CPU 翻倍
- Counter 进程复杂度上升
- shadow 故障传导到主 engine

### 备选 2：新起独立 counter-dump 服务

**拒绝**：运维成本高（新 CI / 部署 / 监控 / 值班），团队规模不支撑（ADR-0059 §A.5.5 的 Bybit vs OpenTrade 分析）。

### 备选 3：Counter 加完整 COW / immutable state，保持自产 snapshot

**拒绝**：改动面巨大（整个 engine 要重写），收益仅限 snapshot 一致性，投资回报率远低于本 ADR 方案。

### 备选 4：用 Kafka Streams / Flink 做 shadow engine

**拒绝**：
- 引入 JVM / Flink 运维面
- OpenTrade 整体 Go 栈，跨语言运维成本高
- Flink checkpoint 机制 vs 自己管 offset 的复杂度差异不大

### 备选 5：Shadow 引擎 apply 先入 MySQL 再读回来生成 snapshot

**拒绝**：MySQL 不是 Counter state 的 1:1 映射（MySQL 是扁平化投影，忽略 match_seq / counterSeq 等内部字段）；走 MySQL 会丢信息。

## 理由 (Rationale)

### 为什么 snapshot 生产必须从 journal replay 而不是读主内存

ADR-0060 分析过：读主内存 + 并发写 = cross-account 不一致；加全局锁 = stop-the-world。从 journal replay 是第三条路：shadow engine 单线程消费 journal，本身无并发写，Capture 天然一致。这也是 Kafka Streams / Flink 等流处理系统的标准设计。

### 为什么共享 `engine` 包而不是独立实现

- Counter 和 shadow 的 apply 逻辑必须严格一致，否则 shadow state 和 Counter state 会漂移
- 两份实现的代码同步会是长期维护负担
- 现有 `engine` 包已经是纯函数风格（apply* 方法不依赖 Counter 的 Kafka / gRPC），直接复用

### 为什么不把 MySQL pipeline 和 snapshot pipeline 合一

详见 ADR-0059 §A.5.5.3 的四条硬约束：故障域 / 一致性语义 / 资源画像 / 迭代节奏都不同，合一会纠缠。共享基础设施（同 binary / 同 repo）但独立 consumer group + 独立 pipeline。

### 为什么 Phase A / Phase B 分两步

直接切换有风险：trade-dump snapshot pipeline 可能有实现 bug，Counter 依赖它启动可能导致生产事故。Phase A 的并行运行 + 灰度对比是必要的 safety net。

### 为什么 S3 不换 EFS

ADR-0059 附录 A 讨论过：**如果切 per-user snapshot** 需要 EFS（小文件 + NFS 元数据友好）。本 ADR 保持 per-vshard snapshot（256 份文件），S3 完全够用（甚至更好：跨 AZ / 无状态 / 便宜）。

EFS 切换是 ADR-0059 附录 A 的长期调研方向，不在本 ADR scope。

## 影响 (Consequences)

### 正面

- Snapshot 一致性从根本上解决（ADR-0060 M7 撤销）
- Counter 变瘦（不再自产 snapshot，消费 / 恢复路径单纯化）
- trade-dump snapshot pipeline 提供的 TECheckpoint watermark 成为生态基础设施（ADR-0062 的 evictor 健康检查直接复用）
- Shadow engine 的独立存在使得未来"灰度对比"机制（比较 Counter 实际 state 和 shadow state 的差异）成为可能，帮助未来 audit

### 负面 / 代价

- trade-dump 进程内存 / CPU 占用显著上升（shadow engine 256 份完整 state）
- S3 写入频次增加（以前 Counter 每节点 256 vshard 各自写，现在 trade-dump 单点写所有 vshard）
- 代码组织复杂度增加（engine 包的跨服务复用要 carefully 设计接口）
- Phase A 灰度期间 snapshot 双写存储成本 × 2

### 中性

- MySQL pipeline 完全不受影响
- 现有 Counter snapshot 代码保留为 legacy fallback，不立即删除

## 已知不防御场景 (Non-Goals / Known Gaps)

### G1: Trade-dump snapshot pipeline 挂机

Snapshot 不再产出新版本，Counter 灾难恢复的 RPO 停在最后一份 snapshot 的时间。

**不防御原因**：snapshot pipeline 本身 HA 靠 consumer group rebalance（独立 consumer group，multiple trade-dump 实例抢 partition），不防御 trade-dump 服务整体挂机。

**缓解**：
- trade-dump 部署多副本
- ADR-0062 G1 的"evictor 健康检查熔断"在 snapshot lag 过大时停 evict

### G2: Shadow engine apply 和 Counter apply 逻辑漂移

如果 `engine` 包的 apply 函数有 bug 且不同版本的 Counter / trade-dump 部署了不同 git commit，shadow state 可能和 Counter state 不一致。

**缓解**：
- Counter 和 trade-dump 同 monorepo，部署时强制同一 commit
- 启动 assertion：比较 `engine` 包的 version hash，不一致则 refuse to start

### G3: S3 写入失败且持续失败

Snapshot pipeline 不能 commit consumer offset，重启后从上次 offset 重新 apply + 再次尝试 save。如果 S3 持续不可用，pipeline 卡在最后一份成功的 snapshot，RPO 持续恶化。

**不防御原因**：S3 可用性是基础设施问题，本 ADR 不兜底。

**外部处置**：S3 监控 + SRE runbook。

### G4: Phase A 期间 Counter 自产 snapshot 和 trade-dump 产 snapshot 不一致

灰度期间两份 snapshot 如果 state 有差异，哪份是真？

**处置方案**：
- Counter recovery 期间仍用**自产** snapshot（Phase A 配置）
- 比对工具离线跑，差异记录告警，人工审查
- Phase B 前必须差异率 0 才切换

### G5: Shadow engine 内存占用爆炸

256 vshard × 每 vshard 百 MB state = ~25 GB shadow 内存；加上 MySQL pipeline 本身的缓存，trade-dump 单进程可能 > 50 GB 内存。

**缓解**：
- 单 trade-dump 实例只承载部分 vshard（consumer group 分工）
- 生产上按 vshard 分布部署多个 trade-dump 实例

### G6: Engine 包纯函数假设被打破

未来有人在 engine 包里加了不纯的副作用（Kafka 调用 / 外部 I/O），shadow engine 会跟着触发，可能引发 bug。

**缓解**：
- `engine` 包 interface 明确禁止外部依赖（review 时硬红线）
- 跨服务消费方 (Counter / shadow) 的测试断言 apply 的确定性

### G7: Snapshot 的 journal_offset 和 te_watermark 不同步

Snapshot 记录两个 offset：journal_offset（catch-up 起点）和 te_watermark（trade-event seek 起点）。如果 TECheckpoint 推进但尚未 snapshot，journal_offset 会 > te_watermark 对应的 checkpoint record 位置。

**不防御问题**：这是正常现象，Counter recovery 协议本就允许 journal_offset > te_watermark 的 checkpoint 位置，catch-up 会把多余事件重新 apply（幂等）。

### G8: Counter 和 trade-dump 的 apply 顺序错乱

同一条 journal event，Counter 先 apply 还是 trade-dump 先 apply？两者无先后关系（独立 consumer group），各自独立推进。这是正确的，因为 event 已经在 Kafka 里 committed，任何消费者看到的是同一份数据。

**不是问题**，但首次 review 时容易困惑，这里显式列出。

## 实施里程碑 (Milestones)

> 原计划分 Phase A（双 snapshotter 并行 1-2 周，M5 diff 工具日跑）和
> Phase B（切换）。**实施时直接跳 Phase A** — 未上线、breaking change
> 无顾虑（MEMORY），合入时就把 Counter 停自产了，不需要灰度共存。

- **M1** ✅ `counter/internal/engine` → `counter/engine`（脱 internal，trade-dump 可 import）；`engine.ApplyCounterJournalEvent` 作为共享幂等 apply 入口；Counter catch-up + trade-dump shadow 共用
- **M2** ✅ `trade-dump/internal/snapshot/shadow`：`Engine` 结构（vshardID / state / counterSeq / teWatermark / nextJournalOffset）+ `Apply` + `Capture` + `RestoreFromSnapshot`。单测覆盖 Apply / Capture / RestoreFromSnapshot 往返
- **M3** ✅ `trade-dump/internal/snapshot/pipeline`：kgo ConsumePartitions 多 partition 消费 + 每 vshard 单线程 Apply + 触发（10s 或 10000 条）+ 异步 Save + in-flight guard + Close drain。10 个单测
- **M4** ✅ `trade-dump --pipelines=sql,snap` flag（默认双开）；复用 counter 侧的 BlobStore fs/s3 实现
- **M5** ⏳ 未做 — Snapshot 差异对比工具（可选，灾难排查用）
- **M6** ⏭️ 跳过 — 原计划 Phase A 并行 1-2 周验证,合入时直接走 Phase B（未上线、breaking-change 无顾虑）
- **M7** ✅ Counter recovery 从同一 `vshard-NNN.pb` key 读 trade-dump 产出的 snapshot；key 格式未变,legacy fallback 路径不再需要
- **M8** ✅ Counter 停自产 snapshot(`periodicSnapshot` + `writeSnapshot` 删除)；ADR-0060 M7 `SnapshotMu` + `sequencer.WithApplyBarrier` 撤销；`lastSnapshotAtMs` + `EvictSnapshotStaleAfter` 从 evict breaker 删除
- **M9** ⏳ 未做 — 生产切换 + SLO 验证（需上线）
- **M10** ⏳ 未做 — `counter-rebuild-snapshot` CLI（灾难恢复专用,集群无任何 snapshot 时从 trade-event offset 0 冷 replay）

## 实施约束 (Implementation Notes)

- `engine` 包的 apply 函数必须保持 pure：入参是 `(state, event)`，出参是 `(new_state, error)` 或原地修改 `state`，不做 I/O
- shadow engine 不需要 sequencer / TxnProducer / 任何 producer 组件
- S3 上传路径使用 `counter/snapshot/blobstore` 现有接口（复用）
- Snapshot pipeline 的 goroutine 数 = 承担的 vshard 数（per-vshard 独立 apply loop），单进程 256 goroutine 不算多
- Consumer group `trade-dump-snapshot` 的 rebalance 时要先完成当前 vshard 的 Capture 再 revoke partition（`kgo.OnPartitionsRevoked` hook）
- `catchUpJournal` 工具函数（ADR-0060 recovery 用）和本 ADR 的 shadow apply 函数共用一份代码
- Phase A 的双写期间，S3 keyspace 有两套：`vshard-NNN.pb`（Counter 写）+ `td-vshard-NNN.pb`（trade-dump 写）。Phase B 切换时改名策略：Counter 写的 key 变成 `legacy-vshard-NNN.pb`，trade-dump 写的 key 变成 `vshard-NNN.pb`

## 参考 (References)

### 项目内

- ADR-0001: Kafka as Source of Truth
- ADR-0023: trade-dump MySQL 投影
- ADR-0048: snapshot + offset 原子绑定（本 ADR 承接 recovery 角色）
- ADR-0049: snapshot protobuf / json
- ADR-0058: Counter 虚拟分片 + 实例锁（vshard 粒度基础）
- ADR-0059 §A.5.5: Snapshot pipeline 的归属决策（本 ADR 是其落地）
- ADR-0060: Counter 消费异步化（定义 TECheckpoint event + recovery 协议契约）
- ADR-0062: 订单终态 evict（已被 ADR-0063 取代）
- ADR-0063: 终态即 evict —— shadow engine 通过 `OrderStatusEvent` 终态分支同步删 byID

### 讨论

- 2026-04-22: 用户与 Claude 关于 "snapshot 一致性修复的根本路径" 的完整讨论（会话记录）

## 后续工作建议

- [ ] `pkg/engine` 的 interface 稳定后，考虑把 apply 函数签名规范化（引入 `Applier` interface）
- [ ] Snapshot pipeline 和 MySQL pipeline 的资源画像监控对齐 SLO
- [ ] 评估 snapshot 增量（incremental）模式的可行性：每次只写 diff，完整 snapshot 每小时一次（远期优化）
- [ ] Phase B 完成后 Counter legacy snapshot 代码移除的独立 clean-up commit
