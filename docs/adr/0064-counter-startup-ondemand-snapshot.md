# ADR-0064: Counter 启动时主动触发 on-demand snapshot（消除本地 catch-up 启动延迟）

- 状态: Proposed
- 日期: 2026-04-23
- 决策者: xargin, Claude
- 相关 ADR: 0017（Kafka transactional.id）、0048（snapshot + offset 原子绑定）、0058（Counter 虚拟分片 + 实例锁 / fence）、0060（Counter 消费异步化 + catch-up 协议，本 ADR supersede 其 §4.2 recovery 主路径）、0061（trade-dump snapshot pipeline，本 ADR 复用其 shadow engine 能力）

## 术语 (Glossary)

| 字段 | 含义 | 业界对标 |
|---|---|---|
| `on-demand snapshot` | Counter 启动时通过 gRPC 请求 trade-dump 临时产出的、对齐到 partition LEO 的一次性 snapshot，用于跳过本地 catch-up | — |
| `TakeSnapshot` RPC | 本 ADR 新增的 trade-dump gRPC 接口，参数 `(vshard_id)`，返回 `(snapshot_key, leo, counter_seq)` | — |
| `LEO` (Log End Offset) | Kafka partition 物理最大 offset + 1。consumer 用 `READ_UNCOMMITTED` 语义 `ListOffsets(timestamp=-1)` 获取 | Kafka 官方术语 |
| `LSO` (Log Stable Offset) | `READ_COMMITTED` consumer 可读边界 = `min(HWM, 最早 pending txn 起始 offset)`。本 ADR 不用这个概念 | Kafka 官方术语 |
| `no-op commit` | Counter 启动期间发起的空 Kafka 事务（BeginTransaction → EndTransaction，不 produce 任何 record），用于同步确认老 owner 的 pending txn 已 abort、LEO 已稳定 | — |
| `hot path` | on-demand 路径，常态 recovery 走这条 | — |
| `fallback path` | "legacy snapshot + 本地 catch-up" 路径（ADR-0060 §4.2 原流程）。trade-dump 不可用时使用 | — |

## 背景 (Context)

### 现状：本地 catch-up 的启动延迟代价

ADR-0060 §4.2 定义了 Counter recovery 的流程：

```
load snapshot
  → restore state + counter_seq
  → catchUpJournal (从 snap.JournalOffset 消费到 partition end)
  → start trade-event consumer
  → close(ready)
```

catch-up 的延迟线性依赖两个因素：

- snapshot 周期（ADR-0061 默认 10s / 10000 events）
- TPS × 周期 = 待 replay 的 journal event 数

预估在 5000 TPS/vshard、1min snapshot 周期的场景下：

- 单 vshard 待 replay ≈ 30 万 event
- 本地 apply ≈ 5-15s 纯 CPU + Kafka fetch 带宽（单 partition ~50MB/s）
- 多 vshard 同机重启时 CPU 并发突刺，整机启动延迟 10~30s

CEX 业界对分片切换的目标通常是秒级，本地 catch-up 路径不达标。

### 核心 insight

trade-dump 是 counter-journal 的**持续在线 consumer**（ADR-0061 §1），其 shadow state **近实时**地跟 journal LEO 同步，只是按 `SnapshotInterval` 定期才持久化到 S3。

如果 Counter 启动时能让 trade-dump **按需产出**一份对齐到当前 LEO 的 snapshot，那么：

- 省去 Counter 本地 catch-up 的 replay 成本
- replay 工作"摊到 trade-dump 常态运行中"，不再集中在启动瞬间
- Counter 启动延迟退化为 "snapshot serialize + S3 传输"，约 1~3s
- 整机 CPU 突刺消除

### 为什么现在做

1. OpenTrade 未上线，改 recovery 协议成本低（MEMORY: feedback_breaking_changes）
2. ADR-0061 Phase B 已落地，Counter 不再自产 snapshot，on-demand 的服务端逻辑可直接挂在 trade-dump snapshot pipeline 上
3. 后续 ADR-0058 的 vshard 主动迁移（非 crash 场景）也能复用此 hot path，提前做对称
4. 启动延迟目前无直接 metric，本 ADR 同时补上观测（见实施约束）

## 决策 (Decision)

### 1. trade-dump 新增 gRPC 接口 `TakeSnapshot`

```protobuf
service TradeDumpSnapshot {
  rpc TakeSnapshot(TakeSnapshotRequest) returns (TakeSnapshotResponse);
}

message TakeSnapshotRequest {
  uint32 vshard_id          = 1;
  string requester_node_id  = 2;  // Counter 自己的 node_id, 审计用
  uint64 requester_epoch    = 3;  // ADR-0058 的 epoch, trade-dump 记录日志
}

message TakeSnapshotResponse {
  string snapshot_key = 1;  // 独立命名空间, e.g. "vshard-005-ondemand-1713830412345.pb"
  int64  leo          = 2;  // snapshot 对齐到的 journal LEO
  uint64 counter_seq  = 3;
}
```

### 2. trade-dump 服务端处理

```go
func (s *SnapshotServer) TakeSnapshot(ctx context.Context, req *TakeSnapshotRequest) (*TakeSnapshotResponse, error) {
    partition := int32(req.VshardId)

    // 2.1 查询 counter-journal 对应 partition 的 LEO
    //     使用 READ_UNCOMMITTED 语义 (ListEndOffsets), 拿物理终点
    leo, err := s.kadm.ListEndOffsets(ctx, s.cfg.JournalTopic)[s.cfg.JournalTopic][partition]
    if err != nil {
        return nil, status.Error(codes.Unavailable, "list leo: "+err.Error())
    }

    // 2.2 等自己的 shadow consumer apply 到 >= leo
    //     shadow consumer 是 ReadCommitted, 自动跳过 aborted record
    waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
    defer cancel()
    if err := s.shadow.WaitAppliedTo(waitCtx, int(partition), leo); err != nil {
        return nil, status.Error(codes.DeadlineExceeded, "wait apply: "+err.Error())
    }

    // 2.3 Capture shadow state
    //     继承 ADR-0061 §4.2 的单线程 apply 保证, Capture 期间无并发
    snap := s.shadow.Capture(int(partition))

    // 2.4 序列化 + S3 上传, 独立 key, 不污染周期 snapshot 序列
    key := fmt.Sprintf("%svshard-%03d-ondemand-%d.pb",
        s.cfg.KeyPrefix, partition, time.Now().UnixMilli())
    if err := s.blobstore.Put(ctx, key, snap.Serialize()); err != nil {
        return nil, status.Error(codes.Unavailable, "upload: "+err.Error())
    }

    return &TakeSnapshotResponse{
        SnapshotKey: key,
        Leo:         leo,
        CounterSeq:  snap.CounterSeq,
    }, nil
}
```

**关键设计选择**：

- **查 LEO 而非 LSO**：详见"备选方案 5"。在 Counter 完成 fence 之后 LEO == LSO，但 LEO 语义更直接——"partition 物理终点"，不需要推理 txn 状态
- **独立 key 命名空间**（`-ondemand-{ts}`）：不覆盖 ADR-0061 §4.4 的周期 snapshot（`vshard-NNN.pb`），避免 on-demand 的多次调用互相覆盖 / 和周期序列产生时序冲突
- **on-demand snapshot 不写入 archive**：归档序列由周期 snapshot 承担；on-demand 文件超 1h 统一清理

### 3. Counter 侧启动流程

现有 `worker.Run()`（[counter/internal/worker/worker.go:200-313](../../counter/internal/worker/worker.go:200)）在 `NewTxnProducer` 之后、`catchUpJournal` 之前插入 on-demand 逻辑：

```
① 获得 vshard 锁 (ADR-0058, 不变)
② NewTxnProducer(transactional.id = counter-vshard-{id})
     → broker InitProducerID → bump Kafka 内部 producer_epoch
     → 老 owner 的 producer 在下次 Produce 时拿 InvalidProducerEpoch
     → coordinator 开始 abort 老 owner 的 pending txn (异步)
③ no-op commit (BeginTransaction + EndTransaction(TryCommit), 不 produce)
     → 同步等一次 round-trip, 确保:
       - 老 owner 的 pending txn abort marker 已写入 partition
       - LEO 已在稳定位置 (不会再被老 owner 推进)
④ gRPC TakeSnapshot(vshard_id) → trade-dump
     → 超时 3s; 失败跳 ⑥
⑤ 成功路径:
     下载 snapshot_key → Restore state + offsets + counter_seq
     journal_offset = LEO, 不需要本地 catch-up
     跳 ⑦
⑥ Fallback 路径:
     Load legacy snapshot (ADR-0061 §4.4 的 vshard-NNN.pb)
     catchUpJournal (ADR-0060 §4.2 原流程, 本地 replay 到 end)
⑦ 启动 trade-event consumer, seek to te_watermark
⑧ close(ready), RPC 对外
```

步骤 ③ 的 no-op commit 约 20~50ms；步骤 ④ 的 gRPC + S3 约 1~2s；总体 hot path 目标 < 3s。

### 4. Fallback 触发条件

| 条件 | 行为 |
|---|---|
| `TakeSnapshot` RPC timeout（> 3s） | 走 fallback |
| `TakeSnapshot` 返回 `Unavailable` / `DeadlineExceeded` | 走 fallback |
| on-demand snapshot 下载失败 | 重试 1 次，仍失败走 fallback |
| 返回的 `leo` 小于 legacy snapshot 的 `journal_offset`（理论上不应发生，防御性） | 放弃 on-demand，走 fallback |
| 配置显式 `--startup-mode=legacy` | 直接走 fallback（用于 debug / 灰度回退） |

Fallback 保证 trade-dump 整服务挂掉时 Counter 仍能启动，只是慢一些。

### 5. 周期 snapshot 配置调整

ADR-0061 §4.1 原默认 10s / 10000 events。本 ADR 落地后：

- **默认放松到 60s / 60000 events**
- 周期 snapshot 的唯一角色从 "recovery 主路径起点" 降级为 "trade-dump 故障时的 fallback 起点"
- RPO 退化为 60s 量级，由 `trade_dump_snapshot_age_seconds` 告警（ADR-0061 §7）兜底
- trade-dump 常态 S3 写入量下降约 6×，CPU 下降

### 6. on-demand snapshot 生命周期

- 写入：trade-dump `TakeSnapshot` 调用时产出，key 格式 `{prefix}vshard-{NNN}-ondemand-{unix_ms}.pb`
- 读取：Counter 启动流程消费后不再使用
- 清理：trade-dump 内部 housekeeper goroutine 每 5min 扫一次，删除 `mtime > 1h` 的 on-demand key
- S3 Lifecycle 规则（可选）：on-demand prefix 配置 24h 过期，作为 housekeeper 兜底

## 备选方案 (Alternatives Considered)

### 备选 1：Counter 写 Barrier event 到 counter-journal，trade-dump 消费到后产 snapshot，反向通知 Counter

Counter 启动时往 journal 写一条特殊 `BarrierEvent(req_id)`，trade-dump 消费到这条时触发 snapshot，通过 etcd watch / 反向 gRPC / 反向 topic 告诉 Counter "done + key"。

**拒绝**：

- **工程复杂度高**：barrier 写入 + trade-dump 消费到 + 反向通知 done，三段各自独立 timeout / 重试 / 幂等
- **污染 journal topic**：business event 之外混入控制 event，shadow engine 和未来其他消费者都要过滤
- **Counter 初始化依赖倒挂**：要在 Ready 前就能向 journal 写 barrier，而 TxnProducer 本身又依赖 state 已恢复，顺序别扭
- **概念优势在本架构下免费**：barrier 的核心价值是"端到端同步位点"，但在 ADR-0058 的 vshard 单写者模型下，fence 完成后 partition LEO 已天然是"凝固的同步位点"，不需要 barrier 来额外提供

若未来改为多 writer 架构（ADR-0058 被 supersede），LEO 不再稳定，barrier 会重新变成唯一正确方案；当前架构下本 ADR 方案工程量显著更小。

### 备选 2：单独缩短周期 snapshot 到 5s / 5000 events

**拒绝（作为独立方案）**：

- worst-case 启动仍需本地 catch-up 5s 量的事件，不能压到秒级
- trade-dump 常态 S3 写入成本显著上升（10× 默认）
- 不消除 CPU 突刺问题

**采纳（作为 fallback 兜底）**：本 ADR 把周期放松到 60s，周期 snapshot 只作为 trade-dump 挂机的 fallback 起点。

### 备选 3：Warm standby（每 vshard 常驻备实例持续 apply）

每 vshard 养一个 Counter 备实例，持续 apply journal 但不接 RPC；主挂时直接 promote，启动 0 延迟。

**拒绝（当前阶段）**：

- 实例数 × 2，机器 / 运维成本翻倍
- 需要重做 ADR-0058 的锁模型（active/passive 双角色、promote 协议、split-brain 防御）
- 本 ADR 方案目标启动延迟 1~3s 对 CEX 业务已达标，进一步到亚秒级的复杂度收益比差

Warm standby 作为下一代架构演进方向（如若单实例 SLO 不够），不在本 ADR scope。

### 备选 4：trade-dump 内存 state 通过 gRPC streaming 直接推给 Counter

跳过 S3 一来一回，理论上能压到亚秒级。

**拒绝（当前阶段）**：

- 省 S3 一来一回约 1~2s，但引入"大 state streaming 协议 + 断点续传 + 序列化兼容" 等新问题
- trade-dump 要 expose mutable state 读接口，并发安全设计复杂
- 本方案目标延迟已达标，先止血；若将来要亚秒级再考虑 streaming

### 备选 5：trade-dump 查 LSO 而非 LEO

**拒绝**：

在步骤 ③（no-op commit）完成后，Counter A 的 pending txn 已被 abort marker 写入 partition，此时 `LSO == LEO`。两者值相同，但 API 层有差异：

- LEO 代表"partition 物理终点"，是 recovery 想要的直接概念
- LSO 引入"consumer 可读边界"的间接层，调试时需要推理 pending txn 状态
- `ReadCommitted` consumer 的 fetch position 本就会跳过 aborted record 追到 LEO，不存在"卡在 aborted 上"的风险

LEO 作为 API 参数更干净，等价结果下选择语义更直接的。

## 理由 (Rationale)

### 为什么 no-op commit 是必要的

Kafka `InitProducerID` 同步返回时保证了"老 producer_epoch 已 fence"（再也不能 produce），但**老 pending txn 的 abort marker 写入到 partition 是异步的**。如果跳过步骤 ③ 直接调 `TakeSnapshot`，存在时间窗口：

- broker 还没把 abort marker 写进 partition
- trade-dump 的 ReadCommitted consumer 被 pending txn 卡住，看到的 LSO < LEO
- LEO 查询虽然拿到物理终点，但 `WaitAppliedTo(leo)` 要等 consumer 追到 LEO，而 consumer 此刻前进不了
- 最坏要等 `kgo.TransactionTimeout = 10s`（ADR-0058 §4.3）

步骤 ③ 的 no-op commit 强制一次 Counter → broker → partition 的完整事务 round-trip，到达 partition 的事件顺序变为：

1. Counter A pending txn 的 abort marker
2. Counter B no-op 的 commit marker

之后 trade-dump consumer 一定能顺畅追到 LEO。成本约 20~50ms，远小于被动等 transaction timeout。

### 为什么在 trade-dump 侧查 LEO 而不是 Counter 查完传过去

两种方案都可行。选择 trade-dump 侧查，原因：

- trade-dump 本就连着 Kafka（shadow consumer 所需），多一次 `ListEndOffsets` 几乎零成本
- Counter 查 LEO 需要额外依赖 Kafka admin client（目前 worker 只有 producer client）
- RPC 参数更简单（只传 vshard_id），协议演进时少一个字段要对齐

### 为什么保留 fallback 而不强制 on-demand

trade-dump 的可用性 SLO 因本 ADR 隐性提升（从"周期 snapshot 延迟"升级为"Counter 启动路径软依赖"），但 "Counter 启动不能被 trade-dump 单点挡住" 是底线：

- trade-dump 全服务故障时 Counter 必须能起（走 fallback，启动慢但正确）
- on-demand 功能灰度期出 bug 时可通过 `--startup-mode=legacy` 快速回退
- on-demand 代码路径有 bug 时，fallback 作为安全网

## 影响 (Consequences)

### 正面

- Counter 启动延迟从 10~30s 压到 1~3s（hot path）
- 多 vshard 同机重启的 CPU 突刺消除（replay 工作已摊到 trade-dump 常态）
- trade-dump 常态 S3 写入量下降（周期 snapshot 放松到 60s）
- trade-dump 常态 CPU 下降（Capture 频率下降）
- vshard 主动迁移（非 crash 场景）也可复用此 hot path，启动时序对称

### 负面 / 代价

- trade-dump 新增 gRPC server（新服务面积、新端口、新监控项）
- Counter → trade-dump 启动期软依赖（有 fallback 但 fallback 慢）
- trade-dump SLO 隐性提升，需相应提高实例冗余 / oncall 响应
- on-demand snapshot 的 S3 key 生命周期管理（housekeeper）
- RPO 从"最近一份周期 snapshot（10s 级）"退化到"60s 级"（周期放松后），fallback 路径下更明显

### 中性

- ADR-0061 shadow engine 核心逻辑不变，只新增 RPC handler 触发点 + `WaitAppliedTo` 方法
- Counter fallback 路径是现有 ADR-0060 §4.2 流程，不引入新代码（只是挪动调用位置）
- ADR-0058 fence 语义完全复用，不变

## 已知不防御场景 (Non-Goals / Known Gaps)

### G1：trade-dump 整服务挂且周期 snapshot 也严重过期

继承 ADR-0061 §G1。本 ADR 把周期放松到 60s 后，fallback 路径 RPO 从 10s 恶化到 60s。

**缓解**：

- `trade_dump_snapshot_age_seconds` 阈值从 5min 降到 2min（周期放松后 lag 上限变窄）
- trade-dump 部署多副本分担 partition

### G2：Counter 启动时 trade-dump shadow consumer lag 很大

trade-dump 自己消费 lag 大（如 broker rebalance、trade-dump 刚启动）时，`WaitAppliedTo(leo)` 要等很久。

**处置**：`WaitAppliedTo` 内部硬超时 2s，超时返回错误，Counter 自动 fallback。

### G3：on-demand 返回的 snapshot 和 legacy snapshot 状态不一致

不会发生。shadow engine 和 Counter 共享 `engine` 包的 apply 函数（ADR-0061 §2 + G6），on-demand 产的 snapshot 是 shadow state 的正常 Capture，语义与周期 snapshot 同源。

### G4：no-op commit 本身失败（Kafka 不可用）

Counter 无法进入 on-demand 路径；fallback 也需要 producer 就位才能最终启动，同样失败。这是现有 ADR-0058 / ADR-0060 共同约束，本 ADR 不新增此类失败场景。

### G5：ADR-0058 §4.5 的"老 owner 假死脏写窗口"

现有窗口是 "老 owner 的已进 broker 但未 commit 的事务，在 fence 之后仍可能成功 commit"。本 ADR 的步骤 ③ no-op commit 不消除该窗口（该窗口靠 `match_seq` / `counter_seq` 应用层 dedup 兜底）。

如果该脏写发生在 Counter B 的步骤 ④ 之前，trade-dump 会看到脏 commit 并 apply，on-demand snapshot 会包含它——这是 **correct behavior**（shadow engine 看到 committed 就是 committed，Counter 重建后的 state 和原本就该一致）。

如果该脏写发生在 Counter B 的步骤 ④ 之后、snapshot Capture 已完成，那么 snapshot 不包含它；Counter B 启动后会从 trade-event 侧通过 `match_seq` guard 识别并去重，不会产生错误副作用。

本场景相比 ADR-0060 §4.2 原 recovery 流程没有新增风险。

### G6：on-demand snapshot 被反复请求导致 S3 成本上升

正常场景下 Counter 启动才会触发 on-demand，频率极低。异常场景（如 Counter 在 ready 循环 crash/restart）可能产生大量 on-demand snapshot。

**缓解**：

- trade-dump 对同一 `vshard_id` 的并发 `TakeSnapshot` 请求做合并（同一时刻只处理一个）
- housekeeper 1h 清理
- `trade_dump_ondemand_snapshot_requests_total` metric 超阈值告警

### G7：counter-journal 的 retention 比周期 snapshot 短

继承 ADR-0060 §G？（retention vs snapshot interval）。本 ADR 把周期放松到 60s 后，journal retention 必须 ≥ 60s，实际建议 ≥ 1h（给 fallback catch-up 留充足空间）。

**缓解**：现有 ADR-0060 §G 的 sanity check（启动时比对 `snapshot.journal_offset` vs `earliest offset`）自动覆盖。

## 实施约束 (Implementation Notes)

### Kafka 相关

- trade-dump 查 LEO：`kadm.ListEndOffsets(ctx, journalTopic)`（franz-go），底层 `ListOffsets` 请求参数 `timestamp=-1, isolation_level=READ_UNCOMMITTED`
- Counter no-op commit：
  ```go
  if err := producer.BeginTransaction(); err != nil { return err }
  // 不 produce 任何 record
  if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil { return err }
  ```
- `kgo.TransactionTimeout = 10s`（ADR-0058 §4.3）不变，仅兜底被动 abort
- producer `transactional.id` 沿用 ADR-0058 的 `counter-vshard-{id}`

### Counter 改动

- `worker.Run()` 在 [worker.go:220](../../counter/internal/worker/worker.go:220)（`NewTxnProducer` 之后、`catchUpJournal` 之前）插入新流程
- 新增 `tradeDumpClient` 字段，worker.go:200 附近初始化 gRPC conn
- 新增配置项：
  - `--startup-mode=auto|on-demand|legacy`，默认 `auto`
  - `--trade-dump-endpoint=<host:port>`
  - `--on-demand-timeout=3s`
- fallback 路径保持现状，不改动 `catchUpJournal` 实现

### trade-dump 改动

- 新增 gRPC server（独立 listener，端口 tbd，写入 config/runbook）
- shadow engine 新增方法：
  ```go
  // 等自己对 vshard 的 apply 推进到 >= targetOffset
  // 超时返回 error, 不降级
  func (s *ShadowEngine) WaitAppliedTo(ctx context.Context, vshard int, targetOffset int64) error
  ```
- 同 vshard 的并发 `TakeSnapshot` 请求做 request coalescing（singleflight）
- housekeeper goroutine：每 5min 扫 S3 `*-ondemand-*` prefix，删除 mtime > 1h 的 key

### 监控

所有 metric 均 per-vshard label：

- `counter_startup_duration_seconds{vshard, mode=on-demand|fallback}` — histogram, p99
- `counter_startup_mode_total{mode, result=success|failure}` — counter
- `trade_dump_ondemand_snapshot_duration_seconds{vshard, phase=wait_apply|capture|upload}` — histogram
- `trade_dump_ondemand_snapshot_requests_total{result=ok|wait_timeout|upload_fail|coalesced}` — counter
- `trade_dump_ondemand_snapshot_bytes{vshard}` — gauge

### 测试

- 单元：
  - `WaitAppliedTo` 的超时 / context cancel 行为
  - no-op commit 失败时 Counter fallback 路径被触发
  - `TakeSnapshot` singleflight 行为
- 集成：
  - Counter 正常启动 → on-demand 成功，启动耗时 < 3s
  - trade-dump 不可达 → Counter fallback，启动成功
  - Counter A 有 pending txn → Counter B 启动后 LEO 稳定，on-demand 正确
  - 10 个 vshard 同时启动 → trade-dump 并发处理无死锁
  - `--startup-mode=legacy` 强制走 fallback
- 压测：
  - 启动耗时 p99 < 3s（hot path）
  - trade-dump 处理 on-demand 请求 p99 < 2s

### 分阶段落地

| Milestone | 内容 | 状态 |
|---|---|---|
| M1 | trade-dump 新增 `TakeSnapshot` gRPC + `WaitAppliedTo` shadow 方法 + housekeeper | pending |
| M2 | Counter 实现 on-demand 路径 + `--startup-mode=auto` 默认，灰度一个 vshard 观察 | pending |
| M3 | 全量开启 on-demand，周期 snapshot 放松到 60s | pending |
| M4 | 撤销 `--startup-mode=legacy` 强制配置选项（保留代码作为 fallback） | pending |

### Runbook 影响

- 部署顺序：trade-dump 必须先于 Counter 上线（ADR-0061 §8 已要求；本 ADR 强化该约束——on-demand 若 trade-dump 不在会退化到 fallback）
- 灰度回退：`--startup-mode=legacy` 可快速跳过 on-demand
- 告警处置：`counter_startup_mode_total{mode=fallback}` 非零时触发 trade-dump 可用性检查

## 参考 (References)

- ADR-0017: Kafka transactional.id 命名（epoch 嵌入约定）
- ADR-0048: Snapshot + offset 原子绑定（本 ADR 承接 recovery 部分的 offset 语义）
- ADR-0058: Counter 虚拟分片 + 实例锁（提供 fence 语义，§4.1-4.5 是 on-demand 流程的前置）
- ADR-0060: Counter 消费异步化 + catch-up 协议（本 ADR supersede 其 §4.2 recovery 主路径，保留 fallback）
- ADR-0061: Trade-dump Snapshot Pipeline（shadow engine 提供 on-demand 服务端能力）
- Kafka KIP-98: Exactly Once Delivery and Transactional Messaging（InitProducerID / pending txn abort 语义）
- Kafka Protocol: ListOffsets API v5+（isolation_level 参数定义 LEO vs LSO 语义）
