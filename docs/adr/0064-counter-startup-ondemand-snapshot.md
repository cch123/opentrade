# ADR-0064: Counter 启动时主动触发 on-demand snapshot（消除本地 catch-up 启动延迟）

- 状态: Proposed(v2 — 2026-04-23 /plan-eng-review 后修正 no-op commit 断言 → sentinel produce,补 epoch 校验、WaitAppliedTo 并发模型、fallback 安全点、metric 粒度)
- 日期: 2026-04-23
- 决策者: xargin, Claude
- 相关 ADR: 0017(Kafka transactional.id)、0048(snapshot + offset 原子绑定)、0058(Counter 虚拟分片 + 实例锁 / fence)、0060(Counter 消费异步化 + catch-up 协议,本 ADR supersede 其 §4.2 recovery 主路径)、0061(trade-dump snapshot pipeline,本 ADR 复用其 shadow engine 能力)、0062(OrderEvictedEvent tag 51 保留,本 ADR 用 tag 52)

## 术语 (Glossary)

| 字段 | 含义 | 业界对标 |
|---|---|---|
| `on-demand snapshot` | Counter 启动时通过 gRPC 请求 trade-dump 临时产出的、对齐到 partition LEO 的一次性 snapshot,用于跳过本地 catch-up | — |
| `TakeSnapshot` RPC | 本 ADR 新增的 trade-dump gRPC 接口,参数 `(vshard_id, requester_node_id, requester_epoch)`,返回 `(snapshot_key, leo, counter_seq)` | — |
| `LEO` (Log End Offset) | Kafka partition 物理最大 offset + 1。`kadm.ListEndOffsets`(底层 `ListOffsets` + `IsolationLevel=READ_UNCOMMITTED` + `timestamp=-1`)获取 | Kafka 官方术语 |
| `LSO` (Log Stable Offset) | `READ_COMMITTED` consumer 可读边界 = `min(HWM, 最早 pending txn 起始 offset)`。本 ADR 不用这个概念 | Kafka 官方术语 |
| `sentinel produce` | Counter B 启动时在 Kafka 事务中 produce 一条 `StartupFenceEvent` 到自己 vshard 的 journal partition 后 commit。这是**真实 round-trip**,触发 franz-go 的 InitProducerID → fence Counter A → abort Counter A 的 pending txn → 写 abort marker 到 partition → 写 commit marker 稳定 LEO。**替代 v1 的 "no-op commit" 断言(该断言错误,v1 的写法在 franz-go 里是纯内存 no-op)** | — |
| `StartupFenceEvent` | 新增的 counter-journal event 类型(tag 52),payload 仅含 `node_id` + `epoch` 作审计。shadow engine 和 Counter engine apply 时均 no-op | — |
| `hot path` | on-demand 路径,常态 recovery 走这条 | — |
| `fallback path` | "legacy snapshot + 本地 catch-up" 路径(ADR-0060 §4.2 原流程)。trade-dump 不可用 / TakeSnapshot 超时时使用 | — |
| `Phase 1 / Phase 2` | Counter startup 的两个阶段。Phase 1 = RPC + download + **parse into fresh ShardState**,失败可 fallback;Phase 2 = **atomic install** fresh state 到 worker,失败必须 crash(in-memory state 已污染) | — |

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

### 1. Proto 变更

#### 1.1 新增 gRPC 接口 `TakeSnapshot`

```protobuf
// api/rpc/tradedump/snapshot/v1/snapshot.proto (新增)
service TradeDumpSnapshot {
  rpc TakeSnapshot(TakeSnapshotRequest) returns (TakeSnapshotResponse);
}

message TakeSnapshotRequest {
  uint32 vshard_id          = 1;
  string requester_node_id  = 2;  // Counter 自己的 node_id, 审计用
  uint64 requester_epoch    = 3;  // ADR-0058 的 epoch, trade-dump 校验并拒绝回退请求
}

message TakeSnapshotResponse {
  string snapshot_key = 1;  // 独立命名空间, e.g. "vshard-005-ondemand-1713830412345.pb"
  int64  leo          = 2;  // snapshot 对齐到的 journal LEO
  uint64 counter_seq  = 3;
}
```

#### 1.2 新增 journal event `StartupFenceEvent`

```protobuf
// api/event/counter_journal.proto (追加)
message CounterJournalEvent {
  // ... 现有字段 ...
  oneof payload {
    // ... 现有 10~15, 50 ...
    // Tag 51 保留(ADR-0062 OrderEvictedEvent, 已退役, 不重用)
    StartupFenceEvent startup_fence = 52;  // 本 ADR 新增
  }
}

message StartupFenceEvent {
  string node_id  = 1;  // 写该事件的 Counter 实例 node_id (审计)
  uint64 epoch    = 2;  // ADR-0058 的 assignment epoch
  int64  ts_ms    = 3;  // Counter 写入时间 (debug/审计)
}
```

**Apply 协议**:`StartupFenceEvent` 对 shadow engine 和 Counter engine 均为 **no-op**(不改 state、不改 counterSeq)。唯一作用是"强制一次真实的事务 round-trip",在 Kafka partition 日志里留下 commit marker 以稳定 LEO,同时触发 franz-go 的 InitProducerID 去 fence 老 Counter A。

**版本兼容约束**:shadow engine 和 Counter engine 的 `ApplyCounterJournalEvent` 遇到**不识别的 payload 类型**(oneof unknown case)必须 no-op 并返回 nil,不得 panic —— 为未来再加 event 留空间。此约束与本 ADR 同 commit 修入 engine。

### 2. trade-dump 服务端处理

```go
func (s *SnapshotServer) TakeSnapshot(ctx context.Context, req *TakeSnapshotRequest) (*TakeSnapshotResponse, error) {
    partition := int32(req.VshardId)

    // 2.1 epoch 校验(defense-in-depth over ADR-0058 vshard lock)
    //     trade-dump 维护 per-vshard last_seen_epoch; 请求 epoch < last_seen 拒绝
    if !s.epochTracker.CheckAndAdvance(partition, req.RequesterEpoch) {
        return nil, status.Errorf(codes.FailedPrecondition,
            "stale epoch %d for vshard %d (last_seen=%d)",
            req.RequesterEpoch, partition, s.epochTracker.Get(partition))
    }

    // 2.2 并发 semaphore: 256 vshard 可能同机同时 restart, 限 16 在飞防 OOM / S3 rate limit
    if err := s.sem.Acquire(ctx, 1); err != nil {
        return nil, status.Error(codes.ResourceExhausted, "too many on-demand in flight")
    }
    defer s.sem.Release(1)

    // 2.3 singleflight(按 vshard_id), 相同 vshard 并发请求共享结果
    key := fmt.Sprintf("vshard-%d", partition)
    result, err, _ := s.sf.Do(key, func() (any, error) {
        return s.takeSnapshotOnce(ctx, partition)
    })
    if err != nil {
        return nil, err
    }
    return result.(*TakeSnapshotResponse), nil
}

func (s *SnapshotServer) takeSnapshotOnce(ctx context.Context, partition int32) (*TakeSnapshotResponse, error) {
    // 2.4 查 LEO(READ_UNCOMMITTED 语义, kadm.ListEndOffsets 默认就是这个)
    offsets, err := s.kadm.ListEndOffsets(ctx, s.cfg.JournalTopic)
    if err != nil {
        return nil, status.Error(codes.Unavailable, "leo_query_fail: "+err.Error())
    }
    pOff, ok := offsets.Lookup(s.cfg.JournalTopic, partition)
    if !ok || pOff.Err != nil {
        return nil, status.Error(codes.Internal, "leo_query_fail: partition missing / err")
    }
    leo := pOff.Offset

    // 2.5 等 shadow 对该 vshard 的 applied offset >= leo
    //     WaitAppliedTo 内部 5ms poll + 硬超时 2s
    //     shadow consumer 是 ReadCommitted, Counter A 遗留的 aborted record 自动跳过
    waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
    defer cancel()
    if err := s.shadow.WaitAppliedTo(waitCtx, int(partition), leo); err != nil {
        return nil, status.Error(codes.DeadlineExceeded, "wait_apply_timeout: "+err.Error())
    }

    // 2.6 Capture + serialize + upload
    snap, err := s.shadow.CaptureForVshard(int(partition))
    if err != nil {
        return nil, status.Error(codes.Internal, "capture_error: "+err.Error())
    }
    bytes, err := snap.Marshal()
    if err != nil {
        return nil, status.Error(codes.Internal, "serialize_error: "+err.Error())
    }
    snapKey := fmt.Sprintf("%svshard-%03d-ondemand-%d.pb",
        s.cfg.KeyPrefix, partition, time.Now().UnixMilli())
    if err := s.blobstore.Put(ctx, snapKey, bytes); err != nil {
        return nil, status.Error(codes.Unavailable, "s3_upload_error: "+err.Error())
    }
    return &TakeSnapshotResponse{
        SnapshotKey: snapKey,
        Leo:         leo,
        CounterSeq:  snap.CounterSeq,
    }, nil
}
```

**关键设计选择**:

- **查 LEO 而非 LSO**:详见"备选方案 5"。在 Counter 完成 sentinel produce 之后 LEO == LSO,但 LEO 语义更直接——"partition 物理终点",API 参数不带 isolation 语义
- **epoch 校验**:`epochTracker` 是 trade-dump in-memory `map[int32]uint64`(进程内,重启丢失可接受——第一次请求会 populate);校验请求 epoch 单调不回退;ADR-0058 vshard 锁是主防御,本检查是 defense-in-depth,防 zombie Counter
- **singleflight**:相同 vshard 的并发请求合并为一次 Capture,避免多份文件 + 防 F8 的 256 并发打爆
- **并发 semaphore**:全局限 on-demand in-flight 到 16(可配置),整机 restart 触发 256 vshard 同时 TakeSnapshot 时防 OOM
- **独立 key 命名空间**(`-ondemand-{ts}`):不覆盖 ADR-0061 §4.4 的周期 snapshot(`vshard-NNN.pb`)
- **housekeeper**:trade-dump 内部 goroutine 每 5min 扫 `*-ondemand-*`,删 mtime > 1h

#### 2.7 Shadow engine 新增 `WaitAppliedTo`(跨 goroutine 安全)

**关键观察**:ADR-0061 [pipeline.go:22-24](../../trade-dump/internal/snapshot/pipeline/pipeline.go:22) 的 threading model 是 "**单 Run goroutine 多路复用全部 256 vshard**",不是 per-vshard goroutine。所以 WaitAppliedTo 的 RPC handler goroutine 必须跨线程读 applied offset。

```go
// trade-dump/internal/snapshot/shadow/engine.go (增量)

type Engine struct {
    // ... 现有字段 ...
    publishedOffset atomic.Int64  // 新增: Run goroutine Store, RPC handler Load
}

// 在 Apply 末尾追加一次 atomic Store, 零竞争(单写者):
func (e *Engine) Apply(evt *eventpb.CounterJournalEvent, kafkaOffset int64) error {
    // ... 现有逻辑保持 ...
    e.publishedOffset.Store(e.nextJournalOffset)  // 新增
    return nil
}

// trade-dump/internal/snapshot/shadow/shadow.go (Shadow 聚合类)
func (s *Shadow) WaitAppliedTo(ctx context.Context, vshard int, target int64) error {
    eng := s.engines[vshard]
    if eng == nil {
        return fmt.Errorf("vshard %d not owned", vshard)
    }
    const poll = 5 * time.Millisecond
    tk := time.NewTicker(poll)
    defer tk.Stop()
    for {
        if eng.publishedOffset.Load() >= target {
            return nil
        }
        select {
        case <-tk.C:
        case <-ctx.Done():
            return ctx.Err()
        }
    }
}
```

**为什么 poll 而不是 channel broadcast**:冷路径(每 Counter 启动调一次 per vshard),2s × 5ms = 最多 400 次原子读,忽略不计。channel broadcast 每次 Apply 都要 close+alloc,对 Run goroutine 这个 hot path 有常态开销,不划算。sync.Cond+ctx 可做但复杂度更高。

**单 Run goroutine 多路复用的系统性风险**:Run goroutine 如果 hang 在某个 vshard 的 Apply(Kafka fetch 长卡、apply 里调外部慢 API),**所有 vshard 的 WaitAppliedTo 同时超时**。必须加 `shadow_apply_stall_seconds{vshard}` gauge(now - last_apply_ts)监控,stall > 5s 告警(见 §实施约束.监控)。

### 3. Counter 侧启动流程

现有 `worker.Run()`([counter/internal/worker/worker.go:200-313](../../counter/internal/worker/worker.go:200))在 `NewTxnProducer` 之后、`catchUpJournal` 之前插入 on-demand 逻辑,分 **Phase 1**(可 fallback)和 **Phase 2**(原子 install,不可回退):

```
Phase 0 — 启动准备
① 获得 vshard 锁 (ADR-0058, 不变)
② NewTxnProducer(transactional.id = counter-vshard-{id})
     → kgo.NewClient 创建客户端, **不触发** InitProducerID(franz-go 懒加载)
     → Counter B 此时还没 fence 老 Counter A

Phase 1 — 非破坏性准备(失败可 fallback 到 legacy 路径)
③ sentinel produce(替代原 v1 的 no-op commit,关键修正 F1)
     producer.BeginTransaction()
     producer.Produce(ctx, {
         Topic:     journalTopic,
         Partition: vshardID,
         Value:     marshal(StartupFenceEvent{node_id, epoch, ts_ms}),
     })
     producer.EndTransaction(ctx, TryCommit)   // ← 真实 round-trip
     
     **作用链**:
       - Produce 触发 franz-go 的 InitProducerID (lazy)
       - InitProducerID 在 franz-go 内部 retry loop 等到 broker 返回 success
       - broker 递增 producer_epoch → fence 老 Counter A
       - 老 Counter A 的 pending txn 被 coordinator 转为 PrepareAbort → CompleteAbort
       - abort marker 写入 partition(InitProducerID 返回时这一步已完成)
       - B 的 sentinel record 写入 partition
       - EndTransaction(TryCommit) 返回 = commit marker 已写入 partition(acks=AllISR)
     
     耗时:通常 ≤ 100ms;老 A 有 pending txn 时 ≤ 数百 ms(等 franz-go retry loop 收敛)
     
     失败 → Phase 1 fallback:NewTxnProducer 重复创建是否幂等需要代码验证
         (当前实现每次 Run 都新建 client, OK)

④ gRPC TakeSnapshot(vshard_id, node_id, epoch) → trade-dump
     超时 3s; **不重试**(见 §4); 失败 → Phase 1 fallback 跳 ⑥'

⑤ 下载 snapshot_key 到内存 bytes → protobuf decode → freshState/freshSeq/freshOffsets
     parse 任何一步失败 → Phase 1 fallback 跳 ⑥'

Phase 2 — 原子 install(失败 → crash,不 fallback)
⑥ 原子 install 到 worker:
     w.state = freshState
     w.seq = freshSeq
     svc = service.New(..., freshState, freshSeq, producer, ...)
     svc.SetOffsets(freshOffsets)
     w.svc = svc
     
     此阶段任何错误 → return fmt.Errorf("CRITICAL: Phase 2 install failed: %w", err)
     → worker.Run 返回 → Counter 进程按 ADR-0058 让出 vshard → 外层重启
     **不能** fallback 到 legacy,因为 w.state 可能已半污染

⑥' Fallback 路径(仅 Phase 1 失败进入):
     Load legacy snapshot (ADR-0061 §4.4 的 vshard-NNN.pb)
     → freshState.Restore(legacySnap)     // parse into fresh
     → Phase 2 install(同 ⑥)
     → catchUpJournal(legacy.JournalOffset)  // ADR-0060 §4.2 原流程

Phase 3 — 消费 + 对外
⑦ 启动 trade-event consumer, seek 到 te_watermark(from snapshot.Offsets)
⑧ close(ready), RPC 对外
```

**延迟拆解**(hot path):

| 阶段 | 预期 |
|---|---|
| sentinel produce(含 InitProducerID fence + abort marker flush + commit marker) | 50~300ms |
| TakeSnapshot gRPC(含 trade-dump WaitAppliedTo + Capture + S3 upload) | 500~1500ms |
| Counter 下载 + parse + Phase 2 install | 300~1000ms |
| **总计** | **1~3 秒** |

Counter A 有 pending txn 时,sentinel produce 阶段会被 franz-go 的 retry loop 拖长(等 broker 完成 abort 写 marker)。worst case 受 `kgo.TransactionTimeout = 10s`(ADR-0058 §4.3)兜底。

#### 3.1 端到端时序图(Counter A crashed,A 留有 pending txn 的场景)

假设 Counter A 生前在 journal partition 5 上写了这些,并在 pending txn 中崩溃:

```
起点状态 (Counter A crashed before committing offset 100-102)

journal partition 5:
  offset 98:  A 的业务 event (committed — 属于 A 早前已成功 commit 的 txn)
  offset 99:  commit marker for offset 98
  offset 100: A 的业务 event (pending txn,被 crash 打断,未 commit)
  offset 101: A 的业务 event (同一个 pending txn)
  offset 102: A 的业务 event (同一个 pending txn)
  ──────── LEO = 103 ────────
  LSO = 100  (ReadCommitted 视角,卡在 pending txn 起点)

trade-dump shadow engine (vshard=5, ReadCommitted consumer):
  applied_offset = 100  (消费到 pending txn 起点就停了,等 decision)

S3:
  vshard-005.pb  ← 上次周期 snapshot,journal_offset = 95
```

v2 流程如下(省略 BFF 和下游消费者视角,聚焦 fence + on-demand + install):

```
Counter B                Kafka partition 5            trade-dump                  S3
   │                           │                           │                       │
   │── ① 拿 vshard 锁 ────────────────────────────────────────                    │
   │                           │                           │                       │
   │── ② NewTxnProducer ──────▶│  (kgo.NewClient 创建 client,                       │
   │    franz-go 懒加载         │   不触发 InitProducerID,不 fence,无网络动作)          │
   │                           │                           │                       │
   │═══════════════════ Phase 1 开始 (非破坏性,可 fallback) ════════════              │
   │                           │                           │                       │
   │── ③ sentinel produce ──────────────────────────────                           │
   │    BeginTransaction()     │                           │                       │
   │    Produce(StartupFence)  │                           │                       │
   │         │                 │                           │                       │
   │         ├─ franz-go 首次触发 InitProducerID                                     │
   │         │                 │                           │                       │
   │         │ ◀── coordinator: "A's txn [100-102] pending                         │
   │         │      → PrepareAbort → send WriteTxnMarkers(abort)"                  │
   │         │                 │                           │                       │
   │         │                 │ offset 103: abort marker ◀─ (by coordinator)      │
   │         │                 │                           │                       │
   │         │ ◀── broker: "CompleteAbort done, new epoch = N+1"                   │
   │         │                 │                           │                       │
   │         │                 │ offset 104: StartupFence ◀─ (B's sentinel record, │
   │         │                 │             (pending B's txn)   payload 为 no-op) │
   │         │                                                                     │
   │    EndTransaction(commit) │                                                   │
   │         │                 │ offset 105: commit marker ◀ (acks=AllISR)         │
   │         │ ◀── success ────┤                                                   │
   │                           │                                                   │
   │    ★ LEO = 106 现在稳定 ★ │                                                   │
   │                           │                                                   │
   │              (同时,trade-dump Run goroutine 看到 abort marker 103 后,         │
   │               自动跳过 100-102 的 aborted data,apply 104 (no-op),             │
   │               consumer position 推进到 106)                                     │
   │                           │─ fetch ─────────────────▶ │                       │
   │                           │                           │ shadow.Apply:          │
   │                           │                           │  offset 100-102 skipped│
   │                           │                           │  offset 104 no-op      │
   │                           │                           │  published_offset=106  │
   │                           │                           │                       │
   │── ④ gRPC TakeSnapshot(vshard=5, epoch=N+1) ─────────▶ │                       │
   │                           │                           │                       │
   │                           │                           │ 2.1 epochTracker 校验:│
   │                           │                           │   req.epoch=N+1 ≥ last│
   │                           │                           │   → 接受               │
   │                           │                           │                       │
   │                           │                           │ 2.2 Sem.Acquire(1)    │
   │                           │                           │                       │
   │                           │                           │ 2.3 singleflight      │
   │                           │                           │                       │
   │                           │ ◀── ListEndOffsets ──────│                       │
   │                           │ ─── LEO = 106 ───────────▶│                       │
   │                           │                           │                       │
   │                           │                           │ 2.5 WaitAppliedTo(106)│
   │                           │                           │   loop: Load>=106 ? OK│
   │                           │                           │                       │
   │                           │                           │ 2.6 Capture → bytes   │
   │                           │                           │ ── upload ────────────▶│
   │                           │                           │                       │ vshard-005-
   │                           │                           │                       │ ondemand-
   │                           │                           │                       │ 17xxx.pb
   │                           │                           │                       │
   │ ◀──── Response{key, leo=106, counter_seq} ───────────┤                       │
   │                           │                           │                       │
   │── ⑤ 下载 on-demand snap ────────────────────────────────────────────────────▶│
   │ ◀──── snapshot bytes ───────────────────────────────────────────────────────┤
   │    parse → freshState / freshSeq / freshOffsets                              │
   │    (解析成功,Phase 1 结束;任一步失败则跳 ⑥' fallback)                            │
   │                           │                           │                       │
   │═══════════════════ Phase 2 开始 (原子 install,不可回退) ═════════              │
   │                           │                           │                       │
   │── ⑥ atomic install ─────                                                       │
   │    w.state = freshState                                                        │
   │    w.seq   = freshSeq                                                          │
   │    svc     = service.New(freshState, freshSeq, producer, ...)                  │
   │    svc.SetOffsets(freshOffsets)                                                │
   │    w.svc   = svc                                                               │
   │    ↑ 任何一步错误 → return 错误 → worker.Run 退出 → Counter process fatal exit   │
   │                                                                                │
   │── ⑦ 启动 trade-event consumer, seek to te_watermark                            │
   │── ⑧ close(ready) → RPC 对外                                                    │
   │                           │                           │                       │

⑥' Fallback 路径 (仅 Phase 1 失败进入):
   - Load legacy snap (S3: vshard-005.pb, journal_offset=95)
   - freshState.Restore(legacySnap)        (fresh parse,不污染 worker)
   - Phase 2 install (同 ⑥,原子 install 成功才继续)
   - catchUpJournal(95 → end)              (ADR-0060 §4.2 原流程,
                                             会消费到 StartupFenceEvent 并 no-op)
   - ⑦ + ⑧ 同上
```

**barrier 的三重作用**(集中在 offset 104 的 StartupFenceEvent 这一条 record 上):

1. **触发 InitProducerID**(barrier 的副作用):franz-go 懒加载 InitProducerID 被首次 Produce 调起,broker 借机 fence 老 A 并写 abort marker (offset 103)
2. **作为物理 marker**(barrier 自身):trade-dump 读到这条就知道 "A 的所有未决状态已经定案"
3. **顶住 LEO**(barrier 之后的副作用):commit marker (offset 105) 让 LEO = 106 稳定,trade-dump 的 LEO 查询有确定的等待终点

shadow engine 对 StartupFenceEvent 的 `Apply` 是 **no-op**(不改 ShardState、不改 counterSeq),它的价值完全在"存在本身"。

#### 3.2 被 abort 的 pending record 的处置

Counter A crash 时遗留在 partition 里的 offset 100-102 这三条 record,sentinel produce 完成后的状态:

- **物理层**:bytes 仍在 partition 的 log segment 里,不会立刻删除;segment retention(时间或大小)到期才随 segment 一起 GC
- **逻辑层**(ReadCommitted consumer):**完全不可见**。fetch response 携带 `aborted_transactions: [{producer_id: A, first_offset: 100}]`,franz-go 客户端自动过滤这几条 record,不交给应用层 `Apply`。shadow engine 从未看到过它们
- **consumer position**:直接从 100 跳到 104(跳过 aborted records 和 abort marker),position = 106

**对业务的含义**:假设这几条是 `FreezeEvent`(A 处理 PlaceOrder 产生的):

- A 生前在内存里冻结了资金,但这次冻结**从未被 commit**
- Counter B 从 trade-dump shadow 恢复的 state 里,这次冻结**不存在**
- BFF 之前发给 A 的 PlaceOrder 调用要么超时要么收到 `Unavailable` 错误
- 客户端必须用幂等 key 重试(ADR-0048 / ADR-0057 的 `client_order_id` + `counter_seq` dedup),重试打到 Counter B,B 会重新处理产出新的、这次真的 commit 的 FreezeEvent

这是标准 Kafka 事务的 exactly-once 语义落在 Counter 上的表现:**aborted = 从未发生**,客户端幂等重试覆盖 "A 崩溃时丢了那次 RPC" 的缺口。

**隐性约束**:OpenTrade 内**禁止** ReadUncommitted consumer 消费 counter-journal。ReadUncommitted 会看到 aborted records 并误 apply,破坏幂等性。此约束写入 §实施约束.Kafka 相关。

### 4. Fallback 触发条件与重试策略

| 条件 | 行为 |
|---|---|
| `TakeSnapshot` RPC timeout(> 3s) | 走 fallback, **不重试** |
| `TakeSnapshot` 返回 `Unavailable` / `DeadlineExceeded` / `FailedPrecondition` / `ResourceExhausted` | 走 fallback, **不重试** |
| on-demand snapshot 下载失败 | **不重试**, 走 fallback(snapshot key 本身可能已无效, 重下没意义) |
| sentinel produce(Phase 1 ③) 失败 | 走 fallback(fallback 路径会再试一次 TxnProducer 初始化) |
| 返回的 `leo` 小于 legacy snapshot 的 `journal_offset`(防御性检查) | 放弃 on-demand, 走 fallback |
| 配置显式 `--startup-mode=legacy` | 直接跳 Phase 1 sentinel produce 之后的所有 on-demand 步骤, 走 fallback(用于 debug / 灰度回退) |

**不重试理由**:

- Counter B 的 sentinel produce 已经完成 fence,trade-dump 不会因"A 的 pending txn 还在" 卡住。超时多半是真故障(trade-dump 挂 / 网络断 / trade-dump 过载)
- 重试多消耗 ≥ 3s,跟 legacy fallback 的 catch-up 量级可比,收益不清晰
- trade-dump 侧 singleflight 防的是 **服务端并发**(同 vshard 不同请求),**不防** Counter 侧重试产生的新请求 —— Counter 重试会产生第二份 snapshot 文件,被 housekeeper 回收但浪费 S3 写

Fallback 保证 trade-dump 整服务挂掉时 Counter 仍能启动,只是慢一些(10~30s)。

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

### 备选 5:trade-dump 查 LSO 而非 LEO

**拒绝**:

在步骤 ③(sentinel produce)完成后,Counter A 的 pending txn 已被 abort marker 写入 partition,此时 `LSO == LEO`。两者值相同,但 API 层有差异:

- LEO 代表"partition 物理终点",是 recovery 想要的直接概念
- LSO 引入"consumer 可读边界"的间接层,调试时需要推理 pending txn 状态
- `ReadCommitted` consumer 的 fetch position 本就会跳过 aborted record 追到 LEO,不存在"卡在 aborted 上"的风险

LEO 作为 API 参数更干净,等价结果下选择语义更直接的。

### 备选 6(v1 方案):no-op commit 不 produce 任何 record

**v1 ADR 采用,v2 /plan-eng-review 后拒绝**。

v1 ADR 的步骤 ③ 写成 `BeginTransaction() + EndTransaction(TryCommit)` 且不 produce。根据 franz-go v1.18.0 源码验证([txn.go:901-904](/Users/xargin/go/pkg/mod/github.com/twmb/franz-go@v1.18.0/pkg/kgo/txn.go:901)):

```go
// If no partition was added to a transaction, then we have nothing to commit.
if !anyAdded {
    cl.cfg.logger.Log(LogLevelDebug, "no records were produced during the commit; thus no transaction was began; ending without doing anything")
    return nil
}
```

- `BeginTransaction()` 只设内存 flag,无网络调用
- `EndTransaction(TryCommit)` 若无 produce,`anyAdded=false`,直接 return nil,无网络
- `NewTxnProducer()` 内 `kgo.NewClient` 也不触发 InitProducerID(franz-go 懒加载,首次 Produce 才调)

**结论**:v1 的"no-op commit"在 franz-go 侧是**纯内存 no-op**,不 fence、不 abort、不写任何东西到 Kafka。Counter A 的 pending txn 只会被 `transaction.timeout.ms = 10s` 被动超时。v1 ADR §理由 的"强制 round-trip"论证是错的。

v2 修正为 **sentinel produce**:在事务中真实 produce 一条 `StartupFenceEvent`,迫使 franz-go 的懒加载 InitProducerID 被触发,进入其内部的 `doWithConcurrentTransactions` retry loop([txn.go:998](/Users/xargin/go/pkg/mod/github.com/twmb/franz-go@v1.18.0/pkg/kgo/txn.go:998)),retry 到 broker 返回 success —— 此时老 txn 已到达 CompleteAbort 状态(abort marker 写入 partition),新 epoch 生效。之后的 Produce + EndTransaction(commit) 再写 B 的 sentinel record + commit marker。

## 理由 (Rationale)

### 为什么 sentinel produce 是必要的(替代 v1 "no-op commit")

Kafka `InitProducerID` 返回 success 时保证了"老 producer_epoch 已 fence 且老 pending txn 已到 CompleteAbort"(因为 franz-go 会在 `ConcurrentTransactions` 错误时内部 retry 直到 CompleteAbort)。但关键问题是:**InitProducerID 本身是懒触发的,必须有一次真实的 Produce 才会被调起来**。

sentinel produce 的作用链:

```
Counter B: producer.Produce(StartupFenceEvent, vshard partition)
    │
    ├─→ franz-go 首次触发 InitProducerID(transactional.id=counter-vshard-{id})
    │     │
    │     ├─→ broker Transaction Coordinator:
    │     │     state==Ongoing(A's pending txn) 
    │     │     → 转 PrepareAbort → 发 WriteTxnMarkers(abort) 到 partition 
    │     │     → 等所有 partition ack → 转 CompleteAbort → bump producer_epoch
    │     │     (franz-go 在此期间可能收到 ConcurrentTransactions 并 retry)
    │     │
    │     └─→ InitProducerID 返回新 epoch(此时 abort marker 已落 partition)
    │
    ├─→ franz-go: AddPartitionsToTxn(journalTopic, vshard)
    │
    └─→ 真实写 StartupFenceEvent 到 partition (pending 状态)

Counter B: producer.EndTransaction(TryCommit)
    │
    └─→ broker 写 commit marker 到 partition(acks=AllISR 保证已写入 ISR)
         → EndTransaction 返回 success
```

到达 partition 的事件顺序(从 LSO / LEO 视角):

1. Counter A pending txn 的 abort marker(offset N)
2. Counter B sentinel StartupFenceEvent(offset N+1)
3. Counter B commit marker(offset N+2)

LEO = N+3。trade-dump `ListEndOffsets` 拿到 N+3。shadow consumer(ReadCommitted)会:跳过 A 的 aborted 数据 → no-op apply StartupFenceEvent → fetch position 前进到 N+3。`WaitAppliedTo(N+3)` 收敛。

成本约 50~300ms(取决于 A 有没有 pending txn)。**关键**:这个成本以前是用 "被动等 transaction timeout 10s" 换来的,现在是主动 fence,worst case 绝对更快。

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

### G4:sentinel produce 本身失败(Kafka 不可用)

Counter 无法进入 on-demand 路径;fallback 也需要 producer 就位才能最终启动,同样失败。这是现有 ADR-0058 / ADR-0060 共同约束,本 ADR 不新增此类失败场景。

### G5:ADR-0058 §4.5 的"老 owner 假死脏写窗口"

现有窗口是 "老 owner 的已进 broker 但未 commit 的事务,在 fence 之后仍可能成功 commit"。本 ADR 的 sentinel produce 加速了 fence(主动 InitProducerID vs 被动 timeout),但**不消除**该脏写窗口(窗口靠 `match_seq` / `counter_seq` 应用层 dedup 兜底)。

- 脏写发生在 Counter B 的步骤 ④ 之前:trade-dump 看到脏 commit 并 apply,on-demand snapshot 会包含它 —— 这是 **correct behavior**(shadow 看到 committed 就是 committed)
- 脏写发生在 snapshot Capture 已完成之后:snapshot 不含,Counter B 从 trade-event 侧的 `match_seq` guard 去重

相比 ADR-0060 §4.2 原 recovery 流程**不引入新风险**,且由于 fence 更快反而缩小了脏写窗口。

### G6:on-demand snapshot 被反复请求导致 S3 成本上升

正常场景下 Counter 启动才会触发 on-demand,频率极低。异常场景(如 Counter 在 ready 循环 crash/restart)可能产生大量 on-demand snapshot。

**缓解**:

- trade-dump 对同一 `vshard_id` 的并发 `TakeSnapshot` singleflight 合并
- 全局 semaphore(限 16 在飞)防同机整机重启时 256 vshard 打爆
- housekeeper 1h 清理
- `trade_dump_ondemand_snapshot_requests_total` metric 超阈值告警

### G7:counter-journal 的 retention 比周期 snapshot 短

继承 ADR-0060 §G(retention vs snapshot interval)。本 ADR 把周期放松到 60s 后,journal retention 必须 ≥ 60s,实际建议 ≥ 1h(给 fallback catch-up 留充足空间)。

**缓解**:现有 ADR-0060 §G 的 sanity check(启动时比对 `snapshot.journal_offset` vs `earliest offset`)自动覆盖。

### G8:Counter B 的 EndTransaction 超时但 broker 实际 commit 成功

网络慢 / Counter B GC 的情况下,Counter B 的 `EndTransaction(TryCommit)` 客户端超时,但 broker 已写入 sentinel record + commit marker。Counter 认为 sentinel produce 失败 → fallback。

- **数据一致性**:无影响。sentinel record 是 no-op event,trade-dump shadow 照常 apply(仍是 no-op)。Counter fallback 路径会走 catchUpJournal 消费到这条 sentinel 并 no-op
- **Side effect**:journal 里多一条 StartupFenceEvent 不对应任何实际 on-demand 调用的记录(孤儿)。无害
- **监控**:`counter_startup_mode_total{mode=fallback, reason=sentinel_produce_timeout}` 异常升高需查 Kafka/网络健康

相比 G5 脏写窗口,G8 的影响面更小(sentinel 是 no-op,相比业务 event 无副作用)。

### G9:Run goroutine 在 trade-dump 单进程内 stall

ADR-0061 pipeline 是**单 Run goroutine 多路复用 256 vshard**。如果 Run 卡在某个 vshard 的 Apply(慢 apply / Kafka fetch 长阻塞 / GC),所有 vshard 的 shadow 都不前进,所有 WaitAppliedTo 同时超时,所有 on-demand 请求都失败退到 fallback。

**缓解**:

- `shadow_apply_stall_seconds{vshard}` gauge 监控 last-apply-age;单 vshard stall > 5s 告警(表示 Run 某 apply hang)
- `shadow_apply_stall_seconds` 全 vshard 中位数 > 1s 告警(表示 Run 整体 starve)
- trade-dump 多实例部署,consumer group rebalance 把 stall 实例的 vshard 迁走

长期方案:把 pipeline 拆成 per-vshard goroutine(ADR-0061 backlog)。

## 实施约束 (Implementation Notes)

### Kafka 相关

- trade-dump 查 LEO:`kadm.ListEndOffsets(ctx, journalTopic)`(franz-go),底层 `ListOffsets` 请求参数 `timestamp=-1, isolation_level=READ_UNCOMMITTED`
- Counter sentinel produce(v2 修正):
  ```go
  if err := producer.BeginTransaction(); err != nil { return err }
  fenceEvt := &eventpb.CounterJournalEvent{
      Meta:         &eventpb.EventMeta{ /* 常规填充 */ },
      CounterSeqId: 0,  // sentinel 不分配 counter_seq
      Payload: &eventpb.CounterJournalEvent_StartupFence{
          StartupFence: &eventpb.StartupFenceEvent{
              NodeId: cfg.NodeID,
              Epoch:  cfg.Epoch,
              TsMs:   time.Now().UnixMilli(),
          },
      },
  }
  bytes, _ := proto.Marshal(fenceEvt)
  producer.ProduceSync(ctx, &kgo.Record{
      Topic:     cfg.JournalTopic,
      Partition: int32(cfg.VShardID),
      Value:     bytes,
  })
  if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
      return fmt.Errorf("sentinel commit: %w", err)
  }
  ```
- `kgo.TransactionTimeout = 10s`(ADR-0058 §4.3)不变,仅兜底被动 abort
- producer `transactional.id` 沿用 ADR-0058 的 `counter-vshard-{id}`
- **所有 counter-journal consumer 必须用 `ReadCommitted` isolation level**(包括 trade-dump shadow consumer、counter 自己的 fallback catchUpJournal consumer、未来任何新增的 consumer)。`ReadUncommitted` 会看到 aborted record,误 apply 后 idempotency 保证失效(详见 §3.2)。code review 需要强制这条
- franz-go 行为引用:[txn.go:901-904 anyAdded short-circuit](/Users/xargin/go/pkg/mod/github.com/twmb/franz-go@v1.18.0/pkg/kgo/txn.go:901)、[txn.go:998 doWithConcurrentTransactions](/Users/xargin/go/pkg/mod/github.com/twmb/franz-go@v1.18.0/pkg/kgo/txn.go:998)

### Counter 改动

- `worker.Run()` 在 [worker.go:220](../../counter/internal/worker/worker.go:220)(`NewTxnProducer` 之后、`catchUpJournal` 之前)插入新流程
- 新增 `tradeDumpClient` 字段,worker.go:200 附近初始化 gRPC conn
- `worker.go` Phase 1/Phase 2 切分原则:
  - Phase 1 期间所有状态只写入 `freshState` / `freshSeq` / `freshOffsets` 局部变量
  - Phase 2 是一个连续的 critical section,开始前做最后一次 sanity check(leo ≥ legacy.JournalOffset、counter_seq 非零等),之后的任何错误都 fatal,不 fallback
- 新增配置项:
  - `--startup-mode=auto|on-demand|legacy`,默认 `auto`
  - `--trade-dump-endpoint=<host:port>`
  - `--on-demand-timeout=3s`
- fallback 路径保持现状,不改动 `catchUpJournal` 实现

### trade-dump 改动

- 新增 gRPC server(独立 listener,端口 tbd,写入 config/runbook)
- proto package:`api/rpc/tradedump/snapshot/v1/snapshot.proto`(遵循现有 api 目录布局)
- shadow engine 新增:
  ```go
  // trade-dump/internal/snapshot/shadow/engine.go
  // Run goroutine 每次 Apply 末尾 Store; 读侧在 shadow.go
  publishedOffset atomic.Int64
  
  // trade-dump/internal/snapshot/shadow/shadow.go
  func (s *Shadow) WaitAppliedTo(ctx context.Context, vshard int, target int64) error
  ```
- `epochTracker`:per-vshard last-seen epoch,`map[int32]uint64` + RWMutex,进程内,重启丢失可接受
- 并发 semaphore:全局限制 on-demand in-flight(默认 16,可配 `--ondemand-concurrency`)
- singleflight(按 vshard_id):同一 vshard 并发请求合并
- housekeeper goroutine:每 5min 扫 S3 `*-ondemand-*` prefix,删 mtime > 1h 的 key

### Counter engine / shadow engine 改动

- 新增 `applyStartupFence` no-op 实现,跨 counter/engine 和 trade-dump/shadow 共用(通过现有 `counter/engine` 包复用 ADR-0061 §2)
- 版本兼容:`ApplyCounterJournalEvent` 的 payload switch 遇到**不识别的 variant**必须 return nil,**不得** panic。现有代码若不符合需一并修

### 监控

所有 metric 均带 per-vshard label(除非另注):

**Counter 侧**:
- `counter_startup_duration_seconds{vshard, mode=on-demand|fallback}` — histogram
- `counter_startup_mode_total{mode, result=success|failure}` — counter
- `counter_ondemand_rpc_duration_seconds{vshard, result}` — histogram(Counter 视角的 gRPC 耗时)
- `counter_ondemand_rpc_total{result=ok|timeout|unavailable|failed_precondition|resource_exhausted|internal_error|other}` — counter
- `counter_sentinel_produce_duration_seconds{vshard, result}` — histogram(Phase 1 step ③ 耗时)

**trade-dump 侧**:
- `trade_dump_ondemand_snapshot_duration_seconds{vshard, phase=leo_query|wait_apply|capture|serialize|upload}` — histogram
- `trade_dump_ondemand_snapshot_requests_total{result=ok|leo_query_fail|wait_apply_timeout|capture_error|serialize_error|s3_upload_error|stale_epoch|coalesced|resource_exhausted}` — counter
- `trade_dump_ondemand_snapshot_bytes{vshard}` — gauge
- `trade_dump_ondemand_inflight{}` — gauge(全局,semaphore 使用量)
- `shadow_published_offset{vshard}` — gauge(对接 WaitAppliedTo 调试)
- `shadow_apply_stall_seconds{vshard}` — gauge(now - last_apply_ts,监控 G9)

**告警阈值建议**:
- `counter_startup_mode_total{mode=fallback}` 非零 → warning(说明 on-demand 在失败,查 trade-dump 健康)
- `shadow_apply_stall_seconds` 任一 vshard > 5s → page(G9 stall)
- `trade_dump_ondemand_snapshot_age_seconds{vshard}` > 2min(继承 ADR-0061 §7,周期收紧)

### 测试

- **单元**:
  - `WaitAppliedTo` 超时 / context cancel 行为(构造 mock Engine.publishedOffset)
  - sentinel produce 失败时 Counter fallback 路径被触发
  - `TakeSnapshot` singleflight:构造同 vshard 并发 100 请求,验证 Capture 只触发一次
  - semaphore block:acquire 超过上限时后续请求拿 ResourceExhausted
  - epochTracker:stale epoch 被拒,新 epoch 更新 last-seen
  - `ApplyCounterJournalEvent` 对未识别 payload variant 不 panic

- **集成**(需 Kafka + trade-dump + Counter 一起起):
  - **Hot path**:Counter 正常启动 → on-demand 成功,启动耗时 < 3s
  - **trade-dump 不可达**:Counter fallback,启动成功(≤ 30s)
  - **Counter A pending txn**:构造方法 —— 单独起一个 `transactional.id=counter-vshard-5` 的 producer,BeginTransaction + Produce + **不** commit + exit。然后起 Counter B with same vshard。验证 B 的 sentinel produce 成功(经 InitProducerID retry loop 收敛),LEO 稳定,on-demand 成功
  - **trade-dump hang 在 apply**:mock shadow Engine 的 Apply 阻塞 5s,验证 WaitAppliedTo 2s 超时 + Counter 自动 fallback
  - **Phase 2 install failure**:注入 `SetOffsets` error,验证 Counter 进程 fatal exit(不 silent fallback)
  - **整机 restart**:10~16 个 vshard 同时启动,验证 trade-dump semaphore 正常工作,无死锁
  - **Stale epoch**:Counter 用老 epoch 请求 TakeSnapshot → FailedPrecondition → fallback
  - **Version compat**:老版 shadow engine 消费包含 `StartupFenceEvent` 的 journal,验证 no-op apply 不 panic

- **压测**:
  - 启动耗时 p99 < 3s(hot path)
  - trade-dump 处理 on-demand 请求 p99 < 2s
  - 16 并发 on-demand 请求下 trade-dump 内存 / CPU 上升可接受

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

- ADR-0017: Kafka transactional.id 命名(epoch 嵌入约定)
- ADR-0048: Snapshot + offset 原子绑定(本 ADR 承接 recovery 部分的 offset 语义)
- ADR-0058: Counter 虚拟分片 + 实例锁(提供 fence 语义,§4.1-4.5 是 on-demand 流程的前置)
- ADR-0060: Counter 消费异步化 + catch-up 协议(本 ADR supersede 其 §4.2 recovery 主路径,保留 fallback)
- ADR-0061: Trade-dump Snapshot Pipeline(shadow engine 提供 on-demand 服务端能力)
- Kafka KIP-98: Exactly Once Delivery and Transactional Messaging(InitProducerID / pending txn abort 语义)
- Kafka KIP-360: Improve reliability of idempotent/transactional producer(InitProducerID retry + epoch bump)
- Kafka Protocol: ListOffsets API v5+(isolation_level 参数定义 LEO vs LSO 语义)
- franz-go v1.18.0 源码:[txn.go:901](/Users/xargin/go/pkg/mod/github.com/twmb/franz-go@v1.18.0/pkg/kgo/txn.go:901)(EndTransaction anyAdded short-circuit)、[txn.go:998](/Users/xargin/go/pkg/mod/github.com/twmb/franz-go@v1.18.0/pkg/kgo/txn.go:998)(doWithConcurrentTransactions retry loop)

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| CEO Review | `/plan-ceo-review` | Scope & strategy | 0 | — | — |
| Codex Review | `/codex review` | Independent 2nd opinion | 0 | — | — |
| Eng Review | `/plan-eng-review` | Architecture & tests (required) | 1 | LOCK-IN / issues_fixed | P0×1(F1 no-op commit 断言错误 → 改 sentinel produce),P1×4(WaitAppliedTo 并发模型、Phase1/2 安全点、epoch 校验、fallback no-retry),P2×3(metric 粒度、semaphore、版本兼容);均已落盘 v2 |
| Design Review | `/plan-design-review` | UI/UX gaps | 0 | — | — |
| DX Review | `/plan-devex-review` | Developer experience gaps | 0 | — | — |

**UNRESOLVED:** 0。

**VERDICT:** ENG REVIEW CLEARED(LOCK-IN v2)。ADR 内所有 P0/P1/P2 findings 已逐项写入,可进入实施(M1 starts next)。

**Review-driven critical correction**:v1 "no-op commit" 流程完全错误(franz-go `BeginTransaction + EndTransaction(no produce)` 是纯内存 no-op,不触发 InitProducerID,不 fence,不写 Kafka);v2 替换为 sentinel produce(新增 `StartupFenceEvent` tag 52)驱动真实 round-trip。其余 findings 按推荐方案落地。
