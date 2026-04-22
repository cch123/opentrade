# ADR-0060: Counter 消费异步化 + Trade-Event Checkpoint

- 状态: Accepted（M1–M8 已落地；**M7 `SnapshotMu` 顶层锁**已随 ADR-0061 Phase B 撤销 — Counter 不再自产 snapshot，无需 stop-the-world 协调）
- 日期: 2026-04-22
- 相关 ADR: 0001（Kafka SoT）、0003（Counter-Match via Kafka）、0004（counter-journal）、0017（Kafka transactional.id）、0018（per-user sequencer + counterSeq）、0048（snapshot + offset 原子绑定，本 ADR 部分 supersede）、0049（snapshot protobuf/json）、0054（per-symbol order slots）、0057（Asset service + Transfer Saga）、0058（Counter 虚拟分片 + 实例锁）、0059（Kafka 集群扩容长期调研）、0061（trade-dump snapshot pipeline，本 ADR §5 M7 在其 Phase B 撤销）、0062（订单终态 evict，配套 ADR）

## 术语 (Glossary)

| 本 ADR 字段 | 含义 | 业界对标 |
|---|---|---|
| `SubmitAsync` | 非阻塞提交 task 到 per-user queue 的新 API，替代原有阻塞 `Execute` | — |
| `pendingList` | VShardWorker 内的"已派发但未完成"trade-event 追踪结构，用于推进 watermark | Kafka Streams 的 stream time |
| `ordered advancer` | 从 pendingList 头部推进"连续完成前缀"的 goroutine | — |
| `te_watermark` | 已完全处理完（所有派生 business event 已 commit 到 journal）的 trade-event offset 水位 | — |
| `TECheckpoint` | VShardWorker 在 watermark 推进后 publish 到 counter-journal 的特殊 event，携带 `(te_partition, te_offset)`，供下游定位 | — |
| `Publish 保序保证` | 依靠 `TxnProducer.mu` 串行化 + Checkpoint 在 business event 之后 Publish，保证 `checkpoint(W)` record offset > te=W 派生的所有 business event record offset | — |
| `catch-up journal` | Counter recovery 时从 `snapshot.journal_offset` 消费 counter-journal 到 end，补齐 RPC 派生事件的效应 | — |
| `cold start dependency` | trade-event consumer 的起始位置完全来自 snapshot；没有 Kafka consumer group offset | — |

## 背景 (Context)

### 当前 Counter 消费模型的两个缺陷

**缺陷 1：消费循环阻塞**

[trade_consumer.go:154-170](../../counter/internal/journal/trade_consumer.go:154) 的 `Run()` 循环串行调用 `handler.HandleTradeRecord(...)`，而 [service.go:174-184](../../counter/internal/service/service.go:174) 里：

```go
func (s *Service) HandleTradeRecord(ctx context.Context, evt *eventpb.TradeEvent, partition int32, offset int64) error {
    if err := s.HandleTradeEvent(ctx, evt); err != nil {
        return err
    }
    s.advanceOffset(partition, offset+1)
    return nil
}
```

`HandleTradeEvent` 内部调 `applyPartyViaSequencer` → `s.seq.Execute(userID, fn)` [service/trade.go:164-174](../../counter/internal/service/trade.go:164)。`Execute` 阻塞等待 per-user drain goroutine 执行 fn 并 `publisher.Publish` 同步返回 [sequencer/user_seq.go:81-97](../../counter/internal/sequencer/user_seq.go:81)。

**后果**：
- 消费循环吞吐受限于"最慢单 user 的 fn 执行时间"
- per-user sequencer 为了并发而设计，但消费侧串行调用使得并发优势被完全抹掉
- Kafka lag 随单 user 处理延迟线性传导

**缺陷 2：snapshot cross-account 不一致**

[snapshot/snapshot.go:203-229](../../counter/snapshot/snapshot.go:203) 的 `Capture` 用 `sync.Map.Range` 遍历所有 user 并调用 `acc.Copy() / MatchSeqSnapshot() / RecentTransferIDsSnapshot()` 三次独立 RLock 读取。

`sync.Map.Range` 的 Go 官方文档明确：**"Range may reflect any mapping for that key from any point during the Range call"** —— 不是 consistent snapshot。

**后果**：并发 Transfer(A→B) 场景下，snapshot 可能记录"A 转账前 + B 转账后"，产生凭空资产。ADR-0048 的"snapshot + offset 原子"语义其实只保了 offset 写入原子，**没保证 snapshot 内容对应任何确定的 offset**。

本 ADR 不单独修复缺陷 2 的根因（由 ADR-0061 通过 "trade-dump 打 snapshot" 从架构层解决），但在过渡期引入 stop-the-world lock 作为临时修复（M7）。

### 为什么现在做

1. 未上线，breaking change 无顾虑（MEMORY）
2. 即将启动 ADR-0061（trade-dump snapshot pipeline），必须先把 Counter 侧的消费模型和 checkpoint 协议稳定下来
3. ADR-0062（订单终态 evict）配套立项，本 ADR 为其提供 journal event 机制（OrderEvictedEvent 走同一个 TxnProducer 路径）

## 决策 (Decision)

### 1. 消费循环 fire-and-forget（SubmitAsync + pendingList + advancer）

#### 1.1 Sequencer 新增 SubmitAsync API

```go
// SubmitAsync 把 fn 投递到 userID 的 FIFO queue 后立刻返回。
// fn 执行完成（含 publisher.Publish 同步返回）后通过 cb 通知调用方。
// 与现有 Execute 的区别：Execute 阻塞等 fn 返回；SubmitAsync 不阻塞。
//
// ErrQueueFull 不再返回 —— per-user queue 已改为无界链表（见 1.2）。
// fn panic / cb panic 按 Go 标准 goroutine 语义传播。
func (s *UserSequencer) SubmitAsync(
    userID string,
    fn func(counterSeq uint64) error,
    cb func(err error),
)
```

`Execute` API 保留给 RPC 路径（Transfer / PlaceOrder / CancelOrder）使用，因为这些 RPC 天然需要同步返回结果。**只有 trade-event 消费路径改用 SubmitAsync**。

#### 1.2 userQueue 改无界链表

替换现有 `chan *task` 为 `container/list.List + sync.Mutex + chan struct{}` 组合：

```go
type userQueue struct {
    mu     sync.Mutex
    tasks  *list.List       // FIFO 无界
    notify chan struct{}    // 容量 1 的信号 channel
    state  atomic.Int32     // 保留 Idle/Running 协议
}

func (uq *userQueue) submit(t *task) {
    uq.mu.Lock()
    uq.tasks.PushBack(t)
    uq.mu.Unlock()
    select {
    case uq.notify <- struct{}{}:
    default: // 已有未消费信号, drop
    }
}
```

Drain goroutine 在拿不到 task 时阻塞在 `<-uq.notify`，有 task 时持续 drain 到空。

`ErrQueueFull` 移除。消费循环永不被 queue 容量 block。内存保护依靠 1.5 的 Publish 失败 5s panic。

#### 1.3 VShardWorker 消费循环重写

```go
type inFlightTE struct {
    tePartition int32
    teOffset    int64
    pending     atomic.Int32  // 还有几个 fn 未完成 (0/1/2)
}

// 消费循环（主 goroutine，非阻塞）
for msg := range consumer.PollFetches() {
    infl := pendingList.enqueue(msg.Partition, msg.Offset, expectedFnCount)
    
    if ownsMaker {
        seq.SubmitAsync(maker_uid, fn_maker, func(err error) {
            onFnDone(infl, err)
        })
    }
    if ownsTaker {
        seq.SubmitAsync(taker_uid, fn_taker, func(err error) {
            onFnDone(infl, err)
        })
    }
    if expectedFnCount == 0 {
        // foreign-user / self-trade-skip 等情形: 立即 done
        onFnDone(infl, nil)
    }
    // 立刻 poll 下一条, 不等 fn 完成
}

func onFnDone(infl *inFlightTE, err error) {
    if err != nil {
        // 仅限不可重试错误传到这里（可重试已在 Publish 内部 5s 循环处理）
        panic(...)  // fail-fast: 让进程重启靠 snapshot 恢复
    }
    if infl.pending.Add(-1) == 0 {
        advancerCh <- infl
    }
}
```

#### 1.4 Ordered Advancer

```go
// 独立 goroutine
for infl := range advancerCh:
    pendingList.markDone(infl)
    // 从队列头部推进"连续完成前缀"
    newWatermark := pendingList.popConsecutivePrefix()
    if newWatermark > lastCheckpoint {
        // 唯一外部副作用: 发 checkpoint 给下游
        txn.PublishCheckpoint(vshardKey, TECheckpoint{
            TePartition: tePartition,
            TeOffset:    newWatermark,
        })
        lastCheckpoint = newWatermark
        // 同步更新 Counter 内部的 Offsets map (供自产 snapshot 使用, 直到 ADR-0061 接管)
        svc.advanceOffset(tePartition, newWatermark+1)
    }
```

**关键保序性质**：`TxnProducer.mu` 全局串行化 + Checkpoint 在 `onFnDone(pending==0)` 后才被 advancer 发送，而 `pending==0` 意味着该 TE 所有 `publisher.Publish` 已返回（fn 里 Publish 同步等 Kafka 确认）。因此 checkpoint(W) 的 record offset **严格大于** te ≤ W 派生的所有 business event 的 record offset。

### 2. TECheckpoint Event

扩展 `CounterJournalEvent` oneof body：

```protobuf
message CounterJournalEvent {
  // ... 现有字段 ...
  oneof body {
    SettleEvent   settle   = 10;
    FreezeEvent   freeze   = 11;
    TransferEvent transfer = 12;
    OrderStatusEvent order_status = 13;
    // ...

    TECheckpointEvent checkpoint = 50;  // 新增
  }
}

message TECheckpointEvent {
  int32 te_partition = 1;  // = vshard_id, 冗余字段方便 debug
  int64 te_offset    = 2;  // 此 checkpoint 覆盖 te_offset ≤ te_offset 的所有业务效应
}
```

Checkpoint 的 partition key 直接指定为 vshard_id，走 `kgo.ManualPartitioner` 写入对应 counter-journal partition。和 business event 进同一 partition → Kafka partition 内 FIFO 保证下游顺序可见。

**Checkpoint 不影响 Counter 自身的 state**（不 apply，不 Capture）。它是给下游（trade-dump snapshot pipeline 由 ADR-0061 使用）的 watermark 信号。

### 3. Publish 失败策略：5s 重试后 panic

`TxnProducer.Publish` 和 `PublishOrderPlacement` 内部按如下策略处理错误：

```go
func (p *TxnProducer) publishWithRetry(ctx context.Context, ...) error {
    deadline := time.Now().Add(5 * time.Second)
    for {
        err := p.runTxn(ctx, fn)
        if err == nil { return nil }
        if time.Now().After(deadline) {
            // 5s 内持续失败 → panic, 触发进程重启 + vshard failover (ADR-0058)
            panic(fmt.Sprintf("journal publish failed for 5s: %v", err))
        }
        // 指数退避或固定间隔重试
        time.Sleep(retryBackoff())
    }
}
```

理由：
- Kafka 瞬时错误常见（leader 切换、短暂网络抖动），5s 内重试能消化绝大多数
- 超过 5s 仍失败说明 Kafka 侧有系统性问题，Counter 自行重试无益
- panic 让进程重启，vshard failover 到其他健康节点（ADR-0058 冷切）

### 4. Recovery 协议修正

#### 4.1 Snapshot 字段扩展

```protobuf
message ShardSnapshot {
  // ... 现有字段 (accounts, orders, seq, etc.) ...

  // 扩展字段
  uint64 snapshot_counter_seq = ??;  // 打 snapshot 瞬间的 s.counterSeq 值
  int64  journal_offset       = ??;  // 打 snapshot 瞬间 counter-journal partition 的下一条消费位置
  // te offsets 已有 (`Offsets` repeated field), 这里改名语义: te_watermark
}
```

#### 4.2 Recovery 流程

```go
func (w *VShardWorker) restore(ctx context.Context) error {
    // Step 1: load snapshot
    snap, err := snapshot.Load(ctx, w.cfg.Store, key)
    if err != nil { return err }
    w.state.Restore(snap)
    w.seq.SetCounterSeq(snap.SnapshotCounterSeq)

    // Step 2: catch-up counter-journal from snap.JournalOffset to end
    //   补齐 snapshot 之后、crash 之前产生的 journal event (RPC 派生 / 未被 snapshot 捕获的 te 派生)
    err = catchUpJournal(ctx, w, snap.JournalOffset)
    if err != nil { return err }
    // catch-up 过程中每 apply 一条 event:
    //   - apply 到 state (幂等: match_seq guard for te-派生, transfer_id ring for transfer, etc.)
    //   - 同步推进 w.seq.counterSeq = max(current, event.counter_seq_id)

    // Step 3: 启动 trade-event consumer, seek to snap.TeWatermark
    //   起始 offset 从 snapshot 读, 不用 Kafka consumer group (ADR-0058 ConsumePartitions assign 模式)

    // Step 4: 启动 gRPC server, 开始接 RPC 流量
    return nil
}
```

**关键前提**：
- match_seq guard 严格幂等，粒度为 `(user, symbol)`（本 ADR 核对过 [state.go:267-281](../../counter/engine/state.go:267)）
- Transfer 靠 per-user `recentTransferIDs` ring（ADR-0057）
- 订单状态路径：**本 ADR 假设订单永不删**（ADR-0062 引入 evict 后由 `recentTerminatedOrders` ring 兜底）
- Kafka 事务 + `read_committed` 过滤 aborted 事务
- catch-up 阶段必须消费到 partition end 才能启动 trade-event consumer（否则 state 不一致）

#### 4.3 Cold start 约束

- trade-event consumer 起始位置**唯一来源是 snapshot.te_watermark**
- 不使用 Kafka consumer group offset（延用 ADR-0058 的 `ConsumePartitions` assign 模式）
- **trade-dump 必须先产出至少一份 snapshot，Counter 才允许上线**（等 ADR-0061 落地后生效；在那之前 Counter 用自己打的 snapshot）
- 灾难恢复 runbook（snapshot 全丢）：从 trade-event 起点 replay（消耗大），用专门工具走慢路径，不走正常启动流程

### 5. Snapshot 一致性的临时修复（过渡方案）

在 ADR-0061 落地前，Counter 仍自己打 snapshot。为修复缺陷 2（cross-account 不一致），引入**顶层 snapshot lock**：

```go
// 新增 vshard-level 读写锁
type ShardState struct {
    // ... 现有字段 ...
    snapshotMu sync.RWMutex  // 新增: Capture 时持 W, 所有 apply 操作持 R
}

// per-user drain goroutine 执行 fn 时, 入口 acquire R lock
// Capture 时 acquire W lock (stop-the-world 一小段)
```

`snapshotMu.Lock()` 期间，所有 drain goroutine 的 `fn()` 等待（fn 会先拿 R lock 再执行 apply）。snapshot 的持锁时长 ≈ `Capture` 本身的遍历时间（指针拷贝方案下 ~5-50ms），可接受。

**这是过渡方案，ADR-0061 落地后撤销**（Counter 不再自己打 snapshot，snapshotMu 可删除）。

### 6. 订单永不删前提假设

本 ADR 的 recovery 正确性论证（特别是 RPC CancelOrder 幂等）依赖 `OrderStore.byID` 永不删除终态订单的当前行为。

ADR-0062 引入终态订单 evict + `recentTerminatedOrders` ring buffer 后，本假设由 ADR-0062 接管论证，**ADR-0062 必须在或早于本 ADR 的 M4 完成前落地至少 M1 + M5**（ring buffer + CancelOrder 查 ring 路径），否则 evict 后 RPC 幂等会断。

## 备选方案 (Alternatives Considered)

### 备选 1：保留 Execute 阻塞消费，用并行消费多条 trade-event

让消费循环 goroutine pool 化，每个 goroutine 独立消费一部分 trade-event。

**拒绝**：破坏"单 partition 内严格 FIFO"的假设，且 trade-event 是按 vshard 分区的（单 partition = 单 vshard = 单 worker），多 goroutine 拆不出来。

### 备选 2：per-user queue 保持 chan 容量 + 消费循环遇到满阻塞 poll

**拒绝**：队列容量是软限，做市商高频场景极易打满；阻塞 poll 等同于消费循环被 block，回到缺陷 1。

### 备选 3：每个 trade-event 的派生 business event 都带 `TradeEventCursor` 字段（不引入独立 checkpoint event）

前一轮讨论探索过。**拒绝**：RPC 派生事件（Transfer/CancelOrder）不归属任何 trade-event，强要他们带 cursor 是架构错配；而且 per-user queue 里 RPC event 和 te event 交错，cursor 语义混乱。独立的 checkpoint event 更干净。

### 备选 4：Publish 失败无限重试

**拒绝**：Kafka 侧如果是系统性故障（集群挂了），无限重试会让 Counter 堆积未处理 task、OOM。5s panic + 进程重启更可控。

### 备选 5：Snapshot 一致性不做临时修复，等 ADR-0061 彻底解决

**拒绝**：ADR-0061 的落地窗口可能 1-2 月，这期间 Counter 打的 snapshot 如果被用于生产恢复，存在资产不一致风险。stop-the-world lock 改动小，过渡期收益明确。

## 理由 (Rationale)

### 为什么 fire-and-forget 而不是 pool 化消费

Counter 的 partition ↔ vshard 严格 1:1 绑定（ADR-0058），单 partition 不能多 goroutine 并行消费（破坏 FIFO 保证）。fire-and-forget 在**保持单消费者**的前提下把处理阶段异步化，是唯一在架构约束下能解耦消费和处理的路径。

### 为什么 per-user queue 改无界

- 业务正常速率下，per-user fn 执行 << 消费速率，queue 长度稳态为 0
- 异常场景（Publish 卡住）已由 5s panic 兜底，不会让 queue 无限膨胀
- 有界队列的 back-pressure 不适合 Counter（消费循环不能 block）

### 为什么 Checkpoint 放 counter-journal 而不是独立 topic

- 复用现有 Kafka 事务 + partition 基础设施
- partition FIFO 自动保证 checkpoint 在 business event 之后可见
- 独立 topic 会增加运维面（topic 数 +1）、破坏事务原子性

### 为什么 Counter 不 commit consumer group offset

[trade_consumer.go:115-150](../../counter/internal/journal/trade_consumer.go:115) 已经是 `ConsumePartitions` assign 模式，不走 consumer group 协议。te 位置的权威源 = snapshot。保持现状，避免引入新的持久化状态和一致性问题。

## 影响 (Consequences)

### 正面

- 消费循环解除阻塞，Counter 吞吐上限由 per-user drain goroutine 并发能力决定，不受单 user 拖慢
- Recovery 协议补齐 counterSeq 推进 + journal catch-up，ADR-0048 的一致性前提得到兑现
- TECheckpoint event 成为 Counter ↔ trade-dump 之间的标准同步接口，为 ADR-0061 铺平道路
- Publish 失败 5s 重试 + panic 模式明确故障边界

### 负面 / 代价

- 代码复杂度增加（pendingList / ordered advancer / 无界 queue 的并发管理）
- per-user queue 内存无界（由 Publish 5s panic 兜底，但极端场景仍可能短暂膨胀）
- Snapshot stop-the-world lock 是过渡方案，额外实现成本，在 ADR-0061 落地后撤销
- 订单永不删假设对 ADR-0062 产生强依赖（M1 + M5 必须同步落地）

### 中性

- 消费循环结构变化使得现有集成测试需要重写（fn 执行异步化后，断言时序从"Execute 返回 = 已处理"变成"advancerCh 有信号 = 已处理"）
- Monitoring 需要新增指标：per-vshard pendingList 深度、advancer 信号延迟、TECheckpoint lag

## 实施里程碑 (Milestones)

- **M1** ✅ `counter/internal/sequencer` 新增 `SubmitAsync(userID, fn, cb)` API；userQueue 改 `container/list.List` + signal chan 无界队列。`Execute` 路径保留给 RPC。单测覆盖 1w 并发 SubmitAsync 不丢、drain 幂等、idle 退出
- **M2** ✅ `counter/internal/worker` 引入 `pendingList` + ordered advancer，消费循环 fire-and-forget。单测覆盖乱序 fn 完成时 watermark 推进、0/1/2 pending
- **M3** ✅ `api/event/counter_journal.proto` 新增 `TECheckpointEvent`;`TxnProducer.PublishToVShard` 直接写指定 partition。advancer 每推进一次 publish 一条 checkpoint
- **M4** ✅ `TxnProducer.runTxnWithRetry` 5s budget + panic;context 取消立即返回;单测覆盖瞬时失败 / 持续失败
- **M5** ✅ 依赖 ADR-0062 M1 + M5（ring buffer + CancelOrder 查 ring）
- **M6** ✅ `snapshot.JournalOffset` 字段;`TxnProducer.JournalOffsetNext()` / `NoteObservedJournalOffset()`;Counter 启动 `catchUpJournal` 从 `snap.JournalOffset` 消费到 HWM idle,用 `engine.ApplyCounterJournalEvent` 幂等 apply
- **M7** ⚠️ 原方案:`ShardState.SnapshotMu` 顶层 RWMutex + `sequencer.WithApplyBarrier`。**已随 ADR-0061 Phase B 撤销** — Counter 不再自产 snapshot,trade-dump 的 ShadowEngine 单线程 Apply 天然无跨账户不一致问题,不再需要 stop-the-world 协调
- **M8** ✅ `counter/internal/metrics` 包 + `--metrics-addr` HTTP /metrics;advancer / TxnProducer retry / evictor / catchUp 埋点;`docs/runbook-counter.md` runbook(snapshot 新鲜度相关监控已在 Phase B 后迁到 trade-dump runbook)

## 已知不防御场景 (Non-Goals / Known Gaps)

这些场景本 ADR 的设计**刻意不兜底**，需要外部机制或人工干预处理。显式列出以便未来 review 不遗漏。

### G1: Kafka 集群持续不可用 ≥ 5s

所有 `TxnProducer.Publish` 在 5s 重试后 panic（第 3 节决策），整个 vshard 进程重启；新 owner 选出后仍然无法 Publish，继续 panic → **全集群进入 restart loop**。

**不防御原因**：Counter 层无法自行解决 Kafka 集群故障，强行本地缓存待发送 event 会引入更复杂的一致性问题（缓存的 event 在 Counter 内存里，和 journal 顺序对不齐）。

**外部处置**：
- SRE 侧 Kafka 集群健康监控，持续告警
- 运维 runbook：Kafka 故障时先恢复 Kafka，再让 Counter 自愈

### G2: Snapshot 存储（S3 / EFS / MinIO）不可用

Counter 启动时 `snapshot.Load` 失败 → 进程无法 ready。

**不防御原因**：snapshot 是 recovery 的唯一真值源（te_watermark 从这里读），没有替代路径。

**外部处置**：
- Snapshot 存储的可用性保证由基础设施团队负责
- 灾难 runbook：snapshot 全丢时从 trade-event 起点冷 replay（专用工具，非正常启动流程）

### G3: Snapshot 人为误删

操作人员误删 S3 key / EFS 文件 → 等同于 G2。

**不防御原因**：权限管控是运维层问题，不在应用层设计范围。

**外部处置**：
- S3 / EFS 开启版本控制 + MFA delete
- snapshot 生产侧（ADR-0061）定期把最近 N 份 snapshot 复制到 cold storage

### G4: Journal topic retention < snapshot 周期

counter-journal 的 Kafka retention 配置短于 snapshot 周期，导致 recovery 的 catch-up 阶段找不到 `snapshot.journal_offset` 对应的 record（已被 retention 过期）。

**不防御原因**：retention 配置属于运维侧，本 ADR 不管控。

**外部处置**：
- 运维规范：counter-journal retention 必须 ≥ 最长 snapshot 间隔 × 3（冗余系数）
- 启动时加 sanity check：取 `snapshot.journal_offset` 所在 partition 的 `earliest offset`，若 `earliest > snapshot.journal_offset` 说明 retention 已过期 → 直接 panic（不要带病启动产生错误状态）

### G5: Advancer goroutine 卡住 / 死锁

如果 advancer goroutine 因 bug 卡住，watermark 停止推进 → TECheckpoint 不再发布 → trade-dump snapshot pipeline 的 te_watermark 固定 → ADR-0061 接管后 Counter recovery 的 te seek 点越来越陈旧（RPO 恶化）。

**不防御原因**：advancer 本身是纯内存计算 + channel 通信，没有外部依赖，正常不会卡；卡住属于 bug，应该通过测试覆盖。

**监控指标**：`counter_advancer_last_publish_ts`，超过 30s 未更新即告警。

### G6: pendingList 无限膨胀

如果某 per-user drain 长期卡住（不应该发生，因为 Publish 有 5s 超时 panic），pendingList 会积压。软上限 10w 仅告警不阻塞。

**不防御原因**：5s Publish panic 已经是上游兜底，pendingList 膨胀是下游症状，解决应该在上游。

**监控指标**：`counter_pendinglist_size{vshard}`，超过阈值告警。

### G7: Snapshot 过渡期（M7）的读写锁失效

`snapshotMu` 如果被某处 apply 路径漏持 R 锁，Capture 时会读到并发写的 state → 再次出现 cross-account 不一致。

**不防御原因**：这是 ADR-0061 落地前的临时方案，根本解决靠 ADR-0061（让 trade-dump 打 snapshot，Counter 不再自己打）。

**缓解**：所有 apply 路径必须 code review 过 snapshotMu R lock 覆盖；ADR-0061 落地后撤销整个 M7。

### G8: Counter crash 在 "fn 返回成功" 和 "onFnDone 执行" 之间 —— RPO 退化

场景：Kafka 事务已 committed（business event 已在 journal 里），但 pendingList 的 `pending.Add(-1)` 还没执行 → advancer 拿不到推进信号 → watermark 不推进 → Counter crash。

**正确性上完全能自愈（不是数据不一致）**：
- Counter 重启 → load snapshot → catch-up counter-journal 到 end → 刚才 committed 的 business event 被 replay apply，`match_seq` / `transfer_id ring` / 各幂等 state 都恢复
- 启动 trade-event consumer 从 `snapshot.te_watermark` 开始（还停在 crash 前）
- Kafka 重发同一条 trade-event → fn 重跑 → `matchSeqDuplicate` 命中（match_seq 已在 catch-up 阶段 advance）→ 跳过，不重复 Publish
- advancer 看到 pending==0，推进 watermark，发新 checkpoint
- 同理：Match 侧 replay 导致的 trade-event 重发也走这条幂等路径，对 Counter 透明

**本 ADR 真正不防御的只是**：crash → 重启这段**物理时间**（秒 ~ 分钟级）内 watermark 冻结，trade-dump snapshot 的新鲜度退化（RPO 退化）。不影响最终一致性，不影响资产守恒。

**外部处置**：重启时间由 ADR-0058 的冷切迁移 SLA 管控，不额外兜底。

## 实施约束 (Implementation Notes)

- `SubmitAsync` 的 cb 在 drain goroutine 里调，cb 必须是非阻塞的短操作（只做 atomic 计数 + channel send）
- pendingList 实现可用 `list.List` + `sync.Mutex`，或 slice + head index；容量默认上限 10w（异常时告警），超过则说明 Publish 侧系统性故障
- `advancerCh` 容量至少 = pendingList 上限 / 10，避免反压传递到 per-user drain
- `ordered advancer` 是单 goroutine，全局串行推进 watermark（per-vshard）
- Capture 加 snapshotMu 后，所有现有 `Account.mu` / `OrderStore.mu` 的 Lock 调用保持不变，两层锁之间没有 cycle 风险（snapshotMu 一定外层）
- Catch-up journal 用独立 kgo client + assign partition 模式，消费到 high watermark 后关闭
- Recovery 过程中 ProduceSync 路径禁用（仅消费 + 本地 apply），避免重启后重复写 journal

## 参考 (References)

### 项目内

- ADR-0001: Kafka as Source of Truth
- ADR-0003: Counter ↔ Match via Kafka
- ADR-0017: Kafka transactional.id
- ADR-0018: per-user sequencer + counterSeq
- ADR-0048: snapshot + offset 原子绑定（本 ADR supersede recovery 协议部分）
- ADR-0058: Counter 虚拟分片 + 实例锁
- ADR-0059 附录 A.7: 曾倾向"不立即切"，本 ADR 的动机推翻该倾向
- ADR-0061（后续）: trade-dump snapshot pipeline，接管 snapshot 生产
- ADR-0062: 订单终态 evict + recentTerminatedOrders ring

### 讨论

- 2026-04-22: 用户与 Claude 关于"snapshot 阻塞主流程 + 异步一致性悖论 + trade-dump 接管 snapshot"的完整设计讨论（会话记录）

## 后续工作建议

- [ ] ADR-0061 起草：trade-dump snapshot pipeline 具体实现
- [ ] ADR-0061 M-last: Counter 撤销 snapshotMu（M7）临时方案
- [ ] ADR-0062 同步落地 M1 + M5，解除本 ADR M5 的 verification gate
- [ ] 压测基准建立：Counter 消费吞吐 / p99 延迟，作为后续优化的比较基线
