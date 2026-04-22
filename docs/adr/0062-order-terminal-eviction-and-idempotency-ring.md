# ADR-0062: 订单终态 Evict + 幂等兜底 Ring Buffer

- 状态: Accepted（M1–M6 + M8 落地；M7 shadow apply 协议在 ADR-0061 shadow engine 中实现；M9 load test 未做）
- 日期: 2026-04-22
- 决策者: xargin, Claude
- 相关 ADR: 0001（Kafka SoT）、0015（client-order-id 索引）、0018（per-user sequencer + counterSeq）、0048（snapshot + offset 原子绑定）、0054（per-symbol order slots）、0057（Asset service + Transfer Saga, ring buffer 设计参考）、0058（Counter 虚拟分片 + 实例锁）、0060（Counter 消费异步化 + TECheckpoint，本 ADR 的 M1 + M5 是 0060 的 recovery 前提）

## 术语 (Glossary)

| 本 ADR 字段 | 含义 | 业界对标 |
|---|---|---|
| `终态订单` / `terminal order` | Status ∈ {Filled, Canceled, Expired, Rejected} 的订单 | — |
| `evict` | 把终态订单从 `OrderStore.byID` 内存 map 中物理删除 | — |
| `recentTerminatedOrders` | per-user ring buffer，记录最近被 evict 的终态订单 (order_id, final_status, terminated_at, client_order_id)，供 RPC CancelOrder 查幂等 | ADR-0057 `recentTransferIDs` 同构 |
| `RetentionDuration` | 订单进入终态到可以 evict 之间的最小时间窗（默认 1 小时） | — |
| `OrderEvictedEvent` | Counter 发往 counter-journal 的 event，标记某订单已被 evict，供下游（trade-dump shadow pipeline）同步状态 | — |
| `Evict 全局熔断` | 当 trade-dump snapshot lag 超过阈值时，Counter 暂停 evict 保护数据安全（M8） | Kafka Streams 的 `task.timeout.ms` 类概念 |

## 背景 (Context)

### 当前问题

[counter/engine/orders.go:86-109](../../counter/engine/orders.go:86) 的 `UpdateStatus`：

```go
func (s *OrderStore) UpdateStatus(id uint64, newStatus OrderStatus, ...) (*Order, error) {
    o, ok := s.byID[id]
    // ...
    if !prev.IsTerminal() && newStatus.IsTerminal() && o.ClientOrderID != "" {
        delete(s.activeByCOID, coidKey(o.UserID, o.ClientOrderID))  // 只删 COID 索引
    }
    // ...
    o.Status = newStatus  // ← 只改状态, byID 里对象保留
}
```

**`byID` 永不删**。全仓库无 `EvictHistory` / `evictOrder` / 终态驱逐 goroutine 相关代码。

### 后果

- 单 vshard 1 年累积估算：4w user × 10w 历史订单 ≈ **4 亿订单对象在内存**
- snapshot `.pb` 膨胀到 GB 级 → Capture deep-copy 时 CPU / GC 压力爆炸（即使 ADR-0060 M7 加了 snapshotMu，stop-the-world 窗口也会拉长到秒级）
- `OrderStore.All()` 迭代持锁时间与订单总数成正比
- 内存 OOM 风险

这是已知的 scaling 瓶颈，和 ADR-0060 的 snapshot 性能问题、ADR-0061 的 snapshot pipeline 都有强耦合。

### 为什么现在做（不做长期调研）

1. 未上线，breaking change 无顾虑（MEMORY）
2. ADR-0060 的 recovery 正确性假设"订单永不删"，这个假设在生产上线后**立即失效**（订单会天然累积）—— 不能拖到"到阈值再说"
3. ADR-0060 的 snapshot 性能（M7 stop-the-world）强依赖订单数可控
4. Ring buffer 模式在 ADR-0057 已经验证可行（transfer_id ring），这里是同构复用

## 决策 (Decision)

### 1. Evict 策略（时间窗）

订单可以 evict **当且仅当同时满足**：

```go
func (o *Order) canEvict(now int64, retention time.Duration) bool {
    return o.Status.IsTerminal() &&
        time.Duration(now*int64(time.Millisecond) - o.TerminatedAt*int64(time.Millisecond)) >= retention
}
```

配置：`RetentionDuration = 1 hour`（固定，不做 per-vshard / per-user 差异化，避免策略分散）。

**counterSeq lag 条件不引入**：时间窗 1 小时已经覆盖 trade-dump 消费 lag 的所有正常 + 大部分异常场景（trade-dump 挂 <1h 都能兜住）；counterSeq lag 维度和时间窗高度重叠，冗余不加价值（详见"理由"章节）。

`Order.TerminatedAt` 字段由 `UpdateStatus` 在终态转换时写入（从 `o.UpdatedAt` 迁移过来 / 新增），确保所有路径（Filled / Canceled / Expired / Rejected）都设置。

### 2. Per-User RecentTerminatedOrders Ring Buffer

#### 2.1 数据结构

在 `engine.Account` 中新增：

```go
type Account struct {
    // ... 现有字段 ...
    recentTerminated *ringBuffer[terminatedOrderEntry]
}

type terminatedOrderEntry struct {
    OrderID        uint64
    FinalStatus    OrderStatus  // Canceled / Filled / Expired / Rejected
    TerminatedAt   int64        // ms timestamp
    ClientOrderID  string
    Symbol         string       // 辅助 debug / 审计
    // 不含 amounts / balances：CancelOrder 响应体不需要，避免 ring 过大
}
```

`ringBuffer` 复用 ADR-0057 的 `recentTransferIDs` 实现（如 `pkg/ring`），FIFO 淘汰。

#### 2.2 容量策略

| 用户类型 | 默认容量 |
|---|---|
| 普通用户 | **1024**（覆盖 1h 内最多 1024 笔终态订单）|
| 做市商（ADR-0058 白名单）| **16384**（覆盖高频交易）|

判定做市商：启动时从 etcd `trading_sharding_config.{coin}` 的 `white_list_user_ids` 读取（ADR-0058 §8.3），这些 user 的 Account 初始化时用大容量 ring。

**Ring 满的行为**：FIFO 淘汰最老条目。淘汰掉的订单如果仍在 `RetentionDuration` 窗口内，`CancelOrder` 重入会返回 `ErrOrderNotFound` —— 这是 G2（见"已知不防御场景"）。

#### 2.3 Ring 参与 Snapshot / Restore

`AccountSnapshot` 扩展：

```protobuf
message AccountSnapshot {
  // ... 现有字段 ...
  repeated TerminatedOrderEntry recent_terminated_orders = ??;
}

message TerminatedOrderEntry {
  uint64 order_id = 1;
  uint32 final_status = 2;
  int64 terminated_at = 3;
  string client_order_id = 4;
  string symbol = 5;
}
```

Capture 时从 `acc.recentTerminated.SnapshotAll()` 序列化（复用 ADR-0057 的 ring snapshot API）。

### 3. Evictor Goroutine（Counter 侧）

#### 3.1 触发机制

VShardWorker 启动一个独立 goroutine：

```go
func (w *VShardWorker) runOrderEvictor(ctx context.Context) {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            if !w.evictHealthCheck() {
                continue  // 熔断: trade-dump 健康异常, 暂停 evict
            }
            w.evictOneRound(ctx)
        case <-ctx.Done():
            return
        }
    }
}
```

30s 周期 + 健康检查熔断（见第 5 节）。

#### 3.2 Evict 一轮的严格顺序

```go
func (w *VShardWorker) evictOneRound(ctx context.Context) {
    now := time.Now().UnixMilli()
    candidates := w.state.Orders().CandidatesForEvict(now, RetentionDuration, maxBatchSize=500)
    for _, o := range candidates {
        acc := w.state.Account(o.UserID)
        
        // Step 1: 记 ring (幂等, 重复记只是覆盖)
        acc.RememberTerminated(o)
        
        // Step 2: 同步 Publish OrderEvictedEvent
        //   Publisher.Publish 按 ADR-0060 语义: 5s 重试, 失败 panic
        evt := buildOrderEvictedEvent(o, counterSeq)
        if err := w.publisher.Publish(ctx, o.UserID, evt); err != nil {
            // panic 由 ADR-0060 Publisher 内部处理, 这里不到达
            return
        }
        
        // Step 3: 从 byID 删
        w.state.Orders().Delete(o.ID)
    }
}
```

**顺序严格要求**：ring → journal → byID delete。任何步骤 crash 都幂等可恢复：

- 崩在 Step 1 后：重启后 candidate 仍满足条件，下次 evict 重做（ring 再记一次，内容相同）
- 崩在 Step 2 后（journal 已 committed）：重启后 candidate 仍满足条件，重做 Step 2（journal 出现第二条 OrderEvicted，下游幂等 apply）
- 崩在 Step 3 后：正常完成

#### 3.3 CandidatesForEvict 实现

```go
func (s *OrderStore) CandidatesForEvict(now int64, retention time.Duration, maxBatch int) []*Order {
    s.mu.RLock()
    defer s.mu.RUnlock()
    out := make([]*Order, 0, maxBatch)
    for _, o := range s.byID {
        if o.canEvict(now, retention) {
            out = append(out, o.Clone())  // clone 避免后续迭代期间被修改
            if len(out) >= maxBatch {
                break
            }
        }
    }
    return out
}
```

注意：遍历 `s.byID` 持 RLock，时间 O(N)。N = byID size，生产期望 `RetentionDuration / per-vshard-tps` 水平（例：1h × 400 tps = 144w）—— 单次 RLock 持续几十 ms 可接受，且 evictor 每 30s 一次。

批量上限 `maxBatch = 500`，避免单轮耗时过长。

### 4. OrderEvictedEvent + 下游同步

#### 4.1 Proto

```protobuf
message OrderEvictedEvent {
  uint64 order_id        = 1;
  string user_id         = 2;
  string symbol          = 3;
  uint32 final_status    = 4;
  int64  terminated_at   = 5;
  string client_order_id = 6;
  uint64 counter_seq_id  = 7;  // 沿用 ADR-0018 counterSeq, evictor 调 seq.Execute 分配
}

// 放进 CounterJournalEvent oneof body
message CounterJournalEvent {
  oneof body {
    // ...
    OrderEvictedEvent order_evicted = 51;  // 紧邻 ADR-0060 的 checkpoint=50
  }
}
```

#### 4.2 Evictor 调用 sequencer

`evictOneRound` 内部对每个订单走 `seq.Execute(o.UserID, fn)`：

```go
_, err := w.seq.Execute(o.UserID, func(counterSeq uint64) (any, error) {
    acc.RememberTerminated(o)
    evt := buildOrderEvictedEvent(o, counterSeq)
    if err := w.publisher.Publish(ctx, o.UserID, evt); err != nil {
        return nil, err
    }
    w.state.Orders().Delete(o.ID)
    return nil, nil
})
```

**复用 per-user FIFO**：evict 和该 user 的 RPC / trade-event fn 互斥，避免 evict 撞上同 user 新订单的处理。

**不用 SubmitAsync**（ADR-0060 的非阻塞 API）：evictor 是独立 goroutine，用 Execute 同步等就行，不存在消费循环阻塞问题。

#### 4.3 Trade-Dump MySQL Pipeline（ADR-0023 投影）

trade-dump 的 MySQL pipeline 消费 OrderEvicted → 在 `orders` 表新增 `evicted_at` 字段（或独立 audit 表 `order_eviction_logs`）记录 evict 时间。

**MySQL 里 orders 表不删行**（查询历史订单仍要用），只标记 evicted。

#### 4.4 Trade-Dump Shadow Pipeline（ADR-0061 范围）

ADR-0061 落地后，shadow engine 收到 OrderEvicted → 从 shadow `OrderStore.byID` 删 + `Account.recentTerminated` 记录 → snapshot 同步缩小。

本 ADR 的 M7 定义这个协议，具体实现在 ADR-0061。

### 5. Evict 全局熔断（健康检查）

`runOrderEvictor` 在每轮扫描前检查 trade-dump 健康状态：

```go
func (w *VShardWorker) evictHealthCheck() bool {
    // 检查点 1: trade-dump snapshot 的最新时间戳
    //   来源: 从 snapshot 存储读最新 snapshot 的 metadata.created_at
    latestSnapshotAge := time.Since(w.loadLatestSnapshotMetadata())
    if latestSnapshotAge > 2 * time.Hour {
        // trade-dump 至少 2h 没产出新 snapshot, 系统性故障
        w.metrics.EvictHaltedDueToSnapshotStale.Inc()
        return false
    }
    
    // 检查点 2: 本 vshard 的 advancer watermark 是否持续推进
    if time.Since(w.lastCheckpointPublishedAt) > 5 * time.Minute {
        // advancer 卡住, 不应该 evict (下游看不到新 checkpoint, snapshot 不会更新)
        return false
    }
    return true
}
```

熔断触发时记录告警 + Prometheus 指标，evict 暂停直到恢复。

**熔断期间 Counter 内存会累积终态订单**，属于已知代价（比错 evict 导致数据丢失好）。

### 6. CancelOrder RPC 幂等路径修正

[service.go CancelOrder 路径] 逻辑改为：

```go
func (s *Service) CancelOrder(ctx context.Context, req CancelReq) (*CancelResp, error) {
    // ... 已有参数校验 ...
    
    _, err := s.seq.Execute(req.UserID, func(counterSeq uint64) (any, error) {
        acc := s.state.Account(req.UserID)
        
        // 路径 A: 订单还在 byID
        if o := s.state.Orders().Get(req.OrderID); o != nil {
            if o.Status.IsTerminal() {
                return buildCancelResp(o, alreadyTerminal=true), nil  // 幂等
            }
            return doCancel(o, ...)  // 正常 cancel
        }
        
        // 路径 B: 订单已被 evict, 查 ring
        if entry := acc.LookupTerminated(req.OrderID); entry != nil {
            return buildCancelRespFromRing(entry), nil  // 幂等: 返回终态缓存
        }
        
        // 路径 C: 订单从未存在 / 已超 ring 保留窗口
        return nil, ErrOrderNotFound
    })
    return err
}
```

**Ring 查询不加锁**：ring 自带 mu（参考 ADR-0057 实现），`LookupTerminated` 是只读操作。

Admin CancelOrder 同样走这套路径。

### 7. 兼容性与迁移

由于 OpenTrade 未上线（MEMORY breaking change 策略），proto schema 直接扩展，不写 backward compat 代码：

- `AccountSnapshot.recent_terminated_orders` 默认空 → restore 时 ring 为空（行为不变）
- `CounterJournalEvent.order_evicted` 新 oneof case → trade-dump 旧代码忽略未知 oneof 也不影响（protobuf 默认 forward-compatible）
- 已存在的终态订单（本 ADR 上线时）没有 `TerminatedAt` 字段 → restore 时 fallback 到 `UpdatedAt`（一次性迁移路径）

## 备选方案 (Alternatives Considered)

### 备选 1：按 counterSeq lag 驱动 evict，不用时间窗

**拒绝**：counterSeq 是 per-vshard 逻辑时间，不同 vshard 的 counterSeq 增长速度差异大（做市商 vshard 快 10 倍）；统一阈值不适用。时间窗是物理时间，全系统一致。

### 备选 2：时间窗 + counterSeq lag 双条件

**拒绝**：两个条件高度重叠（时间窗 1h 下，counterSeq 通常已经涨 10w-1000w 不等），增加配置复杂度但不增加防御能力。详见"理由"章节。

### 备选 3：按数量窗 per-user 保留最近 M 个终态订单

**拒绝**：数量窗无法保证时间维度的一致性（做市商 1 小时内就能产出 10w 笔订单，按数量窗 M=1000 会让"5 分钟前的订单"就被 evict，trade-dump 还没看到）。

### 备选 4：不做 evict，依赖 Counter 定期重启 + snapshot reload 来"自然"丢弃

**拒绝**：重启周期长（几天 - 几周），期间 byID 仍无限增长；且重启操作成本高，不是常态。

### 备选 5：Evict 决策放 trade-dump 侧

**拒绝**（详见上一轮讨论）：
- trade-dump 决策不会影响 Counter 内存，byID 仍增长
- trade-dump 通知 Counter 反向 event 违反 ADR-0001 单向数据流

### 备选 6：Ring buffer 容量全局固定（不区分做市商）

**拒绝**：做市商高频交易下 1024 容量会频繁 overflow，早期 evict 的订单落到 ring 外 → CancelOrder 幂等失效。ADR-0058 已经有白名单机制识别做市商，复用即可。

### 备选 7：Evict 改为 lazy（访问 byID 时顺带清理过期订单）

**拒绝**：lazy 清理无法保证"不被访问的订单"被清理 → 大量冷订单永远留着（只有活跃 user 的 Account 会被访问，冷 user 的历史订单永留）。

## 理由 (Rationale)

### 为什么选时间窗而不是双条件

核心判据：**防御边界是"trade-dump 是否看到了终态 event"**。

- trade-dump 消费 lag 稳态 <1s，异常（挂机）最多几分钟到 1 小时
- 时间窗 1h 覆盖所有正常 + 大部分异常
- 极端情况（trade-dump 死机 >1h）：counterSeq lag 也救不了（该 vshard 的 counterSeq 还在涨，只要没熔断就会触发 evict），需要**健康检查熔断**（第 5 节）

双条件：增加复杂度 0 增加防御。

### 为什么 Evict 决策必须在 Counter 而不是 trade-dump

- Counter 是账户 + 订单状态权威源（ADR-0001），evict 是状态转换之一，必须由权威源发起
- trade-dump 决策不影响 Counter 内存，无法控制 byID 增长
- 反向 event（trade-dump → Counter）违反单向数据流

### 为什么 Ring Buffer 参与 Snapshot

Ring 是 CancelOrder 幂等的兜底，**必须持久化**。如果 ring 只在内存里，Counter 重启后 ring 丢失 → 已 evict 订单的 CancelOrder 重入直接返回 ErrOrderNotFound，破坏幂等。

### 为什么不用 MySQL 做幂等兜底

- MySQL 读延迟 1-5ms，CancelOrder 热路径不可接受
- 跨服务依赖（Counter 去查 trade-dump 的 MySQL）违反 ADR-0003 异步解耦原则

## 影响 (Consequences)

### 正面

- 解除 ADR-0060 "订单永不删"假设，生产上线后 byID 不再无限增长
- snapshot size 稳态可控（仅含活动订单 + 1h 内终态）
- ADR-0061 shadow engine 的内存画像也得到控制
- CancelOrder 幂等从"依赖 byID 永不删"改为"显式 ring"，语义更清晰

### 负面 / 代价

- 增加代码复杂度（ringBuffer + evictor goroutine + 熔断检查）
- Snapshot proto 扩展（必须配 restore 兼容逻辑）
- Ring buffer 内存占用：普通 1024 entry × ~80 字节 = 80KB / user × 4w user / vshard = **3.2 GB / vshard 的 ring 开销**，加做市商（16384 × 80 = 1.3 MB / user）
- CancelOrder 幂等路径多一次 ring 查询（内存操作，μs 级，可接受）

### 中性

- Evictor goroutine 是独立 loop，不干扰消费主流程（ADR-0060 已解除阻塞）
- 健康检查熔断引入运维侧的新告警项（trade-dump snapshot stale）

## 已知不防御场景 (Non-Goals / Known Gaps)

### G1: Trade-dump 死机超过熔断阈值 + Counter crash

熔断阈值默认 2h。若 trade-dump 死机 > 2h：

- evict 暂停 → byID 累积 → 仍能工作但内存上涨
- 若此时 Counter 也 crash + snapshot 过旧 → recovery 的 te_watermark 落后很多 → 要靠 catch-up journal 补齐（journal retention 必须够长，见 ADR-0060 G4）

**不防御原因**：双故障场景下无法同时保证可用性 + 一致性，优先保一致性（停 evict）。

**外部处置**：trade-dump 的 SLA + 告警体系。

### G2: Ring Buffer 溢出（做市商极端情况）

若某做市商 1h 内产生超过 16384 笔终态订单：

- Ring FIFO 淘汰最老 entry
- 被淘汰订单如果仍在 `RetentionDuration` 内（不该发生，因为时间窗同样是 1h）—— 理论上不会
- 但如果 `RetentionDuration` 和 ring 容量不匹配（例：容量 16384 / 订单生成速率 > 16384/hour）—— `CancelOrder` 重入会返回 `ErrOrderNotFound`

**不防御原因**：ring 容量和 retention 的匹配是运维配置问题，无法 100% 防错配。

**缓解**：
- 监控指标 `counter_ring_overflow_total{user_id}`，告警阈值 > 0
- Runbook：触发告警时调大该做市商的 ring 容量

### G3: `TerminatedAt` 字段漏设

如果某个路径改了 `UpdateStatus` 但忘了设 `TerminatedAt`，订单永远 evict 不掉。

**不防御原因**：代码规范问题，通过 code review + 单测覆盖保证。

**缓解**：测试断言所有终态转换路径都设 `TerminatedAt`；可选加 assertion 在 `UpdateStatus` 里强制 terminal 时必须传 `now` 参数。

### G4: Trade-dump MySQL Pipeline 崩溃但 Counter 继续 evict

Evictor 只 Publish OrderEvicted 到 journal，不等 trade-dump MySQL 消费。如果 MySQL pipeline 挂了，MySQL `orders` 表的 `evicted_at` 不会更新，但 Counter 已经删了 byID。

**不防御原因**：journal 是权威，MySQL 是投影，投影延迟是投影自己的问题。

**影响可接受**：MySQL pipeline 恢复后 catch-up journal 会补齐。

### G5: 历史订单无 `TerminatedAt` 字段

ADR 上线前已存在的终态订单没有 `TerminatedAt` 字段。

**处置**：restore 时 fallback 到 `UpdatedAt`（一次性迁移路径），上线后所有新订单都有。

**不防御原因**：未上线，不是生产问题；上线后无此问题。

### G6: Evictor 自身卡住

若 `runOrderEvictor` goroutine 因 bug 卡住（死循环 / 死锁），byID 无限增长。

**不防御原因**：应该通过测试和 code review 保证。

**监控指标**：`counter_evictor_last_round_ts{vshard}`，超过 5 × 扫描周期（150s）未更新即告警。

### G7: Ring 内存占用压力

估算 3.2 GB / vshard 的 ring 开销（普通用户部分）。若单机承载多 vshard（ADR-0058 典型 13-50 vshard / node），单机 ring 开销 40-160 GB。

**不防御原因**：这是设计的内在代价，只能通过参数调优缓解（降低 ring 容量 / 降低 RetentionDuration）。

**监控指标**：`counter_ring_total_memory_bytes{vshard}`，超过配置的软上限告警。

### G8: Evict 和 snapshotMu（ADR-0060 M7）的锁顺序

Evict 流程调 `state.Orders().Delete(id)` 是写操作，必须在 snapshotMu 的 R lock 下执行（和其他 apply 路径一致）。如果漏加 lock，snapshot Capture 期间 evict 并发会破坏 snapshot 一致性。

**不防御原因**：和 ADR-0060 G7 同类问题，靠 code review。

**缓解**：M4 实现时把 evict 流程纳入 snapshotMu R lock 覆盖。

## 实施里程碑 (Milestones)

按依赖顺序：

- **M1** ✅ `Account.recentTerminated*` 字段 + `RememberTerminated` / `LookupTerminated` + ring snapshot round-trip 配套。FIFO / capacity / 并发单测
- **M2** ✅ `AccountSnapshot.RecentTerminatedOrders` proto 扩展 + Capture / Restore（snapshot pkg 在 M1 后已移至 `counter/snapshot`）
- **M3** ✅ `Order.TerminatedAt` 字段 + `UpdateStatus` 在所有终态转换路径 stamp；`OrderStore.CandidatesForEvict` + `Delete`；4 种终态路径单测
- **M4** ✅ `OrderEvictedEvent` proto（tag 51）+ `journal.BuildOrderEvictedEvent`；`VShardWorker.runOrderEvictor` goroutine + `evictOneRound` + `evictOne` 在 sequencer 下的 ring→journal→delete 三步；集成测试：evict 触发 → journal event → byID 不含该订单
- **M5** ✅ `CancelOrder` 三路径（byID / ring / not-found），`cancelResultFromRing` 辅助；所有路径的幂等断言单测（`TestCancelOrder_EvictedOrder_*`）；ADR-0060 M5 gate 解除
- **M6** ✅ `evictHealthCheck` — Counter 侧只剩 checkpoint-stale 分支（`lastCheckpointAtMs` vs `EvictCheckpointStaleAfter`）；熔断日志 + `counter_evict_halted_total{reason=checkpoint_stale}`。**snapshot-stale 分支已下放到 trade-dump**（ADR-0061 Phase B 后 Counter 不再本地持有 snapshot 时间戳，由 `trade_dump_snapshot_age_seconds` 承担）
- **M7** ✅ shadow engine apply `OrderEvicted` 协议在 `counter/engine.ApplyCounterJournalEvent` 统一实现（ADR-0061 M1），Counter catch-up + trade-dump shadow 都走这个入口
- **M8** ✅ `counter_evict_processed_total{vshard, result}` + `counter_evict_halted_total{vshard, reason}` 接到 Prometheus；runbook-counter.md §7 提供排障路径
- **M9** ⏳ 未做 — 压测（1h 持续成交 + evict，验证 byID 稳态 + CancelOrder 幂等不破）

## 实施约束 (Implementation Notes)

- `CandidatesForEvict` 遍历 RLock 时长 O(N)，N = byID size。生产初期 N < 200w，RLock <100ms 可接受；若 N 上升需要分片扫描
- `evictOneRound` 单轮最多处理 500 个订单（`maxBatch`），避免长时间独占 per-user sequencer
- Ring 内的 `terminatedOrderEntry` 字段严格控制大小（不存 decimal 金额 / 成交明细）
- Evict 路径的 `seq.Execute` 必须和其他 apply 路径一样承担 snapshotMu R lock（ADR-0060 M7）
- 做市商白名单来源复用 ADR-0058 `trading_sharding_config.{coin}` 的 etcd 配置，不引入新配置源
- `CandidatesForEvict` 返回前调 `o.Clone()` 避免迭代期间 order 被修改导致的数据竞争
- OrderEvicted event 的 `counter_seq_id` 由 evictor 线程内的 `seq.Execute` 分配，保证和其他 journal event 的 counterSeq 语义一致

## 参考 (References)

### 项目内

- ADR-0015: client-order-id 索引（activeByCOID 设计）
- ADR-0018: counterSeq 机制
- ADR-0048: snapshot + offset 原子绑定
- ADR-0054: per-symbol order slots（activeLimits 设计）
- ADR-0057: Asset service + Transfer Saga 的 `recentTransferIDs` ring 实现
- ADR-0058: Counter 虚拟分片 + white_list_user_ids 机制
- ADR-0060: Counter 消费异步化（本 ADR 的 M1 + M5 解除 0060 recovery gate）
- ADR-0061（后续）: trade-dump snapshot pipeline（本 ADR M7 定义 shadow apply 协议）

### 讨论

- 2026-04-22: 用户与 Claude 关于"订单终态后 snapshot 里能否加载出来 / evict 应该在哪做"的设计讨论（会话记录）

## 后续工作建议

- [ ] 和 ADR-0057 的 ring 实现合并到 `pkg/ring` 通用包
- [ ] ADR-0061 M-shadow-apply: shadow engine 消费 OrderEvicted 的具体实现
- [ ] 做市商白名单 ring 容量独立 ADR（如果未来需要动态调整）
- [ ] 压测后评估 Retention 1h 是否合适，可能需要调整（但要和时间窗重要性评估一起做）
