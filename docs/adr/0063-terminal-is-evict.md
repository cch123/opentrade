# ADR-0063: 终态即 Evict —— 同步删除 + 事件合并

- 状态: Accepted
- 日期: 2026-04-22
- 决策者: xargin, Claude
- 取代: ADR-0062 的 Evict 框架（retention 窗口 / Evictor goroutine / OrderEvictedEvent / 健康熔断）
- 相关 ADR: 0001（Kafka SoT）、0015（client-order-id 索引）、0018（per-user sequencer + counterSeq）、0023（trade-dump MySQL pipeline）、0054（per-symbol order slots）、0060（Counter 消费异步化 + TECheckpoint）、0061（trade-dump snapshot pipeline + shadow engine）、0062（订单终态 Evict，本 ADR 取代其 Evict 部分；Ring 部分已先行回退）

## 背景 (Context)

ADR-0062 建立了"终态订单 1h retention + evictor goroutine 扫描 + `OrderEvictedEvent` 下游通知"的 Evict 框架，目的是防止 `OrderStore.byID` 无限增长（4 亿订单 / vshard / 年）。该 ADR 已落地 M1–M6 + M8。

其中 Ring Buffer（给 `CancelOrder` 在 evict 后兜底幂等）在 2026-04-22 前 PR 中已被回退（commit `32044a7`）。剩余的 Evict 主体仍保留 1h retention + 独立 goroutine + 专用事件的设计。

### 剩余设计的冗余处

回退 Ring 后重审 Evict 主体，发现原有设计存在过度复杂：

1. **Retention 1h 的真实必要性低**：
   - 原本 1h 是为给 `CancelOrder` 重入留"订单仍在 byID"的窗口（1h 内返回 `"order already terminal"`）。Ring 砍掉后，客户端 1h 外拿到新错误、1h 内拿到"已完结"—— 但这两种响应对客户端都是"订单不可 cancel"，体验差异极小。
   - 另一个理由是"给 trade-dump 消费 lag 留窗口"。但 `counter-journal` 是有序 partition（per-vshard），`OrderStatusEvent(new_status=terminal)` 本身就在 `OrderEvictedEvent` 之前发出，trade-dump 按顺序消费自然先标记终态再删 shadow byID —— retention 的唯一收益是"Counter snapshot 不早于 trade-dump 消费终态事件"这一点并不成立（journal 有序 + shadow engine 的 catch-up 已经保证最终一致）。

2. **Evictor goroutine 的复杂度无对应收益**：
   - 30s 周期 + 批量 500 条 + `CandidatesForEvict` O(N) 遍历 + `evictHealthCheck` 熔断 + 独立 metrics —— 所有这些机制都是为了"延迟删除"这个本质可有可无的能力服务的。
   - 熔断的原意是：如果 trade-dump snapshot stale，Counter 暂停 evict 以免推进过快。但推进过快本身不是问题（journal 有序 + 下游幂等 apply），熔断在简化模型下没有防御对象。

3. **`OrderEvictedEvent` 与 `OrderStatusEvent` 语义重复**：
   - 调研 `trade-dump/internal/writer/projection.go` 发现：**MySQL 投影 pipeline 根本没消费 `OrderEvictedEvent`**，它只靠 `OrderStatusEvent` 改 `orders` 表的 status / filled_qty。
   - `OrderEvictedEvent` 的唯一真实消费者是 **shadow engine**（`applyOrderEvictedEvent` 从 shadow byID 删），而 shadow engine 也同时消费 `OrderStatusEvent` —— 两个事件对 shadow engine 来说是"先改状态、后删实体"，可以合并成"改状态到终态时就删"。
   - `OrderEvictedEvent` 的字段（symbol / client_order_id / terminated_at）对 shadow engine 来说已经在之前 `OrderPlacedEvent` 投影里拿到，没必要在 evict 事件里重复。

### 核心判断

**byID 只保留活跃订单是正确的设计终点**；retention 窗口的所有原始收益（客户端幂等 / trade-dump 同步窗口）在当前架构下都不成立或收益极低。应当把"订单进入终态"和"从 byID 删除"折叠成同一个原子动作。

## 决策 (Decision)

### 1. Terminal 即 Delete —— apply 侧语义变化

`applyOrderStatusEvent` 在 `new_status.IsTerminal()` 时，除了原有的 `UpdateStatus` 调用，**额外同步调用 `state.Orders().Delete(order_id)`**。

```go
func applyOrderStatusEvent(state *ShardState, evt *eventpb.OrderStatusEvent) error {
    // ... 原有 filled_qty / status 更新逻辑 ...
    newStatus := OrderStatusFromProto(evt.NewStatus)
    if newStatus != o.Status {
        if _, err := state.Orders().UpdateStatus(evt.OrderId, newStatus, 0); err != nil && !errors.Is(err, ErrOrderNotFound) {
            return fmt.Errorf("order status: %w", err)
        }
    }
    if newStatus.IsTerminal() {
        if err := state.Orders().Delete(evt.OrderId); err != nil && !errors.Is(err, ErrOrderNotFound) {
            return fmt.Errorf("terminal delete: %w", err)
        }
    }
    return nil
}
```

**幂等性**：
- `Delete` 是 `ErrOrderNotFound`-safe（已有）
- 重放 `OrderStatusEvent(Filled)`：第二次 `Get(byID)` 返回 nil，`UpdateStatus` 的 `ErrOrderNotFound` 分支已是 safe，新增的 `Delete` 也 safe
- Counter catch-up + shadow engine 都走这个统一入口（`ApplyCounterJournalEvent`），两侧语义一致

### 2. 发送侧 —— Counter 终态发送路径同步 Delete

Counter 主路径发 `OrderStatusEvent(terminal)` 的地方（`service/trade.go` 的 Rejected / Canceled / Expired / settlement.go 的 Filled）在同一 sequencer 窗口里：

```go
// 原: UpdateStatus + emitStatus
// 新: UpdateStatus + emitStatus + (terminal ? Orders().Delete(o.ID) : nothing)
```

把 Delete 放在 emitStatus（Publish 到 journal）**之后**，保证崩溃语义：
- Publish 失败：Delete 不执行；订单仍在 byID，下次相同路径重入会重发事件（journal 幂等 apply 覆盖）
- Publish 成功 + Delete 失败（不太可能，纯内存操作）：journal 已有事件，重启 catch-up 时 apply 会补删（走新 `applyOrderStatusEvent` 的 terminal 分支）

### 3. 移除 `OrderEvictedEvent`

整体删除：
- `api/event/counter_journal.proto` 里的 `OrderEvictedEvent` message + oneof case (field tag 51 废弃，**不复用**)
- `counter/internal/journal/convert.go` 的 `OrderEvictedEventInput` / `BuildOrderEvictedEvent`
- `counter/engine/journal_apply.go` 的 `applyOrderEvictedEvent` + dispatcher case
- 所有调用点（worker.go 的 evictor 路径、相关测试）

### 4. 移除 Evictor 基础设施

从 `counter/internal/worker/worker.go` 删除：
- `runOrderEvictor` goroutine 及其启动
- `evictOne` / `evictOneRound` 方法
- `evictHealthCheck` 熔断
- `CandidatesForEvict` 调用（对应 `counter/engine/orders.go` 的 `CandidatesForEvict` 方法一并删）
- `Config.OrderEvictInterval` / `OrderEvictRetention` / `OrderEvictMaxBatch` / `EvictCheckpointStaleAfter`
- 对应默认常量 `defaultOrderEvict*`
- `lastCheckpointAtMs` 字段（仅服务于 evict 熔断）

### 5. 移除 `Order.TerminatedAt`

该字段的唯一用途是 `CandidatesForEvict` 的 retention 算时。新模型下 terminal 状态就是 Delete 信号，时间戳无用武之地。删除：
- `counter/engine/order.go` 的 `Order.TerminatedAt` 字段
- `counter/engine/orders.go` `UpdateStatus` 里 stamp `TerminatedAt` 的逻辑
- `counter/snapshot/snapshot.go` `OrderSnapshot.TerminatedAt`
- `api/snapshot/counter.proto` `CounterOrder.terminated_at` (field tag 19 废弃，**不复用**)
- 相关测试断言

### 6. 移除 Evict Metrics

从 `counter/internal/metrics` 删：
- `evictor_last_round_ts` / `ring_overflow_total` / `ring_total_memory_bytes` / `evict_halted_due_to_snapshot_stale` / `evict_processed_total`

## 备选方案 (Alternatives Considered)

### 备选 1：扩展 `OrderStatusEvent` 加 `evicted bool` 字段

**拒绝**：`new_status.IsTerminal()` 已经是"evicted"的充要信息，再加布尔值是信息冗余；非终态转换时该字段永远是 false，proto 上的空间和代码里的 if 都是噪声。

### 备选 2：保留 `OrderEvictedEvent` 作为显式删除通知，但让 Counter 终态时同步发送（仍合并两事件为"同一 sequencer 窗口下两条事件"）

**拒绝**：每笔终态订单多发一条 journal record，journal 体积 +50%（每笔终态伴随 2 条事件而非 1 条），下游 apply 要处理两次（order_status 改状态、order_evicted 删实体）而不是一次。没有收益。

### 备选 3：保留 retention 但改为更短（如 5 分钟）

**拒绝**：retention 本身就是 CancelOrder 幂等窗口的遗产，Ring 砍掉之后这窗口的唯一残余收益是"订单终态后 5 分钟内 CancelOrder 返回 'already terminal' 而不是 'not exist'"—— 两种响应对客户端都是"不可 cancel"，差别不值复杂度。

### 备选 4：继续走 ADR-0062 原路（保留 1h retention + evictor goroutine），只删 Ring

**已拒绝**：2026-04-22 本次讨论的起因就是发现保留原 Evict 框架的复杂度与其收益不成比例（详见"背景"章节）。

## 理由 (Rationale)

### 为什么 Delete 要在 `emitStatus` 之后而不是之前

Counter 向 `counter-journal` 发事件是权威动作（ADR-0001 Kafka SoT）。Delete 是本地内存操作。顺序必须是：

1. `UpdateStatus` 改内存状态（PendingCancel → Canceled 等）
2. `emitStatus` Publish OrderStatusEvent（journal commit，权威定稿）
3. `Orders().Delete(id)` 删 byID（本地清理）

如果反过来先 Delete 再 Publish，崩溃在 Publish 前会导致："内存已无此订单 + journal 无终态事件" —— 重启从 snapshot restore 会发现订单不在 byID，但 journal 上也没有终态 / evict 记录，状态不一致。

### 为什么 shadow engine 不需要额外改动

shadow engine 通过 `counter/engine.ApplyCounterJournalEvent`（ADR-0061 M1）这个统一入口消费所有事件。`applyOrderStatusEvent` 的新逻辑对 Counter catch-up 和 trade-dump shadow 同时生效，两侧不用分叉。shadow engine 原本的 `applyOrderEvictedEvent` 删除后，dispatcher case 减少一项，不影响剩余事件。

### 为什么不留 `Order.TerminatedAt` 做审计

审计数据在 trade-dump MySQL `orders` 表里有 `updated_at` / 终态 row，长期保留，是审计的权威源。Counter 内存里的 `TerminatedAt` 只是 evict 调度辅助，没有业务消费者。

## 影响 (Consequences)

### 正面

- **byID 内存/snapshot 稳态**：只含活跃订单（估算 10w/vshard × 400B = 40 MB），不再随时间累积"1h 内终态订单"（节省约 5-20MB 随做市商活动）。
- **代码净删 ~800 行**：evictor goroutine / CandidatesForEvict / TerminatedAt stamp / OrderEvictedEvent proto + convert + apply + 测试 + metrics 全部消失。
- **事件数减少**：每笔终态订单少发一条 journal record，trade-dump 消费量下降。
- **Apply 路径更简单**：dispatcher 少一个 case；终态处理逻辑集中在 `applyOrderStatusEvent` 一处。
- **无需熔断**：Counter 发布节奏由主事件路径决定，没有"独立调度推进过快"问题。

### 负面 / 代价

- **客户端 CancelOrder 对终态订单立即返回 "not exist or already finished"**，不再有 1h 窗口返回 "already terminal"。但：
  - 客户端 RPC 重试窗口本来就是秒到分钟级，不会踩到这个差异
  - 历史订单状态查询走 GetOrder → trade-dump MySQL
- **trade-dump 消费 lag 期间**客户端 GetOrder 可能短暂看到陈旧活跃状态 —— 但这个窗口从"1h"缩到"lag 秒数"，实际体验改善而非恶化。
- **Proto field tag 19 (`CounterOrder.terminated_at`) 和 tag 51 (`CounterJournalEvent.order_evicted`) 永久废弃**，后续扩展跳过这两个 tag。未上线不是问题。

### 中性

- `Order.TerminatedAt` 删除后，旧 snapshot 里的该字段被忽略（解码器会跳过未知字段）。未上线不存在遗留 snapshot。

## 已知不防御场景 (Non-Goals / Known Gaps)

### G1: 崩溃在 Publish 成功后 + Delete 之前

恢复时：
- snapshot 仍含该订单（terminal 状态，未 Delete）
- journal 上有终态 `OrderStatusEvent`，catch-up replay 时走新的 `applyOrderStatusEvent` terminal 分支 → Delete
- 结果：byID 最终不含订单，一致

**结论**：自然幂等，无需特殊处理。

### G2: 事件重放（shadow engine + Counter catch-up）

`Delete` 本身是 `ErrOrderNotFound`-safe，`UpdateStatus` 也 `ErrOrderNotFound`-safe（`journal_apply.go:300`），重放不破坏状态。

### G3: 并发 CancelOrder + Match 结算到 Filled

per-user sequencer 保证单用户内序列化。极端情况下 cancel 请求在 Filled 已 evict 之后到达 → 走"byID miss → not exist or already finished"路径，符合预期（已被 ADR-0062 回退 ADR 明确接受）。

### G4: Snapshot Capture 期间并发终态 transition

ADR-0060 M7 的 `SnapshotMu` RWMutex 保证 Apply / Capture 互斥。Terminal → Delete 在同一 sequencer 窗口里，两步都在 snapshotMu 的 R lock 下，Capture（W lock）不会看到"UpdateStatus 已改但 Delete 未发生"的中间态。

## 迁移 (Migration)

未上线，breaking change 无顾虑（MEMORY）：

- Proto schema 直接删除字段，field tag 19/51 永久废弃
- 不写 backward compat 代码
- 旧 snapshot（含 `terminated_at` / 非活跃订单）在 restore 时：
  - `CounterOrder.terminated_at` 解码被忽略（protobuf forward-compatible，未知字段跳过）
  - 非活跃订单仍会被 restore 到 byID（正常），接下来第一次 apply 该订单的终态事件时就会被 Delete
  - 现实情况：本地 dev 环境的 snapshot 直接清掉重建，无遗留

## 实施 (Implementation Notes)

本 ADR 作为一个 PR 一次落地，无 milestone 分期（改动面虽大但自洽，分期会留中间态技术债）。

主要改动点：

| 模块 | 改动 |
|---|---|
| `api/event/counter_journal.proto` | 删 `OrderEvictedEvent` + oneof case |
| `api/snapshot/counter.proto` | 删 `CounterOrder.terminated_at` |
| `counter/engine/journal_apply.go` | `applyOrderStatusEvent` terminal → Delete；删 `applyOrderEvictedEvent` |
| `counter/engine/orders.go` | 删 `CandidatesForEvict` + `UpdateStatus` 里 `TerminatedAt` stamp |
| `counter/engine/order.go` + `types.go` | 删 `Order.TerminatedAt` 字段 |
| `counter/internal/service/trade.go` | `emitStatus` 后终态时 `Delete` |
| `counter/internal/service/order.go` | CancelOrder 非终态路径不变 |
| `counter/internal/journal/convert.go` | 删 `OrderEvictedEventInput` + `BuildOrderEvictedEvent` |
| `counter/internal/worker/worker.go` | 删 evictor goroutine / Config 字段 / 常量 / `lastCheckpointAtMs` |
| `counter/internal/metrics` | 删 evict 指标 |
| `counter/snapshot/snapshot.go` | 删 `OrderSnapshot.TerminatedAt` + Capture / Restore |
| 测试 | 删 `order_eviction_test.go` / `eviction_test.go` / OrderEvicted apply 测试；新增 terminal → Delete 覆盖 |
| `docs/adr/0062-*.md` | 状态改为 Superseded（by ADR-0063） |
| `docs/adr/0061-*.md` | shadow apply 描述删 OrderEvicted |
| `docs/runbook-counter.md` | 删 evict 告警段 |

## 参考 (References)

- ADR-0062: 原 Evict + Ring 框架（本 ADR 取代其 Evict 部分）
- commit `32044a7`: 回退 Ring Buffer 部分
- trade-dump 消费调研：`trade-dump/internal/writer/projection.go` 仅消费 OrderStatusEvent，不消费 OrderEvictedEvent
