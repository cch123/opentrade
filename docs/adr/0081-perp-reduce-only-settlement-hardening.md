# ADR-0081: perp reduce_only 结算时硬约束

- 状态: **Proposed**（2026-05-31 起草；从 `docs/roadmap.md` 的 perp 产品化缺口 #8 提升为独立 ADR）
- 日期: 2026-05-31
- 决策者: xargin, Codex
- 相关 ADR: 0068（perp reduce_only MVP）、0077（position_idx）、0078（TP/SL 与 close_on_trigger）

## 范围声明（先读这一段）

当前 reduce_only 主要在下单准入时检查是否存在反向仓位，但成交回流时如果仓位已经被别的订单改变，理论上仍可能出现 reduce_only fill 反向开仓。本 ADR 决定把 reduce_only 从"准入检查"升级为**结算不变量**。

## 背景 (Context)

reduce_only 的产品承诺是：订单只会减少指定仓位，绝不会增加或反向开仓。仅靠下单时检查不够，因为：

- 用户可以同时挂多张 reduce-only。
- 仓位可能被市价单、强平、ADL、TP/SL 先减掉。
- Match 不知道用户仓位，只按订单剩余量撮合。

## 决策 (Decision)

### 1. 引入 reduce-only close capacity reservation

perp-counter 为每个 position leg 维护可关闭数量预留：

```text
ReduceOnlyCapacity {
  user_id
  symbol
  position_idx
  position_side
  position_size
  reserved_close_qty_by_order
}
```

下 reduce-only 单时：

```text
available_close_qty = current_position_size - sum(active_reduce_only_reserved)
if request_qty > available_close_qty:
    reject reduce_only_exceeds_close_capacity
reserve request_qty for order_id
dispatch order with qty=request_qty
```

这保证同一时刻所有 active reduce-only 的总量不超过当前仓位。P1 默认不静默改用户数量，和 [ADR-0080](./0080-perp-admission-risk-price-protection.md) 的“默认 REJECT，不 CLAMP 用户指令”保持一致。`close_all` 这类系统生成指令可以自己用当前 capacity 构造精确 qty，但用户显式下单请求超出 capacity 时必须拒绝。

### 2. 成交结算必须消费 close capacity

settlement 流程：

```text
reduce_only fill enters user sequencer
    |
    v
apply_qty = min(fill_qty, current_leg_size, order_unconsumed_reservation)
excess    = fill_qty - apply_qty
    |
    +-- apply_qty > 0:
    |      close position by apply_qty
    |      decrement order reservation by apply_qty
    |
    +-- excess > 0:
           emit REDUCE_ONLY_INVARIANT_BREACH
           halt/alert/manual repair path
```

正常情况下，Match 不可能成交超过订单剩余量，因此也不会超过 reservation。reservation 是服务端最后一道防线和 replay 锚点；当前 leg size 是最终防线，任何超过当前仓位的 fill 都不能静默反向开仓。

### 3. 仓位被其他路径减少后，主动撤销或缩减 reduce-only 单

任何会减少 position size 的事件后，在用户 sequencer 内执行：

```text
new_capacity = current_position_size
if sum(active_reduce_only_reserved) > new_capacity:
    shrink/cancel lowest-priority reduce-only orders
    dispatch cancel/amend to Match
    emit ReduceOnlyCapacityEvent
```

优先级 P1：

1. 保留更早创建的订单。
2. 后创建的订单先缩减 / 撤销。
3. close_on_trigger 和 liquidation 系统单优先级可由产品配置覆盖。

### 4. 竞态和异常的处理

若 cancel 已发出但 Match 已经撮合旧订单，trade-event 仍可能回流。处理规则统一用 §2 的 `apply_qty` 公式，不再按 reservation 状态分三套语义：

- `apply_qty = min(fill_qty, current_leg_size, order_unconsumed_reservation)`。
- `excess > 0` 进入 `REDUCE_ONLY_INVARIANT_BREACH`：不允许静默反向开仓，必须停机告警 / 进入人工修复队列。MVP 不自动 bust trade。
- 若需要缩减 reservation，旧 reservation 的未消费上限必须在 order terminal 前保留为 in-flight guard，不能物理删除到无法解释旧 fill。

不自动 bust 的原因：成交涉及对手方，单边撤销会破坏账务。正确做法是预留和撤单机制避免发生，异常时显式暴露。

### 5. hedge mode 下按 position_idx 隔离

reduce-only reservation key 必须包含 `position_idx`。long leg 的 close capacity 不能被 short leg 订单消耗。

## 备选方案 (Alternatives Considered)

### A. 只在 settlement clamp，超过部分丢弃

会让对手方成交和用户侧结算不一致。否决。

### B. 让 Match 订阅仓位并实时拒绝

破坏 Match 无账户状态设计，也会引入 stale position。否决。

### C. 下单时允许超额 reduce-only，成交后按当前仓位处理

这是当前风险来源。否决。

## 影响 (Consequences)

### 正面

- reduce_only 从产品承诺变成可验证不变量。
- 多张 reduce-only 不会合计超过仓位。
- TP/SL、close_on_trigger、close_all 都能复用 close capacity。

### 负面 / 代价

- perp-counter 需要维护 active reduce-only reservation 副表。
- 仓位变化后需要向 Match 发缩减 / 撤单指令。
- 极端竞态仍需要 invariant breach 告警和人工流程。

## 实施约束 (Implementation Notes)

- reservation 必须进 snapshot，重启后不能丢。
- order lifecycle terminal 时释放剩余 close reservation。
- partial fill 按 `apply_qty` 消费 reservation，`excess` 触发 invariant breach。
- 单测覆盖：多 reduce-only 超额拒绝、用户请求超 capacity 拒绝不 clamp、仓位先被其他订单减少后的自动撤单、cancel/fill 竞态、excess breach、hedge position_idx 隔离、snapshot roundtrip。

## 参考 (References)

- [ADR-0068: USDT 本位线性永续合约](./0068-usdt-linear-perp.md)
- [ADR-0077: perp 持仓模式](./0077-perp-position-mode-hedge-both-side.md)
