# ADR-0078: perp 订单与持仓产品 API

- 状态: **Proposed**（2026-05-31 起草；从 `docs/roadmap.md` 的 perp 产品化缺口 #5 提升为独立 ADR）
- 日期: 2026-05-31
- 决策者: xargin, Codex
- 相关 ADR: 0014（改单为撤单 + 新建）、0020（订单状态机）、0035（服务端 MARKET）、0068（perp MVP）、0074（保证金模式）、0077（position_idx）

## 范围声明（先读这一段）

本 ADR 定义 perp 产品层 API 的扩展边界：

- amend order
- batch create/cancel
- cancel all / by coin
- pre-create 试算
- close all position
- block trade
- force add/sub position
- swap/transfer position
- close_on_trigger
- TP/SL/TrailingStop 与 perp 持仓语义集成

本 ADR 不要求一次性实现全部 API，但决定它们如何映射到已有 Counter/Match/trigger 架构。

## 背景 (Context)

当前 perp REST 只有最小面：下单、撤单、查单、查仓、查保证金。成熟合约产品需要大量持仓级和批量操作。如果每个 API 都绕过现有订单状态机直接改仓，会破坏 ADR-0014/0020 的可复盘性。

## 决策 (Decision)

### 1. 产品 API 分成三类

```text
+------------------+-----------------------------+-------------------------+
| 类别             | 示例                        | 执行权威                |
+------------------+-----------------------------+-------------------------+
| order commands   | amend/batch/cancel-all      | perp-counter + Match    |
| position commands| close-all/TP-SL/transfer    | perp-counter sequencer  |
| admin commands   | force add/sub/block trade   | admin-gateway + audit   |
+------------------+-----------------------------+-------------------------+
```

所有会影响用户仓位或保证金的命令必须进入用户 sequencer。所有会进入 orderbook 的命令必须通过 Match，不允许产品 API 直接改 Match book。

### 2. amend order 仍实现为 cancel + new

沿用 ADR-0014：

```text
AmendOrder
    |
    +-- validate ownership/current order
    +-- choose amend mode:
        conservative: wait old order terminal before dispatching new
        fast-path: reserve worst-case old + new exposure before dispatching new
    +-- send cancel old and track amend_id
    +-- dispatch new only when selected mode's safety condition holds
    +-- link amend_id for history
```

不做 in-place 修改。原因是 Match orderbook 的位置、time priority、reservation、journal 都能保持简单。

cancel 是异步的，旧单在 cancel terminal 前仍可能成交。默认实现必须采用 conservative 模式：等旧单 terminal（filled/canceled/expired/rejected）后再派发新单。若后续为了低延迟实现 fast-path，必须按旧单剩余风险 + 新单风险的最坏情况预占，并把 amend 状态写入 journal；不能只预留差额后立即让新旧两张单同时 live。

### 3. batch API 返回 per-item result，不默认事务化

`BatchCreateOrders` / `BatchCancelOrders`：

- 同一个 request 有一个 `batch_id` 幂等键。
- 每个子项有自己的 `client_order_id` / `op_id`。
- 默认语义是 best-effort per-item result；部分成功必须可复盘。
- 若未来需要 all-or-nothing，另加 `atomic=true`，只允许同 user + 同 symbol + 小批量，并在 sequencer 内预校验全部 reservation。

### 4. pre-create 是试算，不是锁定

`PreCreateOrder` 返回：

```text
required_initial_margin
estimated_fee
effective_leverage
effective_risk_tier
price_protection_result
max_open_qty
reject_reason_if_any
snapshot_version
```

它不产生 reservation，也不保证随后真实下单一定成功。真实下单仍重新校验 mark、config version、wallet、active orders。

### 5. close all position 生成 reduce-only 子单

```text
CloseAllPositions(user, optional symbol/settle_asset)
    |
    +-- enter user sequencer
    +-- reject new open-increasing orders under close_all_id
    +-- cancel increasing orders and keep in-flight guard
    +-- for each selected position leg:
          create reduce_only protected market order
    +-- track close_all_id
```

不能直接按 mark 结算关仓；必须通过 Match 或清晰的 admin/system settlement 路径，避免绕过市场成交和对手方账务。close-all 不能假设 cancel 后盘口已经静默，必须复用 [ADR-0081](./0081-perp-reduce-only-settlement-hardening.md) 的 close capacity / in-flight guard：旧增仓单 fill 若在 close-all 后回流，要么按仍有效的订单风险正常结算并触发后续 close capacity 重算，要么超过 guard 时进入 invariant breach，不能静默破坏 close-all 的目标状态。

### 6. TP/SL/TrailingStop 归一到 trigger 服务，但绑定 position

perp 的 TP/SL 不是普通触发单的别名。必须绑定：

- `symbol`
- `position_idx`
- `side_to_close`
- `reduce_only=true`
- `close_on_trigger` 可选
- `position_version` 可选保护

触发时若仓位不存在或方向不匹配，则 trigger 终态为 `EXPIRED_POSITION_GONE`，不反向开仓。

### 7. admin force add/sub position 必须独立审计

`ForceAddPosition` / `ForceSubPosition` 只允许 admin-gateway 调用：

- 必须带 reason、ticket、operator。
- 只在用户 sequencer 内 mutation。
- 必须发 `PerpAdminPositionAdjustmentEvent`。
- 不得伪装成普通成交，也不得写入 trade 表为市场成交。

### 8. block trade 与普通撮合隔离

block trade 是大宗协议成交，不进入公开 orderbook，但它是**双边成交**，不能当成单用户 internal settlement：

```text
BlockTradeRequest
    -> admin/rfq approval
    -> block_trade_coordinator creates two version-stamped legs
    -> enter both users' sequencers with OCC guards
    -> apply both legs or reject whole trade
    -> emit BlockTradeEvent + per-user settlement journals
    -> history
```

必须校验双方保证金和仓位限制，并生成独立 `trade_type=BLOCK`。双方可能属于不同 shard，协调方式应复用 [ADR-0071](./0071-perp-sharded-insurance-and-cross-shard-adl.md) 的“协调器决策 / shard 版本戳执行”模式：协调器只编排和记录 intent，真正仓位 mutation 仍在各自 user sequencer 内；任一 leg 版本不匹配或保证金不足，则整笔 block trade reject，不允许一边成功一边失败。行情是否公布、延迟多久公布，是产品策略配置。

## 备选方案 (Alternatives Considered)

### A. 每个高级 API 直接操作仓位

实现快，但绕过订单状态机和 Match，审计不可信。否决。

### B. batch 默认 all-or-nothing

对用户直观，但跨 symbol / 多订单 reservation 复杂，失败回滚成本高。先选 per-item result。

### C. TP/SL 直接放 perp-counter，不复用 trigger

能少一次跨服务，但会复制触发矩阵、TTL、OCO、trailing 逻辑。否决；复用 trigger，但加 perp position binding。

## 影响 (Consequences)

### 正面

- 产品 API 丰富但不破坏现有事件源模型。
- 用户能得到批量操作和持仓级 TP/SL。
- admin 强制操作和 block trade 有独立审计路径。

### 负面 / 代价

- BFF/trigger/perp-counter/history 都要扩协议。
- batch 和 close-all 需要清晰的 per-item 状态模型。
- TP/SL 与 position_idx、reduce_only、mode switch 的交互复杂。

## 实施约束 (Implementation Notes)

- 所有新 API 必须有 `client_op_id` / batch 子项 id。
- product API 的结果要能从 journal 重建，不依赖内存临时状态。
- close-all 第一版只支持 market protected reduce-only，不支持用户指定复杂路径。
- amend 默认等待旧单 terminal；fast-path 必须额外测试 worst-case reservation。
- **ADR-0077 依赖**：TP/SL position binding 落地的同一里程碑，必须把真实的 active-trigger 查询接入 perp-counter 的 `TriggerChecker` seam（ADR-0077 §3）——否则 SetPositionMode 的 flat-only 校验会漏掉 position-bound trigger，留下 orphan trigger。
- block trade 必须测试双用户 / 跨 shard 一腿失败整体 reject。
- 单测覆盖 batch 部分成功、amend cancel/fill race、TP/SL 仓位消失、close-all in-flight fill、admin force 调整审计。

## 参考 (References)

- [ADR-0014: 改单实现为撤单 + 新建](./0014-order-modify-as-cancel-new.md)
- [ADR-0045: 触发单 Trailing Stop](./0045-trigger-trailing-stop.md)
- [ADR-0077: perp 持仓模式](./0077-perp-position-mode-hedge-both-side.md)
