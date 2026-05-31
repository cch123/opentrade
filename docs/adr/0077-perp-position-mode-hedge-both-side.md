# ADR-0077: perp 持仓模式 —— one-way / hedge both-side position

- 状态: **Proposed**（2026-05-31 起草；从 `docs/roadmap.md` 的 perp 产品化缺口 #4 提升为独立 ADR）
- 日期: 2026-05-31
- 决策者: xargin, Codex
- 相关 ADR: 0068（perp 仓位模型）、0070（强平进阶）、0071（跨 shard ADL）、0074（账户与保证金模式）

## 范围声明（先读这一段）

当前每个 `(user, symbol)` 只有一个净仓位：买入增加多仓或减少空仓，卖出相反。本 ADR 增加持仓模式：

- `ONE_WAY`：当前净仓模式。
- `HEDGE`：同一 symbol 可同时持有 long leg 和 short leg，通过 `position_idx` 区分。

本 ADR 不改变 Match；撮合仍只看订单 side / price / qty。持仓模式完全由 perp-counter 解释成交结果。

## 背景 (Context)

成熟合约产品通常允许 hedge mode：用户可同时持同一合约的多仓和空仓，用于策略对冲、网格、套利。现有净仓模式在以下场景不够：

- 用户想保留长期多仓，同时开短仓做短线对冲。
- TP/SL 需要绑定某一侧仓位，不应影响另一侧。
- ADL / 强平需要明确减哪条腿。

## 决策 (Decision)

### 1. 仓位主键加入 `position_idx`

```text
PositionKey {
  user_id
  symbol
  position_idx
}

position_idx:
  0 = ONE_WAY net position
  1 = HEDGE long leg
  2 = HEDGE short leg
```

`PositionMode` 是账户或 symbol 级配置：

```text
PositionModeConfig {
  user_id
  symbol optional
  mode       // ONE_WAY / HEDGE
  updated_at
}
```

### 2. 订单必须携带 position intent

在 hedge mode 下，下单必须指定 `position_idx`：

```text
buy  + position_idx=1 + reduce_only=false -> increase long
sell + position_idx=2 + reduce_only=false -> increase short
sell + position_idx=1 + reduce_only=true  -> close long
buy  + position_idx=2 + reduce_only=true  -> close short

invalid:
buy  + position_idx=1 + reduce_only=true
sell + position_idx=2 + reduce_only=true
buy  + position_idx=2 + reduce_only=false
sell + position_idx=1 + reduce_only=false
```

不允许隐式猜测。BFF 可以做便捷翻译，但 perp-counter wire 必须明确。

### 3. mode 切换必须在无仓无单时执行

```text
SetPositionMode(user, symbol, target_mode)
    |
    +-- active orders or active position-bound triggers? -> reject
    +-- non-flat positions? -> reject
    +-- in liquidation/adl? -> reject
    +-- else update config + journal
```

不做自动净额合并或拆仓。原因是 long/short 两条腿的 entry、margin、TP/SL、risk_id 都可能不同，自动转换会制造对账歧义。

这里的 active orders 包括 Match resting / pending orders，也包括 trigger 服务中的 position-bound TP/SL/OCO/TrailingStop。trigger 不在 Match book 里，但它持有 `position_idx` 语义；mode 切换时若不一起清理，会留下 orphan trigger。

### 4. 强平和 ADL 以 position leg 为单位

在 hedge mode 下：

- isolated：每条 leg 自成 pool。
- cross：long/short leg 都是同一 cross pool 的 member，但 P1 标准风险模型按 gross leg requirement 计保证金，不做 long/short offset 抵减；liquidation plan 可以选择其中一条或多条 leg。
- ADL 候选按 leg 排序，不按用户 symbol 聚合。
- TakenOverLot 记录 `position_idx`，避免回放时减错腿。

cross + hedge 的保证金抵减属于 portfolio / netting risk model，不在本 ADR P1 范围内。这样较保守，会重复占用一部分对冲仓保证金，但避免在没有组合风险模型时低估风险。后续若引入 OFFSET / netting，必须通过 [ADR-0074](./0074-perp-account-margin-modes.md) 的 `PoolRiskModel` 明确实现。

### 5. funding 和 realized PnL 按 leg 结算

funding 不做 long/short 自抵消。即使同一用户同 symbol 同时 long 10、short 8，也按两条腿分别计算 funding payment，除非后续产品规则显式定义 net funding。

## 备选方案 (Alternatives Considered)

### A. 同一 symbol 只存一条净仓，前端虚拟 hedge

简单，但无法绑定 TP/SL 和风险到单独腿，也无法复盘用户策略。否决。

### B. 切换模式时自动合并 / 拆分仓位

体验顺滑，但 entry、margin、realized PnL、条件单归属都难以定义。P1 否决。

### C. position_idx 只在 REST 层存在

服务端不持久化会导致 journal/history 无法复盘。否决。

## 影响 (Consequences)

### 正面

- 支持主流 hedge / both-side position 模式。
- TP/SL、reduce_only、ADL、强平都能精确绑定仓位腿。
- one-way 仍是 `position_idx=0`，兼容现有用户模型。

### 负面 / 代价

- 仓位表、snapshot、journal、history、BFF 都要加 `position_idx`。
- reduce_only、close_all、liquidation plan 复杂度上升。
- 同一用户同 symbol 可能有两条风险相反的仓位，查询展示要清晰。

## 实施约束 (Implementation Notes)

- 先把所有仓位 key 改为 `(user,symbol,position_idx)`，现有行为映射为 idx 0。
- 所有 order / settlement / funding / liquidation / ADL event 都带 `position_idx`。
- hedge mode 下未带 position_idx 的开仓请求 fail-closed。
- mode 切换必须同时查询 trigger active set，存在 position-bound trigger 时 fail-closed。
- 单测覆盖：mode 切换拒绝矩阵、active trigger 拒绝、long/short 双腿成交、非法 reduce_only side 拒绝、reduce_only 只减指定腿、funding per leg、snapshot roundtrip。

## 参考 (References)

- [ADR-0068: USDT 本位线性永续合约](./0068-usdt-linear-perp.md)
- [ADR-0074: perp 账户与保证金模式](./0074-perp-account-margin-modes.md)
