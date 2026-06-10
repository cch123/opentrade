# ADR-0077: perp 持仓模式 —— one-way / hedge both-side position

- 状态: **Accepted**（2026-05-31 起草；2026-06-10 实现前设计修订并接受，修订内容见「设计修订记录」）
- 日期: 2026-05-31（修订 2026-06-10）
- 决策者: xargin, Codex
- 相关 ADR: 0068（perp 仓位模型）、0070（强平进阶）、0071（跨 shard ADL）、0073（TakenOverLot）、0074（账户与保证金模式）、0078（订单/持仓产品 API，TP/SL 绑定持仓）、0081（reduce_only 结算加固）

## 范围声明（先读这一段）

当前每个 `(user, symbol)` 只有一个净仓位：买入增加多仓或减少空仓，卖出相反。本 ADR 增加持仓模式：

- `ONE_WAY`：当前净仓模式。
- `HEDGE`：同一 symbol 可同时持有 long leg 和 short leg，通过 `position_idx` 区分。

本 ADR 不改变 Match；撮合仍只看订单 side / price / qty，发往 Match 的 OrderEvent wire 上**不携带** `position_idx`（见 §6）。持仓模式完全由 perp-counter 解释成交结果。

## 设计修订记录（2026-06-10，实现前审查）

对照已落地的 0074/0075 代码审查原稿，修订六处：

1. **PositionModeConfig 定为 per-(user, symbol)**。原稿 `symbol optional` 允许账户级配置 + symbol 级覆盖两层并存，产生解析歧义（切账户级时已有 symbol 覆盖如何处理、flat 校验范围是哪些 symbol）。现有全部持仓配置（margin mode / leverage / risk_id）的存储与 flat-only 校验都是 per-(user, symbol) 粒度，模式配置与之对齐。账户级默认值降级为被否决的备选方案 D。
2. **trigger 检查改为注入式 seam（接缝，可替换的依赖注入点）**。原稿要求 mode 切换时查询 trigger active set，但 perp 的 TP/SL/OCO/TrailingStop 绑定持仓是 ADR-0078 的范围——trigger 服务今天只对接 spot Counter，perp trigger 在本 ADR 落地时点不可能存在。修订为：perp-counter 定义 `TriggerChecker` 接口并在切换流程中调用；本 ADR 落地时默认实现返回空集（此时空集是精确事实而非 fail-open）；ADR-0078 落地时**必须**接入真实查询（已写入 0078 的实施约束依赖）。
3. **补充 hedge leg 永不翻转（never-flip）不变量**。净仓模式下 `ApplyFill` 的反向超量成交会翻转仓位（flip）；hedge leg 的方向是主键语义的一部分（idx 1 恒为 long，idx 2 恒为 short），翻转即破坏模型。原稿未写此约束。修订为：hedge leg 上的平仓成交按 `min(fill_qty, leg_size)` 截断（clamp），永不翻转；超量部分（admission 与成交之间仓位被并发缩小的 TOCTOU（time-of-check-to-time-of-use，检查与使用之间状态已变的竞态）残留）**显式触发 `REDUCE_ONLY_INVARIANT_BREACH` 告警**，不静默丢弃——这正是 ADR-0081 §2/§4 的 excess 语义；0081 的 close capacity reservation 落地后该路径收敛为不可达的最后防线。
4. **定义 hedge 下持仓配置的粒度**（原稿未定义，见 §7）：margin mode / leverage / risk_id / auto-add 开关保持 per-(user, symbol)，两腿强制一致；`AdjustIsolatedMargin` 天然 per-leg，hedge 下必须携带 `position_idx`。原稿 §3 中「两腿 risk_id 可能不同」的表述随之删除——不自动合并的论据由 entry / margin / TP-SL 归属差异已足够支撑。
5. **ADL（auto-deleveraging，自动减仓）链路全程携带 `position_idx`**。原稿只写了 TakenOverLot 记录 idx；实际链路是 shard 候选上报 → coordinator lot → ADLTask wire → shard 执行 guard 四段，缺任何一段都会减错腿。全部补齐（见 §4）。
6. **明确 `position_idx` 的事件载体规则**（原稿「所有 order/settlement/... event 都带 position_idx」与「Match 不变」存在表述冲突）：journal 事件经由 `PerpPositionSnapshot.position_idx` 携带；无内嵌 snapshot 的事件（PerpOrderStatusEvent / PerpMarginAdjustmentEvent / PerpPositionConfigEvent）显式加字段；发往 Match 的 OrderEvent **不加**——order→leg 的映射由 perp-counter 的订单状态（snapshot + PerpOrderStatusEvent journal）持有，回放可恢复（见 §6）。

## 背景 (Context)

成熟合约产品通常允许 hedge mode：用户可同时持同一合约的多仓和空仓，用于策略对冲、网格、套利。现有净仓模式在以下场景不够：

- 用户想保留长期多仓，同时开短仓做短线对冲。
- TP/SL 需要绑定某一侧仓位，不应影响另一侧（ADR-0078）。
- ADL / 强平需要明确减哪条腿。

## 术语表 (Glossary)

| 本文用词 | 含义 | 业界对应 |
| --- | --- | --- |
| 持仓模式 position mode | 同一 symbol 是净仓还是双向腿 | Binance `positionSide` dual mode / Bybit `positionIdx` mode / OKX long-short mode |
| leg（腿） | hedge 模式下的单向仓位记录 | Bybit position (idx=1/2) |
| `position_idx` | 仓位主键第三段：0=净仓 1=多腿 2=空腿 | Bybit `positionIdx` 同语义同取值 |
| 截断 clamp | 平仓量超过腿大小时按腿大小执行 | reduce-only 语义的结算侧兜底 |
| 翻转 flip | 净仓反向超量成交后开出反向新仓 | 仅 ONE_WAY（idx 0）存在 |

## 决策 (Decision)

### 1. 仓位主键加入 `position_idx`，模式配置 per-(user, symbol)

```text
PositionKey {
  user_id
  symbol
  position_idx
}

position_idx:
  0 = ONE_WAY net position
  1 = HEDGE long leg   (Side 恒为 buy)
  2 = HEDGE short leg  (Side 恒为 sell)
```

`PositionMode` 是 **per-(user, symbol)** 配置（修订 #1）：

```text
PositionModeConfig {
  user_id
  symbol          // 必填，不存在账户级行
  mode            // ONE_WAY / HEDGE，缺省 ONE_WAY
  updated_at
}
```

引擎内仓位存储从 `map[user]map[symbol]Position` 变为 `map[user]map[symbol]{mode, legs[3]}`：模式与腿同处一个 per-symbol 容器，flat-only 校验、快照、回放共用一个边界。一条腿一经创建即保留（含 flat 状态），承载 per-leg 恢复水位（`last_match_seq` / `funding_round_seen` / `last_adl_round`）——与现行 flat 净仓记录保留水位的规则一致。

### 2. 订单必须携带 position intent，矩阵 fail-closed

订单增加 `position_idx` 字段。两个方向都 fail-closed（修订 #3 范围声明）：

```text
mode=ONE_WAY: 仅接受 position_idx=0（reduce_only 语义照旧）
mode=HEDGE:   仅接受 position_idx ∈ {1,2}，且必须命中下表

buy  + position_idx=1 + reduce_only=false -> increase long
sell + position_idx=2 + reduce_only=false -> increase short
sell + position_idx=1 + reduce_only=true  -> close long
buy  + position_idx=2 + reduce_only=true  -> close short

invalid（admission 拒绝）:
buy  + position_idx=1 + reduce_only=true
sell + position_idx=2 + reduce_only=true
buy  + position_idx=2 + reduce_only=false
sell + position_idx=1 + reduce_only=false
mode=HEDGE 且 position_idx=0
mode=ONE_WAY 且 position_idx≠0
```

hedge 下 `(side, position_idx)` 已完全决定开/平，`reduce_only` 是冗余自由度——矩阵把它用作**双重编码一致性校验**而非信息源：客户端意图与服务端解释不一致时在 admission 即拒绝，不允许隐式猜测。BFF 可以做便捷翻译，但 perp-counter wire 必须明确。

**hedge leg 永不翻转**（修订 #3）：平仓成交按 `min(fill_qty, leg_size)` 截断，leg 减到 0 即止；超量部分（excess）不改变仓位几何，但必须显式触发 `REDUCE_ONLY_INVARIANT_BREACH` 告警（journal + 日志），不允许静默丢弃——静默丢弃会让对手方成交与用户侧结算不一致，是 ADR-0081 备选方案 A 被否决的原因。flip 语义仅存在于 idx 0。admission 矩阵保证 hedge 平仓单必为 reduce_only，因此 excess 只在 admission 与成交之间仓位被并发缩小（TOCTOU 残留）时触达；ADR-0081 的 close capacity reservation 落地后，该告警路径收敛为不可达的最后防线。

### 3. mode 切换必须在无仓无单无触发器时执行

```text
SetPositionMode(user, symbol, target_mode, client_op_id)
    |
    v  (user sequencer 内串行执行)
+-- client_op_id 已缓存? ----------------> 返回首次结果（幂等）
+-- target == current? ------------------> accepted（无操作）
+-- 有未终态订单 (该 user+symbol 全部腿)? -> reject: active_orders_cancel_first
+-- TriggerChecker 报告 active trigger? --> reject: active_triggers_cancel_first
+-- 任一腿在 liquidation / takeover 中? --> reject: liquidation_in_flight
+-- 任一腿非 flat (idx 0/1/2)? ----------> reject: position_not_flat
+-- else: 更新 mode + journal(PerpPositionConfigEvent, reason=set_position_mode)
```

时序（user sequencer 串行化排除了与 PlaceOrder / 成交结算 / funding 的交错）：

```text
 BFF            perp-counter(service)        engine              journal
  |  SetPositionMode  |                        |                    |
  |------------------>| seq.do(user) 进入串行点 |                    |
  |                   |-- hasActiveOrders? ----|                    |
  |                   |-- TriggerChecker.Any? -|  (0078 前恒为空)    |
  |                   |-- hasLiquidation*3? ---|                    |
  |                   |---- SetPositionMode -->| 全腿 flat 校验+写 mode
  |                   |                        |  (engine 锁内原子)  |
  |                   |<------ OpOutcome ------|                    |
  |                   |-- emit PositionConfig(position_mode) ------>|
  |<----- resp -------|                        |                    |
```

不做自动净额合并或拆仓。原因是 long/short 两条腿的 entry、margin、TP/SL 归属都可能不同，自动转换会制造对账歧义。

active orders 包括 Match resting / pending orders。trigger 侧（修订 #2）：position-bound TP/SL/OCO/TrailingStop 持有 `position_idx` 语义但不在 Match book 里，mode 切换若不检查会留下 orphan trigger。该检查经 `TriggerChecker` seam 注入；perp trigger 能力随 ADR-0078 落地，0078 实现时必须把真实查询接到这个 seam 上（0078 实施约束已含此依赖）。本 ADR 落地时点 perp trigger 尚不存在，默认实现返回空集是精确语义。

### 4. 强平和 ADL 以 position leg 为单位，`position_idx` 全链路携带

在 hedge mode 下：

- isolated：每条 leg 自成 pool，强平跟踪键从 `(user, symbol)` 变为 `(user, symbol, position_idx)`；触发强平时只取消**该腿**的订单（按订单 `position_idx` 过滤），另一腿的订单与保证金不受影响。bankruptcy reduce_only 订单携带 `position_idx`，其成交按腿结算。
- cross：long/short leg 都是同一 cross pool 的 member，P1 标准风险模型按 gross leg requirement 计保证金，不做 long/short offset 抵减；liquidation plan 可以选择其中一条或多条 leg。**实现注意**：cross 候选健康度模拟在替换候选腿时，必须按 `(symbol, position_idx)` 排除旧腿，不能按 symbol 排除——按 symbol 会把同 symbol 的另一条腿一并丢出 pool，低估 requirement。
- ADL 候选按 leg 排序，不按用户 symbol 聚合。
- **全链路携带 idx**（修订 #5）：shard 候选上报（ADLCandidate）→ coordinator TakenOverLot → ADLTask wire → shard 执行 guard（`ApplyAdlCloseGuarded` 按 `(user, symbol, position_idx)` 定位 + PosSeq/Version 复核）四段都带 `position_idx`，任何一段缺失都可能减错腿。

cross + hedge 的保证金抵减属于 portfolio / netting risk model，不在本 ADR P1 范围内。这样较保守，会重复占用一部分对冲仓保证金，但避免在没有组合风险模型时低估风险。后续若引入 OFFSET / netting，必须通过 [ADR-0074](./0074-perp-account-margin-modes.md) 的 `PoolRiskModel` 明确实现。

backstop 系统账户的承接库存恒以净仓（idx 0）记账，系统账户不参与 hedge 模式。

### 5. funding 和 realized PnL 按 leg 结算

funding 不做 long/short 自抵消。即使同一用户同 symbol 同时 long 10、short 8，也按两条腿分别计算 funding payment，除非后续产品规则显式定义 net funding。同一 `(user, symbol)` 两条腿的同一 funding round 在**同一个 sequencer step 内**结算（一次进入、两条 journal 记录），避免与成交结算交错导致两腿基于不同时点的 size。幂等水位 `funding_round_seen` per leg。

### 6. `position_idx` 的事件载体规则（修订 #6）

```text
携带方式                          消息
-------------------------------- ----------------------------------------
PerpPositionSnapshot 新增字段     settlement / funding / liquidation /
  (所有内嵌 snapshot 的事件自动覆盖)  adl / takeover 的 position_after
显式新增字段                      PerpOrderStatusEvent
                                  PerpMarginAdjustmentEvent
                                  PerpPositionConfigEvent (+ position_mode)
不携带                            发往 Match 的 OrderEvent（Match 对仓位无感知）
```

order → leg 的映射是 perp-counter 私有状态：订单记录含 `position_idx`，随 service snapshot 持久化并由 PerpOrderStatusEvent 进 journal；成交回流按 order_id 查回 idx 路由结算。Match 始终不感知持仓模式（范围声明）。

### 7. hedge 下持仓配置粒度（修订 #4）

| 配置项 | 粒度 | hedge 下行为 |
| --- | --- | --- |
| position mode | per-(user, symbol) | 本 ADR 主体 |
| margin mode (isolated/cross) | per-(user, symbol) | SetMarginMode 原子切**两腿**：全部校验通过才落，任一腿不安全则整体拒绝；cross→isolated 的 `target_margin` 针对单一仓位，hedge 下显式拒绝（`target_margin_unsupported_in_hedge`），切换后用 per-leg AdjustIsolatedMargin 调整 |
| leverage | per-(user, symbol) | SetPositionLeverage 同时 resize 两腿 isolated margin，两腿安全校验都过才落 |
| risk_id | per-(user, symbol) | tier 的 notional cap 按两腿 gross notional 之和 + 未成交订单校验 |
| auto_add_margin 开关 | per-(user, symbol) | 开关两腿统一；触发执行时 per-leg 各自 top-up |
| AdjustIsolatedMargin | **per-leg** | hedge 下请求必须携带 `position_idx` |

两腿配置强制一致与 Binance dual-side（杠杆/保证金模式 per symbol）一致；Bybit 的 buy/sell 分腿杠杆是产品增强，推迟到有需求时通过 per-leg 配置版本另行评估。flat 的 leg 记录保留配置（与现行 flat 净仓记录保留配置一致）。

## 备选方案 (Alternatives Considered)

### A. 同一 symbol 只存一条净仓，前端虚拟 hedge

简单，但无法绑定 TP/SL 和风险到单独腿，也无法复盘用户策略。否决。

### B. 切换模式时自动合并 / 拆分仓位

体验顺滑，但 entry、margin、realized PnL、条件单归属都难以定义。P1 否决。

### C. position_idx 只在 REST 层存在

服务端不持久化会导致 journal/history 无法复盘。否决。

### D. 账户级 PositionModeConfig + symbol 级覆盖（原稿方案）

Binance 的 dual-side 是账户全局开关，看似省配置。但两层配置引入解析歧义（账户级切换时 symbol 覆盖行的处置、flat 校验需要扫全部 symbol），且与本系统全部既有持仓配置的 per-(user, symbol) 粒度冲突。否决；per-symbol 粒度严格更细，账户级开关可在 BFF 做批量便捷操作。

### E. hedge leg 允许翻转，翻转时自动改挂到对侧腿

省一个结算分支，但「sell 平多超量后变成空腿增量」会让平仓单隐式变成开仓单——绕过开仓的 IM（initial margin，开仓初始保证金）预占与风险准入。否决；leg 永不翻转，超量截断。

## 影响 (Consequences)

### 正面

- 支持主流 hedge / both-side position 模式，`position_idx` 取值与 Bybit 对齐。
- TP/SL（0078）、reduce_only、ADL、强平都能精确绑定仓位腿。
- one-way 仍是 `position_idx=0`，现有用户模型零迁移（缺省模式 ONE_WAY，旧快照无 idx 字段解析为 0）。

### 负面 / 代价

- 仓位存储、snapshot、journal、history、BFF、perprisk wire 都要加 `position_idx`；trade-dump `perp_positions` 主键变为 `(user_id, symbol, position_idx)`，各事实表加 `position_idx` 列（未上线，直接改 schema，无兼容层）。
- reduce_only、close_all、liquidation plan 复杂度上升。
- 同一用户同 symbol 可能有两条风险相反的仓位，查询展示要清晰。
- cross + hedge 不做 netting，多占一部分对冲仓保证金（保守换正确）。

## 实施约束 (Implementation Notes)

- 仓位 key 改为 `(user, symbol, position_idx)`，现有行为映射为 idx 0；engine 内模式与腿同容器存储。
- mode 切换成功后立即物化（materialize）新模式的 flat leg 记录（继承 sibling 配置）：journal 回显与配置查询需要载体行，否则从未交易过的 symbol 上的切换对回放不可见。
- 新建 leg 继承 sibling leg 的配置（margin mode / leverage / risk_id / auto-add），保证 §7 的 per-symbol 配置一致性与创建顺序无关。
- journal / snapshot 携带规则见 §6；Match wire 不变。
- hedge mode 下 `position_idx` 缺失或与模式矩阵不符的请求 fail-closed（两个方向）。
- mode 切换经 `TriggerChecker` seam 检查 active trigger；ADR-0078 落地时必须接入真实查询。
- funding 同一 (user, symbol) 两腿在同一 sequencer step 结算。
- ADL 链路（候选上报 / lot / task wire / 执行 guard）全程携带 `position_idx`。
- 单测覆盖：mode 切换拒绝矩阵（订单/trigger/强平/非 flat）、long/short 双腿成交、非法 (side, idx, reduce_only) 组合拒绝、reduce_only 只减指定腿、**hedge leg 超量平仓截断不翻转且 excess 触发 breach 告警**、funding per leg（含同 step 双腿）、**cross+hedge 候选健康度不丢同 symbol 另一腿**、per-leg 强平互不影响、ADL task 按 idx 命中、snapshot roundtrip（含 mode 与三腿）。

## 参考 (References)

- [ADR-0068: USDT 本位线性永续合约](./0068-usdt-linear-perp.md)
- [ADR-0074: perp 账户与保证金模式](./0074-perp-account-margin-modes.md)
- [ADR-0078: perp 订单/持仓产品 API](./0078-perp-order-position-product-api.md)
- [ADR-0081: perp reduce_only 结算加固](./0081-perp-reduce-only-settlement-hardening.md)
- Bybit V5 Position Mode（positionIdx 0/1/2 语义对齐参照）
