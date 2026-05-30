# ADR-0073: perp 强平托管仓位 —— TakenOverLot 生命周期 + RiskPool 结算 + ADL 消耗库存

- 状态: **Proposed**（2026-05-31 起草；从 ADR-0070/0071 中"backstop 接管 + ADL 修保险池 deficit"的语义问题拆出）
- 日期: 2026-05-31
- 决策者: xargin, Codex
- 相关 ADR: 0068（USDT 线性 perp，强平/保险基金/ADL 初版）、0070（强平进阶：部分强平/backstop/ADL）、0071（全局保险基金 + 跨 shard ADL）、0072（强平扫描索引）、0048（snapshot 绑 offset）、0057（asset-service + futures 资金入口）

## 范围声明（先读这一段）

本 ADR 修正一个关键语义：**ADL 不是给保险池"凭空充值"的动作，而是处理被强平后由系统接管的剩余风险敞口**。保险池最终赚/亏多少，应当由整个托管仓位的平仓结果 settlement 计算，而不是由 `mark - adl_price` 这种未实现利润差额在 counter 本地直接记为 `InsuranceDelta`。

当前 OpenTrade 已有两个相近但不完整的实现：

- `perp-counter/internal/service/liquidation.go` 的 `liquidation` 只跟踪一张在途强平 reduce-only 订单，没有 `leaves_qty`、`taken_over_pnl`、`status` 这样的托管仓位生命周期。
- `engine.BackstopTakeover` 会把用户剩余仓位关掉并给 `__perp_backstop__` 记库存，但这只是一次内部成交；没有后续"逐步甩卖/ADL 消耗库存/最终保险池结算"的状态机。

因此，ADR-0070/0071 中按 `deficit / sacrifice_per_qty` 规划 ADL 数量的口径是临时模型。**本 ADR 决定引入显式 `TakenOverLot`，把 ADL 的停止条件改成 `leaves_qty == 0`，把保险池增减放到 RiskPool settlement。**

OpenTrade 未上线，breaking change 直接改（同 [ADR-0057](./0057-asset-service-and-transfer-saga.md)），不写兼容层。

## 术语 (Glossary)

| 本 ADR 字段 | 含义 | 业界对标（仅供映射） |
|---|---|---|
| `TakenOverLot` | 被系统接管的一笔破产仓位库存；从用户账户切出后由系统负责甩卖/ADL/结算 | Bybit `TakenOverPositionDTO` / OKX liquidation engine takeover |
| `leaves_qty` | 托管库存尚未处理完的剩余数量；ADL 和甩卖都减少它 | Bybit `leaves_size_x` |
| `takeover_price` | 接管价格，通常为破产价；用于锁定用户最大亏损边界 | bankruptcy price |
| `unwind` / `甩卖` | 系统库存通过 Match 正常 reduce-only 订单逐步卖出/买回 | liquidation order unwind |
| `ADL consume` | 强制减盈利对手仓，同时减少 `TakenOverLot.leaves_qty` | auto-deleveraging |
| `RiskPool` | 全局保险池/风险池账务模块；负责周转金借出、回收、最终穿仓损益确认 | insurance fund / risk pool |
| `working capital` | 托管库存处理期间从 RiskPool 借出的周转资金或额度 | Bybit `liq_adl_balance` |
| `settlement` | 托管库存处理完后，根据接管余额 + 甩卖/ADL 已实现 PnL 计算保险池最终盈亏 | Bybit `borrowed_balance` 口径 |

## 背景 (Context)

### 当前模型的问题

现有代码把强平结果直接折成 `InsuranceDelta`，并在基金为负时触发 ADL：

```text
liquidation fill / backstop takeover -> insurance_fund += delta
if insurance_fund < 0:
    ADL close profitable users until insurance_fund >= 0
```

这个模型有两个问题：

1. **ADL close 本身不增加系统资产**。关掉盈利用户仓位只是改变该用户的仓位和已实现盈亏，不会自动让保险池余额增加。只有当平台规则明确把某部分利润划给保险池，并且有对应账务事件时，才能记为 fund credit。
2. **缺少被接管库存的停止条件**。如果没有 `leaves_qty`，系统只能按保险池 deficit 决定 ADL 减多少；这会把"处理风险敞口"和"保险池账务修复"混成一个动作。

### Bybit/OKX 公开规则与参考实现给出的方向

主流交易所公开规则里，强平到破产线后通常由 liquidation engine 接管仓位；若好于破产价平掉，盈余进保险基金；若差于破产价，保险基金覆盖；保险基金不足才触发 ADL。这个语义的核心是：**先接管库存，再处理库存，最后结算保险池**。

本地参考实现 `bybit-leaked/trading` 里也能看到同样分层：

- Trading Service 只执行 `AdlExecuteReq`，按指定价格/数量创建 reduce-only ADL 订单，并用 `OrigCrossSeq` 复核仓位是否仍 current。
- ADL 成交后记录 `AdlRecordDTO` 的 `fact_size_x` / `adl_price_x` / `adl_result`，而不是在 Trading Service 本地直接给保险池加钱。
- 保险池借还由 RiskPool 结果模型表达：`liq_adl_balance`、`liq_adl_realised_pnl`、`borrowed_balance`。最终亏损/盈余在 settlement 里确认。

### 与 ADR-0070/0071 的关系

ADR-0070/0071 的 `backstop`、全局协调器、版本戳执行仍保留；本 ADR只修正其账务和停止条件：

- `backstop` 不再只是一次内部成交，而是创建/推进 `TakenOverLot`。
- `ADL` 不再以修复 `insurance_fund < 0` 为直接目标，而是消耗某个 `TakenOverLot.leaves_qty`。
- `InsuranceDelta` 不再从 ADL 的 `sacrifice_per_qty` 派生；保险池最终变化由 `RiskPool` settlement 事件产生。

## 决策 (Decision)

### 1. 引入 `TakenOverLot` 作为强平托管库存权威状态

当逐仓仓位无法通过部分强平恢复安全，或整仓破产单无法及时被市场吸收时，owning `perp-counter` shard 创建一个托管库存：

```text
TakenOverLot {
  lot_id
  user_id              // 原被强平用户
  symbol
  side                 // 原仓位方向；系统库存方向与之相同
  total_qty
  leaves_qty
  takeover_price       // 通常为 bankruptcy_price
  trigger_mark_price
  position_version     // 接管时用户仓位版本，审计用
  status               // Init / Unwinding / AdlInProcessing / Settling / Done
  realised_pnl         // RiskPool 视角的累计甩卖/ADL 结果
  cum_fee
  working_capital_ref
  created_at, updated_at
}
```

**架构归属**：`TakenOverLot` 的全局生命周期由 `perp-risk` / RiskPool 协调器持有；用户仓位 mutation 仍只在 owning `perp-counter` shard 的 per-user sequencer 内执行。这样保持 ADR-0071 的分工：协调器决策和记账，shard 执行带版本戳的仓位变更。

### 2. 接管把用户亏损封顶，但不代表库存已经被市场消化

接管发生时：

1. `perp-counter` 在被强平用户 sequencer 内把用户仓位按 `takeover_price` 关闭。
2. 用户侧最大亏损被锁定在该逐仓保证金边界；用户退出这笔风险。
3. `perp-counter` 发 `PerpTakeoverEvent`，携带 `lot_id`、`taken_over_qty`、`takeover_price`、接管时仓位版本和用户侧结算结果。
4. `perp-risk` 创建 `TakenOverLot`，并按配置向 RiskPool 借出 working capital / 占用 symbol 配额。

这一步只完成"用户退出 + 系统接管风险"，**不要求保险池立即确认最终损益**。最终损益取决于后续库存怎么被甩卖或 ADL 消耗。

### 3. `TakenOverLot` 的处理优先级：先市场甩卖，再 ADL

每个 `TakenOverLot` 按如下顺序处理：

```text
TakenOverLot(leaves_qty > 0)
    |
    v
try unwind via Match reduce-only order
    |
    +-- filled -> leaves_qty -= filled_qty; realised_pnl += unwind_pnl
    |
    +-- not filled / price band blocked / pool quota insufficient / risk threshold hit
            |
            v
       dispatch ADL task to profitable opposite-side users
            |
            v
       ADL filled -> leaves_qty -= fact_qty; realised_pnl += adl_settlement_pnl
```

市场甩卖仍优先，因为它不主动打断盈利用户仓位。ADL 是最后手段，用于库存无法被市场吸收、RiskPool drawdown 超阈值、配额不足或极端行情下需要快速降低系统风险时。

### 4. ADL 的停止条件是 `leaves_qty == 0`

ADL planner 的输入从 `deficit` 改为 `TakenOverLot`：

```text
PlanADL(lot, candidates):
    remaining_qty = lot.leaves_qty
    for candidate by adl_score desc:
        qty = min(candidate.size, remaining_qty)
        dispatch version-stamped ADL task
        remaining_qty -= applied_qty
        if remaining_qty == 0:
            stop
```

执行端仍沿用 ADR-0071 的版本戳握手：任务携带候选仓位的 `side`、`pos_seq`、`position_version`、`adl_round`；owning shard 进入对手用户 sequencer 后复核，不匹配则拒绝。协调器根据实际 `fact_qty` 更新 `lot.leaves_qty`，而不是根据预估 qty 乐观扣减。

### 5. ADL 不直接产生保险池 credit

`PerpAdlEvent` 的语义改为"被减仓用户的执行结果"：

```text
PerpAdlEvent {
  user_id
  symbol
  lot_id
  adl_round
  price
  requested_qty
  fact_qty
  realized_pnl
  position_after
}
```

删除或弃用 ADL 事件里的 `insurance_delta` 权威语义。ADL 对保险池的影响只通过 `TakenOverLot.realised_pnl` 进入最终 settlement；如果需要审计 ADL 对 lot 的贡献，记录为 `lot_fill` / `lot_adl_fill` 明细，而不是 fund movement。

### 6. RiskPool settlement 计算最终保险池盈亏

当 `TakenOverLot.leaves_qty == 0`，进入 settlement。settlement 先计算托管流程实际回收了多少，再和 RiskPool 已拨出的 working capital 对账：

```text
net_recovery = taken_over_balance + lot.realised_pnl - lot.cum_fee
borrowed_balance = working_capital_drawn - net_recovery
final_pool_delta = -borrowed_balance
```

其中：

- `taken_over_balance` 是用户仓位被接管时转入系统侧的剩余权益/亏损边界，按项目实际账本字段定义。
- `lot.realised_pnl` 是托管库存通过市场甩卖和 ADL 消耗得到的累计结果。
- `working_capital_drawn` 是 RiskPool 为这笔 lot 实际拨出的周转金。
- `borrowed_balance > 0` 表示 RiskPool 最终承担损失；`borrowed_balance < 0` 表示托管流程产生盈余并归还/注入 RiskPool。
- `final_pool_delta` 是 fold 到保险池余额的签名增量：正数增加 fund，负数消耗 fund。

settlement 产出新的 RiskPool 账务事件，例如：

```text
RiskPoolSettlementEvent {
  lot_id
  symbol
  coin
  working_capital_ref
  taken_over_balance
  liq_adl_realised_pnl
  final_pool_delta
  status = Done
}
```

**只有这个 settlement 事件才是保险池最终余额的权威输入**。`perp-risk` 的 fund fold 从 `Liquidation/Takeover/ADL` 逐笔 delta，迁移为 fold `RiskPoolSettlementEvent` + working-capital borrow/repay。

### 7. 状态机

```text
Init
  |
  v
Unwinding -----------+
  |                  |
  | market fill      | market blocked / risk threshold hit
  v                  v
Unwinding      AdlInProcessing
  |                  |
  | leaves=0         | ADL fact_qty applied
  +--------+---------+
           |
           v
       Settling
           |
           v
         Done
```

失败和恢复规则：

- ADL task stale / rejected：不改变 `leaves_qty`，重新选候选或回到 market unwind。
- market unwind partial fill：只按实际成交扣 `leaves_qty`。
- RiskPool 借款不足：允许降低单次 market unwind qty，或直接进入 ADL；不得静默放大配额。
- coordinator 重启：从 snapshot + journal replay 恢复 `TakenOverLot`、borrow refs、in-flight ADL rounds、market unwind order refs。

## 备选方案 (Alternatives Considered)

### A. 维持现状：ADL 按保险池 deficit 修复

实现最少，但语义错误：关盈利用户仓位本身不会让保险池资产增加，除非另有明确的利润划拨账务。该模型还缺少被接管库存的停止条件，容易把用户减仓量和 fund accounting 混在一起。**否决**。

### B. 只保留 backstop 系统账户库存，不建 `TakenOverLot`

当前 `__perp_backstop__` 仓位可以表达"系统持有反向库存"，但它缺少 origin、leaves、状态、RiskPool 借据、结算明细；多个强平 lot 混在一个系统账户仓位里后，无法按 lot 做 ADL、配额和最终对账。**否决**。

### C. 每次接管立即把全部亏损记入保险池，后续库存盈亏另算

这会让 RiskPool 账面先承受最大损失，再由后续 unwind/ADL 返还。实现可行，但 fund drawdown 会被短期夸大，可能误触发 ADL 或配额限制。**不作为主路径**；可作为保守部署策略，在 settlement 模型落地前用配置开关降级。

### D. 独立 liquidation engine 持有仓位并直接改用户仓位

这会回到 ADR-0068/0070 已否定的 TOCTOU：独立服务读到的用户仓位可能在执行前已被成交/资金费/追加保证金推进。**否决**。协调器可以持有 `TakenOverLot`，但用户仓位 mutation 必须回 owning shard sequencer 并带版本戳执行。

## 理由 (Rationale)

1. **把风险敞口和资金账务分开**：`TakenOverLot.leaves_qty` 回答"还有多少库存没处理"，RiskPool settlement 回答"保险池最终赚亏多少"。这两个问题不应由同一个 `insurance_delta` 字段混答。
2. **ADL 停止条件变得可解释**：ADL 到托管库存处理完即停，和主流 liquidation-engine takeover 模型一致；不会因为 fund 短期波动而多减或少减用户仓位。
3. **对账可审计**：每个 lot 从接管、甩卖、ADL 到 settlement 都有独立 id 和明细，能还原最终 `final_pool_delta`。
4. **保留 ADR-0071 的正确性核心**：协调器只决策和记账，所有用户仓位变更仍在 owning shard sequencer 内版本戳复核。
5. **为配额和极端行情策略留位置**：RiskPool 可以基于 lot 维度做 per-symbol quota、drawdown threshold、ADL trigger/stop，不需要偷看 shard-local fund。

## 影响 (Consequences)

### 正面

- ADL 的语义、停止条件、用户通知都清晰：处理某个 `lot_id` 的剩余库存。
- 保险池余额不再被 ADL close 伪充值，账务更接近真实风险池。
- 支持多笔托管库存并行处理，能按 lot 做恢复、重试、审计和配额。
- 后续接入 market-maker / auction / external hedger 时，只需要新增 `TakenOverLot` 的 unwind executor，不改用户仓位模型。

### 负面 / 代价

- `perp-risk` 从 fund fold 升级为真正的托管库存状态机，snapshot 内容明显增加。
- `perp-counter` 事件协议要新增/调整 `lot_id`、`fact_qty`、RiskPool settlement 等字段，trade-dump/history/push 都要跟投影。
- 当前已实现的 `PlanADL(deficit, ...)`、`ApplyAdlCloseGuarded` 的 `insuranceDelta` 语义需要重做，相关测试要改。
- backstop 系统账户如果继续存在，必须避免它与 `TakenOverLot` 双重表达同一库存；需要明确一个是账务 lot，一个是对冲/持仓执行账户，不能混作权威。

### 中性

- 强平扫描、部分强平、风险档、mark price 逻辑不变。
- 单实例部署可先用内存 `TakenOverLot` + snapshot 起步；多 shard 时 `perp-risk` 是 lot 生命周期单一 owner。

## 实施约束 (Implementation Notes)

### 落地要点

- `api/event/perp_journal.proto`：
  - `PerpTakeoverEvent` 增加 `lot_id`、`taken_over_qty`、`takeover_price`、`taken_over_balance`。
  - `PerpAdlEvent` 增加 `lot_id`、`requested_qty`、`fact_qty`，弃用 `insurance_delta` 的权威 fund 语义。
  - 新增 `RiskPoolSettlementEvent` 或等价的 risk-pool journal event。
- `pkg/perprisk`：
  - 新增 `TakenOverLot`、`LotStatus`、`ApplyLotFill`、`ApplyLotADL`、`SettleLot` 纯函数。
  - `PlanADL` 签名从 `deficit` 改为 `lot leaves qty`；候选排序仍沿用 `adl_score`。
  - fund fold 改为 borrow/repay/settlement，而不是 ADL `InsuranceDelta`。
- `perp-risk`：
  - 持有 `TakenOverLot` map、working-capital refs、in-flight ADL rounds、unwind order refs；全部进入 snapshot。
  - 按 `lot_id` 派发 ADL task；收到 shard 的 ADL result 后按 `fact_qty` 扣 `leaves_qty`。
- `perp-counter`：
  - 接管时发 takeover event，不直接把后续 ADL 当 fund credit。
  - `ApplyAdlCloseGuarded` 返回实际 close qty / realized pnl / position snapshot，不再更新本地 insurance fund。
  - ADL task 必须带 `lot_id`，执行端 stale/reject 时返回明确结果，协调器不得乐观扣 lot。
- `trade-dump/history/push`：
  - 投影 takeover lot、lot fill、ADL result、RiskPool settlement，保证用户能查到"为什么被 ADL、对应哪个被接管 lot、减了多少"。

### 关键不变量

23. **一个托管库存只有一个权威 lot**：`TakenOverLot` 由 `perp-risk` 持有；backstop 系统账户不得成为 lot 生命周期权威，避免同一风险敞口双重记账。
24. **`leaves_qty` 只按事实成交扣减**：market unwind 和 ADL 都只能用 `fact_qty` 更新 lot；任务派发 qty、预估 qty、候选 size 都不能提前扣。
25. **ADL 不直接改 fund**：ADL 事件只描述被减仓用户执行结果；保险池余额只由 RiskPool borrow/repay/settlement 类事件改变。
26. **用户仓位 mutation 仍在 owning shard sequencer 内**：takeover close、ADL close 都必须带版本戳复核；协调器不得直接改用户仓。
27. **settlement 幂等**：同一 `lot_id` 只能从 `Settling` 进入 `Done` 一次；replay 不得重复 credit/debit fund。
28. **lot 完整恢复**：coordinator snapshot 必须包含所有非 Done lot、borrow refs、in-flight ADL rounds、unwind order refs；恢复后能继续处理，不丢不重。

### 测试要点

- 单测：`TakenOverLot` 状态机、partial market fill、partial ADL fill、stale ADL reject、不足候选、settlement 幂等。
- 对账：多笔 lot 并发下，`sum(final_pool_delta) == -sum(working_capital_drawn - (taken_over_balance + realised_pnl - fee))`。
- 集成：强平接管 → market unwind 部分成交 → ADL 消耗剩余 → settlement；验证 ADL close 不改变 fund，settlement 才改变 fund。
- 恢复：coordinator 在 ADL in-flight / settlement 前崩溃，snapshot+replay 后不重复扣 `leaves_qty`、不重复 settlement。
- race：候选用户在 ADL task 派发后自己成交，执行端版本戳拒绝，lot 不变并重新规划。

## 附录：ASCII 时序图

### 图 1 — 接管创建 lot

```text
perp-counter shard A             perp-risk / RiskPool
  │ user position breaches MM
  │ close user position @ bankruptcy
  │ emit PerpTakeoverEvent{lot_id, qty, price}
  ├──────────────────────────────────────► create TakenOverLot(leaves_qty=qty)
  │                                       borrow working capital / reserve quota
  │ user exits liquidation risk
```

### 图 2 — ADL 消耗 lot，而不是充值 fund

```text
perp-risk                         perp-counter shard B
  │ lot.leaves_qty > 0
  │ select profitable opposite candidate
  │ ADLTask{lot_id, qty, price, side, pos_seq, version}
  ├──────────────────────────────────────► sequencer(B): version check
  │                                       close candidate position
  │◄────────────────────────────────────── ADLResult{lot_id, fact_qty, realized_pnl}
  │ lot.leaves_qty -= fact_qty
  │ if leaves_qty == 0 -> SettleLot
```

### 图 3 — settlement 才改变 RiskPool

```text
TakenOverLot Done candidate
  taken_over_balance
  + liq_adl_realised_pnl
  - fees
        │
        v
RiskPoolSettlementEvent{final_pool_delta}
        │
        v
fund += final_pool_delta
```

## 开放问题 (Open Questions)

- `taken_over_balance` 的精确定义：用接管时用户剩余权益、破产价边界、还是工作资金拨款口径？需要和 `pkg/perpstate` 的逐仓结算代数逐项对齐。
- market unwind executor 放在哪：`perp-risk` 自己发 backstop/system order 到 Match，还是委托某个 `perp-counter` 系统用户 shard 执行？
- backstop 系统账户是否继续持有真实仓位，还是完全由 `TakenOverLot` 表达库存、外部对冲只读 lot feed？
- RiskPool 配额不足时的策略顺序：缩小 market unwind qty、立即 ADL、还是触发 socialized loss。
- 是否需要把 ADR-0070/0071 标记为部分 superseded，或只在实现时改其相关段落。

## 参考 (References)

- [ADR-0068](./0068-usdt-linear-perp.md) — USDT 线性 perp 初版强平、保险基金、ADL。
- [ADR-0070](./0070-perp-liquidation-hardening.md) — 部分强平、backstop、ADL 自动执行；本 ADR 修正其 ADL fund-repair 口径。
- [ADR-0071](./0071-perp-sharded-insurance-and-cross-shard-adl.md) — 全局协调器、版本戳执行；本 ADR 在其协调器内新增 `TakenOverLot` 生命周期。
- [ADR-0048](./0048-snapshot-offset-atomicity.md) — snapshot 绑 offset；`TakenOverLot`、borrow refs、settlement 幂等必须复用。
- 本地参考实现：`/Users/xargin/bybit-leaked/trading/trading_service/internal/tradingcore/tradingimpl/processorimpl/liqadlbiz/adl_execute.go`、`idl/models/tradingdto/taken_over_position_dto.proto`、`idl/models/riskpooldto/risk_pool_result_dto.proto`。
