# ADR-0074: perp 账户与保证金模式 —— cross / unified / portfolio 的演进路径

- 状态: **Proposed**（2026-05-31 起草；从 `docs/roadmap.md` 的"账户与保证金模式"产品化缺口提升为独立 ADR）
- 日期: 2026-05-31
- 决策者: xargin, Codex
- 相关 ADR: 0068（USDT 线性 perp，collateral pool 抽象）、0070（阶梯风险限额 / 部分强平 / ADL）、0071（perp 分片化风控 + 全局协调器）、0072（强平扫描索引）、0073（TakenOverLot + RiskPool settlement）、0056（SymbolConfig via MySQL）、0057（asset-service + futures 资金入口）、0048（snapshot 绑 offset）、0018（UserSequencer）

## 范围声明（先读这一段）

当前 perp 已实现的是 **USDT 线性永续 + 逐仓 isolated**。`MarginMode.CROSS` 只是在 proto / state 里预留的 wire shape；BFF 不暴露 `margin_mode`，perp-counter 也没有 cross 分支。

本 ADR 定义后续账户与保证金模式的落地路径，覆盖：

1. **cross margin**：账户级共享保证金池。
2. **保证金模式切换**：逐仓 ↔ 全仓。
3. **加减逐仓保证金**：手动调整 isolated position margin。
4. **自动追加保证金**：isolated 仓位从可用余额自动补保证金。
5. **持仓级杠杆设置**：杠杆从"下单字段"升级为 position config。
6. **risk_id 设置**：用户选择风险限额档。
7. **客户最大杠杆**：平台 / 客户 / symbol 维度的杠杆上限。
8. **unified margin / portfolio margin 路径**：不立即实现，但当前数据模型不能把路堵死。

本 ADR **不做**：hedge 双向持仓、期权/组合保证金的具体风险公式、借贷/负余额、统一账户里的多币种抵押 haircut 细则、前端交互细节。它们必须在此 ADR 的接口边界内继续展开。

OpenTrade 未上线，breaking change 直接改（同 [ADR-0057](./0057-asset-service-and-transfer-saga.md)），不写兼容层。

## 术语 (Glossary)

| 本 ADR 字段 | 含义 | 业界对标（仅供映射） |
|---|---|---|
| `margin_mode` | 仓位使用的保证金模式：`ISOLATED` / `CROSS` | isolated / cross margin |
| `collateral_pool` | 一组共享权益和风控计算的仓位 + 可动用抵押物 | OKX risk unit / Bybit account pool |
| `position_config` | 用户对某个 symbol 持仓的配置：mode、leverage、risk_id、auto_add_margin | position settings |
| `account_config` | 用户账户级配置：账户模式、客户最大杠杆、默认 mode 等 | account settings |
| `risk_id` | 用户选择的风险限额档 id；决定允许的最大名义值和更保守的 MMR / max leverage | Bybit riskId / risk limit |
| `customer_max_leverage` | 平台给某用户或客户组配置的最大杠杆上限 | user leverage cap |
| `auto_add_margin` | 逐仓仓位触发风险阈值时，从账户可用余额自动转入 position margin | auto add margin |
| `unified margin` | 多产品 / 多资产共享一套抵押物的保证金账户 | unified trading account |
| `portfolio margin` | 按组合压力测试计算保证金要求，而不是逐仓位名义值相加 | portfolio margin / PM |

## 背景 (Context)

### 现状

[ADR-0068](./0068-usdt-linear-perp.md) 明确逐仓起步，并把全仓留在 `CollateralPool` 抽象里：

- 逐仓：每个 `(user, symbol)` 仓位自成一个 pool，`position_margin` 是这个 pool 的权益边界。
- 全仓：一个用户的 cross 仓位共享账户级 pool，仓位本身不独占一份 margin，权益来自账户可用抵押物。

当前代码已经有关键基础：

1. **per-user sequencer**：所有同一用户的仓位 mutation 在一个 sequencer 内串行，cross pool 的账户级计算天然落在该用户 shard。
2. **`CollateralPool` seam**：强平判定应通过 pool health，而不是调用点内联 per-position ratio。
3. **futures wallet**：asset-service 已能通过 `biz_line=futures` 给 perp-counter 充值 / 提现。
4. **risk tier 基础**：[ADR-0070](./0070-perp-liquidation-hardening.md) 已把 MMR / max leverage 分档抽象成风险模型的一部分。

但要做真正的产品化账户，还缺几个明确的操作面：

- 用户不能切换 isolated / cross。
- 用户不能手动给逐仓加减保证金。
- 杠杆只是下单字段，不是可查询、可修改、可审计的持仓配置。
- `risk_id` 没有用户选择语义，只能按名义值被动查档。
- 平台无法按客户 / symbol / 产品维度下发最大杠杆限制。
- `Available/Reserved` 的 wallet 语义适合 isolated MVP，但不足以表达 cross / unified 下"账本余额"与"派生可用保证金"的区别。

## 决策 (Decision)

### 1. 采用分阶段路线，不直接跳 unified / portfolio

落地顺序固定为：

1. **P0：补齐 isolated 操作面**
   先做 `position_config`、持仓级杠杆、risk_id、客户杠杆上限、手动加减保证金、自动追加保证金。此阶段仍只有逐仓，但把账户配置和 journal/snapshot 形状建好。

2. **P1：USDT futures cross margin**
   在 perp-counter 内增加账户级 cross pool。范围只覆盖 USDT 线性合约；isolated 和 cross 可以在同一用户账户内并存，但同一个 `(user, symbol)` 当前净仓只能属于一个 mode。

3. **P2：unified margin 接口化**
   账户抵押物从单一 USDT futures wallet 演进为多资产 / 多产品 collateral account。产品服务仍是仓位执行权威；统一账户只通过明确的 collateral / exposure 接口接入，不把 spot Counter、perp-counter、asset-service 合并成一个大服务。

4. **P3：portfolio margin 风险模型**
   在 unified margin 之上替换保证金要求计算：从 standard risk（名义值 × tier MMR）切到 portfolio risk（压力场景损失 + add-on）。这一步必须依赖更完整的产品族和行情风险因子，不在 cross 阶段硬塞。

这个顺序的核心理由：**cross 是账户共享权益问题；unified 是跨资产 / 跨产品抵押物问题；portfolio 是风险模型问题**。三者不混在同一个里程碑里，否则每一步都无法验收。

阶段路线图：

```text
+-------------------+     +-------------------+     +-------------------+     +-------------------+
| P0 isolated ops   | --> | P1 USDT cross     | --> | P2 unified acct   | --> | P3 portfolio risk |
| - config journal  |     | - account pool    |     | - collateral API  |     | - stress model    |
| - leverage/riskId |     | - pool liquidation|     | - exposure feed   |     | - PM liquidation  |
| - add/remove IM   |     | - mode switch     |     | - multi-asset     |     | - scenario audit  |
+-------------------+     +-------------------+     +-------------------+     +-------------------+
```

### 2. 先重构账户账本语义：账本余额与派生可用保证金分离

在实现 cross 前，perp-counter 不能继续把 `Wallet.Available` 当作唯一真值。需要把可持久化账本桶和派生风控值拆开：

```text
PerpMarginAccount {
  user_id
  settle_asset              // P0/P1 固定 USDT

  free_balance              // 未被 isolated margin 或 open-order reservation 锁住的现金桶
  order_margin_reserved     // 开仓挂单预占 IM
  isolated_margin_locked    // Σ isolated position.margin，可缓存但以 position 为权威

  cross_initial_required    // 派生值：cross 仓位当前 IM 要求，不作为现金桶 journal
  cross_maintenance_required// 派生值：cross pool 当前 MM 要求
  cross_unrealized_pnl      // 派生值：cross 仓位 mark 未实现盈亏

  available_to_trade        // 派生值，不直接 journal
  available_to_withdraw     // 派生值，不直接 journal
}
```

持久化 journal 只记录真实资金动作：

- 充值 / 提现。
- 下单预占 / 撤单释放。
- isolated position margin 的转入 / 转出。
- 成交产生的 realized PnL、fee、funding。
- 配置变更事件。

`cross_initial_required`、`cross_maintenance_required`、`available_to_trade` 由仓位、mark、risk tier、open orders 实时计算或缓存重建，不能作为独立资金流水。这样 unified / portfolio 后也能替换风险公式，而不是重写账本。

### 3. 把 `CollateralPool` 升级为风险模型接口

0068/0070 的 `CollateralPool` seam 继续保留，但 cross 后不能只靠 `MMRFunc(pool_notional)`，因为多 symbol pool 的维持保证金通常是 per-symbol / per-risk-tier 要求求和，portfolio margin 更不是标量 MMR。

新增标准接口：

```go
type PoolRiskModel interface {
    Eval(pool CollateralPool, marks MarkSet) PoolHealth
    InitialRequirement(pool CollateralPool, marks MarkSet) dec.Decimal
    MaintenanceRequirement(pool CollateralPool, marks MarkSet) dec.Decimal
    LiquidationPlan(pool CollateralPool, marks MarkSet) LiquidationPlan
}

type PoolHealth struct {
    Equity                 dec.Decimal
    Notional               dec.Decimal
    InitialRequirement     dec.Decimal
    MaintenanceRequirement dec.Decimal
    MarginRatio            dec.Decimal // 兼容展示；Liquidatable 不再只看 ratio
}
```

标准模式下：

- isolated pool：`Equity = position_margin + uPnL`，`MaintenanceRequirement = notional × tier.MMR`。
- cross pool：`Equity = cross_pool_cash + Σ cross uPnL`，`MaintenanceRequirement = Σ per-position maintenance_requirement`。
- liquidatable：`Equity <= MaintenanceRequirement`。

portfolio 模式下只替换 `PoolRiskModel`，调用点仍只看 `PoolHealth` / `LiquidationPlan`。

### 4. cross margin 语义

P1 只实现 **USDT futures cross pool**：

```text
pool_id = "cross:{user_id}:USDT"
members = all positions where margin_mode == CROSS and settle_asset == USDT
excluded = isolated positions, isolated margin, isolated-only open orders
```

核心规则：

1. **同用户同 settle asset 只有一个 cross pool**。不引入 per-symbol cross 子池，避免把全仓退化成多份小全仓。
2. **cross 仓位不持有 `position_margin` 现金桶**。仓位仍有 entry/size/side/leverage/risk_id，但 margin requirement 是派生值。
3. **isolated margin 不参与 cross drawable**。用户给 BTC 逐仓追加的保证金不能救 ETH cross 仓。
4. **open order IM 仍要预占**。否则 cross 用户可以同时挂出多笔互斥但总 IM 超额的订单。
5. **cross 强平是 pool 触发，position 选择由 `LiquidationPlan` 决定**。不能把每个 cross 仓位单独用 isolated liq price 扫描。
6. **0072 的 per-position liq-price index 只用于 isolated**。cross 需要 account-pool danger index 或 mark tick 后按 owning user pool 评估；不能复用 isolated 强平价索引得出错误结论。

cross 的订单准入：

```text
candidate_state = 当前 cross pool + 本次订单最大增仓影响
if health(candidate_state).Equity < InitialRequirement(candidate_state) + safety_buffer:
    reject insufficient_margin
if requested_leverage > effective_max_leverage(user, symbol, risk_id, mode):
    reject leverage_exceeds_max
```

成交后，订单 reservation 不再转入 `position.margin`，而是被新的 cross position requirement 吸收；未成交部分继续 reservation，撤单 / 过期释放。

cross 下单与成交的资金流：

```text
PlaceOrder
    |
    v
+------------------+
| build candidate  |
| cross pool       |
+------------------+
    |
    v
+------------------+      no       +----------------------+
| equity >= IM req | ------------> | reject insufficient  |
| and lev <= cap ? |               | margin/leverage      |
+------------------+               +----------------------+
    |
   yes
    |
    v
+------------------+     trade fill      +-----------------------+
| reserve order IM | ------------------> | recompute cross req   |
| from free cash   |                     | release filled reserve|
+------------------+                     +-----------------------+
    |                                             |
 cancel/expire                                     v
    |                                    +-----------------------+
    +----------------------------------> | release unused reserve|
                                         +-----------------------+
```

### 5. 保证金模式切换

新增用户操作：

```text
SetMarginMode(user_id, symbol, target_mode, optional_target_margin, client_op_id)
```

通用约束：

- 必须在用户 sequencer 内执行。
- `client_op_id` 幂等，重复请求返回首次结果。
- 该 symbol 不得有会增加仓位的活跃订单；reduce-only 单也建议先撤掉，避免用户以为切换后订单语义跟着变。
- 仓位不得处于 liquidation / takeover / ADL in-flight。
- 切换必须同时复核当前 mark、position version、risk_id、leverage。
- 切换前后两个状态都不能低于 maintenance requirement；切换后的目标状态还必须满足 initial requirement 或配置的更高安全线。

#### isolated → cross

流程：

1. 取当前 isolated position 的 `position_margin`。
2. 构造加入 cross pool 后的 candidate health。
3. 若 candidate cross pool 低于 initial requirement 或 maintenance requirement，拒绝。
4. 将 `position_margin` 转回账户 free/cross drawable，仓位 `margin_mode=CROSS`、`position_margin=0`。
5. 递增 position version，发 `PerpPositionConfigEvent` + `PerpMarginTransferEvent`。

注意：释放的 isolated margin 不是提现，只是从单仓独占权益转成 cross pool 可动用权益。

#### cross → isolated

流程：

1. 计算目标 isolated margin：`max(optional_target_margin, isolated_initial_requirement + buffer)`。
2. 构造 cross pool 移出该仓位后的 candidate health，并构造 isolated pool health。
3. 若 cross pool 移出后不健康，或账户可用抵押物不足以拨入目标 isolated margin，拒绝。
4. 从 free/cross drawable 划出目标 margin 到 position.margin，仓位 `margin_mode=ISOLATED`。
5. 递增 position version，发 journal。

模式切换的共同检查流程：

```text
SetMarginMode
    |
    v
+----------------------+
| enter user sequencer |
+----------------------+
    |
    v
+----------------------+
| active increase ord? |
+----------------------+
    | yes                         no
    v                             |
+----------------------+          v
| reject, cancel first |   +----------------------+
+----------------------+   | liq/takeover/adl ?  |
                           +----------------------+
                              | yes          no
                              v              |
                    +------------------+     v
                    | reject in-flight |  +----------------------+
                    +------------------+  | simulate old/new pool|
                                          +----------------------+
                                                   |
                                                   v
                                      +--------------------------+
                                      | both states healthy ?    |
                                      +--------------------------+
                                          | no              yes
                                          v                 |
                                  +---------------+         v
                                  | reject unsafe |  +------------------+
                                  +---------------+  | apply + journal  |
                                                     +------------------+
```

### 6. 加减逐仓保证金

新增用户操作：

```text
AdjustIsolatedMargin(user_id, symbol, delta, client_op_id)
```

规则：

- 只允许 `margin_mode=ISOLATED` 且仓位非 flat。
- `delta > 0`：从账户可用余额转入 position.margin；不足则拒绝。
- `delta < 0`：从 position.margin 转出到账户可用余额；转出后必须满足：
  - `PoolHealth.Equity > MaintenanceRequirement + liquidation_buffer`
  - `position_margin >= isolated_initial_requirement`，或满足产品配置的最低逐仓保证金规则
  - 仓位不在 liquidation / takeover / ADL in-flight
- 操作不改变 size / entry / realized PnL / leverage。
- 每次调整都 journal，history 可查。

### 7. 自动追加保证金

`auto_add_margin` 是逐仓 position config：

```text
SetAutoAddMargin(user_id, symbol, enabled, optional_max_add_per_event, client_op_id)
```

执行语义：

- 只对 isolated 仓位生效；cross 本来就是共享账户权益，不需要 auto-add。
- 在 mark tick 触发强平检查前，于同一用户 sequencer 内先运行 auto-add 逻辑。
- 当 isolated pool 的 health 低于 `auto_add_trigger_ratio`，且高于立即接管阈值时，从账户可用余额补到 `auto_add_target_ratio`。
- 补充金额受三层限制：账户可用余额、`max_add_per_event`、产品级最大追加配置。
- 可用余额不足时补多少算多少；补完仍破 maintenance，则继续进入正常强平流程。
- auto-add 必须发 journal；否则用户看到仓位 margin 增加却没有账可对。

auto-add 不能异步放在后台 daemon 里做。它必须和强平判断共用同一个 sequencer / mark 视图，否则会出现"强平已经 armed，另一个 goroutine 又补保证金"的 TOCTOU。

auto-add 必须位于强平判断之前：

```text
MarkTick(symbol)
    |
    v
+------------------------+
| enqueue user sequencer |
+------------------------+
    |
    v
+------------------------+
| isolated position ?    |
+------------------------+
    | no                         yes
    v                            |
+------------------------+       v
| eval pool health       |  +------------------------+
+------------------------+  | auto_add enabled ?     |
                            +------------------------+
                               | no              yes
                               |                 |
                               v                 v
                         +-------------+   +------------------------+
                         | eval health |   | transfer free -> margin|
                         +-------------+   | emit margin journal    |
                                           +------------------------+
                                                     |
                                                     v
                                           +------------------------+
                                           | re-eval pool health    |
                                           +------------------------+
                                                     |
                                                     v
                                           +------------------------+
                                           | still below MM ?       |
                                           +------------------------+
                                                | no          yes
                                                v            v
                                           +---------+   +----------------+
                                           | safe    |   | arm liquidation|
                                           +---------+   +----------------+
```

### 8. 持仓级杠杆设置

新增用户操作：

```text
SetPositionLeverage(user_id, symbol, leverage, client_op_id)
```

规则：

- 杠杆成为 `PositionConfig` 的持久字段；下单请求里的 `leverage` 只作为创建或覆盖配置的便捷入口，不能再是唯一来源。
- `effective_max_leverage = min(symbol/risk tier max, customer_max_leverage, account_mode_cap, product_cap)`。
- 设置杠杆不得超过 `effective_max_leverage`。
- 已有仓位改杠杆时：
  - isolated：按新杠杆计算目标 initial margin。降低杠杆需要追加 margin；提高杠杆可以释放多余 margin，但释放后必须仍满足 §6 的安全线。
  - cross：不移动现金桶，只改变 cross initial requirement 和后续订单准入；若新杠杆导致当前 pool initial requirement 不满足，拒绝。
- 有活跃增仓订单时拒绝，要求先撤单；否则旧订单 reservation 与新杠杆要求会混在一起。

杠杆变化不改变 entry、size、realized PnL，也不 retroactively 改历史 settlement。

### 9. `risk_id` 设置

新增用户操作：

```text
SetRiskID(user_id, symbol, risk_id, client_op_id)
```

`risk_id` 表示用户选择的风险限额档：

- `risk_id` 对应 SymbolConfig 里的一档：`max_notional`、`maint_margin_ratio`、`max_leverage`、`liq_fee_rate`。
- 未显式设置时，系统使用能覆盖当前/候选名义值的最低档。
- 显式设置后，订单后的名义值不得超过该档 `max_notional`。
- 风险计算采用更保守口径：`effective_tier = max(auto_notional_tier, selected_risk_id)`。用户主动选择更高 risk_id 可以放大允许名义值，但也接受更高 MMR / 更低 max leverage。
- 降低 risk_id 只在当前仓位 + 活跃订单名义值能被目标档覆盖、且切换后 health 仍安全时允许。

这能兼容两类产品形态：

- 新手用户不关心 risk_id，系统自动取最低覆盖档。
- 专业用户主动提高 risk_id，换取更大的持仓上限，但承担更保守的维持保证金要求。

### 10. 客户最大杠杆

平台需要一个独立于 SymbolConfig 的杠杆上限层：

```text
CustomerLeverageLimit {
  user_id / customer_group_id
  product_type      // perp
  symbol optional
  settle_asset optional
  margin_mode optional
  max_leverage
  reason
  updated_by
  updated_at
}
```

准入时统一计算：

```text
effective_max_leverage =
  min(
    symbol_risk_tier.max_leverage,
    customer_limit.max_leverage,
    product_default.max_leverage,
    margin_mode_cap.max_leverage
  )
```

客户最大杠杆是**上限**，不是用户当前杠杆。调低客户上限时：

- 新订单立即按新上限拒绝。
- 现有仓位不强制改杠杆或强平；但用户不能继续增仓到违反新上限的状态。
- 若产品需要"调低后强制降杠杆"，必须另起风控任务并写审计事件，不能在配置写入时偷偷 mutation 用户仓位。

### 11. unified margin 路径

unified margin 不通过"把所有账户服务合并"实现。选定路径：

1. **产品服务仍是仓位执行权威**：perp-counter 改 perp 仓位，spot Counter 改 spot 余额 / 订单；它们不互相直接改对方状态。
2. **collateral account 抽象独立出来**：asset-service 或后续 margin-account 服务持有统一抵押物账本、haircut、跨产品 reservation。
3. **产品服务通过 exposure/reservation 协议接入**：
   - 下单前申请 collateral reservation。
   - 成交后上报 exposure delta / realized PnL / fee。
   - 风控服务基于产品 exposure snapshot 计算统一 health。
4. **per-user 分片原则不变**：同一用户的高频风险 mutation 必须进入该用户的 shard / sequencer，跨服务只能传递带版本戳的任务，不能基于过期远程读直接改仓位。

P2 的最小形态可以先是"perp-only unified collateral"：多币种抵押物只服务 perp cross pool，其他产品不接入。等协议稳定后再接 spot margin / options。

unified margin 的服务边界：

```text
                         +----------------------+
                         | margin-account       |
                         | collateral authority |
                         +----------------------+
                            ^        ^        ^
               reservation  |        |        | exposure delta
                            |        |        |
+---------------+           |        |        |           +---------------+
| spot Counter  | ----------+        |        +---------- | perp-counter  |
| spot orders   | <------------------+------------------> | perp positions|
+---------------+        versioned collateral API         +---------------+
        |                                                       |
        v                                                       v
+---------------+                                      +------------------+
| spot journal  |                                      | perp journal     |
+---------------+                                      +------------------+
```

### 12. portfolio margin 路径

portfolio margin 是 `PoolRiskModel` 的一种实现，不是新的账户服务拓扑。

standard margin：

```text
InitialRequirement = Σ position_notional × tier.IMR
MaintenanceRequirement = Σ position_notional × tier.MMR
```

portfolio margin：

```text
InitialRequirement = max(stress_loss_scenarios) + concentration_addon + liquidity_addon
MaintenanceRequirement = maintenance_stress_loss + addons
```

接口要求：

- pool 必须能携带 product exposures，而不只是 perp positions。
- mark set 必须扩展为 risk factor set（index、vol、rate、correlation 等）。
- liquidation plan 必须能返回跨产品 action，但执行仍拆回各产品服务的 sequencer。
- portfolio risk model 的版本、参数、场景集必须进入 journal/snapshot/audit；否则历史强平无法复盘。

因此 P0/P1 阶段只把字段留好：`risk_model=STANDARD|PORTFOLIO`、`pool_id`、`exposure_version`。不提前实现 PM 公式。

standard risk 与 portfolio risk 的替换点：

```text
CollateralPool + Marks/RiskFactors
              |
              v
       +---------------+
       | PoolRiskModel |
       +---------------+
          |         |
          |         |
          v         v
 +---------------+ +----------------+
 | STANDARD      | | PORTFOLIO      |
 | sum tier IM/MM| | stress + addon |
 +---------------+ +----------------+
          |         |
          +----+----+
               |
               v
       +----------------+
       | PoolHealth     |
       | LiqPlan        |
       +----------------+
               |
               v
       +----------------+
       | sequencer exec |
       +----------------+
```

### 13. API / journal / projection 形状

PerpService 新增 RPC：

```text
SetMarginMode
AdjustIsolatedMargin
SetAutoAddMargin
SetPositionLeverage
SetRiskID
QueryPositionConfig
QueryAccountConfig
```

Admin / internal API 新增：

```text
SetCustomerLeverageLimit
ListCustomerLeverageLimits
```

journal 新增或扩展：

```text
PerpPositionConfigEvent {
  user_id
  symbol
  margin_mode
  leverage
  risk_id
  auto_add_margin
  position_version
  reason
}

PerpMarginAdjustmentEvent {
  user_id
  symbol
  kind                  // ADD_ISOLATED / REMOVE_ISOLATED / MODE_SWITCH / AUTO_ADD
  amount
  margin_before
  margin_after
  wallet_after
  position_version
  client_op_id
}

PerpCustomerRiskLimitEvent {
  user_id / customer_group_id
  symbol optional
  max_leverage
  reason
  updated_by
}
```

trade-dump / history 必须能查询：

- 当前 position config。
- margin adjustment 流水。
- mode switch 历史。
- leverage / risk_id 变更历史。
- 客户杠杆上限审计。

## 备选方案 (Alternatives Considered)

### A. 直接做 unified / portfolio，跳过 futures cross

- 优点：最终形态更完整。
- 缺点：同时引入跨产品抵押物、跨服务 reservation、组合风险模型，无法把问题切小；任何 bug 都难判断是账本、风控还是执行拓扑问题。
- 结论：否决。先做 USDT futures cross，把 `CollateralPool` / `PoolRiskModel` / 操作面打稳。

### B. cross 仓位也维护一份 `position_margin`

- 优点：复用 isolated 现有结算代码最省事。
- 缺点：这是假 cross。仓位各自锁 margin，就无法表达账户级共享权益；强平也会回到 per-position ratio。
- 结论：否决。cross 仓位的 margin requirement 必须是派生风险值，不是每仓现金桶。

### C. 允许有活跃订单时切换 mode / leverage / risk_id

- 优点：用户体验少一步撤单。
- 缺点：open order reservation 是按旧配置算的，切换后需要批量重算甚至补扣；成交回流可能跨越配置边界，审计复杂且容易错。
- 结论：P0/P1 否决。要求先撤活跃增仓订单。以后如要优化，可做"切换时自动 cancel all increasing orders"，但仍不重算旧订单。

### D. auto-add margin 用后台任务异步扫

- 优点：实现表面简单。
- 缺点：和强平扫描竞争，容易出现同一 mark 下先强平后补 margin，或补 margin 后强平任务仍执行。
- 结论：否决。auto-add 必须内嵌到 mark tick 的 per-user sequencer 流程里。

### E. `risk_id` 完全自动，不给用户设置

- 优点：少一个产品操作面。
- 缺点：专业用户无法主动提高风险限额；也无法对齐主流合约系统的 risk limit 交互。
- 结论：否决。保留自动默认档，同时支持用户显式选择更高/更低 risk_id。

## 理由 (Rationale)

1. **先账户操作面，后 cross**：mode/leverage/risk_id/margin adjustment 都是 cross 的前置配置。如果没有这些 journal/snapshot 基础，cross 落地后再补会造成状态迁移返工。
2. **pool + risk model 是唯一稳定边界**：isolated、cross、unified、portfolio 的差别都应落在 pool membership、collateral source、risk model 三处，而不是散在下单、强平、查询、history 各处。
3. **per-user sequencer 不变**：账户级 cross pool 要求同一用户的所有 cross 仓位在一个执行边界内复核；0071 已经说明 user 分片是 cross margin 的必要代价。
4. **账本和风控派生值分离**：unified / portfolio 后 requirement 会随 mark、haircut、scenario 频繁变化，不能把它 journal 成资金流水。
5. **保守限制活跃订单**：配置切换低频，订单成交高频。用"先撤单再切换"换取实现可证明性，是 P0/P1 合理取舍。

## 影响 (Consequences)

### 正面

- cross margin 可以作为增量落地，不破坏 isolated 已有仓位代数。
- mode / leverage / risk_id / margin adjustment 都有审计事件，history 可复盘。
- unified / portfolio 有明确接口路径，不需要未来把账户服务推倒重来。
- 强平、订单准入、查询展示共享 `PoolRiskModel`，减少公式分叉。

### 负面 / 代价

- wallet 状态需要从 `Available/Reserved` 过渡到更明确的账本桶 + 派生可用值，改动面会碰到 perp-counter、journal、snapshot、trade-dump、history、BFF。
- cross 强平不能复用 0072 的 isolated liq-price index，需要新的 account-pool 评估路径。
- mode/leverage/risk_id 切换要求先撤增仓订单，产品体验比交易所成熟版本更保守。
- unified / portfolio 仍只是路径，不是本 ADR 直接交付的功能。

### 中性

- `margin_mode=CROSS` 的 proto 枚举可以继续复用，但必须在服务端真正校验和落库后才对外暴露。
- isolated position 的 `position_margin` 继续存在；cross position 的 `position_margin` 展示为 `0` 或空，由 API 文档说明其权益来自 cross pool。

## 实施约束 (Implementation Notes)

落地顺序建议：

1. **审计公式调用点**：grep 所有 `MarginRatio` / `LiqPrice` / `Liquidatable` / `Available`，把强平和订单准入收敛到 `PoolRiskModel`。
2. **引入配置状态**：`PerpAccountConfig`、`PositionConfig`、`CustomerLeverageLimit` 进入 engine snapshot。
3. **补 journal/projection**：先让配置变更、margin adjustment 可恢复、可查。
4. **实现 P0 isolated 操作面**：SetLeverage、SetRiskID、AdjustIsolatedMargin、AutoAddMargin。
5. **实现 P1 cross admission**：先下单准入和查询，再接 liquidation plan；不要只允许开仓、不允许强平。
6. **替换 / 扩展强平扫描**：isolated 继续走 liq index，cross 走 account-pool health evaluation。
7. **BFF 对外暴露**：服务端能力和 history/projection 都闭环后，再打开 REST / WS 字段。

必须补的测试：

- mode switch 双向成功 / 失败矩阵：有活跃订单、资金不足、健康度不足、liquidation in-flight、重复 client_op_id。
- isolated add/remove margin 后 liq price / margin ratio 变化。
- auto-add 在 mark tick 中先于 liquidation arm 执行。
- leverage/risk_id 调整对 active orders 的拒绝。
- customer max leverage 的 min-chain 计算。
- cross pool 多 symbol health、订单预占、撤单释放、partial fill 后 requirement 重算。
- snapshot roundtrip：配置、margin adjustment 幂等水位、cross pool derived state 重建一致。

## 开放问题 (Open Questions)

- cross funding 结算是否直接进 `free_balance`，isolated funding 是否继续进 `position_margin`，需要和当前 funding journal 逐项对齐。
- isolated → cross 时是否允许用户保留一部分 margin 作为"手动锁定"，还是全部释放为 cross drawable；P1 建议全部释放，避免半 cross 语义。
- cross 强平的 `LiquidationPlan` 第一版按什么排序：亏损贡献、ADL score、名义值、还是逐 symbol 风险档优先。
- unified margin 的 collateral account 归 asset-service 扩展，还是新建 margin-account 服务；P2 前需要单独 ADR。
- portfolio margin 的风险参数版本如何和历史 mark / index / vol 数据一起归档，确保强平可复盘。

## 参考 (References)

- [ADR-0068: USDT 本位线性永续合约](./0068-usdt-linear-perp.md)
- [ADR-0070: perp 强平进阶](./0070-perp-liquidation-hardening.md)
- [ADR-0071: perp 分片化风控](./0071-perp-sharded-insurance-and-cross-shard-adl.md)
- [ADR-0072: 强平扫描改为强平价排序索引](./0072-perp-liquidation-scan-index.md)
- [ADR-0073: perp 强平托管仓位](./0073-perp-takeover-inventory-and-riskpool-settlement.md)
