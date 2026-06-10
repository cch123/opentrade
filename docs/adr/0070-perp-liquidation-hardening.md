# ADR-0070: perp 强平进阶 —— 阶梯风险限额 + 部分强平 + 兜底接管 + ADL 自动执行

- 状态: **Accepted / Implemented**（2026-05-30 起草；2026-05-30 落地 commit 66ba014 —— 阶梯风险限额（`pkg/perpstate/risk.go` RiskTier/MMRFunc）+ 部分强平（ReduceToTarget）+ 兜底接管（BackstopTakeover）+ ADL 自动执行（SelectAdlCandidates/ApplyAdlClose）四项全部实现；多 shard 保险基金 / 强平扫描索引 / 接管库存结算的后续展开见 ADR-0071 / 0072 / 0073。原从 [ADR-0068](./0068-usdt-linear-perp.md) 开放问题"阶梯杠杆 / 部分强平 / 完整 ADL 自动执行"提升为独立 ADR）
- 日期: 2026-05-30
- 决策者: xargin, Claude
- 相关 ADR: 0068（USDT 线性 perp，§6 SymbolConfig、§8 强平、§9 保险基金/ADL、§备选方案 C/D）、0041（Reservation 前置 IM）、0048（snapshot 绑 offset + 幂等水位）、0055（Match 直出 orderbook）、0056（SymbolConfig via MySQL）、0018（UserSequencer）、0031（cold-standby HA）

## 范围声明（先读这一段）

本 ADR 把 [ADR-0068](./0068-usdt-linear-perp.md) 落地的**强平 MVP**（单档 MMR + 整仓破产单 + ADL 仅告警）升级到生产级，**仍全部内置于 perp-counter、仍走 per-user sequencer**（重申 0068 §备选方案 C，不拆独立强平服务）。四块进阶：

1. **阶梯风险限额（risk tier）**：MMR / 最大杠杆随仓位名义值收紧的分档表。
2. **部分强平（reduce-to-safe）**：只平到 margin_ratio 回到安全线之上，而非一次性整仓接管。
3. **兜底接管（backstop）**：盘口无法成交破产单时的保证成交机制（对标参考实现 BURST / 做市商接管），让仓位**总能平仓**。
4. **ADL 自动执行**：把 0068 的"仅排队告警"升级为真正自动减对手盈利仓（对标参考实现 AGREEMENT）。

**仍不做**（保持 0068 边界，列各自后续）：cross / 统一保证金（强平的级联范围扩展，单独 ADR）、双向持仓 hedge 模式下的 OFFSET 自对冲（一向逐仓下每个 `(user,symbol)` 只有一个仓位，无自对冲对象，见 §备选方案 D）。

参考实现：`cryptofabric/unimargin-liquidate-server`（Java，Raft 副本的独立服务）有完整的 6 策略阶梯（Offset/Reduce/Force/Burst/Agreement/Collateral）+ 阶梯 MMR。本 ADR **取其策略逻辑、不取其架构**——它的 Raft + 独立服务恰恰是 0068 §C 论证过的"独立强平服务读仓位 = TOCTOU（time-of-check-to-time-of-use，检查与使用之间状态已变的竞态）"反例（见 §备选方案 E）。

OpenTrade 未上线，breaking change 直接改（同 [ADR-0057](./0057-asset-service-and-transfer-saga.md)），不写兼容层。

## 术语 (Glossary)

| 本 ADR 字段 | 含义 | 业界对标（仅供映射） |
|---|---|---|
| `risk_tier` | 风险档：名义值区间 → (MMR, 最大杠杆, 强平费率) | BN risk limit tier / Bybit risk limit / OKX 档位 |
| `tier_max_notional` | 该档名义值上限 | BN `notionalCap` |
| `maint_margin_ratio`(MMR) | 维持保证金率，本档生效值 | 三家 maintenance margin rate |
| `partial_liquidation` | 部分强平：只平回安全线所需的最小数量 | BN/Bybit 分档强平的"逐档减仓" |
| `target_margin_ratio` | 部分强平的目标保证金率（MMR + 缓冲），平到此即停 | — |
| `backstop`（兜底接管方） | 盘口无法成交时保证成交的系统账户；其库存风险由风险台/保险基金承接 | 参考实现 market-maker account / 三家"接管方" |
| `ADL`（自动减仓） | 保险基金不足以覆盖穿仓时，强制平掉对手方盈利仓补足 | 三家 auto-deleveraging |
| `adl_score` | ADL 排序键 = 未实现盈利率 × 有效杠杆 | BN ADL ranking |
| `liq_fee_rate` | 强平费率，平仓名义值的一部分进保险基金 | 三家清算费 |
| `bankruptcy_price` | 破产价，仓位权益归零的价 | Bybit `bankPrice` |
| `liq_price` | 强平价，margin_ratio 触及 MMR 的价（仍有权益） | BN `liquidationPrice` |

### 强平价 vs 破产价

`liq_price` 和 `bankruptcy_price` 解决的是两个不同问题，不能混用：

- **MMR / 强平价决定什么时候开始强平**：`margin_ratio = equity / notional`，当 `margin_ratio <= MMR` 时，仓位进入强平检查。逐仓下 `equity = position_margin + unrealized_pnl`；执行前仍必须在 owning user 的 sequencer 内复核，避免扫描到执行之间仓位已被成交或价格变化救回。
- **破产价决定亏损归属边界**：`bankruptcy_price` 是 `equity = 0` 的价格，表示用户这笔逐仓保证金已经完全亏光。强平成交优于破产价时，剩余权益进入保险基金；成交劣于破产价时，缺口由保险基金补。
- **保险基金不足才进入 ADL**：保险基金能覆盖穿仓缺口时，只做基金会计；基金为负且无法覆盖时，才触发 ADL 自动减少对手方盈利仓。这个顺序保证 ADL 是最后手段，而不是常规强平路径。

## 背景 (Context)

### 现状（已落地，2026-05-30 核实代码）

[ADR-0068 §8/§9](./0068-usdt-linear-perp.md) 的强平 MVP 已落地运行（`perp-counter/internal/service/liquidation.go` + `engine/engine.go` + `pkg/perpstate/`）：

1. **单档 MMR（标量）**：`LiquidatablePositions(symbol, s.cfg.MMR)`——全 symbol 一个 MMR 常量，与仓位大小无关。`pool.go` 的 `Liquidatable(marks, mmr)`、`margin.go` 的 `LiqPrice(mmr)` / `BankruptcyPrice()` 都吃标量 mmr。
2. **整仓破产单**：`beginLiquidation` 对整个 `cand.Size` 挂一张 `reduce_only` 单 @ `BankruptcyPrice`；`ApplyLiquidationFill` **逐笔**把释放权益路由进保险基金（partial-**fill** 已正确）。注意：是"部分成交正确"，不是"部分**强平**"——它要平的目标永远是整仓。
3. **ADL 仅告警**：`emitLiquidation` 里 `AdlQueued: s.eng.InsuranceFund(symbol).Sign() < 0`——基金为负只置位告警，**不自动减仓**（0068 §9 明定 MVP 边界）。
4. **无最后手段**：破产单挂在 Match 盘口依赖自然流动性；MVP 注释明确"若流动性不足只部分成交，剩余不自动再挂（在途 guard 持有，下个 tick 跳过）"——**盘口流动性不足 → 仓位无法平仓**。
5. **保险基金 per-symbol**：`engine.AddInsurance(symbol, delta)`。
6. **强平内置 + sequencer**：判定 lock-free 扫，执行在 `s.seq.do(user, ...)` 里带 TOCTOU 复核（`LiquidationCheck`）——0068 §C 的核心正确性保证，本 ADR 全程保持。

### 参考实现做了什么

`unimargin-liquidate-server`（已读源）的强平阶梯：

- **阶梯 MMR**：`MarginCalculator` 按合并仓位名义值查 `RiskLimitTier` 取 MMR；强平价/破产价公式都带这个分档 MMR。
- **REDUCE**：仓位跨档时只平到降一档（`LiquidateCalculator.getReducePositionSize`），分步逼近，部分强平。
- **FORCE**：整仓 IOC 限价单平掉（≈ opentrade 现状）。
- **BURST**：FORCE 失败且权益为负且配了做市商账户 → 系统造一对 no-match 单（被强平方 @破产价 / 做市商 @撮合价），不依赖盘口直接接管。
- **AGREEMENT**：无做市商时的 ADL——查 `getMaxProfitRatePosition` 找盈利最高的对手仓，按破产价强制对平，post-task 循环逼近。
- **OFFSET**：账户内多空对冲仓自对消（hedge / cross netting 才有意义）。
- 架构：独立服务 + JRaft 副本 + post-task 队列，事件驱动（消费 index-price / trade-event）+ 6s 调度兜底。

### 为什么现在做 + 哪些是真缺口

0068 的 MVP 在"保守 MMR + 小概率穿仓人工介入"假设下能运行，但有三个**会导致实际故障**的缺口：

- **缺口 A（盘口流动性不足 → 仓位无法平仓）**：现状破产单挂盘口，无法成交就无法平仓，下个 tick 跳过。行情急跌时盘口流动性本就稀薄，**流动性最差时恰恰无法成交**，亏损扩大、穿透保险基金。这是最危险的缺口。
- **缺口 B（整仓强平过度 + 冲击放大）**：现状一律整仓平。仓位只是轻微破 MM 时，整仓平仓冲击盘口既损害用户（本可只平一点）又放大市场冲击（自我强化的价格下行）。部分强平是行业标准做法。
- **缺口 C（ADL 不执行 → 穿仓无解）**：基金为负只告警，没有自动补足机制。真发生穿仓且无人工干预 = 系统资不抵债。
- 阶梯 MMR 是 A/B/C 的共同基础（破产价、强平触发、最大杠杆都依赖它）。

## 决策 (Decision)

### 1. 阶梯风险限额（risk tier）—— 基础

[ADR-0056](./0056-symbol-config-via-mysql.md) 的 SymbolConfig 给 perp symbol 增一张**有序档表**（替换 0068 §6 的单档 `maint_margin_ratio` / `max_leverage`，breaking、直接改）：

```
risk_tiers (按 tier_max_notional 升序): [
  { tier_max_notional, maint_margin_ratio, max_leverage, liq_fee_rate },
  ...
]
```

按**仓位当前名义值（at mark）**查档：名义值落在哪个区间，取该档 MMR / 最大杠杆。名义值越大，档越高，MMR 越大、最大杠杆越小（风险随规模收紧）。

落地最关键的一点——**保住 0068 不变量 #6（collateral pool 扩展点(extension point)）**：现状 `pool.Liquidatable(marks, mmr)` 吃标量，改成吃一个**MMR 解析函数**：

```go
// 由 pool 自己的名义值解析出本档 MMR；单档 = 返回常量的退化实现。
type MMRFunc func(notional dec.Decimal) dec.Decimal

func (cp CollateralPool) Liquidatable(marks map[string]dec.Decimal, mmrOf MMRFunc) bool
func (cp CollateralPool) Eval(marks map[string]dec.Decimal) Health   // 不变
```

- 强平判定 / 强平价仍**只走 pool 接口**，调用点不内联分档逻辑——cross 仍是"加一个 pool 实现"，不变量 #6 不破。
- `BankruptcyPrice()`（权益=0）与档无关，不变；`LiqPrice` 变成按档 MMR 求解（仍是观测估计，真实触发走 pool health）。
- 前置风控（0068 §4）：`PlaceOrder` 校验杠杆 ≤ 结果名义值所在档的 `max_leverage`（开仓做大 → 杠杆上限收紧）。

### 2. 部分强平（reduce-to-safe）

破 MM 时不再一律整仓平仓，先算**平回安全线所需的最小数量** `q*`：

```
目标：平掉 q* 后，pool.health(剩余仓位, mark).margin_ratio ≥ target_margin_ratio
target_margin_ratio = MMR(剩余名义值) + buffer   // buffer 可配, 给行情继续波动留余量
```

逐仓单仓下，平 `q*` 同时按比例释放 `position_margin`、按成交价结算已实现盈亏、扣 `liq_fee = liq_fee_rate × 平仓名义值` 进保险基金。注意 `q*` 求解要考虑**平仓→名义值下降→可能掉到更松的档**（MMR 变小），故按档分段求解（或逐档迭代逼近，参考实现 `getReducePositionSize` 同思路）。

- 部分强平单挂 **`liq_price`（仍有权益）** 而非破产价——优于破产价、对用户更友好，且仍能成交。
- **下个 mark tick 复查**：价格继续不利 → 再平一档；价格回来 → 停（用户保住残仓）。在途 guard（现成）保证一个 tick 内不重复挂；partial 完成后清 guard，下个 tick 若仍破 MM 再 arm。
- **退化为整仓**：当 `q*` ≥ 剩余仓位（权益已不足以通过部分平仓回到安全线），或剩余 < 最小 lot → 直接整仓走 §3 破产价接管。

即强平变成**单调逼近**：每 tick 平掉"恰好够回到安全线"的量，要么收敛回安全、要么逼近到整仓接管。

### 3. 兜底接管（backstop）—— 消除"盘口流动性不足就无法平仓"

强平单（§2 的 liq_price 部分单 / 整仓破产单）走**升级阶梯**，保证仓位**总能在有限步内平仓**：

```
(a) 挂 reduce_only @ liq_price 到 Match —— 正常情况盘口自然成交吸收
(b) N 个 tick 内未成交/仅部分成交 → 撤剩余, 重挂 @ bankruptcy_price（更激进, 多数情况这就成交了）
(c) 仍无法成交 且 mark 已越过 bankruptcy_price → backstop 接管：
      剩余数量在 perp-counter 内部按 bankruptcy_price 对平到 backstop 系统账户
      —— 不依赖 Match 盘口流动性, 保证闭合
```

backstop 接管（对标参考实现 BURST，但留在 perp-counter sequencer 内、不外部化）：

- backstop = 一个系统账户（`--backstop-account`）。被强平仓在内部按破产价平掉，**对侧库存记到 backstop 账户**（它现在持有一个反向仓），权益缺口由保险基金结算（成交优于破产价→盈余进基金；劣于→基金补）。
- backstop 账户的库存风险由**系统外（off-system）风险台 / 做市商对冲**（与参考实现把仓位转移给做市商账户同构）；perp-counter 只保证"用户仓位一定平仓 + 账目守恒 + 出告警"，不在系统内消化库存风险。
- backstop 接管是**保证闭合的最后一环**，永不"静默跳过"——这是相对 0068 现状"剩余不再挂"的根本修复。

> 为什么 (c) 走内部对平而非"系统账户去 Match 挂单"：再挂 Match 仍可能无法成交（盘口无流动性），无法保证闭合；内部对平在 sequencer 内一步完成、确定闭合、账目守恒，且天然无 TOCTOU。代价是 backstop 账户承接库存（需系统外（off-system）对冲），这是"保证闭合"的必要对价。

### 4. ADL 自动执行 —— 把告警位变成真减仓

当 backstop 接管后**保险基金仍会转负**（穿仓亏损 > 基金余额），触发 ADL（替换 0068 的 `adl_queued` 仅告警）：

```
deficit = -insurance_fund(symbol)   // 基金补完仍欠的额度（名义/数量）
1. 候选 = 该 symbol 上与穿仓仓位反向的盈利仓
2. 排序 adl_score = 未实现盈利率(at mark) × 有效杠杆   （0068 §9 定义）
3. 自高到低, 逐个对手仓按 bankruptcy_price 强制平掉, 直到补足 deficit 数量
4. 每个被 ADL 的对手仓: 在它自己的 sequencer 内平仓, 已实现盈亏按破产价入账, 发 PerpAdlEvent 通知该用户
```

- ADL 把穿透的亏损**确定性地分摊给盈利方**（赢家让出部分利润），这是基金不足以覆盖时维持系统偿付能力的最后手段，行业通用。
- **仅当 `deficit > 0`（基金确实不足以覆盖）才 ADL**；基金足以补足则不修改对手仓（不变量）。

#### 跨 sequencer 的 ADL hand-off（本 ADR 最敏感的并发点）

触发强平运行于**穿仓用户 A 的 sequencer** 内，但被 ADL 的对手仓属于**用户 B/C/...**——修改它们必须进入**它们各自的 sequencer**（0068 不变量 #1：一个用户的所有仓位 mutation 只在该用户 sequencer 内串行）。绝不能在 A 的 sequencer 里直接修改 B 的仓位（那就是 0068 §C 否定的 TOCTOU）。

机制：A 的 sequencer 内只**计算 deficit + 选出候选名单**（读快照），然后把每个 ADL 任务**投递到对应用户的 sequencer** 执行；每个任务在 B 的 sequencer 内**重新复核**该仓位仍反向、仍盈利、仍存在（TOCTOU 复核，对手仓可能已被自己的成交推进），再平。idempotent：每个对手仓按 `last_match_seq` / `adl_round` 守卫，重放/重启不重复减仓。

> 这是 perp-counter 内**唯一**跨 sequencer 的协调点。仍在同一进程、同一 snapshot 内（不引入跨服务读），用内部任务派发实现，而非外部强平服务——保持 0068 §C 的架构与正确性论证。

### 5. 策略选择（决策树，全在 perp-counter sequencer 内）

```
mark tick → pool health 破 MM ?
  否 → 安全, 等下个 tick
  是 → 进强平:
       ◇ 权益 > 用部分平回安全线所需 ?
         是 → 部分强平 q* @ liq_price（§2）, 下个 tick 复查
         否 → 整仓接管:
              挂 @ bankruptcy_price → (盘口无法成交) → backstop 内部对平（§3）
              ◇ 保险基金转负 (deficit>0) ?
                否 → 完成（盈余/小亏由基金吸收）
                是 → ADL 自动减对手盈利仓补 deficit（§4, 跨 sequencer hand-off）
```

对比参考实现 6 策略的映射：Reduce→§2 部分强平；Force→整仓破产单；Burst→§3 backstop；Agreement→§4 ADL；Collateral→决策树本身；**Offset→不做**（一向逐仓无自对冲对象，见 §备选方案 D）。

## 备选方案 (Alternatives Considered)

### A. 部分强平 vs 仅整仓（现状）

- 仅整仓（现状）：实现简单，但破 MM 即整仓平仓冲击盘口——损害用户（本可少平）、放大市场冲击（自我强化）、穿仓概率更高。
- 部分强平（选）：只平回安全线，用户保残仓、市场冲击小，行业标准。代价：求解 `q*`（含跨档）+ 多 tick 复查的复杂度，多笔强平单。
- **选部分强平**，整仓接管作为权益不足时的退化分支保留。

### B. 最后手段：依赖自然盘口（现状）vs 重挂逼近 vs backstop 内部对平 vs 外部做市商

- 自然盘口（现状）：无法成交就无法平仓，**最危险**，否决。
- 重挂逼近破产价（升级阶梯 b）：多数情况足够，但盘口无流动性时仍不闭合。
- backstop 内部对平（选，终局）：保证闭合、无 TOCTOU、账目守恒；代价是 backstop 账户承接库存、需系统外（off-system）对冲。
- 外部做市商账户去 Match 挂单（参考实现 BURST 形态）：把库存转移给做市商，但仍走 Match、仍可能无法成交、且引入外部依赖。
- **选 backstop 内部对平作终局保证**，重挂逼近作中间档；库存对冲在系统外（off-system）解决。

### C. ADL：自动执行（选）vs 仅告警（现状）

- 仅告警（现状）：穿仓时无自动补足，资不抵债依赖人工干预，无人工干预即风险敞口。
- 自动执行（选）：确定性分摊给盈利方，维持偿付能力。代价：跨 sequencer 协调 + 减赢家仓的用户体验/沟通成本。
- **选自动执行**，且只在基金确实不足以覆盖（deficit>0）时触发，最小化对盈利方的影响。

### D. OFFSET（账户内多空自对消）做不做

- 参考实现有 OFFSET，但它服务于 **hedge 模式（同 symbol 同时持多+空）/ cross netting**。opentrade 一向逐仓下每个 `(user,symbol)` 只有一个净仓位，**没有自对冲对象**，OFFSET 无意义。
- **不做**；待 hedge 模式 / cross margin（各自后续 ADR）再引入。

### E.【架构选型，重申】强平内置 perp-counter vs 独立强平服务（参考实现形态）

参考实现是**独立服务 + JRaft 副本**，opentrade 是否沿用？

- 独立强平服务（参考实现）：必须跨服务读仓位再动手——读到的仓位与动手时的仓位之间存在 in-flight 成交推进的窗口（**TOCTOU**），所以它**需要 Raft 来复制强平状态 + post-task 队列来重试**，复杂度大半来自"强平状态和仓位真值不在一处"。
- 内置 perp-counter（选，0068 §C 已定，本 ADR 重申）：强平判定/执行与仓位真值同处一个 sequencer，无跨服务读、无 TOCTOU；HA 依赖既有的 snapshot 绑 offset（[ADR-0048](./0048-snapshot-offset-atomicity.md)）+ 事务 producer fencing（ADR-0032）+ cold-standby（ADR-0031），**不需要再引入一套 Raft**。
- **重申内置**。本 ADR 新增的 §4 ADL 跨 sequencer hand-off 是唯一跨 user 的协调点，仍在**同进程同 snapshot 内**用内部任务派发解决，不外部化——正是因为内置，这个协调才不必跨服务、不必 Raft。参考实现的 Raft 复杂度恰好反证了独立强平服务的代价。

### F. 阶梯档表承载：SymbolConfig MySQL（选）vs 硬编码 vs 配置文件

- SymbolConfig MySQL（选）：复用 [ADR-0056](./0056-symbol-config-via-mysql.md) 既有权威 + 轻量 poll，可运维调档，与单档 MMR 同源。
- 硬编码 / 文件：MVP 实现简单但不可运维热调，否决。
- **选 SymbolConfig**，档表作为 perp symbol 的一组字段。

## 理由 (Rationale)

1. **消除最危险的"仓位无法平仓"缺口**：backstop 保证闭合，修复"行情急跌盘口流动性不足→流动性最差时无法成交"这个会真实造成系统性亏损的缺口。
2. **部分强平是风控标准做法**：少平、少冲击、少穿仓，且对用户友好——行业标准，不是 over-engineering。
3. **ADL 让穿仓有终局解**：确定性分摊优于资不抵债，且只在基金不足以覆盖时启用。
4. **阶梯 MMR 是基础且不破 collateral pool 扩展点(extension point)**：用 `MMRFunc` 穿过 pool 接口，保住 0068 不变量 #6，cross 仍是增量。
5. **架构不变、正确性论证不变**：全部内置 sequencer，重用现成 snapshot/HA；唯一的跨 user 协调（ADL）也在同进程内做，不退回 0068 §C 否决过的"独立强平服务 + Raft"。
6. **借鉴参考实现的策略、弃其架构**：策略阶梯（Reduce/Force/Burst/Agreement）逻辑成熟可借鉴，但它的 Raft + 独立服务是为"externalized 强平状态"付出的代价（开销），opentrade 内置后不必付出。

## 影响 (Consequences)

### 正面

- 强平在流动性不足的盘口下也**保证闭合**，消除最大的系统性亏损敞口。
- 部分强平降低用户损失与市场冲击，提升产品竞争力。
- 穿仓有 ADL 终局解，系统偿付能力可维持。
- 阶梯 MMR 让大仓位风险随规模收紧，符合主流风控。

### 负面 / 代价

- **复杂度与正确性敏感度显著上升**：部分强平 `q*` 求解（含跨档）、升级阶梯状态机、ADL 跨 sequencer 协调——都是直接涉及资金正确性 + 顺序/并发敏感，测试成本高（见 §测试要点）。
- **backstop 引入库存风险**：系统账户承接被接管仓位的反向库存，须有系统外（off-system）对冲机制覆盖，否则风险只是从用户转移到平台账面（须 runbook + 风险台流程）。
- **ADL 影响盈利用户体验**：被减仓的赢家会让出利润，须有清晰的事前规则披露 + 事后通知（PerpAdlEvent → push/站内信），否则是用户信任风险事件。
- **snapshot / 恢复逻辑的复杂度上升**：部分强平进度、升级阶梯计时、backstop 库存、ADL 在途队列都要进 snapshot（0068 不变量 #5 扩展），否则崩溃恢复不一致。

### 中性

- 仍单实例 / 小分片起步；ADL/backstop 是低频事件（极端行情才触发）。
- 仍 isolated margin；cross 的级联强平是另一 ADR，本 ADR 的 pool 接口已为其预留扩展点(extension point)。

## 实施约束 (Implementation Notes)

### 落地要点

- `pkg/perpstate`：`pool.Liquidatable` / 强平价改吃 `MMRFunc`；新增 `ReduceToTarget(pool, marks, mmrOf, targetRatio) → q*`（部分强平求解，含跨档分段，纯函数独立单测）。
- SymbolConfig（[ADR-0056](./0056-symbol-config-via-mysql.md)）：`risk_tiers` 档表字段；perp-counter poll 进内存，解析成 `MMRFunc` + 杠杆上限解析器。
- `perp-counter/internal/service/liquidation.go`：`beginLiquidation` 改为决策树（§5）——部分 vs 整仓；升级阶梯（liq_price→重挂 bankruptcy→backstop）；在途 guard 扩展为带"阶梯档位 + tick 计数"。
- `engine`：`PartialClose(user,symbol,q*,price)`（部分平 + 释放保证金 + 强平费进基金）；`BackstopTakeover(user,symbol,qty,bankruptcy)`（内部对平 + backstop 库存 + 保险结算）；ADL：`SelectAdlCandidates(symbol, side, deficit)` + `ApplyAdlClose(user,symbol,qty,bankruptcy,adlRound)`（在对手用户 sequencer 内，带幂等守卫）。
- ADL 跨 sequencer 派发：穿仓用户 sequencer 内选名单 → 投递任务到各对手用户 sequencer → 每任务 TOCTOU 复核后执行。
- 新事件：`PerpAdlEvent`（通知被减仓用户）；`PerpLiquidationEvent` 扩展 `tier` / `partial`(bool) / `backstop`(bool) 标识。
- snapshot：含部分强平进度、升级阶梯状态、backstop 库存、ADL 在途队列、各幂等水位（扩展 0068 不变量 #5）。

### 关键不变量（落地时逐条 audit，承接 0068 的 #1–#6）

7. **阶梯 MMR 走 pool 接口**：MMR 由 `MMRFunc(pool 名义值)` 解析，强平判定/价格只经 CollateralPool；任何调用点不得内联分档公式（保 0068 #6，落地要 grep 审计）。
8. **部分强平单调去险**：每次部分平**严格减小 |size|**、不增加风险；平价 ≥ bankruptcy_price；强平费 ≤ 本次释放权益；多 tick 收敛回安全或逼近整仓接管，不震荡。
9. **backstop 保证闭合**：整仓接管在有限步内必定闭合（盘口→重挂→内部对平作为最终保障），**永不静默跳过**；backstop 库存 + 保险 delta 必入账 + 出告警。
10. **ADL 仅在基金不足以覆盖时触发**（deficit>0）；每个被 ADL 对手仓在**其自身 sequencer 内**复核仍反向/盈利/存在后才平（TOCTOU）；按 `last_match_seq`/`adl_round` 幂等，重放不重复减仓。
11. **跨 user 的 ADL 协调仍在同进程同 snapshot 内**：穿仓用户 sequencer 只读快照选名单，实际 mutation 一律投递到对手用户 sequencer——绝不在一个 user 的 sequencer 里改另一个 user 的仓位（扩展 0068 #1）。

### 测试要点

- 单测：档边界解析（恰好 tier_max_notional）、`q*` 求解（含平仓掉档）、单调去险、强平费会计、backstop 内部对平账目守恒、保险基金跨 部分→整仓→backstop→ADL 的累计守恒、ADL 排序（adl_score 并列处理）、各幂等水位重放。
- 集成：开仓→mark 逐步下跌→**部分强平多次逼近**→价格继续跌→整仓 backstop 接管→基金转负→**ADL 减对手盈利仓**全链路；无流动性盘口下整仓仍闭合；perp-counter 在部分强平/ADL 中途宕机→snapshot+replay 恢复一致（在途阶梯 + ADL 队列不丢不重）。
- race：ADL 跨 sequencer 派发（`go test -race`）——构造穿仓用户与多个对手用户并发成交，验证对手仓只在自身 sequencer 内被复核+减仓、无数据竞争、无重复减仓。

### 附录：关键流程 ASCII 序列图

#### 图 1 — 阶梯部分强平（多 tick 单调逼近）

```
perp-pricing        perp-counter (user A sequencer 内, 无 TOCTOU)              Match
  │ mark tick ──────►│ pool.health(MMRFunc) 破 MM ?
  │                  │  是, 且权益够"部分平回安全线":
  │                  │   q* = ReduceToTarget(pool, mark, MMRFunc, target)   // 含掉档
  │                  │   撤该仓挂单(释放 IM) ──────────────────────────────►│
  │                  │   reduce_only q* @ liq_price ──────────────────────►│ 撮合
  │                  │◄──────────────── trade-event ──────────────────────┤
  │                  │   PartialClose: 释放比例保证金 + 已实现盈亏 + liq_fee→基金
  │ next mark tick ─►│ 复查: 回到安全线? ── 是 → 停(用户保残仓)
  │                  │                   └ 否 → 再平一档 (回到上面)
```

#### 图 2 — 整仓接管 + backstop 最终保障（保证闭合）

```
perp-pricing     perp-counter (user A sequencer)                 Match            backstop账户/保险基金
  │ mark tick ─►│ 权益不足以部分平回 → 整仓接管:
  │             │ reduce_only 整仓 @ bankruptcy_price ───────►│ 盘口可成交?
  │             │◄───── 部分成交 / N tick 未成交 ─────────────┤ (流动性不足)
  │             │ (升级) 撤剩余, 重挂更激进 ─────────────────►│ 仍无法成交
  │             │ backstop 接管剩余: 内部按 bankruptcy 对平 ─────────────────►│ backstop 记反向库存
  │             │ equity 结算: 优于破产价→盈余 ──────────────────────────────►│ 基金(+)
  │             │             劣于破产价→基金补 ◄──────────────────────────────┤ 基金(-)
  │             │ PerpLiquidationEvent{backstop=true} + 告警(库存需系统外（off-system）对冲)
  │             │ ◇ 基金转负(deficit>0)? → 是 → 进图 3 ADL
```

#### 图 3 — ADL 自动执行（跨 sequencer hand-off，唯一跨 user 协调点）

```
perp-counter
 user A sequencer (穿仓触发方)            内部任务派发              user B/C sequencer (对手盈利方)
   │ deficit = -insurance_fund            │                         │
   │ 候选 = 反向盈利仓, 按 adl_score 排序  │                         │
   │ 选出名单(只读快照, 不修改 B/C 仓位) ──►│ 逐个投递 ADL 任务 ─────►│ (进 B 的 sequencer)
   │                                       │                         │ TOCTOU 复核: 仍反向/盈利/存在?
   │                                       │                         │  是 → 按 bankruptcy 平掉对应量
   │                                       │                         │       已实现盈亏入账(赢家让出利润)
   │                                       │                         │       adl_round 幂等守卫
   │                                       │                         │  PerpAdlEvent → 通知 user B
   │                                       │◄─── deficit 补足? ──────┤
   │ (补足则停; 不足继续下一候选)          │                         │
```

## 开放问题 (Open Questions)

- **强平费率 / 清算者激励**：`liq_fee_rate` 分成（保险基金 vs 清算者）、是否引入外部清算者，MVP 全进基金。
- **backstop 库存的系统外（off-system）对冲流程**：风险台/做市商如何承接 backstop 账户库存，对冲 SLA 与告警阈值（运维 + 业务，非纯工程）。
- **ADL 通知与披露**：事前规则披露 + 事后 `PerpAdlEvent` 通知形态；并列 adl_score 的确定性打破规则。
- **socialized loss 基金 vs ADL 的边界**：是否引入独立的"社会化损失分摊"作为 ADL 之外的中间层。
- **cross / 统一保证金下的级联强平**：pool 接口已预留扩展点(extension point)，级联选择 + 跨 symbol 风险单元是单独 ADR。
- **hedge 模式 + OFFSET**：双向持仓引入后才有自对冲，单独评估。
- **阶梯档表的运维面**：admin-gateway 是否要做 perp 档表的可视化编辑 + 灰度（与 0068 开放问题"perp 风控参数运维面"合并）。

## 参考 (References)

- [ADR-0068](./0068-usdt-linear-perp.md) — USDT 线性 perp（§6 SymbolConfig、§8 强平、§9 ADL、§备选方案 C/D）；本 ADR 是其强平侧 MVP→生产级的展开，重申 §C 内置架构。
- [ADR-0048](./0048-snapshot-offset-atomicity.md) — snapshot 绑 offset + 幂等水位（部分强平进度 / ADL 队列 / backstop 库存全套复用并扩展）。
- [ADR-0056](./0056-symbol-config-via-mysql.md) — SymbolConfig（risk_tiers 档表承载）。
- [ADR-0041](./0041-counter-reservations.md) — Reservation（前置 IM 占用，部分强平释放复用）。
- 参考实现：`cryptofabric/unimargin-liquidate-server`（Java，Raft 独立服务）—— 取其 Reduce/Force/Burst/Agreement 策略逻辑，弃其 Raft + 独立服务架构（其 Raft 复杂度正是 0068 §C 否定的 externalized 强平状态的代价）。
- 实现位置（动工时涉及）：`pkg/perpstate/`（MMRFunc + ReduceToTarget）、`perp-counter/internal/{service,engine}/`（决策树 + backstop + ADL 派发）、`api/event/perp_journal.proto`（PerpAdlEvent + Liquidation 字段扩展）、SymbolConfig（risk_tiers）、`asset`/admin（档表运维面）。
