# ADR-0079: perp 手续费与财务记账

- 状态: **Accepted**（2026-05-31 起草；2026-06-10 修订并接受——修订原因见文末"修订记录"）
- 日期: 2026-05-31（修订 2026-06-10）
- 决策者: xargin, Codex
- 相关 ADR: 0057（asset-service + transfer saga）、0068（perp settlement）、0070（强平加固 / liq_fee_rate）、0073（RiskPool settlement）、0074（保证金模式 / 客户风险上限）、0075（SymbolConfig 产品化）、0076（合约产品扩展边界）

## 范围声明（先读这一段）

本 ADR 定义 perp 的交易手续费与财务流水模型：

- maker/taker 费率：来源、解析时机、确定性要求
- 用户级费率覆盖（admin 设置）；symbol 级费率随 ADR-0075 版本化配置
- 负 maker 费率（rebate，返佣）的数据模型与启用前置条件
- fee_rule_id 与逐笔审计
- 平台手续费账户（系统侧对手账）
- 开仓手续费预占（order cost）与平仓手续费的资金来源
- fee_deficit（手续费坏账）路径
- 累计交易手续费 / 资金费 / 已实现盈亏统计（trade-dump 投影 + history 查询）

本 ADR 不处理：税务、发票、返佣分润、多级代理、用户分组（fee tier / VIP 等级体系）、
手续费余额向 asset-service 的周期性划转（见 §2 非目标）。

强平 / ADL / backstop 接管的费用经济学不在本 ADR 内——那是 liq_fee_rate → 保险基金的
路径（ADR-0070 / ADR-0073），与交易手续费是两条独立资金流（见 §4）。

## 术语表 (Glossary)

| 本文用语 | 行业对应 | 说明 |
| --- | --- | --- |
| maker / taker | BN/Bybit maker/taker | 挂单成交方 / 吃单成交方 |
| rebate（返佣） | BN/Bybit maker rebate | 负 maker 费率时平台付给用户的钱 |
| order cost（下单成本） | Bybit order cost | 下单时预占的总额 = 初始保证金 + 手续费缓冲 |
| fee buffer（手续费缓冲） | Bybit fee to open | 按 taker 费率预估的开仓手续费预占 |
| fee_deficit（手续费坏账） | — | 结算时无法从用户侧收齐的手续费缺口 |
| STP | self-trade prevention，自成交防护 | Match 侧拒绝同一用户自成交（已实现，默认关闭） |
| 结算资产 (settle asset) | BN marginAsset / Bybit settleCoin | 线性合约 = quote 资产（当前恒为 USDT） |
| notional（名义价值） | BN/Bybit notional | price × qty |
| 流水投影 (projection) | — | trade-dump 把 journal 事件物化为 MySQL 行（事件溯源的物化视图） |

## 背景 (Context)

当前 perp settlement 中 fee 恒为零：`settleLeg` 构造 `perpstate.Fill{Fee: zero}`，
`PerpSettlementEvent.fee` 字段存在但永远是 "0"。SymbolConfig 已携带
`FeeParams{maker_fee_rate, taker_fee_rate, fee_rule_id}`（ADR-0075）但无任何消费方。
生产合约系统需要：

- 费率按 symbol（版本化配置）与用户（admin 覆盖）解析，且每笔成交可审计到费率来源。
- maker 可能是负费率（rebate），数据模型必须支持，产品可先配置为 0。
- 费用必须有平台侧对手账，不能只在用户钱包上单边扣减。
- 开仓预占若只含初始保证金，成交后扣手续费可能把 Available 扣成负数（现状
  `routeCashLocked` 对 fee 无下限保护）。
- history 要能回答用户累计手续费、资金费、已实现盈亏。

### 既有架构约束（决定了本 ADR 的形态）

1. **回放确定性**：perp-counter 的恢复 = 快照 + 从绑定 offset 重放输入事件（ADR-0048）。
   重放期间重新执行结算逻辑，因此**结算路径读到的任何输入都必须是确定性的**——
   不能依赖墙钟（catalog Active 视图按 now() 选版本）或可被 admin 随时改写的共享状态。
2. **结算不能失败**：成交已在 Match 发生，结算侧任何 fail-closed 都会制造
   poison message（毒消息，使消费者确定性崩溃的消息）。结算路径只能降级，不能拒绝。
3. **journal oneof**：`PerpJournalEvent.payload` 是 oneof，一个事件只能携带一种载荷，
   不存在"两种载荷同一事件原子发出"的表达方式。
4. **可平仓性**：reduce-only / 强平单今天预占为零（破产用户也无从预占），
   "平仓降低风险，不得因资金不足被拒"是既有不变量。

## 决策 (Decision)

### 1. 费率解析在订单准入时完成，结果钉进订单（admission-time pinning）

费率有两个来源，固定优先级（无 priority 字段，无独立 FeeRule 目录服务）：

```text
用户覆盖 (user+symbol) > 用户覆盖 (user 全局) > SymbolConfig.FeeParams (版本化)
```

- **symbol 级**：`FeeParams{maker_fee_rate, taker_fee_rate, fee_rule_id}` 已在
  ADR-0075 的版本化目录里。生效时间窗口复用目录的 `effective_from_ms` 版本机制，
  不再引入第二套窗口字段。`fee_rule_id` 为空时合成 `sym:<symbol>@v<config_version>`。
- **用户级**：admin 面状态，完全镜像 ADR-0074 §10 客户杠杆上限的机制——
  engine 内存状态 + 快照 + journal 审计事件 + RPC（`SetCustomerFeeRate` /
  `ListCustomerFeeRates`）。symbol = "" 为用户全局行，symbol 行覆盖全局行；
  `fee_rule_id` 置空 = 删除该行。

**解析时机**：`PlaceOrder` 准入时（与 ADR-0075 admission 同一个 catalog 捕获视图，
按 admission 返回的 config_version 经 `catalog.At(symbol, version)` 取不可变内容,
无 TOCTOU），把 `fee_rule_id / maker_fee_rate / taker_fee_rate / fee_asset`
四元组钉进 Order 记录（随服务快照持久化）。结算时只做两件事：按本腿是
maker 还是 taker 选费率，应用确定性抑制规则（§5）。

```text
ResolveFeeAtAdmission(user, symbol, configView) -> FeePin {
  fee_rule_id      // 审计标识
  maker_fee_rate   // 有符号；< 0 = rebate
  taker_fee_rate   // >= 0
  fee_asset        // 结算资产（spec.settle_asset，当前 USDT）
}
```

为什么不在结算时解析（对 05-31 初稿的修订）：结算时解析读 catalog 的
Active 视图（按墙钟选版本）和 admin 覆盖的实时状态，崩溃重放时这两者都可能
已变化 → 同一笔成交重放出不同的手续费 → 钱包状态与已发 journal 永久分叉。
钉进订单后，订单在快照里，重放读到的费率与首次执行一致。代价是
**费率变更只影响新订单，不影响在途订单**——与 Bybit（执行时费率）不同，
是确定性优先的明确取舍；运营上如需立即生效可走撤单重挂的产品流程。

费率约束（`perpcfg.ValidateConfig` 与 SetCustomerFeeRate 共同校验）：

- `taker_fee_rate ∈ [0, 1)`；`maker_fee_rate ∈ (-1, 1)` 且 `maker_fee_rate ≤ taker_fee_rate`
  （保证 taker 费率是缓冲上界，§4）。
- 每个风险档位 `maintenance_margin_ratio ≥ taker_fee_rate`（新增发布期校验；
  建议 ≥ 2×）。这是 §4"平仓不预占"成立的核心不变量：任何健康仓位平仓释放的
  权益 ≥ MM ≥ 平仓手续费，坏账只可能出现在跳空穿仓场景。
- 负 maker 费率受部署级开关 `allow_negative_maker_fee`（默认 false）约束：
  开关关闭时准入钉价把负 maker 费率按 0 处理（确定性降级，启动日志提示）。
  开关的开启前置条件见 §5。

系统账户豁免：backstop 账户（ADR-0070 §BackstopAccount）的订单钉零费率，
`fee_rule_id = "system-exempt"`。

legacy 模式（catalog 未启用）：无结算资产与费率来源，费率恒为零——与现状一致，
存量测试不受影响。

### 2. 手续费进入平台账户（engine 级系统余额）

资金方向：

```text
正手续费:  user 钱包 (Available / 预占缓冲) ──→ platform_fee[asset]
负 rebate: platform_fee[asset]            ──→ user 钱包 (Available)
```

`platform_fee` 是 engine 内按结算资产记账的**净额**系统余额（实现上与保险基金
`insurance map` 同构：内存 map + 快照字段），每一笔变动都内嵌在结算 journal
事件里（§3），projection 可按符号拆出毛收费 / 毛返佣两个方向。审计不变量：

```text
engine.platform_fee[asset] == SUM(perp_settlements.fee - perp_settlements.fee_deficit)
                              （按 fee_asset 分组，对全量行）
```

**非目标（对 05-31 初稿的修订）**：手续费不在热路径上走 asset-service transfer
saga——perp 用户钱包本身就不在 asset-service 里，平台账户跟随同一边界。
累计余额向 asset-service 资金账户的周期性划转（sweep）是后续 admin 操作，
独立排期。同理，**不在每条 journal 事件里盖平台账户余额快照**：平台余额是
跨用户共享聚合，逐事件盖章会把跨用户处理顺序（非确定性）烤进 per-user 事件流；
审计走上面的 SUM 不变量。

### 3. fee 字段并入 PerpSettlementEvent（一笔成交 = 一个事件 = 一行投影）

05-31 初稿设想独立 `PerpFeeEvent` 并"与 settlement 同一 PerpJournalEvent 原子
发出"——oneof 下不可表达；拆成两个相邻事件又引入跨事件原子性论证负担。修订为：
**手续费明细直接作为 `PerpSettlementEvent` 的字段**，原子性由构造保证：

```text
PerpSettlementEvent {
  ... 既有字段 ...
  fee = 13               // 语义升级为有符号：>0 用户支付，<0 用户收到 rebate
  liquidity_role         // MAKER / TAKER
  fee_rule_id
  fee_rate               // 实际应用的有符号费率
  fee_asset              // USDT
  fee_deficit            // 未能收齐的部分（§4），正常为 "0"
  rebate_suppressed      // rebate 被确定性抑制（§5：自成交 / 开关关闭）
  wallet_after           // 本次结算后 Available（单用户维度，重放确定）
}
```

投影侧 `perp_settlements` 增加同名列——成交流水表就是手续费流水表
（fee ledger），不再建独立 `perp_fee_logs` 表（一笔成交一行，独立表是纯粹的
写放大；未来若出现非成交类费用再引入独立事件与表）。push 服务经 protojson
透传，新字段自动到达用户私有流，无需改动。

用户级费率覆盖的变更走新 journal 载荷 `PerpCustomerFeeEvent`（镜像
`PerpCustomerRiskLimitEvent`），投影到追加式审计表 `perp_customer_fee_rules`。

### 4. 开仓手续费预占（order cost）；平仓不预占

```text
开仓/加仓订单 (非 reduce-only):
  order cost = IM + fee_buffer
  IM         = imPrice × qty / leverage          （现状不变）
  fee_buffer = imPrice × qty × pinned_taker_rate （钉单时的 taker 费率——最坏情况角色；
                                                  imPrice 与 IM 同参考价，含 ADR-0083
                                                  滑点上浮）

reduce-only / 平仓 / 强平单:
  预占 = 0   （维持现状；可平仓性不变量优先）
```

预占与释放的记账：

- `Order.ReservedIM` 与 `Order.ReservedFee` 分开跟踪；钱包侧仍是一个
  Reserved / CrossReserved 桶（一次 `Reserve(im + feeBuf)` 原子判定充足性）。
- 成交时实际手续费优先从本订单剩余 `ReservedFee` 划扣（直接转入
  platform_fee），不足部分从 Available 补；maker 成交实际费率低于缓冲费率时
  多余缓冲**留存到订单终态一并释放**（多退少补；逐笔按比例退是不必要的精度,
  终态释放在 cancel/reject/expire/FILLED 路径已有统一出口 `releaseRemainingIM`）。
- 市价单成交价可能高于 imPrice（缓冲参考价），差额部分的手续费从 Available 补——
  这是有界残差（受 ADR-0083 滑点收口约束），不是坏账路径。

**平仓手续费的资金来源**（对 05-31 初稿"平仓预占 close_fee_buffer"的修订——
那会让 Available = 0 的用户无法平仓，破产用户的强平单更无从预占，直接违反
可平仓性不变量）：

```text
逐仓 (isolated) 平仓成交:
  Available += margin_released + realized_pnl      （现状）
  fee 从 Available 扣，下限钳到 0:
    collected = min(fee, max(Available, 0))
    fee_deficit = fee - collected                   （journal 字段，正常为 0）
  ─ §1 的 MMR ≥ taker_rate 不变量保证：健康仓位 margin_released ≥ MM ≥ fee，
    deficit 只在跳空穿仓 + 用户主动平仓竞速强平的窗口内出现。

全仓 (cross) 平仓成交:
  fee 与 realized_pnl 同等地位结算进 Available（可短暂为负，由账户池权益背书；
  池层清算是偿付性守门人，ADR-0074 §4）。不引入逐仓式钳位——全仓的
  "钱包"本就是池的现金分量。
```

fee_deficit 必须 journal（§3 字段）、可被 history 查询；它是平台收入的坏账记录，
**不**从用户钱包透支，也**不**递归触发风险流程（金额上界 ≈ taker_rate × 跳空
名义价值，被保险基金路径的同一场景覆盖）。

**强平 / ADL / backstop 成交不收交易手续费**：它们的费用经济学是
`liq_fee_rate → 保险基金`（ADR-0070 全额接管 / ADR-0070 部分强平 /
ADR-0073 RiskPool），与本 ADR 的 maker/taker 费互斥。同一笔 Trade 里
破产单的对手腿是普通用户订单，正常走本 ADR 收费（两腿独立结算，天然成立）。

结算时序（修订后全链路）：

```text
 用户                BFF        perp-counter                    Match
  │ place order       │              │                            │
  ├──────────────────>├─────────────>│ admission:                 │
  │                   │              │  catalog 视图捕获           │
  │                   │              │  FeePin 钉价(§1)            │
  │                   │              │  Reserve(IM + feeBuf)      │
  │                   │              ├───── OrderEvent ──────────>│
  │                   │              │                            │ 撮合
  │                   │              │<──── Trade (两腿) ──────────┤
  │                   │              │ settleLeg (per-user 串行):  │
  │                   │              │  role = maker|taker        │
  │                   │              │  fee = rate(FeePin,role)   │
  │                   │              │        × price × qty       │
  │                   │              │  抑制规则(§5, 确定性)        │
  │                   │              │  engine 原子段:             │
  │                   │              │   仓位代数 + routeCash      │
  │                   │              │   fee: ReservedFee→平台账户  │
  │                   │              │        不足从 Available     │
  │                   │              │        isolated 钳 0→deficit│
  │                   │              │   platform_fee[asset] 更新  │
  │                   │              │                            │
  │                   │              ├─ PerpJournalEvent          │
  │                   │              │   (Settlement + fee 字段)   │
  │                   │              ▼                            │
  │                trade-dump ── 单事务: perp_settlements 幂等插入   │
  │                   │          + 重算被触达的 (user,symbol,日) 聚合 │
  │                history ──── 成交/费用流水 + 日统计查询            │
```

### 5. 负 maker rebate：结算期只做确定性动作，经济护栏放到配置/监控面

修订原则（对 05-31 初稿的修订）：结算路径**不能失败**（约束 #2）且**必须重放
确定**（约束 #1）。初稿的"rebate account 配额不足时 fail-closed 或降级为 0"
两头都违反：fail-closed 制造毒消息；共享配额的扣减结果依赖跨用户处理顺序，
重放时哪个用户被降级是不确定的。因此：

**结算期（确定性规则，仅依赖本事件载荷与订单钉价）**：

- 自成交抑制：`Trade.maker_user_id == taker_user_id` 时 maker 腿 rebate 降为 0，
  `rebate_suppressed = true`（taker 腿正费率照常收取）。这与 Match 侧 STP 是
  纵深防御关系，且不依赖 Match 配置，重放确定。
- 系统账户豁免（§1）。
- 除此之外 rebate **无条件支付**：`Available += |fee|`，platform_fee 净额相应
  减少（可为负——它是平台负债记录，不是支付闸门）。

**配置/监控面（异步护栏，不进结算路径）**：

- `allow_negative_maker_fee` 部署开关（§1）：开启前置条件为 Match 已启用 STP
  （STPRejectTaker 已实现、默认 STPNone——开启是 Match 部署参数，不是本 ADR
  代码变更）。perp-counter 无法跨服务验证 Match 配置，故以部署开关 + 上线
  checklist 承载该前置条件。
- rebate 支出监控：基于 `perp_user_fee_stats_daily.rebate` 聚合，超出产品预算
  （rebate_cap / 单用户日上限）时由 admin 发布新 SymbolConfig 版本把 maker
  费率归零或移除用户覆盖——版本化、可审计、确定性（只影响新订单）。
- beneficial owner / 客户组维度的自成交识别：系统尚无用户分组概念，作为
  真实 rebate 产品上线（费率 < 0 配置进生产）的前置条件挂在 roadmap，
  不阻塞本 ADR 的数据模型落地。

### 6. 统计由 trade-dump 投影产生；幂等靠"同事务重算被触达聚合键"

perp-counter 不维护任何长期累计统计（保持事件完整、顺序、幂等即可）。
trade-dump 在**同一个 MySQL 事务**里完成：

1. 基础流水幂等插入（现状机制：PK `(user_id, perp_seq_id)` + INSERT 幂等）。
2. 收集本批次触达的聚合键 `(user_id, symbol, stat_date)`（stat_date 按 UTC 日切）。
3. 对每个触达键 `INSERT ... SELECT 重算 ... ON DUPLICATE KEY UPDATE`——
   从去重后的基础流水**整键重算**，而不是增量累加。重算是基础表的纯函数，
   Kafka 重投递 / 批次重放天然幂等，不会双计。

统计表（均 PK `(user_id, symbol, stat_date)`）：

- `perp_user_fee_stats_daily`：`trading_fee`（正向收费和）、`rebate`（返佣和）、
  `fee_deficit` 和，来源 `perp_settlements` 费用列。
- `perp_funding_stats_daily`：`funding_paid` / `funding_received`，来源 `perp_funding`。
- `perp_realized_pnl_stats_daily`：`realized_pnl` 和（交易平仓口径：
  settlements + liquidations + ADL 三表；资金费不计入——资金费有自己的日表，
  与 Bybit closed-PnL 口径对齐）。

history 服务新增查询（沿用 keyset 游标分页惯例）：

- `ListPerpSettlements`：逐笔成交/费用流水（此前不存在成交历史端点，本 ADR 补齐）。
- `ListPerpDailyStats`：按日返回上述三张统计表的拼合行。

## 备选方案 (Alternatives Considered)

### A. fee 只作为 settlement 字段，不单独入账

实现简单，但平台收入、rebate、审计都不完整。否决。（修订后注意区分：字段确实
并入了 settlement **事件**，但平台对手账、deficit、统计均独立成立——A 否决的
是"没有对手账"，不是"没有独立事件"。）

### B. fee rule 在 BFF 计算

BFF 不在成交路径上，无法按真实 maker/taker 和成交价格计算。否决。

### C. 负 maker rebate 先不支持

能简化资金方向，但会阻塞做市商产品。决定从数据模型上支持（有符号 fee 贯穿
journal / 投影 / 统计），产品可先配置为 0。维持原决策。

### D. 结算时解析费率（05-31 初稿方案）

行业惯例（费率变更立即作用于在途订单），但与快照+重放的恢复架构冲突
（重放读到新费率 → 钱包分叉）。要在结算时解析又保确定性，需要把费率变更
做成与成交流同序的事件（进 trade-event 流或独立有序流）——为费率这种低频
变更引入全新的有序分发通道，复杂度不成比例。否决，改为准入钉价（§1）。

### E. 平仓预占 close_fee_buffer（05-31 初稿方案）

可消灭平仓手续费坏账，但 Available = 0 的用户将无法平仓（违反可平仓性），
破产用户的强平单根本无钱可占。Bybit 的等价物（order cost 含 fee to close）
是在**开仓时**预占并折入仓位保证金——那需要给每个仓位增加跨成交的费用准备金
状态（逐仓折入保证金会影响破产价，全仓无保证金桶无处可折）。鉴于
MMR ≥ taker_rate 不变量已把坏账压缩到跳空场景、且坏账有 journal 兜底，
否决预占方案，接受 fee_deficit 为显式记录的残余风险。

## 影响 (Consequences)

### 正面

- 手续费可审计（逐笔 rule id + 费率 + deficit）、可配置（版本化 + 用户覆盖）、
  可复盘（重放确定）。
- rebate 与平台账户有明确资金流与审计不变量（SUM 对账）。
- 用户统计不污染高频 engine；统计幂等不依赖 Kafka 投递语义。
- 开仓预占含手续费缓冲后，正常路径上 fee 永不把 Available 扣负。

### 负面 / 代价

- 费率变更不影响在途订单（确定性的代价；运营可撤单重挂）。
- order cost 略增（fee buffer），可用余额展示口径变化（Bybit 同型）。
- 平仓手续费在跳空场景存在显式坏账（fee_deficit），平台承担。
- Order / 快照 / journal / 投影各加一组字段（一次性 schema 成本，未上线无迁移负担）。

## 实施约束 (Implementation Notes)

- 每笔 settlement 必须记录 `fee_rule_id` 与实际 `fee_rate`，不能只记录其一。
- 费率钉价必须与 ADR-0075 admission 用同一捕获视图（按版本取不可变内容），
  禁止二次 Active() 读取（TOCTOU）。
- `perpstate.Fill.Fee / FillResult.Fee` 保留为**强平费载体**（部分强平 /
  backstop 路径在用），普通成交腿恒传 0；交易手续费是 engine 层独立步骤
  （与仓位代数、routeCash 同一互斥段内原子完成），不复用该字段。
- 结算路径禁止失败、禁止读墙钟、禁止读非快照状态；rebate 无条件支付，
  抑制只允许确定性规则（自成交 / 系统账户 / 钉价时开关）。
- 发布期校验新增：`maker_fee_rate ≤ taker_fee_rate`；
  每档 `maintenance_margin_ratio ≥ taker_fee_rate`。
- engine 快照新增 `platform_fee`、用户费率覆盖；服务快照 OrderSnap 新增
  `reserved_fee` 与 FeePin 四元组。两者都参与 Capture 屏障的一致性图像。
- 单测覆盖：maker/taker 选择、用户覆盖优先级、负 rebate 资金方向、
  自成交 rebate 抑制（taker 照常收费）、fee buffer 多退少补（terminal 释放）、
  isolated fee_deficit 钳位、cross 负 Available 结算、强平腿不收费而对手腿收费、
  平台账户 SUM 不变量、快照往返含费用状态、投影幂等（重放不双计统计）。

## 修订记录

**2026-06-10**（实现前评审，发现初稿五处与既有架构冲突，修订后状态 → Accepted）：

1. §1 结算时 `ResolveFee(ts, configVersion)` → 改为准入时钉价进订单。
   原因：结算时解析依赖墙钟与实时 admin 状态，破坏快照+重放的确定性（约束 #1）。
2. §1 FeeRule 目录（priority + 独立生效窗口 + user group）→ 固定优先级两级解析。
   原因：生效窗口与 ADR-0075 版本机制重复；priority 是配置事故面；user group
   在系统中不存在，挂为 rebate 产品化前置。
3. §3 独立 PerpFeeEvent"与 settlement 同一事件原子发出" → fee 字段并入
   PerpSettlementEvent。原因：oneof 不可表达初稿措辞；并入后原子性由构造保证，
   独立 `perp_fee_logs` 表为纯写放大，一并取消。
4. §4 reduce-only / 强平预占 `close_fee_buffer` → 平仓不预占，新增
   `MMR ≥ taker_fee_rate` 发布校验 + `fee_deficit` 显式坏账路径。
   原因：平仓预占违反可平仓性（零余额无法平仓、破产单无从预占）。
5. §5 结算时 rebate 配额 fail-closed / 降级 → rebate 无条件支付 +
   配置/监控面护栏。原因：结算 fail-closed 制造毒消息；共享配额门控的结果
   依赖跨用户处理顺序，重放不确定。
6. §2 追加非目标：平台账户不走 asset-service 热路径、journal 不逐事件盖
   平台余额快照（共享聚合会把跨用户顺序烤进 per-user 事件流）。
7. §6 统计幂等机制明确为"同事务重算被触达聚合键"（初稿未指定，朴素增量
   在 Kafka 重投递下双计）。

## 参考 (References)

- [ADR-0068: USDT 本位线性永续合约](./0068-usdt-linear-perp.md)
- [ADR-0070: perp 强平加固](./0070-perp-liquidation-hardening.md)
- [ADR-0073: 接管库存与 RiskPool settlement](./0073-perp-takeover-inventory-and-riskpool-settlement.md)
- [ADR-0074: 保证金模式与客户风险上限](./0074-perp-account-margin-modes.md)
- [ADR-0075: perp 合约 SymbolConfig 产品化](./0075-perp-symbol-config-productization.md)
- Bybit: Order Cost (USDT Contract) — 初始保证金 + 双向手续费的下单成本口径
- Confluent: Exactly-once semantics & idempotent consumer patterns —
  投影幂等的"事务内去重+重算"模式
