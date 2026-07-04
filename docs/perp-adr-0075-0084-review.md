# Perp ADR 批次评审(0075–0084 + 0082/0084)

- 评审日期: 2026-05-31
- 评审人: xargin, Claude
- 状态: 评审意见(已落入 ADR 修订，2026-05-31)
- 方法: 通读 10 篇 Proposed ADR,并对照现有代码(`../pkg/etcdcfg/etcdcfg.go`、`../api/event/common.proto`、`../pkg/perpstate`)验证关键判断

## 评审范围

本次评审覆盖以下未提交 ADR:

| ADR | 主题 |
| --- | --- |
| [0075](adr/0075-perp-symbol-config-productization.md) | perp 合约 SymbolConfig 产品化(versioned config) |
| [0076](adr/0076-perp-contract-product-expansion.md) | 合约品类扩展:linear dated futures / settlement,inverse 延后 |
| [0077](adr/0077-perp-position-mode-hedge-both-side.md) | 持仓模式 one-way / hedge both-side |
| [0078](adr/0078-perp-order-position-product-api.md) | 订单与持仓产品 API |
| [0079](adr/0079-perp-fee-accounting.md) | 手续费与财务记账 |
| [0080](adr/0080-perp-admission-risk-price-protection.md) | 订单准入风控与价格保护 |
| [0081](adr/0081-perp-reduce-only-settlement-hardening.md) | reduce_only 结算时硬约束 |
| [0082](adr/0082-match-counter-benchmark-methodology.md) | Match / Counter benchmark 方法 |
| [0083](adr/0083-match-native-protected-market-order.md) | Match 原生 protected market order |
| [0084](adr/0084-private-push-merge-strategy.md) | match→user 私有推送合并策略 |

术语缩写(首次出现即释义):TOCTOU(time-of-check-to-time-of-use,检查与使用之间状态已变的竞态);OCC(optimistic concurrency control,乐观并发控制);STP(self-trade prevention,自成交防护);MMR(maintenance margin ratio,维持保证金率);IM(initial margin,初始保证金);OI(open interest,未平仓量);HFT(high-frequency trading,高频交易)。

## 总体评价

文档质量整体不错:范围声明清晰、备选方案有否决理由、引用链完整、普遍带 fail-closed 和单测约束。但存在 **3 个系统性设计问题贯穿多篇**,其中两个属于"用顺序心智写并发"和"写下不变量但未验证代码兑现"这两类高风险。建议在动手实现前先收敛系统性问题(S1–S3)。

## 结论速览

| ID | 严重度 | 所属 | 问题一句话 |
| --- | --- | --- | --- |
| S1 | 高 | 跨 ADR / 0075 / 0076 | 版本 + 状态机制三套并存,0075 重复发明 ADR-0053 已有的 `PrecisionVersion`/`ScheduledChange` |
| S2 | 高 | 跨 ADR / 0075 | 跨服务 config 传播窗口内的版本 skew 未定义(0053 其实已给出解法) |
| S3 | 高 | 0076 / 0078 / 0081 | counter→Match 异步 cancel 与在途 fill 之间缺 barrier,同一 race 重复出现 |
| A1 | 高/中 | 0081 | reduce_only 结算 race 三分支按 reservation 状态描述,中间支语义自相矛盾 |
| A2 | 中 | 0080 × 0081 | "是否改用户指令" 哲学不一致:0080 选 REJECT,0081 静默 clamp 数量 |
| A3 | 中 | 0077 | mode 切换"无单"闸门漏了 trigger 服务里的条件单(TP/SL) |
| A4 | 中 | 0077 | cross + hedge 下 long/short 两腿保证金是否相抵未定义 |
| A5 | 中 | 0078 | block trade 是双边成交,需原子进两个(可能跨 shard)user sequencer,协调机制缺失 |
| A6 | 中 | 0079 | 负 maker rebate + 缺 STP = 刷返佣套利洞 |
| A7 | 中 | 0079 | close fee 超 buffer 时"从 margin 继续扣"可能扣穿成负余额,与 0076 冲突 |
| A8 | 中 | 0084 | batching 默认开 vs opt-in 自相矛盾;`seq_start/seq_end` 序号来源留白 |
| B1 | 低/中 | 0075 | 收紧 MMR 的配置发布可能瞬间触发存量仓位批量强平,缺护栏 |
| B2 | 低/中 | 0080 | `reference_price_source` 含 counter 算不出的 `book_mid`/`last` |
| B3 | 低/中 | 0083 | `ref = best/mid` 未按 side 钉死;sell 预占把 spot/perp 模型混在一起 |
| B4 | 低 | 0076 | 交割幂等键边界应为 `(user, round)`;expiry 后 funding 是否停未写 |
| B5 | 低 | 0082 | workload 只有 "liquidation disabled",系统性低估尾延迟 |

---

## 修订处置(2026-05-31)

本节记录本轮修订如何处理评审意见。评审意见不是自动约束;每项按架构一致性、实现成本和产品取舍单独判断。

| ID | 处置 | 说明 |
| --- | --- | --- |
| S1 | 采纳 | 0075 改为把 `config_version` 定义成 ADR-0053 `PrecisionVersion` 的泛化与替代,并给出共享 symbol 状态 enum / 迁移矩阵。 |
| S2 | 采纳 | 0075 补订单携带 `symbol_config_version` 到 Match 的版本握手;版本 skew 时 fail-closed。 |
| S3 | 采纳 | 0076 / 0078 改为 quiesce / in-flight guard 语义,不再写成纯"先 cancel 再 settle/close"顺序流。 |
| A1 | 采纳 | 0081 改成单一 `apply_qty = min(fill_qty,current_leg_size,unconsumed_reservation)` 规则,excess 进入 breach。 |
| A2 | 调整采纳 | 没有把 reduce_only 作为可静默 clamp 的例外;选择与 0080 的默认 REJECT 哲学对齐:用户请求超过 close capacity 直接拒绝。系统生成的 close-all 由系统自行构造精确 qty。 |
| A3 | 采纳 | 0077 mode 切换闸门加入 trigger 服务里的 position-bound TP/SL/OCO/TrailingStop。 |
| A4 | 调整采纳 | 0077 明确 P1 cross+hedge 按 gross leg requirement 计保证金,不做 long/short offset 抵减;offset/netting 留给 portfolio / `PoolRiskModel`。这是保守产品取舍,不是实现遗漏。 |
| A5 | 采纳 | 0078 block trade 改为 coordinator + 两条 version-stamped legs,复用 0071 的协调器决策 / shard 版本戳执行模式。 |
| A6 | 采纳 | 0079 补 STP / beneficial-owner / rebate eligibility 作为负 maker rebate 前置。 |
| A7 | 采纳 | 0079 明确 fee 超 buffer 不得扣出负 wallet,不足部分走 deficit / RiskPool settlement。 |
| A8 | 调整采纳 | 0084 钉死普通连接默认 batch on,做市/HFT 可 opt-out;`seq_start/seq_end` 改为 journal `(topic,partition,event_seq)` 命名空间,不用 push-local seq。 |
| B1 | 采纳 | 0075 增加 MMR/risk tier 收紧的 staged / dry-run / policy 护栏。 |
| B2 | 采纳 | 0080 拆 Counter 可执行参考价(mark/index) 与 Match 可执行参考价(best/book/last)。 |
| B3 | 采纳 | 0083 明确 buy 用 best ask、sell 用 best bid;spot/perp reservation 分开写。 |
| B4 | 采纳 | 0076 改为 `(user_id, settlement_round_id)` 幂等边界,并补 expiry 后 funding stop / settle 规则。 |
| B5 | 采纳 | 0082 workload 加 liquidation-enabled 场景。 |

---

## 系统性问题

### S1. 三套"版本 / 状态"机制并存,0075 重复发明 ADR-0053 的轮子 【高】

对照现状代码:

- [`../pkg/etcdcfg/etcdcfg.go`](../pkg/etcdcfg/etcdcfg.go) 的 `SymbolConfig` 只有一个二态 **`Trading bool`**;
- 同文件已有 **`PrecisionVersion uint64` + `ScheduledChange{EffectiveAt}`**(ADR-0053),做的正是"单调版本号 + 定时原子切换 + 让下游区分切换前后准入的订单"。

新 ADR 又各引入一套互不一致的状态/版本:

```
现状代码:   Trading bool                         + PrecisionVersion(ADR-0053)
ADR-0075:   {PREOPEN, TRADING, POST_ONLY,        + config_version
             CANCEL_ONLY, SETTLING, DELISTED}
ADR-0076:   {PREOPEN, TRADING, PRE_DELIVERY,     (正文另有 SETTLING_HALTED)
             SETTLING, DELIVERED, DELISTED}
```

问题:

1. 0075 的状态集与 0076 的状态集**互不一致**,也不和现有 `Trading bool` 对齐;没有任何一篇给出**单一权威的 symbol 状态枚举 + 合法迁移表**(谁能迁到谁)。两篇会各自实现出分叉状态机。
2. `config_version` 与已有 `PrecisionVersion` **语义重叠**。0075 通篇未提如何与 0053 收敛,结果是两个并行版本计数器同时刻画一个 symbol。

建议:

- 0075 显式声明 `config_version` 是 `PrecisionVersion` 的泛化与替代,复用 0053 已落地的 `ScheduledChange.EffectiveAt` 原子切换协议;
- 把 symbol 状态抽成一个共享 proto enum,0075 / 0076 共用,并附状态迁移矩阵;
- `SETTLING_HALTED` 等"散落在正文里的状态"必须进枚举,不能只在文字里出现。

### S2. 跨服务 config 传播窗口内的版本 skew 未定义 —— 0053 已给出解法 【高】

0075 §4 只解决了**单服务内**的原子替换("按 symbol 原子替换,不能 risk_tiers 新而 funding 旧"),未解决**跨服务**:

```
发布瞬间:
  perp-counter  已 watch 到 v(N+1)   ← 用新精度/新 band 准入
  Match         还在 v(N)            ← 用旧精度撮合
  BFF           还在 v(N-1)          ← 展示更旧
```

0075 把 `config_version` 称作"跨服务一致性锚点",但**没有定义版本不一致时谁让步**。而 ADR-0053 的 `PrecisionVersion` 注释已明确:"让下游 consumer 区分 pre- vs post-switch 准入的订单"——这就是答案。

建议:订单携带准入时的 `config_version` 一路传到 Match,Match 比对本地版本,skew 时 fail-closed。0075 §1 目前只说"perp-counter 写进 journal",**没说传给 Match 做准入交叉校验**,这是实打实的缺口(且方案现成)。

### S3. counter→Match 的异步 cancel 与在途 fill 之间缺 barrier 【高】

根因:perp-counter 通过 Kafka **异步**把 cancel 发给 Match,无法同步得知"盘口已静默、不会再有在途成交回流"。

```
        perp-counter sequencer          Kafka            Match symbol worker
                |                          |                      |
  freeze/cancel |--- cancel order -------->|--------------------->| (resting order 仍在)
                |                          |                      |
  settle / zero |   <-- 此前旧单的 fill 可能已撮合并在途 --------|
  position  ====X                          |<----- TradeEvent ----|  ← settle 之后才到达
                |                          |                      |
        把已清零 / 已交割的仓位又改出一笔 → 不变量破坏
```

三处都踩这个坑,处理深度不一:

- [0081](adr/0081-perp-reduce-only-settlement-hardening.md) 处理得最到位:用 reduce-only capacity reservation 当"服务端最后防线 + replay 锚点",显式承认 cancel/fill 竞态。
- [0076](adr/0076-perp-contract-product-expansion.md) §2 交割:写的是"先 cancel open orders,再 settle positions"——**纯顺序流,没有等待 Match 确认 book 清空的 barrier**。交割后一笔在途成交会在已下线合约上重开仓位。
- [0078](adr/0078-perp-order-position-product-api.md) §5 close-all:同样"cancel increasing orders → create reduce_only"顺序流。

建议:0076 / 0078 显式引用 0081 的同一套机制(zeroing / close reservation + 在途成交按 reservation 收口 + breach 告警),三篇统一到一个 "quiesce 协议",不要各写一个"先 A 再 B"的顺序管线。

---

## 单篇问题

### A1. ADR-0081:reduce_only 结算 race 分支语义自相矛盾 【高/中】

§4 把成交 race 按 **reservation 状态**分三支,中间一支有歧义:

> "若 reservation 已被缩减但 fill 已经发生……按**原** reservation 中仍未消费部分结算。"

"原 reservation" 指缩减前的 10 还是缩减后的 4?若 reservation 从 10 缩到 4、来了一笔 6 的 fill,按"原 10 的未消费部分"会**多减 2、可能反向开仓**,恰恰违背 reduce_only 承诺。

根因是**表述维度错了**:这些事件都进同一个 user sequencer **串行**处理,不存在真并发。应当按 sequencer 内权威的当前仓位 clamp,而非按 reservation 是否存在:

```
apply_qty = min(fill_qty, 当前该 leg 剩余仓位, 该 order 未消费 reservation)
excess    = fill_qty - apply_qty
excess > 0 → REDUCE_ONLY_INVARIANT_BREACH(停机告警,不静默反开)
```

这样三分支统一成一条 clamp 规则,且和 §2 的"reservation 是最后防线"自洽。

### A2. ADR-0080 vs 0081:是否改用户指令的哲学不一致 【中】

- [0080](adr/0080-perp-admission-risk-price-protection.md) §2 **特意选 REJECT 而非 CLAMP**,理由是"CLAMP 会改变用户指令"。
- [0081](adr/0081-perp-reduce-only-settlement-hardening.md) §1 却**静默** `accepted_qty = min(request_qty, available_close_qty)`,把用户要平的 10 悄悄改成 4 派发——这正是一种 CLAMP。

两篇 philosophy 打架。建议:要么 reduce_only 也走 REJECT(容量不足直接拒),要么在 0081 显式说明"reduce_only 是已知例外(减仓本就不该超过持仓)",并与 0080 交叉引用,别让两个准入层对"改用户指令"给出相反默认。

### A3. ADR-0077:mode 切换闸门漏了 trigger 条件单 【中】

§3 拒绝条件是"active orders / non-flat / in liquidation"。但 TP/SL 是 [0078](adr/0078-perp-order-position-product-api.md) §6 绑定 `position_idx`、存在 **trigger 服务**里的,不在 Match resting book 里。切换模式时若不把这些条件单算进"active orders",切换后它们绑定的 `position_idx` 语义就 orphan 了。闸门需把 trigger 侧挂单一起纳入。

### A4. ADR-0077:cross + hedge 两腿保证金是否相抵未定义 【中】

§4 说 cross 下 long / short leg 同属一个 cross pool,但未说保证金计算是否对冲。独立计两腿会**重复占用保证金、强平时双重计算风险敞口**(净敞口可能很小却按毛敞口要保证金)。可能推给 0074,但至少要点出这个交互并指明归属。

### A5. ADR-0078:block trade 双边跨 sequencer 协调缺失 【中】

§8 只写"perp-counter internal settlement → journal",但 block trade 是双边成交:两个对手方的仓位/保证金要么都改、要么都不改。这是和 [ADR-0071 跨 shard ADL](adr/0071-perp-sharded-insurance-and-cross-shard-adl.md) 同级的跨 sequencer 协调难题(一方结算时保证金不足怎么办、两 sequencer 不同 shard 如何两阶段)。建议引用 0071 的协调机制,而不是当成单边 mutation。

### A6. ADR-0079:负 maker rebate + 缺 STP = 刷返佣套利 【中】

§1 maker / taker 费率独立、§C 决定支持负 maker rebate。若 |maker rebate| > taker fee,用户自成交即可净赚平台返佣。全篇未提 STP。建议:这篇加一句 STP 前置依赖,或单开 ADR;否则负费率一上线即是经济漏洞。

### A7. ADR-0079:close fee 超 buffer 可能扣穿成负余额 【中】

§4 的"少补……从 wallet / position margin 继续扣并触发风险流程"太含糊。close 时 fee 超过 `close_fee_buffer`,继续扣可能把钱包扣成负,与 [0076](adr/0076-perp-contract-product-expansion.md)"明确不做负余额"冲突。建议明确路由到 0070 / 0073 的 deficit / insurance fund 路径。

### A8. ADR-0084:batching 默认/opt-in 矛盾 + 序号来源留白 【中】

1. **默认开 vs opt-in 自相矛盾**:§1 把 frame batching 当"第一阶段默认行为",§4 给出的却是 `batch:true` 的 **opt-in** 订阅选项。做市 / HFT 用户会无故吃 +10ms。需钉死:默认 on 则必须提供 opt-out;默认 off 则 §1 措辞要改。
2. **`seq_start/seq_end` 序号来源是 gap 检测命门,却被 defer**:§3 只说"不能缺定义"。push 侧序号在实例重启 / rebalance 后能否保持 per-`(connection, stream)` 单调连续、如何和 [0038](adr/0038-bff-reconnect-snapshot.md) 重连快照对账,是这篇真正要解决的点,现在最难的部分留白了。

---

## 次要点

- **B1 — 0075 §3 收紧 MMR 触发存量批量强平**:"按新版本重算是产品决策"兜不住,应加 grandfather / staged 护栏,禁止一次 publish 直接让大量仓位变可强平。
- **B2 — 0080 §2 `reference_price_source` 含 `book_mid`/`last`**,但 counter 不订阅盘口无法评估。要分清:限价 band 参考价哪些能在 counter 层算(mark / index)、哪些只能 Match 层算(book_mid / last),否则配出 counter 执行不了的规则。
- **B3 — 0083 §3 `ref = best/mid` 未按 side 钉死**(买应参考 best ask、卖参考 best bid);§2 perp sell 预占把 spot 的 "base qty" 与 perp IM 混在一行,perp 没有 base 库存,应拆开两种合约的预占模型。
- **B4 — 0076 交割幂等键 `settlement_round_id = symbol:expiry`,但 settle 按 user 扇出**,真正的幂等边界是 `(user, settlement_round_id)`,否则单用户 replay 会重复结算;另外 expiry 后 funding 是否停止累计未写。
- **B5 — 0082 §2 workload 只有 "perp liquidation disabled" 一档**,但强平路径恰是高压时的主要成本来源,应补一个 liquidation-enabled 场景,否则 benchmark 系统性低估尾延迟。

---

## 做得好的地方

- 0081 用 reservation 当 replay 锚点、不自动 bust trade 而是显式 breach 告警 —— 对手方账务安全的判断是对的。
- 0080 / 0083 的 counter(不碰盘口)/ Match(不碰账户)边界划得干净,各自 fail-closed。
- 0084 选"先传输帧 batching、不动 journal 语义"作为第一阶段,把 breaking 风险挡在审计 / projection 之外,递进合理。
- `position_idx`、`fee_rule_id`、`symbol_config_version` 这些字段在 journal / snapshot / history 的贯穿性,各篇 Implementation Notes 都点到了。

---

## 建议的处理顺序

1. **S1 + S2 合并处理**:统一 symbol 状态枚举 + 复用 ADR-0053 的版本机制并让订单携带 `config_version`。这是后续所有 perp 配置的地基,应先收敛。
2. **S3**:把 0076 / 0078 收口到 0081 的 quiesce 机制,消除三处重复的 cancel↔fill race。
3. **A1 / A2**:把 reduce_only 结算规则改写成按当前仓位 clamp,并与 0080 的"是否改用户指令"哲学对齐。
4. 其余 A / B 项按实现排期逐条补入对应 ADR。
