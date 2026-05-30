# ADR-0071: perp 分片化风控 —— 全局保险基金 + 跨 shard ADL（协调器决策 / shard 版本戳执行）

- 状态: **Accepted / MVP Implemented**（2026-05-30 起草；2026-05-31 拍板并落地独立 `perp-risk` 协调器，从 [ADR-0070](./0070-perp-liquidation-hardening.md) 的"单实例假设"缺口提升为独立 ADR，触发自 perp-counter 按 user 分片后保险基金与 ADL 的全局性问题）
- 日期: 2026-05-30
- 决策者: xargin, Claude
- 相关 ADR: 0068（USDT 线性 perp，§8 强平、§9 保险基金/ADL、§备选方案 A/C/E）、0070（强平进阶：阶梯 MMR / 部分强平 / backstop / ADL，**本 ADR 在分片拓扑下扩展其 §3/§4/§跨 sequencer hand-off**）、0010（Counter 按 user_id 分 shard）、0048（snapshot 绑 offset + 幂等水位）、0031（cold-standby HA）、0051（typed producer sequence）、0069（perp-pricing 单实例 + cold-standby，协调器候选宿主）

## 范围声明（先读这一段）

[ADR-0068](./0068-usdt-linear-perp.md) §9 把保险基金做成 **perp-counter engine 内的 per-symbol 标量**（`engine.insurance map[string]dec.Decimal`），[ADR-0070](./0070-perp-liquidation-hardening.md) §4 的 ADL 跨 sequencer hand-off 明确写"**仍在同一进程、同一 snapshot 内**用内部任务派发"。**这两条结论只在 perp-counter 单实例时成立**——而 [ADR-0068 §备选方案 A / 现状 line 67](./0068-usdt-linear-perp.md) 已定 perp-counter **按 `user_id` 分片**（复用 `pkg/shard`，对齐 [ADR-0010](./0010-counter-sharding-by-userid.md)），多实例分片是"HA 之后的增量"。

一旦打开分片：

- **保险基金会被分区(state partitioning)**：每个 shard 的 `insurance[BTC]` 只看得到**自己 owned 用户**在 BTC 上的强平盈余/亏损，全局池被切成 per-`(shard, symbol)` 局部聚合。这与"保险池应当是全局的"（单 symbol 所有用户共用一个吸损池）直接冲突。
- **ADL 跨进程**：按 user 分片后，某 symbol 的盈利对手仓散落在**所有 shard**，没有任何单个 shard 能独立排出全局 ADL 名单；0070 的"同进程同 snapshot 内派发"前提失效。

本 ADR 决定：分片后**引入一个全局风控协调器**（fund 余额 + ADL 排名/决策 + 托管周转金账务的单一 owner），**perp-counter shard 退化为持仓权威 + 执行器**。这看似违背 [ADR-0068 §C / 0070 §E](./0070-perp-liquidation-hardening.md) "不拆独立强平服务"，实则**恪守了 §E 的正确性论证**——§E 反对的是"独立服务读远程仓位、然后自己执行写入"（TOCTOU，time-of-check-to-time-of-use，检查与使用之间状态已变的竞态），本 ADR 的协调器**只决策、不修改仓位**，所有仓位 mutation 回到 owning shard 的 per-user sequencer 内、**带版本戳复核**后执行（§决策 2）。

**仍不变**（继续留在 shard 内，本 ADR 不动）：0070 的阶梯 MMR、部分强平、策略决策树、per-user sequencer、单用户自身仓位的 TOCTOU 复核。**本 ADR 只解决"全局性"那一层**：跨 shard 的 fund 与 ADL。

参考实现（均已读源、逐条核对代码）：
- **`bybit-leaked/trading`（正面范本）**：trading_service **按 user 分片**（`globalvar.ShardName`），ADL **决策在外部**、trading 只执行（`dispatcher/internal/process.go:119` 收 `AdlExecuteReq`），靠 **`OrigCrossSeq` 版本戳**复核（`liqadlbiz/adl_execute.go:82`、`liq_execute.go:39`），保险池是**独立全局服务**（per-coin 全局 + per-symbol 每日配额，`risk_pool_result_dto.proto`），且走**先托管、借周转金、逐步平仓(unwinding)、ADL 最后手段(last resort)**模型。
- **`cryptofabric/unimargin-liquidate-server`（反例，0070 §E 已批）**：单 Raft 组、不分片、**持仓零存**，ADL 对手方从 quote-server 的**最终一致影子副本**现读，**无版本号守卫**（只有本地咨询锁 + reduceOnly）——正是"集中决策者基于过期的远程读取直接修改仓位"的典型反例。

OpenTrade 未上线，breaking change 直接改（同 [ADR-0057](./0057-asset-service-and-transfer-saga.md)），不写兼容层。

## 术语 (Glossary)

| 本 ADR 字段 | 含义 | 业界对标（仅供映射） |
|---|---|---|
| `全局风控协调器`（perp-risk） | fund 余额 + ADL 排名/决策 + 托管周转金账务的**单一全局 owner**；只决策不改仓 | Bybit "ADL Service + 保险池服务"合体 / BN risk engine |
| `分片执行器`（perp-counter shard） | 持仓权威；在 per-user sequencer 内执行 liq/adl/takeover mutation | Bybit trading_service（分片）/ unimargin trade-server |
| `版本戳`（pos_seq + position_version guard） | 协调器决策时所基于的仓位读视图；执行端比对 side / match 序号 / 仓位版本，任一已变则拒绝任务 | **Bybit `OrigCrossSeq`**（关键前置依赖）/ 0070 `last_match_seq` + `Position.Version` / `adl_round` |
| `InsuranceDelta` | 单笔强平/接管释放或穿透的、流入/流出 fund 的签名增量 | — （已存在于 `PerpLiquidationEvent`） |
| `托管`（takeover） | 系统账户接管被强平仓位库存，事后逐步平仓(unwinding) | Bybit 强平托管 `TakenOverPosition` / 0070 §3 backstop 升级 |
| `周转金`（working capital） | 托管库存期间向全局池**借**的持仓资金，逐步平仓完毕**还**（盈余填池/亏损抽池） | Bybit `RiskPoolDTO.borrow_or_lend` |
| `per-symbol 配额`（quota） | 单 symbol 当日可消耗 fund 的上限，防单合约抽干全局池 | Bybit per-symbol 每日额度（BTC 100% / 其余 N%） |
| `adl_score` | ADL 排序键 = 未实现盈利率 × 有效杠杆（沿用 0070） | BN ADL ranking / Bybit ADL 灯(1-5) |
| `insurance_fund` | 吸收穿仓亏损的 USDT 池；本 ADR 改为**全局单一权威**（per-coin），不再 per-shard | 三家 insurance fund / 保险基金 |

## 背景 (Context)

### 现状（已落地，2026-05-30 核实代码）

保险基金是 perp-counter engine 内的 per-symbol 标量，且随强平**原子**变动：

- [`engine.go:53`](../../perp-counter/internal/engine/engine.go) `insurance map[string]dec.Decimal`
- [`engine.go:355`](../../perp-counter/internal/engine/engine.go) `AddInsurance` / [`:365`](../../perp-counter/internal/engine/engine.go) `InsuranceFund` / [`:447`](../../perp-counter/internal/engine/engine.go) `ApplyLiquidationFill`（逐笔把 `marginReleased+realized-fee` 加进 `insurance[symbol]`）/ [`:475`](../../perp-counter/internal/engine/engine.go) `ForceClose`
- [`engine/snapshot.go:19`](../../perp-counter/internal/engine/snapshot.go) `Insurance map[string]string` 随 snapshot 持久化（绑 offset + 幂等水位，[ADR-0048](./0048-snapshot-offset-atomicity.md)）
- [`service/liquidation.go:143`](../../perp-counter/internal/service/liquidation.go) ADL 告警位 `AdlQueued: s.eng.InsuranceFund(o.Symbol).Sign() < 0`

**单实例下这是对的**：一个进程承载全部分区、结算全部用户，"per-symbol in-engine 标量"恰好就是全局 per-symbol 池。**问题只在打开分片那一刻显现**（见 §范围声明）。所以本 ADR 不是修当前 bug，是"打开分片"工作流必须先建立的基础。

### 参考实现对比（已读源、核对代码）

| 维度 | unimargin（反例） | **bybit（范本）** | 本 ADR 取舍 |
|---|---|---|---|
| 执行器拓扑 | 单 Raft 组、不分片、**持仓零存** | **按 user 分片**，持仓在 shard 本地 | 跟 bybit：perp-counter 按 user 分片，持仓本地 |
| ADL 决策者 | liquidate-server 自己（读 quote 影子副本） | **外部**（全局排名，发 `AdlExecuteReq` 进 shard） | 跟 bybit：独立**协调器**决策 |
| ADL 执行者 | liquidate-server 构造对手单下发给 trade-server | **owning shard 执行**（持仓权威） | 跟 bybit：回 owning shard sequencer |
| TOCTOU 守卫 | 本地咨询锁 + reduceOnly（**无版本号**） | **`OrigCrossSeq` 版本戳**，shard 不符即拒 | 跟 bybit：版本戳握手（= 0070 的 `last_match_seq`/`adl_round`） |
| 保险基金 | **无**（系统账户透支垫付） | **独立全局服务**，per-coin 全局 + per-symbol 配额 | 跟 bybit：全局 fund（协调器持有）+ 配额 |
| fund 模型 | n/a | 借/还**周转金**（先托管→借→逐步平仓→还），Kafka 异步结算 | 跟 bybit：托管 + 周转金，credit 异步 fold |
| ADL 排名 | 裸 max 未实现盈利、每轮一个 | 杠杆/盈利加权（ADL 灯 1-5），rank 在 shard 维护 | 沿用 0070 `adl_score`，rank 由 shard 报、协调器排 |

**结论**：bybit 同时回答了"insurance 该独立（独立全局池服务）"和"ADL 决策与执行分离（外部决策 / 分片执行）"两个问题，且用 `OrigCrossSeq` 给出了 unimargin 缺失的那块正确性拼图。本 ADR 把 bybit 的三角色映射到 OpenTrade，并把 bybit 拆分得较细的"ADL 决策者 + 保险池出纳"在 MVP 阶段**合并为一个协调器**（量级低、同属全局，见 §备选方案 E）。

## 决策 (Decision)

### 1. 三角色拆分

```
        ┌──────────────────────────────────────────┐
        │  全局风控协调器  perp-risk                  │   单实例 + cold-standby（对齐 0069 perp-pricing 形态）
        │  低 QPS（只在强平/接管/ADL 时活跃）          │   不持有用户仓位、不在撮合热路径
        │   ├─ 全局保险基金余额 = fold(InsuranceDelta) │   per-coin 全局 + per-symbol 配额
        │   ├─ ADL 排名 + 决策（deficit→减谁/多少）    │   只产出"带版本戳的任务"，不修改仓位
        │   └─ 托管周转金账务（借/还全局池）           │
        └───────────────┬──────────────────────────┘
        InsuranceDelta ▲ │ 版本戳任务（liq/takeover/adl）
        + ADL 候选上报  │ ▼  幂等派发
   ┌───────────────────┴───────────────────────────────────────┐
   │ perp-counter shard #0     #1     ... #N （按 user_id 分片）   │  持仓权威 + 执行器
   │  每 shard：健康检测 + 单仓平仓（本地无条件闭合）              │  高 QPS（每 mark tick × owned 用户）
   │           在 owning user sequencer 内执行 mutation + 版本戳复核 │  阶梯 MMR / 部分强平 / 决策树（0070）仍在此
   └────────────────────────────────────────────────────────────┘
```

- **协调器 = "决策 + 全局账务"**：fund 余额、ADL 决策、托管周转金，三者都需要**跨 symbol / 跨 shard 的全局视角**，且都是低频（强平/接管/ADL 才活跃），合在一个单实例 + cold-standby 服务里。
- **shard = "持仓权威 + 执行"**：高频的健康检测、单仓平仓、所有仓位 mutation 都留在 shard（持仓和 mark 都在本地，又快又新；cross-margin 让 pool-health 天然是账户级、落在该用户 shard）。
- **宿主选择**：协调器新建独立 `perp-risk` 服务（**失败半径隔离**，对齐 0068 §C 看重的故障隔离——风控风暴故障不影响 mark-price 发布）。折叠进 `perp-pricing` 曾作为备选（它已是单实例 + cold-standby、已每 tick fanout 到所有 shard、已有全局视角，见 [0069 §备选方案 E](./0069-external-composite-index-price.md) 的"fold vs split"先例），但 MVP 拍板独立服务：隔离收益大于少一个部署单元的便利。

### 2. 版本戳握手（本 ADR 的核心正确性约束，缺它就是 unimargin）

协调器派发给 shard 的**每一个** liq/takeover/adl 任务，必须携带它**决策时所基于的仓位读视图**（`side`、`pos_seq` = 0070 已有的 `last_match_seq`、`position_version` = `Position.Version`；ADL 另带 `adl_round`）。owning shard 在自己的 per-user sequencer 内执行前**比对版本**：

```
协调器：基于"快照 @ {side=S,pos_seq=N,version=V}"决策 → 派发 task{user, symbol, qty, price, side:S, pos_seq:N, position_version:V, adl_round:R}
shard ：进 user 的 sequencer → if 当前仓位.side/pos_seq/version 任一不匹配 → 拒绝（仓位已被在途成交/资金费/ADL 推进，决策过期）
                              → else 执行 mutation，version++，幂等水位记 adl_round=R（重放不重复）
```

- 这正是 bybit `refPz.Latest().CrossSeq != req.OrigCrossSeq → 拒绝`（`liq_execute.go:39` / `adl_execute.go:82`）。**协调器的决策允许基于稍旧的全局视图，正确性由执行端的版本复核保证**——这把"跨服务读 → 执行写入"的 TOCTOU 窗口消除，恪守 0068 §C/0070 §E 的论证。
- unimargin 缺的就是这一步（对手仓只有本地咨询锁，无版本号），所以它的 ADL 对手方侧是公认的 race。**本 ADR 把版本戳列为强制不变量 #13**。

### 3. 全局保险基金 = perp-journal 的 fold

- shard 强平/接管时，把盈余/穿透作为 `InsuranceDelta` 发到 `perp-journal`（**现状已在发**，`liquidation.go:143` 的 `PerpLiquidationEvent` 已带 `InsuranceDelta`）。**shard engine 不再持有权威 fund 余额**（删 `engine.insurance` 的"权威"语义，退化为本地可选缓存或直接移除）。
- 协调器**消费 `InsuranceDelta` 流 fold 成全局余额**（per-coin 全局；per-symbol vs 全局这个 [0068 §9](./0068-usdt-linear-perp.md) 留开的问题，在 fold 模型里只是"fold 成 map 还是标量"，本 ADR 定 **per-coin 全局 + per-symbol 配额**）。余额随协调器自己的 snapshot 持久化（绑 offset，[ADR-0048](./0048-snapshot-offset-atomicity.md)）。
- **credit（盈余进池）可异步**：线性 perp 里对手方盈亏早已在各自 shard 记过账，fund 只吸收残差，credit 无决策依赖 → 最终一致即可。**只有 deficit + ADL 触发需要协调器的权威余额**（§4）。

### 4. 托管 + 周转金 + 跨 shard ADL（取代"挂破产价单依赖盘口流动性"）

把 0070 §3 backstop 升级为 bybit 的 **先托管、借周转金、逐步平仓(unwinding)、ADL 最后手段(last resort)** 模型：

```
shard：单仓破 MM → 0070 救援阶梯（降档/撤单/加保证金/部分强平）→ 仍不行 → 整仓接管：
        用户仓位即刻平仓退出（平给 backstop 系统账户），backstop 记反向库存
        ── 发 takeover 事件 + InsuranceDelta（接管时的权益缺口）──▶ 协调器
协调器：向全局池为该托管库存"借"周转金（受 per-symbol 配额约束）
        backstop 库存按 bankruptcy_price 逐步平仓(unwinding)（reduce_only 挂 Match）
        ◇ 平仓无法成交 / 配额不足 / fund 转负(deficit>0) ?
          否 → 逐步平仓完毕 → "还"周转金（平仓盈余填池 / 穿仓抽池）
          是 → 触发跨 shard ADL（见下）
```

**跨 shard ADL（0070 §跨 sequencer hand-off 的跨进程版）**：

1. 协调器算 `deficit = -global_insurance_fund(coin)`（基金兜不住的额度）。
2. 候选名单：该 symbol 上反向盈利仓，按 `adl_score` 排序。候选来源二选一（见 §实施约束）：(a) 各 shard 周期上报自己 owned 的高分候选；(b) 查 perp 持仓投影（history/trade-dump）。**两者都允许滞后**——因为：
3. 协调器把每个 ADL 任务**带版本戳**派发到**对手仓 owning shard**；shard 在对手用户 sequencer 内**复核仍反向/盈利/存在 + side / pos_seq / position_version 匹配**（§2），再按 bankruptcy_price 平掉、赢家让利落账、发 `PerpAdlEvent`、`adl_round` 幂等。
4. 补足 deficit 则停；不足继续下一候选。

> 关键：协调器**永不直接改仓**，只发版本戳任务；候选名单滞后无害，因为执行端复核保证正确性。这与 unimargin"集中读 quote 影子副本直接构造对手单"的本质区别，就在第 3 步的版本戳复核。

## 备选方案 (Alternatives Considered)

### A. 保险基金留 perp-counter in-engine per-shard（现状 / 0068 §9）
- 单实例下正确（in-engine 标量 == 全局池）。但**打开分片即被分区(state partitioning)成 per-`(shard,symbol)` 局部聚合**，单 symbol 的吸损池被人为切开，与"全局池"语义冲突，且各 shard 各自判 deficit 会重复/错误触发 ADL。**否决**（仅作单实例 MVP 的退化形态保留）。

### B. 同步保险基金服务（每笔强平 fill RPC 扣 fund）
- 把 fund 做成独立服务，shard 每笔强平 fill 同步 RPC 它扣减。**否决**：① 每笔 fill 变成跨服务分布式事务，击穿 [ADR-0048](./0048-snapshot-offset-atomicity.md) 的 EOS/原子落盘；② ADL 判定要同步读远程余额 → 在途 fill 未结算就读到旧值 → 又回到 TOCTOU。本 ADR 用"credit 异步 fold + deficit 由协调器权威判定"绕开。

### C. 改按 symbol 分片（让 fund/ADL 天然 shard-local）
- 若按 symbol 分片，则 per-symbol fund 和 ADL 对手方都落同一 shard，0070 的"同进程"假设每 shard 内仍成立，无需协调器。**否决**：按 symbol 分片**断掉 cross-margin**（一个用户跨 symbol 的仓位会被分散到不同 shard，无法计算账户级保证金），而 [0068 §3 的"一律走 pool 接口"](./0068-usdt-linear-perp.md) 硬约束正是为 cross-margin 做准备。**user 分片是为 cross-margin 必须付的代价，fund/ADL 走全局协调器是其必然结果**——无法兼得的根本性权衡（fundamental trade-off）。

### D. unimargin 式：集中决策者基于过期的远程读取直接修改仓位
- liquidate-server 现读 trade-server/quote-server 仓位、自己构造对手单。**否决**：对手方侧无版本号守卫（只有本地咨询锁 + reduceOnly），决策基于滞后影子副本、执行不复核——0068 §C 批判的 TOCTOU，代码已证实是 race。本 ADR 取其"集中决策"形、**弃其"直接执行写入"**，用 §2 版本戳把决策与执行解耦且复核。

### E. 协调器内部：合并 vs 拆分（fund 出纳 + ADL 决策者）
- bybit 拆成"外部 ADL Service" + "独立保险池服务"两个。**本 ADR 选合并**为一个 `perp-risk`：fund 余额与 ADL 触发本就是同一个决策（`deficit = -fund → 减多少`），拆开会在这个一致性最敏感点插一次跨服务读；且两者都低频、同属全局。**bybit 的拆分作为未来扩容路径保留**（量级真上来再拆出 fund 出纳）。

### F. 全局池粒度：per-coin 全局 + per-symbol 配额（选） vs 纯 per-symbol vs 纯全局
- 纯 per-symbol：回到"局部聚合池"，单 symbol 缓冲不足。纯全局无配额：单合约爆仓可抽干全局池、影响所有合约。**选 per-coin 全局 + per-symbol 每日配额**（bybit 形态）：既满足"池全局"，又用配额堵住单合约抽干的尾部风险。（具体百分比如"BTC 100% / 其余 20%"是配置项，bybit 那份分析里的数字仅作示意。）

## 理由 (Rationale)

1. **分片是既定前提**（0068 §A / 0010），cross-margin 又要求按 user 分片（备选 C），所以 fund/ADL 的全局性**只能**靠协调器解决——这不是"要不要拆服务"的偏好，是分片拓扑的逻辑后果。
2. **协调器不破 0068 §C/0070 §E 的正确性论证**：§E 反对的是"读远程仓位直接执行写入"，协调器只决策、shard 带版本戳执行+复核，TOCTOU 窗口消除（§2）。bybit 用 `OrigCrossSeq` 在生产中验证了该方案；unimargin 缺它正是反证。
3. **热路径零退化**：单仓强平的检测+平仓仍 shard 本地、无条件闭合、不同步读远程 fund；只有低频的 deficit/ADL 才上协调器。性能与故障半径都更好。
4. **fund 当 fold 几乎零新基建**：`InsuranceDelta` 已在 `perp-journal` 上，全局余额就是这条流的 fold；per-symbol vs 全局也顺手在 fold 层定死。
5. **托管 + 周转金比"挂破产价单依赖盘口流动性"鲁棒**：把"用户仓位即刻平仓退出"与"库存逐步平仓(unwinding)"解耦，直接解决 0070 缺口 A（行情急跌盘口流动性不足→最需平仓时无法成交）；fund 当周转金放贷而非被动累加，ADL 真正退为最后一步。

## 影响 (Consequences)

### 正面
- 保险池真正全局（per-coin），单 symbol 吸损池不再被分片切分；deficit 判定单一权威，不重复/误触发 ADL。
- ADL 在分片拓扑下正确闭合，且沿用版本戳保证不踩 unimargin 的 race。
- 协调器低 QPS + 单实例 + cold-standby，故障与撮合热路径隔离。
- 托管周转金模型让盘口流动性不足下强平也保证闭合（接 0070 缺口 A）。

### 负面 / 代价
- **新增一个有状态服务**（协调器）：自己的 snapshot（fund 余额 + 托管库存 + 周转金借据 + ADL 在途）绑 offset + cold-standby，运维面 +1。
- **跨 shard 派发链路**：协调器→shard 的版本戳任务投递（topic 或 RPC）+ 幂等 + 重启不丢不重，是新的顺序/并发敏感面。
- **全局 fund 最终一致**：credit 异步 fold 会让 fund 短暂比"完美同步"更负，靠 ADL catch-up 收敛——需明确这是设计而非 bug（不变量 #14/#15）。
- **托管周转金与 collateral pool 的对账**：借/还周转金如何与 [0057 资产](./0057-asset-service-and-transfer-saga.md) / collateral pool 守恒对账，是新账务面（列开放问题）。

### 中性
- 单实例 MVP 下本 ADR 可**先不实施**（in-engine fund == 全局，正确）；它是"打开分片"工作流的前置。
- backstop 系统账户的库存本身仍位于某个 shard（它也是个 user），链下对冲流程沿用 0070。

## 实施约束 (Implementation Notes)

### 落地要点
- **协调器 `perp-risk`**（独立新服务）：消费 `perp-journal` 的 `InsuranceDelta` → fold 全局 fund；持有 per-symbol 配额计数（每日重置）；ADL 排名 + 派发；托管周转金借据。自身 snapshot 绑 offset（[0048](./0048-snapshot-offset-atomicity.md)）+ cold-standby（[0031](./0031-ha-cold-standby-rollout.md)）。
- **perp-counter shard**：`engine.insurance` 去"权威"语义（移除或降为本地缓存）；强平/接管照发 `InsuranceDelta`；新增**接收协调器版本戳任务**的入口（liq/takeover/adl），进 owning user sequencer、比对 `side` / `pos_seq` / `position_version`、幂等 `adl_round` 后执行。
- **新事件 / 字段**：`PerpTakeoverEvent`（接管 + 周转金借）、`PerpAdlEvent`（沿用 0070）、`InsuranceDelta` 流明确为协调器的 fold 输入；ADL 任务消息带 `{user, symbol, qty, price, side, pos_seq, position_version, adl_round}`。
- **ADL 候选来源**：MVP 选 (a) 各 shard 周期上报 owned 高分候选给协调器（新鲜、量可控）；(b) 查 perp 持仓投影作回退(fallback)。无论哪个，**执行端版本戳复核保证滞后无害**。
- **配额**：协调器侧 per-symbol 每日额度 = f(全局余额)，借出三者取 min（请求额 / symbol 剩余配额 / 全局可用），对齐 bybit `risk_pool_result_dto`。

### 关键不变量（落地时逐条 audit，承接 0068 #1–#6 / 0070 #7–#11）
12. **全局 fund 单一权威**：分片后 fund 余额只有协调器持有；shard 只产出 `InsuranceDelta`，不持有权威余额、不基于局部 fund 判 deficit。（单实例 MVP 是其退化等价形态——**多实例未上线前，须在 perp-counter 启动处 guard "分片数>1 且无协调器 = 拒绝启动"**，防静默的状态分区。）
13. **版本戳握手（核心正确性约束）**：协调器派发的每个 liq/takeover/adl 任务带 `side`、`pos_seq`、`position_version`（+`adl_round`）；owning shard 执行前比对，不符即拒。绝不允许"无戳直接执行"。（= bybit `OrigCrossSeq`；缺它就是 unimargin 的 race。）
14. **热路径无条件本地闭合**：单仓强平检测+平仓在 owning shard 内完成，不同步读/扣远程 fund；credit 异步 fold、ADL 是 catch-up。
15. **ADL 只由协调器基于全局 fund 决策**：`deficit>0` 才 ADL；任一 shard 不得基于局部 fund 触发 ADL。
16. **全局池 + per-symbol 配额**：单 symbol 当日借出 ≤ 配额；超配额走"减小平仓量 / 优先 ADL"，不静默穿配额。
17. **协调器决策幂等 + 可恢复**：fund fold、托管借据、ADL 在途全随协调器 snapshot 绑 offset；重启 replay 不重复借/重复减仓。

### 测试要点
- 单测：fund fold 守恒（credit/debit/借还周转金累计 = 全局余额）；配额三取 min + 每日重置；版本戳复核（side/pos_seq/version 任一过期→拒、全部匹配→执行一次）；`adl_round` 幂等重放。
- 集成：开仓 → 多 shard 各有对手仓 → 某 user 穿仓接管 → 协调器借周转金逐步平仓(unwinding) → 无法平仓 → **跨 shard ADL 减不同 shard 的对手盈利仓** → fund 收敛；协调器宕机中途 → snapshot+replay 一致（借据/ADL 队列不丢不重）。
- race（`go test -race`）：协调器并发派发 ADL 任务到多 shard，对手仓在自身 sequencer 内被版本戳复核+减仓，构造"对手仓在派发与执行之间被自己成交推进"→ 验证版本戳拒绝、无重复减仓、无跨 shard 直接改仓。

### 附录：关键流程 ASCII 序列图

#### 图 1 — 分片下正常强平 + 全局 fund fold（热路径 shard 本地）

```
perp-pricing    perp-counter shard X (user A sequencer, 本地无 TOCTOU)        协调器 perp-risk
  │ mark tick ─►│ pool.health 破 MM ? 是 → 本地平仓(0070 决策树)
  │             │ reduce_only @ liq/bankruptcy → Match 成交 → 本地结算 A
  │             │ ── PerpLiquidationEvent{InsuranceDelta: ±d} ──(perp-journal)──►│ fold: fund += ±d
  │             │ （A 已平仓退出, 不等协调器, 无条件闭合）                        │ （credit 异步, 最终一致）
```

#### 图 2 — 托管 + 周转金借/还（盘口流动性不足时保证闭合）

```
shard X (user A)              协调器 perp-risk (全局 fund + 配额)        Match / backstop
  │ 整仓接管: A 平给 backstop, backstop 记反向库存
  │ ── PerpTakeoverEvent{库存, 接管缺口 InsuranceDelta} ──►│ 借周转金(≤ symbol 配额): fund -= 借出
  │                                                         │ backstop 库存 reduce_only @ bankruptcy ─►│ 平仓
  │                                                         │◄────────── 成交 / 无法成交 ──────────────┤
  │                                                         │ ◇ 平仓完毕? → 还周转金(盈余填池/穿仓抽池)
  │                                                         │ ◇ 无法成交/配额不足/deficit>0 → 进图 3 ADL
```

#### 图 3 — 跨 shard ADL（协调器决策 / shard 版本戳执行，唯一跨 shard 协调点）

```
协调器 perp-risk (穿仓最后手段决策)    版本戳派发           shard Y/Z (对手盈利方 user B/C)
  │ deficit = -global_fund(coin)        │                    │
  │ 候选 = 反向盈利仓 by adl_score       │                    │   （候选可来自 shard 上报 / 持仓投影, 允许滞后）
  │ 选名单(只读, 不改仓) ───────────────►│ task{B, sym, qty,   │
  │                                      │   price, side:S,    │
  │                                      │   pos_seq:N,        │
  │                                      │   version:V,        ─►│ 进 B 的 sequencer:
  │                                      │   adl_round:R}      │   ◇ B.side/pos_seq/version 匹配且仍盈利/存在 ?
  │                                      │                     │     否 → 拒绝(决策过期, 等下轮)
  │                                      │                     │     是 → 按 bankruptcy 平, 赢家让利落账
  │                                      │                     │          adl_round=R 幂等, PerpAdlEvent→B
  │◄──────────── deficit 补足? ──────────┤◄────────────────────┤
  │ (补足则停; 不足派下一候选)            │                     │
```

## 开放问题 (Open Questions)

- **托管周转金 ↔ collateral pool 对账**：借/还周转金如何与 [0057](./0057-asset-service-and-transfer-saga.md) 资产 / collateral pool 守恒对账，是否需要 backstop 账户在 asset-service 落真实系统账户（同 0070 backstop 库存的链下对冲问题）。
- **ADL 候选来源**：shard 主动上报 vs 协调器查持仓投影 vs 两者结合；上报频率与新鲜度 vs 开销。
- **per-symbol 配额具体策略**：百分比、每日重置时点、BTC 特权与否；配额耗尽时"减平仓量 / 优先 ADL / 更激进价"的次序。
- **socialized loss**：全局池+配额仍兜不住时，社会化损失分摊作为 ADL 之外的终极层，是否本 ADR 范围（倾向单独 ADR）。
- **协调器单点**：单实例 + cold-standby 的 failover 期间，强平继续本地闭合、fund fold 暂停积压（重启 replay 补齐）是否可接受，还是需要更强的协调器 HA。
- **回填 0068/0070**：是否在 0068 §9 / 0070 §4/§11 加 "分片拓扑见 0071" 的前向注记（本 ADR 不改它们，仅声明扩展关系）。

## 参考 (References)

- [ADR-0068](./0068-usdt-linear-perp.md) — USDT 线性 perp（§9 保险基金/ADL = per-symbol in-engine，本 ADR 在分片拓扑下改为全局协调器；§备选方案 A perp-counter 按 user 分片；§C 内置不拆的正确性论证，本 ADR 恪守其精神）。
- [ADR-0070](./0070-perp-liquidation-hardening.md) — 强平进阶（§3 backstop 升级为托管+周转金；§4 + §跨 sequencer hand-off 从"同进程同 snapshot"扩展为跨 shard；`last_match_seq`/`adl_round` 复用为版本戳）。
- [ADR-0010](./0010-counter-sharding-by-userid.md) — 按 user_id 分 shard（perp-counter 对齐）。
- [ADR-0048](./0048-snapshot-offset-atomicity.md) — snapshot 绑 offset + 幂等水位（协调器 fund/借据/ADL 队列复用）。
- [ADR-0069](./0069-external-composite-index-price.md) — perp-pricing 单实例 + cold-standby（协调器候选宿主 + fold-vs-split 先例）。
- 参考实现（已读源、核对代码）：
  - `bybit-leaked/trading`（范本）：`trading_service/internal/dispatcher/internal/process.go:119`（外部 ADL 经 dispatcher 入）、`.../liqadlbiz/adl_execute.go:82` + `liq_execute.go:39`（`OrigCrossSeq` 版本戳）、`globalvar.ShardName`（按 user 分片）、`idl/models/riskpooldto/risk_pool_result_dto.proto`（per-coin 全局 + per-symbol 配额）、`taken_over_position_dto.proto`（托管 + 周转金借还）。**ADL 决策者不在本泄漏内**（确为外部服务）。
  - `cryptofabric/unimargin-liquidate-server`（反例）：单 Raft 组、持仓零存、`QuoteService.getMaxProfitRatePosition` 读 quote 影子副本、对手方侧仅本地咨询锁 + reduceOnly（无版本号）——0068 §C/0070 §E 批判的 TOCTOU 典型反例。
- 实现位置（动工时涉及）：新 `perp-risk/`、`perp-counter/internal/{engine,service}/`（去 fund 权威 + 收版本戳任务入口）、`api/event/perp_journal.proto`（InsuranceDelta fold 输入 + PerpTakeoverEvent + ADL 任务消息）。
