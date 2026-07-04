# 完整交易所缺口清单（2026-07-05）

> 目的：回答"从当前代码到一个完整交易所，还缺哪些东西"。本文是 [industry-gap-analysis-2026-05-31.md](./industry-gap-analysis-2026-05-31.md) 的增量更新：
>
> 1. 刷新其中已落地条目的状态（ADR-0078 / 0079 / 0083 已实现）；
> 2. 补充前文未展开的**交易内核之外的业务域**（资金进出、用户体系、合规、运营）；
> 3. 用圈层模型给出立项 / 排期视角。
>
> 本文不是实现承诺；要落地的项仍需进入 ADR / roadmap。空白业务域已按 [feature-requests.md](../feature-requests.md) 流程登记（见 §7）。

## 1. 总体判断

交易内核（现货 + USDT 线性永续的撮合、清算、事件恢复、HA、条件单、历史投影、admin 平面）已经闭环，工程质量超出 demo 水平：Counter/Match 职责分离、Kafka EOS（exactly-once semantics，恰好一次语义）、snapshot-offset 原子性、vshard failover、trigger OCO/trailing、perp 强平/backstop/ADL（auto-deleveraging，自动减仓）均有成体系的 ADR 与代码。

真正的缺口不在"能不能撮合一笔订单"，而集中在内核之外的三个圈层——**资金进出、用户与合规、生产化运行**——以及内核自身的产品化 backlog（多数已有 Proposed ADR）。

## 2. 圈层模型

```
                 ┌─────────────────────────────────────────────┐
                 │  圈层 4：合规与运营                            │
                 │  KYC/AML · 制裁筛查 · 报表 · 客服 · 上币流程    │
                 │  ┌───────────────────────────────────────┐  │
                 │  │  圈层 3：资金进出                        │  │
                 │  │  链上充提 · 冷热钱包 · 提现风控 · 法币     │  │
                 │  │  ┌─────────────────────────────────┐  │  │
                 │  │  │  圈层 2：用户与账户体系            │  │  │
                 │  │  │  注册/2FA · 子账户 · API-key 自助  │  │  │
                 │  │  │  ┌───────────────────────────┐  │  │  │
                 │  │  │  │  圈层 1：交易内核 ✅ 已闭环  │  │  │  │
                 │  │  │  │  (产品面仍有 ADR backlog)   │  │  │  │
                 │  │  │  └───────────────────────────┘  │  │  │
                 │  │  └─────────────────────────────────┘  │  │
                 │  └───────────────────────────────────────┘  │
                 └─────────────────────────────────────────────┘
                 横切面：生产安全 hardening · 可观测性 · 容灾 · 性能证明
```

圈层间的依赖关系：圈层 2/3 是全新业务域、与内核解耦，可与内核侧 backlog **并行开发**；圈层 4 的合规部分可在圈层 3 设计时只预留接口边界、实施放最后。

## 3. 自 2026-05-31 以来的状态更新

前文 Gap Matrix 中以下条目状态已变化：

| 条目 | 2026-05-31 状态 | 当前状态 |
|---|---|---|
| Perp 账户与保证金模式 | ADR-0074 Proposed | ✅ 已实现 P0+P1（isolated 操作面 + USDT cross margin 全链路；unified / portfolio margin 仍为 ADR 内后续路径） |
| Perp SymbolConfig 产品化 | ADR-0075 Proposed | ✅ 已实现（M1-M8：versioned catalog、状态机准入、config_version） |
| Perp 持仓模式（hedge / both-side） | ADR-0077 Proposed | ✅ 已实现 |
| Perp 订单与持仓产品 API | ADR-0078 Proposed | ✅ 已实现（commit `99ad14d`：amend / batch / cancel-all / pre-check / close-all / block trade MVP；batch amend 记入未来工作） |
| Fee accounting / rebate / 平台账户 | ADR-0079 Proposed | ✅ 已实现（准入钉价、用户费率覆盖、平台手续费账户 + SUM 对账不变量、fee_deficit 坏账路径、日统计） |
| 原生滑点保护 / protected market order | ADR-0083 Proposed | ✅ 已实现（Match 侧 collar 推导 + `effective_limit`；BFF 翻译路径 B 移除） |
| 现货 amend / batch 订单 API | 未拆分 | ADR-0085 Proposed（新增） |

其余条目（安全 hardening、STP/SMP、benchmark、ADR-0076 / 0080 / 0081 / 0084 等）状态不变，本文不重复展开，见前文 Gap Matrix。

## 4. 完全空白的业务域（代码没有，多数未立项）

这是"完整交易所"意义上最大的缺口，全部在交易内核之外。每条给出现状、缺口与建议去向。

### 4.1 链上充提 / 钱包系统（P0，接真实资金的先决条件）

- **现状**：`asset` 服务只有 funding wallet 与内部转账 saga（ADR-0057 / 0065）；对外 API 不存在 Deposit / Withdraw；Counter `Transfer` 的 DEPOSIT/WITHDRAW 语义只是账务动作，没有链上对手方。
- **缺口**：充值地址生成与归集、per-chain 确认数策略、冷热钱包分离、提现审批流（限额、延时、多签）、链上异常处理（reorg 回滚、双花检测）、链上-账务对账。
- **建议去向**：独立"资金/钱包"路线图与服务边界（前文 §8 已建议），不混进 Match/Counter；与 Counter 的唯一接口是带幂等 `transfer_id` 的 `Transfer` RPC。

### 4.2 法币通道（P2，产品决策先行）

- **现状**：无。
- **缺口**：fiat on-ramp / off-ramp（法币入金/出金）、支付渠道对接、法币账务。
- **建议去向**：先做产品决策（是否要法币业务）；技术上复用 4.1 的资金服务边界。

### 4.3 用户账户体系（P0/P1）

- **现状**："用户"只是一个 `user_id` 字符串 + JWT / API-key 静态文件（ADR-0039）。
- **缺口**：注册/登录、密码与 2FA（two-factor authentication，双因素认证）、设备与会话管理、防钓鱼码、API-key 用户自助管理（创建/吊销/scope/IP 白名单——[security.md](../security.md) 已列）、母子账户（sub-account）与母子间资金划转。
- **联动内核**：机构级 STP/SMP（self-trade prevention / self-match prevention，自成交防护）需要按 beneficial owner（受益所有人）分组，子账户模型是其数据前提（前文 §2 P0/P1 项）。
- **建议去向**：独立"用户服务"（user profile + credential + sub-account tree），BFF auth 从静态文件切到该服务；子账户分组模型与 STP/SMP ADR 联合设计。

### 4.4 KYC / AML / 制裁筛查（真实资金 P0，实施可最后）

- **现状**：无；[security.md](../security.md) 明确声明"本文不是合规文档"。
- **缺口**：KYC（know your customer，客户身份认证）分级与额度联动、AML（anti-money laundering，反洗钱）交易监测、Travel Rule、制裁名单筛查、提现风控（异常行为拦截）。
- **建议去向**：外采/对接为主，自建为辅；在 4.1 钱包与 4.3 用户服务的设计中预留 hook（额度检查、提现拦截、账户冻结指令），实施放最后。

### 4.5 杠杆现货 / 借贷（P1/P2，独立业务线）

- **现状**：无，连 backlog 都没有。
- **缺口**：margin trading（借币下单、利息计提、维持保证金率、爆仓）、借贷池 / 利率模型；与统一保证金（ADR-0074 后续路径）存在架构耦合。
- **建议去向**：若目标对齐一线 CEX 现货产品线则立项；设计上先明确与 perp cross margin / unified margin 的关系，避免两套保证金引擎。

### 4.6 运营与市场生命周期（P1）

- **现状**：admin-gateway 有 symbol CRUD + 批量撤单 + 审计（ADR-0052）；perp 侧 symbol lifecycle 状态机（TRADING / POST_ONLY / CANCEL_ONLY / DELISTED）+ versioned config catalog 已随 ADR-0075 落地。
- **缺口**：完整**上币流程**（预热期、集合竞价 / 开盘价保护、首日涨跌幅限制）、下架清退流程、公告与维护窗口机制、现货侧 cancel-only / reduce-only 市场状态编排、风险参数 staged rollout（前文 Gap Matrix "Admin / market ops" 行）。
- **建议去向**：把 ADR-0075 已落地的 symbol lifecycle 状态机推广到现货，并补上币开盘保护（集合竞价 / 涨跌幅限制）机制。

### 4.7 财务与对账报表（P1）

- **现状**：Counter 小时级内存 vs MySQL 对账；perp 手续费账本已成体系（ADR-0079：平台手续费账户 + SUM 对账不变量 + 日统计）。
- **缺口**：现货手续费的平台账户账本（前文已指出现货侧未成体系）、资金证明（proof of reserves，储备金证明）、税务/监管导出、日终结算报表、跨服务资金守恒对账（Counter + asset + 钱包三方）。
- **建议去向**：现货 fee 对齐 ADR-0079 的模式（平台账户 + 对账不变量）；报表走 trade-dump 投影扩展，不动热路径。

### 4.8 客服支撑工具（P1/P2）

- **现状**：无面向客服/运营的查询工具；观测性文档仍是 Draft。
- **缺口**：订单全链路调查（按 order_id 串起 BFF→Counter→Match→Settlement→Push 的事件轨迹）、资金流水导出、带审计的冲正/调账流程（[non-goals.md](../non-goals.md) 正确禁止了 admin 直改余额，因此需要一条走 `Transfer` + 审计的正规通道与配套工具）。
- **建议去向**：与 §6.2 可观测性的 correlation 查询共用地基；冲正流程作为 admin-gateway 扩展 + 审计强制。

## 5. 交易内核的产品缺口（多数已有 ADR，Proposed 未实现）

按状态归拢；详细论证见前文 Gap Matrix 与各 ADR。

| 缺口 | 状态 |
|---|---|
| 统一保证金 / portfolio margin（isolated + USDT cross 已随 ADR-0074 落地，unified 是其后续路径） | [ADR-0074](../adr/0074-perp-account-margin-modes.md) 后续路径，未实现 |
| 交割合约（linear dated futures、结算流程） | [ADR-0076](../adr/0076-perp-contract-product-expansion.md) Proposed |
| 订单准入风控 / 价格带保护（price band、collar、min notional、OI 上限） | [ADR-0080](../adr/0080-perp-admission-risk-price-protection.md) Proposed |
| reduce-only 成交回流硬约束（clamp/reject，防反向开仓） | [ADR-0081](../adr/0081-perp-reduce-only-settlement-hardening.md) Proposed |
| 现货 amend / batch 订单 API | [ADR-0085](../adr/0085-spot-order-product-api.md) Proposed |
| 私有推送合并策略（一笔 taker 吃 N maker 的消息风暴） | [ADR-0084](../adr/0084-private-push-merge-strategy.md) Proposed |
| 机构级 STP/SMP（per-order 模式、trade group、decrement 策略） | 仅有 `STPRejectTaker` 雏形（默认 `STPNone`），无 ADR；依赖 §4.3 子账户分组 |
| Kill switch / DCP（disconnect cancel protection，断线自动撤单）/ countdown cancel-all | 无 ADR（用户主动 cancel-all 已有：spot `CancelMyOrders`、perp cancel-all by coin；缺的是 session/heartbeat 级自动触发）；前文 §3 建议 P1 |
| Pre-check / dry-run 下单试算（预估冻结、IMR/MMR） | perp 已随 ADR-0078 落地 pre-check；现货侧无，可并入 ADR-0085 |
| 高级订单：iceberg、pegged、OTO/OTOCO、amend keep-priority | 无（OCO / trailing / post-only 已有） |
| GTD（good-till-date，指定到期时间）TIF | 无（现有 GTC/IOC/FOK/POST_ONLY；trigger 层有 TTL，但 book 内订单没有到期语义） |
| ADL 队列位置披露、账户风险率等用户侧风险可见性 API | 风险引擎有，产品 API 无 |
| MMP（market maker protection，做市商保护）、mass quote、RFQ | 无（perp block trade MVP 已随 ADR-0078 落地）；做期权 / 机构业务前不急（P2） |
| 期权、投资组合保证金（portfolio margin） | 长期方向（P2） |

## 6. 架构 / 生产化缺口（横切面）

### 6.1 生产安全 hardening（P0）

[security.md](../security.md) 的 TODO 清单全部未完成：BFF per-user / per-IP 限流、Kafka TLS/SASL、MySQL/etcd TLS、服务间 mTLS、snapshot 文件加密与权限（`0600`）、secret manager、日志脱敏、协议层 nonce 防重放（S4 场景：5 秒签名窗口内同请求可重放）。这是接公网流量前的硬门槛，前文 §1 已建议单独立 "production security hardening" ADR。

### 6.2 可观测性（P0）

[agent-first-observability.md](../agent-first-observability.md) 仍是 Draft；metrics / trace / 告警未闭环。缺订单全链路追踪：一笔订单跨 BFF→Counter→Kafka→Match→Kafka→Counter→Push 的 correlation 查询——它同时是 §4.8 客服订单调查工具的地基。

### 6.3 性能证明（P0）

架构目标 20 万 TPS / 10ms P99；[ADR-0082](../adr/0082-match-counter-benchmark-methodology.md) 只有方法论，benchmark 结果目录为空。应在 perp 产品化主线之前跑出基线，否则后续每个功能都无法量化自己的延迟预算消耗。

### 6.4 容灾与部署（P1）

当前假设单机房、snapshot 走共享 NFS mount、docker-compose + 本地进程 smoke。缺：多机房 DR（disaster recovery，灾难恢复）、Kafka/MySQL/etcd 备份与恢复演练、编排化部署（k8s）、滚动发布、staged config rollout。Kafka 集群扩容已有 [ADR-0059](../adr/0059-kafka-cluster-scaling-and-match-output-dispatch.md) 长期调研。

### 6.5 数据生命周期（P1）

trade-dump 的 MySQL 投影与 Kafka topic 均无归档 / 分区 / retention 策略（[architecture.md §19](../architecture.md) 只有一行"冷热分层"远期项）。真实流量下 `trades` / `account_logs` 是无界增长表。

### 6.6 协议契约产品化（P1/P2）

WS 私有流缺 `(topic, seq)` gap detection + 补齐闭环（non-goals 已拒绝服务端 replay，因此 gap recovery 契约更要写实，用 snapshot + history 补齐）；REST 错误码体系、API 文档从索引升级到 schema 级；FIX/SBE 是机构接入长期项。见前文 §6 "Protocol And Recovery Contract"。

### 6.7 测试深度（P1）

单测 / 集成 / smoke 已有。缺：chaos / 故障注入（primary 中途 kill、Kafka 分区不可用、MySQL 慢查询下的 backpressure）、**确定性回放测试**（同一 journal 重放两次，最终状态必须逐字节一致——snapshot + replay 架构最值得加的守护性测试）、多日 soak。

## 7. 分流记录

§4 各条已按流程登记进 [feature-requests.md](../feature-requests.md)（2026-07-05，来源 dev/user，状态 pending），后续从那里分流到 roadmap / ADR / rejected。§5 / §6 中已有 ADR 或已在 roadmap 待办的条目不重复登记。

## 8. 推进顺序建议

前文 §推荐实施顺序仍然成立，调整一点：**若"完整交易所"意味着接真实用户与真实资金，圈层 2/3（用户体系 + 钱包充提）应提前立项**——它们是全新业务域、开发周期最长，且与内核解耦、可并行。

```
                        ┌──────────────────────────────┐
                        │ 目标 = 接真实用户 / 真实资金？  │
                        └──────────┬───────────────────┘
                        yes        │        no（继续工程验证）
              ┌────────────────────┴──────────┐
              v                               v
  并行两条线：                          单线推进内核 backlog：
  ┌─ 线 A（新业务域）─────────┐         1. P0 hardening（§6.1-6.3）
  │ 1. 用户服务 + sub-account │         2. STP/SMP + fee guard
  │ 2. 钱包/充提（KYC 留边界） │         3. perp 产品化收尾
  │ 3. 法币/合规实施          │            （0076/0080/0081）
  └───────────────────────────┘         4. 订单控制自保
  ┌─ 线 B（内核，与 A 并行）──┐            （DCP / pre-check / amend）
  │ 同右侧 1→4               │         5. 协议与市场数据产品化
  └───────────────────────────┘         6. 机构生态（MMP/RFQ/期权）
```
