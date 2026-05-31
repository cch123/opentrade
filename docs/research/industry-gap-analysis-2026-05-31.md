# 行业先进系统差距分析（2026-05-31）

> 目标：把 OpenTrade 当前工作树和公开可查的一线交易所 / 传统交易所能力放在同一张表里，判断“还没做到什么”。本文不是实现承诺；要落地的项仍需进入 ADR / roadmap。

## 范围与证据

### 当前 OpenTrade 基线

本轮基于当前工作树读取：

- [README.md](../../README.md)：OpenTrade 当前定位为“现货 CEX 核心系统”，MVP 闭环但不按生产可用发布。
- [docs/architecture.md](../architecture.md)：现货、perp、Counter/Match/Kafka/snapshot/HA/读写路径。
- [docs/roadmap.md](../roadmap.md)：MVP-0 到 MVP-17 已完成，以及 2026-05-31 起草的 perp / push / benchmark backlog。
- [docs/non-goals.md](../non-goals.md)：明确不做 hot-standby、Push replay、跨 shard 分布式事务等边界。
- [docs/security.md](../security.md)：安全威胁模型和生产前 gap。
- [api/rpc/perp/perp.proto](../../api/rpc/perp/perp.proto)：perp 目前只实现 isolated wire shape，cross 预留。
- [api/event/common.proto](../../api/event/common.proto)：订单类型 / TIF / 状态枚举。
- [match/internal/engine/engine.go](../../match/internal/engine/engine.go)：STP 只有 `STPNone` 和 `STPRejectTaker`。
- [match/cmd/match/main.go](../../match/cmd/match/main.go)：当前 worker 默认 `STPNone`。

### 对照系统与公开来源

只用公开资料，不使用未授权私有材料。

- Binance Spot / Derivatives API：Spot 支持 OCO/OTO/OTOCO、iceberg、pegged orders、SOR、STP、SBE/FIX；USD-M futures 支持 hedge position side、reduceOnly、priceMatch、GTD、STP、batch orders、modify、countdown cancel-all、ADL quantile、portfolio margin API。来源：
  - https://developers.binance.com/docs/binance-spot-api-docs/rest-api/trading-endpoints
  - https://github.com/binance/binance-spot-api-docs
  - https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/New-Order
  - https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Modify-Order
  - https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Place-Multiple-Orders
  - https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Auto-Cancel-All-Open-Orders
  - https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Position-ADL-Quantile-Estimation
  - https://developers.binance.com/docs/derivatives/portfolio-margin/general-info
- Bybit V5 / UTA：统一账户覆盖 spot、margin、USDT/USDC perp/futures、inverse、options；下单支持 slippage tolerance、TP/SL、reduceOnly、closeOnTrigger、SMP、MMP（options）、RPI；另有 DCP、pre-check、cancel-all by symbol/base/settle coin。来源：
  - https://bybit-exchange.github.io/docs/v5/order/create-order
  - https://bybit-exchange.github.io/docs/v5/order/dcp
  - https://bybit-exchange.github.io/docs/v5/order/pre-check-order
  - https://bybit-exchange.github.io/docs/v5/order/cancel-all
  - https://bybit-exchange.github.io/docs/v5/smp
  - https://bybit-exchange.github.io/docs/v5/acct-mode
  - https://bybit-exchange.github.io/docs/v5/account/account-info
- OKX API v5：四类账户模式（Spot/Futures/Multi-currency/Portfolio margin）、net/long-short position mode、attached TP/SL / trailing stop、MMP、block trading / RFQ、options greeks、SBE market data。来源：
  - https://www.okx.com/docs-v5/en/
- Deribit / 机构衍生品：Block RFQ、options/futures block trading、MMP 按 quantity/delta/vega/trade-count 触发、API scopes。来源：
  - https://support.deribit.com/hc/en-us/articles/25951371614621-Deribit-Block-RFQ
  - https://support.deribit.com/hc/en-us/articles/25951393746589-Deribit-Block-RFQ-API-walkthrough
  - https://support.deribit.com/hc/en-us/articles/25944688627229-Block-Trading
- 传统交易所低延迟 / 机构协议：Nasdaq OUCH / FPGA OUCH / Dedicated OUCH，Nasdaq SMP 多层匹配 ID。来源：
  - https://nasdaqtrader.com/Trader.aspx?id=OUCH
  - https://www.nasdaq.com/docs/self-match-prevention-overview
- CME Globex：全球 24 小时电子交易、iLink/MDP3/Drop Copy/风险管理工具/认证接入。来源：
  - https://www.cmegroup.com/solutions/market-access/globex.html

## 总体判断

OpenTrade 现在的强项是“核心撮合 + 账户一致性 + 事件回放 + MVP perp 风险骨架”。这已经超过普通 demo 交易所：Counter/Match 分责、Kafka EOS、snapshot-offset 原子性、vshard failover、trigger OCO/trailing、history projection、admin-gateway 隔离、perp liquidation/backstop/ADL 都有成体系的 ADR 和代码。

和行业头部系统相比，主要缺口不在“能不能撮合一笔订单”，而在六个生产维度：

1. **产品面**：订单族、perp 账户模式、统一保证金、期权、RFQ/block、做市商工具还不完整。
2. **机构控制面**：SMP/STP 组、MMP、DCP/kill switch、pre-check、OTV/fill-ratio 风控、sub-account / beneficial-owner 维度不足。
3. **性能与低延迟面**：有目标和 ADR，但没有可复现 benchmark 结果；热路径仍是 Kafka-first，未证明 20w TPS / 10ms P99。
4. **生产安全面**：BFF rate limit、mTLS、Kafka/MySQL/etcd TLS、API-key lifecycle、不可篡改审计仍是 gap。
5. **协议生态面**：缺 FIX/SBE/低延迟 order-entry、二进制行情、Drop Copy、恢复序号契约和更完整的 API 错误码。
6. **合规/资金面**：链上充提、KYC/AML/Travel Rule、平台手续费账户、税务/报表、资产托管不在核心闭环里。

## Gap Matrix

优先级含义：

- **P0**：生产外部流量前应解决或明确降级策略。
- **P1**：做“成熟交易所产品”需要，能分阶段做。
- **P2**：面向机构 / 头部规模 / 衍生品扩张，短期可作为非目标或长期方向。

| 领域 | 先进系统基线 | OpenTrade 现状 | 缺口判断 | 优先级 | 建议去向 |
|---|---|---|---|---|---|
| 生产安全 | Binance / OKX / Bybit 都有 API key 权限、IP / order rate limit、WAF / ban、签名窗口；生产内网服务默认要求 TLS / auth / 最小权限 | [docs/security.md](../security.md) 已列 BFF rate limit、Kafka/MySQL/etcd TLS、mTLS、API-key rotation、secret manager、snapshot 加密等未完成项 | 明确未达到公网生产安全基线 | P0 | 单独开 “production security hardening” ADR / roadmap |
| 性能证明 | 头部系统公开强调低延迟接入；Nasdaq OUCH / FPGA OUCH 面向确定性低延迟；CEX 也常提供 FIX/SBE | 架构目标 20w TPS / 10ms P99；[ADR-0082](../adr/0082-match-counter-benchmark-methodology.md) 只有方法，历史 benchmark 为空 | 目标未被证据证明 | P0 | 先做 benchmark driver 和结果归档 |
| STP / SMP | Binance / Bybit / Nasdaq 都支持可配置自成交防护，通常覆盖账号组、机构组、sub-account / MPID / ORG / affiliate 等层级，并有 cancel maker/taker/both/decrement 等策略 | Match 有 `STPRejectTaker`，但默认 `STPNone`；没有 per-order 模式、beneficial owner、trade group、maker/taker/both/decrement 策略 | 只具备算法雏形，不是机构级 SMP | P0（若上 maker rebate / 机构） | 新 ADR：STP/SMP groups + fee rebate 前置 |
| 订单准入 / 价格保护 | Binance futures `priceMatch`、`priceProtect`、GTD；Bybit market slippage tolerance 和 price limit；OKX 支持价格超限自动改价开关 | 现货 market slippage 主要在 BFF 翻译；[ADR-0080](../adr/0080-perp-admission-risk-price-protection.md)、[ADR-0083](../adr/0083-match-native-protected-market-order.md) proposed | 已识别但未实现 | P0/P1 | 先实现 Match 原生 protected market + perp admission |
| 订单生命周期控制 | Binance / Bybit 有 batch place/amend/cancel、modify order、modify history、countdown cancel-all / DCP；Bybit pre-check 返回下单后 IMR/MMR | Spot 有 `CancelMyOrders` 和 admin bulk cancel；改单按 cancel-new；perp `PerpService` 只有 place/cancel/query/positions/margin | 缺 amend、batch amend/cancel、GTD、countdown/DCP、pre-check | P1 | 扩 ADR-0078 或拆订单控制 ADR |
| 高级现货订单 | Binance Spot 有 OCO/OTO/OTOCO、iceberg、pegged orders、SOR、order amend keep priority | OpenTrade 有 OCO/trailing trigger；没有 iceberg/peg/OTO/OTOCO/keep-priority amend/SOR | CEX 现货产品面不足；SOR 若只做单 venue 可先非目标 | P1/P2 | 先补 iceberg/peg/OTOCO；SOR 标为长期或非目标 |
| Perp account modes | Binance/Bybit/OKX 支持 isolated/cross/portfolio or multi-currency margin、one-way/hedge、positionSide/position mode、multi-assets | `perp.proto` 注释说明 MVP implements isolated only，cross reserved；ADR-0074/0077 proposed | 已规划但未落地 | P1 | 按 ADR-0074/0077 实现 |
| Perp product expansion | Binance/Bybit 覆盖 USDT/USDC/inverse perp/futures/options；OKX 覆盖 SWAP/FUTURES/OPTION | 当前是 USDT linear perpetual core；linear dated futures / inverse / options 未实现 | 已识别一部分，options 尚未进入近期计划 | P1/P2 | ADR-0076 先 linear dated futures；options 单列长期研究 |
| Funding / fee / platform ledger | 头部系统支持 maker/taker/vip/user/symbol fee、rebate、funding stats、平台收入账户 | Perp fee 是字段/占位；ADR-0079 proposed；现货 fee 也未形成平台账户完整账本 | 生产财务不完整 | P0/P1 | ADR-0079 落地前不要上线负 maker rebate |
| Liquidation / ADL product visibility | Binance 有 ADL quantile API；Bybit/OKX 有账户级风险率、逐步强平和风险提示 | OpenTrade 已有 partial liquidation、backstop、ADL/perp-risk；history proto 有 perp funding/liquidation 字段；用户侧 ADL quantile / 风险解释 API 不完整 | 风险引擎骨架强，产品可见性不足 | P1 | 增加风险视图 API 和 ADL queue disclosure |
| Market maker protection | OKX / Bybit / Deribit options 支持 MMP；Deribit Block RFQ MMP 可按 quantity/delta/vega/trade-count | 无 MMP、mass quote、quote group、market-maker session 风险 | 做市商工具缺口明显 | P2（若做机构/期权则 P0） | 新 ADR：MMP / mass quote / quote risk |
| Block / RFQ / spread | OKX / Deribit 有 block trading、RFQ、multi-leg / combo 生态 | ADR-0078 提到 block trade，但未实现；无 RFQ/spread/combos | 机构大宗交易缺口 | P2 | 等 perp 基础成熟后再做 |
| API protocol ecosystem | Binance 官方列 REST、WebSocket、FIX、SBE；Nasdaq/CME 使用专业接入协议和 Drop Copy | REST/WS + internal gRPC；无 FIX/SBE/二进制行情/Drop Copy；[ADR-0084](../adr/0084-private-push-merge-strategy.md) 只覆盖 WS batching | 对机构 / HFT 不够 | P2 | 先把 WS seq recovery 做实，再评估 FIX/SBE |
| Market data | 头部系统有增量深度、逐笔、K 线、SBE / multicast / historical tick download | 有 quote depth/trade/kline 和 push；无二进制行情、全量历史 tick 下载、深度 gap recovery 协议文档 | MVP 够用，机构数据产品不足 | P1/P2 | API docs + recovery contract + historical export |
| Observability / ops | 生产系统需要 metrics/log/trace/incident tooling；CME/传统交易所还有认证测试、GCC/ops 流程 | [agent-first-observability.md](../agent-first-observability.md) 是 Draft；多数服务 metrics/trace 未闭环 | 可观测性仍是设计稿 | P0 | 做 JSONL log、metrics、trace、order lifecycle runbook |
| Admin / market ops | 头部系统支持 symbol lifecycle、cancel-only/reduce-only、risk config staged rollout、dry-run | `admin-gateway` 有 symbol CRUD、批量撤单、审计；perp symbol config/status 仍 proposed | 现货 admin 初具备；perp / risk rollout 不够 | P1 | 实现 ADR-0075 staged config 和 symbol state |
| Funding wallet / custody / compliance | 完整交易所需要链上充提、KYC/AML、Travel Rule、冷热钱包、资金证明、税务/报表 | Asset service 有 funding wallet / internal transfer saga；README / non-goals 明确链上钱包对接不在 MVP | 核心交易系统外的生产缺口 | P0（若公网真实资金） | 独立资金/合规路线图，不要混进 Match/Counter |

## 已覆盖得比较好的部分

这些不是缺口，后续应保持：

- **账户 / orderbook 职责边界清楚**：Counter 是账户真值，Match 是 orderbook 真值，符合交易系统常见分层。
- **事件与恢复纪律强**：Kafka source-of-truth、typed sequence、snapshot-offset 原子性、trade-dump projection、counter shadow snapshot pipeline 都比普通 MVP 扎实。
- **触发单能力不弱**：stop/take-profit、OCO、trailing、TTL、reservation、trigger HA 和 long-term history 已经覆盖一线交易所常见条件单骨架。
- **运维入口独立**：admin-gateway 与 BFF 分进程，避免 2C 和 ops 混用。
- **perp 风险骨架已经进入成熟区**：USDT linear perp、mark/funding、partial liquidation、backstop、ADL、perp-risk coordinator 已有基础，剩下更多是产品化和账户模式。

## 已在 ADR / roadmap 中覆盖但未实现

这些不需要重新“发现”，需要实现：

- Perp SymbolConfig 产品化：[ADR-0075](../adr/0075-perp-symbol-config-productization.md)
- Linear dated futures / settlement：[ADR-0076](../adr/0076-perp-contract-product-expansion.md)
- Hedge / both-side position mode：[ADR-0077](../adr/0077-perp-position-mode-hedge-both-side.md)
- Perp 订单与持仓 API：[ADR-0078](../adr/0078-perp-order-position-product-api.md)
- Fee accounting / rebate / platform accounts：[ADR-0079](../adr/0079-perp-fee-accounting.md)
- Perp admission risk / price protection：[ADR-0080](../adr/0080-perp-admission-risk-price-protection.md)
- reduce-only settlement hardening：[ADR-0081](../adr/0081-perp-reduce-only-settlement-hardening.md)
- Match / Counter benchmark：[ADR-0082](../adr/0082-match-counter-benchmark-methodology.md)
- Native protected market order：[ADR-0083](../adr/0083-match-native-protected-market-order.md)
- Private push batching：[ADR-0084](../adr/0084-private-push-merge-strategy.md)

## 还没有被充分覆盖的新缺口

这些建议进入新的 roadmap / ADR：

### 1. Production Security Hardening（P0）

范围：

- BFF REST/WS per-user + per-IP rate limit。
- API-key 管理面：创建、禁用、scope、IP 白名单、rotation、最后使用时间、审计。
- Kafka TLS/SASL、MySQL TLS + 最小权限、etcd TLS + client cert、服务间 mTLS。
- snapshot 加密 / 签名 / `0600` 权限。
- 不可篡改审计 sink：admin、asset transfer、risk config、fee config、manual intervention。

理由：这不是“加功能”，是接公网和真实资金前的基本门槛。

### 2. Institutional STP / SMP（P0/P1）

范围：

- per-order `self_trade_prevention_mode`：none / expire taker / expire maker / expire both / decrement。
- `trade_group_id` / `beneficial_owner_id` / parent-subaccount group。
- Match 侧按 taker order 的策略执行，结果写入 trade-event / order-event。
- Fee engine 使用同一 owner/group 判定 rebate eligibility。

理由：当前同用户自成交可以被正确结算，但不是“防止自成交”；负 maker rebate、机构子账户和做市策略上线前必须补。

### 3. Kill Switch / DCP / Countdown Cancel-All（P1）

范围：

- 用户维度 cancel-all by symbol / settle asset / product。
- WS heartbeat 驱动的 disconnect cancel all（DCP）。
- REST countdown cancel-all：客户端持续续租，过期自动撤指定 symbol 的开放订单。
- `cancel-only` / `reduce-only` 市场状态下的操作豁免规则。

理由：这是做市商和 API 用户自保能力。OpenTrade 有 admin bulk cancel 和 user cancel-my-orders，但没有 session-level / heartbeat-level kill switch。

### 4. Pre-Check / Dry-Run Order（P1）

范围：

- 接受与 `PlaceOrder` 同形状的请求，但不入 book、不冻结、不发 Kafka。
- 返回预估冻结、IMR/MMR、price band、fee buffer、最大可开 / 可平、拒绝原因。
- 对 perp 与 spot 分开实现；perp 可先覆盖 isolated。

理由：Bybit pre-check 一类能力对 UI、API client、做市商都很有价值，也能降低无效下单对系统的压力。

### 5. Market Maker Protection / Quote Controls（P2，做期权或机构则 P0）

范围：

- MMP group：按用户 / instrument family / quote group 统计成交 qty、delta、vega、trade count。
- 触发后自动撤 MMP-tagged orders，并在 frozen interval 内拒绝新 MMP order。
- mass quote / mass cancel API。
- order-to-trade ratio / fill-ratio 指标和限流。

理由：这是 options / RFQ / 专业做市生态的核心保护。若 OpenTrade 不做期权，可长期放 P2。

### 6. Protocol And Recovery Contract（P1/P2）

范围：

- 对外 REST / WS API 文档从索引升级到 schema、错误码、rate limit、重试语义。
- WS 私有流使用 `(topic, partition, seq)` gap detection，与 history / snapshot 补齐路径闭环。
- Drop Copy / execution report 专用流。
- 中长期评估 FIX/SBE 或至少 Protobuf binary WS。

理由：当前 API 足够本地 smoke，但不够多 client / 机构接入。

### 7. Market Data Productization（P1/P2）

范围：

- 深度增量 recovery 明确 `lastUpdateId` / seq contract。
- historical tick / depth / klines export。
- 二进制行情或 SBE schema。
- 数据质量监控：gap、stale quote、book crossed、kline gap。

理由：Quote/Push 已有基础，但市场数据作为产品还不完整。

### 8. Compliance / Real-Funds Envelope（P0 if real money）

范围：

- KYC/AML/Travel Rule / sanctions screening 接口边界。
- 链上充提、冷热钱包、审批、限额、异常风控。
- 平台资金账户、手续费账户、保险基金账户、用户资金隔离账。
- 财务报表、税务、监管导出。

理由：这不属于 Match/Counter 内核，但属于“交易所系统”和真实资金上线条件。

## 推荐实施顺序

如果目标是从“工程验证”走向“可接真实外部用户”，建议顺序如下：

1. **P0 hardening**：security、rate limit、API-key lifecycle、audit、benchmark、basic observability。
2. **STP/SMP + fee guard**：在负 maker rebate、VIP fee、机构 subaccount 之前完成。
3. **Perp 产品化主线**：ADR-0075/0074/0077/0078/0079/0080/0081 依赖关系收敛。
4. **订单控制与风险自保**：amend/batch/GTD/DCP/pre-check/protected market。
5. **协议和市场数据产品化**：WS seq recovery、API docs、binary feed、historical exports。
6. **机构衍生品生态**：MMP、block/RFQ、options、portfolio margin。

## 不建议马上做的方向

- **直接替换 Kafka 热路径为 UDP/Aeron**：已有 [cex-counter-match-aeron-research.md](./cex-counter-match-aeron-research.md) 说明长期方向，但当前先跑 ADR-0082 benchmark；只有 Kafka 成为硬瓶颈时再切。
- **一上来做 portfolio margin / options**：先把 isolated perp 的 fee、risk config、reduce-only、position mode 和 settlement 做稳。
- **做 SOR**：如果 OpenTrade 不是多 venue 聚合路由，SOR 没有清晰产品意义；可以先标为非目标或长期。
- **Push 服务端历史 replay**：已在 [non-goals.md](../non-goals.md) 拒绝。要做 gap recovery，优先用 snapshot + history 补齐。
