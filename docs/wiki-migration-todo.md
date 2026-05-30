# OpenTrade 文档库 LLM Wiki 改造 TODO

本文记录把现有 `docs/` 优化成 LLM wiki 风格知识库的执行计划。先作为待评审 TODO 存放，不改变当前正式文档入口。

## 目标

- 给人和 LLM 一个稳定的“当前系统知识地图”，不用从几十篇 ADR 和长架构文档里倒推出系统现状。
- 保留 ADR / runbook / research / API 契约作为 source of truth；wiki 层只负责组织、摘要、交叉引用和导航。
- 让每个核心概念、服务、状态模型、接口契约、行为语义、流程都有短页说明，并能反向链接到代码入口和相关 ADR。
- 明确“当前行为”和“历史决策”的区别，避免把 ADR 历史误读成系统现状。

## 分类规则

这些分类不是按文件类型划分，而是按读者正在问的问题划分。

| 分类 | 回答的问题 | 典型内容 |
|---|---|---|
| Concepts | 这个概念是什么，为什么存在？ | source of truth、reservation、vshard、snapshot offset、mark price |
| Services | 这个服务负责什么，边界在哪？ | Counter、Match、BFF、trade-dump、Trigger、perp-counter |
| State / Data Model | 系统有哪些状态？谁拥有？怎么持久化？ | Account、Order、Position、Reservation、Snapshot、Projection |
| Contracts | 接口和事件长什么样？兼容性怎么保证？ | REST、WebSocket、gRPC proto、Kafka event、MySQL projection schema |
| Behavior / Semantics | 系统在特定输入、状态和边界条件下应该怎么表现？ | 订单状态机、撮合规则、订单类型语义、结算语义、WS 可见性保证 |
| Flows | 一件事端到端怎么发生？ | 下单、撤单、snapshot recovery、market-data fanout、资金费结算 |
| Invariants | 什么规则永远不能破？ | 账户真值、orderbook 真值、per-user FIFO、offset 原子性 |
| Operations | 怎么运行、验证、排障？ | dev setup、smoke、runbook、benchmarks、postmortems |
| Verification | 怎么证明行为没坏？ | 单测、race、smoke、determinism、replay、对账、invariant tests |
| Evolution / Migration | 协议、schema、分片、拓扑变更时怎么演进？ | proto 兼容、topic 演进、vshard rebalance、SymbolConfig 迁移 |
| Decisions | 为什么这么设计？以前考虑过什么？ | ADR 当前有效决策图、superseded 决策链 |
| Research | 什么还在调研或未定论？ | 外部方案对比、性能调研、候选设计 |
| Planning | 当前要做什么，什么还没做？ | roadmap、bugs、feature requests、open questions |

## 目录策略

- [ ] 第一轮不强制建很多子目录，先用 `docs/wiki/index.md` 按分类组织 flat pages，降低迁移成本。
- [ ] 如果 wiki 页超过 40 篇，再考虑把 `docs/wiki/` 拆成 `concepts/`、`services/`、`behavior/` 等子目录。
- [ ] `docs/index.md` 作为对外总入口；`docs/wiki/index.md` 作为知识地图入口。
- [ ] `README.md` 只保留最短项目介绍和关键入口，避免继续扩成二级文档中心。

## 优先级

| 优先级 | 范围 | 判断标准 |
|---|---|---|
| P0 | 入口、系统地图、服务边界、状态真值、关键行为、核心链路 | 新人和 LLM 能建立正确心智模型 |
| P1 | 契约、验证、运维、决策地图 | 能安全改代码、跑验证、追设计依据 |
| P2 | 迁移演进、research 整理、planning 汇总、知识图谱自动检查 | 降低长期维护成本 |

## Phase 0: 盘点现状

- [ ] 统计 `docs/` 下所有 Markdown 文件，标注所属分类。
- [ ] 标记“当前事实源”文档：`architecture.md`、`adr/`、`api/`、runbook、research、proto、schema。
- [ ] 找出过长文档里的主题边界，优先处理 `docs/architecture.md`。
- [ ] 找出已过期、重复、或被 ADR supersede 的说明，先记录，不急着删除。
- [ ] 检查现有 ADR 状态，特别是 `Superseded by` 是否都能链到新 ADR。
- [ ] 检查重复编号问题，例如 `0063` 同时出现两个文件时是否需要说明或修正。

## Phase 1: 建 wiki 骨架

- [ ] 新增 `docs/index.md` 作为文档总入口。
- [ ] 新增 `docs/wiki/README.md`，说明 wiki 层和 ADR / runbook / research / contracts 的职责边界。
- [ ] 新增 `docs/wiki/index.md`，按 Concepts / Services / State / Contracts / Behavior / Flows / Invariants / Operations / Verification / Evolution / Decisions / Research / Planning 组织目录。
- [ ] 定义 wiki 页模板，固定包含：
  - `What it is`
  - `Why it exists`
  - `Owned state`
  - `Inputs / Outputs`
  - `Behavior`
  - `Critical invariants`
  - `Failure / recovery behavior`
  - `Code entry points`
  - `Related contracts`
  - `Related ADRs`
  - `Related pages`
- [ ] 约定 wiki link 写法：正文可用 `[[Counter]]` 这种 LLM 友好形式，但必须同时保留普通 Markdown 链接，保证 GitHub 可读。

## Phase 2: P0 核心地图

- [ ] `docs/wiki/system-overview.md`：当前系统一页图，说明核心服务、真值边界和事件流。
- [ ] `docs/wiki/service-map.md`：服务职责、owned state、输入输出、代码目录。
- [ ] `docs/wiki/source-of-truth.md`：Counter / Match / MySQL / Kafka / Asset funding / perp-counter 的真值边界。
- [ ] `docs/wiki/kafka-topics.md`：topic、partition key、producer、consumer、事务边界。
- [ ] `docs/wiki/order-lifecycle.md`：下单、撮合、结算、推送的端到端流程。
- [ ] `docs/wiki/snapshot-and-recovery.md`：snapshot offset atomicity、冷启动、catch-up、fallback。
- [ ] `docs/wiki/perp-overview.md`：USDT linear perp 的最小可用集和现货系统的关系。

## Phase 3: P0 服务页

- [ ] `docs/wiki/counter.md`：Counter 的账户真值、user sequencer、journal、snapshot、vshard。
- [ ] `docs/wiki/match.md`：Match 的 orderbook 真值、per-symbol actor、撮合输出。
- [ ] `docs/wiki/bff.md`：REST / WS 接入、鉴权、限流、查询路由和推送桥接。
- [ ] `docs/wiki/trade-dump.md`：MySQL projection、snapshot pipeline、on-demand snapshot。
- [ ] `docs/wiki/quote.md`：行情投影、depth / trades / kline、market-data 生产。
- [ ] `docs/wiki/push.md`：WebSocket fanout、sticky routing、coalesce、私有/公共推送。
- [ ] `docs/wiki/trigger.md`：触发单状态、market-data 输入、counter 下单、trigger-event。
- [ ] `docs/wiki/asset.md`：funding wallet、AssetHolder、transfer saga。
- [ ] `docs/wiki/history.md`：MySQL projection 只读查询、cursor、BFF 列表查询。
- [ ] `docs/wiki/admin-gateway.md`：内部运维入口、symbol 管理、灰度、审计。
- [ ] `docs/wiki/perp-counter.md`：合约保证金账户、仓位、资金费、强平检测。

## Phase 4: P0 状态与数据模型

- [ ] `docs/wiki/account-model.md`：现货账户余额、available / frozen、日志与投影。
- [ ] `docs/wiki/order-model.md`：订单字段、内部状态、外部状态、终态保留策略。
- [ ] `docs/wiki/reservation-model.md`：Reservation 和 Transfer.FREEZE 的边界。
- [ ] `docs/wiki/snapshot-model.md`：snapshot 内容、offset、blobstore、调试格式。
- [ ] `docs/wiki/projection-model.md`：trade-dump MySQL orders / trades / account_logs / triggers 的语义。
- [ ] `docs/wiki/position-model.md`：perp position、collateral pool、margin、funding_round_seen。

## Phase 5: P0 行为语义

- [ ] `docs/wiki/order-semantics.md`：订单类型、TIF、Post-Only、IOC/FOK、Market、clientOrderId 幂等的行为定义。
- [ ] `docs/wiki/order-state-machine.md`：内部订单状态、外部可见状态、状态迁移、终态语义。
- [ ] `docs/wiki/matching-semantics.md`：价格时间优先、自成交保护、partial fill、cancel race 的行为。
- [ ] `docs/wiki/settlement-semantics.md`：冻结、解冻、成交结算、手续费、余额可见性的语义。
- [ ] `docs/wiki/visibility-semantics.md`：REST accepted、WebSocket 推送、History projection 之间的一致性和延迟语义。
- [ ] `docs/wiki/trigger-order-semantics.md`：Stop、Take Profit、Trailing Stop、OCO、TTL、reservation 的行为。
- [ ] `docs/wiki/perp-semantics.md`：逐仓保证金、reduce_only、资金费、强平检测、mark price 的行为。

## Phase 6: P1 契约层

- [ ] `docs/wiki/api-contracts.md`：REST / WS / gRPC 的契约入口和兼容性规则。
- [ ] `docs/wiki/rest-contracts.md`：BFF REST 的请求、响应、错误、accepted vs finalized 语义。
- [ ] `docs/wiki/ws-contracts.md`：订阅、推送消息、重连补齐、顺序和丢弃语义。
- [ ] `docs/wiki/grpc-contracts.md`：内部 RPC、调用方、幂等字段、错误语义。
- [ ] `docs/wiki/event-contracts.md`：Kafka event 的字段语义、前态/后态、sequence、partition key。
- [ ] `docs/wiki/mysql-projection-contracts.md`：MySQL projection 表的读语义、延迟、唯一键和幂等写。
- [ ] 明确契约页引用 proto / schema / ADR，wiki 页不复制完整字段定义。

## Phase 7: P1 Invariants 与 Verification

- [ ] `docs/wiki/invariants.md`：当前系统最重要的不变量总表。
- [ ] `docs/wiki/verification-index.md`：单测、race、smoke、determinism、replay、对账的入口。
- [ ] `docs/wiki/determinism-tests.md`：Match deterministic behavior、orderbook replay 的验证方式。
- [ ] `docs/wiki/replay-and-recovery-tests.md`：journal replay、snapshot recovery、offset guard 的验证方式。
- [ ] `docs/wiki/reconciliation.md`：Counter / trade-dump / History / Asset 的对账思路。
- [ ] `docs/wiki/arch-guards.md`：把现有 `docs/arch-guards.md` 纳入 wiki 导航，不急着搬内容。

## Phase 8: P1 Operations 与 Observability

- [ ] 把 `docs/dev-setup.md`、`docs/smoke.md`、`docs/runbook-counter.md` 纳入 `Operations` 导航。
- [ ] `docs/wiki/operations-index.md`：按“本地启动 / 冒烟 / 指标 / 排障 / 复盘 / benchmark”组织入口。
- [ ] `docs/wiki/observability.md`：指标、日志、lag、snapshot freshness、projection delay、WS fanout 健康度。
- [ ] `docs/wiki/security-and-trust.md`：信任边界、auth 模式、admin 权限、API-Key / JWT。
- [ ] 检查 runbook 是否引用了已经迁移或废弃的指标。
- [ ] 后续如新增 `runbook-trade-dump.md`、`runbook-trigger.md`，统一挂到 Operations。

## Phase 9: P1 Decisions 层

- [ ] 新增 `docs/wiki/decision-map.md`，按主题总结当前有效决策。
- [ ] 为每个主题列出“当前采用方案”和“被替代方案”，例如：
  - Counter sharding: 旧固定 shard -> vshard + owner lock
  - Market order: client-side translation -> native server-side
  - Snapshot: backup node -> trade-dump snapshot pipeline / on-demand
- [ ] 在 `docs/adr/README.md` 保持原有主题索引，不把它改成 wiki 页。
- [ ] 每个服务页和行为页必须链接到相关 ADR，而不是复制 ADR 的历史论证。

## Phase 10: P2 Evolution / Migration

- [ ] `docs/wiki/evolution-index.md`：所有演进和迁移类文档入口。
- [ ] `docs/wiki/proto-evolution.md`：proto 字段兼容、新增 enum、生成代码、调用方升级规则。
- [ ] `docs/wiki/event-evolution.md`：Kafka topic / event schema / sequence 字段演进规则。
- [ ] `docs/wiki/schema-migration.md`：MySQL projection schema 变更和回填策略。
- [ ] `docs/wiki/sharding-migration.md`：counter vshard rebalance、match symbol migration、push sticky 变更。
- [ ] `docs/wiki/symbol-config-migration.md`：SymbolConfig 迁移、精度变更、灰度和回滚。

## Phase 11: P2 Research 与 Planning

- [ ] `docs/wiki/research-index.md`：已完成 / 进行中 / 作废调研入口。
- [ ] `docs/wiki/open-questions.md`：跨 ADR / research 的未决问题，不混进 roadmap。
- [ ] `docs/wiki/planning-index.md`：roadmap、bugs、feature requests、postmortems action items 的统一入口。
- [ ] 将 `docs/bugs.md`、`docs/feature-requests.md`、`docs/roadmap.md` 纳入 Planning，但不在第一轮改内容。

## Phase 12: 质量检查

- [ ] 加一个轻量脚本检查 Markdown 链接是否断掉。
- [ ] 检查 wiki 页是否存在孤立页，没有被 `docs/wiki/index.md` 引用的要补入口或删除。
- [ ] 检查 ADR 状态是否只使用约定状态：`Proposed`、`Accepted`、`Superseded by NNNN`、`Deprecated`。
- [ ] 检查 wiki 页是否都包含 `Related ADRs`、`Related contracts` 和 `Code entry points`。
- [ ] 可选：生成 `.understand-anything/knowledge-graph.json`，用知识图谱看是否有明显孤岛。

## 不做

- [ ] 不把 ADR 重写成 wiki 页；ADR 保留历史决策语境。
- [ ] 不在第一轮重排大量文件路径，避免链接大面积失效。
- [ ] 不删除 research 里的未定论材料，只在 wiki 中标注“未决 / 参考”。
- [ ] 不把长架构文档拆碎到无法单独阅读；先用 wiki 作为导航层。
- [ ] 不在 wiki 页复制完整 proto / SQL schema；契约页只做语义导航，字段定义仍以源码为准。

## 完成标准

- [ ] `README.md` 的文档入口能指向 `docs/index.md`。
- [ ] 新人可以从 `docs/index.md` 进入，在 10 分钟内理解服务边界、状态真值和核心交易链路。
- [ ] LLM 可以通过 wiki 页快速定位相关 ADR、接口契约、代码目录、行为语义和运行文档。
- [ ] 核心系统知识不再只埋在 `architecture.md` 和 ADR 历史里。
