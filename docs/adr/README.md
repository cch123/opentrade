# Architecture Decision Records (ADR)

本目录记录 OpenTrade 项目所有重大架构与技术决策。每个决策一个独立文件，编号递增，不重用不删除（作废用 `Superseded` 标记并链到新决策）。

## 什么时候写 ADR

- 任何影响系统拓扑、数据流、持久化模型、一致性假设的决定
- 影响跨模块接口或协议的决定
- 影响长期可维护性或升级路径的技术选型
- 评估过多个选项后做出的选择（记录被拒绝的选项和原因）
- 用户和开发者讨论后达成共识的技术点

## 什么时候不写 ADR

- 模块内部实现细节（写代码注释即可）
- 临时的 workaround（写代码注释 + TODO）
- 纯业务规则（写业务文档）

## 流程

1. 复制 [`template.md`](template.md) → `NNNN-kebab-case-title.md`
2. 填写 Status=`Proposed` → 讨论 → `Accepted`
3. 在本 README 索引表末尾添加一行
4. 被后续 ADR 覆盖的老 ADR 改 Status=`Superseded by NNNN`，保留历史

## 状态说明

- `Proposed` — 提出中，待讨论
- `Accepted` — 已采纳，作为当前架构
- `Superseded by NNNN` — 被新决策取代（链接到新 ADR）
- `Deprecated` — 废弃但未被取代

## 索引

| 编号 | 标题 | 状态 | 日期 |
|---|---|---|---|
| [0001](0001-kafka-as-source-of-truth.md) | Kafka 作为事件权威源，不引入自建 WAL 或 Raft | Accepted | 2026-04-18 |
| [0002](0002-counter-ha-via-etcd-lease.md) | Counter 主备通过 etcd lease 选主，不用 Raft | Accepted | 2026-04-18 |
| [0003](0003-counter-match-via-kafka.md) | Counter 与 Match 通过 Kafka 通信，不走同步 RPC | Accepted | 2026-04-18 |
| [0004](0004-counter-journal-topic.md) | 引入 counter-journal 作为 Counter 的规范化 WAL topic | Accepted | 2026-04-18 |
| [0005](0005-kafka-transactions-for-dual-writes.md) | Counter 用 Kafka 事务原子写双 topic，不引入 dispatcher | Accepted | 2026-04-18 |
| [0006](0006-snapshots-by-backup-node.md) | 快照由备节点产生 | Accepted | 2026-04-18 |
| [0007](0007-async-matching-result.md) | 下单 API 返回"已受理"，撮合结果通过 WS 异步推送 | Accepted | 2026-04-18 |
| [0008](0008-sidecar-persistence-trade-dump.md) | Counter/Match 不直接写 MySQL，通过 trade-dump 旁路持久化 | Accepted | 2026-04-18 |
| [0009](0009-match-sharding-by-symbol.md) | Match 按 symbol 分片，etcd 配置驱动，支持停机迁移 | Accepted | 2026-04-18 |
| [0010](0010-counter-sharding-by-userid.md) | Counter 按 user_id 分 10 个固定 shard | Accepted | 2026-04-18 |
| [0011](0011-counter-transfer-interface.md) | Counter 提供统一的 Transfer 接口（deposit/withdraw/freeze/unfreeze） | Accepted | 2026-04-18 |
| [0012](0012-multi-module-monorepo.md) | 采用 multi-module monorepo + Go workspace | Accepted | 2026-04-18 |
| [0013](0013-tech-stack-choices.md) | 核心技术选型：franz-go、etcd-v3、shopspring/decimal、zap | Accepted | 2026-04-18 |
| [0014](0014-order-modify-as-cancel-new.md) | 改单实现为"撤单 + 新建" | Accepted | 2026-04-18 |
| [0015](0015-idempotency-at-counter.md) | clientOrderId 幂等在 Counter 层处理，仅对活跃订单去重 | Accepted | 2026-04-18 |
| [0016](0016-per-symbol-single-thread-matching.md) | 每个 symbol 在 Match 内单线程撮合 | Accepted | 2026-04-18 |
| [0017](0017-kafka-transactional-id-naming.md) | Kafka transactional.id 按 shard 稳定命名，用于主备 fencing | Accepted | 2026-04-18 |
| [0018](0018-counter-sequencer-fifo.md) | Counter Sequencer：懒启动 per-user worker + channel FIFO | Accepted | 2026-04-18 |
| [0019](0019-match-sequencer-per-symbol-actor.md) | Match Sequencer：per-symbol 常驻 goroutine + channel FIFO | Accepted | 2026-04-18 |
| [0020](0020-order-state-machine.md) | 订单状态机：内部 8 态 + 外部 6 态（Binance 风格） | Accepted | 2026-04-18 |
| [0021](0021-quote-service-and-market-data-fanout.md) | Quote 作为独立服务；市场数据通过 Kafka topic 扇出 | Accepted | 2026-04-18 |
| [0022](0022-push-sharding-sticky-routing.md) | Push 分片与 sticky WS 路由 | Accepted | 2026-04-18 |
| [0023](0023-trade-dump-batching-and-commit-order.md) | trade-dump 批量写入与 offset 提交顺序 | Accepted | 2026-04-18 |
| [0024](0024-trade-event-order-accepted-extension.md) | 扩展 trade-event.OrderAccepted 以支持行情重建 | Accepted | 2026-04-18 |
| [0025](0025-quote-engine-state-and-offset-strategy.md) | Quote 内部结构与 offset 回放策略 | Accepted | 2026-04-18 |
| [0026](0026-push-ws-protocol-and-mvp-scope.md) | Push WS 协议与 MVP-7 单实例范围 | Accepted | 2026-04-18 |
| [0027](0027-counter-sharding-rollout.md) | MVP-8 Counter 10-shard 路由落地 | Accepted | 2026-04-18 |
| [0028](0028-trade-dump-journal-projection.md) | trade-dump 投影 counter-journal 到 orders / accounts / account_logs | Accepted | 2026-04-18 |
| [0029](0029-bff-ws-reverse-proxy.md) | BFF WebSocket 反代模式 | Accepted | 2026-04-18 |
| [0030](0030-match-etcd-sharding-rollout.md) | MVP-11 Match etcd 驱动 sharding + 热加减 symbol | Accepted | 2026-04-18 |
