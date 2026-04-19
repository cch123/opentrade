# 非目标 / Non-Goals

本项目**已明确决定不做**的方向。和 [ADR](./adr/) 互补：ADR 记"决定做 X 以及为什么"，本文记"决定不做 Y 以及为什么"。

**为什么要有**：防止未来反复提被拒绝过的方向；新贡献者能快速理解项目边界；review / 对齐时一目了然。

**收录标准**：必须是**永久性拒绝**（有 ADR / 讨论记录支撑），不是"还没做"或"推迟做"。时间 / 精力原因推迟的东西进 [roadmap.md 待办](./roadmap.md#填坑--backlog)。

**条目格式**：
- 简短描述（做了会是什么样）
- **Why not**：拒绝的具体理由
- **关联**：ADR / commit / issue

---

## 架构层

- **Counter 订阅市场数据** — Counter 是纯账户状态机（ADR-0001），不依赖 match / quote 的实时输出来决策下单。**Why not**：市价单的滑点保护、触发价比对等"看市场"的逻辑放到 BFF / conditional / 客户端去；Counter 只做账户真值，保持单一职责 + 可分 shard。**关联**：[ADR-0035](./adr/0035-market-orders-native-server-side.md)。
- **跨 shard 分布式事务** — 同一笔 trade 的 maker / taker 可能落在不同 Counter shard；两边通过 trade-event 独立结算，**不做**两阶段提交。**Why not**：成本 >> 收益；match 的 match_seq_id + counter 的 per-(user,symbol) `LastMatchSeq` 幂等足够保证最终一致；任何一边 crash 都能靠 snapshot + Kafka replay 追上。**关联**：ADR-0007 / [ADR-0048](./adr/0048-snapshot-offset-atomicity.md)。
- **同步下单返回终态** — `POST /v1/order` 只返回 "received"，accepted / filled / rejected 走 WS private channel 异步推送。**Why not**：撮合是异步流水线（BFF → Counter → Kafka → Match → Kafka → Counter → Push），强行同步会把撮合吞吐拉垮 + 丢掉 WS 推送体系的价值。**关联**：ADR-0007。

## 可用性层

- **Match / Counter hot-standby** — HA 只做 cold-standby：primary 周期 snapshot，standby 平时不 tail journal；failover 时 standby 加载 snapshot + seek Kafka offset 恢复。**Why not**：hot-standby 需要 replicate in-memory state 的一致性协议（Raft / 自研），复杂度远高于 cold-standby 的可接受 RTO（秒级）。**关联**：[ADR-0031](./adr/0031-counter-ha.md)。
- **Push 服务端消息 replay** — 客户端断线重连不从服务端 replay 历史消息；客户端自己 `GET /v1/depth/{symbol}` + `/v1/klines` 拉 snapshot 对齐。**Why not**：Push 是纯 fanout，不保留历史；replay 需要 per-user WAL，存储 + 一致性成本远超其价值。**关联**：[ADR-0038](./adr/0038-bff-reconnect-snapshot.md)。

## 状态 / 持久化层

- **Order book 独立 WAL** — Match 不单独写 order book WAL；重启靠 snapshot + Kafka offset replay 重建。**Why not**：Kafka 已经是 durable log，再叠一层 WAL 是 double writes，增加失败面；snapshot 原子绑 offset 的设计让 replay 确定性可恢复。**关联**：[ADR-0048](./adr/0048-snapshot-offset-atomicity.md)。
- **Kafka consumer group offset 作为权威** — Counter / Match 都废掉 `CommitUncommittedOffsets`；snapshot 里的 per-partition `Offsets` 是唯一恢复权威。**Why not**：consumer group commit 和 snapshot 落盘不原子，会产生"offset 已 commit 但 snapshot 还是旧的"窗口，导致漏事件。**关联**：[ADR-0048](./adr/0048-snapshot-offset-atomicity.md)。
- **Reservation 事件进 counter-journal** — Reserve / ReleaseReservation 只随 snapshot 持久化，不进 journal。**Why not**：条件单触发前 reservation 变化频率低 + 调用方（conditional）有自己的 journal；双写会造成事件风暴 + 放大 journal 体积。**关联**：[ADR-0041](./adr/0041-counter-reservations.md)。

## Admin / Ops 层

- **Admin 操作做成 etcd 直写 UI** — 运维不获得"裸写 etcd key"的 GUI / 简化工具。**Why not**：绕过审计（谁写了什么不可追溯）、绕过校验（可写非法 JSON / 冲突的 shard 归属）、绕过幂等（同一操作重放副作用未定义）。所有 admin 写操作都走 BFF `/admin/*` 路由，强制跑审计 + 校验 + 合法态校验。**关联**：[ADR-0052](./adr/0052-admin-console.md)。
- **Admin 直接操纵 balance（绕过 Transfer）** — 不提供"admin 设置某用户 USDT 余额 = X"的后门。FREEZE / UNFREEZE / DEPOSIT / WITHDRAW 全部走 Counter `Transfer` RPC（ADR-0011），任何账户变动都产生 counter-journal 事件 + trade-dump 投影。**Why not**：Counter 是账户真值的单一入口（ADR-0001），一旦允许 admin 写路径绕过事件链路，对账 + 重放 + snapshot 恢复的一致性保证全部失效。**关联**：[ADR-0011](./adr/0011-counter-transfer-interface.md) / [ADR-0052](./adr/0052-admin-console.md)。
- **独立 admin-gateway 服务（MVP）** — 目前 admin 平面复用 BFF（`/admin/*` 前缀 + RequireAdmin 中间件），不拆独立进程。**Why not**：MVP 阶段 admin 流量稀疏（人工 + 少量 ops bot），复用 BFF 的 auth / rate-limit / access log 成本远低于新服务的运维开销。非永久拒绝——当 admin 流量 > 10% trading 流量或合规要求 ops plane 物理隔离时重评估。**关联**：[ADR-0052 备选方案 A](./adr/0052-admin-console.md)。

## 依赖层

- **Redis / Memcached** — 进程内 cache（BFF market-data cache）够用；外部 KV 只有 etcd（选主）和 Kafka（事件）。**Why not**：少一个运维 footprint；Redis 的常见用法（热缓存、rate limiter）都可以用 in-process map + token bucket 覆盖。
- **外部 OIDC / OAuth（Auth0 / Keycloak）** — JWT HS256 共享密钥 + 自签 API-Key（BN 风格）覆盖 MVP 场景。**Why not**：避免外部 auth provider 依赖；共享密钥 / API-Key pair 对 MVP 够用。**关联**：[ADR-0039](./adr/0039-bff-auth-jwt-apikey.md)。

## 收录新条目

碰到新的"决定不做"方向时：
- 若刚写完一个 ADR，就在同一 PR 里补一条到本文
- 若拒绝发生在讨论 / bug fix 过程中，记下拒绝理由和出处（commit / 对话 / 链接）补进来
- 定期 review：某条被拒绝的方向如果因为环境变化重新变成候选，从本文移除并到 roadmap 里开条目
