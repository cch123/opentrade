# ADR-0001: Kafka 作为事件权威源，不引入自建 WAL 或 Raft

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0002, 0003, 0004, 0005

## 背景

交易系统需要对订单、成交、账户变更做持久化与故障恢复。候选方案：

1. 每个服务（Counter/Match）自建 WAL 磁盘日志 + 主从复制
2. 用 Raft 三副本（etcd-raft 库）做强一致复制
3. 用 Kafka 作为统一的事件存储与传输骨干

系统规模目标：单实例 20 万 TPS，单 symbol 4 万 TPS，P99 下单延迟 ≤10ms。

用户担忧：单机 WAL 如果磁盘坏了，数据可能丢失。

## 决策

**采用 Kafka 作为唯一的事件权威源（Source of Truth）**：

- Counter 和 Match 的所有状态变更都通过 Kafka 事件产生
- 内存状态 = Kafka 事件流回放结果
- 快照只是回放起点的优化，不是必需
- Kafka 集群配置 `replication.factor=3`, `min.insync.replicas=2`, `acks=all`（关键 topic）

## 备选方案

### 方案 A：每个服务自建 WAL + 主从复制
- 优点：延迟最低
- 缺点：主从异步复制切换丢数据；每个服务都要实现 WAL、复制、故障恢复

### 方案 B：Raft 三副本（etcd-raft 库）
- 优点：强一致，自动选主，业界成熟
- 缺点：Go 里实现 Raft 状态机复杂；三副本写开销；Counter/Match 各自实现一套增加维护成本

### 方案 C（选中）：Kafka 统一事件骨干
- 优点：复用 Kafka 的副本/持久化/复制能力；服务无状态化（状态可重建）；天然审计流
- 缺点：Kafka 写延迟（acks=all ≈ 3-5ms）吃掉部分延迟预算

## 理由

- **减少自研组件**：Kafka 自身已是 HA 系统（内部用 Raft/KRaft 保证 broker 复制），无需在应用层再搭一套。
- **架构统一**：同一事件流既是 WAL，也是下游消费源（trade-dump、push、quote 都消费它），无需维护两套数据通路。
- **故障恢复简单**：任何服务挂掉都可以从 Kafka 回放历史事件重建状态。
- **调试与审计友好**：事件流是完整的系统活动记录。
- **延迟权衡可接受**：Kafka acks=all 约 3-5ms，对于 10ms P99 预算仍可行。

## 影响

### 正面
- Counter/Match 不需要自实现 WAL/复制/选主
- 下游消费者解耦（quote/push/trade-dump 独立演进）
- 任何节点崩溃都不丢数据（只要 Kafka 不丢）

### 负面
- 每次写操作至少一次 Kafka 网络往返
- Kafka 集群本身成为关键依赖，Kafka 挂则交易停
- 对 Kafka 配置和运维能力有要求

### 中性
- 需要设计 topic 结构和 partition 策略（见 ADR-0004、0009、0010）

## 实施约束

- Kafka 集群至少 3 broker，跨 AZ（单机房内跨机架）
- Producer 配置：
  - `enable.idempotence=true`
  - `acks=all`（关键 topic）
  - `max.in.flight.requests.per.connection=5` 或更低
  - `retries=Integer.MAX`
- Consumer 配置：
  - `isolation.level=read_committed`（对于事务性 topic）
  - `enable.auto.commit=false`（手动 commit，配合 EOS）
- 监控：Kafka lag、ISR 状态、unclean leader election
- Topic 保留策略：核心事件 topic 保留 ≥ 7 天

## 参考

- [Kafka 事务与 EOS 官方文档](https://kafka.apache.org/documentation/#semantics)
- 讨论：2026-04-18 需求收集第三轮
