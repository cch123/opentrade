# ADR-0002: Counter 主备通过 etcd lease 选主，不用 Raft

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0001, 0005, 0017

## 背景

Counter 是有状态服务（用户余额、冻结、订单生命周期），需要 HA。

问题：如果 Counter 主挂了，主从异步复制会导致切换时丢数据。用户曾询问是否需要 Raft。

## 决策

**Counter 每个 shard 一主一备，通过 etcd lease 选主，不自实现 Raft。** 数据持久化由 Kafka（ADR-0001）保证，Counter 自身是 "state reconstructed from Kafka" 的无持久化状态模型。

切主时用稳定的 Kafka `transactional.id` fence 掉老主（ADR-0017）。

## 备选方案

### 方案 A：Counter 内嵌 Raft 三副本（etcd-raft 库）
- 优点：强一致，自动选主
- 缺点：
  - Go 里手写 Raft 状态机复杂，维护成本高
  - 三副本同步写延迟更高
  - 和 Kafka 的持久化职责重复

### 方案 B：主从异步复制 + 外部检测切主
- 优点：实现简单，延迟低
- 缺点：切换时异步复制 lag 的数据丢失，不可接受

### 方案 C（选中）：etcd lease 选主 + Kafka 作为状态真相
- 优点：
  - 实现简单（几十行代码）
  - 复用 Kafka 的持久化能力，无需 Counter 自己做复制
  - 状态可从 Kafka 完全重建
- 缺点：
  - 切主时需要 10-15s RTO（lease 过期时间）
  - 依赖 etcd 可用性

## 理由

在 ADR-0001 确立 Kafka 为 SoT 之后，Counter 进程实际上是 Kafka 事件流的 materialized view。这种模型下：
- 任何 Counter 节点都可以从 Kafka 重建状态
- HA 需求退化为"选一个主对外服务"
- 不需要复杂的状态复制协议

etcd lease 提供：
- 唯一活跃主（lease 独占）
- 主心跳断则自动释放
- 多个备可竞争成为新主

## 影响

### 正面
- 代码量极小（对比 Raft 实现）
- 切主后新主从 Kafka 追平状态，天然一致
- 依赖的是 etcd（运维团队熟悉）而不是自研 Raft

### 负面
- RTO 约 10-15s（lease TTL 相关）—— 交易系统可接受
- 需要小心 split-brain：老主以为自己还是主、新主已经升起
  - 缓解：Kafka transactional.id fencing（ADR-0017）

### 中性
- Counter 主备之间不直接通信，都通过 Kafka 同步状态

## 实施约束

- etcd lease TTL：10s（可调）
- 主节点每 3s 续 lease
- 备节点 watch lease key，lease 释放后抢锁
- 新主 ready 条件：
  1. 拿到 etcd lease
  2. 调 Kafka `InitTransactions()`（fence 老主）
  3. 消费 counter-journal 到 latest offset
  4. 在 etcd 注册 service endpoint
- 老主（若未崩）收到 lease 丢失通知后：
  1. 停止接新请求
  2. 若有进行中的 Kafka 事务，abort
  3. 降级为备（继续 tail Kafka）

## 参考

- [etcd lease 文档](https://etcd.io/docs/v3.5/learning/api/#lease-api)
- ADR-0017: Kafka transactional.id 命名
- 讨论：2026-04-18 架构简化讨论
