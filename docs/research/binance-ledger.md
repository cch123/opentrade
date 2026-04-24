# Binance Ledger 架构参考

来源：[How Binance Ledger Powers Your Binance Experience](https://www.binance.com/en/square/post/200468)

> 本文是外部资料摘要，用于给 OpenTrade 的账户、流水、快照、HA 设计提供参考。它不是 ADR，不代表 OpenTrade 已采纳 Binance 的完整实现。

## 1. 文章核心

Binance Ledger 是 Binance 内部承载账户余额与交易流水的核心账本系统。它负责两件事：

- 存储每个账户、资产维度的余额。
- 接收上层服务的交易指令，按事务语义更新余额，并留下可审计、可对账的 balance log。

文章强调 Ledger 的三类硬要求：

| 要求 | 含义 |
|---|---|
| 高吞吐 | 峰值时能处理大量 TPS |
| 24/7 可用 | 账本服务不能因维护、故障、切主而长时间不可用 |
| bit-level 准确 | 不能丢钱，不能重复记账，不能出现错误余额 |

一个转账例子可以抽象为：

```text
tx-001:
  account_1 BTC -1
  account_2 BTC +1
```

账本不仅要更新余额表，还要追加两条余额变更流水，供审计、对账、下游投影使用。

## 2. 传统关系库方案

行业常见实现是用关系型数据库事务包住多条 `UPDATE balance ...`：

```sql
begin transaction;

UPDATE balance
SET balance = balance - 1
WHERE account_id = 1 AND asset = 'BTC' AND balance - 1 >= 0;

UPDATE balance
SET balance = balance + 1
WHERE account_id = 2 AND asset = 'BTC';

commit;
```

优点：

- 实现简单，事务语义由数据库提供。
- 能直接使用读写分离、分库分表等常见优化。
- 运维、备份、监控、故障恢复路径成熟。

缺点：

- 热点账户会被行锁串行化。
- 并发越集中，TPS 下降越明显。
- 横向扩展困难，因为同一账户同一资产的余额行天然是冲突点。

## 3. Hot Account 问题

所谓 hot account，是指某些账户在短时间内被大量交易同时更新。例如多个请求同时从同一个账户扣 BTC。关系库方案需要锁住 `(account_id, asset)` 余额行，同一时刻只有一个事务能持有锁，其他事务只能等待。

文章给出的关键结论是：在热点账户场景下，内部测试 TPS 至少下降 10 倍。

这点对交易所非常关键，因为热点不只来自真实用户：

- 系统账户、手续费账户、清算账户可能天然高频。
- 大户、做市商账户可能产生集中流量。
- 批量撮合结算会让同一账户在短窗口内反复更新。

## 4. Binance 的解决思路

### 4.1 单线程状态机

Binance 的核心思路是把账本更新从多线程竞争模型转换为单线程状态机模型：

- 所有交易指令按确定顺序进入状态机。
- 状态机一次处理一条交易。
- 内存中更新余额，不再依赖数据库行锁做并发控制。
- 因为没有并发写同一状态，也就没有热点账户锁竞争。

这不是让整个系统单线程，而是把状态变更的临界区收敛到单线程；网络层、RPC、复制、查询投影仍然可以并行。

### 4.2 Disruptor 队列

状态机层是单线程，但网络层是多线程。两层之间需要高性能消息传递。文章提到使用 LMAX Disruptor：

- 基于 ring buffer。
- lock-free 或低锁竞争。
- 适合高吞吐、低延迟的生产者消费者模型。

抽象上可以理解为：网络线程只负责反序列化请求、入队；状态机线程从队列取命令，按顺序执行。

### 4.3 内存状态 + RocksDB 本地存储

文章描述 Binance 通过 in-memory model 获得高性能，并使用 RocksDB 做本地存储。

可以推断其目标是：

- 热路径尽量在内存里执行。
- 本地 RocksDB 用于持久化状态、快照或日志索引。
- 节点重启时能从本地状态恢复，再追补复制日志。

### 4.4 Raft 高可用复制

单节点内存状态机不能单独承担生产账本，因此 Binance 用 Raft 保证多副本一致性：

| 角色 | 职责 |
|---|---|
| Leader | 处理客户端请求，把操作复制给 follower |
| Follower | 跟随 leader apply 操作；leader 故障时参与选主 |
| Learner | 非投票副本，用于把幂等交易变更记录发送给其他服务 |

Raft 的关键收益：

- 所有副本按同一日志顺序 apply，状态一致。
- 多数派健康时服务可继续运行。
- leader 故障后 follower 可被选为新 leader。

文章称跨可用区部署时，只要超过半数节点健康，数据不丢，failover 可在 1 秒内完成。

### 4.5 CQRS 查询分离

Binance 把写路径和查询路径拆开：

- Raft domain：基于 RocksDB + Raft，服务高性能写入。
- View domain：监听 Raft domain 的消息，把交易和余额数据写入关系型数据库，服务外部复杂查询。

这就是 CQRS：

- Command path 只关心确定性写入和状态推进。
- Query path 可以面向产品查询、审计、报表建索引。
- 查询投影允许异步追赶，不阻塞账本主写路径。

### 4.6 请求处理链路

文章中的请求处理可以整理为：

```text
client
  -> network layer: 反序列化 RPC，放入 request queue
  -> ledger request handler: 准备上下文，放入 raft queue
  -> raft layer: 复制日志到 followers，完成共识后放入 apply queue
  -> ledger state machine: apply 交易，更新余额，放入 response queue
  -> network layer: 序列化响应，返回 client
```

层与层之间都通过队列传递数据，避免共享状态上的复杂锁竞争。

### 4.7 快照与恢复

Binance Ledger 支持两类快照：

- 周期性普通快照。
- 在相同 Raft log index 触发的一致性快照，保证各节点状态机快照点一致。

快照会上传到 S3：

- 给 Checker 做校验。
- 作为冷备份。

节点重启恢复流程：

```text
读取本地 snapshot
  -> 重建状态机
  -> replay 本地 raft log
  -> 从 leader 同步最新 log
  -> 追到最新 index 后恢复服务
```

如果本地 snapshot 或 raft log 不存在，则从 leader 获取。

### 4.8 性能数据

文章披露的测试配置与结果：

- 4 节点集群：1 leader + 2 followers + 1 learner。
- leader / follower / learner 使用 16c64g 规格。
- 集群处理超过 10,000 TPS。
- 因为交易逐条按序处理，没有锁和竞争，热点账户场景 TPS 与普通场景接近。
- 大多数交易在 10ms 内完成，较慢交易在 25ms 内完成。

这些数据适合作为设计直觉参考，不应直接作为 OpenTrade 的容量承诺。

## 5. 对 OpenTrade 的参考价值

OpenTrade 当前设计与这篇文章有几处高度一致：

| Binance Ledger | OpenTrade 对应设计 |
|---|---|
| 单线程状态机规避热点锁 | Counter per-user sequencer、Match per-symbol actor、vshard worker |
| 写路径与查询路径分离 | Counter / Match 写 Kafka；trade-dump 落 MySQL 投影 |
| balance log 用于审计与对账 | `counter-journal`、`account_logs`、trade-dump 幂等投影 |
| 快照 + 日志 replay 恢复 | ADR-0048 snapshot-offset 原子性、ADR-0061 trade-dump snapshot pipeline |
| Learner / view domain | trade-dump、push、history 等旁路消费者 |

但也有明显差异：

| 维度 | Binance Ledger | OpenTrade 当前路线 |
|---|---|---|
| 一致性复制 | Raft 多副本状态机 | Kafka 作为 durable log + vshard ownership / cold-standby |
| 主写存储 | 内存状态 + RocksDB + Raft log | Counter 内存状态 + Kafka journal + snapshot |
| 查询投影 | Learner 发变更到 view domain | trade-dump 消费 Kafka 写 MySQL |
| failover 目标 | 多数派存活，约 1s failover | 当前以 cold-standby / startup catch-up / on-demand snapshot 为主 |

## 6. 可借鉴点

### 6.1 热点账户不能靠数据库锁硬扛

如果未来 OpenTrade 增加 funding、fee、broker、insurance fund、system account 等高频账户，不能让这些账户直接落到关系库行锁热路径。应该继续保持：

- 余额真值在 Counter 状态机。
- 所有余额变更经过 `Transfer` / settlement 等规范入口。
- MySQL 只做查询投影，不做资金写入权威。

### 6.2 写路径保持确定性状态机

Binance 的经验强化了 OpenTrade 已有原则：

- 业务状态推进按明确 sequencer 串行。
- 外部并发请求先入队，再由单线程状态机 apply。
- 幂等 key、序列号、journal offset 是恢复与对账边界。

### 6.3 查询域不要污染写域

复杂查询、分页、报表、审计最好继续放在 trade-dump / history，不应为了查询方便把关系库拉回主写路径。

### 6.4 一致性快照值得保留为长期方向

文章的一致性快照思路可以对应到 OpenTrade 的几个方向：

- snapshot 必须绑定 journal offset / watermarks。
- 多分区、多 vshard 的全局对账快照，需要明确 cut point。
- S3 / MinIO 上的快照应有 checker 校验，而不是只作为重启缓存。

### 6.5 Learner 模式可类比旁路消费者

Binance Learner 是非投票副本，负责把幂等交易变更送到其他服务。OpenTrade 可以把 trade-dump snapshot shadow、push、history 看成类似的旁路角色：

- 不参与主写决策。
- 只消费规范化事件。
- 通过幂等 apply 和 offset 管理追赶主账本。

## 7. 不建议直接照搬的点

### 7.1 不应为了相似而引入 Raft

OpenTrade 目前已经把 Kafka 定位为 durable log 和服务间事件总线。如果再引入 Raft 复制 Counter 状态机会带来：

- 双日志系统：Kafka log + Raft log。
- 两套 offset / index / snapshot 对齐语义。
- 复杂的故障恢复矩阵。

除非未来明确要把 Counter 改成强一致多副本热切，否则不建议仅因 Binance 采用 Raft 就引入 Raft。

### 7.2 RocksDB 不是优先事项

当前 OpenTrade 已经有 snapshot + Kafka replay。RocksDB 本地状态只有在以下问题变得突出时才值得评估：

- 单 vshard 内存状态过大。
- 重启恢复必须显著减少反序列化成本。
- 需要本地增量 checkpoint 或 page cache 友好的状态存储。

否则它会增加运维、压缩、坏块、版本兼容和备份复杂度。

### 7.3 10,000 TPS 数据不能直接迁移

Binance 文章中的 TPS 是特定硬件、特定实现、特定交易模型下的结果。OpenTrade 的容量评估应继续依赖本项目自己的 benchmark：

- 单 vshard Counter 写入吞吐。
- `counter-journal` 分区吞吐。
- trade-dump shadow apply 吞吐。
- snapshot serialize / upload 时间。
- 热点账户、系统账户、批量结算等专项压测。

## 8. 后续可落地检查项

- [ ] 为系统账户、手续费账户补充热点账户压测场景。
- [ ] 对 `counter-journal` 到 `account_logs` 的对账链路补 checker 文档。
- [ ] 在 snapshot checker 中显式校验 `sum(balance delta)` 与账户余额投影一致性。
- [ ] 评估是否需要类似 learner 的专用非查询消费者，用于独立生成冷备快照。
- [ ] 在故障演练中覆盖：主写服务 crash、snapshot 缺失、journal replay、trade-dump lag、S3 snapshot 损坏。

