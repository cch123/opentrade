# ADR-0005: Counter 用 Kafka 事务原子写双 topic，不引入 dispatcher

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0003, 0004, 0017

## 背景

下单路径上 Counter 主需要原子地写两个 Kafka topic：
- `counter-journal`（给 Counter 备和 trade-dump 消费，按 user_id 分区）
- `order-event`（给 Match 消费，按 symbol 分区）

如果两次写不原子：
- 只有 journal 成功：order 没到 Match，订单卡住
- 只有 order-event 成功：Counter 状态未变但订单已撮合 → 后续 trade-event 回来时 Counter 找不到对应订单，数据不一致

## 决策

**Counter 主用 Kafka 事务（transactional producer）原子写入 counter-journal + order-event。** 不引入独立的 dispatcher 服务。

消费 trade-event 做结算时，也用 EOS 事务（consume + produce journal + commit offset 原子）。

## 备选方案

### 方案 A：Kafka 事务（选中）
- 优点：
  - 强原子性，两次写要么都成要么都失败
  - Counter 主代码清晰
  - Kafka 原生支持，无需自建协议
- 缺点：
  - 事务 commit 延迟 +3-5ms（两阶段提交）
  - 下单 P99 约 5-7ms（仍在 10ms 预算内）

### 方案 B：引入 dispatcher 中间件
- Counter 主只写一次 journal
- dispatcher tail journal，转写 OrderPlaced 到 order-event
- 优点：Counter 主路径极简（单次写）
- 缺点：
  - 多一个服务要运维
  - dispatcher 本身也可能失败（需要 EOS 事务，并未简化）
  - 订单到 Match 多一跳延迟（+2-3ms，不影响下单响应但影响撮合延迟）

### 方案 C：两次独立写 + outbox 补偿
- Counter 主顺序写 journal、order-event
- order-event 失败后台 reconciler 补发
- 优点：延迟最低
- 缺点：补偿逻辑复杂；故障窗口内订单卡住

## 理由

- **事务延迟对下单路径可接受**：5-7ms P99 仍在预算内
- **减少服务数量**：不引入 dispatcher 降低运维复杂度
- **代码清晰**：Counter 主一次事务包住两次写 + 内存更新
- **EOS 消费支持好**：Kafka 提供 `sendOffsetsToTransaction`，Counter 消费 trade-event 的原子结算可直接实现
- **franz-go 事务支持完善**：Go 生态里 franz-go 是事务性能最好的客户端

## 影响

### 正面
- Counter 的双 topic 写原子性靠 Kafka 原生保证
- 无需自建补偿或 dispatcher
- EOS 消费链路可用（结算不重复不遗漏）

### 负面
- 每次下单 Kafka 事务开销 3-5ms
- 需要规划稳定的 `transactional.id`（ADR-0017）
- 主备切换需要 fence 机制，避免老主残留事务

### 中性
- 如果后续压测延迟超预算，可以切换到 dispatcher 方案（拉平到异步链路）

## 实施约束

- franz-go 配置：
  ```go
  kgo.TransactionalID("counter-shard-N-main")
  kgo.TransactionTimeout(10 * time.Second)
  kgo.RequiredAcks(kgo.AllISRAcks())
  ```
- 下单事务序列：
  ```
  BeginTxn
    Produce(counter-journal, FreezeEvent)
    Produce(order-event, OrderPlaced)
  CommitTxn
  ```
- 结算 EOS 序列：
  ```
  BeginTxn
    Produce(counter-journal, SettlementEvent)
    SendOffsetsToTransaction(trade-event consumer offset)
  CommitTxn
  ```
- 事务失败处理：
  - `AbortTxn` → 内存不变 → 返回 BFF 错误
  - 用户带 clientOrderId 重试，去重表命中直接返回（ADR-0015）
- 主备切换：新主必须先调 `InitTransactions()` fence 老主（ADR-0017）

## 参考

- [Kafka EOS 设计文档](https://www.confluent.io/blog/transactions-apache-kafka/)
- [franz-go 事务示例](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo)
- ADR-0017: transactional.id 规划
- 讨论：2026-04-18 "dispatcher 也可能写失败" → 回到 Kafka 事务
