# ADR-0004: 引入 counter-journal 作为 Counter 的规范化 WAL topic

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0001, 0002, 0003, 0005

## 背景

Counter 备节点需要 tail Kafka 重建与主一致的内存状态。Counter 主的状态变化有三个源头：

1. 自身处理 BFF 下单/撤单 请求 → 产生 OrderPlaced / OrderCancel
2. 消费 `trade-event`（Match 产生）→ 结算余额
3. 消费 `wallet-event`（未来 wallet-service 产生）→ 充提入账

这三类事件的 partition key 不同：
- `order-event` 按 symbol 分区（给 Match 用）
- `trade-event` 按 symbol 分区（撮合关联）
- `wallet-event` 按 user_id 分区

如果 Counter 备直接订阅这三个 topic：
- 跨 topic 无全局顺序（可能先看到 trade 后看到 order）
- 需要 buffer、关联、等待逻辑，复杂易错

## 决策

**引入新的 topic `counter-journal` 作为 Counter 主的规范化 WAL**：
- 所有"内存状态变更"事件都由 Counter 主写入此 topic
- 按 user_id 分区
- Counter 备只 tail 这一个 topic 重建状态
- 同时作为账户流水，trade-dump 消费它写 MySQL

Counter 主的处理模式：
- 外部事件（trade-event、wallet-event）→ Counter 主消费后转写为规范化 journal event → 再更新内存
- 所有状态变化先落 journal，保证主备一致

## 备选方案

### 方案 A：Counter 备直接 tail 多个 topic（order-event + trade-event + wallet-event）
- 优点：不增加新 topic
- 缺点：
  - 跨 topic 乱序，需要 buffer 和关联逻辑
  - 备处理复杂，和主代码差异大
  - 耦合 Match 的 event schema

### 方案 B（选中）：引入 counter-journal 规范化流
- 优点：
  - 备只消费单一 topic，顺序天然保证
  - 备代码极简（state machine apply）
  - journal 同时是账户流水，trade-dump 直接用
  - Counter 主作为 normalization point 解耦外部 schema
- 缺点：
  - Counter 主处理外部事件时多一次 Kafka 写

### 方案 C：事件驱动纯异步（Counter 主也只消费 Kafka，不直接响应 RPC）
- 优点：极致统一
- 缺点：RPC 下单响应要等 consume round-trip，延迟过高

## 理由

- **备的恢复逻辑极简**：单 topic 顺序 apply 就是全部
- **journal 多用途**：既是 Counter WAL，也是账户流水（被 trade-dump 消费）、也是审计源
- **外部 schema 变化的隔离**：Match 改 trade-event，Counter 备无感知

## 影响

### 正面
- Counter 备代码和主处理状态的逻辑几乎相同（apply journal event）
- trade-dump 无需额外计算，直接转写 MySQL
- 账户审计、对账有规范化数据源

### 负面
- Counter 主消费 trade-event 时多一次 Kafka 写（写 journal）
- 下单路径 Counter 主需要原子写两个 topic（order-event + journal）→ 催生 ADR-0005（Kafka 事务）

### 中性
- counter-journal 的 event schema 需要规范化设计（见 api/event/counter_journal.proto）

## 实施约束

- `counter-journal` topic 配置：
  - 分区数：预留 64-128（覆盖未来 shard 扩容）
  - 分区 key：`user_id`
  - 保留：≥ 7 天
  - `replication.factor=3`, `min.insync.replicas=2`
- Event 类型（初版）：
  - `FreezeEvent` — 下单冻结
  - `UnfreezeEvent` — 撤单解冻
  - `SettlementEvent` — 成交结算
  - `TransferEvent` — Transfer 接口（deposit/withdraw/freeze/unfreeze）
  - `OrderStatusEvent` — 订单状态更新（filled/partial/cancelled）
- 每个 event 必须带：
  - `seq_id`（全局单调）
  - `user_id`
  - `ts`
  - 变更前后的 balance 快照（用于对账和审计）

## 参考

- ADR-0001, 0002, 0003, 0005
- 讨论：2026-04-18 "Counter 备 tail 的是什么 topic"
