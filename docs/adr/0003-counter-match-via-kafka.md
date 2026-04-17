# ADR-0003: Counter 与 Match 通过 Kafka 通信，不走同步 RPC

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0001, 0005, 0007

## 背景

下单流程涉及 Counter 冻结 → Match 撮合 → Counter 结算三步。Counter 到 Match 的订单传递通道有多种选择：

1. 同步 gRPC：Counter 调 Match，等撮合结果返回
2. 异步 gRPC（one-way）：Counter 调 Match，不等结果；结果通过 Kafka 回流
3. Kafka 通道：Counter 写 Kafka，Match 消费

用户关心：同步 RPC 等撮合结果会让 Counter 状态难以管理（中间态：已冻结、已发送 Match、等结果）。

## 决策

**Counter 到 Match 通过 Kafka `order-event` topic 通信（symbol 分区）。** Counter 写完 Kafka 事件即返回 BFF「已受理」，不等 Match 撮合结果；撮合结果通过 `trade-event` topic 异步回流给 Counter 做结算。

## 备选方案

### 方案 A：同步 gRPC 等撮合结果
- 优点：语义直观
- 缺点：
  - Counter 要维护"已冻结、已发送"等中间态，状态复杂
  - RPC 超时/网络抖动时重试幂等困难
  - Match 挂掉会阻塞 Counter 的处理链路
  - Counter 的 Raft commit 延迟被 RPC 污染

### 方案 B：异步 gRPC one-way + Kafka 回流
- 优点：延迟低（gRPC 1-2ms vs Kafka 3-5ms）
- 缺点：
  - 需要额外的 outbox 表防止 gRPC 失败丢订单
  - 失败补偿逻辑复杂
  - Counter 要知道 Match 拓扑（增加耦合）

### 方案 C（选中）：Kafka 通道
- 优点：
  - Counter 和 Match 完全解耦
  - Kafka 保证持久化，无需 outbox 表
  - Match 挂掉不影响 Counter 受理（Kafka 堆积）
  - 天然支持多消费者（未来加风控、审计只需订阅 topic）
- 缺点：
  - 每次下单多 1-2ms Kafka 延迟

## 理由

- **状态管理简化**：Counter 只管"已写 Kafka = 已移交"，无需维护 in-flight 状态
- **故障隔离**：Match 故障不影响 Counter 受理新订单（Kafka 缓冲）
- **天然顺序保证**：同一 symbol 的订单落同一 partition，Match 单线程消费保序
- **撮合结果异步**：下单 API 本来就返回「已受理」（ADR-0007），无需在下单路径上等撮合

## 影响

### 正面
- 两模块完全解耦，独立演进
- Match 的主备切换/扩缩容对 Counter 透明
- 事件流可审计、可回放

### 负面
- 下单到撮合的端到端延迟 +2-3ms（vs 同步 RPC）
- 用户必须通过 WS 或 poll 才能拿到撮合结果（不是同步下单响应）

### 中性
- 需要精心设计 order-event schema 和 partition 策略（ADR-0009）

## 实施约束

- `order-event` topic 按 symbol 做 partition key
- Producer 必须 idempotent + transactional（配合 ADR-0005）
- Match 消费端 per-partition 单线程（保证同 symbol 内顺序）
- Match 必须按 `order_id` 做业务去重（防御性，ADR-0015）
- Counter 消费 trade-event 用 EOS（consume + produce journal + commit offset 原子）

## 参考

- ADR-0001: Kafka as Source of Truth
- ADR-0005: Kafka transactions for dual writes
- 讨论：2026-04-18 "counter 通过 rpc 调用 match" 的反思
