# Counter→Match order event 是否应支持批量操作

> **调研日期**：2026-04-19
>
> **触发问题**：用户侧 batch place / batch cancel / mass cancel API 是否要在 counter→match 这条内部链路上也做成"一条消息多笔操作"？

## 1. OpenTrade 现状

[api/event/order_event.proto](../api/event/order_event.proto)：

```proto
message OrderEvent {
  EventMeta meta = 1;
  uint64 counter_seq_id = 2;
  oneof payload {
    OrderPlaced placed = 10;
    OrderCancel cancel = 11;
  }
}
```

严格 **1 event = 1 订单动作**，partition key = symbol，`counter_seq_id` 用于 match 幂等去重（ADR-0015）。没有 batch/mass-cancel 变体。

## 2. "批量"可能指的三种形态，先分清

| 形态 | 说明 | 典型例子 |
|------|------|---------|
| **A. 用户侧 batch** | 一次 REST/WS 请求携带多笔订单 | Binance `POST /batchOrders`、OKX `batch-orders`、Bybit batch place/cancel |
| **B. 用户侧 mass cancel / kill-switch** | 一条请求"清掉我在某 symbol/某账户的所有活动单" | BN `cancelOpenOrders`、OKX `mass-cancel`、期权 MMP |
| **C. counter→match 内部 wire batch** | 一条内部消息携带 N 笔 order 操作 | 本文重点 |

A 和 B 是**用户↔网关**的协议问题，目标是省外网 RTT 和让 MM 在极端行情下一口气撤干净；C 是**内部总线**的协议问题，目标是 counter 和 match 之间的吞吐/延迟。

三者经常被混为一谈。实际上主流做法是：A、B 都做，**C 不做**。

## 3. 业内通行做法

### 3.1 内部总线形态

各家公开资料（架构博客、技术分享、API 文档反推）指向同一个模式：

| 交易所 | counter→match 动作集 | wire-level batch？ | mass-cancel 原生 opcode？ |
|--------|-------------------|-------------------|--------------------------|
| Binance | new / cancel / modify | ✗ | 未公开；MMP 走单独控制面 |
| OKX | new / cancel / amend | ✗ | mass-cancel 多为 counter 层展开 |
| Bybit | `x_create` / `x_cancel` / `x_replace` 三种 | ✗（1 msg = 1 TransactDTO） | ✗；CancelAll 在 counter 层展开成 N 条 `x_cancel` |
| Deribit / 期权主流 | new / cancel / edit | ✗ | 有 MMP kill-switch，匹配引擎内 1 条生效 |

共性结论：**counter→match 线上没有"一条消息多笔"的 batch，最多给 mass-cancel 单独开一个 opcode**。

### 3.2 为什么不做 wire batch

1. **内部链路不缺吞吐**。同机房 Kafka append / 共享内存 ring 的单条成本是微秒级，单分片几十万~百万 TPS。batch 省下的是每条几百纳秒的 header/序列化，换来的是解包复杂度、幂等边界变粗、错误局部化丢失。性价比极低。

2. **撮合引擎是单线程 per-shard 顺序消费**。无论 wire 上是 1 条还是 N 条，进撮合那一刻都要拆成 N 次顺序执行。wire batch 只影响"从 Kafka 读出到拆包"这一段，不影响撮合本体。

3. **幂等和错误语义变复杂**。现在 `counter_seq_id` 粒度 = 单订单动作；改 batch 后要么整 batch 原子（部分失败怎么回？），要么 batch 内每条独立（和单条 wire 没区别，还多一层封装）。

4. **分片边界**。batch 里的订单常跨 symbol，partition key 按 symbol 分片后还是要拆，batch 根本活不过 router。

5. **监控/回放/重建粒度**。1 event = 1 动作时，offset 和订单动作 1:1，dump / replay / 对账都简单；batch 后需要引入"子序号"，snapshot 边界定义也复杂一档（参考我们 ADR-0048 的单条 offset 原子绑定讨论）。

### 3.3 mass-cancel 为什么另说

- 用户语义："现在就撤光"，N 可能上千，但**结果只关心清零**，不关心每条单独的 ACK 顺序。
- 实现选择二选一：
  - **Counter 内展开**（Bybit / 多数 CEX）：counter 按自己的活动单列表 for-loop 产出 N 条 `cancel` 打给 match。好处是 match 侧不用新 opcode；坏处是极端情况下这 N 条要排进同一个分片的 Kafka，有 burst。
  - **Match 原生 opcode**（期权/做市场景居多）：一条 "cancel all for user X on symbol S" 进撮合，撮合内部 O(k) 扫该用户活动单全撤。好处是 counter→match 真的只有 1 条消息；坏处是撮合需要维护 user→活动单索引。

两种都**不是通用"wire batch"**，而是专门给 mass-cancel 这个特定语义开的口子。

## 4. 做市商痛点其实不在这里

MM 的关键诉求：

| 痛点 | 真正的解法 |
|------|----------|
| 一次挂 200 个单省外网 RTT | 用户侧 batch API（形态 A） |
| 市况突变瞬间全撤 | MMP / kill-switch（形态 B） |
| 改价不丢队列位置 | amend / replace opcode（单订单，不需要 batch） |
| 单单 ACK 延迟低 | 优化单条链路，不是批量 |
| 高吞吐 | 分片 + 并行，不是把消息合并成一条 |

只要 A 和 B 覆盖了，counter→match 是 1 条还是 N 条对 MM **不可观测**。没有任何公开资料显示主流 CEX 在 MM 反馈下把内部 wire 改成了 batch。

## 5. 对 OpenTrade 的启示

**短期不改现有 counter→match 协议**。`OrderEvent.oneof { placed, cancel }` 的单动作粒度继续用，理由：

- counter_seq_id × (symbol partition) 的幂等模型干净，ADR-0015 依赖这个粒度。
- ADR-0048 的 snapshot↔offset 原子绑定同样依赖"1 offset = 1 动作"的粗细度，batch 后要加一层子序号，代价不小。
- 目前没有吞吐瓶颈证据指向这一层。

**需要跟进的两件事**（按用户侧痛点来，不是按内部协议来）：

1. **用户侧 batch place / batch cancel API**（形态 A）。BFF 收到后在 counter 层展开成 N 条 `OrderEvent`，和主流一致。
2. **mass-cancel / kill-switch 语义**（形态 B）。先考虑 "counter 内展开" 的低成本方案；等 MMP / 做市场景真的接进来，再评估是否给 match 加原生 opcode。届时可以考虑在 `OrderEvent` 里加一个 `OrderMassCancel` payload，而不是把现有 `OrderCancel` 变成 repeated——后者会破坏现有幂等语义。

**不要做的**：把 `OrderCancel` 或 `OrderPlaced` 改成 `repeated` 的 wire batch。主流不做是有道理的，我们也没必要趟这个雷。

## 6. 决策建议（一句话）

> **内部 counter→match 线保持 1 event = 1 动作；批量需求在用户侧（BFF/gateway）展开，mass-cancel 按需单开 opcode。**
