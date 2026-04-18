# ADR-0033: MVP-13 Push 多实例 sticky 路由(user 过滤版)

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0022, 0026, 0027

## 背景 (Context)

ADR-0022 规定 Push 水平扩展为 10 个实例,LB 按 `user_id` hash sticky 路由;
私有数据(`counter-journal`)消费时按 partition 归属过滤,公共行情每个实例独立
group 全量消费。

MVP-7 跑通了**单实例** push(ADR-0026)。MVP-13 让多实例跑起来,前提是
确保:

1. **私有事件不重复推送**:如果两个 push 实例都收到了 user X 的 `counter-journal`
   事件,用户会看到两次订单更新。
2. **sticky 路由被遵守**:如果客户端绕过 LB 直连错实例,握手要拒绝 + 告诉
   "正确实例是哪个"。

严格版 ADR-0022 方案 B 要求 Kafka partition hash **和** `pkg/shard.Index`
对齐,这样每个 push 实例只订阅特定 partition 子集。对齐意味着 Counter 的
`journalProducer` / `TxnProducer` 要用 custom partitioner(用 `pkg/shard` 的
xxhash 代替 franz-go 默认的 murmur2)。这是跨服务改动,超出 MVP-13 预算。

## 决策 (Decision)

MVP-13 实现 **"全量消费 + user_id 过滤"** 版:

- 每个 push 实例仍然 `counter-journal` 全量消费(独立 consumer group)。
- `PrivateConsumer` 加过滤:`shard.Index(userID, TotalInstances) != InstanceOrdinal`
  → 静默丢弃事件。
- WS handshake:`TotalInstances > 1` 时,带 `X-User-Id` 的连接如果不归属本
  实例,返回 `403 Forbidden` + `X-Correct-Instance: <ordinal>`,客户端
  应当重连到对应实例。
- 匿名连接(无 `X-User-Id`)任何实例都接受 —— 它们只能订阅公开流,不涉及
  私有数据路由。
- 单实例模式(`TotalInstances=1`,默认)关闭所有 sticky 逻辑,向后兼容
  MVP-7。

partition-level 严格对齐(ADR-0022 方案 B)留 **MVP-13b**,需要:

1. Counter 用 `kgo.RecordPartitioner` 基于 `pkg/shard.Index` 选 partition。
2. `PrivateConsumer` 用 `kgo.ConsumePartitions` 指定 partition 子集。
3. `pkg/shard` 加 `PartitionsForInstance(instance, total, numPartitions)` helper。

## 备选方案 (Alternatives Considered)

### A. ADR-0022 严格版(partition 对齐)
- 优点:每个实例只拉它需要的 partition,Kafka 网络流量 N 倍放大降为 1 倍。
- 缺点:跨服务改 Counter 的 partitioner;风险是 partition 分布变化可能让
  已有 consumer 看到事件在"新 partition"。
- 结论:推迟到 MVP-13b。

### B. 只做 WS handshake 校验,不过滤 consumer
- 优点:代码改动最少。
- 缺点:handshake 拦住了错连的 user,但如果 user 确实连到了正确实例,其他
  实例仍然会把 user 的事件发到内存 hub 里 —— 虽然 hub 没有对应 conn
  所以 `SendUser` 是 noop,但反序列化 + hash lookup 的开销白白发生。
- 结论:consumer 侧早 drop 更清洁。

### C. 让 LB 把 LB hash 传到 push(如 `X-Routing-Ordinal` header)
- 优点:push 不用自己算 hash。
- 缺点:两头 hash 函数要一致,而且依赖 LB 的自定义能力(nginx / envoy 都要
  配 Lua/WASM)。
- 结论:拒绝;让 push 自己算更独立。

### D. partition 对齐用 Kafka 默认 murmur2 代替 `pkg/shard.Index`
- 优点:不用改 Counter producer。
- 缺点:`pkg/shard` 是 BFF 路由选 Counter shard 的权威 hash(ADR-0027);
  push 这边用别的 hash 会让"同一个 user 在 BFF 和 Push 被分到哪里"出现
  两套独立逻辑。将来任意一个改了都要同步调整。
- 结论:拒绝;保持"一个 hash 走天下"。

### E. push 单 consumer group 让 Kafka rebalance 分 partition
- 优点:Kafka 原生分区,不用自己算。
- 缺点:rebalance 时 partition 所有权会迁移,和 LB 的 sticky 窗口不同步;
  重平衡期间可能 drop 事件或重复。ADR-0022 已经讨论拒绝。
- 结论:拒绝。

## 理由 (Rationale)

- **user 过滤正确性 = partition 对齐**:只要 LB 和 push 用同一个
  `shard.Index` 函数,最终 "正确实例"的定义是一致的。区别只在 Kafka 网络
  流量;MVP 承担得起。
- **默认关闭,向后兼容**:`TotalInstances=1` 让所有老部署、dev 环境、现有
  单测继续跑,不需要改 flag。
- **双保险(consumer filter + handshake reject)**:任一层出错都有兜底 —— 漏
  连到错实例会在 handshake 拦住;漏发事件有 consumer filter 兜底。

## 影响 (Consequences)

### 正面

- Push 可以水平扩展,多实例部署不会导致私有事件双推或交叉污染。
- 客户端自动发现"正确实例":403 + `X-Correct-Instance` 给 LB 或 client SDK
  一条明确的纠错信号。
- ADR-0022 承诺的核心 sticky 模型实现了 —— 只是 partition-level 优化留到
  MVP-13b。

### 负面 / 代价

- **Kafka 流量放大**:N 个 push 实例各自消费全量 `counter-journal`。MVP 规模
  可接受;上生产前必须做 MVP-13b 收住。
- **CPU 浪费**:每个实例对 9/10 的事件走完"解码 → JSON → hash → drop"全
  流程。热路径的解码成本会随 N 线性增长。
- **前端 rebalance 压力**:`X-Correct-Instance` 假设客户端 / LB 能按响应
  header 做 redirect/reconnect。纯 WS 客户端库不一定实现。实际生产 LB
  的 hash 和 push 的 hash 对齐后,这个 header 只是"防御性兜底",日常不
  命中。

### 中性

- `TotalInstances` 是静态配置 —— 缩扩容要更新 push 的 flag + LB 的规则
  同步。operator 保证一致。动态扩容(consistent hashing)留 Backlog,和
  ADR-0027 Counter re-shard 一起规划。

## 实施约束 (Implementation Notes)

### 代码

- [push/internal/consumer/private.go](../../push/internal/consumer/private.go):
  `PrivateConfig` 加 `InstanceOrdinal / TotalInstances`;`dispatch` 前调
  `ownsUser`。
- [push/internal/ws/conn.go](../../push/internal/ws/conn.go):`Config` 加同名
  字段。
- [push/internal/ws/server.go](../../push/internal/ws/server.go):`Handler`
  在 `Accept` 前做 403 check。
- [push/cmd/push/main.go](../../push/cmd/push/main.go):`--instance-ordinal`
  / `--total-instances` flag,校验 0 ≤ ordinal < total。

### hash 一致性

`shard.Index(userID, total)` = `xxhash.Sum64String(userID) % total`。
BFF 用同一个函数选 Counter shard(ADR-0027);LB 必须用**同一个 hash** 做
sticky。两种 LB 实现路径:

1. **在 nginx 层做 xxhash 模块**(需要自研 module 或 lua-resty-xxhash)。
2. **BFF 做路由层**:客户端连 BFF,BFF 按 `X-User-Id` 算 ordinal,反代到
   对应 push 实例(配合 ADR-0029 BFF WS 反代一并做)。

推荐路径 2(MVP-14 Backlog 项):BFF 已经是 WS 反代(MVP-10),把
`--push-ws` 从单个 URL 改为 `[]URL`,按 `shard.Index(userID, len(urls))` 选。

### 测试

- [push/internal/consumer/sticky_test.go](../../push/internal/consumer/sticky_test.go):
  `ownsUser` 的四个分支。
- [push/internal/ws/sticky_test.go](../../push/internal/ws/sticky_test.go):
  owner 接受 / 非 owner 403 + header / 匿名绕过 / 单实例关闭 filter。

### 监控指标(Backlog)

- `push_private_events_dropped_total{reason="non_owner"}` —— 期望等于
  `(TotalInstances-1)/TotalInstances` 比例。偏离说明 LB 路由和 push filter
  的 hash 不一致。

## 未来工作 (Future Work)

- **MVP-13b**:Counter 用 `pkg/shard`-based partitioner;`PrivateConsumer`
  用 `ConsumePartitions` 订阅子集。彻底消除 N 倍流量放大。
- **MVP-14 候选**:BFF 按 user 路由到对应 push 实例,取代外部 LB 的
  sticky 模块。利用 MVP-10 的 `bff/internal/ws` 反代改造。
- **动态扩缩容**:当 `TotalInstances` 变化时(operator 临时扩容),已经建
  立的 WS 连接会因"新 ordinal 不属于我"被迫重连。需要一个"grace 期"机
  制 —— 扩容前先让老实例保持一段时间的"old hash" ownership,新实例逐步
  接管。ADR-0027 的 re-shard 工具同一套需求。
- **订阅级别的 rate limit / coalescing**(ADR-0026 未做)。

## 参考 (References)

- ADR-0022: Push 分片与 sticky WS 路由
- ADR-0026: Push WS 协议 & MVP-7 单实例范围
- ADR-0027: Counter 10-shard 路由落地
- 实现:
  - [push/internal/consumer/private.go](../../push/internal/consumer/private.go)
  - [push/internal/ws/server.go](../../push/internal/ws/server.go)
  - [push/cmd/push/main.go](../../push/cmd/push/main.go)
  - [pkg/shard/shard.go](../../pkg/shard/shard.go)
