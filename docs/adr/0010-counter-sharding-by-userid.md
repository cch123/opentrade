# ADR-0010: Counter 按 user_id 分 10 个固定 shard

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0002, 0018

## 背景

Counter 需要 shard 以承载 100w 用户与 20w TPS。需要确定分片数量与分片策略。

## 决策

**Counter 分 10 个固定 shard，按 `user_id hash % 10` 路由。** BFF 按 user_id 计算 shard，通过 gRPC 调到对应 Counter 主实例。

## 备选方案

### 方案 A：动态分片（consistent hashing）
- 优点：扩容时平滑迁移
- 缺点：
  - Counter 有状态（余额、订单、dedup 表），迁移复杂
  - 迁移期间 user 请求路由不稳定
  - Kafka partition 策略随分片数变化 → 事件流混乱

### 方案 B（选中）：10 个固定 shard
- 优点：
  - Kafka partition 数量固定（journal 按 user 分 64-128 个 partition，映射稳定）
  - BFF 路由极简（hash 取模）
  - 状态迁移不是常态操作
- 缺点：
  - 扩容困难（需要 re-shard 全部数据）
  - 数字选错难改

### 方案 C：50-100 个 shard
- 优点：粒度更细，单 shard 压力小
- 缺点：MVP 运维成本高，每 shard 2 节点 = 100-200 个节点

## 理由

- **10 shard 足够覆盖 20w TPS**：每 shard 2w TPS，single Go 进程可达
- **Kafka partition 充裕**：counter-journal 用 64 个 partition（固定），每 shard 消费 6-7 个，负载均衡
- **扩容不是 MVP 阶段需求**：业务规模稳定前不改
- **状态量可控**：100w 用户均分到 10 shard = 每 shard 10w 用户，内存 ~百 MB 量级

## 影响

### 正面
- 路由逻辑简单可预测
- 运维节点数适中（10 shard × 2 副本 = 20 节点）
- Kafka topic/partition 设计稳定

### 负面
- 未来扩容需要 re-shard（迁移方案复杂）
- 热点用户（如市商账户）可能造成 shard 负载倾斜

### 中性
- 用户永久绑定到 shard（通过 user_id hash），适合做 per-user 定序

## 实施约束

### 路由公式

```go
shardID := int(farmhash.Fingerprint64([]byte(userID)) % 10)
```

采用稳定 hash（farmhash / xxhash），避免 Go map hash 随版本变化。

### Kafka 分区

- `counter-journal`：64 个 partition
- 每个 partition 按 `user_id` hash 分配
- 每个 Counter shard 消费 6-7 个 partition（64 / 10 取整）
- 具体映射：`partition = hash(user_id) % 64`，`shard = hash(user_id) % 10`
- 两者独立 hash，映射无需一致（消费时过滤）

**更严格的方案**：让 partition 数是 shard 数的整数倍（如 60 或 80），这样每个 shard 消费固定 partition 组：`shard_id = partition_id % 10`。推荐用 60 或 80，启动时更清晰。

### etcd 配置

```
/cex/counter/shards/
    counter-shard-0: { primary: "counter-0-a", backup: "counter-0-b" }
    counter-shard-1: { primary: "counter-1-a", backup: "counter-1-b" }
    ...
    counter-shard-9: ...
```

BFF 启动时加载全部 shard 路由表，watch 变更（主备切换时 primary endpoint 变化）。

### 未来扩容方案（本 ADR 不实现）

- 选项：shard 数翻倍（10 → 20）需要停机 + 全量 re-shard
- 选项：加新 shard 只接新用户，老用户保留（长期用户分布不均）
- 实际建议：业务明显超规模前预先设计 resharding 工具

## 参考

- ADR-0002: Counter HA
- ADR-0018: Counter Sequencer
- 讨论：2026-04-18 "counter 按 10 shard"
