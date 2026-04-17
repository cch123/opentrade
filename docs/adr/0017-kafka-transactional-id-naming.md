# ADR-0017: Kafka transactional.id 按 shard 稳定命名，用于主备 fencing

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0002, 0005

## 背景

Counter/Match 使用 Kafka 事务原子写入多 topic（ADR-0005）。主备切换时需要保证：
- 老主不能再写 Kafka（防止脑裂）
- 新主接管写入

Kafka 事务机制中，`transactional.id` 是 fencing 的关键——同一个 `transactional.id` 重新 `InitTransactions()` 会 fence 掉所有旧的 producer 会话。

## 决策

**每个 Counter / Match shard 分配稳定的 `transactional.id`，主备共享同一个 id，切主通过 `InitTransactions()` 自动 fence。**

格式：
- Counter: `counter-shard-{N}-main`
- Match: `match-shard-{N}-main`

（尾缀 `-main` 表示是处理主请求的事务流；后续如有结算流等可加 `-settlement` 等后缀）

## 备选方案

### 方案 A：用 instance hostname 作为 id
- 例：`counter-pod-abc123`
- 优点：唯一无冲突
- 缺点：
  - **破坏 fencing**：老主和新主 id 不同，新主无法 fence 老主
  - 老主重启回来可能仍能写 Kafka → 脑裂

### 方案 B（选中）：shard-level 稳定 id
- 优点：
  - 同 shard 的主备共享 id
  - 切主时新主用同 id 初始化 → Kafka 自动 invalidate 老主的 epoch
  - 老主再尝试写入收到 `INVALID_PRODUCER_EPOCH` 错误，主动退出
- 缺点：
  - 需要 etcd 强保证 "同一时刻同 shard 只有一个主"（由 lease 保证，ADR-0002）

### 方案 C：随机 UUID
- 优点：绝对唯一
- 缺点：同 Bug A（无 fencing 效果）

## 理由

Kafka 事务 fencing 语义：
- Producer 调 `InitTransactions()` 会获取 `transactional.id` 的 producer epoch（递增）
- Broker 记录当前 epoch，拒绝旧 epoch 的写入
- 新 producer 用同 id 重启 → epoch 递增 → 旧 producer 被 fence

这是 Kafka 专门为 "主备切换场景不脑裂" 设计的机制，我们直接利用即可。

## 影响

### 正面
- 主备切换无脑裂风险
- 切换流程简单（新主 `InitTransactions` 一步到位）
- 无需额外分布式锁机制

### 负面
- 每个 shard 的 id 必须全局唯一稳定，etcd 需要可靠存储
- Kafka 侧需要为事务性 id 分配配额

### 中性
- 同一 shard 的主备不能同时跑（但本来也不允许）

## 实施约束

### etcd 配置

```
/cex/counter/shard-config/shard-0: { transactional_id: "counter-shard-0-main" }
/cex/counter/shard-config/shard-1: { transactional_id: "counter-shard-1-main" }
...
/cex/match/shard-config/shard-0:   { transactional_id: "match-shard-0-main" }
```

启动时从 etcd 读，不写死代码。

### Counter/Match 主的启动流程

```go
// 1. 拿到 etcd lease (ADR-0002)
lease, err := election.Campaign(ctx, shardID)
if err != nil { return err }

// 2. 用 shard 稳定 id 初始化 Kafka transactional producer
producer, err := kgo.NewClient(
    kgo.TransactionalID(config.TransactionalID),  // "counter-shard-3-main"
    ...
)
if err != nil { return err }

// InitTransactions 自动 fence 老主
if err := producer.InitProducerID(ctx); err != nil {
    return err  // 致命错误,放弃 lease 退出
}

// 3. 注册为主,开始服务
```

### 老主收到 fence 后的行为

```go
// 业务 goroutine 捕获 Kafka 事务错误
if errors.Is(err, kerr.InvalidProducerEpoch) {
    log.Fatal("fenced by new primary, exiting")
    // 主动退出,上层 watchdog 会重启为备
}
```

### 不要把 transactional.id 写在代码里

- 配置化（etcd），便于演进（比如一个 shard 拆成多个事务流）
- 避免代码里硬编码导致新老部署不一致

### 事务超时

- `transaction.timeout.ms`：10s
- 主若 10s 内未 commit/abort，broker 自动 abort
- 主备切换不需要等超时（新主 `InitTransactions` 会立即 fence）

## 参考

- [Kafka Transactions Design Doc (KIP-98)](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)
- [franz-go Transactional Producer Docs](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.BeginTransaction)
- ADR-0002, 0005
- 讨论：2026-04-18 "切主时 fence 老主"
