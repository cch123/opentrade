# ADR-0050: Match 输入 topic 按 symbol 分（order-event per-symbol）

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0003, 0005, 0009, 0019, 0030, 0048

## 背景 (Context)

当前 `order-event` 是单一 Kafka topic，由 Kafka 默认 hash partitioner 按 `symbol` 作 key 打到不同 partition。当 symbol 数 > partition 数时，一个 partition 承载多 symbol 事件。这给匹配侧带来两类问题：

1. **跨 symbol 共 partition** → [ADR-0048](./0048-snapshot-offset-atomicity.md) §4 的 "跨 symbol 取 MIN offset" 合并逻辑：恢复时每个 partition 取所有 owned symbol worker 的最小位点，慢 symbol 拖慢快 symbol 的恢复。
2. **symbol 热加减** → 新 symbol 没有自己的 partition 分配，订单必须挤进已有 partition 的流量里；热下线 symbol 时 partition 不变，残余事件仍在旧 partition。

进一步：监控 / 限流 / 未来按 symbol 切流均以 topic 为自然单位，共享 topic 阻挡这条演进路。

## 决策 (Decision)

### 1. 命名规范：`order-event-<symbol>`

- Topic 名 `order-event-<symbol>`（连字符），例如 `order-event-BTC-USDT`
- Kafka 允许字符 `[a-zA-Z0-9._-]`，符号 `-` 合法且和项目内 symbol（已使用 `-` 分隔 base/quote）无冲突
- 保留一条 `^order-event-.+$` regex 作为 consumer 订阅源
- counter journal topic（`counter-journal`）**保持单一**：key=user_id，不受本 ADR 影响

### 2. Producer：counter 按 symbol 选 topic

- `TxnProducerConfig` 加 `OrderEventTopicPrefix string`（默认 `"order-event"`）
- `PublishOrderPlacement(ctx, journalEvt, orderEvt, journalKey, orderKey)` 的 `orderKey` 参数已经是 symbol（caller 即 `service.PlaceOrder / CancelOrder` 传 `req.Symbol`）→ 直接拼 `prefix + "-" + orderKey`
- 旧 `OrderEventTopic` 字段保留但 deprecated；若 `OrderEventTopicPrefix == ""` 则 fallback 到旧 `OrderEventTopic` 走一条 topic，纯为 CI 老 fixture 兼容

### 3. Consumer：match 用 regex 订阅

- `OrderConsumer` 改 `kgo.ConsumeRegex()` + `kgo.ConsumeTopics("^order-event-.+$")`
- `InitialOffsets` 从 `map[int32]int64` 升级到 `map[string]map[int32]int64`（topic → partition → offset），`AdjustFetchOffsetsFn` 按 topic 查 saved offsets
- ADR-0048 §4 的 "跨 symbol 取 MIN" 逻辑：**逻辑保留但按 (topic, partition) 分组**。per-symbol topic 下不同 symbol 落不同 topic → 自然退化为 per-worker 独立（min 是它自己）；只有当同一 topic 存多个 partition + 多个 worker 共享时 min 才起作用，这是未来某个 symbol 量大到要拆 partition 的情形

### 4. Topic 自动创建

**MVP 依赖 broker `auto.create.topics.enable=true`**：counter 第一次写新 topic 时 broker 自动建（默认 1 partition / 1 replica，生产环境应手动调）。本 ADR 不强制 Admin API 创建，保留给运维层面：

- 生产建议：运维侧通过 etcd 配置（ADR-0030）新加 symbol 时，**同步**预创建对应 Kafka topic（`kafka-topics.sh --create` / Terraform / K8s Operator）。
- 未来工作：counter 或 match 加 AdminClient wrapper 主动 ensure topic 存在。

### 5. 迁移策略：硬切 + 老 topic 保留归档

1. 全集群停机（counter / match 同步）
2. 部署新版本（counter 走 `OrderEventTopicPrefix`，match 走 regex）
3. 新 topic 从空开始；老 `order-event` 不再被读写，保留作为历史审计
4. 各服务 snapshot 里的 `offsets` 条目 `topic="order-event"` 在新版启动时无法匹配任何 owned topic → `AdjustFetchOffsetsFn` 默认分支把它 fallback 到 `AtStart`，等价于一次性重扫新 topic（新 topic 是空的，所以 re-scan 什么都读不到，无副作用）
5. 稳定一个 snapshot 周期后可清理老 topic

**不做双写**：counter 同时写老 + 新 topic 的方案被拒 —— 老 topic 没人读，双写纯浪费 IO；硬切简单、失败半径小（都是内部流量）。

## 备选方案 (Alternatives Considered)

### A. 保持单 topic，custom partitioner 精确映射 symbol → partition

- 仍然共享 topic；partition 扩容 / 热加减 symbol 要 rebalance
- 不解决"监控 / 限流按 symbol"的诉求
- 结论：拒

### B. 一服务一 topic（match shard 粒度）：`order-event-shard-N`

- 粒度过粗：symbol 迁移到另一个 shard 就要跨 topic 搬
- 共 partition 问题依然存在（shard 内多 symbol 共 partition）
- 结论：拒

### C. Kafka Streams / rekey-by-symbol pattern

- 太重；我们不需要 streaming join
- 结论：拒

### D. 按 symbol group 分 topic（主流 pair 独占 + 其他合并到 `order-event-misc`）

- 能覆盖 90% 流量；实现复杂（策略路由）
- 结论：未来优化方向。MVP 先一 symbol 一 topic，再按实际负载合并冷 symbol

## 理由 (Rationale)

- **恢复语义统一**：每个 symbol 的 offset 完全独立，ADR-0048 的复杂 min 合并退化为"每 worker 管自己的 topic partition"
- **热加减 symbol 干净**：新 symbol → 新 topic，和现有 topic 零耦合
- **监控 / 限流颗粒度**：Kafka topic 已是自然 metric 维度（lag / throughput / retention 都按 topic 报）
- **迁移简单**：老 topic 自动过期不碍事；不做双写省事
- **未来演进保留**：per-symbol 需要多 partition 扩容时，只扩本 symbol 的 topic，不触发跨 symbol rebalance

## 影响 (Consequences)

### 正面

- 每个 symbol 恢复 / 限流 / 监控独立
- ADR-0048 的 "跨 symbol min" 变成 no-op 代码（保留但没负担）
- symbol 热加减不影响其他 symbol 的流量

### 负面 / 代价

- **Topic 数量爆炸**：N symbol → N topic。Kafka 单集群支撑 10k topic 是 OK 的，但 metadata 请求 / broker 内存线性增长。对 MVP 规模（<1k symbol）无压力
- **broker 依赖 auto-create**：需要 operator 清楚边界。生产环境建议显式预创建
- **counter TxnProducer 第一次写新 topic 时会有短暂 metadata 刷新延迟**（毫秒级）
- **老 snapshot 的 offsets 引用老 topic 后不生效**：硬切接受，回 AtStart；新 topic 空的 → 无副作用

### 中性

- order-event schema 不变（只是 topic 改名）
- counter-journal 流不受影响
- trade-event 流不受影响（match → counter）

## 实施约束 (Implementation Notes)

### 代码改动

**counter/internal/journal/txn_producer.go**:
- `TxnProducerConfig` 加 `OrderEventTopicPrefix string`（默认 `"order-event"`）
- `PublishOrderPlacement` 内部：`topic := p.orderEventTopicFor(orderKey)`，若 prefix 非空则 `prefix + "-" + orderKey`，否则 fallback 到旧 `OrderEventTopic`
- `produce(topic, ...)` 签名不变，调用方传新 topic

**counter/cmd/counter/main.go**:
- 加 flag `--order-topic-prefix=order-event` 默认值
- 老 flag `--order-topic=order-event` 保留但日志 warning 建议迁到 prefix

**match/internal/journal/consumer.go**:
- `ConsumerConfig` 加 `TopicRegex string`（默认 `"^order-event-.+$"`），`Topic` 字段 deprecated
- `InitialOffsets` 从 `map[int32]int64` 升级到 `map[string]map[int32]int64`
- `kgo.ConsumeTopics(regex)` + `kgo.ConsumeRegex()`
- `AdjustFetchOffsetsFn` 按 topic key 查 saved offsets；不在 map 的 topic partition 走 default `AtStart`

**match/cmd/match/main.go**:
- `mergeRestoredOffsets(reg)` 返回 `map[string]map[int32]int64`（topic → partition → offset）
- 每个 worker 按自己的 symbol 推断 topic（`orderEventTopicForSymbol(sym) = "order-event-" + sym`）
- snapshot 里的 `KafkaOffset.Topic` 已经在事件流转时记录，restore 时直接用

**match/internal/snapshot/snapshot.go**:
- `offsetsMapToSlice` 当前 hardcode `Topic: "order-event"`；改成接收 symbol 或 topic 参数，生成正确 per-symbol topic
- 回滚兼容：load 时 `KafkaOffset.Topic == "order-event"` 归类为老值，main merge 时忽略

### Flag 总览

```
# counter
--order-topic-prefix=order-event   # 新，per-symbol topic 前缀
--order-topic=order-event          # 旧，deprecated（prefix 为空时 fallback）

# match
--order-topic-regex=^order-event-.+$   # 新，consumer regex 订阅
--order-topic=order-event              # 旧，deprecated
```

### 失败场景

| 情况 | 结果 |
|---|---|
| broker 不 auto-create topic | counter 首次 produce 失败，需 operator 预创建 |
| 老 snapshot offsets 引用 `order-event` | restore 时落入 AdjustFetchOffsetsFn 默认分支 → `AtStart` |
| 某 symbol 流量爆炸 → topic partition 不够 | 用 `kafka-topics.sh --alter --partitions N` 扩；match consumer regex 自动 pick up 新 partition |
| 老 symbol 退役 | 老 topic 空转；可手动删除（`kafka-topics.sh --delete`），不影响其他 symbol |

### 测试

- counter: TestPublishOrderPlacement 验证 topic 按 symbol 拼
- match: TestConsumer 验证 regex 订阅 + 多 topic offset seek
- round-trip 从 snapshot 灌 offsets → consumer seek → 收到 event

## 未来工作 (Future Work)

- **Admin API 主动创建 topic**：ensure topic 存在 / replication 符合预期。配合 etcd symbol add 流程
- **冷 symbol 合并到 `order-event-misc`**：按实际负载决策（上文备选方案 D）
- **ADR-0048 §4 的跨 symbol min 合并代码**：per-symbol topic 下变成 no-op，可保留也可清理。保留更保险（未来 partition 扩容仍适用）

## 参考 (References)

- [ADR-0003](./0003-counter-match-via-kafka.md) — Counter/Match 通过 Kafka 通信
- [ADR-0005](./0005-kafka-transactions-for-dual-writes.md) — Counter 事务双写
- [ADR-0009](./0009-match-sharding-by-symbol.md) — Match 按 symbol 分片
- [ADR-0048](./0048-snapshot-offset-atomicity.md) §4 — 跨 symbol min offset
- 实现:
  - [counter/internal/journal/txn_producer.go](../../counter/internal/journal/txn_producer.go)
  - [match/internal/journal/consumer.go](../../match/internal/journal/consumer.go)
  - [match/cmd/match/main.go](../../match/cmd/match/main.go)
