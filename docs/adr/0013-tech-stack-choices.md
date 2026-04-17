# ADR-0013: 核心技术选型

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude

## 背景

需要确定 MVP 阶段的核心技术栈，避免后期替换代价。

## 决策

| 类别 | 选型 | 版本 |
|---|---|---|
| 语言 | Go | 1.26+ |
| Kafka 客户端 | [twmb/franz-go](https://github.com/twmb/franz-go) | latest |
| 配置/选主 | etcd v3 客户端 `go.etcd.io/etcd/client/v3` | latest |
| RPC | gRPC (`google.golang.org/grpc`) | latest |
| 序列化 | Protobuf (`google.golang.org/protobuf`) | latest |
| 金额/价格 | [`shopspring/decimal`](https://github.com/shopspring/decimal) | latest |
| 日志 | `go.uber.org/zap` | latest |
| 指标 | Prometheus `prometheus/client_golang` | latest |
| ID 生成 | 雪花 ID (自实现或 `bwmarrin/snowflake`) | — |
| 数据库 | MySQL 8 (orders/trades/logs) | — |
| 快照存储 | S3（或兼容实现，MVP 用 MinIO） | — |
| 缓存 | Redis（dedup、热数据） | — |

## 备选方案对比

### Kafka 客户端
| 库 | 评价 |
|---|---|
| [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) | 功能最全但依赖 librdkafka C 库，部署复杂 |
| [segmentio/kafka-go](https://github.com/segmentio/kafka-go) | 纯 Go，事务支持较新但稳定 |
| **franz-go（选中）** | 纯 Go，性能最好，事务支持完善，API 设计好 |

### Decimal
- `shopspring/decimal`（选中）：生态广泛、API 友好
- `cockroachdb/apd`：精度控制更严格但 API 复杂
- 字符串 + 自实现：不值得

### 日志
- `zap`（选中）：高性能，结构化，生态广
- `zerolog`：类似性能，API 略差
- 标准库 `slog`（Go 1.21+）：可选，但生态集成弱于 zap

## 理由

- **franz-go**：交易系统对 Kafka 事务有强依赖（ADR-0005），需要事务支持好的客户端；纯 Go 便于部署
- **etcd v3**：选主（ADR-0002）和配置中心（ADR-0009）都用 etcd，官方客户端最可靠
- **shopspring/decimal**：金额/价格绝对不能用 float；这是 Go 生态的事实标准
- **zap**：高并发场景日志性能关键；结构化日志利于未来接入 ELK

## 影响

### 正面
- 无 C 依赖（除 MySQL 驱动），容器镜像小
- 全栈 Go，语言统一
- 选型均为各领域的事实标准，生态健康

### 负面
- franz-go 相对新，团队需要学习其 API 风格
- etcd 运维需要团队具备相关技能

### 中性
- 标准库 `slog`（Go 1.21+）在未来可能替代 zap，先用 zap 保险

## 实施约束

### Decimal 使用规范

在 `pkg/decimal` 做薄封装：
```go
type Decimal = shopspring.Decimal
func Parse(s string) (Decimal, error) { ... }
func Zero() Decimal { ... }
```

- 所有价格、数量、余额字段统一用此类型
- proto 字段用 string 表示 decimal（避免 float64 序列化误差）
- 编译期禁用 float 做金额运算（code review 强制）

### franz-go 常用配置

```go
client, err := kgo.NewClient(
    kgo.SeedBrokers(brokers...),
    kgo.ClientID(fmt.Sprintf("%s-shard-%d", service, shard)),
    kgo.ProducerLinger(0),                          // 低延迟优先
    kgo.RequiredAcks(kgo.AllISRAcks()),
    kgo.ProducerBatchCompression(kgo.SnappyCompression()),
    kgo.MaxBufferedRecords(1_000_000),
    kgo.TransactionalID(...),                       // 若需事务
    kgo.TransactionTimeout(10 * time.Second),
    kgo.FetchIsolationLevel(kgo.ReadCommitted()),   // 消费事务性 topic
    ...
)
```

### etcd 客户端常用配置

```go
cli, err := clientv3.New(clientv3.Config{
    Endpoints:   endpoints,
    DialTimeout: 5 * time.Second,
    Logger:      zapLogger,
})
// Lease: ADR-0002
// Watch: ADR-0009 symbol 路由
```

### 版本锁定

- 根 `go.work.sum` 锁定共享依赖版本
- 每个 module 的 `go.sum` 独立校验

## 参考

- ADR-0002, 0005, 0009
- 讨论：2026-04-18 MVP 技术选型确认
