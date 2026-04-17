# ADR-0022: Push 分片与 sticky WS 路由

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0004, 0021

## 背景

Push 服务承载 100 万 WebSocket 连接，按用户推送私有数据（订单/账户）和公共行情。需要确定：
- 分几个 Push 实例
- WS 连接如何路由
- 私有数据和公共行情的消费方式

## 决策

**Push 分 10 个实例，LB 按 `user_id hash` sticky 路由 WS 连接。**
- 每个 Push 实例承载 10-15 万连接
- 私有数据：按 user_id 分区消费 `counter-journal` → 直接推给本实例连接
- 公共行情：所有 Push 实例各自独立 consumer group 消费 `market-data` 全量 → 按订阅过滤推送

## 备选方案

### 方案 A：LB 随机路由 + 连接发现服务
- 用户断开重连可能到不同实例
- Push 实例之间需要协调"用户 X 在哪台"
- 需要 Redis 维护 `user_id → push_instance` 映射
- 缺点：增加协调层，复杂度高

### 方案 B（选中）：Sticky by user_id hash
- 优点：
  - 同 user 永远到同一实例（除非实例挂）
  - 该实例本地就能订阅该 user 的私有数据 partition
  - 无需额外协调
- 缺点：
  - 实例挂时该 user 的连接需要迁移（走重连）
  - 负载均衡性依赖 hash 分布

### 方案 C：用户私有数据也通过 Kafka fan-out
- 所有 Push 实例消费 counter-journal 全量
- 按 user_id 过滤本实例连接的用户
- 缺点：
  - 流量放大 10 倍
  - 对私有数据（交易活跃用户事件多）浪费严重

## 理由

- **用户私有数据集中**：同 user 的所有连接在同一 Push 实例，可按 partition 精准订阅
- **公共行情扇出合理**：行情数据量远小于私有数据 × 用户数，冗余消费成本低
- **无协调层**：LB hash 天然一致，无需 Redis/etcd 注册表

## 影响

### 正面
- 架构简单，无额外服务
- 私有数据推送本地化，延迟低
- 水平扩展方便（加实例，调整 hash 模数）

### 负面
- Push 实例挂时用户必须重连（需客户端支持，本来 WS 也要处理重连）
- hash 负载不均风险（热点用户集中在某实例）

### 中性
- 实例数（10）要和 Counter 分片数（10）对齐，简化运维心智

## 实施约束

### LB 路由策略

#### 方案 A（推荐 MVP）：LB 层做 user_id hash

- Nginx / Envoy 配置按请求 header（如 `X-User-Id` 或从 JWT 解析）hash
- Push 实例注册到 upstream pool
- 连接建立时路由到 `instances[hash(user_id) % N]`

#### 方案 B（备选）：客户端直连带路由 token

- BFF 认证后下发一个带 `push_instance` 信息的 token
- 客户端连特定 push 实例的域名（`push-0.example.com`）
- 优点：LB 简单
- 缺点：客户端感知拓扑

**MVP 选 A**，后续需要跨机房多活时可切 B。

### 私有数据订阅

每个 Push 实例在启动时按 `instance_id` 计算应订阅的 `counter-journal` partition：

```go
// 假设 counter-journal 64 partition, Push 10 实例
// 每实例订阅 6-7 个 partition
func myPartitions(instanceID, totalInstances, totalPartitions int) []int {
    result := []int{}
    for p := 0; p < totalPartitions; p++ {
        if p % totalInstances == instanceID {
            result = append(result, p)
        }
    }
    return result
}
```

**关键**：partition 分配方案必须与 **LB 的 hash 规则一致**，保证：
`LB 路由到 instance_id 的 user` 的事件在 `counter-journal` 中落在 `instance_id 订阅的 partition` 里。

实际做法：
- `partition_id = hash_p(user_id) % 64`
- `instance_id = hash_i(user_id) % 10`
- 要求：**对于任意 user_id，若 `instance_id = A`，则 `partition_id % 10 = A`**

满足此约束的简单方案：**让 partition 数是 instance 数的整数倍，且使用同一个 hash 函数**。
- 例如：64 partition，10 instance，不是整数倍 → 不简单
- 改为：**60 partition，10 instance**（6:1）
- `partition_id = hash(user_id) % 60`，`instance_id = partition_id % 10`
- 天然对齐

⚠️ 注意：这和 Counter shard 数（ADR-0010 的 10）也要协调。counter-journal 的 partition 策略应服务所有消费者：
- 60 partition，Counter 消费 6 个（shard_id = partition % 10）
- 60 partition，Push 消费 6 个（instance_id = partition % 10）
- 60 partition，trade-dump 消费全部（N 个 consumer）

### 公共行情订阅

每个 Push 实例独立 consumer group 消费 `market-data` 全量：

```go
kgo.ConsumerGroup(fmt.Sprintf("push-md-%s", instanceID))
```

每实例消费全量市场数据，本地按连接订阅关系过滤推送。

### 连接与订阅管理

```go
type PushInstance struct {
    conns       sync.Map  // conn_id → *Connection
    subsByConn  sync.Map  // conn_id → subscribed symbols
    subsBySymbol sync.Map // symbol → set<conn_id>
    userToConns sync.Map  // user_id → set<conn_id> (一个用户可多连接)
}
```

收到 private event：
```go
func (p *PushInstance) onPrivateEvent(userID string, evt Event) {
    conns := p.userToConns.Load(userID)
    for _, c := range conns {
        c.Send(evt)
    }
}
```

收到 market data：
```go
func (p *PushInstance) onMarketData(symbol string, data Data) {
    connIDs := p.subsBySymbol.Load(symbol)
    for _, cid := range connIDs {
        p.conns.Load(cid).Send(data)
    }
}
```

### 重连与断点续传

- 客户端 WS 断开 → 自动重连
- 重连后 **主动拉一次状态快照**（订单、账户）以补齐期间错过的事件
- Push 侧可缓存每个用户最近 N 秒的 event stream（可选优化）

### 容量与限流

- 单实例连接上限：15 万（Go 单机极限 10-50 万，留余量）
- 每连接订阅数：上限 50 个 symbol（防刷）
- 消息速率限制：每连接每秒 1000 条（防下游阻塞）

### 广播队列

每个 Push 实例内部：
- 所有 WS 连接的发送使用独立 goroutine + channel
- 慢连接不应阻塞其他连接（channel 满丢消息 + 告警）
- K 线/深度推送用 coalescing（合并相邻更新）减少低价值推送

### 扩缩容

- 加实例需要：
  - LB 更新 upstream pool + 调整 hash 模数
  - 客户端重连到新分布
  - 运维窗口（短暂推送中断）
- 缩实例：同理

## 参考

- ADR-0004: counter-journal partition 策略
- ADR-0021: Quote + market-data
- 讨论：2026-04-18 "push 分 10 个 + quote 直推 push"
