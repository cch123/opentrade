# ADR-0021: Quote 作为独立服务；市场数据通过 Kafka topic 扇出给 Push

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0003, 0022

## 背景

行情数据（深度、K 线、逐笔成交）的生成和推送可以：
1. 由 Match 直接产生并推给 Push
2. 由 Push 内部根据 trade-event 自己算
3. 独立 Quote 服务消费 trade-event 算好再给 Push

Push → 用户的通道固定是 WS。Quote → Push 的通道可选 RPC stream / Kafka topic。

## 决策

**Quote 作为独立服务**，消费 `trade-event` 生成 market-data（深度增量、K 线、逐笔）。Quote 把生成结果写入 `market-data` Kafka topic（按 symbol 分区）。Push 订阅 `market-data` topic，按连接订阅过滤后推送给用户。

```
trade-event (Kafka, symbol 分区)
     │
     ▼
  Quote (消费 + 计算)
     │
     ▼
  market-data (Kafka, symbol 分区)
     │
     ▼ (fan-out 到每个 Push 实例)
  Push (过滤 + WS 推送)
```

## 备选方案

### 方案 A：Match 直接算并推送
- 优点：零链路延迟
- 缺点：
  - Match 热路径不应承担行情计算
  - Match 分片按 symbol，Push 按 user_id，协议不匹配
  - Match 需要知道 Push 拓扑

### 方案 B：Push 自己算
- 优点：少一跳
- 缺点：
  - 每个 Push 实例都要维护完整 orderbook 快照和 K 线状态
  - 10 个 Push 实例 × 全量状态 = 10 倍内存浪费
  - K 线聚合窗口在每个实例独立算，结果可能不一致

### 方案 C（选中）：独立 Quote + Kafka fan-out
- 优点：
  - 行情计算集中，状态只维护一份
  - Quote 可独立扩缩容（按 symbol 分片）
  - Push 无状态，只做订阅过滤和推送
  - market-data 作为独立 topic，其他消费者（如 API 查询层）也可订阅
- 缺点：
  - Push ← Quote 延迟 +1-2ms（Kafka 一跳）
  - 多一个服务要运维

### 方案 D：Quote → Push 用 gRPC stream 直推
- 优点：延迟最低
- 缺点：
  - Quote 需要知道所有 Push 实例地址
  - 扩缩容时连接管理复杂
  - 不解耦

## 理由

- **行情计算是 stateful**：orderbook 快照、K 线窗口需要状态，独立服务管理更清晰
- **Push 无状态化**：只维护连接和订阅关系，推送逻辑简单
- **Kafka 自然扇出**：多个 Push 实例用不同 consumer group 各自消费全量即可
- **延迟可接受**：行情推送本来就不在下单延迟预算内，+1-2ms 无感

## 影响

### 正面
- Quote / Push / Match 三个服务职责清晰
- market-data topic 作为独立接口，新消费者（数据分析、历史 K 线服务）易于接入
- Push 水平扩展简单

### 负面
- Quote 挂 → 行情停更（但交易继续）
- Kafka fan-out 流量：每个 Push 实例消费一遍全量 market-data

### 中性
- Quote 的分片策略待定（MVP 单实例即可，后续按 symbol 分片）

## 实施约束

### Quote 的职责

1. **增量深度**（Order Book Depth）
   - 维护每个 symbol 的 orderbook 快照（从 trade-event 重建；初始化时可从 Match 拉一份全量 snapshot 或从 Kafka 回放）
   - 产出增量深度更新（change per price level）
   - 频率：每 100ms 或每 N 个变动，取先到

2. **K 线**（Candlestick）
   - 窗口：1m / 5m / 15m / 1h / 1d
   - 从 trade-event 聚合 OHLCV
   - 每窗口关闭时 emit 一条 K 线完成事件
   - 实时更新当前未关闭 K 线（每 trade 更新一次）

3. **逐笔成交**（Trades Stream）
   - 直接转发 trade-event 中的 Trade 部分
   - 过滤掉非用户可见的事件（OrderAccepted 等）

### market-data topic 设计

- 分区：按 symbol
- 分区数：与 trade-event 对齐（便于单消费者处理单 symbol）
- 保留：短（1 天够用，行情是时效性数据）
- 消息类型（proto oneof）：
  - `DepthUpdate` — 增量深度
  - `DepthSnapshot` — 定期全量（便于新连接客户端对齐）
  - `KlineUpdate` — K 线更新（未关闭的当前窗口）
  - `KlineClosed` — K 线关闭（不再变动）
  - `PublicTrade` — 逐笔

### Push 的订阅过滤

- Push 本地维护 `connection_id → subscribed_symbols` 映射
- market-data 消息按 symbol 过滤 → 推给订阅该 symbol 的连接
- 每个 Push 实例都消费全量 market-data（用各自独立 consumer group）

### 深度全量发送策略

- 客户端新订阅 symbol 时，Push 需要发一次 snapshot（无论 Quote 最近有没有 emit）
- 方案：
  - Quote 定期（如 5s）emit `DepthSnapshot` 到 market-data
  - Push 缓存每个 symbol 最近一次 snapshot 在本地内存
  - 新订阅 → 先发缓存 snapshot，再开始发增量

### Quote 的 HA

MVP 阶段单实例 + 快速重启（无 user 影响，丢几秒行情可接受）。后续：
- 主备模式（类似 Counter/Match）
- 或者多 Quote 实例共同计算（相同输入 → 相同输出，Push 消费 group 里挑一个）

## 参考

- ADR-0003: Counter-Match via Kafka
- ADR-0022: Push 分片
- 讨论：2026-04-18 "quote 直接推给 push" → 改为 Kafka fan-out
