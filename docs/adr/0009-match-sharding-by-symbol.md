# ADR-0009: Match 按 symbol 分片，etcd 配置驱动，支持停机迁移

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0016, 0019

## 背景

Match 是 CPU 密集型内存服务（每个 symbol 的 orderbook 撮合）。单 symbol 目标 4w TPS，symbol 数量动态（可能从 10 个扩展到 1000+），需要横向扩展。

## 决策

**Match 按 symbol 分片**：
- 每个 Match 实例负责一组 symbol
- symbol → shard 的映射存 etcd，配置驱动
- 支持停机迁移 symbol（5-30s 窗口）
- 热门 symbol 独占实例，冷门 symbol 共享

## 备选方案

### 方案 A：按 symbol hash 自动分片（consistent hashing）
- 优点：自动均衡
- 缺点：
  - 热门 symbol（BTC-USDT）可能和冷门挤在一起 → 难优化
  - rebalance 时 orderbook 迁移复杂（订单状态需要搬）
  - 不好"独占"热门 symbol

### 方案 B（选中）：静态配置驱动
- 优点：
  - 热门独占、冷门共享可精确控制
  - 容量规划清晰
  - 新 symbol 加入通过配置声明即可
- 缺点：
  - 需要人工决策 symbol 归属
  - 平衡性靠运维判断

### 方案 C：动态 rebalance（如 Kafka Streams 的 task rebalance）
- 优点：自动化
- 缺点：
  - orderbook 带状态，rebalance 期间撮合必须停
  - 实现复杂度高，MVP 不值得

## 理由

- **确定性**：哪个 symbol 由哪个 shard 撮合永远可预测，便于排查问题
- **热门 symbol 性能保证**：独占实例 = 独占 CPU + 内存 + Kafka partition
- **迁移窗口可接受**：用户已同意 symbol 迁移允许停机几十秒

## 影响

### 正面
- 运维可控、性能可预测
- 新上 symbol 流程清晰
- 热门 symbol 性能不受冷门影响

### 负面
- 需要人工评估 symbol 负载与 shard 归属
- 自动负载均衡需要额外工具（本 ADR 不涵盖）

### 中性
- 对 Counter 的 `order-event` 分区策略：按 symbol 做 partition key（自然和 Match shard 对齐）

## 实施约束

### etcd 配置结构

```
/cex/match/shards/
    match-shard-0: { instance_id: "match-0", version: "v1.2" }
    match-shard-1: { instance_id: "match-1", version: "v1.2" }

/cex/match/symbols/
    BTC-USDT:  { shard: "match-shard-0", trading: true }
    ETH-USDT:  { shard: "match-shard-1", trading: true }
    DOGE-USDT: { shard: "match-shard-5", trading: true }
```

### Match 实例启动流程

1. 读取自身 `match-shard-{N}` 配置，确定 version 与 instance_id
2. watch `/cex/match/symbols/*` 过滤 `shard == self`
3. 对每个负责的 symbol：
   - 加载最近快照（含 orderbook + 消费 offset）
   - 订阅 `order-event` 对应 partition
   - 回放到 latest offset
   - 启动 SymbolWorker（ADR-0019）
   - 标记 symbol `ready`
4. 全部 symbol ready 后在 etcd 注册 service endpoint

### 新增 symbol 流程

1. etcd 写入配置：`{shard, trading: false}`
2. 对应 Match 实例 watcher 触发，加载空 orderbook
3. 验证就绪后改 `trading: true`
4. BFF/Counter 发现配置生效，允许下单

### 停机迁移 symbol 流程

1. 将 symbol 置 `trading: false`
2. 通知用户/拒绝新单
3. 撤销所有活跃订单（通过 Match 广播 cancel）
4. 等待 orderbook 为空 + 所有 inflight trade 结算完成
5. 源 Match 生成 final snapshot
6. 目标 Match 加载（空 book）
7. etcd 切 shard 归属
8. 置 `trading: true`，恢复服务

### 与 BFF/Counter 路由同步

- BFF 下单前检查 `symbols` 配置：`trading=false` 直接拒绝
- Counter 写 `order-event` 前检查：降低无效 event 数量
- `order-event` 的 partition 对应 symbol → shard 映射需一致

## 参考

- ADR-0016: Per-symbol 单线程撮合
- ADR-0019: Match Sequencer
- 讨论：2026-04-18 "match 的分片"
