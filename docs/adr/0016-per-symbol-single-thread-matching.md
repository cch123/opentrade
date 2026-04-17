# ADR-0016: 每个 symbol 在 Match 内单线程撮合

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0009, 0019

## 背景

Match 的撮合是热路径，并发模型的选择直接影响性能与正确性。常见模式：
1. per-symbol 单线程（Actor）
2. per-symbol 锁 + 共享线程池
3. 无锁数据结构（lock-free orderbook）

## 决策

**每个 symbol 一个独占 goroutine，单线程处理该 symbol 的所有事件（下单、撤单、成交）。** 对应 Kafka partition 按 symbol 分区，单消费者。

## 备选方案

### 方案 A（选中）：per-symbol 单 goroutine
- 优点：
  - 零锁，无竞争，撮合逻辑纯序列
  - orderbook 数据结构无需 thread-safe
  - 定序由消息入队顺序天然保证
  - 确定性执行（便于重放、测试）
- 缺点：
  - 单 symbol 吞吐上限 = 单核处理速度（≈ 4 万 TPS，满足需求）
  - goroutine 数量 = symbol 数量（≤ 2000，可接受）

### 方案 B：per-symbol 锁 + 线程池
- 优点：
  - 池化 goroutine，总数可控
- 缺点：
  - 每次处理要抢锁，有开销
  - 锁竞争在热门 symbol 上严重
  - orderbook 需要 thread-safe 封装

### 方案 C：Lock-free orderbook
- 优点：理论上最高吞吐
- 缺点：
  - 实现复杂易错
  - Go 生态少有现成方案
  - 可读性差，不利于维护

## 理由

- 4 万 TPS/symbol 在单核 Go 完全可达（撮合逻辑 O(log N) 或常量级每次）
- orderbook 数据结构（价格档位 + 订单队列）在单线程下最自然
- 确定性执行利于快照/回放/测试
- goroutine 数量可控（Match 实例负责几十个 symbol，内存 < 100 MB）

## 影响

### 正面
- Match 实现简单、易测试、易优化
- orderbook 不需要并发控制开销
- 快照/回放的确定性保证（对未来双主等扩展有益）

### 负面
- 单 symbol 吞吐硬上限 ≈ 单核速度
- 超热门 symbol（如未来 BTC-USDT 突破 4w TPS）需要横向分拆（但 MVP 不存在此问题）

### 中性
- Match 的 SymbolWorker 实现见 ADR-0019

## 实施约束

### goroutine 模型

```go
type SymbolWorker struct {
    symbol   string
    inbox    chan *Event     // FIFO,来自 Kafka consumer dispatch
    book     *Orderbook
    producer *kgo.Client     // 产出 trade-event
    seq      uint64
}

func (w *SymbolWorker) Run(ctx context.Context) {
    for {
        select {
        case <-ctx.Done(): return
        case evt := <-w.inbox:
            w.handle(evt)
        }
    }
}

func (w *SymbolWorker) handle(evt *Event) {
    // 串行处理,无锁
    // 按 evt 类型分支: OrderPlaced / OrderCancel / Snapshot / ...
}
```

### Kafka consumer → SymbolWorker 分发

- Kafka consumer 按 partition 消费
- 同 partition 内消息顺序到达
- 根据 `evt.symbol` 投递到对应 SymbolWorker 的 inbox
- inbox buffer 大小：2048（约 50ms 缓冲）
- inbox 满时 consumer 暂停拉取（背压）

### orderbook 数据结构建议

- 价格档位：`treemap`（按价格排序），或两侧各一个红黑树/SkipList
- 每档位下订单队列：链表（便于插入和遍历撮合）
- 订单索引：`order_id → *Order` map（O(1) 撤单）

### Panic 处理

- SymbolWorker panic 立即通知 Match 实例退出并告警
- 不要 recover 继续（可能已是不一致状态）
- 由 Match 实例进程重启恢复（从快照 + Kafka 重放）

### 禁止不确定性来源（为未来双主准备）

- 禁用 `time.Now()` 作为业务逻辑输入（若需要时间，从事件 ts 字段取）
- 禁用 map range（Go 随机序）
- 禁用全局随机数
- 违反则单元测试失败（lint 检查）

## 参考

- ADR-0009: Match 按 symbol 分片
- ADR-0019: Match Sequencer
- 讨论：2026-04-18 "match 单 symbol 单线程"
