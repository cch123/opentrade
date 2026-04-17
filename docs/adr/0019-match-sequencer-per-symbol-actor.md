# ADR-0019: Match Sequencer — per-symbol 常驻 goroutine + channel FIFO

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0009, 0016, 0018

## 背景

Match 需要对同一 symbol 的事件做严格 FIFO 定序。scale：
- symbol 数量：≤ 2000（预期远少于此）
- 每 symbol 吞吐：≤ 4 万 TPS
- symbol 集合相对静态（通过 etcd 配置变更）

与 Counter 的百万 user 规模不同，Match 的定序单元少且固定，可以更简单直接。

## 决策

**Match Sequencer 采用"per-symbol 常驻 goroutine"模式（Actor 模型）**：
- 每个 symbol 一个独占 goroutine，Match 实例启动时固定创建
- 每 symbol 一个 `chan Event`（FIFO 由 Go channel 保证）
- 不动态退出（symbol 关闭交易时才退出）
- 每 symbol 独立的单调 `seq_id`

## 备选方案

### 方案 A：同 Counter 的懒启动模式
- 缺点：symbol 数少且稳定，懒启动增加复杂度无收益

### 方案 B（选中）：per-symbol 常驻 goroutine
- 优点：
  - 代码最简单（Actor 模型）
  - FIFO 严格（channel 语义）
  - 零调度开销，goroutine 一直等在 channel 上
  - symbol 数量少，goroutine 栈总量可控（2000 × 2KB = 4MB）
- 缺点：
  - 冷门 symbol 占常驻 goroutine（但每个才 2KB 栈，可接受）

### 方案 C：共享 goroutine 池 + per-symbol 锁
- 优点：goroutine 数固定
- 缺点：
  - 锁开销
  - 无法保证 FIFO（同 Counter 的 mutex 问题）

## 理由

- symbol 数少且稳定，无需动态管理
- 常驻 goroutine 开销可忽略
- Actor 模型天然契合 orderbook 的"单线程处理"需求（ADR-0016）
- per-symbol seq_id 独立更自然（每 symbol 一个计数器）

## 影响

### 正面
- 实现最简单，代码清晰
- 定序 + 撮合在同一个 goroutine，无跨线程同步
- 启动和停止流程清晰（随 symbol 配置变更）

### 负面
- 长尾冷门 symbol 占常驻资源（但可接受）

### 中性
- Kafka partition 和 SymbolWorker 是 N:1 关系（一个 partition 可承载多个 symbol，但一个 symbol 只被一个 Worker 处理）

## 实施约束

### 数据结构

```go
// match/internal/sequencer/symbol_worker.go
type SymbolWorker struct {
    symbol  string
    inbox   chan *Event
    book    *Orderbook
    seq     uint64         // per-symbol 单调,需要持久化到快照
    prod    *kgo.Client    // 输出 trade-event
    
    // 优雅停止
    done    chan struct{}
}

type Event struct {
    Kind     EventKind   // OrderPlaced / OrderCancel / Expire
    Order    *Order
    Ts       int64
    // 元数据: 来源 Kafka offset 用于 EOS commit
    Partition int32
    Offset    int64
}
```

### 启动与生命周期

```go
// match/internal/server/server.go
func (s *MatchServer) Start(ctx context.Context) error {
    for _, symbolCfg := range s.symbolsFromEtcd() {
        w := s.newSymbolWorker(symbolCfg)
        go w.Run(ctx)
        s.workers[symbolCfg.Name] = w
    }
    // 启动 Kafka consumer,按 evt.Symbol dispatch 到 w.inbox
    return s.consumer.Start(ctx, s.dispatch)
}

func (s *MatchServer) dispatch(evt *Event) error {
    w, ok := s.workers[evt.Symbol]
    if !ok { return ErrSymbolNotOnThisShard }
    select {
    case w.inbox <- evt:
    case <-time.After(1 * time.Second):
        return ErrBackpressure  // 下游满,暂停 consumer
    }
    return nil
}
```

### SymbolWorker 主循环

```go
func (w *SymbolWorker) Run(ctx context.Context) {
    defer close(w.done)
    for {
        select {
        case <-ctx.Done():
            return
        case evt := <-w.inbox:
            w.seq++
            trades := w.book.Apply(evt)  // 可能产生多条 trade (撮合链)
            for _, tr := range trades {
                w.emitTradeEvent(tr, w.seq)
            }
        }
    }
}
```

### 停止流程（symbol 迁移 / trading: false）

```go
func (s *MatchServer) StopSymbol(symbol string) {
    w, ok := s.workers[symbol]
    if !ok { return }
    close(w.inbox)   // drain 剩余事件
    <-w.done         // 等 Worker 退出
    delete(s.workers, symbol)
}
```

### inbox 配置

- buffer size: 2048
- 满了 → dispatch 返回 backpressure → Kafka consumer 暂停拉取（自然流控）

### seq_id 持久化

- 每次快照（ADR-0006）包含 `symbol → seq_id` 映射
- 启动加载快照时 SymbolWorker 从快照值继续递增
- 从 Kafka 回放增量时 seq 继续递增（与消费 offset 同步）

### 禁止不确定性（同 ADR-0016）

SymbolWorker 内部严禁使用：
- `time.Now()` 作为业务输入
- Go map range（遍历需用排序切片）
- 全局随机数

## 与 Counter Sequencer 差异对照

| 维度 | Counter (0018) | Match (0019) |
|---|---|---|
| 单元 | user_id | symbol |
| 数量 | 百万级 | <2000 |
| 实现 | 懒启动 worker | 常驻 goroutine |
| 空闲回收 | 30s idle 退出 | 不退出 |
| seq_id | shard 级单调 | per-symbol 单调 |
| FIFO 保证 | Go channel | Go channel |

## 参考

- ADR-0009: Match 按 symbol 分片
- ADR-0016: per-symbol 单线程撮合
- ADR-0018: Counter Sequencer
- 讨论：2026-04-18 "counter 和 match 定序组件要求不一样"
