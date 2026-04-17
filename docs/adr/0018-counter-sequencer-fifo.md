# ADR-0018: Counter Sequencer — 懒启动 per-user worker + channel FIFO

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0010, 0019

## 背景

Counter 需要对同一 user 的操作做严格 FIFO 定序（ADR-0010）。scale 要求：
- 每 shard 承载 10 万用户，峰值 2 万 TPS
- 同一 user 多请求必须按到达顺序处理
- 不同 user 并行处理

Go 的 `sync.Mutex` **不保证 FIFO**（可能被新到 goroutine 插队），不能作为定序组件。

## 决策

**Counter Sequencer 采用"per-user lazy worker"模式**：
- 每个活跃 user 一个 `chan task`（FIFO 由 Go channel 语义保证）
- 首个请求到达时拉起一个 worker goroutine 独占处理该 user
- 30 秒无请求后 worker 自动退出
- shard 级单调 `seq_id`（`atomic.Uint64`）统一分配给所有 event

## 备选方案

### 方案 A：sync.Mutex per-user
- 优点：实现最简单
- 缺点：
  - **不保证 FIFO**（可能插队，1ms starvation 前）
  - 对交易系统致命：用户先发的订单可能后处理

### 方案 B（选中）：per-user channel + 懒启动 worker
- 优点：
  - FIFO 严格保证（channel 语义）
  - 内存随活跃用户动态
  - 实现简洁
- 缺点：
  - 活跃用户多 goroutine 多（不过 2 万 TPS 下峰值仅 ~100 并发 goroutine）

### 方案 C：固定 worker pool + hash 分桶
- 如 256 个常驻 goroutine，用户 hash 到桶
- 优点：goroutine 数固定
- 缺点：
  - 同桶用户之间 head-of-line blocking
  - 2 万 TPS / 256 桶 = 每桶 78 req/s，触发概率非零
  - 复杂度不低于方案 B

### 方案 D：per-user 常驻 goroutine
- 活跃用户持续持有 goroutine
- 缺点：100 万用户峰值能把进程拖垮（10w × 2KB = 200MB 栈只是初始，若膨胀更糟）

## 理由

- **FIFO 是定序组件的刚需**：不能用 sync.Mutex
- **Go channel 是天然 FIFO 队列**：无需额外实现
- **懒启动控制内存**：活跃用户才占 goroutine
- **shard 级 seq_id 简单**：无需 per-user 维护数字状态

## 影响

### 正面
- FIFO 严格，无插队
- 内存随活跃度自适应
- 代码简洁可读

### 负面
- worker 拉起和退出有短暂开销（goroutine create ≈ µs 级，可忽略）
- 需要小心退出竞态（worker 判断 channel 空退出后又有新 task 进来）

### 中性
- 不支持"跨用户全局 FIFO"（无需求）

## 实施约束

### 数据结构

```go
// pkg/sequencer/user_seq.go
type UserSequencer struct {
    shardSeq atomic.Uint64
    users    sync.Map   // user_id → *userQueue
}

type userQueue struct {
    tasks   chan *task
    state   atomic.Int32   // 0=idle, 1=running
}

type task struct {
    fn   func(seqID uint64) (any, error)
    resp chan taskResult
}

type taskResult struct {
    v   any
    err error
}
```

### 核心方法

```go
func (s *UserSequencer) Execute(userID string, fn func(seqID uint64) (any, error)) (any, error) {
    uq := s.getOrCreate(userID)
    t := &task{fn: fn, resp: make(chan taskResult, 1)}
    
    select {
    case uq.tasks <- t:  // FIFO 入队
    default:
        return nil, ErrQueueFull  // channel 满,返回 429
    }
    
    if uq.state.CompareAndSwap(0, 1) {
        go uq.drain(s)
    }
    
    r := <-t.resp
    return r.v, r.err
}

func (s *UserSequencer) getOrCreate(userID string) *userQueue {
    if v, ok := s.users.Load(userID); ok {
        return v.(*userQueue)
    }
    uq := &userQueue{tasks: make(chan *task, 256)}
    actual, _ := s.users.LoadOrStore(userID, uq)
    return actual.(*userQueue)
}

func (uq *userQueue) drain(s *UserSequencer) {
    idle := time.NewTimer(30 * time.Second)
    defer idle.Stop()
    for {
        select {
        case t := <-uq.tasks:
            seq := s.shardSeq.Add(1)
            v, err := t.fn(seq)
            t.resp <- taskResult{v, err}
            if !idle.Stop() { <-idle.C }
            idle.Reset(30 * time.Second)
        case <-idle.C:
            uq.state.Store(0)
            // 二次检查,防止退出竞态
            if len(uq.tasks) > 0 && uq.state.CompareAndSwap(0, 1) {
                idle.Reset(30 * time.Second)
                continue
            }
            return
        }
    }
}
```

### 关键行为

- **channel 满 → 返回 429**：单用户恶意刷请求的防护
- **shard 级 seq_id**：`atomic.Uint64`，无需 per-user 状态
- **退出竞态**：worker 将 state 置 0 后再检查 channel，若有 task 则重新 CAS 拉起
- **gRPC handler 本身阻塞等 `t.resp`**：无需额外线程

### 不要做的事

- ❌ 不要用 `sync.Mutex` 做 per-user 串行
- ❌ 不要给每个注册用户预分配 goroutine
- ❌ 不要用全局单 goroutine（吞吐瓶颈）

### 测试要求

- 单元测试：
  - 并发 10 goroutine 同 user 乱序 submit，验证 fn 按入队顺序执行
  - 不同 user 并行执行
  - channel 满返回 ErrQueueFull
  - 30s 空闲 worker 退出，再来请求能重启

### 规模验证

| 指标 | 数值 |
|---|---|
| shard TPS | 2 万 |
| 处理延迟 | ~5ms |
| 峰值并发活跃 user | ≈ 100 |
| 峰值 worker goroutine | ≈ 100 |
| goroutine 总栈内存 | ~200 KB |
| sync.Map entry | 活跃 + 30s 内 ≈ 数万 |
| entry 内存 | ~100 B |
| 总内存 | 1-10 MB |

## 参考

- ADR-0010: Counter 分片
- ADR-0019: Match Sequencer (差异化设计)
- [Go sync.Mutex 实现](https://pkg.go.dev/sync#Mutex) — 说明不保证 FIFO
- 讨论：2026-04-18 "sync.Mutex 不是定序组件,必须 FIFO"
