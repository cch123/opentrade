# ADR-0025: Quote 内部结构与 offset 回放策略

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0001, 0008, 0021, 0024

## 背景 (Context)

ADR-0021 确定 Quote 作为独立服务，消费 `trade-event` 产出 `market-data`。
MVP-6 动工时需要敲定：

1. Quote 内部如何拆分（单引擎 / per-symbol goroutine）；
2. per-symbol 状态（orderbook、Kline 开 bar）怎么持久 / 怎么恢复；
3. Kafka consumer group 的 offset 策略；
4. 多个子功能（PublicTrade / Kline / Depth）如何共享 trade-event 输入。

约束：

- trade-event 按 symbol 分 partition（ADR-0019），同一 symbol 事件全局有序。
- Quote state 全部在内存：orderbook 的 `price → qty` map、每个
  `(symbol, interval)` 的当前 open bar。
- ADR-0021 已说明 "MVP 单实例，快重启"，但丢几秒行情的 hand-wave 不够严谨
  —— depth 错误会立即反映到 UI。

## 决策 (Decision)

### 1. 内部结构（单 Engine，内锁保护）

Quote 只有一个 `engine.Engine`，内部持有：

```
Engine
├── klines : map[symbol]*kline.Aggregator   (per-symbol 多个 interval 的 open bar)
├── books  : map[symbol]*depth.Book          (per-symbol orderbook projection)
└── mu     : sync.Mutex
```

trade-event consumer 和 DepthSnapshot ticker 在两个 goroutine，用 `mu` 序列化。
不搞 per-symbol actor：实现上简单，实测 trade-event 每秒吞吐远低于单核
反序列化 + map 查询。如果后续成为瓶颈，再按 symbol 拆 actor。

三个子组件（trades / kline / depth）作为 Engine 内的纯函数模块：

- `trades.FromTrade(evt)` — 无状态，Trade 剥掉 user id 得到 PublicTrade。
- `kline.Aggregator.OnTrade(price, qty, ts)` — 多 interval OHLCV 聚合。
- `depth.Book.OnOrderAccepted / OnTrade / OnOrderClosed` — orderbook level
  维护 + DepthUpdate。

Engine 把它们的 fan-out 拼起来并给每个 MarketDataEvent 戳上 EventMeta。

### 2. state 不做持久化，每次启动从 `trade-event` 最早 offset 重放

consumer 配置：

```go
kgo.ConsumerGroup(groupID),                  // 任意稳定 id
kgo.FetchIsolationLevel(kgo.ReadCommitted()),
kgo.DisableAutoCommit(),                     // 永不 commit offset
kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
```

后果：每次重启 Quote → 从 topic head 扫一遍 → 重建内存 state → 再跟上现在
流量。正常运行期间不 commit offset（kafka broker 会看到这个 group
"lag = end_offset"，是预期内的）。

### 3. 发 market-data 失败即退出 Run

publisher 失败返回 error，consumer `Run` 退出，main 触发 stop → supervisor
重启 → 再次从 earliest 重扫。重启是幂等的（PublicTrade / Kline / Depth
内容都是函数式地由 trade-event 派生）。

### 4. DepthSnapshot ticker

main.go 单独 goroutine，每 5s 调用 `Engine.SnapshotAll()` 并 publish。
为 Push 端新连接对齐（ADR-0021 实施约束）留好。

## 备选方案 (Alternatives Considered)

### A. Per-symbol goroutine + per-goroutine channel（类似 Match）
- 优点：真正无锁，可并行处理多 symbol。
- 缺点：实现复杂（dispatcher、shutdown 等），MVP 不需要。Quote 和 Match
  差别大：Match 单 symbol 热路径，Quote 被 Match 下游限速（match 产出多快
  quote 才多快），并发收益有限。
- 结论：延迟到出现瓶颈再做。

### B. Engine state 定期 snapshot 到磁盘/S3（类似 match/counter）
- 优点：重启不用从头扫。
- 缺点：MVP 代码量翻倍；depth map 的序列化、版本演化、合并增量都要实现。
- 结论：等 trade-event 体量到十万级再做；MVP 从头扫足够快。

### C. commit Kafka offset，重启从上次继续
- 优点：重启快。
- 缺点：内存 state 丢失，重启后 depth / kline 基准完全错误。必须配合 B 才
  成立。
- 结论：拒绝。

### D. 每次启动分配 unique group id（例如含启动时间）
- 优点：不污染某一个固定 group 的 lag 指标。
- 缺点：broker 堆积大量 "abandoned" group，retention 清理之前资源占用；
  可观测性反而变差。
- 结论：拒绝。保留固定 group + 明确文档化 "lag = end_offset 预期"。

### E. 用 direct partition consume（不用 consumer group）
- 优点：语义上最清晰，没有 group 概念。
- 缺点：需要自己发现 partition 数，partition 数变化时手动重配置；
  ConsumeResetOffset 在 group 模式下已够用。
- 结论：目前不值得。

## 理由 (Rationale)

- **MVP 不做 state snapshot**：trade-event 当前量级（开发阶段）扫完全 topic
  毫秒级，生产阶段也能在数秒内。等瓶颈出现再投入序列化 / 合并逻辑。
- **退出即重启** 配合 "每次从 earliest" 天然幂等：没有半处理状态需要清理。
- **单锁 vs actor**：Engine 全局锁粒度足够小（每个 trade-event 的
  Handle 全过程持锁 ~微秒级），不会成为瓶颈。

## 影响 (Consequences)

### 正面

- 代码量小：一个 Engine + 三个无状态/小状态子模块 + thin consumer/producer。
- 崩溃安全：重启完全幂等，不依赖任何 offset 或文件。
- 易扩展：后续加新 market-data payload（funding rate / index price）只需在
  Engine 里挂新 handler。

### 负面 / 代价

- **冷启动时间随 trade-event 保留期线性增长**。这是已知 limitation，替代方案
  见 B。
- **consumer group lag = end_offset 永远**。需要在监控里对 Quote 这个 group
  做特例处理，不能按一般 lag 告警。
- 重启期间 market-data 流 stall（几秒到几十秒，视 topic 大小）。下游 Push
  在这段时间不会收到新增量，但不会收到错误数据。

### 中性

- Depth 初始化期内，如果 Quote 重建到当前仅仅看了一半历史，emit 的 depth
  可能偏少；等它追赶到 topic tail 以后就恢复正常。这个过程对外透明（每次
  depth 都是 self-consistent，只是范围偏小）。

## 实施约束 (Implementation Notes)

- `Engine` 并发安全由 `sync.Mutex` 保证；`seq` 用 `atomic.Uint64`
  生成（在锁外也安全）。
- `consumer.TradeEventConsumer` 内部 **不** 调用 `CommitUncommittedOffsets`
  —— 这是有意的。
- `producer.MarketDataProducer` 按 `MarketDataEvent.symbol` 作 partition key。
- Kline 不回填空 bar：若两笔 trade 相隔多个 interval，中间 bar 不 emit；
  消费者自行填充。ADR-0021 未强制要求，MVP 保持简单。
- DepthSnapshot 周期从 flag `--snapshot-interval`（默认 5s）控制；0 表示禁用。
- 内存水位：book/kline 数据结构 ~每活跃 symbol O(levels + intervals)。单实例
  千级 symbol 仍然小。

## 未来工作 (Future Work)

- 按需加 state snapshot（S3 + 定时 emit），结合 offset commit 实现热重启 —
  ADR TBD。
- Per-symbol 拆分成独立 actor（若单锁成为瓶颈）。
- Kline gap 填充 + idle 推进（若产品需要持续 bar）。

## 参考 (References)

- ADR-0001: Kafka as Source of Truth
- ADR-0008: Sidecar Persistence（相似的 "Kafka 即权威，projection 可重建" 模式）
- ADR-0021: Quote 服务与市场数据扇出
- ADR-0024: OrderAccepted 扩展字段
- 实现：[quote/internal/engine/engine.go](../../quote/internal/engine/engine.go)、
  [quote/internal/consumer/consumer.go](../../quote/internal/consumer/consumer.go)、
  [quote/internal/kline/kline.go](../../quote/internal/kline/kline.go)、
  [quote/internal/depth/depth.go](../../quote/internal/depth/depth.go)
