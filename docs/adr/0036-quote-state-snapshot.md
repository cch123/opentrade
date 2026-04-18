# ADR-0036: Quote 引擎状态 snapshot + 热重启

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0025（决策被扩展）, 0001, 0021

## 背景 (Context)

ADR-0025 决定 Quote 冷启动时从 `trade-event` 最早 offset 重放：state 全靠
Kafka 派生，重启幂等且代码简单。这在开发阶段扫完 topic 只花毫秒；但当保
留期内事件累到亿级时，启动期 market-data 流会 stall 分钟级别，对下游
Push + 用户侧 UI 都是可观察的抖动。

ADR-0025 §未来工作 明确写了 "按需加 state snapshot（S3 + 定时 emit），结
合 offset commit 实现热重启 — ADR TBD"。本 ADR 落地 MVP 本地磁盘版本，S3
上云留给未来。

## 决策 (Decision)

### 1. 本地 JSON snapshot

引擎状态周期写入 `--state-snapshot-dir/<instance-id>.json`：

- 每 `--state-snapshot-interval`（默认 30s）一次
- 关闭时额外写一次 final snapshot
- 原子写：`tmp + rename`（和 counter snapshot 同模式）

snapshot 内容：

```
{
  "version": 1,
  "taken_at_ms": ...,
  "seq": <engine emit seq>,
  "offsets": { "<partition>": <next-to-consume offset> },
  "symbols": {
    "<symbol>": {
      "depth": { bids, asks, prices, orders[] },
      "kline": { bars: { <interval-enum>: {O,H,L,C,V,qv,count,...} } }
    }
  }
}
```

### 2. 重启时走"有 snapshot 则恢复"

启动流程：

1. `snapshot.Load(path)`：不存在 → cold start（保留 ADR-0025 的 earliest
   重放行为）
2. 存在 → `Engine.Restore(snap)`，consumer 用 `AdjustFetchOffsetsFn` 把每
   个 partition 的起始 offset 改为 `snap.Offsets[partition]`
3. snapshot 不认识的 partition（topic 扩分区）回落到 `AtStart`，自动
   backfill

### 3. state-offset 原子性

关键难点：如何保证 "state 里已经吃了的事件" 和 "我们上报的 offset" 保持一
致，避免双算或漏算。

做法：`Engine.HandleRecord(evt, partition, offset)` 把"应用事件"和"推进
offset"放在同一个 `engine.mu` 加锁段里完成；`Engine.Capture()` 也拿同一把
锁。三者任意时点互斥，snapshot 永远看到"已处理到 offset X（不含），state
完整反映至 X-1"的一致视图。

因此 consumer 不走 Kafka `CommitUncommittedOffsets`：offset 权威位置是
snapshot 文件本身。

### 4. Kline / Depth 本地序列化

两个子模块各加 `Capture() State` + `Restore(State) error`：

- `depth.Book`：bids/asks level map、priceOf、orders 引用全量 JSON 化
- `kline.Aggregator`：per-interval 当前 open bar（O/H/L/C、volume、
  quoteVolume、count）

两者都保留"JSON 字符串 ↔ Decimal"转换，避免浮点 / 精度丢失。

## 备选方案 (Alternatives Considered)

### A. 直接 commit Kafka consumer group offset，state 继续不持久化

- 被 ADR-0025 §C 拒过：state 丢失 + offset 前进 = 错 depth
- 现在仍然错

### B. 不用 group，走 `ConsumePartitions`（直连分区）

- 优点：语义最清晰，offset 完全由我们自己管
- 缺点：需要 admin API 发现分区数；group 模式下 `AdjustFetchOffsetsFn` 提供
  足够的能力做到同样效果且改动更小
- 结论：保留 group，用 `AdjustFetchOffsetsFn` 注入初始 offset

### C. 事件级 idempotency（基于 trade_id / seq_id）

- 缺点：Kline 不是幂等操作（一笔 trade 加到 volume 两次不可逆），要加去重
  层，复杂度反而高
- 结论：走 HandleRecord 原子推进 offset 更简洁

### D. 直接 S3 / 对象存储

- 被本 ADR 拒：MVP 用单实例 + 本地盘；多实例/HA 阶段再上 S3
- Backlog 化处理：S3 版的 Quote snapshot 留给上云 / 冷备 MVP

## 理由 (Rationale)

- **热重启**：trade-event 达亿级也是 O(1) 启动时间
- **最小侵入**：文件格式 JSON，方便排查 + diff；与 counter snapshot 形态统一
- **原子性保证**：一把 mutex 解决所有一致性问题，不需要引入 WAL / checkpoint
- **向后兼容**：`--state-snapshot-dir ""` 即回退到 ADR-0025 行为，适合压测
  / 调试场景

## 影响 (Consequences)

### 正面

- 重启 → 毫秒级恢复到"上次关机那一秒的"状态
- `market-data` 流中断时间从分钟级降到秒级
- 不再依赖 Kafka 的 retention > 状态"重建时间"（现在 retention 只要
  > snapshot 到 crash 的窗口即可）

### 负面 / 代价

- 代码量：snapshot 包 + 两个子模块 Capture/Restore + consumer 改签名 + main
  wiring，合计 ~500 LOC
- snapshot 丢失或损坏时的 fallback 还是 cold start（`Load` 解码失败直接
  `Fatal` 保护操作者，避免拿脏数据继续跑）
- Kline 的 open bar 跨重启"继承"—— 如果重启耗时过长（跨越多个 interval），
  open bar 的 openTime 仍是原来的，首条 trade 会触发 close + new bar，这
  是正确行为（旧 bar 关闭时用的是上次 last close 价，trade 到来时才真正
  rollover 到新 bar）

### 中性

- snapshot 文件大小 O(active symbols × levels) + O(partitions)，千级 symbol
  下预计十 MB 以内，`StateSnapshotInterval=30s` 下磁盘压力可忽略

## 实施约束 (Implementation Notes)

### Consumer 改动

- `consumer.Config.InitialOffsets map[int32]int64`：restore 时主 wire 进来
- 有 InitialOffsets → 注册 `AdjustFetchOffsetsFn`；没有 → 保持
  `ConsumeResetOffset(AtStart)`
- Handler 签名从 `Handler = func(*TradeEvent) []*MarketDataEvent` 改为
  `RecordHandler = func(*TradeEvent, int32, int64) []*MarketDataEvent`，把
  `rec.Partition / rec.Offset` 透传到 engine

### Engine 改动

- 新字段 `offsets map[int32]int64`（`engine.mu` 保护）
- 新 `HandleRecord(evt, part, off)` = `dispatch(evt)` + `offsets[part] = off+1`
  （替换 consumer 的入口；`Handle` 保留给内部/测试）
- 新 `Capture() *Snapshot` / `Restore(snap) error` / `Offsets() map[int32]int64`

### 失败场景

| 情况 | 结果 |
|---|---|
| snapshot 文件不存在 | cold start，rescan 从 earliest |
| snapshot version mismatch | 启动失败（operator 应清理旧文件） |
| snapshot JSON 损坏 | 启动失败（同上） |
| 写 snapshot 失败 | 记 error 日志，继续跑（下一个 tick 再试）；crash 损失 = 最多一个 interval |
| partition 比 snapshot 新多了 | 新 partition fallback 到 `AtStart`（`AdjustFetchOffsetsFn` 默认分支） |

### Flag 总览（quote）

```
--state-snapshot-dir ./data/quote           # 留空禁用
--state-snapshot-interval 30s               # 0 禁用定时，只留关机 snapshot
--snapshot-interval 5s                      # 既有：wire-level DepthSnapshot（不变）
```

## 未来工作 (Future Work)

- **上 S3**：本地盘 → Put/Get 对象存储；为未来多实例 Quote + cold-standby
  HA 铺垫
- **bar rollover-on-restart**：重启跨越多个 interval 时，对所有遗漏 bucket
  emit empty KlineClosed（和 Kline gap 填充 backlog 合并）
- **压缩 / 分 symbol 文件**：JSON → protobuf，或按 symbol 拆多文件，活跃 symbol
  增量保存；目前 JSON 单文件在 MVP 规模内够用

## 参考 (References)

- [ADR-0025](./0025-quote-engine-state-and-offset-strategy.md) — 原 cold-start 策略
- [ADR-0001](./0001-kafka-as-source-of-truth.md) — Kafka 即权威
- 实现：
  - [quote/internal/snapshot/snapshot.go](../../quote/internal/snapshot/snapshot.go)
  - [quote/internal/engine/engine.go](../../quote/internal/engine/engine.go)
  - [quote/internal/consumer/consumer.go](../../quote/internal/consumer/consumer.go)
  - [quote/cmd/quote/main.go](../../quote/cmd/quote/main.go)
