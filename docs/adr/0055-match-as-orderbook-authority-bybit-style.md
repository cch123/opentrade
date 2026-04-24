# ADR-0055: 行情权威迁移 — Match 直接产出 OrderBook 全量 + 增量，Quote 降级为无状态转发（参考 Bybit）

- 状态: Accepted
- 日期: 2026-04-20
- 决策者: xargin, Claude
- 相关 ADR: 0021（Quote 独立服务 fanout）、0024（trade-event.OrderAccepted 扩展）、0025（Quote 引擎状态与 offset 策略）、0036（Quote snapshot 热重启）、0048（snapshot offset 原子绑定）、0049（snapshot protobuf）、0050（match 输入 topic per-symbol）、0051（typed producer seq naming）
- 实施提交: `35ba911`（feat: ADR-0055 Match 直出 OrderBook Full+Delta，Quote 降级无状态转发）

## 术语 (Glossary)

本 ADR 沿用"字段名偏直白 + 术语表映射行业词"的项目原则。

| 本 ADR 字段 | 含义 | 行业等价 |
|---|---|---|
| `OrderBookFull` | 某 symbol 在某个 `match_seq_id` 时刻的**全量盘口快照**（Top N 档的 bids/asks） | Bybit `OrderBookProfile` / OKX `books` snapshot / Binance REST `/depth` 快照 |
| `OrderBookDelta` | 相对于上一条帧的**增量变更**（增/删/改的档位列表） | Bybit `OrderBookTrade` / OKX `books` delta / Binance WS `depthUpdate` |
| `match_seq_id` | Match 产出事件的全局单调序（ADR-0051 已定） | Bybit `cross_seq` / OKX `seqId` |
| `Top N 档` | Match 直出的盘口深度档位数，对外一致（MVP 定 50 档） | Binance `depth5/10/20` / Bybit `L2_25` / OKX `books5` |
| `行情权威方` | 维护 orderbook 真值状态、可产出全量快照的服务 | Bybit：Match/Cross 引擎（OB 组件内嵌） |

**不在本 ADR 范围**：
- K 线、逐笔成交（`PublicTrade`）仍由 Quote 计算（它们不是 orderbook 状态的投影，而是基于 trade-event 的独立聚合）
- 内部撮合全档挂单簿（Match 内部 `orderbook.OrderBook`）仍保持全档，对外裁到 Top N

## 背景 (Context)

### 现状

**Quote 自己维护 orderbook 状态**，通过消费 `trade-event` 的 `OrderAccepted` / `Trade` / `OrderCancelled` / `OrderExpired` 在 `quote/internal/depth/depth.go` 的 `Book` 里重建盘口，然后定时 tick 发 `DepthSnapshot`、事件驱动发 `DepthUpdate`。

关键事实：
1. **Match 自己已经有权威 orderbook**（`match/internal/orderbook/`），Quote 是在用 trade-event 重新"推演"一遍
2. Quote 的 `depth.Book` 和 Match 的 orderbook 是**两份独立状态**，理论上可以 drift
3. 当前**没有任何跨服务一致性检测机制**（checksum / 对账 / 反向 RPC 拉取），见 ADR-0048 背景讨论
4. Quote 自己打 snapshot（ADR-0036），用自己的 `QuoteSeqId`（ADR-0051）做 snapshot 位点锚，恢复靠 Kafka offset seek（ADR-0048 同款范式）

### 问题

**双份状态是天然的不一致风险源**：
- Quote 消费顺序一旦有 gap（ADR-0048 的 output-flush barrier 只覆盖 output 方向，input 方向依赖 Kafka 保序 + offset seek），book 就会偏
- `OrderAccepted` 扩展字段（ADR-0024）全靠 Match 正确填 —— Quote 能不能准确重建 book 依赖 trade-event 语义是否**完备**，任何撮合侧行为变更都可能让 Quote 侧逻辑滞后
- 未来触发单触发、改单（ADR-0014）、self-trade prevention 等新语义如果没在 trade-event 里充分表达，Quote 侧就会对不上 —— "Quote 跟 Match 保持一致"变成一个**持续的回归风险**，而不是一次性做对

### 行业对照：Bybit / Binance / OKX 三种行情架构

本节比较三家一线 CEX 的行情（盘口）发布架构。Bybit 有泄露代码实证，Binance/OKX 基于公开 API 文档 + 架构习惯反推。

#### Bybit（泄露代码实证）

proto 定义（`trading/idl/svc/trading/orderbook/order_book.proto`）：

```proto
message OrderBook {
    esymbol.Symbol symbol = 1;
    int64 cross_seq = 2;
    int64 timestamp = 3;
    oneof data {
        OrderBookProfile profile = 6;   // 全量快照
        OrderBookTrade trade = 7;       // 增量
    }
}
```

Match/Cross 引擎**直接**把 `Profile` 和 `Trade` 帧发到 Kafka topic `push.orderBookL2_25.{SYMBOL}`。消费端（`mt4/pkg/trading/orderbook/kafka_consumer.go`）冷启策略：

```go
const startOffset = 150
c.StartOffset = kcomm.OffsetTail(startOffset)  // 往前回溯 150 条
// 丢弃所有 delta 直到遇到第一个 Profile：
if ob.Type == snapshot { c.ready = true }
if !c.ready { continue }
```

核心机制：**Match 周期性往 topic 里插 Profile 帧，消费者重启只要往前回溯一段就能找到最近的 Profile 自举**。Quote/MD 层**无本地状态**，纯 Kafka tail。

#### Binance（公开文档 + 架构反推）

对外协议：
- **WS `@depth` 流**：纯 delta，每帧带 `U`（首 updateId）/ `u`（末 updateId）；合约多一个 `pu`（前一帧的 `u`，强校验链）
- **REST `GET /api/v3/depth`**：全量快照，带 `lastUpdateId`
- **客户端同步**：REST 拉快照 → WS 缓冲 → 丢弃 `u < lastUpdateId` 的帧 → 从第一条满足 `U ≤ lastUpdateId+1 ≤ u` 的帧开始应用 → 之后必须连续（否则重拉 REST）
- **无 in-stream snapshot**，**无 checksum**

推测内部架构：
```
Match（C++ / 私有低延迟协议）
  └─► 内部 order log → delta feed
         ├─► WS gateway（fanout delta）
         └─► Snapshot cache service（旁路消费 delta 或周期从 match 拉 → 维护最新全量 → 暴露 REST /depth）
```

核心机制：**delta WS 和 snapshot REST 走两条独立通道，`lastUpdateId` 单调序把两条流缝起来**。Snapshot 是独立的 stateless HTTP 缓存层，对 match 热路径零侵入。客户端要实现 REST+WS stitching，复杂度下推到客户端。

#### OKX（公开文档 + 架构反推）

对外协议：
- **多频道产品化分层**：`books5`（5 档 / 100ms snapshot-only）、`bbo-tbt`、`books`（400 档 / 50ms delta）、`books50-l2-tbt`（50 档 tbt）、`books-l2-tbt`（400 档 tbt）
- 每帧带 `seqId` + `prevSeqId`（-1 表示首帧）→ **显式链式校验**
- **CRC32 checksum**（top 25 档 `price:size` 拼串）写在 delta 帧里
- Snapshot 在订阅时 + 周期性在流内都发

推测内部架构：
```
Match（权威 orderbook）
  └─► 撮合事件流
         └─► Book Builder service（有状态，维护多档位视图 5/50/400）
                ├─► 每档位独立 ticker 生成 snapshot + delta
                ├─► 算 CRC32 写进每帧
                └─► WS gateway 分频道推送
```

核心机制：**Book Builder 是关键中间层**，维护不同档位视图 + checksum 计算。Book Builder 有状态但**不是权威**（挂了可从 match 重建）。checksum 提供强客户端自愈能力 —— 对不上 → unsub + re-sub → 重新推 snapshot。

#### 三者对比

| 维度 | Bybit | Binance | OKX |
|---|---|---|---|
| Snapshot/Delta 是否同流 | ✅ 同 Kafka topic `oneof` | ❌ WS(delta) + REST(snapshot) 分离 | ✅ 同 WS 频道 |
| 流内 snapshot 频率 | 周期性（~每 N 条 delta） | 无（WS 纯 delta） | 50-100ms / per-frame |
| 序号 | `cross_seq` | `U`/`u`/`pu` 区间 | `seqId` + `prevSeqId` 链 |
| Checksum | ❌ | ❌ | ✅ CRC32 top25 |
| 多档位频道 | 单模式（L2_25） | 按档位 REST 参数 | ✅ 产品化多频道 |
| 客户端自检 | seq 断即重订 | `U/u` 对不上重拉 REST | checksum 不对即重订 |
| 权威源 | Match/Cross（OB 内嵌） | Match → Snapshot cache tier | Match → Book Builder tier |
| 对 Match 热路径侵入 | 中（match 直发 Profile） | 低（match 只发 delta） | 低（match 只发 delta） |
| 下游消费者状态 | 无状态 | 无状态（REST+WS 缝合） | 无状态 |

#### 我们为什么选 Bybit 风格

| 选项 | 适合场景 | 为什么**不**选 |
|---|---|---|
| Binance 风格（REST + WS 分离） | 极高单 symbol 吞吐、需要 CDN 边缘化 snapshot | 我们的 BFF marketcache 已经是"REST → 内存 → 客户端"的事实 REST-snapshot 层（[ADR-0038](./0038-bff-reconnect-snapshot.md)），但如果走 BN 模式，**BFF 需要反向从 Match 拉快照** 或 **BFF 旁路消费 delta 自建 snapshot** —— 前者违反 [ADR-0003](./0003-counter-match-via-kafka.md)，后者等价于把 Quote 的问题搬给 BFF |
| OKX 风格（多档位 + CRC + Book Builder） | 对做市商和普通用户有差异化产品矩阵、需要强客户端自愈 | 我们 MVP 阶段单一档位视图够用；Book Builder 多档位矩阵在 Quote 层未来可以加（上文"未来工作"已留口子）；现在引入 CRC 收益不抵成本 |
| **Bybit 风格**（同流 `oneof` + 周期 Full） | **现状 Quote 已是 market-data fanout 中间层**（[ADR-0021](./0021-quote-service-and-market-data-fanout.md)），把它降为无状态转发**改动最小** | 选择 |

核心判断：架构演进应当**沿着当前拓扑最小侵入地改善**，而不是翻盘重做。Bybit 的 "oneof 同流 + 下游 stateless tail" 正好匹配我们现有的 "trade-event → Quote → market-data → Push / BFF" 链路 —— 只需要把 Quote 的职责从"算 book"降成"转发 book"，下游一切不动。

Bybit 这条路径对我们的意义：
- **权威状态只在 Match 一处维护**（orderbook drift 不可能发生）
- **Profile 既是对外接口，也是下游冷启种子**（同一份数据用两次）
- **Quote/MD 侧无状态**，重启秒级、横向扩容自由

## 决策 (Decision)

**采纳 Bybit 式架构**：Match 作为 orderbook 权威方，直接产出全量 + 增量帧到行情 topic；Quote 降级为**无状态转发 + 非盘口类行情聚合**（K 线、逐笔成交仍在 Quote）。

### 1. 新 proto：`market-data` topic 里的盘口帧

新增一个 `oneof` 结构（放在 `api/event/market_data.proto`，与既有 `DepthSnapshot` / `DepthUpdate` 并存到 P3 下线前，具体命名实施时再定）：

```proto
message OrderBook {
    string symbol = 1;
    int64  match_seq_id = 2;   // Match 产出序（ADR-0051 已有）
    int64  ts_unix_ms = 3;
    oneof  data {
        OrderBookFull  full  = 10;   // 全量，Top N 档
        OrderBookDelta delta = 11;   // 增量
    }
}
```

`OrderBookFull` 包含 Top N 档的 bids/asks；`OrderBookDelta` 只含变动档位（qty=0 表示删除）。**档位数 N 对外一致**（MVP 定 50），不再按频道分级。

### 2. 产出责任从 Quote 迁移到 Match

- Match 每次 orderbook 变更后 emit `OrderBookDelta`（与现有 trade-event 同事务，沿用 ADR-0032 transactional producer）
- Match 周期性（`--orderbook-full-interval`，默认每 N=256 条 delta **或** T=5s 取先到）emit `OrderBookFull`
- Match 启动完成（restore 完 ADR-0048 的 offsets 后）必须**立刻**emit 一条 `OrderBookFull` 作为启动锚点
- 所有帧走**新 topic `orderbook-{symbol}`**（per-symbol，与 ADR-0050 match-input 同风格），或沿用 `market-data` topic 用类型字段区分（最终选型见"实施约束"）

### 3. Quote 降级

保留 Quote 的职责：
- `PublicTrade`（逐笔）—— trade-event 消费聚合
- `Kline`（K 线）—— trade-event 聚合 open bar

**移除** Quote 的盘口相关职责：
- `quote/internal/depth/depth.go` 整个模块下线
- Quote 不再消费 `OrderAccepted` / `OrderCancelled` / `OrderExpired`（只留 `Trade`，用于 PublicTrade 与 Kline）
- Quote snapshot（ADR-0036）移除 `books` 字段，只保留 `klines`

### 4. 下游消费者（Push / BFF marketcache / 第三方）

- 订阅新 topic，消费 `oneof{Full, Delta}`
- 冷启：`OffsetTail(N)` 回溯到最近 `Full` 帧 → 初始化本地 view → 应用后续 `Delta`
- 断线重连：沿用 ADR-0038 的重连补齐快照流程，但"快照"来源从"Quote 缓存"改为"上游 Full 帧"

### 5. 实施时序

本 ADR 在提交 `35ba911` 一次性落地（项目未上线，不需要灰度）：

| 阶段 | 动作 | 状态 |
|---|---|---|
| P0 | Match 内部 orderbook 新增 `TopN()` / 脏档位跟踪 (`markDirty` + `DrainDirty`) | ✅ |
| P1 | 新 proto（`OrderBook{oneof Full|Delta}`, `match_seq_id`）+ Match `MarketDataProducer` 上线，写 market-data topic | ✅ |
| P2 | 下游消费者切换：BFF marketcache 缓存 Full，Push 拆 Full→`depth.snapshot@{symbol}` / Delta→`depth@{symbol}` | ✅ |
| P3 | Quote `depth/` 包整体下线；snapshot schema 移除 QuoteDepth（proto tag 1 reserved） | ✅ |
| P4 | ADR-0024 的 `OrderAccepted.*` 字段暂保留（trade-dump / history 可能仍需） | 保留待后续评估 |

## 备选方案 (Alternatives Considered)

### A. 保持现状：Quote 自己算 book（ADR-0025/0036 路线）

- 优点：已经在跑，改动为零
- 缺点：
  - Match 和 Quote 两份 orderbook 状态永远存在 drift 风险
  - 撮合侧新语义（改单、SMP、触发细节）要同步教 Quote，跨服务耦合
  - 没有权威真值的情况下，一旦 drift 发生，排查链路长、难复现
- 结论：**短期继续（本 ADR 本身是"暂不立即实施"），长期拒**

### B. Quote 保持消费者角色，但引入周期 checksum 对账

- Match 侧定期广播 `(symbol, match_seq_id, book_checksum)`，Quote 比自己的 checksum 对不上就告警 / 重建
- 优点：改动小，保留现有架构
- 缺点：
  - 仍是双份状态，只是多了个检测机制 —— 出错了怎么"重建"？没有权威源可以拉
  - checksum 不对之后的恢复路径还是得发明（回到方案 C 或本 ADR 的方向）
- 结论：拒。对账是"找到问题"，不是"解决问题"

### C. 引入 Match → Quote 反向 RPC `PullSnapshot(symbol)`

- Quote 启动或检测到 drift 时主动向 Match 拉全量
- 优点：权威一致、无需周期性在 Kafka 里带 Full 帧
- 缺点：
  - 违反 ADR-0003 "Counter/Match 不走同步 RPC" 的全项目约束（虽然那是针对 Counter↔Match；Quote↔Match 的 RPC 在项目里也无先例）
  - Match 变成 query 源，撮合热路径旁路多一个接口面
  - 多实例 Quote 同时启动会对 Match 产生 stampede
  - Bybit 这种"反向 RPC"只用在 `trading_service ↔ OB`（用户状态恢复），不用在 `quote ↔ OB`
- 结论：拒。Quote 侧问题靠 Kafka 自包含（方案本 ADR）更好

### D. 让 Push 直接消费 Match 的新 topic，彻底绕开 Quote

- 优点：链路更短，Quote 只做 Kline / PublicTrade
- 缺点：
  - Push 分片按 user_id（ADR-0022），Match 按 symbol（ADR-0009/0050）—— 每个 Push 实例要消费全部 symbol 的 orderbook topic，fan-out 爆炸
  - ADR-0021 已经讨论并拒绝过这种方案（"Match 分片按 symbol，Push 按 user，协议不匹配"）
  - 本 ADR 不挑战 ADR-0021 的 fanout 结构，只是换了 fanout 的源头
- 结论：拒。Quote 仍然作为 market-data fanout 中间层存在，只是内部不再算 book

### E. 把 Full 帧写独立 topic（Delta 和 Full 分开）

- 对标：ITCH 有独立的 snapshot feed
- 优点：Full 帧大流量不阻塞 delta 小帧的消费
- 缺点：
  - 消费端要协调两个 topic 的顺序（Full 的 `match_seq_id` 和 Delta 的要能对齐）
  - Bybit 没这么做，同一 topic 用 `oneof` 足矣
  - 增加 operator 管理成本（多一个 topic 的保留策略、分区配置）
- 结论：拒。同 topic `oneof` 更简单

## 理由 (Rationale)

### 为什么选本 ADR

- **权威方唯一化**：Match 内部已有 orderbook，Quote 再维护一份是纯冗余。取消冗余 = 取消 drift 来源
- **Quote 代码大幅简化**：`depth/` 整个模块下线（~500 LOC + tests），Quote snapshot schema 缩一半
- **冷启恢复自包含**：下游消费者只要 Kafka 里能回溯到最近 Full 帧就能 bootstrap，不依赖任何本地状态 —— ADR-0036 的"本地 snapshot"对 depth 部分变得不必要
- **工业界实证**：Bybit / Binance / OKX / CME 全部走"权威方产 Full + Delta 到同一日志流"的模式；没有一家用"消费者自建状态+定时 snapshot"的做法
- **保留演进空间**：未来加 orderbook checksum（OKX 模式）、L3 逐单盘口、盘口压缩等都在 Match 一处做，下游自动受益

### 为什么不立即动工

- 当前 Quote 能跑、盘口正确、ADR-0036/0048 的 snapshot 机制已经工程化
- Match 还没有"裁 Top N + 序列化对外"的代码路径，工程量中等（估 ~800 LOC + tests，包含 proto / producer / ticker / restore 锚点 / 下游切换）
- ADR-0054（per-symbol 挂单数上限）、ADR-0053（精度治理）等更优先级任务在进行中
- Breaking change 但不紧急 —— OpenTrade 未上线，随时可做，不需要兼容层

## 影响 (Consequences)

### 正面

- Match ↔ Quote 盘口一致性风险**消除**（不是减轻，是根除）
- Quote 服务代码量 / snapshot 复杂度下降约 30-40%
- Match 成为行情"第一性"来源，符合"Kafka 作为事件权威源"（ADR-0001）在行情域的延伸
- 对标行业主流架构，新人上手更直觉
- 为未来 orderbook L3（逐单盘口）、盘口 checksum、跨区域多活复刻等留好接口

### 负面 / 代价

- **Match 热路径增加序列化开销**：每次 orderbook 变更多做一次 Top N 截取 + proto marshal。按 p99 单次 50 档 Top N ~3-5μs 估算，对 Match 单 symbol 撮合延迟（当前 p99 <100μs）是 3-5% 增量 —— 可接受，实施时需基准测试确认
- **Kafka 存储放大**：Full 帧比 Delta 大一个数量级；按每 5s 或 256 delta 发一次 Full、每帧 ~4KB 估算，每 symbol 每小时约 3MB 额外存储，32 symbol × 7 天保留约 16GB。量不大但需纳入容量规划
- **proto / topic 变更是 breaking**：所有下游（BFF marketcache、Push、未来的第三方行情）都得跟着切。因 OpenTrade 未上线，不写兼容层
- **老 ADR 需要重写/标注**：
  - ADR-0025 的"Quote per-symbol Book"部分 → P3 完成后更新为"Quote 无 depth 状态"
  - ADR-0036 的 `books` 字段 → P3 完成后从 snapshot schema 删除，保留 klines
  - ADR-0024 的 `OrderAccepted` 扩展字段（Price/Qty/RemainingQty/Side/Type）→ P3 后可移除（Quote 不再重建 book），或保留给 trade-dump / history 用
- 工程投入：~800 LOC + 基准测试 + 分阶段灰度，估 2-3 周全职

### 中性

- Quote 仍然作为独立服务存在（K 线、PublicTrade、未来的 index price / mark price 聚合）—— 不会被整个删掉
- `match_seq_id`（ADR-0051 已命名）成为跨 Quote/Push/BFF 的唯一行情序号，不再需要 `quote_seq_id` 做 depth 的二级编号（`quote_seq_id` 仍用于 PublicTrade / Kline 的发序）

## 实施约束 (Implementation Notes)

**本 ADR 为方向性决策，不含立即实施项**。真正动工时需要另起 implementation ADR（或在本 ADR 上补"实施 §"），锁定以下细节：

### 待定项

- **Topic 选型**：复用 `market-data` 加消息类型字段 vs 新开 `orderbook-{symbol}` per-symbol topic。倾向后者（和 ADR-0050 match-input 对称，per-symbol 天然顺序）
- **Full 帧频率**：N 条 delta 或 T 秒触发的具体值（Bybit 150，我们建议 256 delta / 5s，取先到者）
- **档位数 N**：MVP 定 50（Binance/OKX 常见档位），未来按 symbol 活跃度分级再议
- **Match 内部 orderbook → Top N 截取**：用 `container/heap` 维护 Top N 还是每次遍历 `btree` 前 N 节点，需基准测试
- **Delta 合并策略**：同一 `match_seq_id` 内可能多档位变化，是否合并成一条还是一个撮合 tick 多条。倾向单条合并（降低下游处理次数）
- **Full 帧生成不阻塞撮合**：应在 match 的 emit 协程里异步做，不在撮合协程里
- **cold start 保证**：Match restore 完（ADR-0048）必须先发一条 Full 再开始接受新单，否则下游回溯可能看不到对应 `match_seq_id` 的 Full

### 测试

- 单元：Top N 截取正确性、Delta 生成幂等、Full 帧包含当前全量
- 集成：模拟 Match crash + restart，下游 `OffsetTail(N)` 能在 2×Full-Interval 内找到 Full 自举
- 基准：Match 撮合路径 p99 / p999 变化 < 10%
- 一致性：运行 1h 高压撮合，下游 book 和 Match `orderbook.OrderBook` 全档对比 → 零偏差

### 灰度

- P1 阶段 Match 同时发老 trade-event 和新 OrderBook 帧，下游二选一
- BFF marketcache 先切，观察 1 周；Push 再切；Quote depth 最后下线
- 回滚路径：下游切回订阅 Quote 的老 DepthSnapshot topic，Match 保留老 emit 路径到 Quote 完全下线后再摘

## 参考 (References)

- [ADR-0021](./0021-quote-service-and-market-data-fanout.md) — Quote 独立服务、market-data fanout 结构（本 ADR 不挑战此结构，只换 fanout 源头）
- [ADR-0024](./0024-trade-event-order-accepted-extension.md) — OrderAccepted 扩展字段（本 ADR 落地后可简化）
- [ADR-0025](./0025-quote-engine-state-and-offset-strategy.md) — Quote 内部结构（depth 部分本 ADR 完成后重写）
- [ADR-0036](./0036-quote-state-snapshot.md) — Quote snapshot（本 ADR 完成后 books 字段移除）
- [ADR-0048](./0048-snapshot-offset-atomicity.md) — snapshot/offset 范式（Match 侧延续，Quote 侧 depth 部分不再需要）
- [ADR-0049](./0049-snapshot-protobuf-with-json-debug.md) — snapshot 格式
- [ADR-0050](./0050-match-input-topic-per-symbol.md) — match 输入 per-symbol topic 模式（本 ADR 输出侧同构）
- [ADR-0051](./0051-typed-producer-sequence-naming.md) — `match_seq_id` 命名
- 外部参考：
  - Bybit 泄露代码：`trading/idl/svc/trading/orderbook/order_book.proto`（`oneof{profile, trade}`）
  - Bybit 消费端回溯策略：`mt4/pkg/trading/orderbook/kafka_consumer.go`（`OffsetTail(150)`）
  - OKX `books` / `books-l2-tbt` 频道（`seqId`, `prevSeqId`, `checksum`）
  - Binance REST `/depth` + WS `depthUpdate`（`lastUpdateId` / `U` / `u` 串联）
  - CME MDP 3.0：snapshot feed + incremental feed（独立 topic 版本）
- 实现位置（动工时涉及）：
  - [match/internal/orderbook/](../../match/internal/orderbook/) — 新增 Top N 截取
  - [match/internal/journal/producer.go](../../match/internal/journal/producer.go) — 新增 OrderBook 帧 emit
  - [api/event/market_data.proto](../../api/event/market_data.proto) — 新增 `OrderBook` / `OrderBookFull` / `OrderBookDelta` 消息
  - [quote/internal/depth/](../../quote/internal/depth/) — P3 删除
  - [bff/internal/marketcache/](../../bff/internal/marketcache/) — P2 切换消费源
