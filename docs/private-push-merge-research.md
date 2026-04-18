# 私有推送消息合并策略调研

> **关联 backlog**：[roadmap.md §【待调研】match→user 推送消息的合并策略](./roadmap.md#填坑--backlog)
>
> **调研日期**：2026-04-18
>
> **触发问题**：一个 taker 订单吃多个 maker 时，推给 taker 的 WebSocket 消息数量。

## 1. OpenTrade 现状

一笔 [Trade](../api/event/trade_event.proto) 在 [counter/internal/service/trade.go](../counter/internal/service/trade.go) 里会：

1. 对 maker / taker 各调一次 `applyPartyViaSequencer`
2. 每次产出 **2 条** private journal：
   - `OrderStatusEvent`（订单状态 + 累积 filled_qty）
   - `SettlementEvent`（本笔 balance 变化 + 余额快照）
3. [push PrivateConsumer](../push/internal/consumer/private.go) 逐条序列化为 JSON WS 帧，`hub.SendUser(userID, frame)` 逐条 fanout，**无 batch / coalesce / throttle**

**放大系数**：taker 吃 N 个 maker → taker 收到 **2N 条** WS 消息。
- N = 100 → 200 条
- N = 10000 → 20000 条

## 2. 业内 9 家对比

### 2.1 协议形态与消息粒度

| 交易所 | 通道结构 | 消息粒度 | 本笔字段 | 累积字段 | 帧层 batch | 100 fill 预期条数 |
|--------|---------|---------|---------|---------|-----------|------------------|
| **Binance** | 单通道 `executionReport` | 每笔一条 | `l/L/n` | `z/Z` | ✗ | ~100 |
| **BingX** | 单通道 `executionReport`（抄 BN） | 每笔一条 | `l/L` | `z/Z` | ✗（GZIP 压缩） | ~100 |
| **OKX** | `orders` + 可选 `fills`（VIP5+） | 每笔一条 | `fillSz/fillPx` | `accFillSz` | ✗ | ~100（普通）/ ~200（订双） |
| **Bybit** | `execution` + `order` + `fast_execution` | 每笔一条，数组打包 | `execQty` | `cumExecQty`（在 order 上） | ✓ **时间窗 batch** | **~20-60** |
| **MEXC** | `orders.pb` + `deals.pb` 双通道 | 各推一条 | deals 有 `price/quantity` | orders 有 `cumulativeQuantity` | ✗ | **~200** |
| **KuCoin** | `/spotMarket/tradeOrdersV2` 单通道 | `type=match` 一条 + `type=filled` 终态一条 | `matchSize/matchPrice` | `filledSize` | ✗ | ~100+1 |
| **Gate.io** | `spot.orders` + `spot.usertrades` 双通道 | 各推一条 | usertrades 有 `amount/price` | orders 有 `filled_total` | ✓ **跨 pair 合并** | ~200（同 pair） |
| **Coinbase** | 单通道 `user` | **订单级快照** | **✗ 无本笔** | `cumulative_quantity/number_of_fills/avg_price` | ✓ orders 数组 by 50 | **<< 100**（明细走 REST） |
| **Hyperliquid** | `userFills` + `userEvents` + `orderUpdates` | `aggregateByTime` 开关 | 可选（默认有） | 累积在 `orderUpdates` | ✓ `WsTrade[]` 原生数组 | **1（aggr）/ 100（不 aggr）** |

### 2.2 设计哲学与适用场景

| 交易所 | 核心设计思路 | 目标客户 | 明细获取路径 | 代价 |
|--------|------------|---------|------------|------|
| Binance | **字段合并最小化**：状态+本笔+累积+手续费塞一条 | 通用，做市友好 | WS 即完整 | 消息多 |
| BingX | 兼容 BN 生态 | 从 BN 迁移的客户 | WS + GZIP | 同 BN |
| OKX | **分层订阅**：普通订 orders，高阶加 fills | 零售 + 机构分层 | WS orders 完整 + fills 更细 | VIP 门槛 |
| Bybit | **协议内帧聚合**：服务端自动攒 | 做市 / 高频 | WS 完整 | 5-20ms 延迟 |
| MEXC | **双通道并推**（最省事的实现） | 通用 | WS 完整 | 消息最多 |
| KuCoin | **状态机分消息**（match / filled 语义清晰） | 通用 | WS 完整 | match 和 filled 拆开 |
| Gate.io | **多 pair 合并**（跨交易对 batch） | 多 pair 同时玩的客户 | WS 完整 | 同 pair 内没合并 |
| Coinbase | **WS-only-cumulative**：WS 管累积，REST 管明细 | **零售 C 端为主** | WS 累积 + REST `/fills` | 做市不友好，REST 额外一跳 |
| Hyperliquid | **客户端声明式聚合**：订阅时选 `aggregateByTime` | DEX / 块级撮合原生适配 | WS 都覆盖 | 只聚合 taker 方向 |

### 2.3 关键字段对照（"本笔 vs 累积"识别信号）

| 交易所 | "本笔"字段 | "累积"字段 | 订单状态字段 |
|--------|----------|-----------|-------------|
| Binance | `l` (qty), `L` (price), `n` (fee) | `z` (qty), `Z` (quote), `Y` (last quote) | `X` (状态), `x` (执行类型) |
| BingX | 同 BN | 同 BN | 同 BN |
| OKX | `fillSz`, `fillPx`, `fillFee` | `accFillSz`, `avgPx` | `state` (`partially_filled`/`filled`) |
| Bybit | `execQty`, `execPrice`, `execFee` | `cumExecQty`, `cumExecValue`, `cumExecFee` | `orderStatus` |
| MEXC | deals: `price`, `quantity` | orders: `cumulativeQuantity`, `avgPrice` | orders: `status` |
| KuCoin | `matchSize`, `matchPrice`, `tradeId` | `filledSize`, `filledFunds` | `type` (`match`/`filled`) + `status` |
| Gate.io | usertrades: `amount`, `price`, `fee` | orders: `filled_total`, `fill_price` | orders: `event` (`put`/`update`/`finish`) |
| Coinbase | **无** | `cumulative_quantity`, `filled_value`, `number_of_fills`, `avg_price` | `status` |
| Hyperliquid | fills 里有 `sz`, `px`, `fee` | orderUpdates: `totalSz`, `origSz` | `orderStatus` (`filled`/`open`/`canceled`) |

## 3. 关键洞察

- **撮合层都不做订单级合并**：所有家（除 Coinbase 和 Hyperliquid aggr=true）都同时暴露"本笔"和"累积"字段，这本身是"按 fill 粒度输出"的证据。撮合层合并会破坏审计 / 对账 / K 线的按笔语义，属于反模式。
- **合并只发生在三处**：
  1. 协议字段层（Binance / BingX / KuCoin-match）把订单状态 + 本笔 + 累积挤一条
  2. 帧层数组（Bybit / Hyperliquid / Coinbase）把多个 event 塞同一 WS 帧
  3. 客户端 opt-in（Hyperliquid aggregateByTime）让客户端选择要明细还是合并
- **OpenTrade 现状 ≈ MEXC/Gate**，属于"没优化"那档，不反常但有改进空间。

## 4. OpenTrade 标定

| 维度 | 对齐哪家 | 差距 |
|------|---------|------|
| 通道结构 | 类 MEXC（但更分裂：OrderStatus + Settlement 是**两个 event 类型同一通道**） | **独有的"一笔两条 event"**，所有家都是"一笔一条或多通道各一条" |
| 本笔/累积字段 | 有 `filled_qty_after`（累积），**无本笔独立字段**（由客户端从上一条推导） | 缺 Binance/OKX 的"本笔"显式字段，客户端对账更难 |
| 帧层 batch | 无 | 落后 Bybit / Gate / Coinbase / Hyperliquid |
| 100 fill 消息数 | **~200** | ≈ MEXC/Gate 双通道；是 BN/OKX 的 2×；是 Bybit 的 ~5×；是 Hyperliquid (aggr) 的 200× |
| 明细存储 | MySQL trades（[trade-dump](../trade-dump)） | 全行业标配，路径对 |

## 5. 五条候选改造路径

| 方案 | 对齐 | 做法 | 200 → N | 侵入面 | 权衡 |
|------|------|------|--------|-------|------|
| **A** 协议字段层合并 | BN / BingX / KuCoin-match | counter 合 `OrderStatusEvent` + `SettlementEvent` → 单条 `TradeUpdate`（状态 + balance delta/after + 本笔 fill） | 200 → ~100 | journal proto + push 输出 + BFF + 客户端契约（breaking） | 收益确定；**一次性改完长期不动**；要评估是否有只订一种事件的消费方 |
| **B** 传输帧层 batch | Bybit | push 侧 `(user_id, 5-20ms 窗口)` 聚合多条到同一 WS 帧（`data:[]`） | 200 → 可变（跟 burst） | push 内部，journal / 客户端基本不动 | 侵入最小；代价是 +5-20ms 延迟，"一 event 一 frame"语义变 |
| **C** 分层频道 | OKX / MEXC / Gate | 订单级 + 明细级两个 subscription，普通 user 订订单级 | 可选 | BFF / push 协议扩展 | 产品分层，留到对外 API 成熟 |
| **D** 客户端声明式聚合 | Hyperliquid `aggregateByTime` | 订阅时 `aggregate=bool`，服务端按 `taker_order_id` 在一个 match round 合并成 1 条 `TradeUpdate`（含 avg_price + total_qty + fills_count） | 200 → 2（aggr）/ 200（不 aggr） | counter / push 双视图，复杂度高 | 最灵活；对大单客户端吸引力大；只对 taker 方向有效 |
| **E** WS-only-cumulative | Coinbase | WS 只推订单累积快照，每笔明细去 REST `/myTrades` 查 | 200 → 10-20（跟订单状态变化） | journal 大改，客户端协议换 | 最省带宽；**做市 / 高频不友好**（要 PnL/fee rebate 实时流）；OpenTrade [history ListTrades](../history) 基建已对齐 |

### 5.1 组合推荐

- **保守路线（建议起点）**：先做 **A + B** 组合
  - A：消除"一笔两条 event"的冗余，对齐主流（BN/OKX/KuCoin）
  - B：防御突发大单（Bybit 式帧聚合）
  - 两者正交，可独立推进
- **进阶路线**：再加 **D**（Hyperliquid 式 opt-in 聚合）作为做市商 / 大单客户的明确入口
- **不推荐**：单独做 E（过于激进，做市商体验下降）

## 6. 极端大单场景（吃几万 maker）

**问题**：协议层即使做了合并（200 → 100），吃几万 maker 仍是几千到几万条 WS 消息。一线所真的扛吗？

**答案**：不扛。**一线所都不在协议层解决这个问题**，而是靠三层防御组合：

### 6.1 源头防护（让超大单扫不到几万档）

- 价格保护类订单：BN `MARKET_PROTECTED` / `priceMatch`；OpenTrade 已有 [ADR-0035 滑点保护](./adr/0035-market-orders-native-server-side.md)
- IOC / FOK 限制扫深
- 最大下单限制（maxQty / maxNotional）
- Iceberg / TWAP 客户端自拆
- STP 自成交防护

### 6.2 基础设施层降级（真扫穿时，不保完整性）

- **WS best-effort 语义**：BN 文档明确"User Data Streams 是 best-effort … 连接中断期间事件会丢失，推荐 REST 做 reconciliation"
- **Send buffer 满 → disconnect**：BN / OKX / Bybit 通用策略；客户端消费慢直接踢
- **WS rate limit**：BN 5 records/s 上限，超限断链
- **历史教训**：2022 LUNA 崩盘、FTX crash 期间一线所都大量丢 executionReport，默认"已知特性"

### 6.3 客户端兜底

- WS 是快速路径、REST 是真相源（BN/OKX 文档反复强调）
- 重连后全量 resync（拉 openOrders + fills）
- 检测 executionId gap → REST 补

### 6.4 为什么协议层不强合并

- 每笔 fill 要独立 `trade_id`（税务 / MiFID II / 做市 rebate）
- maker 侧每个 order owner 不同，合并后无法分配 rebate
- [trade-dump](../trade-dump) 要按笔落库，合并后无法 reconstruct

### 6.5 UI 层真相

- **UI 不会展示几万行**。订单历史列表一个订单一行（显示累积 + 均价），详情页默认显示前 50 笔 fills + "查看全部"分页
- 前端 store 用 `order_id` 聚合，**100 条 WS = 100 次本地 state update**（不是 100 次 DOM render），配合 virtualized list
- **WS 粒度 ≠ UI 粒度**，是正交维度

### 6.6 对 OpenTrade 的启示

协议优化（A/B/C/D/E）只解决"正常 + 中等 burst"场景。极端大单需要配套：

1. **push per-user send buffer + disconnect-on-full 策略**（当前是否有需核查）
2. **WS rate limit**（按 user，对齐 BN 式）
3. **BFF 重连 resync 协议**（私有流版的 [ADR-0038](./adr/0038-bff-reconnect-snapshot.md)，WS 断开后 REST 一次性返回 last_event_id 后的快照）
4. **文档化 "WS best-effort, REST 是真相源"**，明确客户端合约

**这些基础设施项的优先级 ≥ 消息合并**，属于正确性而非优化。建议单独立条追踪。

## 7. 决策依据待收集

调研时还未做的量化 / 定性输入：

1. 实测 burst 场景（MM 细密挂单 / 市价扫深）下 push 实际帧率、带宽、下游客户端处理时间
2. 客户端（含 MM、高频、零售）对"一笔事件 = 一条消息"的期望强度调研
3. 现有 counter-journal 下游消费方里，是否有"只想要 OrderStatus"或"只想要 Settlement"的场景（如果没有，A 方案就是纯赚）
4. 若走 D/E，是否对外宣传类似"Hyperliquid 风格聚合"作为产品卖点
5. 极端行情下 push / WS 的稳定性表现（要上 chaos 测试）

## 8. 不动的部分

无论选哪条路径，以下都保持不变：

- [match Engine](../match/internal/engine) 按 fill 输出 Trade 事件
- [trade-dump](../trade-dump) 按笔落 MySQL `trades` 表
- [counter handleTrade](../counter/internal/service/trade.go) 按笔处理 + 按笔推进 matchSeq（[ADR-0048 后续项已做](./roadmap.md)）
- [history ListTrades](../history) 按笔返回
- [quote](../quote) K 线按笔聚合

## 9. 参考资料

- [Binance User Data Stream](https://developers.binance.com/docs/binance-spot-api-docs/user-data-stream)
- [OKX V5 API 总览](https://www.okx.com/docs-v5/en/)
- [Bybit V5 Execution WS](https://bybit-exchange.github.io/docs/v5/websocket/private/execution)
- [Bybit V5 Order WS](https://bybit-exchange.github.io/docs/v5/websocket/private/order)
- [Bybit V5 Fast Execution](https://bybit-exchange.github.io/docs/v5/websocket/private/fast-execution)
- [MEXC Spot V3 User Data Streams](https://www.mexc.com/api-docs/spot-v3/websocket-user-data-streams)
- [KuCoin Private Order Change V2](https://www.kucoin.com/docs/websocket/spot-trading/private-channels/private-order-change-v2)
- [BingX API Docs](https://bingx-api.github.io/docs/)
- [Gate.io Spot WebSocket V4](https://www.gate.com/docs/developers/apiv4/ws/en/)
- [Coinbase Advanced Trade WebSocket Channels](https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels)
- [Hyperliquid WebSocket Subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)
