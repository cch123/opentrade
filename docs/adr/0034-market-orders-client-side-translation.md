# ADR-0034: MARKET 单由客户端翻译为 LIMIT+IOC，后端不承载

- 状态: Superseded by [ADR-0035](./0035-market-orders-native-server-side.md)
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0020, 0021, 0035

> **⚠️ 本 ADR 的结论是错的。** 写作时把 BN 的"滑点容差"FAQ 误当成 BN 的
> 唯一 MARKET 形态；核对 BN REST API 后发现 `type=MARKET + quantity /
> quoteOrderQty` 服务端原生支持，滑点容差只是可选的前端 UI 糖。修正决策见
> [ADR-0035](./0035-market-orders-native-server-side.md)，保留本 ADR 的
> 讨论过程作为历史。

---

## 背景 (Context)

需求：用户希望下市价单(MARKET)。当前实现：
- Match engine **已经**支持 MARKET（`orderbook.Market` + `engine.Match` 里 `crosses()` 对 Market 恒为 true）
- Counter `engine.ComputeFreeze` 显式拒：`ErrMarketInMVP` —— 因为"MARKET 买单无 price，怎么估 freeze 量"没想清楚

要真让 MARKET 端到端跑通，有几种架构分叉：

1. **Counter 原生 MARKET**：Counter 要么接受 `quoteOrderQty`（quote 数量，BN 的一种 API 形态），要么引入 last price 缓存来估 freeze
2. **BFF 做翻译层**：BFF 维护 per-symbol last price，把 client 的 `type=market` 翻译成 LIMIT+IOC 下发
3. **客户端做翻译**：client 自己订阅行情 → 算保护价 → 提交 LIMIT+IOC；服务端只认识 LIMIT

参考 BN（Binance）实际做法：
- BN spot REST API 的 `type=MARKET` **没有**"滑点保护"参数，就是纯吃单（吃穿 orderbook 也不 care）
- BN 网页/App 的"滑点容差"是**前端 UI 层**的行为 —— 前端订阅 ticker，算保护价，调后端 REST 时用 `type=LIMIT, tif=IOC, price=保护价`
- matching engine 内部**只看到 LIMIT+IOC**，不区分"这原本是不是 MARKET"

关键观察：BN 的"MARKET + 滑点容差"不是服务端功能，是**客户端语法糖**。服务端完全不需要维护 last price。

## 决策 (Decision)

**方案 3：MARKET 在客户端翻译为 LIMIT+IOC，OpenTrade 服务端永不承载 MARKET。**

- Counter 继续拒 MARKET（`engine.ComputeFreeze` 的 `ErrMarketInMVP` 保留，语义升级为"永远不实现"）
- Match engine 里 `orderbook.Market` 类型的支持保留（无害的 dead branch，后续若需要可清理，但不急）
- **BFF** 在 `/v1/order` 入口直接拒 `order_type=market`，返回 `400 Bad Request` 并在错误 body 里写清楚：
  `"submit as {order_type:\"limit\", tif:\"ioc\", price:<slippage-bounded>}"`
- **客户端**负责：
  1. 订阅 `quote` 的行情（market-data WS stream，或 REST 拉最近成交）
  2. 按用户设定的滑点百分比算保护价：`buy ≈ last × (1 + slippage)`，`sell ≈ last × (1 - slippage)`
  3. 用 `LIMIT + IOC + price=保护价 + qty=意图量` 提交

## 备选方案 (Alternatives Considered)

### A. Counter 原生 MARKET（`quoteOrderQty`）
- 优点：协议原生，client 语义干净
- 缺点：要改 proto (`OrderPlaced.quote_qty`) + Match engine（按 quote 预算消耗 liquidity，引入 `RemainingQuote` 字段）+ Counter freeze/settlement 路径都要多一套分支。~300 行 + 破坏性 wire 变化
- 拒绝理由：BN 没这么做；工程量与价值不匹配

### B. Counter 原生 MARKET（`quantity` 形式，需要 last price）
- 优点：同上
- 缺点：Counter 要订阅 `market-data` 维护 last price cache，变成 stateful 行情观察者；freeze 估算对深度变化敏感，不准时会 under-freeze（用户穿仓风险）
- 拒绝理由：引入新职责，和 Counter "纯 state machine" 定位冲突

### C. BFF 做翻译层
- 优点：client 不用自己订阅行情；一次 POST 就行
- 缺点：
  - BFF 要消费 `market-data` topic（多一个 Kafka consumer，启动多一个依赖）
  - 多实例 BFF 每个都维护一份 per-symbol cache，主流交易对的 trade QPS 很高，**N 个 BFF 都订阅一遍流量放大 N 倍**
  - cache miss / 过期的降级策略还得设计（返 503？用某个深度推算？）
- 拒绝理由：交易量大，全订阅代价高；和 BN 架构不一致

### D（选中）. 客户端翻译
- 优点：
  - 后端零新职责、零改动
  - 客户端本来就在展示行情，拿 last price 零成本
  - 多实例扩展"免费"（last price 订阅是客户端本地的事）
  - 和 BN 架构对齐
- 缺点：
  - 每个客户端（Web / iOS / Android / CLI / 自动化 bot）都要实现"订阅 + 算保护价 + 提交 LIMIT IOC"的逻辑
  - 第三方 API user 需要读文档理解这套模式
- 结论：缺点是一次性文档/SDK 成本；收益是后端长期简洁

## 理由 (Rationale)

- **避免 Counter 变成行情观察者**：Counter 的核心职责是状态机（账户 + 订单生命周期），不应当因为"市价单"这个边缘语法糖多订阅一个 topic
- **避免 BFF 流量放大**：交易量大时，每个 BFF 实例单独订阅 `market-data` 会把 Kafka 网络流量 × N（N = BFF 实例数）。对比客户端每个连接订阅自己关心的 symbol 深度，流量有上限
- **和 BN 一致**：BN 规模比我们大得多都这么做，架构被验证过
- **LIMIT+IOC 已经够表达**：现有系统对 LIMIT+IOC 的支持完整（engine.Match 的 `priceAcceptable` 兼容，IOC 的 expired 处理路径已跑）—— 无需新代码

## 影响 (Consequences)

### 正面

- Counter / Match / BFF 零改动（除 BFF 入口的一条拒错分支）
- 服务端没有"新 symbol 上架时 last price 还没有"的边缘态
- 客户端对自己下的"市价单"有完全控制（滑点选什么、cache 多新鲜、是否 fallback）
- Match engine 里 `orderbook.Market` 的分支继续保留为 dead code —— 如果未来真的需要在服务端做 MARKET，已有脚手架可复活

### 负面 / 代价

- 每个客户端 SDK 要写一次翻译逻辑（网页前端 + 移动端 + 第三方 bot）。文档必须清晰
- 用户通过 curl 原始调用时必须自己算保护价。初期用户体验成本
- 如果后续业务真的需要"服务端保证的市价保护"（例如合规要求"交易所不能让用户成交到 5% 偏离"），再回头做服务端翻译；重开 ADR

### 中性

- Match engine 对 MARKET 的支持继续保留但不可达。一种未来 hedge，但也可能永远用不上

## 实施约束 (Implementation Notes)

### BFF 拒 MARKET

`bff/internal/rest/orders.go` 的 `handlePlaceOrder`：`parseOrderType` 返回 `ORDER_TYPE_MARKET` 时直接 `400`，body 里写明提示。新增 `TestPlaceOrderMarketRejected` 验证不会误转发到 counter。

### 客户端文档（Backlog）

专门写一篇 `docs/market-orders.md` 说明：
- 推荐订阅 `market-data` 的哪些 stream 拿 last price
- 保护价的公式
- 未成交部分（IOC expired）如何 surface 给用户
- 新 symbol 还没成交过（没 last price）时客户端怎么处理（提示用户下 LIMIT，或按订单簿顶档估算）

此 ADR 不包含客户端 SDK 实现。

### Counter 的 `ErrMarketInMVP` 改名

小清理：`ErrMarketInMVP` 语义从"暂时不支持"变成"永不支持（由客户端翻译）"。名字保留，加注释指向 ADR-0034。（未在本 ADR 代码改动里做，列 Backlog。）

## 未来工作 (Future Work)

- **客户端 SDK / 文档**：上面提到的 `docs/market-orders.md`
- **如果需要服务端保护**（合规等原因）：再次评估方案 C（BFF 翻译）；彼时可能用"边缘 BFF 只服务热门 symbol 的 MARKET"降低流量放大
- **Match engine 清理**：确认 MARKET 支持永远不需要后，把 `orderbook.Market` 和相关 dead branch 删掉（engine.go 的 `crosses / priceAcceptable / finalize` 几处）

## 参考 (References)

- [Binance 滑点容差机制说明](https://www.binance.com/zh-CN/support/faq/detail/c6c69301d6f6448487ce91a36074f305) — 页面明确把 MARKET 加滑点容差解释为"实际下 LIMIT+IOC"
- ADR-0020: 订单状态机（LIMIT + IOC 已完整支持）
- ADR-0021: Quote / market-data（客户端订阅 last price 的来源）
- 实现：[bff/internal/rest/orders.go](../../bff/internal/rest/orders.go)
