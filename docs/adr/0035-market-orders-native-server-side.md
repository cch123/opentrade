# ADR-0035: MARKET 单服务端原生支持 + 可选 BFF 滑点保护翻译

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0020, 0005, 0034（已作废）

## 背景 (Context)

ADR-0034 曾把 BN 的"滑点容差"FAQ 误认为 BN MARKET 的**唯一**形态，从而得出
"服务端永不承载 MARKET、全部客户端翻译 LIMIT+IOC"的结论。复核 BN 现货
REST API 后发现：

- `POST /api/v3/order type=MARKET + quantity / quoteOrderQty` 是**服务端原生
  支持**的，撮合引擎真·吃穿 orderbook，没有内建滑点保护
- 网页 / App UI 上的"滑点容差"开关是**可选前端糖**：勾上后，客户端在发 REST
  之前先把 MARKET 转成 `LIMIT + IOC + price = last × (1 ± slippage)`
- 两种模式独立且正交：默认 MARKET 无保护；用户显式启用才有保护

ADR-0034 的方案 C（"BFF 全量订阅 market-data 做翻译"）被"交易量下流量放大
N 倍"这条合理顾虑否决。但这个顾虑只对"**BFF 订阅行情维护 last price cache**"
这种实现方式成立；如果把 "last price 从哪里来" 这个问题交还给**客户端**——
客户端本来就在 UI 里展示 last price——BFF 只对 **每次请求携带的 last_price**
做一次保护价计算，就完全没有"N 倍放大"的问题。

用户给出的三条具体规范也直接对应这一点：

1. 市价单**默认**没有滑点保护；
2. 启用滑点保护必须**由客户端传入 last_price**，否则服务端拒单；
3. 实现这两条路径。

## 决策 (Decision)

### 路径 A：原生 MARKET（对齐 BN `type=MARKET`）

服务端直接支持 `type=MARKET`，BN 子集：

- **MARKET sell** + `qty`(base)：Counter freeze = `qty`（base）
- **MARKET buy** + `quote_qty`(quote)：Counter freeze = `quote_qty`，Match 引擎按
  quote 预算消耗 ask 侧 liquidity
- **不做 MARKET buy + `quantity`**：这种形态需要 Counter 在下单时估算 quote
  freeze，进而需要维护 last price 状态（订阅 market-data）。明确排除，避免让
  Counter 变成行情观察者

行为：

- 打到 ask / bid 深度的最好档位开始吃，直到 `qty` / `quote_qty` 用尽
- 深度不够则剩余部分 `Expired`（现有 IOC 路径已处理）
- 无价格保护，**用户可能成交到任意差价**

### 路径 B：可选滑点保护（由 BFF 翻译，last_price 由客户端提供）

`POST /v1/order` 增加两个可选字段：

- `last_price` — 客户端当前 UI 显示的最新成交价（decimal 字符串）
- `slippage_bps` — 可接受滑点，10000 bps = 100%（0 或缺省 → 关闭保护）

BFF 行为：

| 输入 | BFF 行为 |
|---|---|
| `type=market` + `slippage_bps=0`（默认） | **不翻译**，按路径 A 直传 Counter（需要 `qty` 或 `quote_qty`） |
| `type=market` + `slippage_bps>0` + `last_price` 非空 | 算保护价：buy=`last×(1+bps/10000)` / sell=`last×(1-bps/10000)`；**翻译**成 `type=limit, tif=ioc, price=保护价`，`qty` 保留；发给 Counter |
| `type=market` + `slippage_bps>0` + `last_price` 空 | **400 Bad Request**，message 指出需要 `last_price` |
| `type=market` + `slippage_bps>0` + `quote_qty` | **400 Bad Request**：`quote_qty` 语义是"花这么多"，滑点保护（限价上下限）无意义，明确拒避免二义 |
| `type=limit`（既有） | 原路径 |

**关键**：BFF 完全不自己维护 last price。翻译所需的价格来自客户端请求，每次
请求自带。服务端任何组件都不订阅 market-data。

## 备选方案 (Alternatives Considered)

### X. ADR-0034 原方案（服务端永不承载 MARKET）
- 缺点：客户端无法直接发 MARKET；每个 SDK 都要自行实现翻译；文档负担高
- 拒绝理由：BN 服务端原生支持 MARKET；我们追求的是"BN 风格"

### Y. BFF 订阅 market-data 自维护 last price（ADR-0034 讨论的方案 C）
- 缺点：每 BFF 实例订阅全量 market-data，流量 N 倍放大
- 拒绝理由：交易量大时不可接受；last_price 由客户端带即可

### Z. Counter 原生 MARKET + `quantity` 形式 buy
- 缺点：Counter 要 cache last price 做 freeze 估算 → stateful 行情观察者
- 拒绝理由：违反 Counter "纯账户状态机"定位

### Q. 客户端全权（彻底 ADR-0034）
- 缺点：每个客户端 SDK 重复实现 "last price 订阅 + 翻译"；curl 用户体验差
- 拒绝理由：保护价翻译这种无状态计算放 BFF 成本极低，不值得推给每个客户端

## 理由 (Rationale)

- **路径 A 复刻 BN 原生 MARKET**，用户下 MARKET 就是 MARKET，吃穿了也是用户自
  己的事（和 BN 一致）
- **路径 B 的 last_price 由客户端带**—— 绕开了"谁订阅行情维护 cache"这个分歧
  点。客户端 UI 本来就在展示 last price，把它当参数传没额外成本
- BFF 做翻译是**无状态的纯计算**，没有流量放大、没有启动依赖、没有多实例一致
  性问题
- 两条路径正交：服务端的实现和 BFF 的翻译可以独立演进/测试

## 影响 (Consequences)

### 正面

- 默认 MARKET 行为直观（和 BN 一样）
- 滑点保护不需要新 Kafka consumer / 新依赖
- BFF 保持几乎无状态（除现有 rate-limit 计数器外）
- 未来如果真要做"服务端 last price 校验保护价合理性"，可以在 BFF 层加一个
  "last_price 不能偏离 depth top 5%"之类的防御，不影响架构

### 负面 / 代价

- 路径 A 会让深度浅的 symbol 上用户吃到坏价格 —— 这是 BN 行为，不做兜底
- 路径 B 依赖客户端**诚实传 last_price**。恶意客户端可以传一个远偏离实际的
  price 绕过保护；但这只是伤害它自己，不伤害他人。如果未来要防御，BFF 可以
  做 "last_price vs top-of-book 合理性检查"（本 ADR 不做）
- 新字段：proto `OrderPlaced.quote_qty`、REST `placeOrderBody.quote_qty /
  last_price / slippage_bps`；客户端 SDK 要更新
- Match engine 新增"按 quote 预算消耗"分支，小增复杂度

### 中性

- MARKET buy 的 `quantity` 形态 **明确不支持**。如果未来需要，要么做路径 Z
  (Counter 订 last price)，要么在 BFF 里由客户端提供 last_price 估算（类似路径 B
  但用于估 freeze）。Backlog。

## 实施约束 (Implementation Notes)

### proto

`api/event/order_event.proto` `OrderPlaced`：

```proto
message OrderPlaced {
  // ... 现有字段 ...
  string qty = 9;        // base qty; empty for market buy with quote_qty
  string quote_qty = 11; // quote budget; required for market buy with this form
}
```

wire-compatible（字段号 11，旧消费者忽略）。

### Match engine

- `orderbook.Order` 加 `QuoteQty` / `RemainingQuote`（仅 market buy 用）
- `IsQuoteDriven()`: `Type == Market && Side == Bid && QuoteQty > 0`
- `engine.Match` 对 quote-driven taker：
  - `matchQty = min(best.Remaining, remaining_quote / best.Price)`
  - `RemainingQuote -= matchQty × best.Price`
- 其他 shape 行为不变
- `IsLive()` 对 quote-driven 用 `RemainingQuote > 0`

### Counter freeze

`engine.ComputeFreeze` 去掉 `ErrMarketInMVP`：

| side × type | freeze asset | freeze amount |
|---|---|---|
| limit buy | quote | price × qty |
| limit sell | base | qty |
| market sell | base | qty |
| market buy（with quote_qty） | quote | quote_qty |
| market buy（with qty only） | — | **明确拒**：返回 "market buy requires quote_qty" |

### Counter order / settlement

- `engine.Order` 加 `QuoteQty`（snapshot 持久化）
- `unfreezeResidual(o)` 对 market buy 特殊处理：`residual = frozen_amount -
  sum(已 unfreeze quote)`；其他保持
- 实际 per-trade settlement 现有逻辑不变（`delta_quote = -price × qty`）

### BFF

`placeOrderBody`：

```go
type placeOrderBody struct {
    // ... 现有字段 ...
    QuoteQty     string `json:"quote_qty,omitempty"`
    LastPrice    string `json:"last_price,omitempty"`
    SlippageBps  int    `json:"slippage_bps,omitempty"`
}
```

`handlePlaceOrder`：

1. `type=market` 默认分支：去掉之前 ADR-0034 里 `return 400`，直传 Counter
2. `type=market` + `slippage_bps>0`：
   - 要求 `last_price` 非空，否则 400
   - 要求不带 `quote_qty`（滑点保护 + quote 预算二义），否则 400
   - 计算保护价，重写为 `type=limit, tif=ioc, price=保护价`，下传

### 客户端约定（文档 Backlog）

- 下 MARKET 不要滑点保护：`{type:"market", side:"buy", quote_qty:"100"}`（或
  sell + qty）
- 下 MARKET 要滑点保护：`{type:"market", side:"buy", qty:"1", last_price:"50000",
  slippage_bps:50}` — BFF 会翻译为 `LIMIT price=50250, tif=ioc, qty=1`

## 未来工作 (Future Work)

- **BFF 保护价合理性校验**：last_price 和当前 depth top-of-book 偏差 > X% 拒
  单，防止恶意 / 陈旧 last_price
- **MARKET buy with `quantity`**：如果有需求，BFF 可以接受 `qty + last_price` 估算
  `quote_qty = qty × last_price × 1.05`（5% 冗余 freeze）下传。本 ADR 不做
- **`docs/market-orders.md`**：客户端指导（两条路径用法、字段、失败场景）

## 参考 (References)

- [Binance Spot API — Create Order](https://binance-docs.github.io/apidocs/spot/en/#new-order-trade) —
  `type=MARKET` + `quantity` / `quoteOrderQty` 定义
- [BN 滑点容差 FAQ](https://www.binance.com/zh-CN/support/faq/detail/c6c69301d6f6448487ce91a36074f305)
- ADR-0034（已作废）：最初的"服务端永不承载 MARKET"方案
- 实现：
  - [api/event/order_event.proto](../../api/event/order_event.proto)
  - [match/internal/orderbook/types.go](../../match/internal/orderbook/types.go)
  - [match/internal/engine/engine.go](../../match/internal/engine/engine.go)
  - [counter/internal/engine/freeze.go](../../counter/internal/engine/freeze.go)
  - [counter/internal/service/order.go](../../counter/internal/service/order.go)
  - [bff/internal/rest/orders.go](../../bff/internal/rest/orders.go)
