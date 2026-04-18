# Market orders — client guide

OpenTrade 支持两条等价于 Binance 现货的 MARKET 下单路径：

- **路径 A · 原生 MARKET**：服务端直接吃穿 orderbook，无价格保护（默认）
- **路径 B · BFF 滑点保护**：客户端传 `last_price + slippage_bps`，BFF 改写为
  `LIMIT + IOC` 下发

两条路径共用同一个 REST endpoint：`POST /v1/order`。决策背景见
[ADR-0035](./adr/0035-market-orders-native-server-side.md)。

## 路径 A：原生 MARKET（无滑点保护）

> 行为和 BN `POST /api/v3/order type=MARKET` 一致：吃穿就吃穿，后端不兜底。

### MARKET sell by quantity

```http
POST /v1/order
X-User-Id: u1
Content-Type: application/json

{
  "symbol": "BTC-USDT",
  "side": "sell",
  "order_type": "market",
  "qty": "0.5"
}
```

- `qty` 是 base 数量（要卖多少 BTC）
- 冻结 `0.5 BTC`，Match 按 bid 价队列吃
- 深度不够则剩余 `expired`，冻结自动释放

### MARKET buy by quote budget

```http
POST /v1/order
X-User-Id: u1
Content-Type: application/json

{
  "symbol": "BTC-USDT",
  "side": "buy",
  "order_type": "market",
  "quote_qty": "100"
}
```

- `quote_qty` 是 quote 预算（要花 100 USDT 买 BTC）—— 对应 BN 的 `quoteOrderQty`
- 冻结 `100 USDT`，Match 按 ask 价队列吃，消耗 `price × qty` 累加到 100
- 精度残余（< 1 satoshi 对应的 quote 金额）走 `expired` 释放

> **不支持** MARKET buy + `qty`（BN 的 `quantity` 形态）。原因：估算 quote
> freeze 需要 last price，Counter 不订阅行情。如果需要，用路径 B。

## 路径 B：带滑点保护的 MARKET

> 对应 BN App / 网页的"滑点容差"开关。**由客户端提供 last_price**，BFF 算
> 保护价后翻译成 `LIMIT + IOC`。

### MARKET buy with slippage cap

```http
POST /v1/order
X-User-Id: u1
Content-Type: application/json

{
  "symbol": "BTC-USDT",
  "side": "buy",
  "order_type": "market",
  "qty": "0.5",
  "last_price": "50000",
  "slippage_bps": 50
}
```

BFF 改写：

```
type=limit, tif=ioc, price=50250, qty=0.5
```

公式：`price = last_price × (1 + slippage_bps / 10000)`（sell 同公式减号）。

- `slippage_bps` 单位 basis point：50 = 0.5%，10000 = 100%
- `last_price` 必须是客户端 UI 当前显示的最新成交价（或任何合理参考价），
  服务端不校验偏差
- 翻译后是 `LIMIT + IOC`：未吃完的部分立即 `expired`，不挂单

### MARKET sell with slippage cap

同理：

```json
{
  "symbol": "BTC-USDT", "side": "sell", "order_type": "market",
  "qty": "0.5", "last_price": "50000", "slippage_bps": 50
}
```

BFF 翻译为 `LIMIT IOC price=49750 qty=0.5`。

## 字段总览

| 字段 | 类型 | 说明 |
|---|---|---|
| `order_type` | `"limit"` / `"market"` | |
| `side` | `"buy"` / `"sell"` | |
| `qty` | decimal string | base 数量；市买 + `quote_qty` 时留空 |
| `quote_qty` | decimal string | quote 预算；仅市买使用；和 `slippage_bps` 互斥 |
| `price` | decimal string | LIMIT 必填；MARKET 忽略 |
| `tif` | `"gtc"` / `"ioc"` / `"fok"` / `"post_only"` | MARKET 忽略 |
| `last_price` | decimal string | 路径 B 必填 |
| `slippage_bps` | int `(0, 10000]` | 路径 B 启用滑点保护 |

## 错误响应

| 情况 | HTTP | message（示意） |
|---|---|---|
| MARKET buy 既无 `quote_qty` 也无 `slippage_bps` | 400 | `market buy requires quote_qty (ADR-0035); ...` |
| MARKET buy 同时给了 `qty` 和 `quote_qty` | 400 | `pass either qty ... or quote_qty, not both` |
| MARKET sell 没给 `qty` | 400 | `market sell requires qty` |
| `slippage_bps>0` 但 `last_price` 空 | 400 | `slippage_bps requires last_price ...` |
| `slippage_bps>0` 且带了 `quote_qty` | 400 | `slippage_bps with quote_qty is ambiguous ...` |
| `slippage_bps` 非正或 >10000 | 400 | `slippage_bps must be in (0, 10000]` |
| 余额不足 | 409 | Counter 层返回 `FailedPrecondition` |

## 注意事项

- 路径 A 没有价格保护。深度浅的 symbol 上用户可能成交到坏价格 —— 这是 BN
  的行为，系统不兜底。想要保护就走路径 B。
- 路径 B 的 `last_price` 由客户端诚实提供。恶意客户端可以传一个远偏离实际
  的价格"绕过"保护，但这只伤害它自己，不影响其他用户。后续可能在 BFF 加
  `last_price vs depth top-of-book` 的合理性校验（backlog，见 ADR-0035 §未
  来工作）。
- `client_order_id` 在两条路径下都是幂等键（ADR-0015）。重复提交同一个
  `client_order_id` 会返回先前落库的订单，`accepted=false`。

## 相关

- [ADR-0035](./adr/0035-market-orders-native-server-side.md) — 决策
- [ADR-0034](./adr/0034-market-orders-client-side-translation.md) — 早期作废
  方案（历史记录）
- [bff/internal/rest/orders.go](../bff/internal/rest/orders.go) — 实现
- [counter/internal/engine/freeze.go](../counter/internal/engine/freeze.go) —
  freeze 规则
