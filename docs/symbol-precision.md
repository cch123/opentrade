# Symbol 精度指南(业务向)

> 目标读者:产品 / 运营 / 新入手的开发 / 接入方。
> 工程细节 → [ADR-0053](./adr/0053-symbol-precision-and-tiered-evolution.md)。

## 1. 为什么要管精度?

交易所的每个交易对(symbol)都要回答三个问题:

| 问题 | 业务后果(如果不管) |
|---|---|
| 价格最小可以变多少? | 用户挂 `0.000000001 USDT` 的档位,orderbook 膨胀,撮合变慢 |
| 数量最小可以填多少? | 用户挂 `0.00000000001 BTC` 的卖单,永远卖不出去,占内存 |
| 订单总金额最低多少? | 有人花 `$0.01` 下单,系统 100% 成本,0% 收益 |

主流交易所(Binance / OKX / Bybit)都用一套 "精度 filter" 在**下单入口**就拦掉不合规订单。本文档解释 OpenTrade 怎么配这些 filter、每个字段的业务含义。

## 2. 名词速查(中英对照)

| 字段名 | 业务叫法 | 你可以这样想 |
|---|---|---|
| `TickSize` | 价格步长 | 价格只能跳这么多(比如 BTC-USDT 每次至少 0.01) |
| `StepSize` | 数量步长 | 下单数量的最小粒度(比如 BTC 最小 0.00001) |
| `QuoteStepSize` | 金额步长 | **市价买(按金额)**时,花的钱的粒度(比如 USDT 最小 0.01) |
| `MinQty / MaxQty` | 数量上下限 | 一笔单最少/最多买几个 BTC |
| `MinQuoteQty` | 金额下限(数量侧) | 市价买的 USDT 最少花多少 |
| **`MinQuoteAmount`** | **最小下单金额** | **一笔单价值不得低于 $5(price × qty)** |
| `MarketMinQty / MarketMaxQty` | 市价单数量上下限 | 市价单有单独更严的数量限制 |
| `EnforceMinQuoteAmountOnMarket` | 市价单是否套最小金额 | 市价单没有 price,是否也管 "一笔 ≥ $5" |
| `AvgPriceMins` | 均价窗口 | 市价单算金额时用过去几分钟的成交均价 |
| `AdmissionTier` | 订单入场档位 | 订单下单时锁定的精度档,之后不会变 |

如果你看了 Binance / Bybit 文档,我们的命名映射:

| OpenTrade | Binance | Bybit V5 |
|---|---|---|
| `TickSize` | `PRICE_FILTER.tickSize` | `priceFilter.tickSize` |
| `StepSize` | `LOT_SIZE.stepSize` | `basePrecision` |
| `QuoteStepSize` | ❌(业务拼凑) | `quotePrecision` |
| `MinQuoteAmount` | `MIN_NOTIONAL.minNotional` | `minOrderAmt` |
| `MarketMinQty` | `MARKET_LOT_SIZE.minQty` | ❌ |
| `EnforceMinQuoteAmountOnMarket` | `MIN_NOTIONAL.applyToMarket` | ❌ |

> 我们没用 "notional" 这个词,因为中文"名义价值"太抽象,新人看一眼不懂。直接叫**最小下单金额**。

## 3. 一个完整例子:BTC-USDT

假设 BTC-USDT 的配置长这样:

```json
{
  "shard": "match-0",
  "trading": true,
  "base_asset": "BTC",
  "quote_asset": "USDT",
  "precision_version": 1,
  "tiers": [
    {
      "price_from": "0",
      "price_to": "0",
      "tick_size": "0.01",
      "step_size": "0.00001",
      "quote_step_size": "0.01",
      "min_qty": "0.0001",
      "max_qty": "1000",
      "min_quote_qty": "1",
      "min_quote_amount": "5",
      "market_min_qty": "0.0001",
      "market_max_qty": "500",
      "enforce_min_quote_amount_on_market": true,
      "avg_price_mins": 5
    }
  ]
}
```

### 这些数字意味着

- 价格只能填 `0.01 USDT` 的整数倍 → `50000.00 ✓`、`50000.01 ✓`、`50000.005 ✗`
- 买卖数量只能填 `0.00001 BTC` 的整数倍 → `0.12345 ✓`、`0.123456 ✗`
- **市价买按金额**时,USDT 预算得是 `0.01 USDT` 的整数倍 → `100.00 ✓`、`100.001 ✗`
- 单笔最少买 `0.0001 BTC`,最多 `1000 BTC`
- 市价买的 USDT 预算至少 `1 USDT`(上限数量侧)
- **一笔订单价值至少 `5 USDT`**(price × qty 或市价单的预算)
- 市价单数量独立护栏:最少 `0.0001 BTC`,最多 `500 BTC`(比限价单更严)
- 市价单也要过最小 `5 USDT` 检查 —— 用过去 5 分钟的成交均价估金额

### 举几个下单例子

| 下单请求 | 判定 | 原因 |
|---|---|---|
| `LIMIT buy 0.001 BTC @ 50000.00` | ✅ 通过 | 价格、数量都合规,金额 `50 USDT ≥ 5` |
| `LIMIT buy 0.0001 BTC @ 50000.00` | ✅ 通过 | 金额 `5 USDT = MinQuoteAmount`,等于边界通过(判定是 `< 5` 才拒) |
| `LIMIT buy 0.00009 BTC @ 50000.00` | ❌ `MinQuoteAmount` | 金额 `4.5 USDT < 5` |
| `LIMIT buy 0.001 BTC @ 50000.005` | ❌ `InvalidPriceTick` | 价格不是 0.01 的整数倍 |
| `LIMIT buy 0.0000001 BTC @ 50000.00` | ❌ `InvalidLotSize` | 数量不是 0.00001 的整数倍 |
| `LIMIT buy 0.00001 BTC @ 50000.00` | ❌ `MinQty` | 数量 `0.00001 < MinQty 0.0001` |
| `MARKET buy 100 USDT` | ✅ 通过 | 100 是 0.01 倍数、≥ 1、≥ 5 |
| `MARKET buy 0.5 USDT` | ❌ `MinQuoteQty` | 0.5 < 1 |
| `MARKET buy 3 USDT` | ❌ `MinQuoteAmount` | 3 ≥ MinQuoteQty 1,但 < MinQuoteAmount 5 |
| `MARKET buy 100.001 USDT` | ❌ `InvalidQuoteStep` | 100.001 不是 0.01 倍数 |

### 字段之间的关系图

```
            ┌──────────────────────────────────┐
            │ 下单请求:价格 P、数量 Q 或 预算 B │
            └────────────────┬─────────────────┘
                             │
                 ┌───────────┴───────────┐
                 │                       │
           [LIMIT 限价]             [MARKET 市价]
                 │                       │
        P mod TickSize == 0        (无 P,取参考价)
        Q mod StepSize == 0        Q mod StepSize == 0
        MinQty ≤ Q ≤ MaxQty        MarketMinQty ≤ Q ≤ MarketMaxQty
        P*Q ≥ MinQuoteAmount       若 Enforce=true:
                 │                   参考价*Q ≥ MinQuoteAmount
                 │                 若按金额买:
                 │                   B mod QuoteStepSize == 0
                 │                   B ≥ MinQuoteQty
                 │                   B ≥ MinQuoteAmount
                 │                       │
                 └───────────┬───────────┘
                             │
                        ┌────▼────┐
                        │ 进入撮合 │
                        └─────────┘
```

**关键规则**:
- `Tick` 管**价格的粒度**、`Step` 管**数量的粒度**、`QuoteStep` 管**金额的粒度**
- `Min*Qty` 管"最小卖几个",`MinQuoteAmount` 管"最低值多少钱"—— 两个必须同时满足
- 市价单有单独的 `MarketMinQty / MarketMaxQty`(可比限价更严),以及 `EnforceMinQuoteAmountOnMarket` 开关决定是否也检查金额

## 4. 新币上架:怎么填?

### 推荐套路

1. **先看这个币在别家交易所的 tick / step**(Binance / OKX / Bybit `exchangeInfo` 查)
2. **`MinQuoteAmount` 我们默认 `5 USDT`**(对齐 BN 现货),避免小额单拖累系统
3. **`TickSize` 一般取价格的 `1/10000` 数量级**
4. **`StepSize` 让 `MinQty × 最低价格 ≈ MinQuoteAmount`**

### 举几个典型币的推荐配置

| Symbol | 典型价格 | TickSize | StepSize | MinQuoteAmount |
|---|---|---|---|---|
| BTC-USDT | $50,000 | `0.01` | `0.00001` | `5` |
| ETH-USDT | $3,000 | `0.01` | `0.0001` | `5` |
| SHIB-USDT | $0.00002 | `0.00000001` | `1` | `5` |
| DOGE-USDT | $0.1 | `0.00001` | `1` | `5` |
| PEPE-USDT | $0.000001 | `0.000000001` | `100` | `5` |

> 超低价币用大 StepSize(成百上万个 token 一起卖)、小 TickSize(价格用小数点后 8-10 位)。这是行业惯例,不是 OpenTrade 特殊。

## 5. 币价大涨大跌怎么办?(分档演进)

### 场景:MEME 币往返

某 MEME 币上线时 $0.00001,涨到 $200,之后跌回 $0.0001。整个过程:

- **初始 tick 0.00000001**:$0.00001 时合理;涨到 $200 后意味着每跳 `0.00000005%`,没有意义
- **如果中途把 tick 改成 0.01**:原来 $0.00001 挂的单(价格是 `0.0000012345`)就不合规了,被强制撤单 → 用户体验雪崩
- **如果跌回 $0.0001 时还想再把 tick 改回细**:orderbook 中 tick=0.01 时期的老挂单怎么办?

### 我们的方案:单调分档

一个 symbol 可以配多个 `tier`(精度档位),**每档对应一个价格区间**。订单下单时按当时价格落到对应档位,**档位锁定到订单**,以后价格漂到别的档也不变。

```json
"tiers": [
  {"price_from": "0",       "price_to": "0.01",  "tick_size": "0.00000001", "step_size": "1000", ...},
  {"price_from": "0.01",    "price_to": "10",    "tick_size": "0.0001",     "step_size": "1",    ...},
  {"price_from": "10",      "price_to": "1000",  "tick_size": "0.01",       "step_size": "0.001",...},
  {"price_from": "1000",    "price_to": "0",     "tick_size": "1",          "step_size": "0.0001",...}
]
```

- 价格 $0.005 → 落到档 0 → tick=0.00000001
- 价格 $50 → 落到档 2 → tick=0.01
- 价格 $2000 → 落到档 3 → tick=1

**涨跌往返零撤单**:
- $0.005 挂的单用档 0 的 tick,涨到 $50 时这批单依然活着(档位已锁定)
- $50 下新单按档 2 的 tick,和档 0 的老单共存
- 跌回 $0.005 时,再下新单又按档 0 → 没有任何老单被撤

### 演进规则(运维必读)

tier 列表**只能加不能改**:
- ✅ **拆分**:`[10, 1000)` 拆成 `[10, 100)` + `[100, 1000)` (老挂单继续按"粗档"存活)
- ✅ **末端追加**:价格冲破顶档时加 `[1000, 10000)` → 改成 `[1000, 10000)` + `[10000, +∞)`
- ❌ **合并**:`[0, 1) + [1, +∞)` 合并为 `[0, +∞)`
- ❌ **删除**:去掉中间某档
- ❌ **改边界**:把 `[1, 100)` 的 `1` 改成 `2`

合并/删除会让老挂单失去自己档位的 context → 直接拒绝。

## 6. 市价单的两种姿势

OpenTrade 市价买有两种下单方式(对齐 BN,详见 [market-orders.md](./market-orders.md) 和 [ADR-0035](./adr/0035-market-orders-native-server-side.md)):

### A. 按数量买:`MARKET buy qty=0.5 BTC`
- 冻结按 `参考价 × 0.5 BTC` 估算
- 走 **StepSize / MarketMinQty / MarketMaxQty** 校验
- `EnforceMinQuoteAmountOnMarket=true` 时还要过 `参考价 × 0.5 ≥ MinQuoteAmount`

### B. 按金额买:`MARKET buy quote_qty=100 USDT`
- 冻结 `100 USDT`
- 走 **QuoteStepSize / MinQuoteQty / MinQuoteAmount** 校验
- 买到的 BTC 数量由撮合结果决定,精度残余(0 < x < StepSize 的未用金额)会退回

为什么区分这么细?因为 A / B 两种场景的"合规边界"不一样:
- A 基于 base(BTC)数量的粒度 —— 用 `StepSize`
- B 基于 quote(USDT)金额的粒度 —— 用 `QuoteStepSize`

BN 只管 A、Bybit 两个都管。OpenTrade 选 Bybit 的做法。

## 7. 冻结金额怎么算?

用户下单时,Counter 会冻结资产,避免超卖。冻结逻辑和精度挂钩:

| 订单类型 | 冻结什么 | 多少 |
|---|---|---|
| LIMIT buy | quote | `ceil_to_quote_step(price × qty)` |
| LIMIT sell | base | `qty` |
| MARKET sell(按数量) | base | `qty` |
| MARKET buy(按数量) | quote | `ceil_to_quote_step(参考价 × qty + 滑点 buffer)` |
| MARKET buy(按金额) | quote | `quote_qty`(按金额下单,直接冻结) |

`ceil_to_quote_step` 保证冻结金额是 `QuoteStepSize` 的整数倍(不会冻 `9.9999999...`)。撮合完成后的残余金额(市价单场景)通过 `UnusedQuote` 字段解冻。

## 8. 精度变更的操作流程

管理员通过 admin-gateway 改精度(见 [ADR-0052](./adr/0052-admin-console.md)):

```
PUT /admin/symbols/BTC-USDT/precision
{
  "effective_at": "2026-05-01T00:00:00Z",
  "new_tiers": [...],
  "new_precision_version": 2,
  "reason": "BTC 单价突破 $200k,拆分高价档"
}
```

系统会:
1. 记到审计日志
2. 推送给 match / counter(etcd watch)
3. 在 `effective_at` 到点时:
   - 暂停该 symbol 新订单(~100ms)
   - **拆分场景**:无需撤单,老挂单继续按原档生效
   - **首次上 Tiers(从兼容模式切强校验)**:扫 orderbook,标记不合规订单并走正常撤单流程(通知用户)
4. 原子切换 `precision_version`,继续接单

> **始终提前公告**:任何精度切换至少提前 10 分钟 schedule,给做市商留出调整时间。

## 9. 兼容模式(M0 阶段)

**如果 `tiers` 为空**,系统走"兼容模式":
- 不做任何精度校验
- 下单行为和改造前完全一致
- 这是为了让本功能可以**零风险上线**,然后分 symbol 逐步开启

迁移路径:
- **M0**:本文档写的 schema 上线,所有 symbol 的 `tiers` 仍为空
- **M1**:admin-gateway 支持 PUT 精度
- **M2**:运营用推荐工具 + 人工审核,给每个 symbol 填单档 tier(不强制)
- **M3**:灰度开启每 symbol 的强校验 —— 不合规订单会被撤掉(会提前通知用户)
- **M4**:MEME / 高波动币按需拆档

## 10. FAQ

**Q:为什么 MinQty 和 MinQuoteAmount 都要配?不是重复吗?**
A:前者管"最少几个 BTC",后者管"最低值多少钱"。低价币 `MinQty` 够高但金额还是很小时,`MinQuoteAmount` 拦下来;高价币金额 OK 但有人尝试买 `0.00000001 BTC` 时,`MinQty` 拦下来。两个是正交的。

**Q:为什么市价单要有单独的 `MarketMinQty / MarketMaxQty`?**
A:市价单吃穿 orderbook,风险更大。实操上交易所都对市价单设更严的上限(防滑点炸穿)和下限(防刷单)。

**Q:`EnforceMinQuoteAmountOnMarket=false` 是什么场景?**
A:极低门槛的微额交易对(比如内部测试币),不希望 `MinQuoteAmount` 挡住。默认 `true`,和 BN 现货默认对齐。

**Q:`AvgPriceMins` 为什么要配?**
A:市价单没有 price,怎么知道"价值多少 USDT"?答案:用过去 N 分钟的成交均价估。BN 默认 5 分钟,我们沿用。这个字段**只在市价单走 `MinQuoteAmount` 时用到**。

**Q:分档 tier 会不会把 orderbook 搞成碎片?**
A:不会。tier 只影响"新订单入场时用哪套粒度校验",orderbook 本身还是一个数组/红黑树,按价格排序。不同 tier 的订单可以自由撮合(每笔成交量是双方 stepSize 的倍数,数学上不产生 dust)。

**Q:`PriceTo: 0` 是什么意思?**
A:约定"开区间上界 = +∞",只有最后一档能这样写。你可以读成"从 PriceFrom 开始,往上没有限制"。

**Q:所有数值都是字符串?**
A:是。金融数值(价格/数量/金额)用 `shopspring/decimal` 的字符串表示,避免 float 精度丢失。`"0.00001"` 不是 `0.00001`(float),是精确的十进制数。

## 11. 相关文档

- [ADR-0053 精度治理工程决策](./adr/0053-symbol-precision-and-tiered-evolution.md)
- [ADR-0035 市价单原生支持](./adr/0035-market-orders-native-server-side.md)
- [ADR-0030 Match etcd symbol sharding](./adr/0030-match-etcd-sharding-rollout.md)
- [ADR-0052 Admin console](./adr/0052-admin-console.md)
- [market-orders.md 下单姿势](./market-orders.md)
- [glossary.md 项目术语表](./glossary.md)
