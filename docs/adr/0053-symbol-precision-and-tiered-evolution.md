# ADR-0053: Symbol 精度治理与分档演进（Tiered Precision）

- 状态: Proposed
- 日期: 2026-04-19
- 决策者: xargin, Claude
- 相关 ADR: 0030（match etcd symbol sharding）、0035（市价单服务端原生）、0041（counter reservations）、0048（snapshot/offset 原子绑定）、0052（admin console）

## 术语 (Glossary)

本 ADR 字段命名偏向"读代码一眼懂"，不追求与 BN 官方字段名逐字对齐。映射关系如下：

| 本 ADR | 含义 | 行业等价 |
|---|---|---|
| `TickSize` | 价格最小步长 | Binance `PRICE_FILTER.tickSize` / Bybit `priceFilter.tickSize` |
| `StepSize` | base 资产数量最小步长 | Binance `LOT_SIZE.stepSize` / Bybit `basePrecision` |
| `QuoteStepSize` | quote 资产数量最小步长（市价买 by quote） | Bybit `quotePrecision` |
| `MinQty` / `MaxQty` | base 数量上下限 | Binance `LOT_SIZE.min/maxQty` |
| `MinQuoteQty` | quote 数量下限 | Bybit `lotSizeFilter.minOrderAmt`（数量语义） |
| **`MinQuoteAmount`** | **最小下单金额**（= `price × qty`） | **Binance `MIN_NOTIONAL.minNotional` / Bybit `minOrderAmt`（金额语义）** |
| `MarketMinQty` / `MarketMaxQty` | 市价单独立的数量护栏 | Binance `MARKET_LOT_SIZE` |
| `EnforceMinQuoteAmountOnMarket` | 市价单是否套 `MinQuoteAmount`（市价单无 price，用滚动均价估） | Binance `MIN_NOTIONAL.applyToMarket` |
| `AvgPriceMins` | 市价单估金额的滚动均价窗口 | Binance `MIN_NOTIONAL.avgPriceMins` |
| `AdmissionTier` | 订单进场时绑定的档位（本 ADR 原创，服务于分档演进） | 无 |

**不用 "notional"** 的理由：金融术语"名义价值"对非交易背景开发者不直观；Bybit 官方字段名就叫 `minOrderAmt`（最小下单金额），本 ADR 采用同一直白命名风格。

## 背景 (Context)

当前 OpenTrade **没有任何 symbol 精度规格**。具体表现：

1. `pkg/etcdcfg/etcdcfg.go:28-40` 的 `SymbolConfig` 只有 `Shard / Trading / Version` 三个字段，没有 tickSize / stepSize / minQuoteAmount
2. `match/internal/orderbook/types.go:109-110` 定义了 `RejectInvalidPriceTick` / `RejectInvalidLotSize` 常量，但撮合代码路径从未赋值
3. Counter 冻结逻辑 `counter/engine/freeze.go:42-81` 只检查 `qty > 0`
4. 撮合完成判定 `match/internal/engine/engine.go:179` 是严格零值（`Remaining == 0`），无 dust 阈值
5. Quote-driven 市价买 `engine.go:127` 用 `quote/price.Truncate(18)`，必然产生 ~1e-14 级 Quote 残余（`engine_test.go:233-235` 已观测）

引出三类风险：

- **Dust 挂单**：GTC 部分成交后残量不足一分钱也回插 orderbook，长期累积占内存 / 拉低撮合性能
- **对账困难**：Counter 冻结按 `price*qty` 的 Decimal 乘法，部分成交的 unfreeze / 残留金额漂移在 1e-14 级别，无 epsilon 约束时会触发虚警
- **MEME 币大涨大跌场景（`0.00001 → 200 → 0.0001` 往返）**：即使加了定长 tick，一旦涨 10^7 倍，原 tick 不适用；反跌时如果重新调细又需要 cancel all → 体验雪崩

一线交易所的 filter 体系 —— Binance `exchangeInfo`、OKX `instruments`、Bybit V5 `instruments-info` —— 是业界标准解法，不复造轮子。本 ADR 决定这套精度治理在 OpenTrade 的落地形态。

## 决策 (Decision)

### 1. Filter schema —— Bybit V5 骨架 + Binance 补强

三家对比：

| 能力 | Binance | OKX | Bybit V5 |
|---|---|---|---|
| tickSize / stepSize / minQty | ✅ | ✅ | ✅ |
| 最小下单金额 | ✅(`MIN_NOTIONAL`) | ❌ | ✅(`minOrderAmt`) |
| **base / quote 精度分离** | ❌ | ❌ | **✅**(`basePrecision` + `quotePrecision`) |
| 市价单独立约束 | ✅(`MARKET_LOT_SIZE`) | ❌ | ❌ |
| 最小下单金额套市价单 | ✅(`applyToMarket` + `avgPriceMins`) | ❌ | ❌ |
| 价格偏离护栏 | ✅(`PERCENT_PRICE_BY_SIDE`) | ❌ | ❌ |
| Decimal 映射友好度 | 一般(filter type 分类) | 好 | **最好**(平铺) |

**选 Bybit 做骨架**，因为 OpenTrade 已经落地 ADR-0035 的 quote-driven 市价买（`quoteOrderQty` 模式），Bybit 的 `quotePrecision` + `minOrderAmt` 是**唯一直接对位**的解法；BN/OKX 都要在业务层拼凑。

**从 BN 抄两件事**：
- `MinQuoteAmount` 的 `EnforceMinQuoteAmountOnMarket` + `AvgPriceMins` 语义（市价单无 price，用 quote 服务提供的 N 分钟滚动均价估金额）
- `MarketMinQty / MarketMaxQty`（市价单独立的数量护栏）

**OKX 弃用**：缺 MinQuoteAmount + 不分 quote 精度，对 OpenTrade 零收益。

### 2. `SymbolConfig` 扩展（向后兼容）

```go
// pkg/etcdcfg/etcdcfg.go
type SymbolConfig struct {
    // 既有
    Shard   string
    Trading bool
    Version string

    // 新增（零值 = 兼容模式，跳过精度校验）
    BaseAsset, QuoteAsset string
    PrecisionVersion      uint64           // 每次精度变更 +1，幂等键
    Tiers                 []PrecisionTier  // 单调分档（按 PriceFrom 升序）
    ScheduledChange       *PrecisionChange // 预告下次变更（至少提前 N 分钟）
}

type PrecisionTier struct {
    PriceFrom, PriceTo dec.Decimal  // [From, To)，首档 From=0，末档 To=+Inf

    // Bybit 骨架
    TickSize       dec.Decimal   // 价格步长
    StepSize       dec.Decimal   // base 数量步长（限价 & 市价卖）
    QuoteStepSize  dec.Decimal   // quote 数量步长（市价买 by quote，ADR-0035）
    MinQty, MaxQty dec.Decimal   // base 护栏
    MinQuoteQty    dec.Decimal   // quote 护栏
    MinQuoteAmount dec.Decimal   // 最小下单金额（price*qty 下限）

    // BN 补强
    MarketMinQty, MarketMaxQty    dec.Decimal // 市价单独立数量护栏（零值 = 沿用 MinQty/MaxQty）
    EnforceMinQuoteAmountOnMarket bool        // 市价单是否套 MinQuoteAmount
    AvgPriceMins                  uint32      // 市价单估金额的滚动均价窗口（默认 5，由 quote 服务提供）
}

type PrecisionChange struct {
    EffectiveAt      time.Time        // UTC 绝对时间
    NewTiers         []PrecisionTier
    NewPrecisionVer  uint64           // = 当前版本 + 1
    Reason           string           // 审计用
}
```

### 3. 分档演进（Tiered Precision）—— 解决往返

**核心不变式**：
```
对任意活跃挂单 o：
  o.Remaining mod o.AdmissionStepSize == 0
  （order 自带入场档位的 stepSize，冻结到 Order 结构）
```

**规则**：

1. **入场 tier 冻结到 Order**：新订单入场时按 `mid-price`（best bid/ask 中位，quote 服务提供）落到对应 `Tier`，把 `AdmissionTickSize / AdmissionStepSize / AdmissionQuoteStepSize` 写入 Order 结构并持久化到 Kafka journal
2. **入场后不再按价格漂移校验**：orderbook 里的挂单不因市场价穿档而变"非法"—— 这是"扛往返"的核心
3. **Tier 只允许 append 或细分**：
   - `[10, 1000)` 拆成 `[10, 100)` + `[100, 1000)` ✅（旧订单沿用粗档自动合规）
   - 向末端追加新档位 `[1000, 10000)` ✅
   - 合并相邻档位 ❌（旧订单失去其档位 context）
   - 删除档位 ❌
4. **单档即方案 A**：一 symbol 一档 `[0, +Inf)` 就是定长 tick，绝大多数稳定币对永远不需要拆档
5. **往返零成本**：价格从 Tier 4 跌回 Tier 1，Tier 4 期间挂的旧单继续按 Tier 4 stepSize 活着；Tier 1 的新单按 Tier 1 stepSize 进场；两批订单在同一 orderbook 共存，撮合时 `matchQty = dec.Min(taker.Remaining, maker.Remaining)`，对双方各自的 stepSize 都是整数倍（LCM 推理见下）

**撮合时的 dust 消除推理**：
- 假设 taker.step = Sa, maker.step = Sb，二者可能不同档位
- `matchQty = min(taker.Remaining, maker.Remaining)`
- `taker.Remaining mod Sa == 0`, `maker.Remaining mod Sb == 0`
- 新 Remaining = old - matchQty，仍然是 Sa（或 Sb）的倍数，不变式保持
- 不产生 dust

### 4. 三层校验（Defense in Depth）

| 层 | 职责 | 失败行为 | 新增代码位置 |
|---|---|---|---|
| BFF | 拉 symbol cache 做 client hint 预检（减少无效 RPC） | HTTP 400 + reason | `bff/internal/rest/orders.go`，可选 |
| **Counter（权威）** | 下单前 `validateOrderAgainstTier(order, tier)`，冻结按 `QuoteStepSize` 向上取整 | `RejectInvalidPriceTick` / `RejectInvalidLotSize` / `RejectMinQuoteAmount`（新增） | `counter/engine/validate.go`（新文件） |
| **Match（防御性）** | 收到 order-command 后再校验一次 `AdmissionTier` 与 symbol 当前 `PrecisionVersion` 兼容 | 走现有 reject event | `match/internal/engine/engine.go:validate` |

**reject reason 统一复用** `orderbook/types.go` 已有常量 + 新增：
```go
RejectInvalidPriceTick   // 已存在，首次启用
RejectInvalidLotSize     // 已存在，首次启用
RejectMinQuoteAmount     // 新增
RejectMinQty             // 新增
RejectMaxQty             // 新增
RejectPrecisionMismatch  // 新增：PrecisionVersion 不匹配（极罕见）
```

### 5. Quote-driven 市价买的残余退回

旧：`engine.go:127` `matchQty = dec.Min(best.Remaining, (quote/price).Truncate(18))`

新：
```go
rawQty   := quote.Div(price)
matchQty := floorToStep(rawQty, admissionTier.StepSize)  // 向下取整到 stepSize
if matchQty.LessThan(admissionTier.MinQty) { break }      // 预算不够一个 step，收尾
```

**未用金额 `UnusedQuote = BudgetQuote - Σ(fill.price * fill.qty)`** 在 `TakerFilled / TakerExpired` 事件里新增字段携带：

```protobuf
message TakerResult {
  ...
  string unused_quote = 20;  // market buy by quote 的残余，Counter 消费后解冻
}
```

Counter 消费 trade-event 时调 `Unfreeze(user, quote_asset, unused_quote)`，与 ADR-0048 的 trade-event 幂等天然兼容（按 trade_id 去重）。

### 6. 在线变更协议（Precision Rollout）

admin-gateway 新增：
```
PUT    /admin/symbols/{symbol}/precision   # body: PrecisionChange
DELETE /admin/symbols/{symbol}/precision   # 取消 pending change
```

流程：
1. admin PUT `ScheduledChange { EffectiveAt: now + 10min, NewTiers: [...] }`
2. etcdcfg 写 `/cex/match/symbols/<SYMBOL>` 更新的 SymbolConfig，match/counter 通过 watch 收到
3. `EffectiveAt` 到点，match 侧：
   - `Trading = paused`（新 order 拒绝 `SYMBOL_PRECISION_SWITCHING`，~100ms 窗口）
   - **Tier 细分**：无需 cancel 任何挂单（旧挂单自动合规）
   - **首次上 Tiers（从兼容模式切强校验）**：扫 orderbook，标记 `Remaining mod NewStepSize != 0` 的挂单，走正常 cancel event 回 Counter（走 ADR-0041 reservation 解冻路径）
   - 原子 swap `PrecisionVersion` 并 resume
4. 事件带 `precision_version` → Counter 对账能识别切换前后的订单

### 7. 观测与对账

- **不变式守护** daily job：扫 orderbook，任何 `Remaining mod AdmissionStepSize != 0` 告警
- **冻结对账** daily job：`Σ(Counter 冻结) == Σ(orderbook 挂单金额) + Σ(in-flight trade-event 未结算)`，epsilon = `max(stepSize * price) * orderCount`
- 指标：
  - `match_reject_total{reason=price_tick|lot_size|min_quote_amount|min_qty|precision_mismatch}`
  - `match_orderbook_depth_by_tier{symbol, tier_index}`（识别分档是否合理）
  - `match_quote_unused_total{symbol}`（market buy 残余累计）

### 8. 迁移策略

| 阶段 | 动作 | 线上影响 |
|---|---|---|
| **M0** | 本 ADR merge，SymbolConfig 加字段，全部零值 | 零（兼容路径） |
| **M1** | admin-gateway 加精度 CRUD + 精度推荐工具（`opentrade-cli precision recommend`） | 零 |
| **M2** | 单档 Tier 回填全量 symbol（BTC-USDT: `tick=0.01 step=0.00001 minQuoteAmount=5`） | 校验未启用，零 |
| **M3** | symbol 级 flag `StrictPrecision=true` 灰度切强校验，一次一个 symbol | 不合规挂单被 cancel（极少数历史订单） |
| **M4** | 按需拆档（MEME 币 / 高波动币），通过 `PUT /precision` 走 rollout 协议 | 按协议，无全局 cancel |

## 备选方案 (Alternatives Considered)

### A. 纯 Binance schema —— 拒绝

- 优点：字段最完整（含 `PERCENT_PRICE_BY_SIDE` / `ICEBERG_PARTS` 等）
- 缺点：没有 `quotePrecision`，quote-driven 市价买没法干净落地；需要在业务层拼"先按 price 算 qty 再落 LOT_SIZE"的多步校验

### B. 纯 OKX schema —— 拒绝

- 优点：字段最少，好理解
- 缺点：缺 `MinQuoteAmount` → 低价币 dust 风险显著；缺 quote 精度分离 → 同 A

### C. 定长 tick（单档 Tier，不支持拆档） —— 推迟（Phase 1 采纳）

实际上本 ADR 的 Phase 1（M2）就是单档。**预留多档接口**但 Phase 1 不实现拆档逻辑，等真遇到 MEME 币再开 Phase 2（M4）。这不是拒绝而是分期。

### D. Significant Figures（"保留 N 位有效数字"） —— 拒绝

- 优点：自动适应价格尺度，单一参数
- 缺点：撮合引擎的 price-level 索引基于 Decimal 相等性，有效数字语义下 `1.2345 == 1.23450` 但 string 不同，索引 key 需要规范化；对 `match/internal/orderbook/book.go` 侵入极大；没有成熟现货交易所样本（dYdX / Hyperliquid 是衍生品，索引价撮合，不是 orderbook）

### E. 整数化价格（sats/fixed-point） —— 拒绝

- 优点：完全消除 Decimal 除法不精确问题，撮合性能最优
- 缺点：对现有 `pkg/dec` + `shopspring/decimal` 全链路重写；序列化协议（proto / JSON）要重新设计；Kafka 历史消息不兼容；ROI 不对等

### F. 撮合引擎内部做 dust 阈值回退（"Remaining < threshold 视为 filled"） —— 拒绝

- 优点：改动极小
- 缺点：阈值是魔数，不同 symbol 合适值不同；用户看到"部分成交但订单被关闭"会疑惑；**根本问题在入场**——脏订单进来了就是问题，该在入口拦掉而不是事后修补

### G. Binance 的 `applyToMarket=false` 默认 —— 采纳但可配置

本 ADR 默认 `EnforceMinQuoteAmountOnMarket=true`（对齐 BN `applyToMarket` 默认 + 更严格），但字段暴露给 admin 可配。极端情况下可关闭（如极小额限额的 symbol）。

## 理由 (Rationale)

**Bybit quote/base 精度分离**是 OpenTrade 最稀缺的能力，ADR-0035 已经把系统推到"quote-driven 市价买是一等公民"的位置，继续用 BN 的"只有 LOT_SIZE"模型会让 Counter / Match 两侧都在业务代码里补 quote 精度逻辑，不如从 schema 层一次做对。

**分档演进**是现货市场对 MEME 币场景的工程妥协。BN / OKX 的实际做法是"初期定保守 tick + 涨到上限发公告 cancel all + 跌穿了就下架"，这是成熟但对用户不友好的方案。OpenTrade 可以做得更好：单调分档 + 入场 tier 冻结，换来"涨跌往返零 cancel"的体验优势。代价是 Order 结构多 3 个字段（`AdmissionTickSize / AdmissionStepSize / AdmissionQuoteStepSize`）和撮合时双档位 LCM 推理，工程成本可控。

**三层校验**延续 OpenTrade 的防御性风格（Counter/Match 重复校验是 ADR-0015 幂等 + ADR-0018 sequencer 的既定模式），不新增概念。

**零值 = 兼容模式**让本 ADR 可以 M0 零风险 merge，后续分 symbol 逐步切换，符合 ADR-0030 / ADR-0027 的渐进 rollout 哲学。

## 影响 (Consequences)

### 正面

- 消除 dust 挂单与对账漂移（不变式保证）
- Quote-driven 市价买的残余明确退回，用户不再默认吃 1e-14 级损失
- MEME 币大涨大跌场景原生支持，不靠公告 cancel
- 为后续风控（`PERCENT_PRICE_BY_SIDE` / 价格偏离保护）铺路 —— schema 扩展即可
- 撮合引擎代码中闲置的 `RejectInvalidPriceTick / RejectInvalidLotSize` 终于启用

### 负面 / 代价

- `SymbolConfig` 字段量翻倍（从 3 个到 15+），etcd value 从几十字节涨到 KB 级
- Order 结构新增 `Admission*` 三字段，持久化到 Kafka journal → snapshot 大小增加 ~24 bytes/order
- 撮合引擎的校验路径增加 ~5 次 Decimal 比较，per-order latency 增加 ~微秒级（已用 bench 预估，可接受）
- admin 运维心智负担：每 symbol 要思考 tick / step / minQuoteAmount 的取值
- M3 切换时有少量历史挂单被 cancel（需提前公告）

### 中性

- 不改变 ADR-0048 snapshot/offset 原子性
- 不改变 ADR-0022 push sharding
- quote 服务需要稳定提供 `mid-price` / `avgPrice(mins)` 查询 —— 目前是有的（trade-event 驱动 ticker），本 ADR 不新增 quote 数据源

## 实施约束 (Implementation Notes)

### 新增 / 修改文件

```
pkg/etcdcfg/
  etcdcfg.go                       # SymbolConfig 扩展 + PrecisionTier / PrecisionChange
  precision.go                     # floorToStep / roundToTick / validateOrderAgainstTier
  precision_test.go

counter/engine/
  validate.go                      # 下单前校验（新文件）
  freeze.go                        # 冻结按 QuoteStepSize 向上取整
  trade_event_consumer.go          # 处理 UnusedQuote 解冻

match/internal/
  orderbook/types.go               # 新增 Reject* 常量
  orderbook/order.go               # Order 结构加 Admission* 三字段
  engine/engine.go:127             # quote-driven 市价买改 floorToStep
  engine/engine.go:179             # 完成判定 + 不变式断言
  engine/validate.go               # 防御性校验（新文件）

admin-gateway/internal/server/
  server.go                        # PUT/DELETE /admin/symbols/{symbol}/precision
  precision.go                     # PrecisionChange 语义校验 + rollout 调度

api/rpc/counter/counter.proto      # TakerResult 新增 unused_quote
api/rpc/match/match.proto          # 同步

tools/opentrade-cli/
  precision_recommend.go           # 基于 N 日成交价推荐 tier 配置（人工审核）
```

### 测试覆盖

- `pkg/etcdcfg/precision_test.go`：floorToStep / roundToTick 边界（零值、负数、极大 decimal）
- `pkg/etcdcfg`：Tier 单调性校验（拒绝合并 / 删除 / 乱序）
- `counter/engine/validate_test.go`：六种 reject reason 全覆盖 + 兼容模式（零值 Tier 跳过校验）
- `counter/engine/freeze_test.go`：QuoteStepSize 向上取整的金额一致性
- `match/internal/engine`：
  - `admission_tier_assignment_test.go`：mid-price 落档
  - `dust_invariant_test.go`：随机化 fuzz 1000 轮，断言 orderbook `Remaining mod Step == 0`
  - `quote_buy_unused_test.go`：UnusedQuote 字段在不同残余场景下正确
  - `tier_split_no_cancel_test.go`：tier 从 `[0,+Inf)` 拆成 `[0,100)+[100,+Inf)`，旧挂单不被 cancel
- `admin-gateway/internal/server/precision_test.go`：rollout 协议（schedule / cancel-schedule / effective-at 触发）
- `tools/opentrade-cli/precision_recommend_test.go`：推荐值合理性（基于历史成交价）

### 不变式断言（生产环境开启）

```go
// match/internal/engine/engine.go 完成判定
if !order.Remaining.Mod(order.AdmissionStepSize).IsZero() {
    panic(fmt.Sprintf("dust invariant violated: order=%s remaining=%s step=%s",
        order.ID, order.Remaining, order.AdmissionStepSize))
}
```

panic 走 sentry + kill-switch → HA 切备（ADR-0031），不允许 silent 继续。

### admin 操作审计

所有 `/admin/symbols/.../precision` 调用走 `pkg/adminaudit`（ADR-0052），`op="admin.precision.schedule" / "admin.precision.cancel_schedule"`。

### 分期落地 bug tracking（配合 feedback_bug_tracking 规范）

本 ADR 拆成独立 commit，每个 commit 带 bug tracking 条目：
- `precision/M0-schema`：SymbolConfig 扩展，零值兼容
- `precision/M1-admin`：admin-gateway 精度 CRUD + 推荐工具
- `precision/M2-backfill`：全量 symbol 填单档 Tier（运维脚本）
- `precision/M3-strict-validation`：Counter + Match 打开校验
- `precision/M4-tier-split`：拆档 rollout 协议

## 参考 (References)

- [Binance Spot Filters 官方文档](https://developers.binance.com/docs/binance-spot-api-docs/filters)
- [OKX API v5 Get Instruments](https://www.okx.com/docs-v5/en/#public-data-rest-api-get-instruments)
- [Bybit V5 Get Instruments Info](https://bybit-exchange.github.io/docs/v5/market/instrument)
- [OKX precision adjustment 公告流程](https://www.okx.com/en-us/help/okx-to-adjust-tick-size-and-trading-amount-precision-for-some-spot-trading-pairs)
- ADR-0030 Match etcd sharding — symbol 配置源
- ADR-0035 市价单服务端原生 — quote-driven market buy 基础
- ADR-0041 Counter reservations — 冻结/解冻流水
- ADR-0048 Snapshot/offset 原子性 — 精度 rollout 不破坏
- ADR-0052 Admin console — 精度 CRUD 入口
- 讨论来源：2026-04-19 会话（Claude + xargin），山寨币往返场景由 xargin 提出
