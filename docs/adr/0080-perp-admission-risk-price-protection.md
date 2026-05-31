# ADR-0080: perp 订单准入风控与价格保护

- 状态: **Proposed**（2026-05-31 起草；从 `docs/roadmap.md` 的 perp 产品化缺口 #7 提升为独立 ADR）
- 日期: 2026-05-31
- 决策者: xargin, Codex
- 相关 ADR: 0035（MARKET 服务端原生）、0053（symbol 精度）、0068（perp 保证金闸门）、0070（risk tier）、0075（SymbolConfig 产品化）、0083（Match 原生 protected market）

## 范围声明（先读这一段）

本 ADR 定义 perp 订单准入风控和价格保护，包括：

- symbol 级 min/max price
- last/index/mark price band
- 动态 buy/sell collar
- 异常价格拒绝或自动修正
- min notional / max qty
- open interest / position value 限制
- 客户可交易 symbol 白名单

ADR-0083 专门处理 Match 内基于 orderbook 的 protected market order；本 ADR 是更广义的准入管线。

## 背景 (Context)

perp 已有保证金预占，但成熟准入还需要防止：

- 用户用离谱限价污染盘口。
- 市价单在盘口稀薄时吃到极端价格。
- 单用户或全市场 open interest 过大。
- 未授权客户交易高风险 symbol。
- 小额订单造成 dust 和 projection 噪声。

这些规则有些属于账户服务，有些必须在 Match worker 内基于最新 orderbook 判断。边界必须清晰。

## 决策 (Decision)

### 1. 准入分两层：perp-counter 风控 + Match 盘口保护

```text
BFF
 |
 v
perp-counter admission
 |  - auth / whitelist
 |  - symbol status
 |  - precision / min notional / max qty
 |  - margin / leverage / position value / OI snapshot
 |
 v
Match admission
 |  - current book collar
 |  - post-only crossing
 |  - price band against worker-local reference
 |
 v
orderbook
```

Counter 不订阅 orderbook；Match 不读取账户。两个服务各自 fail-closed。

### 2. SymbolConfig 定义静态和动态保护参数

```text
order_limits:
  min_price
  max_price
  tick_size
  min_qty
  max_order_qty
  min_notional
  max_position_value
  max_open_interest

price_protection:
  limit_price_band_bps
  market_slippage_bps_default
  buy_collar_bps
  sell_collar_bps
  counter_reference_price_source  // mark/index only
  match_reference_price_source    // best_bid/best_ask/book_mid/last
  abnormal_price_action   // REJECT / CLAMP
```

P1 默认选择 `REJECT`，不自动改用户价格。`CLAMP` 只允许在明确产品 API 中开启，因为它会改变用户指令。

Counter 只能执行 `mark` / `index` 这类它本地可获得的参考价规则；`book_mid`、`best_bid/best_ask`、`last` 属于 Match worker 或行情投影侧规则，不能配置成 Counter 权威规则。配置发布时必须校验执行层，避免配出 Counter 无法计算的 price band。

### 3. 客户可交易 symbol 白名单

```text
TradabilityRule {
  user_id/group
  symbol optional
  product_type
  allow_open
  allow_close
  max_leverage optional
  reason
}
```

规则：

- `allow_open=false, allow_close=true` 用于风控降级，只允许减仓。
- 白名单在 perp-counter 准入执行，BFF 只做展示优化。
- admin 变更必须审计。

### 4. open interest 与 position value 分开

- `position_value_limit`：单用户 / 单 symbol 限制，perp-counter 本地可算。
- `open_interest_limit`：全市场限制，需要全局聚合。

P1 对 OI 使用 eventually consistent 聚合，因此 `max_open_interest` 在 P1 是软上限 / 准入保护，不是严格全局不变量：

```text
perp-journal -> trade-dump/perp-risk OI projection -> config/risk cache -> perp-counter admission
```

当缓存过期超过阈值时，open-increasing orders fail-closed，reduce-only 仍允许。

若产品要求硬 OI cap，必须新增中心化 OI reservation / allocator：

```text
PlaceOrder(open-increasing)
    -> reserve OI delta in OI allocator
    -> reserve margin in perp-counter
    -> dispatch to Match
    -> release unused OI on cancel/expire/partial fill
```

没有 allocator 前，文档和 API 都不能承诺 OI 永不超限。

### 5. 价格 band 的参考价必须带版本 / 时间戳

准入拒绝事件记录：

```text
reference_price
reference_source
reference_ts
band_bps
reject_reason
symbol_config_version
```

否则用户无法理解为何某个限价被拒。

## 备选方案 (Alternatives Considered)

### A. 所有价格保护都在 BFF 做

BFF 看到的是缓存行情，不是撮合瞬间盘口。只能做 UX 提示，不能作为权威。否决。

### B. Counter 订阅 orderbook 做完整保护

会把账户服务变成行情消费者，且存在 stale book。否决。

### C. Match 读取用户仓位做所有风控

破坏账户权威边界。否决。

## 影响 (Consequences)

### 正面

- 准入规则可配置、可审计。
- Counter/Match 边界清晰。
- reduce-only 在风险降级时仍可通行。

### 负面 / 代价

- 需要 OI projection 和过期策略。
- 用户 reject reason 增多，BFF 要清晰展示。
- 部分规则需要 Counter 和 Match 都校验，测试矩阵变大。

## 实施约束 (Implementation Notes)

- 所有 reject reason 使用稳定枚举，不只写字符串。
- open-increasing / reduce-only / close-only 必须在准入层明确区分。
- price band 单测覆盖 mark/index/last 参考价缺失、过期、异常。
- OI 缓存过期时只拒绝增仓，不阻塞减仓。
- 配置校验必须拒绝 Counter 层使用 book-only reference price。
- P1 `max_open_interest` 文案标记为 soft cap；hard cap 另依赖 OI allocator。

## 参考 (References)

- [ADR-0035: MARKET 单服务端原生支持](./0035-market-orders-native-server-side.md)
- [ADR-0083: Match 原生 protected market order](./0083-match-native-protected-market-order.md)
