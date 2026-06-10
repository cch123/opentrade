# ADR-0083: Match 原生 protected market order

- 状态: **Proposed**（2026-05-31 起草；2026-06-10 实施前评审修订，见文末修订记录）
- 日期: 2026-05-31
- 决策者: xargin, Codex
- 相关 ADR: 0035（MARKET 单服务端原生支持）、0041（Counter reservations）、0055（Match 作为 orderbook 权威）、0074（perp 保证金模式）、0080（订单准入风控与价格保护）

## 术语表 (Glossary)

| 本文用词 | 含义 | 行业对照 |
|---|---|---|
| protected market order | 带滑点保护的市价单：按撮合时刻盘口推导一个保护价，超出部分不成交 | Bybit v5 `slippageToleranceType=Percent` + `slippageTolerance`；BN 无对外等价物（内部有 price band） |
| slippage_bps | 滑点容忍度，basis points（基点，1 bp = 0.01%） | Bybit `slippageTolerance`（百分比形式） |
| collar | Match 由盘口参考价推导出的保护价（买单上限 / 卖单下限） | 即 protection price / price collar |
| quote_cap | 用户承诺的 quote 资产最大支出上限，Counter 按它冻结 | 无直接对应；BN `quoteOrderQty` 是"按预算买"，quote_cap 是"按数量买的支出上限" |
| effective limit | Match 实际执行用的价格上限 = min(collar, quote_cap/qty)，见决策 3 | — |
| IOC | immediate-or-cancel（立即成交否则取消）：不入簿，未成交剩余立即过期 | 同名 TIF |
| IM | initial margin（初始保证金） | 同 |

## 范围声明（先读这一段）

ADR-0035 的路径 B 由 BFF 用客户端提供的 `last_price + slippage_bps` 翻译成 `LIMIT IOC`。它只能保证不超过客户端看到的保护价，不能保证相对 Match 撮合时刻的最新盘口。

本 ADR 决定在 Match symbol worker 内实现 native protected market order：保护价由 Match 基于撮合时刻的 orderbook 计算。同时**移除** BFF 翻译路径（理由见决策 7 与备选方案 A）。

## 背景 (Context)

真正的滑点保护需要使用撮合瞬间的盘口：

```text
client/BFF last price  -- 可能 stale（行情推送与下单之间存在延迟）
Match orderbook best   -- symbol worker 内的当前盘口，权威（ADR-0055）
```

但 Counter 负责资金冻结，不订阅盘口（ADR-0035 §备选方案 Z 的边界）。因此协议必须给 Counter 一个冻结上限，同时让 Match 在该上限内按最新盘口执行，并且**实际支出不得超过冻结额**——这是本次修订补强的核心不变量。

现状代码事实（实施前核查）：

- `OrderPlaced.freeze_cap`（wire field 10）已存在：Counter 把 `FrozenAmount` 写上 wire，但 Match 从未读取。本 ADR 把它升级为有语义的字段。
- Match 的 quote-driven 撮合（`Order.QuoteQty / RemainingQuote`）与 IOC 过期路径已就绪，collar 只需作为撮合循环的价格上限注入。
- spot Counter 的 `FrozenAmount − FrozenSpent` 残余释放机制（ADR-0035）对新形态可直接复用。
- perp-counter 对 market 单已按 mark price 预占 IM；terminal 事件统一走 `releaseRemainingIM` 释放残余。

## 决策 (Decision)

### 1. 单一保护字段 `slippage_bps`，不引入 OrderProtection 消息

```text
OrderPlaced（Counter → Match wire）：
  slippage_bps uint32   // 新增。0 = 无保护；>0 = protected market，单位 bp
  freeze_cap   string   // 已有字段。protected market buy by qty 时 = quote_cap
```

约束：

- `slippage_bps > 0` 仅对 `type = MARKET` 合法；出现在 LIMIT 上直接拒单（BFF 与 Counter 双重校验）。
- 取值范围 `(0, 10000]`（最多 100%）。
- 初稿的 `protection_type`（NONE / BOOK_BPS / EXPLICIT_PRICE）整体删除：EXPLICIT_PRICE 在语义上恒等于 `LIMIT IOC`，系统已原生支持，不为同一行为保留两种 wire 编码（见备选方案 D）；删掉 EXPLICIT_PRICE 后 protection 退化为一个标量，无需消息包装。

### 2. spot 三种形态与 Counter 冻结规则

Counter 不知道盘口，只按用户承诺的上限冻结：

```text
形态                                  冻结                  备注
protected market sell + qty           base = qty            与普通 market sell 相同
protected market buy + quote_qty      quote = quote_qty     预算即上限，与 ADR-0035 相同
protected market buy + qty            quote = quote_cap     新增形态；quote_cap 必填
```

- `quote_cap` 仅在第三种形态合法（其余形态出现即拒单），由用户/客户端显式提供，底层 API 不代算——BFF 的行情缓存是 best-effort（冷启动为空），不能作为资金上限的来源。
- ADR-0035 "market buy 仅 qty 不带 quote 预算则拒单" 的规则对**无保护**市价买单维持不变；`qty + slippage_bps + quote_cap` 是唯一放行 base 数量市价买的入口。

### 3. Match 在 symbol worker 内计算 collar 并执行（资金安全不变量）

```text
on protected market order (同一 symbol worker tick 内，order 入簿撮合之前):
    buy:  ref = current best ask
    sell: ref = current best bid
    ref 不存在（对侧盘口为空）→ reject no_book_reference（fail-closed）

    buy:  collar = ref × (1 + slippage_bps / 10000)
    sell: collar = ref × (1 − slippage_bps / 10000)

    buy + qty 形态（quote_cap 冻结）:
        effective_limit = min(collar, truncate_down(quote_cap / qty))
    其余形态:
        effective_limit = collar

    以 effective_limit 作为撮合循环的价格上限执行；
    market 单不入簿，未成交剩余按既有路径 expire。
```

**不变量 INV-1（资金安全）**：taker 实际 quote 支出 `Σ pᵢ·qᵢ ≤ effective_limit × qty ≤ quote_cap`。其中 `quote_cap / qty` 必须向下截断（向零取整），保证乘回去不超过 cap。没有这一条，collar × qty 可以大于 quote_cap，结算会把冻结余额打穿（Counter 侧表现为 settlement 负余额错误 → poison message，消费者卡死）。初稿缺失此约束，是本次修订的主因。

**不变量 INV-2（确定性回放）**：collar 只由该订单在 per-symbol 输入序列中所处位置的 book 状态决定。Match 重启后按 snapshot + offset 恢复（ADR-0048/0055），重放到同一序列位置时 book 状态相同，collar 结果逐位一致。因此 collar 不写入任何需要持久化的撮合状态，只进输出事件。

- `effective_limit` 不做 tick 对齐：它只是撮合循环里的比较上限，成交价永远取 maker 价（天然 tick 对齐），不会产生非对齐价格的成交。
- `book mid` 不作为 collar 基准（只能用 best ask / best bid）：mid 在单侧深度薄时会系统性偏向另一侧，作为保护基准会放大而非限制滑点。
- FOK（fill-or-kill）与 STP（self-trade prevention，自成交防护）的预检沿用同一价格上限：带 `effective_limit` 的 market 单在 `crosses / priceAcceptable` 中按 LIMIT 同规则比价。

### 4. settlement 消耗与释放（复用 ADR-0035 机制）

交易回流后 Counter 按实际成交：

- protected market buy（两种形态）每笔 fill 消耗 `match_price × match_qty`（无价格改善退款——订单本身没有用户报价）。
- terminal（FILLED / EXPIRED / CANCELED / REJECTED）时释放 `FrozenAmount − FrozenSpent` 残余，机制与 market-buy-by-quote 完全一致。
- protected market buy + qty 形态 `Qty > 0`，订单状态机走普通 base-driven 路径（`filledAfter ≥ Qty → FILLED`），无需 quote-driven 的特殊 terminal 信号。

### 5. perp 形态与 IM 预占

perp 订单只有 qty-driven 形态，无 quote 预算单，`quote_cap` 不适用。protected market 的 IM 预占参考价在 mark price 基础上做逆向调整：

```text
protected market buy  (做多开仓):  im_price = mark × (1 + slippage_bps / 10000)
protected market sell (做空开仓):  im_price = mark
reduce_only:                        不预占（既有规则不变）
```

理由：买单成交价上限是 `best_ask × (1 + bps)`，Counter 不知道 best ask，用 `mark × (1 + bps)` 作为保守近似；卖单成交价低于 mark 时名义价值更小，mark 本身已是保守值。该参考价同样用于 leverage 上限与 risk tier notional cap 的准入检查。

**边界声明**：当盘口整体偏离 mark 时 `mark × (1 + bps)` 不是成交价的严格上界，IM 可能预占不足。这与现状（无保护 market 单按 mark 预占）的风险敞口同源且更小（多了 collar 截断）；硬性兜底仍是结算侧的保证金重算 + 强平引擎（ADR-0070），不是下单时的预占。spot 没有此豁免——spot 的 INV-1 是严格上界。

### 6. 事件与审计

- 新增 reject reason：`REJECT_REASON_NO_BOOK_REFERENCE`（对侧盘口为空，无法计算 collar）。
- `OrderExpired` 事件新增 `protect_limit` / `protect_ref` 字段（非保护单为空串）：过期是用户最需要解释"为什么没成交"的时刻，把 effective limit 与参考价随事件落盘（trade-dump 可审计）。quote-driven 买单 FILLED 时本就以 OrderExpired 作为 terminal 信号，同样携带。
- 逐笔 `Trade` 不加保护字段：collar 是 taker 订单维度的属性，per-fill 重复携带是噪音；成交价本身已在 Trade 上。
- Match 在计算 collar 时输出结构化日志（order_id / ref / collar / effective_limit），覆盖全部成交、无 OrderExpired 的场景。

### 7. BFF 原生透传，移除路径 B 翻译

- spot `POST /v1/order`：删除 `last_price` 字段与 MARKET→LIMIT IOC 改写逻辑；`slippage_bps` 改为透传，新增 `quote_cap` 透传。
- perp `POST /v1/perp/order`：新增 `slippage_bps` 透传。
- 客户端若想要"以我看到的价格为基准"的保护（原路径 B 语义），直接下 `LIMIT IOC`，价格自行计算——表达能力无损失。
- 项目未上线，按既定约定不保留兼容层，协议改干净（不引入 client_protected / match_protected 双模式标注）。

## 端到端序列（spot protected market buy + qty）

```text
 client            BFF              Counter                Kafka               Match(symbol worker)
   |                |                  |                     |                        |
   |-- market buy   |                  |                     |                        |
   |   qty=0.5      |                  |                     |                        |
   |   slippage=50bp|                  |                     |                        |
   |   quote_cap=23k|                  |                     |                        |
   |                |-- PlaceOrder --->|                     |                        |
   |                |   (透传)         |-- 校验形态/范围      |                        |
   |                |                  |-- freeze quote=23k  |                        |
   |                |                  |-- OrderPlaced ----->|                        |
   |                |                  |   slippage_bps=50   |                        |
   |                |                  |   freeze_cap=23000  |----- consume --------->|
   |                |                  |                     |     同一 tick:          |
   |                |                  |                     |     ref = best ask 45000|
   |                |                  |                     |     collar = 45225      |
   |                |                  |                     |     eff = min(45225,    |
   |                |                  |                     |          23000/0.5=46k) |
   |                |                  |                     |         = 45225         |
   |                |                  |                     |     IOC 撮合 ≤ 45225    |
   |                |                  |<==== Trade(s) ======|<== emit ===============|
   |                |                  |<== OrderExpired ====|   (剩余过期, 带         |
   |                |                  |    protect_limit    |    protect_limit/ref)   |
   |                |                  |-- 消耗 Σp·q, 释放    |                        |
   |                |                  |   23000 − Σp·q      |                        |
```

空盘口分支：`ref 不存在 → OrderRejected(no_book_reference) → Counter 全额解冻`。

## 备选方案 (Alternatives Considered)

### A. 保留 BFF 翻译路径作为兼容回退（初稿决策 5）

被否决。上线前不保留兼容层是项目既定约定；且该路径的语义（按客户端所见价格保护）用 `LIMIT IOC` 即可无损表达，保留它只是为同一语义维护第二套入口与双模式标注。

### B. Counter 订阅 orderbook 后计算保护价

Counter 看到的 book 仍可能落后 Match，且破坏账户服务"状态机 only"边界（ADR-0035 §Z）。否决。

### C. Match 直接决定冻结金额

Match 不管理资金，无法冻结；且冻结需要在订单进入撮合前完成，顺序上不成立。否决。

### D. OrderProtection 消息 + EXPLICIT_PRICE（初稿决策 1）

EXPLICIT_PRICE ≡ LIMIT IOC，为已有行为新增第二种 wire 表达，徒增协议面与测试矩阵。删除后 protection 只剩一个标量字段，消息包装失去意义。否决。

### E. Match 内对 buy+qty 形态做 qty/quote 双预算撮合循环

即撮合循环同时跟踪 `Remaining` 与 `RemainingQuote` 两个预算。与 `effective_limit = min(collar, quote_cap/qty)` 在资金安全上等价，但要把 quote-driven 与 base-driven 两条循环逻辑合流，复杂度高。min() 方案纯粹是入口处一次除法，撮合循环零改动。否决。

（注：min() 方案在 `collar > quote_cap/qty` 时保护价被 cap 收紧，可成交的价格区间比双预算方案窄——双预算方案可以在高价档少买一点而不是直接停。这是有意取舍：cap 收紧只发生在用户给的 quote_cap 撑不起 `collar × qty` 时，此时"少成交"是符合资金承诺的行为。）

## 影响 (Consequences)

### 正面

- 滑点保护基于撮合时刻盘口，消除路径 B 的 stale 参考价问题。
- Counter 仍不依赖行情；实际支出 ≤ 冻结额成为协议级不变量（INV-1）。
- spot 首次获得"按 base 数量的市价买"（qty + quote_cap 形态），补上 ADR-0035 的功能缺口。
- 未成交剩余自然走既有 IOC expire / 残余释放路径，无新状态机。

### 负面 / 代价

- order wire 新增 `slippage_bps`，`freeze_cap` 从 write-only 升级为 Match 消费的语义字段。
- Counter 冻结/结算各加一个形态分支；spot Order 需要持久化 `slippage_bps`（journal 回放用）。
- 用户必须理解 `quote_cap` 与 `slippage_bps` 的关系：cap 不足时保护价会被收紧（见备选方案 E 注）。
- perp 的 IM 预占只是保守近似而非严格上界（决策 5 边界声明）。

## 实施约束 (Implementation Notes)

- Match 必须在同一个 symbol worker tick 内读取 book 并执行，collar 不允许异步计算（TOCTOU，time-of-check-to-time-of-use，检查与使用之间状态已变的竞态）。
- 对侧盘口为空 fail-closed（reject，而非按无保护市价单执行）。
- `quote_cap / qty` 除法向下截断（INV-1）。
- `effective_limit` / `ref` 写入 OrderExpired + Match 结构化日志（决策 6）。
- 单测覆盖：buy 用 best ask / sell 用 best bid、空 book reject、partial fill 后剩余 expire、quote_cap 收紧 effective limit（INV-1 边界）、quote_cap 残余 refund、quote-driven 形态带 collar、FOK 预检尊重 collar、spot/perp 预占分离、LIMIT + slippage_bps 拒单、perp expire 释放 ReservedIM。

## 修订记录

- 2026-06-10（实施前评审）：
  1. 新增 INV-1：`effective_limit = min(collar, quote_cap/qty)`，封死"collar × qty > quote_cap 打穿冻结"的资金安全缺口（初稿未约束 Match 支出与 Counter 冻结的关系）。
  2. 删除 `OrderProtection` 消息与 `EXPLICIT_PRICE`（≡ LIMIT IOC），protection 退化为单一 `slippage_bps` 字段；`quote_cap` 复用已有 wire 字段 `freeze_cap`。
  3. 删除决策 5 的 BFF 翻译回退与 client_protected/match_protected 双模式（上线前不留兼容层）。
  4. 删除"BFF 可根据 UI 滑点参数给出 quote_cap"（BFF 行情缓存是 best-effort，不能作为资金上限来源）；quote_cap 必须显式提供。
  5. perp IM 预占规则精确化为 `mark × (1 + bps)`（buy）/ `mark`（sell），并声明其非严格上界的边界与兜底。
  6. 审计字段落点从"TradeEvent / OrderExpired reason"改为 OrderExpired 专用字段 + 新增 `no_book_reference` reject reason + Match 结构化日志。
  7. 新增 INV-2（确定性回放）、术语表、端到端序列图。

## 参考 (References)

- [ADR-0035: MARKET 单服务端原生支持](./0035-market-orders-native-server-side.md)
- [ADR-0055: Match 作为 orderbook 权威](./0055-match-as-orderbook-authority-bybit-style.md)
- [ADR-0074: perp 账户保证金模式](./0074-perp-account-margin-modes.md)
- [ADR-0080: perp 订单准入风控与价格保护](./0080-perp-admission-risk-price-protection.md)
- [Bybit v5 Place Order — slippageTolerance](https://bybit-exchange.github.io/docs/v5/order/create-order)
