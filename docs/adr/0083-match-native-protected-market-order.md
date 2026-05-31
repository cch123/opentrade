# ADR-0083: Match 原生 protected market order

- 状态: **Proposed**（2026-05-31 起草；从 `docs/roadmap.md` 的 Match 原生滑点保护待办提升为独立 ADR）
- 日期: 2026-05-31
- 决策者: xargin, Codex
- 相关 ADR: 0035（MARKET 单服务端原生支持）、0041（Counter reservations）、0055（Match 作为 orderbook 权威）、0080（订单准入风控与价格保护）

## 范围声明（先读这一段）

ADR-0035 的路径 B 由 BFF 用 `last_price + slippage_bps` 翻译成 `LIMIT IOC`。它只能保证不超过客户端/BFF 看到的保护价，不能保证相对 Match 撮合时刻的最新盘口。

本 ADR 决定在 Match symbol worker 内实现 native protected market order：保护价由 Match 基于当前 orderbook 计算。

## 背景 (Context)

真正的滑点保护需要使用撮合瞬间的盘口：

```text
client/BFF last price  -- stale possible
Match orderbook mid    -- current inside symbol worker
```

但 Counter 负责资金冻结，不能订阅盘口。因此协议必须给 Counter 一个冻结上限，同时让 Match 在上限内按最新盘口执行。

## 决策 (Decision)

### 1. 新增 order protection fields

```text
OrderProtection {
  protection_type      // NONE / BOOK_BPS / EXPLICIT_PRICE
  slippage_bps
  explicit_limit_price optional
  quote_cap optional
}
```

语义：

- `BOOK_BPS`：Match 按订单方向使用当前 best ask / best bid 计算 collar。
- `EXPLICIT_PRICE`：用户指定绝对保护价，等价 protected IOC limit。
- `quote_cap`：Counter 可冻结的最大 quote/notional 上限。

### 2. Counter 按 quote_cap / worst-case notional 预占

Counter 不知道盘口，只按用户承诺的上限冻结：

```text
spot buy protected market:
  reserve quote <= quote_cap

spot sell protected market:
  reserve base qty

perp protected market:
  reserve IM + fee buffer by qty and conservative mark / quote_cap
```

若用户不提供 `quote_cap`，BFF 可根据 UI 滑点参数给出；底层 API 第一版要求显式提供，避免无限冻结。

### 3. Match 在 symbol worker 内计算 collar 并执行 IOC

```text
on protected market order:
    buy:  ref = current best ask; limit = ref * (1 + slippage_bps)
    sell: ref = current best bid; limit = ref * (1 - slippage_bps)
    execute as IOC limit
    expire unfilled remainder
```

若对应侧盘口为空或 ref 不可用，reject `no_book_reference`。`book_mid` 只能作为展示或显式配置的次级参考，不作为 P1 默认 collar 基准。

### 4. settlement 释放未用 reservation

交易回流后 Counter 按实际成交：

- 消耗实际 notional / IM。
- 释放未成交或价格优于上限节省的 reservation。
- 对 market-buy-by-quote，若 quote_cap 未用完，剩余直接释放。

### 5. BFF 旧翻译路径保留为兼容 fallback

BFF 可继续提供旧模式，但对外标注：

- `client_protected`：保护价由 BFF/客户端计算。
- `match_protected`：保护价由 Match 当前盘口计算。

新产品默认使用 `match_protected`。

## 备选方案 (Alternatives Considered)

### A. 继续只用 BFF 翻译

实现已存在，但参考价 stale。否决作为最终形态。

### B. Counter 订阅 orderbook 后计算保护价

Counter 看到的 book 仍可能落后 Match，且破坏账户服务边界。否决。

### C. Match 直接决定冻结金额

Match 不管理资金，无法冻结。否决。

## 影响 (Consequences)

### 正面

- 滑点保护基于撮合时刻盘口。
- Counter 仍不依赖行情。
- 未成交剩余自然 IOC expire。

### 负面 / 代价

- order wire 需要新增 protection fields。
- Counter reservation 要支持 quote_cap 上限和未用释放。
- 用户必须理解 quote_cap 与 slippage_bps 的关系。

## 实施约束 (Implementation Notes)

- Match 必须在同一个 symbol worker tick 内读取 book 和执行，不能异步计算 collar。
- book 为空 fail-closed。
- 保护价和参考价写入 TradeEvent / OrderExpired reason，方便审计。
- 单测覆盖 buy uses best ask、sell uses best bid、空 book、partial fill expire、quote_cap refund、spot/perp reservation 分离、BFF fallback。

## 参考 (References)

- [ADR-0035: MARKET 单服务端原生支持](./0035-market-orders-native-server-side.md)
- [ADR-0080: perp 订单准入风控与价格保护](./0080-perp-admission-risk-price-protection.md)
