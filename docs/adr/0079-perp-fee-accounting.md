# ADR-0079: perp 手续费与财务记账

- 状态: **Proposed**（2026-05-31 起草；从 `docs/roadmap.md` 的 perp 产品化缺口 #6 提升为独立 ADR）
- 日期: 2026-05-31
- 决策者: xargin, Codex
- 相关 ADR: 0057（asset-service + transfer saga）、0068（perp settlement）、0073（RiskPool settlement）、0075（SymbolConfig 产品化）

## 范围声明（先读这一段）

本 ADR 定义 perp 的费用和财务流水模型：

- maker/taker 费率
- 用户 / 币种 / symbol 覆盖
- 负 maker rebate
- fee rule id
- 平台手续费账户入账
- 平仓手续费预占
- 累计交易手续费 / 资金费统计

本 ADR 不处理税务、发票、返佣分润、多级代理。

## 背景 (Context)

当前 perp settlement 中 fee 只是一个简单字段或占位。生产合约系统需要：

- 费率按用户等级、symbol、maker/taker、活动覆盖。
- maker 可能是负费率，即平台给 rebate。
- 费用需要进入平台账户，不能只从用户余额里减掉。
- 平仓 / 强平需要预估手续费，否则用户看似有足够保证金但结算时被 fee 打穿。
- history 要能回答用户累计手续费、资金费、已实现盈亏。

## 决策 (Decision)

### 1. 引入 FeeRule 和 FeeEngine

```text
FeeRule {
  fee_rule_id
  product_type        // perp
  symbol optional
  settle_asset optional
  user_id/group optional
  maker_rate
  taker_rate
  effective_from_ms
  effective_to_ms
  priority
}
```

撮合回流 settlement 时调用：

```go
ResolveFee(user, symbol, liquidityRole, ts, configVersion) -> FeeQuote
```

`FeeQuote` 必须包含 `fee_rule_id`、rate、fee_asset、signed_fee_amount。正数表示用户付费，负数表示用户获得 rebate。

### 2. 手续费进入独立平台账户

资金方向：

```text
positive fee:
  user futures wallet/position margin -> platform_fee_account

negative maker rebate:
  platform_rebate_account -> user futures wallet/position margin
```

平台账户是系统账户，属于 asset-service / perp-counter 可审计账本的一部分。不能只在用户侧扣费而没有对手账。

### 3. fee journal 与 settlement journal 分层

settlement 事件记录成交和仓位变化；fee event 记录资金归属：

```text
PerpFeeEvent {
  user_id
  symbol
  trade_id
  order_id
  liquidity_role
  fee_rule_id
  fee_rate
  fee_asset
  fee_amount_signed
  platform_account
  wallet_after
}
```

可以和 settlement 在同一 `PerpJournalEvent` 中原子发出，但 projection 必须能把 fee 独立落账。

### 4. 平仓手续费预占

对开仓 / 增仓订单：

```text
reserved = initial_margin + open_fee_buffer
```

对 reduce-only / close-all / liquidation：

```text
reserved = close_fee_buffer
```

buffer 使用 `max_taker_fee_rate` 或 symbol/customer 配置的保守值。真实成交后多退少补；少补不能让仓位结算失败，但也不能把用户钱包扣成负余额。路由规则：

- 普通平仓 fee 超过 buffer：先扣可用余额，再扣该仓位可用权益；不足部分进入 `fee_deficit`，由风险流程处理，不能产生负 wallet。
- 强平 / 接管路径 fee 超过 buffer：并入 [ADR-0070](./0070-perp-liquidation-hardening.md) / [ADR-0073](./0073-perp-takeover-inventory-and-riskpool-settlement.md) 的 deficit / RiskPool settlement，不从用户钱包透支。
- deficit 必须 journal，history 可查。

这与 [ADR-0076](./0076-perp-contract-product-expansion.md) 的“不做负余额”边界一致。

### 5. 负 maker rebate 依赖 STP 和反套利保护

负 maker rebate 上线前必须满足：

- Match 已启用 STP（self-trade prevention，自成交防护），同一用户、同一 beneficial owner、同一客户组的自成交不能拿 maker rebate。
- 若发生允许成交的内部交叉（如 admin block trade / forced transfer），fee engine 必须能标记 `rebate_eligible=false`。
- fee rule 支持 `rebate_cap` 和 per-user/day rebate limit，异常自成交或 wash trade 命中风控后可降级为 0 rebate。

否则当 `abs(maker_rebate) > taker_fee` 时，用户可以自成交刷平台返佣。

### 6. 统计由 trade-dump / history projection 产生

不在 perp-counter 内维护长期累计统计权威。trade-dump 投影：

- `perp_fee_logs`
- `perp_user_fee_stats_daily`
- `perp_funding_stats_daily`
- `perp_realized_pnl_stats_daily`

perp-counter 只保证事件完整、顺序、幂等。

## 备选方案 (Alternatives Considered)

### A. fee 只作为 settlement 字段，不单独入账

实现简单，但平台收入、rebate、审计都不完整。否决。

### B. fee rule 在 BFF 计算

BFF 不在成交路径上，无法按真实 maker/taker 和成交价格计算。否决。

### C. 负 maker rebate 先不支持

能简化资金方向，但会阻塞做市商产品。决定从数据模型上支持，产品可先配置为 0。

## 影响 (Consequences)

### 正面

- 手续费可审计、可配置、可复盘。
- maker rebate 和平台账户对账有明确资金流。
- 用户统计不污染高频 engine。

### 负面 / 代价

- settlement 路径要解析 fee rule。
- 需要平台系统账户和 projection 表。
- fee buffer 会增加下单预占，用户可用余额展示更复杂。

## 实施约束 (Implementation Notes)

- 每笔 settlement 必须记录 `fee_rule_id`，不能只记录 rate。
- fee rule 变更要 versioned publish，历史不重算。
- negative fee 必须检查平台 rebate account 配额；不足时 fail-closed 或降级为 0 要由产品配置决定。
- negative rebate 生效前必须验证 STP / beneficial-owner 规则；自成交不发 rebate。
- fee 超 buffer 不得产生负 wallet，必须进入 deficit / RiskPool 路径。
- 单测覆盖 maker/taker、override priority、negative rebate、自成交无 rebate、fee buffer refund、fee deficit、projection 幂等。

## 参考 (References)

- [ADR-0068: USDT 本位线性永续合约](./0068-usdt-linear-perp.md)
- [ADR-0075: perp 合约 SymbolConfig 产品化](./0075-perp-symbol-config-productization.md)
