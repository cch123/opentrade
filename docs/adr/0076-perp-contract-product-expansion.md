# ADR-0076: perp 合约品类扩展 —— linear dated futures / settlement，inverse 延后

- 状态: **Proposed**（2026-05-31 起草；从 `docs/roadmap.md` 的 perp 产品化缺口 #2 提升为独立 ADR）
- 日期: 2026-05-31
- 决策者: xargin, Codex
- 相关 ADR: 0068（USDT 线性 perp）、0069（指数价）、0073（RiskPool settlement）、0075（perp SymbolConfig 产品化）

## 范围声明（先读这一段）

当前系统只支持 USDT 本位线性永续。这个 ADR 定义下一阶段如何扩展到 **线性交割合约** 和交割结算流程，同时明确 **inverse 币本位 perp / futures 先不支持**，只在数据模型上预留 future extension。

近期支持范围：

1. **linear perpetual**：现有 USDT 本位线性永续继续作为基线。
2. **linear dated futures**：有到期日的 USDT / 稳定币本位线性交割合约。
3. **settlement flow**：到期结算、预期结算价、最终结算价。
4. **多稳定币结算路径预留**：USDT 之外的稳定币 settle asset 可以复用同一线性模型，但不在本 ADR 强制实现。

明确不做：

- inverse perpetual / inverse dated futures（币本位、反向合约）。
- 非稳定币抵押共享、跨币种 haircut、借贷和负余额。
- 期权和 portfolio margin 公式。

## 背景 (Context)

ADR-0068 的代数是线性 USDT：

```text
pnl = (exit_price - entry_price) * qty
margin_asset = USDT
```

dated futures 与永续的主要差别不是成交代数，而是**合约生命周期和到期结算**。如果把交割流程用临时 job 直接改仓，会污染仓位、history 和 trade-dump 的事件源语义。

inverse 币本位合约的 PnL 和保证金计价都不同，确实需要 `ContractMath` 扩展点；但它会同时引入 coin-settled wallet、非 USDT 风险池、资金费/强平币种差异。近期没有必要把这些复杂度拉进第一版合约品类扩展。

## 决策 (Decision)

### 1. 抽象 `ContractSpec` 和 `ContractMath`

每个 symbol 从 ADR-0075 catalog 读取：

```text
ContractSpec {
  symbol
  contract_type       // LINEAR_PERP / LINEAR_FUTURE; inverse values reserved for future
  base_asset
  quote_asset
  settle_asset
  contract_size
  multiplier
  expiry_time_ms optional
  settlement_price_source optional
}
```

合约代数收敛到接口：

```go
type ContractMath interface {
    Notional(position, mark) dec.Decimal
    UnrealizedPnL(position, mark) dec.Decimal
    RealizedPnL(entry, exit, qty, side) dec.Decimal
    InitialMargin(notional, leverage) dec.Decimal
    BankruptcyPrice(position) dec.Decimal
}
```

现有线性 USDT 是第一个实现；linear dated futures 复用同一个 `LinearContractMath`，只增加 expiry / settlement 状态机。inverse values 可以先出现在 enum reserved / catalog validation allowlist 之外，不对外启用。

### 2. 交割合约增加独立生命周期

交割合约复用 [ADR-0075](./0075-perp-symbol-config-productization.md) 的共享 symbol 状态 enum。dated futures 的常规状态机：

```text
PREOPEN -> TRADING -> PRE_DELIVERY -> SETTLING -> DELIVERED -> DELISTED
              |             |
              |             +-> SETTLING_HALTED -> SETTLING
              +-- normal trading
```

交割流程：

```text
expiry reached
    |
    v
publish PRE_DELIVERY / stop new open orders
    |
    v
quiesce each user+symbol:
  - reject new open-increasing orders
  - dispatch cancel for open orders
  - keep settlement_round_id guard
  - keep close/zeroing reservation for in-flight fills
    |
    v
load final settlement price
    |
    v
settle each user in its sequencer with (user, settlement_round_id) idempotency
    |
    v
emit settlement journal + mark delivered when all users done
```

结算必须进入每个 owning user 的 sequencer，不能由全局 settle job 直接批量改仓位。`settlement_round_id = symbol:expiry_time` 是全局 round id，但幂等边界必须是 `(user_id, settlement_round_id)`，否则单用户 replay 会重复结算。

交割不能假设“cancel open orders → positions settled”之间没有在途成交。cancel 是发往 Match 的异步命令，旧订单 fill 可能在 cancel terminal 前回流。交割 quiesce 必须复用 [ADR-0081](./0081-perp-reduce-only-settlement-hardening.md) 的思路：在用户 sequencer 内保留 in-flight guard，交割后若旧 fill 到达，只能按交割前保留的 reservation/position version 收口；超过 guard 的 fill 进入 invariant breach 告警，不能静默重开已交割仓位。

### 3. 预期结算价和最终结算价分开

- `estimated_settlement_price`：到期前展示和风控参考，可变。
- `final_settlement_price`：到期后锁定，进入 settlement journal，不可变。

最终价必须带 `price_source_version`。若价格源不可用，symbol 进入 `SETTLING_HALTED`，等待 admin 发布最终价或恢复数据源。

进入 `PRE_DELIVERY` 后停止产生新的 funding round；已经生成但尚未结算的 funding round 必须在最终 settlement 前按 `(user, funding_round_id)` 幂等结清，或者由 admin policy 明确取消并写入 journal。

### 4. 多结算资产只先支持稳定币线性路径

P1 只支持每个 symbol 一个 `settle_asset`。账户层面仍按 settle asset 分 pool：

```text
USDT linear pool: settle_asset=USDT
USDC linear pool: settle_asset=USDC   // future, same linear math
```

不在本 ADR 做跨币种抵押共享；那属于 unified margin / collateral haircut。币本位 inverse 即使未来接入，也必须先补对应 settle asset wallet、risk pool 和 fee/funding 账务，不能只靠 `ContractMath` 开关打开。

## 备选方案 (Alternatives Considered)

### A. 每种合约一个服务

隔离性强，但撮合、订单、history、push、risk 都会重复。否决。

### B. 在现有线性代码里加 if/else

短期快，但到期结算、最终结算价、交易状态和后续 inverse 预留会散落到调用点。否决。

### C. 立即支持 inverse 币本位 perp

产品覆盖更完整，但需要 coin-settled wallet、币本位 RiskPool、币种化手续费/资金费、反向合约强平公式一起到位。近期收益不足以抵消复杂度。否决，先只预留接口。

## 影响 (Consequences)

### 正面

- 线性交割合约通过 `ContractSpec` 的 expiry / settlement 字段接入，核心服务拓扑不变。
- 到期交割有明确状态机和 journal，可复盘。
- inverse 币本位被明确延后，近期实现范围更窄。

### 负面 / 代价

- `pkg/perpstate` 需要把合约生命周期和 settlement round 显式化。
- liquidation、risk tier、fee、history 都必须携带 contract type / settle asset。
- dated futures 需要 expiry scheduler 和 settlement price 发布流程。

## 实施约束 (Implementation Notes)

- 先把现有 linear USDT 永续改成 `ContractSpec + LinearContractMath` 形态，不改变行为。
- 所有 journal 事件补 `contract_type`、`settle_asset`、`symbol_config_version`。
- dated futures 的 settle job 必须幂等：全局 `settlement_round_id = symbol:expiry_time`，用户级 guard 为 `(user_id, settlement_round_id)`.
- 交割时先进入 quiesce，cancel open orders 并保留 in-flight guard，再 settle positions，避免 settlement 后旧订单成交重开仓位。
- catalog validation 必须拒绝 inverse contract type，直到后续 ADR 明确打开。
- 单测覆盖线性等价、到期状态机、settlement replay、in-flight fill after quiesce、funding stop、inverse 配置被拒。

## 参考 (References)

- [ADR-0068: USDT 本位线性永续合约](./0068-usdt-linear-perp.md)
- [ADR-0075: perp 合约 SymbolConfig 产品化](./0075-perp-symbol-config-productization.md)
