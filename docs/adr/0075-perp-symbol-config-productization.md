# ADR-0075: perp 合约 SymbolConfig 产品化

- 状态: **Proposed**（2026-05-31 起草；从 `docs/roadmap.md` 的 perp 产品化缺口 #1 提升为独立 ADR）
- 日期: 2026-05-31
- 决策者: xargin, Codex
- 相关 ADR: 0056（SymbolConfig via MySQL）、0068（USDT 线性 perp）、0069（外部复合指数价）、0070（风险档 / 强平费率）、0074（账户与保证金模式）

## 范围声明（先读这一段）

当前 perp 的很多合约参数仍散在启动参数、常量或服务内配置里。这个 ADR 决定把 perp 合约规格、交易状态、精度、风控档、资金费、手续费、价格保护参数都纳入 **versioned SymbolConfig**，由 MySQL 权威存储、各服务轻量订阅 / poll。

本 ADR 不实现新合约品类；linear dated futures 以及 inverse 的预留边界见 [ADR-0076](./0076-perp-contract-product-expansion.md)。本 ADR 只先把配置模型补齐，让后续品类能被同一个 catalog 表达。

## 背景 (Context)

ADR-0056 已决定 SymbolConfig 从 etcd 迁到 MySQL，但它主要服务 spot 的 tick / lot / min quote。perp 已经引入更多 symbol 级参数：

- 合约规格：contract type、base/quote/settle coin、contract multiplier、alias。
- 状态机：preopen、trading、post_only、cancel_only、settling、delisted。
- 订单约束：min/max price、min/max qty、min notional、max order qty。
- 风控：risk tiers、MMR、max leverage、liq fee。
- 定价：index source、mark formula、funding interval/cap/clamp。
- 费用：maker/taker、rebate、fee rule id。

如果这些继续分散在各服务 flag 中，会出现三类问题：

1. **服务间配置不一致**：BFF 允许下单，perp-counter 拒绝，Match 又按另一个精度撮合。
2. **无法做状态变更**：暂停开仓、只撤单、交割结算等都需要 symbol 状态机。
3. **无法复盘**：历史成交和强平必须知道当时使用的是哪版 risk tier / fee / funding 参数。

## 决策 (Decision)

### 1. 建立 perp product catalog，SymbolConfig 按版本发布

配置拆成稳定合约规格和可变交易参数：

```text
perp_symbols
  symbol
  contract_type        // LINEAR_PERP / LINEAR_FUTURE; inverse values reserved but disabled
  base_asset
  quote_asset
  settle_asset
  contract_size
  price_scale
  qty_scale
  alias
  status
  config_version

perp_symbol_configs
  symbol
  config_version
  precision
  order_limits
  risk_tiers
  funding_params
  fee_params_ref
  price_protection
  effective_from_ms
  created_by
```

`config_version` 是跨服务一致性锚点，也是 [ADR-0053](./0053-symbol-precision-and-tiered-evolution.md) 里 `PrecisionVersion` 的泛化与替代：精度、交易状态、风控档、资金费、费用和价格保护都跟随同一个单调版本。发布协议复用 ADR-0053 的 `ScheduledChange{EffectiveAt}` 思路：配置先发布为 future version，到 `effective_from_ms` 后各服务按 symbol 原子切换。

perp-counter 在订单准入时把 `symbol_config_version` 写入 `OrderEvent`，Match 必须用该版本做交叉校验：

```text
perp-counter admission @ config_version=N
        |
        v
OrderEvent{symbol_config_version=N}
        |
        v
Match local config version?
  == N      -> continue admission
  <  N      -> reject config_version_too_new / wait for cache
  >  N      -> reject stale_order_config
  missing   -> reject unknown_symbol_config
```

这样跨服务传播窗口内不会出现 Counter 用新精度准入、Match 用旧精度撮合。BFF 可以展示旧配置，但权威准入只以 perp-counter + Match 的版本握手为准。perp-counter 在 settlement、强平 journal 中继续写入当时版本，trade-dump / history 可以按版本复盘。

### 2. 交易状态机由 SymbolConfig 驱动

状态含义由一个共享 proto enum 表达，0075/0076/Match/perp-counter 只能引用同一个状态集：

```text
PREOPEN          允许查询/订阅，不允许交易
TRADING          正常下单、撤单、撮合
POST_ONLY        只允许 post-only 新单和撤单
CANCEL_ONLY      只允许撤单，不允许新单
PRE_DELIVERY     交割合约到期前准备态，只允许减仓/撤单/系统指令
SETTLING         到期或异常结算中，只允许系统指令
SETTLING_HALTED  结算价或结算依赖不可用，冻结新交易等待 admin 处理
DELIVERED        交割完成，只保留资金/仓位结算结果
DELISTED         下线，只保留历史查询
```

合法迁移矩阵：

```text
PREOPEN -> TRADING -> POST_ONLY -> TRADING
                 |       |
                 |       +-> CANCEL_ONLY -> TRADING
                 |
                 +-> PRE_DELIVERY -> SETTLING -> DELIVERED -> DELISTED
                 |              \-> SETTLING_HALTED -> SETTLING
                 |
                 +-> CANCEL_ONLY -> SETTLING / DELISTED
```

任何跳过矩阵的迁移都必须走 admin emergency path，并写入 reason/ticket/audit。现有 `Trading bool` 是旧二态视图：`TRADING` / `POST_ONLY` 映射为可交易，其他状态映射为不可开仓；实现迁移期可以保留派生字段，但不能作为新功能权威。

准入统一走：

```text
REST/BFF -> perp-counter admission -> Match admission
                         |
                         v
                  SymbolConfig snapshot
```

Match 仍是撮合权威，但不做账户风控。它只校验与 orderbook 有关的精度、价格范围、post-only、状态；perp-counter 校验保证金、risk tier、客户权限。

### 3. 风控档和资金费参数也进配置版本

每个 perp symbol 至少包含：

```text
risk_tiers:
  - risk_id
    max_notional
    initial_margin_ratio
    maintenance_margin_ratio
    max_leverage
    liq_fee_rate

funding:
  interval_seconds
  premium_source
  interest_rate
  cap
  floor
  clamp
  settlement_delay_ms
```

风险档变更只能从新 `config_version` 生效。已有仓位按新版本重算 risk 必须有护栏：

- 收紧 MMR / 降低 max leverage / 降低 max notional 的版本默认进入 `staged` 模式，只影响新开仓和增仓。
- 若产品要让存量仓位也立即使用新档，必须发布 `risk_reprice_policy`，包含生效时间、影响 symbol、最大预估受影响账户数、是否允许批量强平。
- `risk_reprice_policy` 生效前要先跑 dry-run projection，超过阈值时 admin 发布失败。
- journal 必须记录触发重算的版本和 policy id，避免历史强平不可解释。

### 4. 配置发布采用 versioned snapshot + watcher

服务启动和周期刷新流程：

```text
+----------------+      poll/watch      +----------------+
| MySQL catalog  | -------------------> | config cache   |
+----------------+                      +----------------+
        ^                                      |
        | admin publish                        | atomic swap
        |                                      v
+----------------+                      +----------------+
| admin-gateway  |                      | services       |
| audit log      |                      | bff/counter/...|
+----------------+                      +----------------+
```

每个服务保留本地只读快照。一次刷新必须按 symbol 原子替换，不能把 risk_tiers 更新成新版本而 funding 仍是旧版本。

### 5. Admin 发布必须可审计、可回滚

Admin API：

```text
CreatePerpSymbol
UpdatePerpSymbolConfig
SetPerpSymbolStatus
RollbackPerpSymbolConfig(symbol, target_version)
ListPerpSymbolVersions
```

每次发布都写 admin audit：操作者、diff、reason、版本号、发布时间。回滚不是覆盖旧行，而是发布一个新版本，其内容复制目标旧版本。

## 备选方案 (Alternatives Considered)

### A. 继续用服务 flag

实现快，但无法热更新、无法多服务一致、无法复盘。否决。

### B. 只在 perp-counter 管配置，BFF/Match 不订阅

账户风控一致，但 Match 仍可能接受被产品状态禁止的订单，BFF 展示也会过期。否决。

### C. etcd 作为合约配置权威

etcd 适合小规模路由 / 选主配置，不适合复杂产品 catalog 和版本审计。沿用 ADR-0056 的 MySQL 权威。

## 影响 (Consequences)

### 正面

- 合约参数可热发布、可审计、可回滚。
- 下单、强平、资金费、历史复盘可以引用同一个 config version。
- 为 linear dated futures、未来 inverse、portfolio margin 预留统一 catalog。

### 负面 / 代价

- BFF、perp-counter、Match、pricing、history 都要接 config cache。
- 配置发布流程需要 admin 审计和版本 diff。
- 任何订单准入 bug 都要区分是代码问题还是配置版本问题。

## 实施约束 (Implementation Notes)

- 先实现只读 catalog + config cache，再迁移现有 flag。
- 新订单必须记录 `symbol_config_version`。
- mark/funding tick 也必须记录 funding config version。
- Match 必须拒绝未知版本或本地缓存过期超过阈值的 symbol，避免用陈旧精度撮合。
- 单测覆盖：状态机准入、版本原子替换、risk_tier 回滚、服务缓存缺失时 fail-closed。

## 参考 (References)

- [ADR-0056: Symbol 配置存储从 etcd 迁到独立 MySQL](./0056-symbol-config-via-mysql.md)
- [ADR-0068: USDT 本位线性永续合约](./0068-usdt-linear-perp.md)
- [ADR-0070: perp 强平进阶](./0070-perp-liquidation-hardening.md)
