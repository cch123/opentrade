# ADR-0075: perp 合约 SymbolConfig 产品化

- 状态: **Accepted / Implemented (M1-M8)**（2026-05-31 起草；2026-06-10 全部决策点落地——versioned catalog、状态机准入、config_version 握手、风控/资金费/定价/费用入版本、staged 风险档 + reprice policy dry-run、admin 发布/回滚/审计、BFF instruments；里程碑 commit 见文末"实现进度"表）
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

## 实现进度（2026-06-10）

| 里程碑 | 内容 | commit |
| --- | --- | --- |
| M1 | `pkg/perpcfg` 基础：catalog 类型、共享 proto 状态 enum、状态迁移矩阵 + 准入谓词、配置校验（含 MMR×max_leverage < 1 不变量）、TightensRisk staged 触发器、订单形状检查 + 四分支版本握手助手 | `171e2ed` |
| M2 | MySQL catalog（`perp_symbols` / `perp_symbol_configs` append-only 版本行 / `perp_catalog_version` 单行 poll 锚点，ADR-0056 模式）+ Store 接口（版本由 store 串行分配、发布与锚点同事务、回滚=发布副本）+ 服务侧 Cache（1s poll、按 symbol 原子替换、`effective_from_ms` 查询时切换、staleness fail-closed、EffectiveRiskVersion staged 解析） | `348db44` |
| M3 | wire 面：`OrderPlaced.symbol_config_version`、握手 RejectReason（too_new / stale / unknown / status_forbids）、settlement/funding/liquidation/takeover 事件版本戳 + `risk_policy_id`、Mark/FundingTick `config_version` | `4f20d38` |
| M4 | perp-counter：PlaceOrder 状态机 + 精度/限额准入（单次捕获视图，其版本戳入 OrderEvent）、unknown/stale fail-closed、CancelOrder 状态门、engine RiskResolver 接缝（per-symbol + pinned version，legacy flag 模型仅作 fallback）、journal 版本戳、状态机关闭撮合时强平直走 backstop、`--catalog-dsn` 启动 fail-closed | `265b683` |
| M5 | Match：四分支 config_version 握手（无 catalog 收到带戳订单 / 缓存超期一律 unknown_symbol_config fail-closed）、orderbook 范畴状态门（POST_ONLY/CANCEL_ONLY/…；PRE_DELIVERY 放行——reduce-only 是 counter 账户面职责，由版本相等性钉住）、同版本 tick/lot/价格界复检；撤单在 Match 侧不做状态门（见下"实现决策"） | `3d0035e` |
| M6 | perp-pricing：catalog 驱动 symbol 集合 + per-symbol funding/定价参数（ADR-0069 指数配置并入 catalog）、tick goroutine 内逐 tick 调和 runtime（热上币 / 版本重建 / 失效摘除）、Mark/FundingTick 版本戳、calc 非对称 funding cap/floor | `deb478f` |
| M7 | admin-gateway `/admin/perp/*`：Create / Update（按最新已发布版本做组级合并 + 可选 expected_version OCC 前置条件）/ SetStatus（矩阵 + force 应急路径需 reason）/ Rollback（发布目标版本副本，source_version 溯源）/ ListVersions，全部先审计后响应；BFF `GET /v1/perp/instruments`（仅展示，准入权威是版本握手） | `3242727` |
| M8 | staged 守护：开/加/翻仓时引擎在 `Position.RiskConfigVersion` 上钉版本（snapshot + journal 快照携带）；`ProjectRiskConfig` RPC（affected=维持保证金要求升高的账户数，liquidatable=按当前 mark 直接击穿维持线的账户数）；admin 在 IMMEDIATE 收紧发布前对全部 perp 分片 dry-run，分片错误 / 未配置分片 / 超出 `max_affected_accounts` / 有 liquidatable 但未声明 `allow_mass_liquidation` 一律拒绝发布并审计 | `8dbfa17` |

### 实现决策（对正文的细化与偏差说明）

1. **`perp_symbol_configs` 多一列 `pricing_json`**：正文 §1 把"定价：index source、mark formula"列入 catalog 范围，但表 sketch 未单列；实现将 ADR-0069 指数配置（sources/quorum/staleness/deviation band）与 mark 公式参数（EMA alpha、basis cap、impact notional）放入独立的 `pricing_params`，与 `funding_params` 解耦但随同一 `config_version` 发布。
2. **`perp_symbols.status / config_version` 是"最新已发布"指针**：仅供 ops 列表使用。带 `effective_from_ms` 的未来版本发布后，该指针先于生效领先——服务一律从 `perp_symbol_configs` 推导 active 版本（max version where effective_from_ms <= now），切换无需任何写者。
3. **撤单在 Match 不做状态门**：撤单只移除挂单，对任何状态的语义都是安全的；面向用户的撤单限制（SETTLING 等只允许系统指令）由 perp-counter 准入层执行。这样强平清理、交割结算永远清得动盘口。对应地，状态机关闭新单时（CANCEL_ONLY 等），perp-counter 的强平流程跳过注定被拒的 Match 往返、直接走内部 backstop，保住"有限步收敛"。
4. **fail-closed 边界**：新开仓敞口 fail-closed（unknown symbol / 缓存超期 / Match 无 catalog 收到带戳订单都拒）；存量敞口 fail-open 到最后可解析的模型（symbol 从 catalog 消失时风控评估回退 legacy flag 模型而非静默归零），撤单在 symbol 缺失时放行（降敞口操作）。
5. **staged 钉版本语义**：开仓/加仓/翻仓将 `Position.RiskConfigVersion` 钉到当时 active 版本；减仓/平仓不动。生效解析 = max(pin, 最高的已生效 IMMEDIATE 版本)。dry-run 的 `max_affected_accounts` 预算约束 affected 计数，liquidatable > 0 必须显式 `allow_mass_liquidation`。
6. **perp-pricing 配置重建**：版本切换时该 symbol 的 premium 累加器重启、当期 funding round 重锚（明确放弃用混参样本结算，宁可跳过一轮）；新增外部指数源的 fetcher 需要重启进程（评估器参数热生效），运行时仅告警。
7. **遗留模式**：catalog 未配置（无 `--catalog-dsn`）时全链路回到 flag 驱动、版本戳 0、Match 跳过握手——dev 单机路径不变。

### 后续（不属于本 ADR 交付）

- 价格保护参数已入 catalog（`price_protection`），执行落在 [ADR-0080](./0080-perp-admission-risk-price-protection.md)；费用参数同理落在 [ADR-0079](./0079-perp-fee-accounting.md)。
- trade-dump 投影未新增 `risk_config_version` 列（journal 快照已携带，需要复盘列时再加投影）。
- spot SymbolConfig 仍在 etcd（ADR-0056 的 MySQL 迁移未动）；本 ADR 的 perp catalog 与之并行，不影响 Match 的 etcd 分片归属。

## 参考 (References)

- [ADR-0056: Symbol 配置存储从 etcd 迁到独立 MySQL](./0056-symbol-config-via-mysql.md)
- [ADR-0068: USDT 本位线性永续合约](./0068-usdt-linear-perp.md)
- [ADR-0070: perp 强平进阶](./0070-perp-liquidation-hardening.md)
