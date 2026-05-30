# ADR-0069: 外部多源复合指数价（composite index price）

- 状态: **Accepted / Implemented**（2026-05-30 起草；2026-05-30 落地：perp-pricing 复合指数 + stale/degraded 闸门 + per-symbol funding interval）
- 日期: 2026-05-30
- 决策者: xargin, Claude
- 相关 ADR: 0068（USDT 线性 perp，§5 标记价/指数价、§备选方案 B）、0021（Quote 行情 fanout）、0038（BFF market-data cache）、0055（Match 直出 orderbook）、0056（SymbolConfig via MySQL）、0031（cold-standby HA）

## 范围声明（先读这一段）

本 ADR **只改 perp 指数价 `index_price` 的来源与算法**，不动标记价 / 资金费的公式，也不动 perp-counter / Match：

- 现状（[ADR-0068](./0068-usdt-linear-perp.md) §5）：`perp-pricing` 的指数价 = **自家现货单一来源**（消费现货 `market-data` 的 `BTC-USDT` 中间价）。这是 0068 自己标注的**自参考可被操纵**风险点，列在其开放问题里。
- 本 ADR：把指数价升级成**多来源加权 + 离群剔除 + 失活降级**的 composite index。标记价（`mark = index + clamp(EMA(perp_mid − index), ±cap)`，EMA = exponential moving average，指数移动平均）与资金费（现为 Binance 法 premium index + 利率项 + 双重限幅，见 `perp-pricing/internal/calc`）的公式**本 ADR 一行不改**——只是它们消费的 `index` 输入从单源换成 composite。
- 仍是单实例 + cold-standby 的轻量 `perp-pricing` 服务，**不**单开独立 index 服务（理由见 §备选方案 E；保留未来拆分的扩展点(extension point)）。
- 参考实现：`cryptofabric/unimargin-index-server`（Java）做的就是这件事——`{"${Huobi.btcusdt}":1,"${Binance.BTCUSDT}":2,"${Okex.BTC-USDT}":1}` 加权公式 + 30s 源失活 + ≥3 源时按中位数剔 5% 离群。本 ADR 把同类逻辑落到 Go 的 `perp-pricing` 里，并补齐参考实现缺的两件事：**最小源仲裁（quorum）**与**指数失活时冻结强平**。

OpenTrade 未上线，按既有惯例（同 [ADR-0057](./0057-asset-service-and-transfer-saga.md)）不写兼容层——直接把单源指数改成复合指数。

## 术语 (Glossary)

| 本 ADR 字段 | 含义 | 业界对标（仅供映射，不取其实现） |
|---|---|---|
| `index_price` | 指数价，标记价的现货锚 | 三家 index price / 指数价格 |
| `index_source` | 一个外部/内部价源（交易所 + 现货 symbol + 权重） | BN/OKX 的成分交易所 |
| `composite index` | 多个 `index_source` 经失活过滤 + 离群剔除后的加权均价 | 三家 composite/component index |
| `source_max_age` | 单源最大允许陈旧时长，超过即视为失活、不参与计算 | — |
| `quorum` | 产出**新鲜**指数所需的最少存活源数 | — |
| `deviation_band` | 离群剔除阈值：偏离中位数超过此比例的源被丢弃 | — |
| `index_stale` | 指数因存活源 < quorum 而无法刷新的状态；随 MarkTick 下发 | — |
| `degraded`（自参考降级） | 仅剩自家现货一个源时的降级状态，明确标记风险 | — |

## 背景 (Context)

### 现状（已落地，2026-05-30 核实）

`perp-pricing/cmd/perp-pricing/main.go` 的指数价来源是**单一的**：

```
spotMid, ok := book.Mid(cfg.SpotSymbol)   // --spot-symbol BTC-USDT，来自现货 market-data 一个 topic
...
mark, fundingEst := c.Tick(spotMid, perpMid)   // 这个 spotMid 就是 index_price
```

即 `index_price = 自家现货 BTC-USDT 的盘口中间价`。`calc.Tick(index, perpMid)` 把 `index` 当外部输入，**标记价/资金费的代数完全不关心 index 怎么来的**——这是本 ADR 能"只换输入、不动公式"的结构前提。

[ADR-0068 §5 + §备选方案 B](./0068-usdt-linear-perp.md) 已明确：

> 自参考有被现货盘口操纵传导的风险，MVP 接受并在 runbook 标注；外部 composite index（多所加权 + 离群剔除）列 §开放问题。

### 为什么现在做

1. **自参考是结构性风险**：perp 的未实现盈亏 + 强平判定全跑 mark，而 mark 锚在 index 上。若 index 只看自家现货 `BTC-USDT`，攻击者**操纵（压低）现货盘口**就能传导到 perp 的 mark，触发非自然强平——0068 用"保守 MMR + 限幅"缓解，但未消除根因。
2. **参考实现给了可直接参照的实现范本**：`unimargin-index-server` 已经把多源加权 + 离群 + 失活验证可用，算法清晰可移植。
3. **改动半径极小**：只在 `perp-pricing` 内部把"取一个 mid"换成"取一个 composite"，下游（mark/funding 公式、`mark-price` topic 格式、perp-counter）几乎不动。现在做成本最低。

### 参考实现做了什么 / 缺了什么

`unimargin-index-server`（已读源）：

- **加权公式**：配置 `{"${Huobi.btcusdt}":1,"${Binance.BTCUSDT}":2,"${Okex.BTC-USDT}":1}`，展开为 `(Huobi*1 + Binance*2 + Okex*1)/(1+2+1)`。每 500ms 重算，每 1s 发 `index-price-event`。
- **失活过滤**：源价仅当 `now − update_time ≤ 30s` 有效。
- **离群剔除**：存活源 ≥3 时，按中位数丢弃偏离 >5% 的源。
- **缺口 1（本 ADR 补）**：没有显式 **quorum**——存活源跌到 1 个它仍会算，等于静默退化成自参考而不报警。
- **缺口 2（本 ADR 补）**：指数失活时它只是停发，**下游怎么办没有契约**。perp 这边必须明确：指数失活 → 标记价标 `stale` → **perp-counter 暂停强平**（强平依赖新鲜 mark，基于陈旧/降级 mark 去平仓比不平更危险）。
- 不取的部分：参考实现还额外算了一版 mark price（基差平滑），但 opentrade 的 mark+funding 已在 `perp-pricing` 落地且更完整（参考实现不算 funding），**指数侧才是真正缺失的部分**，故本 ADR 只接指数。

## 决策 (Decision)

### 1. composite index 算法

每个 perp symbol 配一组 `index_source`，每个源给出 `(price, ts)`。每个 perp-pricing tick（沿用现有 `--tick-interval`，默认 1s），在**拥有 Calc 的那个单 goroutine 内**按下式算指数：

```
1. 快照所有源的 (price, ts)
2. 失活过滤：丢弃 now - ts > source_max_age 的源 → 得到存活集 S
3. quorum 闸门：|S| < quorum  →  指数失活（见 §3），本 tick 不产新鲜 index
4. 离群剔除：若 |S| ≥ 3：
     m = median(price for S)
     S' = { s ∈ S : |s.price - m| / m ≤ deviation_band }   // 默认 0.05
   否则 S' = S（1~2 个源无法做中位数过滤，直接用）
5. 加权均价：index = Σ_{s∈S'} (w_s · price_s) / Σ_{s∈S'} w_s   // 权重在存活子集上重新归一
```

`index` 作为输入传入现有 `calc.Tick(index, perpMid)`，**之后的一切（mark / funding / topic / perp-counter）不变**。

### 2. 价源与接入方式

- **自家现货源**：沿用现有现货 `market-data` Kafka 消费（`book.Mid(spotSymbol)`），作为一个 `index_source`（权重可配）。
- **外部交易所源**：在 `perp-pricing` 内新增 `internal/index` 的 source adapter（Binance / OKX / Huobi 起步，websocket 订阅其现货 ticker/bookTicker，参考实现同款），各自维护 `(price, ts)` 写进一个并发安全的 `SourceBook`（镜像现有 `MidBook` 的写法）。
- 统一接口：

```go
// 每个价源（自家现货 / 外部所）实现它；tick goroutine 只读快照。
type IndexSource interface {
    Name() string                       // 如 "binance:BTCUSDT"
    Latest() (price dec.Decimal, tsMs int64, ok bool)
    Weight() dec.Decimal
}
```

- adapter 各自带断线重连（参考实现每 5s 检查 ws 连接），重连期间该源 `ok=false`→自动被失活过滤排除。

### 3. 失活降级与安全闸门（本 ADR 相对参考实现的关键补强）

指数源是外部依赖，**必须显式定义"源不够时怎么办"**，否则 perp 会静默基于错误指数执行强平。分三档：

| 存活源数 | 状态 | perp-pricing 行为 | perp-counter 行为 |
|---|---|---|---|
| `≥ quorum` 且含 ≥1 外部源 | 正常 | 正常发 `MarkTick{index_stale=false}` | 正常结算 + 强平 |
| `≥ quorum` 但**只剩自家现货** | `degraded`（自参考降级） | 发 `MarkTick{index_stale=false, degraded=true}` + 告警 | 正常结算 + 强平（但带降级告警，runbook 关注） |
| `< quorum` | `index_stale` | 持有**最后一次有效值（last-good index）**，发 `MarkTick{index_stale=true}`（mark 用冻结 index 算，仅供展示/盈亏，不可强平） | **暂停强平**该 symbol（不产生新破产单），未实现盈亏照常展示；超过硬 TTL 继续 stale 则升级告警 |

即：**强平只在指数新鲜时进行**。这是本 ADR 的核心安全不变量——宁可暂停强平（风险后置、人工可介入），不可基于陈旧/可疑指数执行强平（不可逆、且正是攻击面）。

### 4. 配置形态

MVP 用 perp-pricing 的配置承载（flag + 一个 per-symbol 的 JSON，参考实现同款），长期对齐 [ADR-0056](./0056-symbol-config-via-mysql.md) 的 SymbolConfig（MySQL 权威 + 轻量 poll）：

```jsonc
// index_sources.json（per perp symbol）
{
  "BTC-USDT-PERP": {
    "quorum": 2,
    "source_max_age_ms": 5000,
    "deviation_band": "0.05",
    "sources": [
      { "name": "self:BTC-USDT",     "weight": 1 },   // 自家现货
      { "name": "binance:BTCUSDT",   "weight": 2 },
      { "name": "okx:BTC-USDT",      "weight": 1 },
      { "name": "huobi:btcusdt",     "weight": 1 }
    ]
  }
}
```

新增 flag：`--index-config=index_sources.json` / `--index-source-max-age=5s` / `--index-quorum=2` / `--index-deviation-band=0.05`。保留 `--spot-symbol`（self 源的现货 symbol）。

### 5. `mark-price` topic 字段扩展

`MarkTick` 加两个布尔位（[breaking change 直接改，未上线](./0068-usdt-linear-perp.md)）：

```
MarkTick { symbol, mark_price, index_price, funding_rate, ts,
           index_stale,   // 存活源 < quorum：本 tick 的 index 是冻结值，perp-counter 据此暂停强平
           index_degraded // 仅剩自家现货：自参考降级，告警但不暂停
}
```

perp-counter 消费侧：`index_stale=true` → 跳过该 symbol 的 `scanLiquidations`（仍 `SetMark` 供盈亏展示），并打点告警。`FundingTick` 同理在 stale 时**不结算**（资金费按 mark 算名义值，陈旧 mark 会算错），顺延到下一个新鲜边界。

## 备选方案 (Alternatives Considered)

### A. 维持自家现货单源（现状）vs 复合指数

- 单源（现状）：零外部依赖、零新故障面，但自参考、可被现货盘口操纵传导，且单一现货源故障 = 指数完全不可用。
- 复合（选）：抗操纵、抗单源故障；代价是引入外部行情依赖 + 失活/降级的运维面。
- **选复合**：0068 已认定自参考是结构性风险，复合是标准解法，且改动半径限于 perp-pricing 内部。

### B. 外部源接入：拉（perp-pricing 自己连交易所 ws）vs 推（复用现有行情管线）

- 拉（选，MVP）：perp-pricing 内直接 ws 订阅外部所（参考实现同款），简单自洽，QPS 低（指数 1s 级）。
- 推：让 quote / 一个独立采集器把外部行情写入 Kafka，perp-pricing 消费。更解耦但要先有外部行情采集基建，MVP 没有。
- **MVP 选拉**；若未来外部源增多或要被多服务复用，再抽成独立采集 + topic（与 §E 的拆分是同一条路）。

### C. 加权方案：等权 vs 配置权重 vs 成交量加权

- 配置权重（选，MVP）：参考实现同款，简单、可运维调。
- 等权：配置权重的退化特例。
- 成交量加权（VWAP across exchanges）：更抗流动性稀薄的交易所操纵，但要实时拉各所成交量，复杂，列 future。
- **MVP 选配置权重**，存活子集上重新归一。

### D. 离群剔除：中位数偏离带 vs MAD vs 截尾均值

- 中位数偏离带（选）：参考实现同款（>5% 剔），直观、可解释、易单测。
- MAD（绝对中位差）/ 截尾均值：统计上更稳，但对 2~3 个源意义不大且更难解释。
- **MVP 选中位数偏离带**，且明确"<3 源不做剔除"（无法稳健估中位数）。

### E.【架构分叉点】指数计算放 perp-pricing 内 vs 单开独立 index 服务

参考实现把 index-server 拆成**独立服务**（它还兼管对外行情订阅，所以独立有它的理由）。opentrade 是否照搬？

- **折叠进 perp-pricing（选，MVP）**：指数和标记价同处一个 tick goroutine，省一次 topic 跳转、省一套服务运维，且 mark 本就依赖 index、二者同处一进程是自然的。QPS 低（1s 级），单实例足够。
- **独立 index 服务 + `index-price` topic**：perp-pricing 消费 index。更解耦（指数可被 funding / 风控 / 行情展示等多方复用），但多一套 HA/snapshot/监控面 + 一跳延迟。
- **MVP 折叠**，但**把价源层隔离成 `perp-pricing/internal/index`**（`IndexSource` 接口 + `SourceBook` + composite 计算独立可测），使得未来"指数被多方复用"时，把这个包提成独立服务 + 发 `index-price` topic 是**增量而非重写**——同 0068 对 collateral pool 扩展点(extension point)的处理思路。

## 理由 (Rationale)

1. **只换输入不动公式**：`calc.Tick` 早已把 index 当外部输入，复合指数是纯粹的"输入什么"问题，mark/funding/topic/perp-counter 的正确性不受影响，回归影响范围小。
2. **安全优先于可用**：指数失活时**冻结强平**而非基于错误数据强行计算——强平不可逆且是攻击面，宁可暂停强平也不基于错误数据执行。这是本 ADR 相对参考实现最重要的补强。
3. **复用已验证的算法**：加权 + 失活 + 离群三件套直接对标 `unimargin-index-server` 已验证可用的实现，降低设计风险；只补它缺的 quorum + 下游失活契约。
4. **保留拆分扩展点(extension point)**：价源层独立成包，未来要独立 index 服务是增量。

## 影响 (Consequences)

### 正面

- 消除自参考的根因，perp mark/强平抗现货盘口操纵能力实质提升。
- 抗单一现货源故障：任一源失效，只要 ≥quorum 仍在就照常。
- 指数失活有明确、安全的降级契约（冻结强平 + 告警），不再"静默退化"。

### 负面 / 代价

- **引入外部行情依赖**：perp-pricing 现在要连 Binance/OKX/Huobi 的 ws，多了一类故障面（断线、限频、协议变更）；靠失活过滤 + quorum + 降级态吸收。
- **外部源协议适配的维护成本**：各所 ws 报文格式不同，adapter 要各写各维护（参考实现已有可借鉴的解析）。
- **强平可能因指数失活而暂停**：极端情况下（多源同时失效）强平中止，风险后置到人工——这是刻意的安全取舍，runbook 须写明监控与人工介入流程。
- 合规/法务面：拉外部交易所行情可能涉及数据使用条款，上线前需确认（非工程问题，标注待办）。

### 中性

- perp-pricing 仍单实例 + cold-standby，QPS 不变（指数 1s 级，远低于撮合）。
- composite 在单 goroutine 内算，给定同一组源快照结果确定，沿用现有单写者模型。

## 实施约束 (Implementation Notes)

### 落地要点

- 新增 `perp-pricing/internal/index`：`IndexSource` 接口、`SourceBook`（并发安全 latest 价表，镜像 `MidBook`）、`Composite(snapshot, cfg) (index, stale, degraded)` 纯函数（失活/quorum/离群/加权全在这，独立单测）。
- 外部 adapter（`internal/index/binance`、`okx`、`huobi`）：ws 订阅 + 重连 + 写 `SourceBook`，镜像参考实现的 channel adapter。
- `runTickLoop` 改一行：`spotMid` → `index := composite.Eval(...)`；据 `stale/degraded` 设置 `MarkTick` 的两个布尔位；stale 时沿用上一有效 index（last-good index）算 mark（仅展示）且 funding 不结算。
- perp-counter perp-pricing 消费侧：`index_stale=true` → 跳过 `scanLiquidations(symbol)`（保留 `SetMark`），打点 + 告警。

### 关键不变量（落地时逐条 audit）

1. **强平只在指数新鲜时进行**：`index_stale=true` 的 symbol，perp-counter 本 tick 不得产生任何破产/强平单。
2. **quorum 硬闸**：存活源 < quorum 必置 `index_stale`，绝不用 < quorum 的源算"新鲜"指数。
3. **离群剔除需 ≥3 源**；权重在**存活子集**上重新归一（不能用全集分母）。
4. **资金费不在 stale 边界结算**：陈旧 mark 会算错名义值，顺延到下一个新鲜边界（与 0068 §7 的 `funding_round_seen` 幂等兼容——顺延不跳号）。
5. **composite 计算是纯函数 + 单 goroutine**：给定源快照确定可复现，便于单测与回放。
6. **degraded（仅自家现货）必须显式告警**：不可静默退化成自参考。

### 测试要点

- 单测：加权均价（含权重重归一）、失活过滤边界（恰好 max_age）、quorum 边界（恰好 quorum / 少一个）、离群剔除（2 源不剔 / 3 源剔 / 全部偏离）、degraded 判定、stale 时持有 last-good。
- 集成：某外部源断线 → 自动失活不影响指数；跌破 quorum → MarkTick 置 stale → perp-counter 暂停强平；恢复 → 强平恢复。
- 注入式：构造"自家现货盘口被操纵但外部所不动"→ 验证复合指数不被单个异常源主导而偏移（抗操纵回归）。

### 附录：关键流程 ASCII 序列图

#### 图 1 — composite index 计算（源 fan-in → 过滤 → 加权 → 输入 mark）

```
 外部所 ws (binance/okx/huobi)        perp-pricing                          perp-counter
   │ ticker push                  ┌── SourceBook (并发安全, 各源最新 price,ts)
   ├─────────────────────────────►│   self:BTC-USDT 也是一个源(现货 market-data)
   │                              │
   │            每 tick (1s) ─────► tick goroutine (单写者, 持 Calc):
   │                              │   1 快照所有源 (price,ts)
   │                              │   2 失活过滤 now-ts > max_age 丢
   │                              │   3 ◇ 存活 < quorum ? ──是──► index_stale, 用 last-good
   │                              │        │否
   │                              │   4 ≥3 源: 按中位数剔 >band 离群
   │                              │   5 index = Σ w·p / Σ w  (存活子集归一)
   │                              │   mark = index + clamp(EMA(perp_mid-index))
   │                              │   MarkTick{mark,index,funding,index_stale,degraded} ─►│ SetMark
   │                              │                                                        │ if !stale: scanLiquidations
```

#### 图 2 — 失活降级闸门（强平安全）

```
 存活源数 S
   │
   ◇ |S| < quorum ?
   │
 是─┴────────────────── 否
 │                       │
 ▼                       ◇ S 里只剩自家现货 ?
index_stale=true         │
持有 last-good index   是─┴── 否
mark 仅供展示           │      │
告警(硬TTL升级)         ▼      ▼
 │                  degraded  正常
 ▼                  自参考降级 复合指数
perp-counter:       告警      │
  暂停 scanLiquidations       │
  funding 不结算(顺延)        │
                    └────┬────┘
                         ▼
                  perp-counter: 正常结算 + 强平
```

## 开放问题 (Open Questions)

- **成交量加权（cross-exchange VWAP）**：比配置权重更抗流动性稀薄的交易所操纵，何时引入。
- **源动态增减 / 权重热更**：MVP 走配置 + 重启或 SymbolConfig poll；是否要做到无重启热加载（参考实现用 Nacos 热更）。
- **外部行情数据使用合规**：拉各所行情的条款确认（法务，非工程）。
- **指数独立成服务的触发条件**（§备选方案 E）：指数被几个消费方复用后值得拆。
- **stale 时的 mark 展示策略**：冻结 last-good 是否会误导用户盈亏显示；是否要在 stale 超过某阈值后连展示也标灰。

## 参考 (References)

- [ADR-0068](./0068-usdt-linear-perp.md) — USDT 线性 perp（§5 标记价/指数价、§备选方案 B、开放问题"外部 composite index"）；本 ADR 是其指数侧的展开。
- [ADR-0056](./0056-symbol-config-via-mysql.md) — SymbolConfig（指数源配置的长期承载）。
- 参考实现：`cryptofabric/unimargin-index-server`（Java）—— 加权公式 + 30s 失活 + ≥3 源中位数剔 5% 离群；本 ADR 补其缺的 quorum + 下游失活契约。
- 实现位置（动工时涉及）：`perp-pricing/internal/index/`（新增价源层 + composite 纯函数）、`perp-pricing/cmd/perp-pricing/`（tick loop 接入 + flag）、`api/event/mark_price.proto`（MarkTick 加 `index_stale` / `index_degraded`）、`perp-counter/internal/markprice/`（消费侧 stale → 暂停强平）。
