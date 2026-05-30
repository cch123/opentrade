# ADR-0068: USDT 本位线性永续合约（perp）—— 仓位 / 保证金 / 标记价 / 资金费 / 强平

- 状态: **Accepted**（账户架构选 A1 = 新建独立 perp-counter 服务，2026-05-29 xargin 拍板）
- 日期: 2026-05-29
- 决策者: xargin, Claude
- 相关 ADR: 0001（Kafka 真值源）、0003（Counter↔Match via Kafka）、0016/0019（per-symbol 单线程撮合）、0018（UserSequencer）、0031（cold-standby HA）、0032（事务 producer fencing）、0041（Reservation 资金预留）、0048（snapshot 绑 offset + flush barrier）、0049（snapshot protobuf）、0050（order-event per-symbol topic）、0051（typed producer seq）、0053（symbol 精度）、0055（Match 直出 orderbook）、0056（SymbolConfig via MySQL）、0057（asset-service + biz_line=futures 预留）、0065（funding wallet MySQL 权威）

## 范围声明（先读这一段）

本 ADR 只覆盖**第一版 perp 的最小可用集**，由 2026-05-29 与 xargin 对齐：

- **USDT 本位线性合约**（linear，USDT-margined）。不做币本位（inverse）。
- **逐仓保证金**（isolated margin）起步，**不实现**全仓（cross）。但保证金模式从 day 1 按 **collateral pool** 抽象建模（§3.1，抵押/保证金池），逐仓是其退化特例，全仓为纯增量、不预埋返工——详见 §备选方案 D。
- **账户架构已选 A1 = 新建独立 perp-counter 服务**（2026-05-29 xargin 拍板）；备选 A2（扩 Counter）的完整对比保留在 §备选方案 A。
- 明确推迟到后续 ADR / future work：cross margin、阶梯杠杆（risk tier）、外部 composite index、完整 ADL 自动减仓、子账户、统一保证金（unified account）、组合保证金（portfolio margin）。下文凡涉及这些只画接口位，不实现。

OpenTrade 未上线，按既有惯例（同 [ADR-0057](./0057-asset-service-and-transfer-saga.md)）不写兼容层。

## 实现进度 (2026-05-30 更新)

逻辑核心 + **perp-counter / markprice 的全部本地可验证集成**已落地并入 CI（离线 `make build/vet` + 各模块 `test -race` 全绿，并对 perp-counter 二进制做了 startup/SIGTERM/snapshot 冒烟）。perp-counter 自身已 feature-complete：下单→结算→标记价/资金费→强平执行→snapshot→冷备 HA→futures 充值入口。仅剩 M7 的 4 个**他模块**接入面（BFF/push/trade-dump/history）待各自基础设施。

| 里程碑 | 范围 | 状态 | commit |
|---|---|---|---|
| M1 | perp proto（PerpService/perp-journal/mark-price）+ perp-counter 模块骨架 | ✅ | `453fddb` |
| M2 | 仓位/保证金/资金费/强平代数（`pkg/perpstate`） | ✅ | `e036dba` |
| M3 | 前置保证金闸门 + reduce_only + 成交结算 + matchSeq 守卫 + 自成交 | ✅ | `d4f9f2b` |
| M4 | markprice mark/funding 计算核心 + 服务骨架 | ✅ | `c078001` |
| M5 | 资金费扫描结算（funding_round_seen 幂等） | ✅ | `18a1897` |
| M6 | 强平检测（collateral pool health 破 mmr） | ✅ | `18a1897` |
| 集成-K | **perp-counter ↔ Match Kafka 接线**：order-event 生产 + perp-journal WAL + perp-trade-event 消费 + 订单生命周期（Accepted/Rejected/Cancelled/Expired）+ IM 释放 | ✅ | `29d8769` |
| 集成-MP | **markprice ↔ perp-counter**：markprice 消费现货+perp market-data 出 mark-price；perp-counter 消费 MarkTick→SetMark / FundingTick→**per-user 资金费结算**（走 sequencer，invariant #1） | ✅ | `7b7646b` `94846b0` |
| 集成-LQ | **强平执行流**：mark tick 触发 → TOCTOU 复核 → 撤挂单 → 破产价 reduce_only 单 → 成交**逐笔**路由权益进保险基金（partial-fill 正确）+ PerpLiquidationEvent + ADL 告警位 | ✅ | `94846b0` |
| 集成-SN | **snapshot 持久化**：engine 状态 + service 订单表 + 绑 perp-trade-event offset + 幂等水位 + 在途强平，`snapshotMu` capture barrier（flush 后原子读，ADR-0048）；启动 restore + 周期/退出 save | ✅ | `b248090` |
| 集成-HA | **冷备 HA**：`--ha-mode=auto` etcd 选主（镜像 match）；只主跑管线，提升即 restore+seek offset，降级写终态 snapshot；安全性靠已落地的 snapshot/offset 绑定 + 事务 producer fencing（ADR-0031/0032） | ✅ | `4a3b4b5` |
| 集成-AH | **futures AssetHolder**（M7 基石）：perp-counter 实现 AssetHolder 合约，funding→futures 保证金充值走现成 saga（asset-service 仅需 `--peer-holders futures=...` flag，零改动）；transfer_id 去重入 snapshot，出 PerpMarginEvent（ADR-0057） | ✅ | `8ac0606` |
| 集成-TD | **trade-dump perp 投影**：`perp-journal` → MySQL（positions/wallets/orders + settlements/funding/liquidations/margin 账本）。纯 `BuildPerpBatch` 单测，perp_seq_id 守卫幂等；`03-perp-schema.sql` | ✅ | `2516634` |
| 集成-PSH | **push perp 私有流**：`perp-journal` → 用户 WS（新 `perp-user` stream，与现货 `user` 分开）；镜像 PrivateConsumer | ✅ | `3d7792a` |
| 集成-BFF | **BFF perp REST**：`/v1/perp/{order,positions,margin}` 路由到 PerpService（`SetPerp` 注入，`--perp` 空则 503） | ✅ | `ad119ef` |
| 集成-HIST | **history perp 查询**：`ListPerpPositions/Funding/Liquidations`（proto + buf 重生成 + sqlmock 单测，keyset 分页） | ✅ | `672b573` |

**M7 接入面已闭环**：perp 现在端到端打通——用户经 BFF 下单/查仓/充保证金、私有流推送、历史查询、MySQL 投影全部就位。各模块的纯逻辑（投影/路由/查询）已单测；真正端到端跑通需把 perp Match 部署 + perp-counter + markprice + 这些消费方一起拉起（broker/etcd/MySQL）。

### 集成-K 落地说明（perp-counter ↔ Match）

- **Match 零改动**：`OrderEvent` / `TradeEvent` proto 本就 symbol-agnostic，`leverage` / `reduce_only` 留在 perp-counter 自己的订单表里（成交回流时按 order_id 取），不上 wire。perp 走**独立 Match 部署**（纯 flag：`--symbols=...-PERP` / `--trade-topic=perp-trade-event` / 独立 group + 选主路径），靠 symbol 的 `-PERP` 后缀路由到它 owned 的 `order-event-...-PERP` topic。验证了 §1「Match 原样复用」。
- **物理隔离的 topic**（§0）：order-event 沿用 `order-event-<symbol>` 前缀；trade-event 用**独立** `perp-trade-event`（与现货 `trade-event` 隔离）；WAL 为 `perp-journal`（key=user_id）。
- **新增 `perp-counter/internal/journal`**：`Producer`（结构化满足 service 的 `Dispatcher`+`Journal`，事务/幂等双模，ADR-0032）、`TradeConsumer`（ReadCommitted 消费组，snapshot 为权威 offset 不回提，ADR-0048）。service 不反向依赖 journal（service 建 proto、journal 只碰 Kafka）。
- **补齐生命周期处理**：消费侧此前只有 `HandleTrade`；新增 Accepted→NEW、Rejected/Cancelled/Expired→**释放订单残余预留 IM + 退场**。否则每次撤单/拒单/过期都漏占保证金。残余 IM 在 `afterFill` 按 `MarginAdded` 递减，部分成交后撤单只释放未成交部分。
- **幂等**：成交走仓位 `last_match_seq` 守卫；生命周期事件走订单态（terminal 退场 / NEW 单调边沿）。`HandleTradeEvent` 记录 per-partition consumed offset，供后续 snapshot 绑定。
- **运维约束**：`perp-trade-event` 的分区数须 ≥ perp Match 的 `--vshard-count`（Match 按 `shard.Index(user_id, vshardCount)` 显式选分区）；MVP 单实例 perp-counter 用消费组吃下全部分区，本地结算全部用户。

### M7 接入面落地说明（4 个他模块面，纯逻辑均已单测）

- **trade-dump perp 投影**（`2516634`）：`writer.BuildPerpBatch` 纯投影（OrderStatus/Settlement/Funding/Liquidation/Margin → 行）；状态表 perp_seq_id 守卫 latest-wins，账本 `(user_id, perp_seq_id)` INSERT IGNORE；`03-perp-schema.sql` 7 张表；`--perp-journal-topic` 可选消费。符合 [[project_adr0066_admission_rule]] 准入（状态日志）。
- **push perp 私有流**（`3d7792a`）：新 `StreamPerpUser="perp-user"` 与现货 `user` 分流；`PerpPrivateConsumer` 镜像 PrivateConsumer（同 sticky 归属 ADR-0033、tail-start、protojson 帧）；`--perp-topic` 可选。
- **BFF perp REST**（`ad119ef`）：`client.Perp` alias + `SetPerp` 注入（不动 NewServer 签名）；`/v1/perp/order|positions|margin`；`--perp` 空则 503。leverage/reduce_only 透传、前置风控 REJECT 上抛。
- **history perp 查询**（`672b573`）：history.proto 加 3 RPC（buf 重生成）；`ListPerpPositions/Funding/Liquidations`，funding/liq keyset 分页（`PerpLedgerCursor` ts+perp_seq_id）；sqlmock 单测。

**剩余真正待办**：仅集成测试（把全套服务 + broker/etcd/MySQL 拉起跑端到端）、以及正文已列的功能 MVP 边界（部分强平再挂、ADL 自动执行、cross margin、外部 index 等——均属"后续 ADR / future work"，非本期接入面）。

### 已落地实现的 MVP 边界（已在代码注释 + commit 记录，列此备查）

- **强平**：每仓一张破产价单；若流动性不足只部分成交，剩余不自动再挂（在途 guard 持有，下个 tick 跳过）；ADL 只算+告警（`adl_queued`），不自动减仓（§9）。
- **order-event / perp-journal 非单事务原子**：稳态双写都成功；崩溃恢复靠 snapshot（含订单表）+ offset 重放 + 幂等水位兜底。真正单 Kafka 事务原子化是后续硬化项。
- **markprice mid**：只用 OrderBook Full 帧（忽略 Delta，同 BFF marketcache），mid 刷新频率 = Full 周期；mark EMA 平滑足够，MVP 可接受。
- **perp-counter 分片**：MVP 单实例消费组吃全部分区；多实例 per-user 分片 + BFF 按 shard 发现是 HA 之后的增量。

## 术语 (Glossary)

| 本 ADR 字段 | 含义 | 业界对标（仅供读者映射，不取其实现细节） |
|---|---|---|
| `position` / 仓位 | 用户在某 perp symbol 上的持仓：方向 + 数量 + 均价 + 占用保证金 | Binance position / OKX 持仓 / Bybit position |
| `position_size` | 仓位数量，base 单位（如 BTC 张数，linear 下 1 张 = 1 base） | BN `positionAmt` / Bybit `size` |
| `entry_price` | 持仓均价（加权平均开仓价） | BN `entryPrice` / OKX `avgPx` |
| `position_margin` | 该仓位占用的保证金（逐仓：每仓独立一份 USDT） | BN `isolatedMargin` / Bybit `positionIM` |
| `leverage` | 杠杆倍数。`init_margin = notional / leverage` | 三家同名 |
| `init_margin` (IM) | 起始保证金，开仓/挂单前置占用 | BN initial margin / OKX `imr` |
| `maint_margin_ratio` (MMR) | 维持保证金率，保证金率低于它触发强平 | BN maintenance margin / Bybit MMR |
| `margin_ratio` | 保证金率 = (position_margin + unrealized_pnl) / notional(mark) | BN margin ratio |
| `isolated` / `cross` | 保证金模式：逐仓（每仓独立、爆仓不连坐）/ 全仓（账户共享）。MVP 只做逐仓 | 三家通用：逐仓/全仓、isolated/cross margin |
| `collateral pool` | 共享一份保证金的一组仓位（+ drawable 可动用余额 + health 函数）；逐仓=单仓一池，全仓=账户全仓一池。**本项目内部抽象名**，非业界统一词 | 分组单位近似 OKX risk unit / collateral pool |
| `mark_price` | 标记价。未实现盈亏 + 强平判定都跑它，抗插针 | 三家 mark price / 标记价格 |
| `index_price` | 指数价。标记价的锚，来自现货参考 | 三家 index price / 指数价格 |
| `unrealized_pnl` | 未实现盈亏，按 mark_price 算，不落账 | BN `unRealizedProfit` |
| `realized_pnl` | 已实现盈亏，平仓/资金费时落进保证金余额 | BN `realizedPnl` |
| `funding_rate` | 资金费率，多空周期互付 | 三家 funding rate / 资金费率 |
| `funding_round_id` | 一次资金费结算的幂等键（如 `BTC-USDT-PERP:1748505600`） | — |
| `liq_price` | 强平价，mark 触及即强平（派生值，不持久化） | BN `liquidationPrice` |
| `bankruptcy_price` | 破产价，仓位权益归零的价；强平单挂在这 | Bybit `bankPrice` |
| `insurance_fund` | 保险基金，吸收强平穿仓亏损的 USDT 池 | 三家 insurance fund / 保险基金 |
| `ADL` | 自动减仓，保险基金兜不住时强制平掉对手盈利仓 | 三家 auto-deleveraging |
| `reduce_only` | 只减仓单，不会反向开新仓（强平单 / 用户平仓用） | 三家 reduceOnly |
| `biz_line=futures` | asset-service 早已预留的合约账户业务线（[ADR-0057](./0057-asset-service-and-transfer-saga.md) 术语表） | — |

## 背景 (Context)

### 现状

1. **系统纯现货**：[architecture.md §3](../architecture.md) 的功能范围全是现货订单类型 + 现货交易能力。
2. **Counter 只有余额，没有仓位**：`pkg/counterstate` 的 `Account` / `Balance` 里没有任何 position / margin / leverage / PnL 概念（2026-05-29 核实）。现货一笔成交是在 maker / taker 的两个 `(user, asset)` balance 间挪 base/quote；perp 一笔成交改的是**仓位 + 已实现盈亏**，是完全不同的结算代数。
3. **现货明确"不做风控前置"**：[architecture.md §3.3](../architecture.md) 把"风控前置"列入 MVP 不做。**perp 绕不开**——不在下单前占用起始保证金，用户就能开出自己 cover 不起的仓位。
4. **资金入口已就位**：[ADR-0057](./0057-asset-service-and-transfer-saga.md) 的 asset-service 已经把 `biz_line=futures` 写进预留枚举。用户充值 USDT 进 funding wallet，要玩合约就走现成的 `funding → futures` saga 划转。**perp 的保证金账户充值不需要任何新划转协议**——只要新账户实现 `AssetHolder` 接口接进 saga。

### 为什么现在能做得相对干净

- 撮合骨架（per-symbol 单线程 actor、order-event/trade-event、snapshot 绑 offset、cold-standby HA、事务 producer）**完全是 symbol-agnostic 的**，perp symbol 直接复用，Match 引擎本体不动。
- 事件溯源 + 幂等三件套（ADR-0048 EOS）已沉淀在 `pkg/`，新服务复用。
- asset-service 的 saga + `AssetHolder` 接口（ADR-0057）已把"多业务线账户"的扩展成本压到"实现一个接口"。

### 什么会变

perp 引入七个现货没有的概念，难点全在账户/风控侧：仓位、保证金引擎、标记价/指数价、资金费、强平、保险基金/ADL、风险限额。下文逐个定义 MVP 形态。

## 决策 (Decision)

### 0. 合约与 symbol

- perp symbol 命名 `BTC-USDT-PERP`（`-PERP` 后缀，直白；和现货 `BTC-USDT` 物理隔离，是不同的撮合 book、不同的 Kafka topic）。
- linear：`position_size` 用 base 单位，`price` 用 USDT，盈亏 = `(exit - entry) × size`（USDT），保证金 USDT 计价。MVP 不引入合约乘数（1 张 = 1 base），精度沿用 [ADR-0053](./0053-symbol-precision-and-tiered-evolution.md) 的 tick/lot 治理 + perp 专属字段（见 §6）。

### 1. Match 原样复用（几乎不动）

perp 的盘口、价格-时间优先撮合、Full/Delta 直出（[ADR-0055](./0055-match-as-orderbook-authority-bybit-style.md)）和现货一模一样。Match **不感知保证金、不感知仓位**，只撮单 emit `trade-event`。变更仅两处：

- 新增 perp symbol 的 input topic `order-event-BTC-USDT-PERP`（[ADR-0050](./0050-match-input-topic-per-symbol.md) 的 per-symbol topic 直接覆盖）。
- `trade-event` 增字段标识 perp + 携带 `reduce_only`（撮合层对 reduce-only 的处理见 §实施约束）。强平单是"来自强平引擎的一个普通 reduce-only 单"，Match 不需要特殊路径。

### 2. 新增 `perp-counter` 服务（A1，已定）

> 账户架构岔路已定为 A1（新建独立服务，2026-05-29 拍板），完整对比见 §备选方案 A。下文按 A1 展开。

`perp-counter` 是 perp 的账户真值，对齐 Counter 的事件溯源机制，但只管合约：

- 持有 per-user **保证金余额**（USDT）+ **逐仓仓位表** `(user, symbol) → Position`。
- 是 `biz_line=futures` 的 `AssetHolder`（ADR-0057）：实现 `TransferOut/In/CompensateTransferOut`，用户的 `funding → futures` 充值走 saga 进来。
- 消费 perp 的 `trade-event` 做仓位结算 + 已实现盈亏。
- 消费 `mark-price` topic 做未实现盈亏更新 + 强平判定（§5）。
- per-user `UserSequencer`（ADR-0018）串行化同一用户的所有写；snapshot 绑 offset（ADR-0048）；cold-standby HA（ADR-0031）；事务 producer 写 `perp-journal`（ADR-0032/0051，新 seq `perp_seq_id`）。
- 按 `user_id` 分 shard，复用 `pkg/shard`（shard 数可独立于现货 Counter）。

### 3. 仓位与保证金模型

#### 3.1 collateral pool —— 让逐仓/全仓不互相挖坑的那道缝

保证金模式从一开始就建模成 **collateral pool（抵押/保证金池）** 抽象，MVP 只实现逐仓：

- **collateral pool** = 一组仓位 + 一个可动用余额池（drawable）+ 一个 health 函数 `health(pool, mark) → (equity, maint_margin, ratio)`。
- **逐仓** = 每个仓位自成一个 pool，drawable 就是它独占的 `position_margin`。
- **全仓**（future）= 账户全部 cross 仓位同属一个 pool，drawable = 整个保证金钱包。
- 即 **逐仓是全仓的退化特例**（pool size = 1 + drawable 是固定碎块）。

硬约束：**equity / margin_ratio / 强平判定一律走 pool 接口，调用点不得硬编码"margin_ratio 一定是 per-position"**。满足这条，加全仓 = 新增一个 pool 实现 + 级联平仓选择，§3.2 仓位模型 / §4 保证金 / §5 mark / §7 资金费 / Match 全部不动。

#### 3.2 Position 字段

```
Position {
  user_id, symbol,
  margin_mode     // ISOLATED（MVP 唯一值）/ CROSS（future）；决定本仓归哪个 collateral pool
  size            // 带符号 or side+size；MVP 用 side(LONG/SHORT)+size(≥0)
  entry_price     // 加权均价
  position_margin // 逐仓：本 pool 独占的 drawable；全仓(future)：恒 0，drawable 走账户钱包
  leverage
  realized_pnl    // 累计已实现（审计/展示）
  last_match_seq  // per-(user,symbol) 幂等水位（ADR-0048 §4 同款）
  funding_round_seen // 最近已结算的 funding_round_id，幂等
  position_version   // 乐观锁/投影守卫（ADR-0048 双层 version 同款）
  // 派生（不持久化，按 mark 实时算）：unrealized_pnl / margin_ratio / liq_price
}
```

逐仓语义：每个 `(user, symbol)` 仓位的保证金独立，一个仓位爆仓不牵连同用户其它仓位或保证金余额的 available 部分（除非用户主动追加保证金）。这是选 isolated 起步的核心简化点。

### 4. 保证金引擎 + 前置风控（复用 Reservation）

**开仓/挂单前置占用**直接复用 [ADR-0041](./0041-counter-reservations.md) 的 Reservation 机制：

- `PlaceOrder(perp)` 进 perp-counter → 算 `init_margin = order_notional / leverage + 手续费 buffer` → 从保证金余额 `available` 里 `Reserve`（不够直接 REJECTED，这就是现货跳过、perp 必做的前置风控）→ 再发 `order-event` 给 Match。
- 成交回流（`trade-event`）→ 把预留的 IM 转成 `position_margin`，建/加仓。
- 撤单 / 部分成交剩余 → `ReleaseReservation` 退回 available。

平仓（reduce_only 反向单）成交时：按平掉的比例释放 `position_margin` 回 available + 结算 `realized_pnl`（用**成交价**，不是 mark）。

> collateral pool 视角：上面的占用/释放都发生在**订单目标仓位所属的 pool** 内。逐仓下 pool = 单仓，"占用 available"即占用该仓 pool 的 drawable；全仓（future）下 available 指向账户钱包。结算代数不变，只是 drawable 指向不同。

### 5. 标记价 / 指数价服务（新增 `markprice`）

新增轻量服务 `markprice`（单实例 + cold-standby 起步，QPS 远低于撮合）：

- **指数价 `index_price`**：MVP 取**自家现货** `BTC-USDT` 的中间价/最新价（消费现货 `market-data`）。⚠️ 自参考有被现货盘口操纵传导的风险，MVP 接受并在 runbook 标注；外部 composite index（多所加权 + 离群剔除）列 §开放问题。
- **标记价 `mark_price`**：抗插针，公式 MVP 取 `mark = index_price + clamp(EMA(perp_mid − index_price), ±cap)`（基差 EMA 限幅），参数可配。**未实现盈亏 + 强平判定一律用 mark，不用 perp 最新成交价**——这是防止"砸自己的 perp 盘口触发连环强平"的关键。
- **资金费率 `funding_rate`**：按结算周期内 premium 的 TWAP 限幅，`funding_rate = clamp(TWAP(premium_index), ±cap)`（MVP 省掉利率项，后续可加）。
- 产出 `mark-price` topic：`{symbol, mark_price, index_price, funding_rate, ts}`，高频 tick（如 1s）。资金费结算边界额外发一条 `FundingTick{symbol, funding_round_id, funding_rate, ts}`（§7）。

### 6. SymbolConfig 扩展（perp 专属风控参数）

复用 [ADR-0056](./0056-symbol-config-via-mysql.md) 的 SymbolConfig（MySQL 权威 + 轻量 poll），给 perp symbol 加字段：`is_perp` / `max_leverage` / `maint_margin_ratio` / `funding_interval`（默认 8h）/ `funding_rate_cap` / `mark_price_cap` / `liq_fee_rate`。MVP 单档 MMR + 单档 max_leverage；阶梯杠杆（risk tier，随仓位名义值收紧）列 future work。

### 7. 资金费结算（多空互付）

每 `funding_interval`（默认 UTC 00/08/16:00），`markprice` 在边界 emit `FundingTick{funding_round_id, rate}`。perp-counter 消费后，对自己 owned 的、该 symbol 的每个仓位：

```
funding_payment = position_notional(at mark) × funding_rate
rate > 0：多头付空头；rate < 0：反向。落进 realized_pnl / position_margin。
```

幂等：每个仓位记 `funding_round_seen`，`funding_round_id ≤ seen` 跳过（重启/重放安全，和 LastMatchSeq 同构）。

### 8. 强平引擎（内置 perp-counter，不单开读服务）

强平逻辑**放在 perp-counter 内、走 per-user sequencer**，不做"独立强平服务读仓位再动手"——后者是典型 TOCTOU（读到的仓位和它动手时的仓位之间，perp-counter 自己可能已经因为一笔在途成交把仓位推进了，独立服务读到的是过期仓位）。

每个 `mark-price` tick fanout 到所有 perp-counter shard，每 shard 在 sequencer 里对自己 owned 用户的相关 collateral pool（§3.1）算 health：

```
ratio = health(pool, mark).ratio
      = (Σ_pool position_margin + Σ_pool unrealized_pnl(mark)) / Σ_pool notional(mark)
ratio ≤ maint_margin_ratio → 触发强平
```

逐仓下 pool = 单仓，上式退化为单仓 margin_ratio（MVP 实际跑的就是这个）；全仓（future）下 pool = 账户全部 cross 仓位，是账户级判定。**强平选择函数返回"本 pool 内待平仓位集合"**：逐仓恒返回那一仓，全仓返回按风险排序的级联子集。下游接管机制（撤单 → reduce_only → 保险基金）对两者完全一样。

强平流程（逐仓，单仓）：
1. 仓位转 `LIQUIDATING` 态，撤掉该仓位所有挂单（释放它们预留的 IM）。
2. perp-counter 以 `bankruptcy_price` 挂一张 `reduce_only` 强平单到 Match（接管仓位）。
3. `trade-event` 回流：结算已实现盈亏。
   - 成交价优于破产价 → 剩余 `position_margin` 差额进 `insurance_fund`。
   - 成交价触及破产价仍亏 → `insurance_fund` 补足；基金不够 → 进 ADL（§9）。

强平**决策流**（与附录"图 3"时序图互补；✅ = 已落地，全链路 Kafka 接线已通，见集成-MP/LQ commits `94846b0`）：

```
 [markprice] 发 mark tick ──(mark-price topic, ~1s)
       │  fanout → 所有 perp-counter shard                              ✅ 集成-MP
       ▼
 每 shard 在 per-user sequencer 内,对 owned 仓位算 collateral pool health
       │
       ▼
 margin_ratio = (position_margin + 未实现盈亏(mark)) / 名义值(mark)       ✅ LiquidatablePositions
       │
       ▼
 ◇ ratio ≤ 维持保证金率(MMR) ?
       │
  否 ──┴── 是
  │        │
  ▼        ▼
安全     仓位 → LIQUIDATING 态
等下个    │
 tick     ▼
        撤该仓全部挂单 ── 释放预留 IM                                     ✅ 集成-LQ
          │
          ▼
        算 bankruptcy_price,挂 reduce_only 强平单 ──order-event──▶[Match] ✅ 集成-LQ
          │
          ◀──────────── trade-event(成交) ──────────────────────[Match]
          ▼
        ForceClose @ 成交价:平掉仓位,算 equity = margin + 本次已实现        ✅ ForceClose
          │
          ▼
 ◇ equity 正/负 ? (成交价 优于 / 劣于 破产价)
          │
  >0(优于)─┴─<0(穿仓)
  │           │
  ▼           ▼
盈余进      ◇ 保险基金够补 ?
保险基金        │
 (+)      是 ──┴── 否
  │        │        │
  │        ▼        ▼
  │      基金补     ADL 队列 + 告警
  │      (-)       (MVP 只排不自动平)
  │        │        │
  └────────┴────────┘
           ▼
 emit PerpLiquidationEvent(perp-journal) + 仓位/保险基金更新
           ▼
        ( 完成 )
```

两个设计取舍图中已体现：判定跑 **mark price**（非最新成交价）防自激连环强平；判定 + 执行都在 **per-user sequencer** 内串行，避免与在途成交的仓位变更竞态（TOCTOU）。

### 9. 保险基金 + ADL

- `insurance_fund`：perp-counter 内的 per-symbol USDT 池（也可全局，MVP 选 per-symbol 简单）。强平盈余进、穿仓亏损出，随 snapshot 持久化。
- ADL：基金兜不住时，按 `盈利 × 杠杆` 排名强制平掉对手方盈利仓。**MVP 只实现基金会计 + ADL 排名队列的计算和告警，真正的自动减仓执行列 future work**（先让基金 + 保守 MMR 扛，穿仓是小概率，发生即告警人工介入）。

## 备选方案 (Alternatives Considered)

### A. 【核心岔路，已定 A1】账户状态放哪：新建 perp-counter vs 扩展现有 Counter

**A1（推荐）新建独立 `perp-counter` 服务**

- 优点：现货 Counter 零污染（账户真值单一职责，ADR-0001）；失败半径隔离（perp 强平风暴打不垮现货撮合）；分片/HA/snapshot 各调各的；和 [ADR-0057 §备选方案 C](./0057-asset-service-and-transfer-saga.md)（已拒"把 funding 塞进 Counter"）+ `biz_line=futures` 预留一脉相承。
- 缺点：一套新服务的运维面（HA / snapshot / 监控）——但 `pkg/` 已封装大部分，增量主要是业务代码。

**A2 扩展现有 Counter 加 position/margin**

- 优点：少一套服务。
- 缺点：Counter 在撮合写热路径上，往里塞合约结算 + 强平 + 资金费会放大故障面、把现货 SLA 和合约耦死；现货按 user 10-shard 的拓扑未必适配 perp 负载；和 0057 已确立的"业务线独立服务"方向冲突。

**已定 A1**（2026-05-29 xargin 拍板），理由同 0057 拒绝"扩 Counter"：撮合关键组件不背新业务的故障面。记录在案：若当初选 A2，本 ADR §2-§9 的状态机/结算/强平逻辑不变，只是宿主从新服务变成 Counter 内的 perp 子模块。

### B. 标记价来源：自家现货 vs 外部 composite index

- 自家现货（MVP 选）：零外部依赖，但自参考、可被现货盘口操纵传导。
- 外部 composite index（多所加权 + 离群剔除）：抗操纵强，但引入外部行情依赖 + 故障面。
- **MVP 选自家现货**，runbook 标注风险；composite index 列 §开放问题，等有真实对抗场景再上。

### C. 强平宿主：内置 perp-counter vs 独立强平服务

- 内置（选）：强平决策和保证金真值同处一个 sequencer，无 TOCTOU。
- 独立服务：读仓位 + 动手分离，必须处理"读到的仓位已被在途成交推进"的竞态，复杂且易错。
- **MVP 内置**。未来若强平计算量大到需要独立扩容，再以"perp-counter 出强平指令、执行仍回 sequencer"的方式拆，而不是让外部服务直接动仓位。

### D. 保证金模式：逐仓优先 vs 全仓优先（设计成增量而非返工）

- 逐仓（选）：每仓独立保证金，爆仓不连坐，风控边界清晰，结算代数最简单。
- 全仓（future）：账户级权益共享，强平要算整个账户的组合保证金，复杂得多。
- **按约定逐仓起步**，全仓列 future work。

**为什么不怕"后期补全仓要大改"**：两种模式的分叉只集中在两处——(1) 保证金记账单位（每仓独立池 vs 账户共享池）；(2) 强平触发与范围（单仓 margin_ratio vs 账户权益级联）。其余全部 margin-mode-agnostic：仓位记录、mark/index、资金费 per-仓算法、Match、order flow、强平接管机制。所以只要 §3.1 的 collateral pool 缝做对（health / 强平走 pool 接口、不硬编码 per-position），全仓就是"加一个 pool 实现 + 级联选择"，且**返工半径限于 perp-counter 内部，碰不到别的服务**——这也是把账户域隔离成独立服务（§备选方案 A1）的附带收益。反例：若 MVP 把 per-position margin_ratio 判定散写在各调用点，后期补全仓就要全网捞这些点改，那才是真"改很多"。

### E. 合约形态：线性 vs 反向

- 线性 USDT 本位（选）：盈亏/保证金都 USDT 计价，数学直观。
- 反向币本位：盈亏按张数 + 币价算，数学更绕，适合特定品种。
- **按约定只做线性**。

## 理由 (Rationale)

1. **复用压倒重写**：Match、事件溯源、HA、snapshot、Reservation、asset-service saga 全部直接复用；真正新写的是仓位结算代数 + 标记价 + 资金费 + 强平四块业务逻辑。
2. **逐仓 + 线性是风控可证明性最高的起点**：每仓独立、USDT 计价，强平/资金费/盈亏都是单仓闭式计算，便于单测和对账。
3. **mark price 解耦撮合价**：未实现盈亏和强平跑 mark，从设计上消除"砸自己 perp 盘口触发连环强平"这类自激风险。
4. **强平内置避免 TOCTOU**：保证金真值的所有 mutation（成交结算、资金费、强平）都在同一 per-user sequencer 串行，杜绝 TOCTOU。
5. **架构方向自洽**：独立 perp-counter + biz_line=futures 是 0057 已铺好的路，不发明新模式。

## 影响 (Consequences)

### 正面

- 在现货骨架上长出合约交易域，撮合/行情/推送/持久化基建大比例复用。
- 现货路径零改动、零新增故障面（推荐 A1 下）。
- 资金费/强平/保险基金作为 first-class 概念沉淀，后续 cross margin / 阶梯杠杆是增量而非重构。

### 负面 / 代价

- **大工程**：新服务 perp-counter + markprice + 一批新 proto（perp rpc / perp-journal / mark-price event）+ trade-dump 新投影（positions / funding / liquidations）+ BFF/Push/History 扩展。分阶段见 §实施约束，整体是多里程碑、跨多周的量级，**不是一次 PR**。
- **正确性敏感**：强平、资金费、盈亏结算是钱直接相关 + 顺序敏感 + 并发敏感，测试成本高（见 §测试要点）。
- **mark price 自参考风险**：MVP 用自家现货，被操纵风险存在，靠保守 MMR + 限幅 + 告警缓解，runbook 须写明。
- **前置风控改变下单语义**：perp 下单可能因保证金不足被 REJECTED，和现货"基本不前置拒"不同，BFF/客户端契约要区分现货/合约。

### 中性

- perp-counter / markprice MVP 单实例或小分片起步，量级远低于撮合热路径。
- ADL MVP 只算不自动执行；保险基金 + 保守 MMR 兜底，穿仓告警人工介入。
- 现货与合约是两套 symbol 命名空间、两套 book、两套账户，BFF 对外按 symbol 后缀路由。

## 实施约束 (Implementation Notes)

### 分阶段落地（建议里程碑，每个一篇可独立 review 的 PR + 单测）

- **M1 — proto + 服务骨架**：`api/rpc/perp`（PlaceOrder/Cancel/Query/QueryPosition/QueryMargin）、`api/event/perp_journal.proto`、`api/event/mark_price.proto`、`api/rpc/markprice`；perp-counter / markprice 的 main 骨架（参考 counter / quote）。
- **M2 — 仓位结算（无杠杆风控、无强平、喂假 mark）**：trade-event → 建/加/减仓 + 加权均价 + realized/unrealized；snapshot 绑 offset round-trip 单测。这一步等价于 §范围声明里 spike 想验证的核心代数。
- **M3 — 保证金 + 前置风控**：接 Reservation，PlaceOrder 占 IM、成交转 position_margin、撤单释放；REJECTED 路径。
- **M4 — markprice 服务**：index（自家现货）+ mark（基差 EMA 限幅）+ funding_rate；发 `mark-price` topic。perp-counter 消费 mark 更新 unrealized。
- **M5 — 资金费结算**：FundingTick + per-position 幂等结算（funding_round_seen）。
- **M6 — 强平 + 保险基金**：mark tick fanout、margin_ratio 判定、LIQUIDATING 流程、bankruptcy 单、insurance_fund 会计；ADL 排名 + 告警（不自动执行）。
- **M7 — 接入面**：asset-service `biz_line=futures` AssetHolder（funding→futures 充值）、BFF perp REST/WS 路由、Push perp 私有流、trade-dump perp 投影、History perp 查询。

### Flag（perp-counter）

`--grpc-addr` / `--kafka-brokers` / `--journal-topic=perp-journal` / `--mark-price-topic=mark-price` / `--shard-*`（对齐 counter）/ `--snapshot-*` / `--etcd`（HA）/ `--insurance-fund-symbol-scope=per-symbol`。markprice：`--index-source=spot-self` / `--mark-ema-alpha` / `--mark-basis-cap` / `--funding-interval=8h` / `--funding-rate-cap`。

### 关键不变量（落地时逐条 audit 代码兑现）

1. 一个仓位的所有 mutation（开/加/减/资金费/强平）只在该 user 的 sequencer 内发生，串行。
2. 未实现盈亏 + 强平判定只用 mark_price；已实现盈亏只用成交价。
3. `funding_round_id ≤ funding_round_seen` 必跳过；`match_seq ≤ last_match_seq` 必跳过（重放/重启幂等）。
4. 强平接管前先撤该仓所有挂单并释放其预留保证金，否则会重复占用。
5. snapshot 含 per-partition offset + 所有仓位 + 保证金 + insurance_fund + 各幂等水位，缺一不可恢复（ADR-0048）。
6. equity / margin_ratio / 强平判定只通过 collateral pool 接口取（§3.1），任何调用点不得内联 per-position margin_ratio 公式——这是逐仓→全仓能做成增量的前提，落地时要专门 grep 审计。

### 测试要点

- 单测：加权均价、realized/unrealized、IM 占用/释放、资金费多空互付（含 rate<0）、margin_ratio 边界触发强平、破产价计算、insurance_fund 盈余/穿仓、各幂等水位重放、snapshot round-trip。
- 集成：开仓→mark 波动→强平全链路；资金费边界对全量仓位结算且只结算一次；perp-counter 在结算/强平中途宕机→snapshot+replay 恢复一致。
- race：perp-counter sequencer（`go test -race`，已纳入 RACE_MODULES 习惯）。

## 开放问题 (Open Questions)

显式声明 scope 边界，避免实施时偷偷塞进本 ADR：

- ~~账户架构岔路最终拍板~~ ✅ 已定 A1（2026-05-29，新建独立 perp-counter）；状态已转 Accepted。
- **外部 composite index**：MVP 自家现货，何时/如何引入多所加权指数 + 离群剔除。
- **cross margin / 统一保证金 / 组合保证金**：逐仓之后的下一档，单独 ADR。
- **perp 快照的生产者（self vs trade-dump shadow）**：现货已由 trade-dump 的 shadow 重放 counter-journal 独占产快照（[ADR-0061](./0061-trade-dump-snapshot-pipeline.md)；Counter 只读消费）。perp 当前落地的 `engine.Snapshot/Restore` 是 **perp-counter 自产**模型（绑 offset，[ADR-0048](./0048-snapshot-offset-atomicity.md)），本 ADR 正文也只引用了 0048、未引用 0061。待决策：是否让 perp 对齐 ADR-0061（trade-dump shadow 重放 perp-journal 产快照）——perp-journal 的 `PerpSettlement/Funding/Liquidation` 均带 `PerpPositionSnapshot`，信息足够 shadow 重建。**注**：快照的*内容*（所有仓位 + 保证金 + insurance_fund + 各幂等水位）已在 §决策 §3 + 不变量 #5 定死并实现，本条只决"谁产"。
- **阶梯杠杆（risk tier）**：MMR/max_leverage 随仓位名义值收紧的分档表。
- **完整 ADL 自动执行**：MVP 只算不执行，何时把自动减仓真正接上。
- **强平手续费 / 清算者激励 / 部分强平**：MVP 单仓一次性破产价接管，部分强平（只平到回到 MM 以上）列后续。
- **perp symbol 的上下架 / 风控参数运维面**：admin-gateway 是否要扩 perp 专属管理操作。

## 参考 (References)

- [ADR-0041](./0041-counter-reservations.md) — Reservation（perp 前置 IM 占用复用）
- [ADR-0048](./0048-snapshot-offset-atomicity.md) — snapshot 绑 offset + 幂等水位（perp 全套复用）
- [ADR-0055](./0055-match-as-orderbook-authority-bybit-style.md) — Match 直出 orderbook（perp book 复用）
- [ADR-0056](./0056-symbol-config-via-mysql.md) — SymbolConfig（perp 风控参数承载）
- [ADR-0057](./0057-asset-service-and-transfer-saga.md) — asset-service + `biz_line=futures` 预留（perp 保证金充值入口）
- 实现位置（动工时涉及）：新增 `api/rpc/perp/` `api/rpc/markprice/` `api/event/perp_journal.proto` `api/event/mark_price.proto`、新增 `perp-counter/` `markprice/`、`asset/` 加 futures holder、`bff/` `push/` `history/` `trade-dump/` perp 扩展。

---

## 附录：关键流程 ASCII 序列图

### 图 1 — 开仓成交结算（前置 IM → 成交建仓）

```
Client    BFF        perp-counter            Match        markprice   perp-journal
  │  POST   │  PlaceOrder(perp,            │              │            │
  │ /order  │   reduce_only=false)         │              │            │
  ├────────►├────────►│                    │              │            │
  │         │         │ Reserve IM         │              │            │
  │         │         │ = notional/lev     │              │            │
  │         │         │ (余额不足→REJECTED) │              │            │
  │         │         │ order-event ──────►│              │            │
  │         │◄────────┤ "received"         │ 撮合          │            │
  │◄────────┤         │                    │              │            │
  │         │         │◄─── trade-event ───┤ (成交价 px)   │            │
  │         │         │ 建/加仓:            │              │            │
  │         │         │  entry=加权均价     │              │            │
  │         │         │  预留IM→position_margin            │            │
  │         │         │  last_match_seq++  │              │            │
  │         │         │ perp-journal(仓位+保证金变更) ─────────────────►│
  │         │         │◄── mark tick ──────────────────────┤(更新       │
  │         │         │  unrealized=(mark-entry)×size       │ unrealized)│
  │  WS push 仓位/保证金更新 (经 Push 消费 perp-journal)     │            │
  │◄════════╪═════════╪════════════════════════════════════╪════════════┤
```

### 图 2 — 资金费结算（边界对全量仓位一次性、幂等）

```
markprice                 perp-counter (每 shard 各自结算 owned 用户)      perp-journal
   │ 到 funding_interval 边界                                                  │
   │ FundingTick{round_id, rate} ─────►│                                       │
   │ (经 mark-price topic 广播)         │ for 每个 owned (user,symbol) 仓位:    │
   │                                    │   if round_id ≤ funding_round_seen:   │
   │                                    │       skip (幂等)                     │
   │                                    │   payment = notional(mark) × rate     │
   │                                    │   rate>0 多付空 / rate<0 反向          │
   │                                    │   落 realized_pnl / position_margin   │
   │                                    │   funding_round_seen = round_id       │
   │                                    │ perp-journal(资金费明细) ────────────►│
   │                                    │ (结算后某些仓位 margin_ratio 下降，    │
   │                                    │  可能立即进入图 3 强平判定)            │
```

### 图 3 — 强平（mark 触及 → 内置 sequencer 接管 → 破产价平仓 → 保险基金）

```
markprice          perp-counter (sequencer 内, 无 TOCTOU)        Match        insurance_fund
   │ mark tick ───────►│                                          │              │
   │                   │ margin_ratio =                           │              │
   │                   │  (position_margin+unrealized)/notional   │              │
   │                   │ if ≤ maint_margin_ratio:                 │              │
   │                   │   仓位→LIQUIDATING                        │              │
   │                   │   撤该仓所有挂单(释放其预留IM) ──────────►│              │
   │                   │   reduce_only 强平单 @bankruptcy_price ──►│ 撮合          │
   │                   │◄──────────── trade-event ────────────────┤              │
   │                   │ 结算:                                     │              │
   │                   │  成交优于破产价 → 盈余 ──────────────────────────────────►│ (+)
   │                   │  触及破产价仍亏 → 基金补足 ◄──────────────────────────────┤ (-)
   │                   │  基金不足 → 标记 ADL 队列 + 告警(MVP 不自动执行)          │
   │                   │ perp-journal(强平明细 + 基金变动)                         │
```
