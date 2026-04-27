# OpenTrade — Crypto Spot CEX 交易系统架构设计

版本：v0.1 (MVP 规划版)
日期：2026-04-18

> 本文档为总体架构概览；每一项具体技术决策（含备选方案、拒绝理由）记录在 [`docs/adr/`](./adr/README.md) 的独立 ADR 文件中。本文档变更应同时对应 ADR 更新。

---

## 1. 系统目标

构建一套支持现货交易的中心化加密货币交易所（CEX）核心撮合与账户系统，包含四个主要模块：
`BFF`、`counter`（柜台/清算）、`match`（撮合）、`push`（推送）；
辅以 `quote`（行情）与 `trade-dump`（持久化）两个旁路服务。

## 2. 关键规模与性能目标

| 指标 | 目标 |
|---|---|
| 单实例下单吞吐 | 20 万 TPS |
| 单 symbol 撮合吞吐 | 4 万 TPS |
| 在线 WS 连接 | 100 万 |
| 下单受理 P99 延迟 | ≤ 10 ms |
| symbol 数量 | 动态，无上限 |

## 3. 功能范围（现货）

### 3.1 订单类型

**撮合引擎直接识别**：

- 限价（Limit）
- 市价（Market，包含 BN `quoteOrderQty` 形态，见 [ADR-0035](./adr/0035-market-orders-native-server-side.md)）
- IOC（Immediate-Or-Cancel）
- FOK（Fill-Or-Kill）
- Post-Only

**触发单（trigger 服务在市场价穿越阈值时下单，ADR-0040 起）**：

- STOP_LOSS / STOP_LOSS_LIMIT
- TAKE_PROFIT / TAKE_PROFIT_LIMIT
- TRAILING_STOP_LOSS（水印追踪 + bps 回撤，[ADR-0045](./adr/0045-trigger-trailing-stop.md)）
- OCO（One-Cancels-Other，N 腿原子下单 + 级联取消，[ADR-0044](./adr/0044-trigger-oco.md)）
- 资金预留（Reserve / ReleaseReservation / `PlaceOrder(reservation_id)`，[ADR-0041](./adr/0041-counter-reservations.md)）
- 过期 TTL（expires_at_unix_ms → EXPIRED + 释放 reservation，[ADR-0043](./adr/0043-trigger-expiry.md)）

### 3.2 交易能力

- 下单、撤单
- 改单 = 撤单 + 新建（失去价格时间优先级）
- 批量下单（同一 symbol 内）
- 自成交保护（STP）
- clientOrderId 幂等（可选字段）

### 3.3 MVP 不做

- 风控前置
- ~~鉴权 / API Key 管理（预留 middleware 挂载点）~~ — 已补：header / HS256 JWT / BN 风格 HMAC API-Key，详见 [ADR-0039](./adr/0039-bff-auth-jwt-apikey.md)
- 钱包对接（充提链上交互）
- 费率 / VIP 等级 / 邀请返佣（Counter 暴露配置接口，MVP 用常数或 0）
- 告警
- 审计日志收集管道（只打 zap 结构化日志，收集另论）

## 4. 模块职责

| 模块 | 职责 | 不负责 |
|---|---|---|
| **BFF** | REST 下单/撤单/查询；WebSocket 网关；鉴权挂载点（header / JWT / API-Key，ADR-0039）；滑动窗口限流；market-data cache 供重连补齐（ADR-0038） | 业务状态、admin ops（见 `admin-gateway`） |
| **admin-gateway** | 独立进程（默认 `:8090`，与 BFF `:8080` 隔离）；ops 内网入口：symbol CRUD（写 etcd）、按 user / symbol 批量撤单（打 Counter `AdminCancelOrders`）、JSONL 审计；`pkg/auth` 共享 role 机制，`AdminMiddleware` + `RequireAdmin` 守卫；ADR-0052 | 2C 流量、用户下单 |
| **counter** | 账户余额、冻结、解冻；sequencer（同用户串行）；clientOrderId 去重；订单生命周期状态；费率接口；`Transfer`（deposit/withdraw/freeze/unfreeze）；`Reserve` / `ReleaseReservation` / `PlaceOrder(reservation_id)` 为触发单预留资金（ADR-0041）；`AdminCancelOrders(user_id?, symbol?)` 按过滤器批量撤单，每笔走正常 sequencer 路径（ADR-0052） | orderbook、撮合逻辑、行情订阅 |
| **match** | 内存 orderbook（per symbol 单线程）；撮合规则；成交事件生成 | 余额、权限、费率计算 |
| **push** | WebSocket 连接维持；订阅关系管理；私有数据（订单/账户）+ 公共行情扇出；coalesce 可替代流 + per-conn token bucket（ADR-0037） | 行情计算、业务状态 |
| **quote**（旁路） | 消费 trade-event → 生成增量深度、逐笔、K 线（含 gap 填充）；发布 market-data；state snapshot 热重启（ADR-0036） | 连接推送 |
| **trigger**（旁路） | 订阅 market-data → 维护 pending 触发单状态（含 trailing watermark / OCO 组 / TTL）→ 触发时调 counter.PlaceOrder；每次状态转换向 `trigger-event` topic 发 post-change full snapshot 供长期历史（ADR-0047）；单实例 + cold-standby HA（ADR-0040~0045） | 行情产出、订单撮合 |
| **trade-dump**（旁路） | 消费 counter-journal + trade-event → 幂等写 MySQL | 在线查询 |
| **history**（只读） | 读 trade-dump 的 MySQL 投影，对外提供 `GetOrder` / `ListOrders` (scope=OPEN/TERMINAL/ALL) / `ListTrades` / `ListAccountLogs` gRPC；cursor 分页；BFF 的列表类 REST 走这里（ADR-0046） | 写、Kafka 消费、业务状态 |

### 4.1 定序组件（Sequencer）

**Counter 和 Match 对定序有不同要求，采用差异化实现。** 详见 [ADR-0018](./adr/0018-counter-sequencer-fifo.md)、[ADR-0019](./adr/0019-match-sequencer-per-symbol-actor.md)。

| 维度 | Counter Sequencer | Match Sequencer |
|---|---|---|
| 定序单元 | user_id | symbol |
| 数量级 | 百万级（动态活跃） | <2000（静态配置） |
| 实现 | 懒启动 per-user worker，30s idle 退出 | per-symbol 常驻 goroutine |
| FIFO 保证 | Go channel | Go channel |
| 单调序列字段 | `counter_seq_id`（counter shard 级单调，`atomic.Uint64`） | `match_seq_id`（per-symbol 单调） |
| 内存稳态 | 1-10 MB | ~16 MB |

**关键原则**：**严格 FIFO 由 Go channel 语义保证；`sync.Mutex` 不是定序组件**（Go 的 mutex 允许新到 goroutine 在一定条件下插队，不能用作严格定序）。

## 5. 核心架构图

```
   ┌── etcd ── /cex/counter/{nodes,assignments,coordinator} / SymbolConfig ─┐
   │                                                                         │
 ┌─▼─ BFF (无状态 N 实例) ───────────────────────────────────────────────┐   │
 │ REST + WS / 鉴权 / 限流 / market-data cache (ADR-0038)                 │   │
 │ watch /cex/counter/assignments → user_id hash → vshard 路由 (ADR-0058) │   │
 └─┬──────┬─────────┬──────────┬──────────────────────────┬─────────────┘   │
   │gRPC  │gRPC     │gRPC      │gRPC                      │WS sticky        │
   ▼      ▼         ▼          ▼                          ▼                 │
 counter trigger   asset      history (只读)              push               │
 N node  HA cold   HA cold    MySQL ro 投影               N 实例             │
 ×256    standby   standby    orders/trades/              WS 扇出            │
 vshard  ADR-0042  ADR-0065   account_logs/triggers       ADR-0022           │
 ADR-58  +ADR-67               ADR-0046                                       │
 ADR-60  read snap                                                            │
         from td                                                              │
   │ │     │ │       │            ▲                       ▲ ▲                 │
   │ │     │ │ mkt   │            │ MySQL ro              │ │ priv + pub      │
   │ │     │ │ data  │            │                       │ │                 │
   ▼ ▼     ▼ │       ▼            │                       │ │                 │
 ┌── Kafka cluster (3 broker, ISR ≥ 2) ────────────────────┴─┴──────────────┐ │
 │ counter-journal        (256 vshard partition; Counter WAL, ADR-0004)     │ │
 │ order-event[-<symbol>] (per-symbol partition; Counter→Match, ADR-0050)   │ │
 │ trade-event            (256 vshard partition; Match→Counter/Quote/dump)  │ │
 │ trigger-event          (user_id partition; Trigger→trade-dump, ADR-0047) │ │
 │ asset-journal          (user_id partition; Asset→trade-dump, ADR-0065)   │ │
 │ market-data            (symbol partition; Quote→Push/Trigger/BFF)        │ │
 └─┬─────────┬───────────────────────────┬────────────────────────────────┘ │
   │poll     │poll                       │poll                                │
   ▼         ▼                           ▼                                    │
 match     quote                    trade-dump (持久化 + snapshot 生产)       │
 per-sym   depth/                   pipelines:                                │
 shard     kline/                     sql        → MySQL projection (ADR-08)  │
 主备(02)  trades                     snap       ShadowEngine + Capture       │
 snapshot  snapshot                              → BlobStore (ADR-0061)        │
 →trade-   →market-                   trig-snap  同 snap (ADR-0067)            │
  event     data                    on-demand RPC:                             │
                                      TakeSnapshot         (ADR-0064)          │
                                      TakeTriggerSnapshot  (ADR-0067 M4)       │
                                     │                                         │
                                     ▼                                         │
                             MySQL    BlobStore (fs / s3)                      │
                             (history  ▲ counter & trigger                      │
                              reads)   │ startup READ                           │
                                                                                │
 ┌── admin-gateway ─────────────────────────────────────────────────────────┐ │
 │ 内部 ops / rollout / 灰度切换 / SymbolConfig 维护                        │ │
 └──────────────────────────────────────────────────────────────────────────┘ │
                                                                                │
   ─────────────────────────────────────────────────────────────────────────────┘
```

> **关键变化**（与旧 ADR-0010 / ADR-0006 时代对比）：
> - **Counter**：N node × 256 vshard，每 vshard 单 owner（不是 1 主 1 备）。Coordinator 用 HRW 算分配；故障 failover 自动，新增节点扩容用 `counter-migrate` 手动 rebalance（ADR-0058 / [runbook §8](./runbook-counter.md#8-vshard-扩容--再平衡-adr-0058)）
> - **Trigger**：HA cold standby（ADR-0042）+ 启动从 trade-dump 共享 BlobStore 读 snapshot（ADR-0067，自己不再写）
> - **Asset**：funding-wallet service（ADR-0065），承担充提冻 / 内部转账，独立 `asset-journal`
> - **trade-dump**：三条 pipeline 同进程（sql / snap / trig-snap）；同时提供 on-demand snapshot RPC（ADR-0064 / 0067 M4）作为 counter / trigger 启动 hot path
> - **history**：只读 MySQL 投影服务（ADR-0046），BFF 列表类查询走它，不走 counter 内存
> - **admin-gateway**：运维侧（rollout / 灰度切换 / SymbolConfig 维护）

> **读路径（ADR-0046）**：BFF 的列表类 REST 不直接读 MySQL —— 走
> `history` gRPC。history 是无状态只读服务，订阅 trade-dump 投影的
> `orders` / `trades` / `account_logs` / `triggers` 表，对上提供 cursor 分页 +
> 时间窗口 + (symbol / status / asset / biz_type) 过滤。延迟受 trade-dump
> 批提交语义影响（ADR-0023，≤ 一批），实时成交进度仍走 Push 的
> `user` 流（ADR-0007）。活跃单按 id 查询 (`GET /v1/order/:id`) 保留 Counter hot path。

## 6. 核心数据流

### 6.1 下单流程（端到端）

```
1. 用户 → BFF (REST 或 WS)
2. BFF → Counter 主 (gRPC, 按 user_id hash 路由)
3. Counter 主:
     a. 锁 user
     b. clientOrderId 去重查询
     c. 内存读余额, 计算冻结金额
     d. 若余额不足 → 直接返回拒绝 (不写 Kafka)
     e. Kafka 事务:
        - produce counter-journal  (FreezeEvent, user 分区)
        - produce order-event      (OrderPlaced, symbol 分区)
        - commit transaction       (~3-5ms)
     f. 事务成功 → 更新本地内存 (available-=, frozen+=, 记录 order)
     g. 返回 BFF (order_id, status=received)
4. BFF 返回用户: 已受理 + order_id
5. Match (per-symbol) 消费 order-event → 入 orderbook → 撮合
6. Match Kafka 事务产出 trade-event (symbol 分区)
7. Counter 主消费 trade-event (EOS 事务):
     a. 算 settlement (maker/taker 金额, 费用)
     b. produce counter-journal (SettlementEvent)
     c. sendOffsetsToTransaction (trade-event consumer offset)
     d. commit → 更新内存 (frozen-=, available+=, base/quote 对应变化)
8. Counter 备 / trade-dump / push tail counter-journal → 各自处理
9. Quote 消费 trade-event → 算增量深度/K线/逐笔 → market-data topic
10. Push 消费 (counter-journal 私有数据 + market-data 公共行情) → WS 推给订阅用户
```

### 6.2 撤单流程

```
1. 用户 → BFF → Counter 主
2. Counter:
     a. 查 order_id 对应 symbol
     b. Kafka 事务: produce counter-journal(CancelPending) + order-event(OrderCancel)
     c. 返回 BFF: 撤单已提交 (注意: 不是"已撤销")
3. Match 消费 order-event(OrderCancel) → 从 orderbook 移除 → 产出 trade-event(OrderCancelled)
4. Counter 消费 trade-event(OrderCancelled) → 解冻 frozen → 写 journal
5. 用户通过 WS 收到最终撤单结果
```

### 6.3 Transfer 流程（deposit / withdraw / freeze / unfreeze）

预留给未来 wallet-service 调用，MVP 阶段对内开放。

```
1. 内部调用方 → Counter 主 gRPC (Transfer RPC)
2. Counter:
     a. lockUser(user_id)
     b. TransferID 去重检查
     c. 内存校验 (余额是否足够,按 Type)
     d. 单次 Kafka 写: counter-journal (TransferEvent), 无需事务
     e. 更新内存
     f. 返回 (status, balance_after)
```

### 6.4 消费 Kafka 事件的幂等

所有消费方都要支持：
- producer 自带的单调字段（`counter_seq_id` / `match_seq_id` / `quote_seq_id` / `trigger_seq_id`，每条事件类型自己声明），或 `(topic, partition, offset)`，作为幂等键
- consumer offset 提交与业务写入的原子性：
  - **Counter 主**：用 Kafka EOS 事务（`sendOffsetsToTransaction`）
  - **trade-dump**：MySQL UNIQUE 索引 + `INSERT ... ON DUPLICATE KEY UPDATE`，`accounts` / `account_logs` 行级 guard 用 `counter_seq_id`，`trades` 用 `(symbol, match_seq_id)`
  - **Match**：按 `order_id` 业务去重（orderbook 自带），同时拿 `counter_seq_id` 做防重投兜底
  - **Counter（消费 trade-event）**：内存里维护 `Account.matchSeq` 按 `(user, symbol)` 拦 match 重发

### 6.5 订单状态机

采用 **Binance 风格：内部 8 态 + 外部 6 态**。详见 [ADR-0020](./adr/0020-order-state-machine.md)。

**内部 8 态**（Counter 权威，写 journal）：
`PENDING_NEW` / `NEW` / `PARTIALLY_FILLED` / `FILLED` / `PENDING_CANCEL` / `CANCELED` / `REJECTED` / `EXPIRED`

**外部 6 态**（API / WS 对用户暴露）：
`NEW` / `PARTIALLY_FILLED` / `FILLED` / `CANCELED` / `REJECTED` / `EXPIRED`

### 6.6 触发流程（ADR-0040~0045）

触发单自己有一套独立状态机（`PENDING` / `TRIGGERED` / `CANCELED` / `REJECTED` / `EXPIRED`，ADR-0040 / 0043），不经过 Counter 的 order 状态机 —— 直到触发那一刻。

```
1. 用户 → BFF REST (POST /v1/trigger 或 /v1/trigger/oco)
2. BFF → trigger 主 gRPC (PlaceTrigger / PlaceOCO)
3. trigger 主:
     a. 验证 shape（ type / side / stop_price / trailing_delta_bps / expires_at_unix_ms ）
     b. 分配 id，若 reserver 配了 → 调 Counter.Reserve 冻结资金
        (ADR-0041: Available → Frozen + reservations[refID] = {user, asset, amount})
     c. 落 pending 表；返回 id 给用户 (status = PENDING)
4. trigger 消费 market-data → 每笔 PublicTrade:
     a. standalone 触发单：比较 stop_price
     b. trailing：更新 watermark → 比较 effective_stop (ADR-0045)
     c. 任一触发 → fire 外置 Counter.PlaceOrder(reservation_id, client_order_id="trig-<id>")
        Counter 在同一个用户 sequencer 闭包内:
          - 查 reservations[refID]，验证 (asset, amount) 匹配
          - 删 reservation，不再 freeze，正常创建 order → Kafka 事务
        失败 → trigger REJECTED + best-effort Release
        成功 → trigger TRIGGERED
     d. OCO 组的任一腿到终态 → 同组 PENDING 腿自动 CANCELED + Release (ADR-0044)
5. 后台 sweeper (主 only):
     - 到 expires_at_unix_ms 的 pending → EXPIRED + Release (ADR-0043)
6. 用户可随时 DELETE /v1/trigger/{id} → CANCELED + Release
```

Counter 在触发单路径上完全不感知 "trigger"：它看到的就是普通 `PlaceOrder(reservation_id=…)`，由 reservation_id 路径走已冻结资金的快捷分支。

PENDING_NEW 和 PENDING_CANCEL 仅在 Counter 内部可见（用于监控、排查、一致性校验），对外呈现为相邻的非 pending 态。

**clientOrderId 幂等语义**（详见 [ADR-0015](./adr/0015-idempotency-at-counter.md)）：
- 仅对**活跃订单**去重（NEW / PARTIALLY_FILLED / PENDING_* 期间占用）
- 订单进入终态后，同一 clientOrderId 可复用（对齐 Binance/OKX）
- 无需独立 dedup 表或 Redis TTL

## 7. Kafka Topic 设计

| Topic | 生产者 | 分区 (key / 数量) | 主要消费者 | 内容 |
|---|---|---|---|---|
| `counter-journal` | Counter (vshard owner) | user_id → vshard / 256 partition | push, trade-dump | 所有账户/冻结变更的规范化流水（Counter 的 WAL，ADR-0004 / ADR-0058） |
| `order-event[-<symbol>]` | Counter (vshard owner) | symbol / per-symbol | Match | OrderPlaced / OrderCancel（ADR-0050 per-symbol topic 拆分） |
| `trade-event` | Match (per-symbol shard) | user_id → vshard / 256 partition | Counter, quote, trade-dump | TradeMatched / OrderCancelled / OrderRejected |
| `trigger-event` | Trigger primary | user_id | trade-dump | 触发单状态转换 post-change full snapshot，供 trade-dump 投影 + shadow snapshot (ADR-0047 / ADR-0067) |
| `asset-journal` | Asset primary | user_id | trade-dump | funding-wallet 充提冻 / 内部转账流水（ADR-0065） |
| `market-data` | Quote | symbol | push, trigger, BFF cache | 增量深度 / K 线 / 逐笔 |

### 7.1 Kafka 配置要求

- 3+ broker 集群
- `replication.factor=3`, `min.insync.replicas=2`
- `acks=all`（重要 topic：counter-journal / trade-event / trigger-event / asset-journal）
- `enable.idempotence=true`（所有 producer）
- 事务 producer：每个 Counter vshard / 每个 Match per-symbol shard 一个稳定 `transactional.id`，新 owner 接管时同 id 调 `InitTransactions()` 自动 fence 老 owner（ADR-0017 / ADR-0058）
- 事务消费：`isolation.level=read_committed`

### 7.2 transactional.id 规划

```
counter-vshard-NNN              # 每个 vshard 一个稳定 id（NNN ∈ [000, 255]）
match-shard-{symbol-group}-main # Match 仍是 per-symbol-group 主备模型
trigger-primary                 # Trigger 单实例 cold-standby (ADR-0042)
asset-primary                   # Asset 单实例 cold-standby (ADR-0065)
```

Counter 这边的 vshard → transactional.id 是**固定映射**（不挂 etcd），由 vshard id 直接派生；assignment 表（`/cex/counter/assignments/vshard-NNN`）变更时新 owner 拿到 vshard，调用同名 transactional.id 的 `InitTransactions()` 把老 owner 在 Kafka 上 fence 掉（epoch 单调递增，老 owner 后续 produce 一律失败）。

## 8. 一致性模型

### 8.1 强一致性点

- 用户余额变更：Counter vshard owner 内存 + counter-journal Kafka（acks=all）单调递进
- 同一用户的操作顺序：按 user_id hash 路由到同一 vshard owner，vshard 内 per-user FIFO sequencer (ADR-0058 + ADR-0060)
- 同一 symbol 的撮合顺序：按 symbol 路由到同 Match shard，Match 内单线程（ADR-0016 / ADR-0019）
- Counter 下单的原子性：Kafka 事务包住 counter-journal + order-event 两次写（ADR-0005）
- Counter 消费 trade-event 结算：EOS 事务（consume + produce journal + commit offset 原子）

### 8.2 最终一致性点

- vshard 切换瞬间：老 owner crash → coordinator failover → 新 owner restore，期间该 vshard 短暂不可写（≤ etcd lease TTL + restore 时长）；状态恢复后从 trade-dump-produced snapshot + counter-journal catch-up 推到 HWM (ADR-0058 / ADR-0061)
- trade-dump → MySQL：延迟 100ms - 秒级
- quote → market-data：延迟 ms - 百毫秒级
- push 私有数据推送：延迟 ms 级

### 8.3 Source of Truth

**Kafka 是唯一权威事件流**：
- 所有模块状态都可以从 Kafka 历史事件回放重建
- 快照只是回放起点的优化，不是必需
- MySQL 只是查询投影，丢失可重建

## 9. HA 与故障处理

### 9.1 节点故障与切换

不同服务的高可用形态不同：

- **Counter**（ADR-0058 vshard 模型）：每个 vshard 单 owner，没有 1 主 1 备概念。Coordinator 通过 etcd watch 监测 `/cex/counter/nodes/`，节点 lease 过期 → 它的所有 vshard 通过 `sweepOrphans` force-reassign 到活节点（epoch+1，同 transactional.id 自动 fence 老 owner）。所有 counter 节点同时也都参选 coordinator，保证写入侧不空窗。
- **Match**（per-symbol 1 主 1 备）：仍是经典 cold standby —— etcd lease 选主，主备各占一个 shard 实例，主 crash 备抢锁升主，同 `transactional.id` 调 `InitTransactions()` fence 老主。
- **Trigger / Asset**（单实例 cold standby，ADR-0042 / ADR-0065）：与 Match 同模型，etcd lease + standby。
- **BFF / push / quote / history / admin-gateway**：无状态或弱状态，多实例水平扩展，单实例死亡负载层重路由。

通用 Kafka fencing：所有 transactional producer 走稳定 `transactional.id`，新 owner / 新主接管时同 id 调用 `InitTransactions()`，epoch 单调递增。

### 9.2 Counter 节点崩溃（ADR-0058 cold handoff）

```
T0: vshard X 在 node A 上正常服务
T1: node A crash（进程挂 / 机器宕 / 网络分区）
T2: etcd lease 过期（默认 10s）→ /cex/counter/nodes/A 消失
T3: coordinator 的 failover loop watch 到 nodes/ 变更 → sweepOrphans
      - 拿当前活节点列表
      - 找出 owner 已死的所有 vshard（包括 vshard X）
      - 用 HRW 算每个 orphan 的新 owner（比如 node B）
      - 对每个 orphan: ForceReassign → CAS 写
        Assignment{Owner: B, Epoch: N+1, State: ACTIVE}
T4: node B 的 Manager.WatchAssignedAssignments 收到新条目 → 启动 VShardWorker:
      - 同 transactional.id (counter-vshard-X) 调 InitTransactions() → fence 老 owner
      - hot path: 调 trade-dump TakeSnapshot RPC 拿一份 LEO-aligned snapshot
                 (ADR-0064; 失败 / 超时 → 落 cold path)
      - cold path: 从共享 BlobStore 读最近一次周期 snapshot
      - catch-up: 从 snapshot 嵌的 journal_offset 重播 counter-journal 到 HWM
                  (ADR-0060 §4.2)
      - trade-event consumer 用 AdjustFetchOffsetsFn seek 到 snapshot 嵌的 offset
                  (ADR-0048)
      - close(ready) → vshard X 对外服务恢复
T5: 老 node A（若网络分区后活回来）尝试 Kafka 写时被 fence 拒绝 → 进程退出
```

RTO（per vshard）：lease TTL + reassign + restore，典型 15-30s。多 vshard 并行，不同 vshard 互不阻塞。

### 9.3 Match 主崩溃

同传统 1 主 1 备：etcd lease 过期 → 备抢锁 → 同 `transactional.id` 调 `InitTransactions()` fence 老主 → 从最近 snapshot + order-event 增量回放重建 orderbook → ready。RTO 10-15s。

### 9.3b Trigger 主崩溃（ADR-0042 + ADR-0067）

Trigger 是 cold standby 单实例 + ADR-0067 后 snapshot 由 trade-dump 单一生产，启动恢复路径与 Counter 对齐：

```
T0~T3: 同 Match 流程 (etcd lease 过期 → standby 抢锁升主)
T4: 新主:
      - hot path: 调 trade-dump TakeTriggerSnapshot RPC 拿 LEO-aligned snapshot
                  (ADR-0067 M4; 失败 → 落 cold path)
      - cold path: 从共享 BlobStore 读 trade-dump 周期产出的 fixed-key snapshot
                   (ADR-0067 M5)
      - market-data consumer 用 AdjustFetchOffsetsFn 从 snapshot 嵌的 offset 续消费
      - expiry sweeper 接管, gRPC server 对外
T5: 老主（若存活）split-brain → 两个都下 PlaceOrder(reservation_id=X)
      → Counter 的 client_order_id / reservation_id dedup 吸收，无业务错误
```

历史上"无 snapshot 窗口的 pending 会丢"那个 trade-off 在 ADR-0067 后大幅收敛 —— trigger-event 由 trade-dump shadow 实时消费 + 周期 capture，新主可以拿到 trigger-event LEO 对齐的 snapshot；只有"trigger primary 的 trigger-event 写入还没成功就 crash"那种极小窗口才会丢，常态恢复几乎无损（ADR-0067 §G* 列了剩余的 known gap）。

### 9.4 Kafka 不可用

- 所有 Counter/Match 主退化为 degraded，拒绝新写入
- 用户侧收到 503 或明确错误
- Kafka 修复后自动恢复（producer 重试 / consumer 续消费）
- 期间数据不丢（用户看到失败 → 可重试）

### 9.5 丢数据场景分析

| 场景 | 数据影响 |
|---|---|
| Counter 单次 Kafka 写失败 | 用户看到失败，无副作用 |
| Counter 写 journal 成功后 crash | 新 vshard owner 接管后数据在 journal，用户 clientOrderId 重试命中去重返回 |
| Kafka 集群全挂 | 无数据丢失（服务不可用），恢复后继续 |
| 单个 broker 挂 | 无影响（ISR ≥ 2，acks=all） |
| 两个 broker 挂 | 写入阻塞（ISR < min），无数据丢失 |
| push 挂 | WS 断连，客户端重连+主动拉状态；可能错过几条推送 |
| trigger 主非 graceful crash | 上一次 snapshot 之后新下的 pending 触发单会丢；已触发的因为 Counter.PlaceOrder 幂等不受影响 |
| trigger 主 + backup 同挂 | 到 graceful 恢复前，pending 触发单不触发；资金仍锁在 Counter reservation |

## 10. 持久化与存储

### 10.1 存储分层

| 层 | 介质 | 内容 | 用途 |
|---|---|---|---|
| 内存 | RAM | Counter 余额、Match orderbook | 热服务 |
| Kafka | 本地盘 | 所有事件流 | WAL / 回放源 |
| 快照 | 本地盘 + S3/EFS | 定期状态快照 | 冷启动加速 |
| MySQL | 远程 | 历史订单 / 成交 / 流水 | 历史查询 |

### 10.2 Counter / Match 不直接写 MySQL

- trade-dump 消费 counter-journal + trade-event
- 按 producer 名义命名的单调字段做幂等写入：counter-journal 用 `counter_seq_id`（accounts/account_logs 的 guard），trade-event 用 `match_seq_id`（trades 的 UNIQUE）
- MySQL schema（初版）：
  - `accounts(user_id, asset, available, frozen, updated_at, counter_seq_id, account_version, balance_version)`
  - `orders(order_id, user_id, symbol, side, type, price, qty, filled_qty, status, created_at, updated_at)`
  - `trades(trade_id, symbol, maker_order_id, taker_order_id, price, qty, taker_side, ts, match_seq_id)`，`UNIQUE KEY (symbol, match_seq_id)`
  - `account_logs(shard_id, counter_seq_id, asset, user_id, delta_avail, delta_frozen, avail_after, frozen_after, biz_type, biz_ref_id, ts)` — journal 流水镜像，PK `(shard_id, counter_seq_id, asset)`
  - `triggers(id, user_id, symbol, side, type, status, stop_price, limit_price, qty, oco_group_id, last_update_ms, ...)` — trigger-event 投影（ADR-0047）；`last_update_ms` guard 拦旧事件回写
  - 可按 user_id / symbol 分库分表（MVP 先单库）

### 10.3 读路径

| 查询 | 来源 | 延迟 | 入口 |
|---|---|---|---|
| 当前余额 | Counter 内存（gRPC） | ms 级 | BFF → counter.QueryBalance |
| 活跃订单按 id | Counter 内存 | ms 级 | BFF → counter.QueryOrder |
| 订单列表 (open / terminal) | MySQL（trade-dump 投影） | 10-100ms | BFF → history.ListOrders |
| 成交历史 | MySQL | 10-100ms | BFF → history.ListTrades |
| 资金流水 | MySQL | 10-100ms | BFF → history.ListAccountLogs |
| 活跃触发单列表 | trigger 内存 + snapshot | ms 级 | BFF → trigger.ListTriggers (scope=active) |
| 已完结触发单列表 | MySQL（trade-dump 投影 triggers） | 10-100ms | BFF → history.ListTriggers (scope=terminal) |
| 触发单按 id | trigger first + history fallback | ms 级 | BFF → trigger.QueryTrigger → history.GetTrigger |
| K 线 / 深度 | BFF 本地 market-data cache（ADR-0038） | ms 级 | BFF REST 直答 |

所有走 MySQL 的查询都通过独立的 `history` 只读服务（ADR-0046）完成，
BFF 不持 MySQL 连接。history 建议指向只读副本，和主写 DB 隔离。

## 11. 快照机制

### 11.1 Counter 快照

Counter 在 ADR-0058（vshard 冷切）+ ADR-0060（消费异步化）+ ADR-0061（trade-dump 接管 snapshot）落地后的快照模型：

- **Counter 自身不写 snapshot**（ADR-0061 Phase B）。Counter 只在进程启动时 READ snapshot 做 state restore + trade-event consumer seek 定位
- **Snapshot 的唯一 writer 是 trade-dump 的 snap pipeline**（`trade-dump/internal/snapshot/pipeline`）
  - Per-vshard `ShadowEngine`（`trade-dump/internal/snapshot/shadow`）消费 counter-journal 独立 partition，单线程 Apply → 天然免 stop-the-world 锁（ADR-0060 M7 的 `SnapshotMu` 已随 Phase B 撤销）
  - 触发：距上次 snapshot ≥ `SnapshotInterval`（默认 10s）**或** 累计 apply ≥ `SnapshotEventCount`（默认 10000）
  - 内容：所有用户余额 + 活跃订单 + match_seq guard + transfer_id ring + 终态订单 ring（ADR-0062）+ `(te_partition, te_watermark)` + `journal_offset`
  - 存储：blob store（`--snapshot-backend=fs|s3`，key `vshard-NNN.pb`）
  - Save 成功即 commit — 没有 Kafka consumer group，下次重启从 snapshot 的 `journal_offset` seek 恢复（ADR-0048 同模式）

**数据流**：

```
  Counter ─write──▶ counter-journal (256 partition, 每 vshard 1 partition)
                         │
              ┌──────────┴──────────┐
              │ read                │ read
              ▼                     ▼
    trade-dump sql            trade-dump snap   ← 唯一 snapshot writer
    (MySQL 投影)              (ShadowEngine)
              │                     │
              ▼                     ▼
           MySQL               blob store
                                   │
                       ┌───────────┘
                       ▼ read (on Counter startup)
                  Counter restore
                  + trade-event seek
                  + catch-up journal (ADR-0060 §4.2)
```

Counter 启动的完整恢复流程（`counter/internal/worker/worker.go:Run`）：

1. `snapshot.Load(store, "vshard-NNN")` → `ShardSnapshot`
2. `snapshot.Restore` 填 state / sequencer / dedup
3. `catchUpJournal`：从 `snap.JournalOffset` 消费 counter-journal partition 到 HWM，对每条 event 调 `engine.ApplyCounterJournalEvent`（幂等）— 覆盖"snapshot 之后、crash 之前"publish 的那批事件
4. 启动 trade-event consumer，seek 到 `snap.Offsets` 里的 `te_watermark`
5. `close(ready)` → gRPC 开放流量

### 11.2 Match 快照

- 由 Match 备节点产生
- 触发条件：每 10,000 笔订单 或 每 60 秒
- 内容：每个 symbol 的 orderbook（买卖盘、所有活跃订单）+ 消费到的 `order-event offset`
- 存储：同上

### 11.2b Quote 快照（ADR-0036）

Quote 走"主单实例 + 本地 JSON snapshot + per-partition Kafka offset 原子推进"模式：

- 由**主**（单实例）每 30s 产生；内容 = per-symbol depth book + kline aggregators + engine emit seq + 每 partition offset
- **非 Kafka-journal**：Quote 状态不进 counter-journal，snapshot 是唯一 durability。HA 部署需要共享 mount（NFS / EFS）让 backup 读到
- **原子性**：snapshot Capture 和 engine 状态更新拿同一把锁，offset 和 state 永远一致 ↔ 重启从保存的 offset 续消费，不重不漏

### 11.2c Trigger 快照（ADR-0067）

Trigger 已经迁出"自己写 snapshot"的旧模式 —— 由 trade-dump 的 trig-snap pipeline 接管，与 Counter 同模型（ADR-0067 镜像 ADR-0061 Phase B）：

- **Trigger 自身不写 snapshot**。Trigger 只在进程启动时 READ snapshot 做 state restore
- **Snapshot 的唯一 writer 是 trade-dump 的 trig-snap pipeline**（`trade-dump/internal/snapshot/trigger/{pipeline,shadow}`）：消费 `trigger-event` 维护 `trigger.shadow.Engine`（pending / terminals / per-partition apply cursor / market-data offsets），周期 + 事件计数双触发 Capture → BlobStore（fixed key `trigger`）
- **启动恢复**双路径：
  - **Hot path**：调 trade-dump 的 `TakeTriggerSnapshot` gRPC，强制现产一份 LEO-aligned snapshot 直接 load（ADR-0067 M4；hot path 失败 / 超时 / 不可用 → 落 cold path）
  - **Cold path**：直接读 BlobStore 上 trade-dump 周期产出的 fixed-key snapshot（ADR-0067 M5）
- 配合 ADR-0048 + ADR-0061 同款"snapshot 嵌 offset → 重启 seek 续消费"原子性；trigger 在 `--snapshot-dir` 这一路对 BlobStore 来说是只读 handle

### 11.3 冷启动

ADR-0058 vshard 模型下，Counter 的冷启动是**每个 vshard 独立**的恢复（不再是"每个 shard 主备并行加载"）；Trigger 一个进程一个 engine 走同样的两条路径。

```
1. 进程启动并注册到 etcd (counter: /cex/counter/nodes/{nodeID})
2. 拿到自己负责的 vshard 列表 (counter) / 整体 (trigger / asset / quote)
3. 对每个 vshard / 整体执行：
     a. Hot path: 调 trade-dump TakeSnapshot / TakeTriggerSnapshot RPC →
        直接 load 返回的 LEO-aligned snapshot
        (counter ADR-0064 / trigger ADR-0067 M4)
     b. Cold path: 失败 / 超时 / 不可用 → 从共享 BlobStore 读最近一份周期 snapshot
        (counter ADR-0061 / trigger ADR-0067 M5)
     c. catch-up: 从 snapshot 嵌的 journal_offset 重播 counter-journal /
        trigger-event 到 HWM (counter ADR-0060 §4.2)
4. trade-event consumer 用 AdjustFetchOffsetsFn seek 到 snapshot 嵌的 offset
   (ADR-0048)
5. 标记 ready → 对外服务
```

千万用户场景下的 Counter 启动目标：< 1 分钟（每个 vshard worker 并行恢复，加上 hot path RPC 把延迟压在 trade-dump 那一边）。

## 12. 分片与弹性

### 12.1 Counter 分片（ADR-0058 vshard 模型）

Counter 走 **vshard cluster + Coordinator + 自动 failover + 手动 rebalance** 的模型，不是老式"固定 N shard，每 shard 一主一备"。

- **256 个虚拟分片（vshard）**：用 `user_id` hash `% 256` 算 vshard id（BFF 和 counter 共用同一个 hash 函数，路由确定性）
- **N 个 counter 节点**：每个节点登记到 `/cex/counter/nodes/{nodeID}`（带 lease），任意时刻 vshard → 节点的映射写在 `/cex/counter/assignments/vshard-NNN`
- **Coordinator**（任一 counter 当选 leader）负责 vshard 分配：用 HRW (Rendezvous Hashing) 把 vshard 均匀分到活节点；新节点加入只搬 ~1/N 的 vshard，不会全表洗牌
- **Failover 自动**：节点 etcd lease 过期 → coordinator 的 failover loop 触发 `sweepOrphans` 把它拥有的 vshard force-reassign 给活节点（epoch+1 同时 fence 老 owner 的 transactional producer）
- **Rebalance 手动**：新增节点不会触发自动迁移；运维用 `counter-migrate plan|rebalance|move` 工具串行迁。完整状态机（ACTIVE → MIGRATING → HANDOFF_READY → ACTIVE@new,ep+1）+ 操作步骤见 [runbook-counter.md §8](./runbook-counter.md#8-vshard-扩容--再平衡-adr-0058)
- **每个 vshard 一个 transactional.id**（`counter-vshard-NNN`），新 owner 接管时同 ID 调 `InitTransactions()` 自动 fence 老 owner（ADR-0017 / ADR-0058）
- **每个 vshard 在节点内是一个 VShardWorker**：state + sequencer + TxnProducer + 独立 trade-event consumer goroutine；消费异步化（ADR-0060：SubmitAsync + pendingList + advancer + TECheckpoint）

### 12.2 Match 分片

- 按 symbol 分组
- 热门 symbol（BTC/USDT、ETH/USDT）独占 shard
- 冷门 symbol 共享 shard
- 外部配置（etcd）:
  ```
  /cex/match/symbols/
    BTC-USDT: {shard: "match-0", trading: true, version: "v1.2"}
    DOGE-USDT: {shard: "match-5", trading: true, version: "v1.3"}
  ```
- 新增 symbol：etcd 加配置 → 对应 shard 加载空 book → `trading: true`
- symbol 迁移：**接受停机迁移**（5-30s 窗口）
  1. 源 shard `trading: false`
  2. 撤完活跃订单（或强撤）
  3. 源 shard 生成 final snapshot
  4. 目标 shard 加载（应为空）
  5. etcd 切归属 → `trading: true`

### 12.3 Push 分片

- 10 个 push 实例
- LB 按 `user_id hash % 10` sticky 路由 WS 连接
- 单实例承载 10-15 万连接
- 订阅关系本地内存维护

## 13. 灰度升级

### 13.1 Match 灰度（按 symbol）

- etcd 配置新版本 shard 负责冷门 symbol
- 观察指标无异常 → 逐步迁移热门 symbol 到新版本
- 回滚：反向迁移

### 13.2 Counter 灰度（按 shard）

- 单 shard 滚动升级流程：
  1. 升级 shard-N 的备节点
  2. 备 catch up → ready
  3. etcd 切 leader（手动脚本或自动）
  4. 老主降级为备 → 升级
  5. 老主 catch up → 完成
- shard-0 先升级，观察 24h → 推下一个

### 13.3 Event Schema 兼容性

- Protobuf：只加字段、不改 tag、不删字段
- 所有消费方必须向前+向后兼容

## 14. 可观测性

### 14.1 日志

- `zap` 结构化日志
- 默认字段：`time, level, service, shard, trace_id, user_id, msg`
- trace_id 贯穿整条链路（通过 RPC metadata 和 Kafka event header）
- MVP 不搭日志收集（本地 stdout / 文件）

### 14.2 指标（框架层默认）

所有服务白送以下 Prometheus 指标：

- `cex_rpc_requests_total{service,method,code}`
- `cex_rpc_duration_seconds{service,method}` (histogram)
- `cex_kafka_produce_total{topic,result}`
- `cex_kafka_consume_lag{topic,partition}`
- `cex_kafka_txn_abort_total{service}`
- `cex_snapshot_duration_seconds`
- Go runtime metrics

业务指标各服务自行叠加。

### 14.3 MVP 不做

- 告警规则
- Tracing（OpenTelemetry 预留接口）
- 日志收集管道

## 15. 限流

- BFF 层按 user_id / IP 做滑动窗口限流
- 实现：内存令牌桶 + 滑窗计数（单机），集群一致性非必须（MVP）

## 16. 代码组织

### 16.1 Multi-module Monorepo + Go Workspace

```
opentrade/
├── go.work                           # 开发期联动所有 module
├── go.work.sum
├── Makefile
├── README.md
├── buf.yaml / buf.gen.yaml           # protoc / connect-go 生成
│
├── api/                              # module: proto + 生成代码
│   ├── go.mod
│   ├── event/                        # event topic envelopes
│   │   ├── counter_journal.proto
│   │   ├── order_event.proto
│   │   ├── trade_event.proto
│   │   ├── trigger_event.proto
│   │   ├── asset_journal.proto
│   │   └── market_data.proto
│   ├── rpc/                          # service interfaces (counter / match / bff / trigger / asset / ...)
│   ├── snapshot/                     # snapshot wire formats (counter / trigger)
│   └── gen/                          # 生成的 *.pb.go / *.connect.go
│
├── pkg/                              # module: 公共库
│   ├── go.mod
│   ├── kafka/                        # franz-go 封装：事务 producer + EOS consumer
│   ├── election/                     # etcd lease 选主
│   ├── snapshot/                     # BlobStore (fs / s3) + counter & trigger 编解码
│   ├── connectx/                     # connect-go 中间件 (auth / metrics / 限流)
│   ├── logx/                         # zap 封装
│   ├── metrics/                      # Prometheus 封装 + middleware
│   ├── idgen/                        # 雪花 ID
│   ├── dec/                          # shopspring/decimal 封装
│   ├── etcdcfg/                      # etcd 配置 / SymbolConfig watcher
│   ├── shard/                        # user_id → vshard 共用 hash
│   └── ...
│
├── counter/                          # module: 柜台/清算 (ADR-0058 vshard 模型)
│   ├── go.mod
│   ├── cmd/
│   │   ├── counter/main.go           # vshard cluster member; ADR-0058 移除 legacy --total-shards
│   │   └── counter-migrate/          # ADR-0058 vshard 在线再平衡工具 (runbook-counter §8)
│   ├── engine/                       # 账户/订单状态机 + journal apply
│   │                                 #   (脱 internal, ADR-0061 M1; trade-dump shadow engine 复用)
│   ├── snapshot/                     # Capture/Restore wire
│   │                                 #   (脱 internal, ADR-0061 M1; trade-dump pipeline 产出该格式)
│   └── internal/
│       ├── clustering/               # ADR-0058 etcd 集群成员 + vshard 分配 + Coordinator
│       ├── dedup/                    # transfer_id ring (per-vshard)
│       ├── journal/                  # TxnProducer + trade-event consumer (per-vshard)
│       ├── metrics/                  # Prometheus 指标 (ADR-0060 M8)
│       ├── reconcile/                # MySQL vs 内存对账 (ADR-0008)
│       ├── sequencer/                # per-user FIFO + SubmitAsync (ADR-0060 M1)
│       ├── server/                   # connect-go server (CounterService / AssetHolder)
│       ├── service/                  # RPC → engine 适配 + trade event handler
│       ├── snapshot/                 # 启动加载 trade-dump 产出的 snapshot (ADR-0061 / 0064)
│       ├── symregistry/              # etcd SymbolConfig watcher
│       ├── tradedumpclient/          # trade-dump on-demand RPC client (ADR-0064 hot path)
│       └── worker/                   # VShardWorker (startup restore + 异步消费 + advancer)
│
├── match/                            # module: 撮合服务
│   ├── go.mod
│   ├── cmd/match/main.go
│   └── internal/
│       ├── orderbook/                # 内存 orderbook (价格档位 + 订单队列)
│       ├── engine/                   # 撮合逻辑
│       ├── journal/                  # Kafka producer/consumer
│       └── snapshot/
│
├── trigger/                          # module: 条件单 (ADR-0040~46 / 0067)
│   ├── go.mod
│   ├── cmd/trigger/main.go           # runPrimary + runElectionLoop (ADR-0042 cold standby)
│   └── internal/
│       ├── engine/                   # pending / OCO / trailing 状态机
│       ├── consumer/                 # market-data consumer (snapshot offset seek)
│       ├── counterclient/            # counter gRPC (PlaceOrder / Reserve / Release)
│       ├── journal/                  # trigger-event producer (ADR-0047)
│       ├── server/                   # connect-go server
│       ├── service/                  # gRPC → engine 适配
│       └── snapshot/                 # 启动加载 trade-dump 产出的 snapshot (ADR-0067 M5)
│
├── asset/                            # module: 资金钱包 (ADR-0065)
│   ├── go.mod
│   ├── cmd/asset/main.go
│   └── internal/
│       ├── engine/                   # funding-wallet 状态机
│       ├── holder/                   # AssetHolder (账户层 RPC client)
│       ├── journal/                  # asset-journal 写入
│       ├── metrics/                  # Prometheus
│       ├── saga/                     # 充提冻 saga
│       ├── server/                   # connect-go server
│       ├── service/                  # gRPC → engine 适配
│       └── store/                    # 持久化 (本地 + MySQL)
│
├── bff/                              # module: API 网关
│   ├── go.mod
│   ├── cmd/bff/main.go
│   └── internal/
│       ├── client/                   # counter / trigger / asset / history gRPC client
│       ├── clusterview/              # /cex/counter/assignments watcher
│       ├── marketcache/              # market-data 本地缓存 (ADR-0038)
│       ├── ratelimit/                # 限流
│       ├── rest/                     # REST handler
│       └── ws/                       # WebSocket gateway
│
├── push/                             # module: WebSocket 推送 (ADR-0022)
│   ├── go.mod
│   ├── cmd/push/main.go
│   └── internal/
│       ├── consumer/                 # counter-journal + market-data
│       ├── hub/                      # 连接 hub
│       ├── subscribe/                # 订阅关系
│       └── ws/                       # 协议帧
│
├── quote/                            # module: 行情聚合
│   ├── go.mod
│   ├── cmd/quote/main.go
│   └── internal/
│       ├── depth/                    # 深度增量
│       ├── kline/                    # K 线
│       └── trades/                   # 逐笔
│
├── trade-dump/                       # module: 持久化 + snapshot 生产 (sidecar)
│   ├── go.mod
│   ├── cmd/trade-dump/main.go        # --pipelines=sql,snap,trigger-snap (默认全跑)
│   └── internal/
│       ├── mysqlsink/                # Kafka → MySQL projection 管线
│       │   ├── consumer/             #   journal / trade / trigger Kafka 消费者
│       │   └── writer/               #   MySQL 幂等写入 (ADR-0008/23/28/47)
│       ├── snapshot/                 # ShadowEngine + 周期 capture
│       │   ├── counter/              #   counter snapshot pipeline + shadow (ADR-0061)
│       │   │   ├── pipeline/
│       │   │   └── shadow/
│       │   └── trigger/              #   trigger snapshot pipeline + shadow (ADR-0067)
│       │       ├── pipeline/
│       │       └── shadow/
│       └── snapshotrpc/              # TakeSnapshot / TakeTriggerSnapshot RPC (ADR-0064/67)
│
├── history/                          # module: 只读历史查询 (ADR-0046)
│   ├── go.mod
│   ├── cmd/history/main.go
│   └── internal/
│       ├── cursor/                   # opaque 分页游标 (base64(JSON))
│       ├── mysqlstore/               # orders / trades / account_logs / triggers 读层
│       └── server/                   # connect-go server + scope→statuses 展开
│
├── admin-gateway/                    # module: 运维网关
│   ├── go.mod
│   ├── cmd/admin-gateway/main.go
│   └── internal/
│       ├── counterclient/            # counter 内部调用
│       ├── rollout/                  # 灰度 / SymbolConfig 维护
│       └── server/                   # 内部 ops API
│
├── tools/
│   ├── tui/                          # 运维 TUI
│   ├── web/                          # 运维 Web
│   └── precision-cli/                # symbol precision 配置
│
├── deploy/
│   ├── docker/                       # docker-compose.yml: kafka / etcd / mysql / minio
│   ├── k8s/                          # 预留
│   └── scripts/                      # 切主 / 重做快照 / 迁移 symbol
│
└── docs/
    ├── architecture.md               # 本文件
    ├── runbook-counter.md            # Counter 运维 (含 §8 vshard 扩容/再平衡)
    ├── roadmap.md
    └── adr/                          # 决策记录 0001~
```

### 16.2 依赖关系

- `api`、`pkg` 为基础模块，不依赖其他内部 module
- 各服务 module 依赖 `api` + `pkg`，**不互相依赖**（module 边界）
- `trigger` 运行时通过 gRPC 调 Counter（和 BFF 一样，不引入代码依赖）
- `history` 只读 MySQL，不消费 Kafka、不调其他服务；BFF 通过 gRPC 调 history
- `go.work` 本地 replace，发布时用版本锁定

### 16.3 技术选型

| 类别 | 选型 |
|---|---|
| 语言 | Go 1.26+ |
| Kafka 客户端 | [twmb/franz-go](https://github.com/twmb/franz-go) |
| 配置/选主 | etcd v3（`go.etcd.io/etcd/client/v3`） |
| RPC | [connect-go](https://github.com/connectrpc/connect-go)（兼容 gRPC 协议；从 grpc-go 迁移） |
| 序列化 | Protobuf |
| 金额/价格 | `shopspring/decimal`（封装在 `pkg/dec`） |
| 日志 | `go.uber.org/zap` |
| 指标 | Prometheus (`prometheus/client_golang`) |
| 存储 | MySQL（账户/订单/成交/触发单）、fs / S3（快照 BlobStore）、内存 ring（dedup） |
| 容器化 | Docker + docker-compose（MVP），k8s（后续） |

## 17. MVP 分期计划

**已迁移到 [`docs/roadmap.md`](./roadmap.md)** —— 该文档维护已完成 / 计划中 / Backlog
三段状态，以及每个 MVP 对应的 commit 和 ADR 引用。

本架构文档只描述"应该长什么样"（目标架构），不再跟踪执行进度。

## 18. 测试策略

### 18.1 单元测试

- orderbook、engine、decimal、dedup 等纯逻辑 100% 覆盖
- Table-driven 用例

### 18.2 集成测试

- `deploy/docker/docker-compose.yml` 起 Kafka + etcd + MySQL + MinIO
- Go 测试用 `testcontainers-go` 或直接依赖 docker-compose up
- 每个服务有一个 `integration_test.go` 走端到端场景

### 18.3 性能基准

- `match`：单 symbol 撮合吞吐基准测试
- `counter`：下单受理吞吐与延迟基准
- 在 MVP-3 结束时跑一次，验证是否接近 20 万 TPS / 10ms P99

## 19. 远期方向（本文档不展开）

短期 MVP 路线和填坑项见 [`docs/roadmap.md`](./roadmap.md)。以下是**超出 MVP 视野**的
方向，列此留痕，不在近期排期：

- 风控前置
- 合约交易（本次仅现货）
- 决定性回放 / 双主撮合（若后续需要亚秒级切换再评估）
- 冷热数据分层（MySQL → 归档）
- 多机房容灾

---

## 附：核心决策摘要（详细见 [`docs/adr/`](./adr/README.md)）

1. **Kafka 即 Source of Truth**（[ADR-0001](./adr/0001-kafka-as-source-of-truth.md)）：不自建 WAL/Raft，复用 Kafka 副本能力。
2. **Counter HA**（[ADR-0002](./adr/0002-counter-ha-via-etcd-lease.md)）：etcd lease 选主，Kafka 事务 + transactional.id fencing。**已被 ADR-0058 cluster + vshard cold handoff 取代**（不再 1 主 1 备）。
3. **Counter ↔ Match 走 Kafka**（[ADR-0003](./adr/0003-counter-match-via-kafka.md)）：不走同步 RPC，解耦 + 异步。
4. **counter-journal 作为 Counter WAL**（[ADR-0004](./adr/0004-counter-journal-topic.md)）：备节点只 tail 这一个 topic。
5. **Kafka 事务原子写**（[ADR-0005](./adr/0005-kafka-transactions-for-dual-writes.md)）：Counter 主同时写 journal + order-event；不引入 dispatcher。
6. **快照由备打**（[ADR-0006](./adr/0006-snapshots-by-backup-node.md)）：主专注低延迟，备做快照零代价。**已被 ADR-0061 / ADR-0067 取代**：trade-dump 的 shadow pipeline 是 Counter / Trigger 快照的唯一生产者。
7. **下单异步语义**（[ADR-0007](./adr/0007-async-matching-result.md)）：API 返回"已受理"（3-7ms P99），撮合结果走 WS。
8. **trade-dump 旁路写 MySQL**（[ADR-0008](./adr/0008-sidecar-persistence-trade-dump.md)）：Counter/Match 不直写 MySQL。
9. **Match 按 symbol 分片**（[ADR-0009](./adr/0009-match-sharding-by-symbol.md)）：etcd 配置驱动，支持停机迁移。
10. **Counter 10 shard**（[ADR-0010](./adr/0010-counter-sharding-by-userid.md)）：按 user_id hash 固定分片。**已被 ADR-0058 vshard cluster 取代**（256 vshard + Coordinator + HRW，节点数解耦于 vshard 数）。
11. **Transfer 统一接口**（[ADR-0011](./adr/0011-counter-transfer-interface.md)）：deposit/withdraw/freeze/unfreeze 合一。
12. **Multi-module monorepo**（[ADR-0012](./adr/0012-multi-module-monorepo.md)）：go.work 管理，各服务独立 go.mod。
13. **技术选型**（[ADR-0013](./adr/0013-tech-stack-choices.md)）：franz-go、etcd-v3、shopspring/decimal、zap、Prometheus。
14. **改单 = 撤 + 新建**（[ADR-0014](./adr/0014-order-modify-as-cancel-new.md)）：Match 不原生支持 replace。
15. **clientOrderId 去重仅覆盖活跃订单**（[ADR-0015](./adr/0015-idempotency-at-counter.md)）：对齐 Binance/OKX。
16. **per-symbol 单线程撮合**（[ADR-0016](./adr/0016-per-symbol-single-thread-matching.md)）：无锁，确定性。
17. **Kafka transactional.id fencing**（[ADR-0017](./adr/0017-kafka-transactional-id-naming.md)）：按 shard 稳定命名（Counter 改 vshard 后形如 `counter-vshard-NNN`，ADR-0058）。
18. **Counter Sequencer**（[ADR-0018](./adr/0018-counter-sequencer-fifo.md)）：懒启动 per-user worker + channel FIFO。
19. **Match Sequencer**（[ADR-0019](./adr/0019-match-sequencer-per-symbol-actor.md)）：per-symbol 常驻 goroutine + channel FIFO。
20. **订单状态机**（[ADR-0020](./adr/0020-order-state-machine.md)）：内部 8 态 + 外部 6 态（Binance 风格）。
21. **Quote + market-data fan-out**（[ADR-0021](./adr/0021-quote-service-and-market-data-fanout.md)）：Quote 独立服务，Kafka 扇出。
22. **Push 分片 + sticky 路由**（[ADR-0022](./adr/0022-push-sharding-sticky-routing.md)）：按 user_id hash。
