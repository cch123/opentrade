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

- 限价（Limit）
- 市价（Market）
- IOC（Immediate-Or-Cancel）
- FOK（Fill-Or-Kill）
- Post-Only
- 止损（Stop / Stop-Limit）

### 3.2 交易能力

- 下单、撤单
- 改单 = 撤单 + 新建（失去价格时间优先级）
- 批量下单（同一 symbol 内）
- 自成交保护（STP）
- clientOrderId 幂等（可选字段）

### 3.3 MVP 不做

- 风控前置
- 鉴权 / API Key 管理（预留 middleware 挂载点）
- 钱包对接（充提链上交互）
- 费率 / VIP 等级 / 邀请返佣（Counter 暴露配置接口，MVP 用常数或 0）
- 告警
- 审计日志收集管道（只打 zap 结构化日志，收集另论）

## 4. 模块职责

| 模块 | 职责 | 不负责 |
|---|---|---|
| **BFF** | REST 下单/撤单/查询；WebSocket 网关；鉴权挂载点；滑动窗口限流 | 业务状态 |
| **counter** | 账户余额、冻结、解冻；sequencer（同用户串行）；clientOrderId 去重；订单生命周期状态；费率接口；`Transfer`（deposit/withdraw/freeze/unfreeze） | orderbook、撮合逻辑 |
| **match** | 内存 orderbook（per symbol 单线程）；撮合规则；成交事件生成 | 余额、权限、费率计算 |
| **push** | WebSocket 连接维持；订阅关系管理；私有数据（订单/账户）+ 公共行情扇出 | 行情计算、业务状态 |
| **quote**（旁路） | 消费 trade-event → 生成增量深度、逐笔、K 线 | 连接推送 |
| **trade-dump**（旁路） | 消费 counter-journal + trade-event → 幂等写 MySQL | 在线查询 |

### 4.1 定序组件（Sequencer）

**Counter 和 Match 对定序有不同要求，采用差异化实现。** 详见 [ADR-0018](./adr/0018-counter-sequencer-fifo.md)、[ADR-0019](./adr/0019-match-sequencer-per-symbol-actor.md)。

| 维度 | Counter Sequencer | Match Sequencer |
|---|---|---|
| 定序单元 | user_id | symbol |
| 数量级 | 百万级（动态活跃） | <2000（静态配置） |
| 实现 | 懒启动 per-user worker，30s idle 退出 | per-symbol 常驻 goroutine |
| FIFO 保证 | Go channel | Go channel |
| seq_id | shard 级单调（`atomic.Uint64`） | per-symbol 单调 |
| 内存稳态 | 1-10 MB | ~16 MB |

**关键原则**：**严格 FIFO 由 Go channel 语义保证；`sync.Mutex` 不是定序组件**（Go 的 mutex 允许新到 goroutine 在一定条件下插队，不能用作严格定序）。

## 5. 核心架构图

```
         ┌──── etcd (选主 + 配置: match shard 映射, counter 路由) ────┐
         │                                                             │
    ┌─ BFF (无状态, N 实例) ─────────────────────────────────┐
    │ REST + WS, 鉴权挂载点, 滑窗限流                         │
    └──┬───────────────────────────────────────┬─────────────┘
       │ user hash gRPC                         │ WS sticky (user hash)
       ▼                                        ▼
  ┌─ counter (10 shard × 一主一备) ───┐      ┌── push (10 shard) ──┐
  │ 主: 内存校验 → Kafka 事务         │      │ WS 连接 + 订阅过滤   │
  │     (counter-journal + order-event)│      └────────▲────────────┘
  │ 备: tail journal,打快照 → S3      │               │
  │ Transfer / PlaceOrder / Cancel    │               │
  └─────┬─────────────────────────▲───┘               │
        │ 事务写                   │ tail 结算         │
        ▼                          │                   │
  ┌─ Kafka Cluster (3 broker, ISR≥2) ─────────────────┤
  │  counter-journal  (user 分区)                     │
  │  order-event      (symbol 分区)                   │
  │  trade-event      (symbol 分区)                   │
  │  wallet-event     (user 分区, 未来)               │
  │  market-data      (symbol 分区)                   │
  └─────┬───────────────────────────────────────┬─────┘
        │ (per-symbol partition)                 │
        ▼                                        │
  ┌─ match (N shard × 一主一备) ──┐              │
  │ 主: 消费 order-event → 撮合     │              │
  │     → Kafka 事务产出 trade-event │              │
  │ 备: tail 同步状态              │              │
  │ 快照 10k 笔 / 1min → S3       │              │
  └──┬────────────────────────────┘              │
     │ trade-event                                │
     └──────┬─────────────────────────────────────┤
            │                                     │
  ┌─ quote ─┼───────┐  ┌── trade-dump ─┐          │
  │ 深度/K线│       │  │ 幂等写 MySQL  │          │
  └────┬────┘       │  └───────────────┘          │
       │ market-data│                              │
       └────────────┴──────────────────────────────┘
                                                   ▼
                                                  push
```

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
- `(event_type, seq_id)` 或 `(topic, partition, offset)` 作为幂等键
- consumer offset 提交与业务写入的原子性：
  - **Counter 主**：用 Kafka EOS 事务（`sendOffsetsToTransaction`）
  - **trade-dump**：MySQL UNIQUE 索引 + `INSERT ... ON DUPLICATE KEY UPDATE`
  - **Match**：按 `order_id` 业务去重（orderbook 自带）

### 6.5 订单状态机

采用 **Binance 风格：内部 8 态 + 外部 6 态**。详见 [ADR-0020](./adr/0020-order-state-machine.md)。

**内部 8 态**（Counter 权威，写 journal）：
`PENDING_NEW` / `NEW` / `PARTIALLY_FILLED` / `FILLED` / `PENDING_CANCEL` / `CANCELED` / `REJECTED` / `EXPIRED`

**外部 6 态**（API / WS 对用户暴露）：
`NEW` / `PARTIALLY_FILLED` / `FILLED` / `CANCELED` / `REJECTED` / `EXPIRED`

PENDING_NEW 和 PENDING_CANCEL 仅在 Counter 内部可见（用于监控、排查、一致性校验），对外呈现为相邻的非 pending 态。

**clientOrderId 幂等语义**（详见 [ADR-0015](./adr/0015-idempotency-at-counter.md)）：
- 仅对**活跃订单**去重（NEW / PARTIALLY_FILLED / PENDING_* 期间占用）
- 订单进入终态后，同一 clientOrderId 可复用（对齐 Binance/OKX）
- 无需独立 dedup 表或 Redis TTL

## 7. Kafka Topic 设计

| Topic | 生产者 | 分区 key | 主要消费者 | 内容 |
|---|---|---|---|---|
| `counter-journal` | Counter 主 | user_id | Counter 备、push、trade-dump | 所有账户/冻结变更的规范化流水（Counter 的 WAL） |
| `order-event` | Counter 主 | symbol | Match | OrderPlaced / OrderCancel |
| `trade-event` | Match | symbol | Counter 主、quote、trade-dump | TradeMatched / OrderCancelled / OrderRejected |
| `wallet-event` | wallet-service（未来） | user_id | Counter 主 | DepositConfirmed / WithdrawCompleted |
| `market-data` | quote | symbol | push | 增量深度 / K线 / 逐笔 |

### 7.1 Kafka 配置要求

- 3+ broker 集群
- `replication.factor=3`, `min.insync.replicas=2`
- `acks=all`（重要 topic：counter-journal、trade-event）
- `enable.idempotence=true`（所有 producer）
- 事务 producer：每个 Counter 主 shard / 每个 Match 主 shard 一个稳定 `transactional.id`
- 事务消费：`isolation.level=read_committed`

### 7.2 transactional.id 规划

```
counter-shard-{0..N-1}-main
match-shard-{symbol-group}-main
```

通过 etcd 绑定 `shard_id → transactional.id`，主备切换时新主用同一 id 调 `InitTransactions()` → 自动 fence 老主。

## 8. 一致性模型

### 8.1 强一致性点

- 用户余额变更：Counter 主内存 + counter-journal Kafka（acks=all）单调递进
- 同一用户的操作顺序：按 user_id 路由到同 Counter shard，shard 内 per-user lock 串行
- 同一 symbol 的撮合顺序：按 symbol 路由到同 Match shard，Match 内单线程
- Counter 下单的原子性：Kafka 事务包住 journal + order-event 两次写
- Counter 消费 trade-event 结算：EOS 事务（consume + produce journal + commit offset 原子）

### 8.2 最终一致性点

- Counter 备节点：tail counter-journal，落后主 ms 级
- trade-dump → MySQL：延迟 100ms - 秒级
- quote → market-data：延迟 ms - 百毫秒级
- push 私有数据推送：延迟 ms 级

### 8.3 Source of Truth

**Kafka 是唯一权威事件流**：
- 所有模块状态都可以从 Kafka 历史事件回放重建
- 快照只是回放起点的优化，不是必需
- MySQL 只是查询投影，丢失可重建

## 9. HA 与故障处理

### 9.1 主备选举

- Counter/Match 每个 shard 一主一备
- 通过 **etcd lease** 选主（`pkg/election`）
- Lease TTL：10 秒（可调）；主定期续约
- Lease 过期 → 备抢锁 → fence 老主（Kafka transactional.id + epoch）

### 9.2 Counter 主崩溃

```
T0: 主正常处理
T1: 主 crash（进程挂 / 机器宕机 / 网络分区）
T2: etcd lease 过期（10s）
T3: 备抢到 lease → 升主
T4: 新主:
      - 以同 transactional.id 调 InitTransactions() (fence 老主的未完成事务)
      - 继续 tail counter-journal 到最新 offset (已在追, 几乎无延迟)
      - 标记 ready → 对外提供服务
T5: 老主（若还活着）尝试 Kafka 写时被 fence 拒绝 → 自杀或降级为备
```

RTO：10-15s

### 9.3 Match 主崩溃

同上流程。新主从最近 snapshot + order-event 增量回放重建 orderbook。

### 9.4 Kafka 不可用

- 所有 Counter/Match 主退化为 degraded，拒绝新写入
- 用户侧收到 503 或明确错误
- Kafka 修复后自动恢复（producer 重试 / consumer 续消费）
- 期间数据不丢（用户看到失败 → 可重试）

### 9.5 丢数据场景分析

| 场景 | 数据影响 |
|---|---|
| Counter 单次 Kafka 写失败 | 用户看到失败，无副作用 |
| Counter 写 journal 成功后 crash | 备升主后数据在 journal，用户 clientOrderId 重试命中去重返回 |
| Kafka 集群全挂 | 无数据丢失（服务不可用），恢复后继续 |
| 单个 broker 挂 | 无影响（ISR ≥ 2，acks=all） |
| 两个 broker 挂 | 写入阻塞（ISR < min），无数据丢失 |
| push 挂 | WS 断连，客户端重连+主动拉状态；可能错过几条推送 |

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
- 按 `UNIQUE (event_type, seq_id)` 幂等写入
- MySQL schema（初版）：
  - `accounts(user_id, asset, available, frozen, updated_at, seq_id)`
  - `orders(order_id, user_id, symbol, side, type, price, qty, filled_qty, status, created_at, updated_at)`
  - `trades(trade_id, symbol, maker_order_id, taker_order_id, price, qty, taker_side, ts)`
  - `account_logs(seq_id, user_id, asset, delta, balance_after, biz_type, biz_ref, ts)` — journal 流水镜像
  - 可按 user_id / symbol 分库分表（MVP 先单库）

### 10.3 读路径

| 查询 | 来源 | 延迟 |
|---|---|---|
| 当前余额 | Counter 内存（gRPC） | ms 级 |
| 活跃订单 | Counter 内存 | ms 级 |
| 历史订单（>24h） | MySQL | 10-100ms |
| 成交历史 | MySQL | 10-100ms |
| K 线 / 深度 | quote 内存 + Redis 缓存 | ms 级 |

## 11. 快照机制

### 11.1 Counter 快照

- **由 Counter 备节点产生**（主专注低延迟服务）
- 触发条件：每 10,000 笔事件 或 每 60 秒（取先到）
- 内容：所有用户余额 + 活跃订单 + dedup 表 + 消费到的 `(counter-journal offset, trade-event offset)`
- 存储：本地 EFS 写入 → 异步 rsync / 上传到 S3
- 文件命名：`counter-shard-{N}-{offset}-{ts}.snap`
- 保留：近 7 天

### 11.2 Match 快照

- 由 Match 备节点产生
- 触发条件：每 10,000 笔订单 或 每 60 秒
- 内容：每个 symbol 的 orderbook（买卖盘、所有活跃订单）+ 消费到的 `order-event offset`
- 存储：同上

### 11.3 冷启动

```
1. 加载最近 snapshot → (state, offset)
2. 从 offset 开始 tail 对应 Kafka topic
3. 回放到 latest offset
4. 注册到 etcd (ready 状态)
5. 对外服务
```

千万用户场景下的 Counter 冷启动目标：< 1 分钟（按 shard 并行加载）。

## 12. 分片与弹性

### 12.1 Counter 分片

- **10 个 shard**（固定，MVP 不支持 resharding）
- BFF 按 `user_id hash % 10` 路由
- 每 shard 主备 2 节点 → 共 20 节点（MVP 可缩减为 1 shard + 1 副本做验证）

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
├── README.md
├── .gitignore
│
├── api/                              # module: proto + 生成代码
│   ├── go.mod
│   ├── buf.yaml / buf.gen.yaml       # 或 Makefile 驱动 protoc
│   ├── event/
│   │   ├── counter_journal.proto
│   │   ├── order_event.proto
│   │   ├── trade_event.proto
│   │   ├── wallet_event.proto
│   │   └── market_data.proto
│   ├── rpc/
│   │   ├── counter.proto
│   │   ├── match.proto
│   │   └── bff.proto
│   └── gen/                          # 生成的 pb.go / grpc.pb.go
│
├── pkg/                              # module: 公共库
│   ├── go.mod
│   ├── kafka/                        # franz-go 封装: 事务 producer, EOS consumer
│   ├── election/                     # etcd lease 选主
│   ├── snapshot/                     # 快照 save/load (本地 + S3 接口)
│   ├── logx/                         # zap 封装
│   ├── metrics/                      # prometheus 封装 + middleware
│   ├── idgen/                        # 雪花 ID
│   ├── decimal/                      # shopspring/decimal 封装
│   └── config/                       # 配置加载 (etcd + 本地 yaml)
│
├── counter/                          # module: 柜台服务
│   ├── go.mod
│   ├── cmd/counter/main.go
│   └── internal/
│       ├── server/                   # gRPC server
│       ├── engine/                   # 账户状态机 (余额/冻结/订单表)
│       ├── journal/                  # Kafka producer (事务)
│       ├── tradeconsumer/            # 消费 trade-event (EOS)
│       ├── snapshot/
│       └── router/                   # per-user lock + sequencer
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
├── bff/                              # module: API 网关
│   ├── go.mod
│   ├── cmd/bff/main.go
│   └── internal/
│       ├── rest/                     # REST handler
│       ├── ws/                       # WebSocket gateway
│       ├── client/                   # counter/match gRPC client
│       ├── ratelimit/
│       └── auth/                     # middleware 挂载点
│
├── push/                             # module: WS 推送
│   ├── go.mod
│   ├── cmd/push/main.go
│   └── internal/
│       ├── ws/
│       ├── subscribe/                # 订阅关系
│       └── consumer/
│
├── quote/                            # module: 行情
│   ├── go.mod
│   ├── cmd/quote/main.go
│   └── internal/
│       ├── depth/                    # 深度增量
│       ├── kline/                    # K 线
│       └── trades/                   # 逐笔
│
├── trade-dump/                       # module: 持久化
│   ├── go.mod
│   ├── cmd/trade-dump/main.go
│   └── internal/
│       ├── consumer/
│       └── writer/                   # MySQL 幂等写入
│
├── deploy/
│   ├── docker/
│   │   └── docker-compose.yml        # 本地: kafka + etcd + mysql + minio
│   ├── k8s/                          # 预留
│   └── scripts/                      # 切主 / 重做快照 / 迁移 symbol
│
└── docs/
    └── architecture.md               # 本文件
```

### 16.2 依赖关系

- `api`、`pkg` 为基础模块，不依赖其他内部 module
- 各服务 module 依赖 `api` + `pkg`，不互相依赖
- `go.work` 本地 replace，发布时用版本锁定

### 16.3 技术选型

| 类别 | 选型 |
|---|---|
| 语言 | Go 1.22+ |
| Kafka 客户端 | [twmb/franz-go](https://github.com/twmb/franz-go) |
| 配置/选主 | etcd v3（`go.etcd.io/etcd/client/v3`） |
| RPC | gRPC |
| 序列化 | Protobuf |
| 金额/价格 | `shopspring/decimal` |
| 日志 | `go.uber.org/zap` |
| 指标 | Prometheus (`prometheus/client_golang`) |
| 存储 | MySQL（账户/订单/成交）、S3/EFS（快照）、Redis（dedup/缓存） |
| 容器化 | Docker + docker-compose（MVP），k8s（后续） |

## 17. MVP 分期计划

| 阶段 | 内容 | 验收标准 |
|---|---|---|
| **MVP-0** | 脚手架：monorepo + go.work + proto + pkg 基础 + docker-compose | `go build ./...` 全通过；docker-compose up 起依赖 |
| **MVP-1** | Match 单机：消费 order-event → 撮合 → 产出 trade-event；快照+恢复 | 单 symbol 灌 1000 条订单撮合正确；kill -9 重启状态不丢 |
| **MVP-2** | Counter 单机：Transfer 接口 + counter-journal + 快照 | deposit/withdraw/freeze/unfreeze 正确；重启恢复 |
| **MVP-3** | Counter ↔ Match 联动：PlaceOrder / Cancel；Kafka 事务；成交结算 | 两用户对敲一笔成交，双方余额正确；端到端 P99 延迟验证 |
| **MVP-4** | BFF (REST)：下单/撤单/查询 + 滑窗限流 | curl 下单成交 |
| **MVP-5** | Push (WS)：私有频道 + 公共频道 + sticky 路由 | 浏览器连 WS 下单后实时看到变化 |
| **MVP-6** | Quote：增量深度 + 1min K 线 + 逐笔 | WS 订阅深度能看到跳动 |
| **MVP-7** | trade-dump → MySQL：幂等写 | 历史订单/成交能查 |
| **MVP-8** | HA：etcd 选主 + 主备切换脚本 | kill 主节点后 10-20s 恢复服务 |
| **MVP-9** | 分片：Counter 10 shard + Match per-symbol | 路由、迁移、灰度流程打通 |

每个 MVP 完成标志：**单元测试 + 集成测试（docker-compose 起依赖） + demo 脚本**。

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

## 19. 待定 / 后续

- 风控前置
- 合约交易（本次仅现货）
- 撮合撮合决定性回放 / 双主（若后续需要亚秒级切换再评估）
- 订单状态的对账机制（Counter 内存 vs MySQL 聚合 vs Kafka 回放）
- 冷热数据分层（MySQL → 归档）
- 多机房容灾

---

## 附：核心决策摘要（详细见 [`docs/adr/`](./adr/README.md)）

1. **Kafka 即 Source of Truth**（[ADR-0001](./adr/0001-kafka-as-source-of-truth.md)）：不自建 WAL/Raft，复用 Kafka 副本能力。
2. **Counter HA**（[ADR-0002](./adr/0002-counter-ha-via-etcd-lease.md)）：etcd lease 选主，Kafka 事务 + transactional.id fencing。
3. **Counter ↔ Match 走 Kafka**（[ADR-0003](./adr/0003-counter-match-via-kafka.md)）：不走同步 RPC，解耦 + 异步。
4. **counter-journal 作为 Counter WAL**（[ADR-0004](./adr/0004-counter-journal-topic.md)）：备节点只 tail 这一个 topic。
5. **Kafka 事务原子写**（[ADR-0005](./adr/0005-kafka-transactions-for-dual-writes.md)）：Counter 主同时写 journal + order-event；不引入 dispatcher。
6. **快照由备打**（[ADR-0006](./adr/0006-snapshots-by-backup-node.md)）：主专注低延迟，备做快照零代价。
7. **下单异步语义**（[ADR-0007](./adr/0007-async-matching-result.md)）：API 返回"已受理"（3-7ms P99），撮合结果走 WS。
8. **trade-dump 旁路写 MySQL**（[ADR-0008](./adr/0008-sidecar-persistence-trade-dump.md)）：Counter/Match 不直写 MySQL。
9. **Match 按 symbol 分片**（[ADR-0009](./adr/0009-match-sharding-by-symbol.md)）：etcd 配置驱动，支持停机迁移。
10. **Counter 10 shard**（[ADR-0010](./adr/0010-counter-sharding-by-userid.md)）：按 user_id hash 固定分片。
11. **Transfer 统一接口**（[ADR-0011](./adr/0011-counter-transfer-interface.md)）：deposit/withdraw/freeze/unfreeze 合一。
12. **Multi-module monorepo**（[ADR-0012](./adr/0012-multi-module-monorepo.md)）：go.work 管理，各服务独立 go.mod。
13. **技术选型**（[ADR-0013](./adr/0013-tech-stack-choices.md)）：franz-go、etcd-v3、shopspring/decimal、zap、Prometheus。
14. **改单 = 撤 + 新建**（[ADR-0014](./adr/0014-order-modify-as-cancel-new.md)）：Match 不原生支持 replace。
15. **clientOrderId 去重仅覆盖活跃订单**（[ADR-0015](./adr/0015-idempotency-at-counter.md)）：对齐 Binance/OKX。
16. **per-symbol 单线程撮合**（[ADR-0016](./adr/0016-per-symbol-single-thread-matching.md)）：无锁，确定性。
17. **Kafka transactional.id fencing**（[ADR-0017](./adr/0017-kafka-transactional-id-naming.md)）：按 shard 稳定命名。
18. **Counter Sequencer**（[ADR-0018](./adr/0018-counter-sequencer-fifo.md)）：懒启动 per-user worker + channel FIFO。
19. **Match Sequencer**（[ADR-0019](./adr/0019-match-sequencer-per-symbol-actor.md)）：per-symbol 常驻 goroutine + channel FIFO。
20. **订单状态机**（[ADR-0020](./adr/0020-order-state-machine.md)）：内部 8 态 + 外部 6 态（Binance 风格）。
21. **Quote + market-data fan-out**（[ADR-0021](./adr/0021-quote-service-and-market-data-fanout.md)）：Quote 独立服务，Kafka 扇出。
22. **Push 分片 + sticky 路由**（[ADR-0022](./adr/0022-push-sharding-sticky-routing.md)）：按 user_id hash。
