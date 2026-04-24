# ADR-0040: 触发单独立服务（stop-loss / take-profit）

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0001, 0008, 0010, 0021, 0025, 0027, 0035, 0036, 0038

## 背景 (Context)

到 MVP-13/14 为止，OpenTrade 只支持 LIMIT 和 MARKET 两种订单类型
（MARKET 有 BFF 滑点保护路径，ADR-0035）。客户一个明确的缺口是**触发单**：
把订单的激活条件从 "用户主动下发" 变成 "价格穿越阈值"。常见形态：

- **STOP_LOSS**：价格朝不利方向动到阈值 → 按市价强制止损
- **STOP_LOSS_LIMIT**：同上，但最终下 LIMIT 而不是 MARKET（限定最差成交价）
- **TAKE_PROFIT / TAKE_PROFIT_LIMIT**：价格朝有利方向到阈值 → 锁利
- 方向矩阵：sell/buy 各有四种语义（见下）

ADR-0035 §Z 明确拒绝过"让 Counter 订阅行情"，因为 Counter 的定位是**纯账
户状态机**。触发单的触发逻辑需要一个新的 **行情观察者**。

## 决策 (Decision)

### 1. 独立 `trigger` 服务，订阅 Quote 的 `market-data`

新增微服务 `trigger`：

- gRPC 服务 `TriggerService.{Place,Cancel,Query,List}`；外部通过 BFF
  REST `/v1/trigger*` 到达
- 消费 Quote 发布的 `market-data`（仅关心 `PublicTrade`）——不订
  `trade-event`，避免让 Match 的 topic 成为 "结算 + 业务触发" 双职能
  （ADR-0021 定调 Quote 是下游市场数据 fan-out 的唯一源）
- 触发时调用 Counter gRPC `PlaceOrder`（通过 `pkg/shard.Index` 按
  user_id 路由到对应 shard，mirror BFF 的做法）
- 本地 JSON snapshot + per-partition offset（照抄 Quote ADR-0036 的模式，
  包括 `AdjustFetchOffsetsFn` 加载保存 offset）

### 2. 触发矩阵

以 `stop_price` 为阈值，触发条件：

| side | type              | 触发条件                    |
|------|-------------------|-----------------------------|
| sell | STOP_LOSS(_LIMIT) | `last_price <= stop_price` |
| sell | TAKE_PROFIT(_LIMIT) | `last_price >= stop_price` |
| buy  | STOP_LOSS(_LIMIT) | `last_price >= stop_price` （break-in） |
| buy  | TAKE_PROFIT(_LIMIT) | `last_price <= stop_price` （mean-reversion） |

触发后：

- `STOP_LOSS` / `TAKE_PROFIT` → 内部下 MARKET 单（sell 带 qty；buy 必须带
  quote_qty，对齐 Counter 的 ADR-0035）
- `STOP_LOSS_LIMIT` / `TAKE_PROFIT_LIMIT` → 内部下 LIMIT 单，`price =
  limit_price`，`tif` 来自原始请求（默认 GTC）

### 3. 资金不预留（MVP-14a）

本 ADR 先做 **不 freeze** 的版本：

- 触发单下发时，Counter 余额不变
- 触发时下内部 PlaceOrder，Counter 按常规校验 + freeze；如果余额不足，
  Counter 返回 `FailedPrecondition`，trigger 把该条置为
  `REJECTED` + `reject_reason`

用户体验代价：止损单触发瞬间发现没钱 → 仓位继续向坏方向走。**MVP 明确接受
此代价，换取实现简单 + 零 Counter 侧改动**。MVP-14b 会补 freeze 流（见
Future Work）。

### 4. 幂等 + 重放安全

- `client_trigger_id` 作为用户侧幂等键：重复 Place 返回原 id +
  `accepted=false`
- 内部触发 Counter 时，`client_order_id = "trig-<id>"` 固定派生 —— 重启
  replay 或触发竞态 (同一 PublicTrade 扫到 → fire) 时 Counter 的 dedup 归
  一到同一订单
- 引擎状态转换不靠乐观标记：从 PENDING 拉出 → 释锁 → 调 Counter → 回锁
  commit。中间 crash：restart 时 state 仍是 PENDING，下一条 PublicTrade
  会再 fire；Counter 看到同样的 `trig-<id>` 客户端订单 id 走 dedup 返回上
  次的 `order_id`；最终状态对齐

### 5. 单实例 + 冷备

MVP-14a 是**单实例 + 本地 snapshot**。etcd 选主 + cold standby（类似
Counter/Match 的 ADR-0031 方案）留给 14c。代价：冷启动几秒 + 触发期间断
服。

## 备选方案 (Alternatives Considered)

### A. 把条件逻辑塞进 Counter
- 最低延迟（Counter 已经拿订单 + 账户）
- 但是让 Counter 成为行情订阅者，违反 ADR-0035 §Z 和 ADR-0001（Counter
  是账户状态机，不是行情观察者）
- 拒绝

### B. 把条件逻辑塞进 Match
- Match 已经在交易主链路上；触发 = 撮合内生
- 代价：Match 需要接受 "pending order" 的 new 类型，trade-event 协议
  复杂化；多 symbol 触发需要跨 symbol 协调（Match 是 per-symbol 单线
  程，ADR-0016）
- 拒绝：触发几乎是"观察 + 下单"，不涉及撮合内核，跟 Match 解耦最干净

### C. 订 `trade-event` 而不是 `market-data`
- 绕过 Quote 的一跳（~ms）
- 但：`trade-event` 还带 Accepted/Rejected/Cancelled/Expired 四种与触发
  无关的 payload，要 filter；让 Match 的 topic 变成 "既结算又业务触发"
- Quote 的 PublicTrade 已经 pre-filtered 到 (symbol, price, ts)；直接用
- 选 market-data（和 Push / BFF-marketcache 同模式，ADR-0038）

### D. 先做 freeze（MVP-14 一次性交付）
- UX 好很多（下单即锁资金）
- 但要么扩 Counter 的 transfer proto（新 reserve-for-trigger type +
  biz_ref_id 语义），要么让 trigger 发 FREEZE transfer 再 fire 前 UNFREEZE
  + PlaceOrder（两步有竞态：竞速的订单可能吃光余额）
- **scope 太大**；先把"触发链路"这条骨干跑通，冻资金作为 MVP-14b 迭代

### E. 把触发统一丢 Counter 去轮询
- 即：Counter 拉 market-data 周期性扫 pending triggers
- 本质上 = A，同样拒绝

## 理由 (Rationale)

- **职责分离**：Counter 永远不观察行情；Quote 只发布；trigger 是纯粹
  的 "stateful observer + dispatcher"
- **Kafka 作为权威**：snapshot + offset 保证重启幂等；跟 Quote 一模一样
  （ADR-0036）可供运维 mental model 复用
- **BN 对标**：API 形状（type + stop_price + optional limit_price + tif）
  和 BN spot REST `/api/v3/order` 的 `type=STOP_LOSS / STOP_LOSS_LIMIT /
  TAKE_PROFIT / TAKE_PROFIT_LIMIT` 对齐，现成 SDK 迁移成本低
- **Counter 零改动**：触发时用的就是标准 PlaceOrder 入口；Counter 不知道
  调用方是用户还是 trigger —— 最小耦合

## 影响 (Consequences)

### 正面

- 用户终于能下止损单；补齐 BN 基础单类型
- 新服务完全自包含（proto + cmd + internal + tests），不动 Counter /
  Match / Quote / Push
- REST 侧一致：`/v1/trigger` 跟 `/v1/order` 同风格，client_trigger_id
  也是 `client_order_id` 的翻版
- 本地 snapshot 让重启不丢 pending 触发单（相比"每次从 earliest 重扫"）

### 负面 / 代价

- 新服务要运维一根——日志、监控、部署 pipeline（触发单量级通常远小于
  真实订单，压力本就很低）
- 无 freeze 的 UX 折扣：极端行情下触发瞬间发现余额已耗尽；MVP 文档说明
- 触发延迟 = Match→Quote→market-data partition fetch→trigger 引擎
  处理→Counter PlaceOrder ≈ 数 ms 到十几 ms；微秒级套利场景不适合（但
  OpenTrade 面向散户交易，可以接受）
- 单实例：选主 cold-standby 留给 MVP-14c

### 中性

- 触发后不再订阅 order-event 跟踪结果 —— 用户通过 WS / `/v1/order/{id}`
  自行观察内部订单；trigger 只负责 "已成功下发内部订单" 状态

## 实施约束 (Implementation Notes)

### Proto

`api/rpc/trigger/trigger.proto`：

- enum `TriggerType`: STOP_LOSS / STOP_LOSS_LIMIT / TAKE_PROFIT /
  TAKE_PROFIT_LIMIT
- enum `TriggerStatus`: PENDING / TRIGGERED / CANCELED / REJECTED
- 四个 RPC：Place / Cancel / Query / List
- `Trigger` 消息包含 stop_price、limit_price、qty、quote_qty、tif、
  placed_order_id、reject_reason

### 模块布局

```
trigger/
  cmd/trigger/main.go            # 主进程
  internal/engine/                   # 状态 + 触发规则 + 持久化 hooks
  internal/snapshot/                 # JSON 快照
  internal/consumer/                 # market-data 消费（Quote 同款 AdjustFetchOffsetsFn）
  internal/service/                  # engine ↔ gRPC 适配层
  internal/server/                   # gRPC server 注册 + error mapping
  internal/counterclient/            # 分片 Counter 客户端（pkg/shard）
```

### Engine 并发模型

- 单 mu 覆盖 pending / terminals / byClient / lastPrice / offsets
- `HandleRecord` 拿锁 → 更新 offset + 扫 pending → 收集要 fire 的 ids →
  释锁 → 对每个 id 调 `tryFire`（各自释/拿锁一次）
- Counter RPC 永远在锁外调
- `ShouldFire` 是纯函数，单测覆盖所有 side × type 组合

### BFF 集成

- `bff/internal/client/trigger.go`：narrow 客户端 + Dial
- `bff/internal/rest/trigger.go`：四个 handler + JSON ↔ proto 转换
- `bff/cmd/bff/main.go`：`--trigger` flag 指定 gRPC 端点；空则
  `/v1/trigger*` 返回 503（和 ADR-0038 同策略）

### 失败场景

| 情况 | 结果 |
|---|---|
| Counter 不可达 / timeout | 触发单 → REJECTED + reject_reason |
| Quote 挂 / 无 PublicTrade 流入 | 触发单一直 PENDING（不触发）——和 Push / BFF-marketcache 共享故障 mode |
| trigger 崩溃 | 下一次启动从 snapshot + offset 续；pending 还在，未 commit 的 triggered 会因 Counter dedup 收敛 |
| snapshot 磁盘满 | log error，continue；下一次 tick 重试；最坏情况 = 崩溃时丢最近 n 秒 pending |
| 同一 price 多条 triggers 同时 fire | 都过 RPC；Counter 各自校验；互不影响（每个有独立 `trig-<id>` client order id） |

### 默认 flag

```
trigger:
  --grpc-addr          :8082
  --brokers            localhost:9092
  --counter-shards     localhost:8081           # 逗号分隔；按 shard-id 顺序
  --market-topic       market-data
  --snapshot-dir       ./data/trigger
  --snapshot-interval  30s
  --terminal-history   1000                     # 保留的 triggered/canceled/rejected 数
  --idgen-shard        900                      # snowflake shard，错开 counter 0..99

bff:
  --trigger        ""                       # 空则 503
```

## 未来工作 (Future Work)

- **MVP-14b**：资金预留。最干净的实现是在 Counter 里新增
  `Reserve(user, asset, amount, ref_id) → reservation_id` + `Release`
  RPC；trigger 在 Place 时 Reserve，触发前 Release（或 PlaceOrder
  带 `use_reservation_id` 原子替换）；cancel/expire 走 Release
- **MVP-14c**：HA。etcd leader election + cold standby（照搬 ADR-0031 的
  Counter/Match 模型）
- **OCO**（One-Cancels-Other）：两个挂钩在一起的 triggers，任一 fire
  则另一个自动 CANCELED；独立 ADR，等 MVP-14a 上线观察后再做
- **Trailing stop**：stop_price 随 last_price 按 delta / percentage 漂移；
  需要在 engine 里加周期性重估
- **expiry**：`expires_at_unix_ms` 字段 → 自动 EXPIRED 状态
- **多实例 + partition 过滤**：按 user_id hash 分片，同 Push 的 ADR-0033
  模式

## 参考 (References)

- ADR-0021: Quote 服务与 market-data fan-out
- ADR-0025: Quote 内部结构与 offset 回放策略
- ADR-0035: MARKET 单服务端原生支持
- ADR-0036: Quote 引擎状态 snapshot + 热重启
- ADR-0038: BFF 重连补齐快照
- [Binance Spot API — Order types](https://binance-docs.github.io/apidocs/spot/en/#new-order-trade)
- 实现：
  - [api/rpc/trigger/trigger.proto](../../api/rpc/trigger/trigger.proto)
  - [trigger/cmd/trigger/main.go](../../trigger/cmd/trigger/main.go)
  - [trigger/internal/engine/engine.go](../../trigger/internal/engine/engine.go)
  - [bff/internal/rest/trigger.go](../../bff/internal/rest/trigger.go)
