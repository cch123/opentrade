# ADR-0041: Counter 资金预留（Reservations）

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0001, 0005, 0008, 0011, 0035, 0040

## 背景 (Context)

[ADR-0040](./0040-trigger-order-service.md) §资金不预留（MVP-14a）
里明确给出了一条妥协：触发单不在下发时冻结资金。触发瞬间如果账户余额
已被挪作他用，Counter `PlaceOrder` 直接返回 `FailedPrecondition`，条
件单走 REJECTED。UX 很糟：止损单触发时才告诉用户 "来不及救了"。

MVP-14b 要补上这个洞，关键是把"账户余额的某一部分"在触发单下发的那一刻就
锁到 Frozen，在触发瞬间原子地转成真实订单的 FrozenAmount，中间哪怕用户
发其它 REST 订单也吃不到这笔钱。

已有的 `Transfer(FREEZE)` （ADR-0011）**从 Available → Frozen 的数值
效果** 跟我们想要的一致，但语义和权限完全对不上。本 ADR 在 Counter 里新
加 Reserve / ReleaseReservation，解释为什么不直接复用 Transfer，并把
实现边界写清楚。

## 决策 (Decision)

### 1. 新增两条 Counter gRPC

```proto
rpc Reserve(ReserveRequest) returns (ReserveResponse);
rpc ReleaseReservation(ReleaseReservationRequest) returns (ReleaseReservationResponse);
```

- **Reserve**：调用方传订单 shape（symbol / side / order_type /
  price / qty / quote_qty），Counter 跑 `engine.ComputeFreeze` 算出
  `(asset, amount)`，把钱从 Available 挪到 Frozen，并在一张副表
  `reservations[ref_id] → {user, asset, amount, createdAt}` 里落记
  录。dedup 以 `reservation_id` 为键，重复调用返回原记录 +
  `accepted=false`
- **ReleaseReservation**：移动反向（Frozen → Available），删记录。
  未知 ref_id 返回 `accepted=false`（幂等）

### 2. `PlaceOrder` 学会消费 reservation

`PlaceOrderRequest.reservation_id` 新字段。当非空：

1. Counter 算出 `(asset, amount)`（同一套 ComputeFreeze）
2. 查 reservation。找不到 → reject；user 不匹配 → reject；
   `(asset, amount)` 不匹配 → reject
3. 删 reservation 记录，**不再动 balance**（钱已经在 Frozen 里）
4. 正常走订单创建 + 发 counter-journal FreezeEvent

关键属性：步骤 2 ~ 4 都在用户的 sequencer 闭包里执行，外界无法在
"reservation 消费" 和 "order 创建" 之间插入别的订单。

### 3. 权限边界：Reserve 不暴露到 BFF REST

Transfer 是用户级 RPC（`/v1/transfer`）；任何持 X-User-Id 的请求都能
冻结/解冻自己的余额。Reserve 走 Counter gRPC，BFF REST 不挂
`/v1/reservation*` 路由，只有 trigger 服务（内网 gRPC 客户端）调
Counter.Reserve。

### 4. 持久化：snapshot 专属

Reserve / Release **不** 发 counter-journal 事件：

- Reservation 是 Counter 的内部 bookkeeping，它的生命周期是 "trigger 下
  发 → 触发或取消" ，通常秒到小时级。Kafka 上积累的
  Reserve/Release 噪声远大于它的分析价值
- Counter 的 snapshot（ADR-0031 / MVP-12）新增 `reservations` 段，
  graceful restart 可恢复
- 非 graceful crash: Counter 丢 reservation 记录，但 balance 也回到
  pre-Reserve 状态（因为 balance 的 frozen 增量没进 Kafka journal 也
  不会被 replay），两边一致
- trade-dump 的 `accounts` projection 在 reservation 持有期间会偏（
  Available 看起来多、Frozen 少）；consume 时 PlaceOrder 发正常
  FreezeEvent，BalanceAfter 收敛，cancel-release 时双方都回到 pre-
  Reserve 状态，也收敛。只有"持有中"那一段是 stale 的，可以接受

## Reserve vs Transfer FREEZE 对比

这张表是本 ADR 的核心 —— 两者底层都是 `Available → Frozen`，但语义完
全不同：

| 维度 | Transfer FREEZE（ADR-0011） | Reserve（本 ADR） |
|---|---|---|
| **调用者** | 用户 / 管理员（BFF REST `/v1/transfer`） | 内部特权服务，目前只有 trigger；BFF 无对应路由 |
| **目的** | 取款冻结、合规锁仓、风控扣押 | 触发单为触发时刻预留专款 |
| **是否带 ref 记录** | ❌ 钱挪到 Frozen 就结束；Counter 不知道"为什么挪"。journal 里 `biz_ref_id` 是纯字符串 metadata | ✅ 副表 `reservations[ref_id] → {user, asset, amount}` 建立了 "谁的钱、留给谁用" 的绑定 |
| **幂等键** | `transfer_id` | `reservation_id`（完全独立命名空间，不共用 Transfer 的 dedup 表） |
| **解冻路径** | 必须再发 `Transfer(UNFREEZE)`，中间钱回到 Available | `ReleaseReservation`（显式取消），或 `PlaceOrder(reservation_id=…)` 原子消费 —— 后者是关键 |
| **race 暴露** | "UNFREEZE → 下单" 两步有窗口，并发订单可能吃掉刚释放的余额 | `PlaceOrder` 在同一 sequencer 闭包里 "删 reservation + 创建 order + 提交 journal"，没有窗口 |
| **Kafka journal** | 发 `TransferEvent`（用户流水、对账的一等公民） | 不发；仅 snapshot 持久化 |
| **trade-dump projection** | 实时跟随 | 持有期间 stale（balance 已移动，projection 没看到） |
| **BFF 路由** | `/v1/transfer` | 无路由；操作经由 trigger gRPC |

### 为什么不把 Reserve 做成 Transfer 的新 type

尝试过方案：扩 `TransferType` 新增 `TRANSFER_TYPE_RESERVE / _RELEASE` +
`biz_ref_id=trig-X`。被否决的原因：

1. **权限**：Transfer 是用户级 RPC。即使加 ACL 限制新 type 只能由 root
   key 调用，也会让 Transfer 这条路径 需要在 request 级别分辨调用方身份。
   不如直接走新 RPC 干净
2. **原子消费**：即使加了 `biz_ref_id`，触发路径上仍然是"
   `Transfer(UNFREEZE, biz_ref_id=X)` → `PlaceOrder`" 两个 RPC，
   sequencer 闭包也分开，race 窗口照样在。没有 "PlaceOrder 直接消
   费 reservation" 这一步就解决不了
3. **journal 噪声**：每个触发单从 Place 到 Trigger 产出 1-2 条
   TransferEvent，对 trade-dump 的 account_logs 表是扰动，对账审查时
   要一直 filter `biz_type='trigger'`。分开 proto 类型 + 不发
   journal 把 bookkeeping 内部化更简洁
4. **语义分层**：Transfer 表达的是 "用户 / 风控显式操作账户"；
   Reservation 表达的是 "订单子系统的内部资金挂起"。把两者混在一起会
   让 "用户流水" 这条概念线变模糊

## 备选方案 (Alternatives Considered)

### A. Transfer FREEZE + ref 侧表（前面讨论过）
拒绝：原子消费做不到。

### B. Trigger 不预留，触发时拿锁 + 查余额
拒绝：这是 MVP-14a，UX 差。MVP-14b 的目标就是解决这个。

### C. Counter 暴露 `PlaceOrderAtomic(reserve_and_place)`，一个 RPC 搞定
- 触发单下发时不做任何事，触发时一个 RPC 完成 "检查 + 冻结 + 下单"
- 缺点：触发单在用户端 placed 后，真正的资金锁定要到触发才发生 ——
  用户完全可以在这段时间把钱用光。等于又回到 MVP-14a，没解决问题
- 拒绝

### D. 把 reservation 写 counter-journal
- trade-dump projection 始终 up-to-date
- 代价：新两条 event type（ReservationCreated / ReservationReleased），
  trade-dump 需要消费，accounts 表加 2 种 `biz_type`，所有对账脚本要跟着动
- MVP-14b 接受"持有中 projection stale"的代价换取实现极小化。上生产后
  如果 staleness 成为痛点再补 journal，snapshot → journal 是 additive
  变更
- 拒绝（MVP 阶段）

### E. Reservation 走 `(asset, amount)` 全量数据，让 trigger 自己算
- trigger 不用 Counter 的 ComputeFreeze，自己预估需要冻的资产和量
- 缺点：freeze 公式的权威必须在 Counter（ADR-0035 已经反复强调），
  trigger 复制这段逻辑会造成两边漂移
- 改为 ReserveRequest 直接传 **订单 shape**（symbol/side/type/price/qty/
  quote_qty），Counter 自己跑 ComputeFreeze，就不用 trigger 关
  心 asset/amount

## 理由 (Rationale)

- **单一 freeze 公式**：Reserve 和 PlaceOrder 都调 `engine.ComputeFreeze`，
  永远不会算出两个不同的 freeze；不匹配自动是 caller bug
- **锁边界清晰**：sequencer 保证同用户操作序列化；跨用户天然并行
- **对外 API 最小变更**：Counter 加 2 条 RPC + PlaceOrderRequest 加 1 字
  段。所有老路径（普通 PlaceOrder / Transfer / Cancel）行为零变化
- **trigger 零知识**：trigger 的 placer 接口多了两个方法，
  Place/Cancel/Trigger 的语义保持直接映射，阅读成本低

## 影响 (Consequences)

### 正面

- 触发单用户体验大幅提升：下单即锁资金，触发几乎不可能因余额不足失败
- Counter API 加法式扩展（新 RPC + proto 字段），调用方不升级完全 backward
  compatible
- 测试覆盖齐全：reservation 的 8 个路径（happy / dedup / 余额不足 /
  release / mismatch / consume / unknown / cross-conn race）都有单测

### 负面 / 代价

- 代码量：proto + engine + service + server + snapshot + 4 个 trigger
  侧改 ~ 800 LOC
- trade-dump projection 在 reservation 持有期间偏高 Available：接受，
  文档化，journal-ify 留给未来
- Counter crash 无 snapshot 时会丢触发单的资金锁定 —— trigger
  侧触发瞬间回到 MVP-14a 行为（balance 不足 → REJECTED）。graceful
  shutdown + snapshot 配合可用
- Snapshot 文件稍增大（每条 reservation ~200B JSON）

### 中性

- `reservation_id` 命名空间和 `transfer_id` / `client_order_id` 完全独
  立；trigger 约定用 `"trig-<id>"`，其它服务未来接入时自己挑前缀
  避免冲突
- Reserve 走用户 sequencer，吞吐受单用户 sequencer 限制；和 Transfer /
  PlaceOrder 共享，触发单流量通常远小于下单流量，不是瓶颈

## 实施约束 (Implementation Notes)

### Counter engine

- `engine.Reservation` 类型 + `reservationStore`（`byRef` + `byUser` 双索引
  for O(1) 查 + O(users) 遍历用于 Snapshot）
- `ShardState.{CreateReservation,ReleaseReservationByRef,ConsumeReservationForOrder,
  RestoreReservation,LookupReservation,AllReservations}` 对外的 API
- 返回的 Reservation 永远是 Clone，内部结构对外隐藏

### Counter service

- `Reserve` / `ReleaseReservation` 走 `s.seq.Execute(userID, …)`；
  dedup 检查 + 余额检查 + 状态变更在同一 closure 内
- `PlaceOrder` 在 balance check 前分叉：`reservation_id != ""` 走
  ConsumeReservationForOrder + skip balance check；否则原路径

### Counter snapshot

```json
"reservations": [
  {"user_id":"u1","ref_id":"trig-42","asset":"USDT","amount":"100","created_at_ms":...}
]
```

Capture 扫 `state.AllReservations()`；Restore 遍历 + `state.RestoreReservation()`
（后者不动 balance，只插入 record）。

### Trigger integration

`engine.Engine.reserver` 可选接口。为 nil 时回退 MVP-14a 行为。为非 nil
时：

- `Place`：validate → `idgen.Next()` → 外层调 `Reserve` → 回来 commit 到
  maps。dedup 竞态时释放 orphan reservation
- `Cancel`：状态转换 + 异步 `ReleaseReservation`（最多丢一条 release，
  下次 snapshot 保留 reservation，重启 replay 可清理）
- `tryFire`：PlaceOrder 带 `reservation_id`。成功即 TRIGGERED（Counter
  消费了 reservation）；失败 → REJECTED + 尝试 `ReleaseReservation` 清
  理

### 失败场景

| 场景 | 结果 |
|---|---|
| Reserve 余额不足 | `ErrInsufficientAvailable`；trigger 返回 400 给用户，未存储 |
| Reserve 重复（同 ref_id） | 幂等；返回原 record 和 accepted=false |
| PlaceOrder(reservation_id) 查不到 | `ErrReservationNotFound` / NOT_FOUND；trigger 置 REJECTED |
| PlaceOrder shape 和 Reserve 不一致 | `ErrReservationMismatch` / FailedPrecondition；trigger 置 REJECTED，尝试 Release |
| Counter crash + 无 snapshot | Reservation 丢，balance 回到 pre-Reserve；trigger 触发时走 REJECTED |
| Trigger 本身 crash | Counter 保留 reservation 直到 TTL 或人工清理；14c 的 HA + 重启恢复会处理；目前留 orphan |

## 未来工作 (Future Work)

- **Journaling**：Reserve / Release 进 counter-journal，trade-dump 实时收敛
- **Reservation TTL**：`CreatedAtMs` 已经记了；后台定期清理超过 N 小时未
  消费的 orphan（防 trigger crash 永久不消费）
- **Audit endpoint**：`CounterService.ListReservations` 供运维 / 对账
- **允许 reservation 部分消费**：现在要求 exact match。如果未来支持
  修改触发单（比如改 qty）不想重新 Reserve，可以支持"消费一部分
  reservation"
- **可枚举 reservation 的 Transfer biz_type**：如果某天真的想把用户级 Transfer
  也纳入同一命名空间（不是 override 语义，是聚合审计视图），给 Transfer
  加一个指向 reservation 的 foreign key

## 参考 (References)

- ADR-0011: Counter Transfer 统一入口
- ADR-0035: MARKET 单服务端原生支持（ComputeFreeze 的 single source of truth）
- ADR-0040: 触发单独立服务
- 实现：
  - [api/rpc/counter/counter.proto](../../api/rpc/counter/counter.proto)
  - [counter/engine/reservations.go](../../counter/engine/reservations.go)
  - [counter/internal/service/reservation.go](../../counter/internal/service/reservation.go)
  - [counter/internal/service/order.go](../../counter/internal/service/order.go)
  - [counter/snapshot/snapshot.go](../../counter/snapshot/snapshot.go)
  - [trigger/internal/engine/engine.go](../../trigger/internal/engine/engine.go)
  - [trigger/internal/counterclient/client.go](../../trigger/internal/counterclient/client.go)
