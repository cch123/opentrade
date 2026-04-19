# ADR-0054: 单用户 per-symbol 挂单数上限（Limit / Conditional 双槽位）

- 状态: Proposed
- 日期: 2026-04-19
- 决策者: xargin, Claude
- 相关 ADR: 0030（match etcd symbol sharding）、0040（conditional order service）、0041（counter reservations）、0044（conditional OCO）、0045（trailing stop）、0048（snapshot/offset 原子绑定）、0052（admin console）、0053（symbol 精度治理）

## 术语 (Glossary)

本 ADR 沿用 ADR-0053 的"字段名偏直白 + 术语表映射行业词"原则。

| 本 ADR 字段 | 含义 | 行业等价 |
|---|---|---|
| `MaxOpenLimitOrders` | 单用户在该 symbol 上的**活跃 LIMIT 单**上限 | Binance `MAX_NUM_ORDERS` / OKX `maxLimitOrders` / Bybit（账户级等价） |
| `MaxActiveConditionalOrders` | 单用户在该 symbol 上的**未触发条件单**上限 | Binance `MAX_NUM_ALGO_ORDERS` / OKX `maxTriggerOrders` / Bybit TP/SL 数量上限 |
| `活跃 LIMIT 单` | `Type == LIMIT` 且 `Status` 非 terminal（即 PendingNew / New / PartiallyFilled / PendingCancel） | BN 所谓"open orders"去掉 MARKET / LIMIT_MAKER |
| `未触发条件单` | conditional 服务中 `Status == CONDITIONAL_STATUS_ACTIVE`（尚未 triggered / canceled / expired） | BN algo orders 的 NEW 状态 |

**不计入槽位**：
- MARKET 单（无论 IOC/FOK；本身瞬时 terminal，不占 orderbook）
- conditional 触发后**派生**出的 counter LIMIT 单本身占 `MaxOpenLimitOrders`（见下文语义），但这个"派生"不在 conditional 槽位继续扣 —— conditional 触发瞬间从 pending → terminal，ALGO 槽位立刻释放

## 背景 (Context)

当前状态：

1. `counter/internal/engine/orders.go:19-25` 的 `OrderStore` 只有 `byID` 与 `activeByCOID` 两个索引，**无 per-(user, symbol) 活跃单计数**
2. `conditional/internal/engine/engine.go:171-177` 的 `pending / byClient / lastPrice`，**无 per-(user, symbol) 索引**，List/tryFire 都全表扫
3. `pkg/etcdcfg/SymbolConfig` 目前只承载 shard、trading、精度（ADR-0053），**无数量上限字段**
4. Counter `Service.PlaceOrder`（`counter/internal/service/order.go:74`）在精度校验 + 余额冻结之外**不限挂单数**；单用户可 DOS 自家 shard 内存 + 拖慢 Match orderbook

引出三类风险：

- **内存风险**：OrderStore `byID` 持有全量订单（含历史 terminal 在 GC 前），单用户放 10^5 LIMIT 把 shard 内存吃爆
- **Match orderbook 膨胀**：每条活跃 LIMIT 都进 orderbook，撮合复杂度随深度上升
- **刷单/探测**：无上限等同于给攻击者放白名单刷 PlaceOrder 事件流（ADR-0048 snapshot 体积、Kafka 吞吐都受影响）

业界解法统一：BN/OKX/Bybit 都有"per-symbol per-account 活跃单数量 filter"，本 ADR 决定对齐 BN 的两槽位拆分（普通 vs algo），不复造。

## 决策 (Decision)

### 1. 两个独立槽位，per-(user, symbol) 计数

- **Counter 槽位**：单用户在 symbol 上的**活跃 LIMIT 单**数 ≤ `MaxOpenLimitOrders`，默认 **100**
- **Conditional 槽位**：单用户在 symbol 上的**未触发条件单**数 ≤ `MaxActiveConditionalOrders`，默认 **10**（BN 典型值 5；MVP 留余量）
- 两个槽位**互不影响**：LIMIT 不占 conditional 额度，反之亦然
- MARKET 单**不计**任何槽位

### 2. 配置通道 —— 复用 `SymbolConfig`

```go
// pkg/etcdcfg/etcdcfg.go
type SymbolConfig struct {
    // 既有字段……
    MaxOpenLimitOrders          uint32 `json:"max_open_limit_orders,omitempty"`
    MaxActiveConditionalOrders  uint32 `json:"max_active_conditional_orders,omitempty"`
}
```

- 零值 = **用全局默认**（Counter/Conditional 启动配置里的 `DefaultMaxOpenLimitOrders` = 100、`DefaultMaxActiveConditionalOrders` = 10）
- admin-gateway（ADR-0052）承担下发；与 ADR-0053 的精度字段走同一 `PUT /admin/symbols/{symbol}` 通路

### 3. 校验点 + Reject 语义

**Counter `Service.PlaceOrder`**（`counter/internal/service/order.go`）：

- 校验位置：精度校验之后、进入 sequencer 的 lambda 内（和余额检查同一锁区，避免并发下单绕过）
- 仅当 `OrderType == LIMIT` 时检查
- 超限 →
  ```go
  PlaceOrderResult{Accepted: false, RejectReason: "MAX_OPEN_LIMIT_ORDERS_EXCEEDED"}
  ```
- 新增 `RejectReason` 常量 `RejectMaxOpenLimitOrders` 放在 `pkg/etcdcfg/precision.go`（或迁到更中立的 `pkg/order/reject.go`；实施期再定）

**Conditional `Engine.Place`**（`conditional/internal/engine/engine.go:243`）：

- 校验位置：`buildConditional` 之后、插入 `pending` 之前，仍在 `e.mu` 临界区内
- 所有 conditional type（STOP_LOSS、TAKE_PROFIT、TRAILING_STOP、OCO）一律计入
- 超限 → 返回 `ErrMaxActiveConditionalOrdersExceeded`（新增 sentinel）

### 4. 内存索引

**Counter `OrderStore`** 增加：

```go
activeLimits map[string]map[string]int // user_id → symbol → count
```

维护点：
- `Insert`: `Type == LIMIT && !Status.IsTerminal()` → `++`
- `UpdateStatus`: 从非-terminal 进入 terminal 且 `Type == LIMIT` → `--`；降到 0 时删 key 防内存泄漏
- `RestoreInsert`: 同 Insert 语义
- 热路径读：`CountActiveLimits(userID, symbol) int`，走 RLock

**Conditional `Engine`** 增加：

```go
activeConditionals map[string]map[string]int // user_id → symbol → count
```

维护点：
- `Place` 成功落 pending → `++`
- `tryFire → graduateLocked` / `Cancel` / `Expire` → `--`
- 快照恢复从 pending 重建

两边都是**派生索引**，不入 snapshot schema —— 从活跃订单集合 100% 可重建，避免与 ADR-0048 snapshot/offset 绑定产生耦合。

### 5. conditional 触发派生单的占槽行为（BN 对标）

conditional 触发时派生 counter LIMIT 单（`counterclient` 走 PlaceOrder）：

- **该派生单照常占 Counter `MaxOpenLimitOrders` 槽位**（走 PlaceOrder 正常路径，计数自然 ++）
- **不预占**：conditional `Place` 阶段**不预留** Counter 槽位
- **触发时超限**：派生 PlaceOrder 拿到 `MAX_OPEN_LIMIT_ORDERS_EXCEEDED` → conditional 服务将该单置 terminal `EXPIRED_IN_MATCH` 状态（BN 术语）
- 对应 `RejectReason` 需要在 `counterclient.PlaceOrder` 返回路径透传到 conditional engine

这一行为与 BN spot 完全一致，符合用户"对标 BN"要求。

## 备选方案 (Alternatives Considered)

### A. 全局 per-user 上限（所有 symbol 汇总 100）

- 优点：一行计数；新用户不会意外堆 100 个 symbol 各 100 单
- 缺点：与 BN/OKX/Bybit 语义不符；做市商在 10 个 symbol 同步报价就会卡死

**弃用**。

### B. 两层嵌套（全局上限 + per-symbol 上限）

- 优点：更灵活
- 缺点：两层计数 × 两个槽位 = 4 个索引；MVP 阶段复杂度超产品收益

**暂不做**。未来如需可以加独立 `MaxOpenLimitOrdersGlobal`（user 总上限）字段。

### C. Conditional 预占 Counter 槽位

- 优点：触发一定能下成，体验好
- 缺点：一个用户挂 100 个 stop 单就让该 symbol 所有新 LIMIT 报错，与"conditional 不产生市场影响"的直觉反；BN 未采用

**弃用**。

### D. 把计数存在 Account 结构里而不是单独索引

- 优点：少一个数据结构
- 缺点：Account 是 per-user 的，计数 key 需要 (user, symbol) 二维；拆到 Account 里反而污染了余额语义

**弃用**。

## 理由 (Rationale)

- **为什么 per-symbol 而非 per-user**：BN/OKX/Bybit 统一语义；做市友好
- **为什么两槽位分开**：普通单与 algo 单的使用密度截然不同（一个跟盘口走、一个设"保险丝"），混一起会逼用户取舍
- **为什么 default 100 / 10**：100 覆盖 90% 散户 + 轻量做市；10 比 BN 的 5 松一档，给 OCO（ADR-0044）+ TrailingStop（ADR-0045）叠加场景留头寸
- **为什么存活索引 vs 每次遍历**：pending/active 订单总量在单 shard 内可达 10^4+，下单热路径不能接受 O(n) 扫描
- **为什么不入 snapshot**：派生自权威状态，重建成本低（restore 一遍活跃订单即可），避免膨胀 snapshot 二进制 + ADR-0048 的原子绑定复杂度

## 影响 (Consequences)

### 正面

- Counter / Match 内存上限可预估（per shard × 10 shard × ~N symbol × 100 单）
- 攻击面收窄：单 user 刷 Place 不再能无限膨胀 OrderStore / orderbook
- 为后续"VIP 分级" / "做市商白名单"预留了 per-symbol 可配口子

### 负面 / 代价

- 两边 Engine 都要新增一个索引 + 热路径多一次 map 查找（单 RLock 读 + 两级 map，实测应 <100ns，可忽略）
- 测试面扩大：Place/Cancel/PartialFill/Expire/RestoreFromSnapshot 每条路径都要加"计数是否准"的断言
- conditional 触发派生单的拒绝路径（`EXPIRED_IN_MATCH`）是新状态分支，需要 push / quote / trade-dump 侧识别

### 中性

- 热重载：阈值调低（100→50）时，已超限的老单**不强制撤**，只拒新增。需要在运维 runbook 写清楚
- `ClientOrderID` dedup 命中（重试）不应重复扣计数 —— dedup 在计数之前就返回，天然不冲突
- counter shard 边界：per-user 都在同一 shard 内，零跨 shard 协调

## 实施约束 (Implementation Notes)

### 落地顺序

1. **M1**：`pkg/etcdcfg.SymbolConfig` 加字段 + JSON tag + admin-gateway PUT 校验（`uint32`，0 = 兜底默认；合理上限如 10000 防误配）
2. **M2**：Counter `OrderStore.activeLimits` 索引 + `CountActiveLimits` + `Insert/UpdateStatus/RestoreInsert` 维护 + 单测
3. **M3**：Counter `Service.PlaceOrder` 加超限 reject + `RejectMaxOpenLimitOrders` 常量 + 全局默认配置项
4. **M4**：Conditional `Engine.activeConditionals` 索引 + `Place` 超限 reject + 快照恢复重建
5. **M5**：Conditional 触发派生单的拒绝路径打通（`counterclient` 透传 RejectReason → `tryFire` 置 `EXPIRED_IN_MATCH`）
6. **M6**：admin console UI 暴露两个字段 + 运维文档

### 关键测试

- Counter：100 单占满 → 第 101 reject；cancel 一单 → 第 101 可下；部分成交不影响计数；全部 fill 后计数归零并清理 map key
- Counter：snapshot/restore 后计数与活跃单数一致（ADR-0048 兼容性断言）
- Conditional：STOP + OCO + TrailingStop 混合场景下槽位共用同一计数
- 链路：conditional 触发派生单遇 Counter 满 → `EXPIRED_IN_MATCH` + push 能发出终态

### 配置降级兼容

- 既有 etcd 中不带这两字段的 SymbolConfig `Unmarshal` 仍合法（`omitempty`）
- Counter/Conditional 启动配置里**必须**提供非零全局默认，否则服务启动 fail-fast（防止 0 = 彻底禁止下单的误配）

## 参考 (References)

- Binance spot filter: `MAX_NUM_ORDERS`、`MAX_NUM_ALGO_ORDERS`、`EXPIRED_IN_MATCH` 状态
- OKX `instruments`: `maxLimitOrders` / `maxTriggerOrders`
- Bybit V5 instruments-info: TP/SL 数量上限
- ADR-0048: snapshot/offset 原子绑定（本 ADR 明确选择"不入 snapshot"）
- ADR-0040 / 0044 / 0045: conditional 单生命周期（graduate / cancel / expire）
