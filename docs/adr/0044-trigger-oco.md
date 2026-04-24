# ADR-0044: 触发单 OCO（One-Cancels-Other）

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0040, 0041, 0042, 0043

## 背景 (Context)

BN 的 spot `POST /api/v3/order/oco` 允许同时下两条腿（take-profit +
stop-loss），任一 成交就自动取消另一条。用户侧典型用法：

- 持仓后同时挂止盈止损，谁先到就执行谁；避免"先止盈再挂止损"的时间差
- 盘口剧烈波动时，不用手动清理残单

ADR-0040 只支持独立（standalone）触发单，需要用户自己写两条然后指望触
发后手动取消另一条。MVP-14e 补齐。

## 决策 (Decision)

### 1. N-leg OCO group（N ≥ 2）

新 RPC：

```proto
rpc PlaceOCO(PlaceOCORequest) returns (PlaceOCOResponse);

message PlaceOCORequest {
  string user_id = 1;
  string client_oco_id = 2;                 // group-level idempotency
  repeated PlaceTriggerRequest legs = 3;
}
```

- 腿数 ≥ 2（典型 2，允许更多）
- 所有腿必须共享 `user_id` / `symbol` / `side`
- 每腿复用既有 PlaceTrigger 结构 —— MVP-14a/b/d 的全部能力
  （LIMIT/MARKET 变种、过期、reservation）自动套用

引擎给 group 分配 `oco_group_id = "oco-<leg1_id>"`。

### 2. Terminal 级联取消

任一腿到达 terminal 状态（TRIGGERED / REJECTED / CANCELED / EXPIRED），
引擎遍历同 group 还是 PENDING 的兄弟，全部设为 CANCELED + best-effort
`ReleaseReservation`。

实现点：

```go
// 在锁内：
//   - 当前腿 graduateLocked
//   - 如果 c.OCOGroupID != "": cascadeOCOCancelLocked(c, reason)
//       -> 收集所有 PENDING 同 group 的腿，CANCELED + graduate
//       -> 返回 []releaseTarget
// 释锁后：
//   - bestEffortRelease 当前腿（原来 cancel / reject 路径已有）
//   - releaseAll(targets) 对级联腿发 Release
```

复用同一个 `cascadeOCOCancelLocked` helper，Cancel / tryFire / SweepExpired
都走这一条。

### 3. Group-level 幂等

新 `ocoByClient map[string]string` 记 `client_oco_id → oco_group_id`。
重复 PlaceOCO 返回原 group 的 leg ids + `accepted=false`。和既有
`client_trigger_id` 级 dedup 正交 —— 两者都可以独立工作。

MVP 竞态保护：Place 内部先 fast-path check 一次 dedup，Reserve 结束后
commit 前再查一次；lost race 时 orphan reservations 通过
`bestEffortRelease` 回滚。

### 4. Reservation 生命周期

每腿独立 Reserve（MVP-14b / ADR-0041）；一腿 Reserve 失败 → 全部已
Reserve 的腿回滚。

级联 CANCELED 的腿走同样的 best-effort ReleaseReservation。orphan（上游
调用方重试 + 竞态）通过 Place commit 前的 dedup 检查释放。

### 5. 快照

`TriggerSnap.OCOGroupID` + 顶层 `OCOByClient` map 均入
snapshot。Graceful restart 后级联逻辑、dedup 均继续工作。

## 备选方案 (Alternatives Considered)

### A. 两条独立 PlaceTrigger + 后置 "link group" RPC
- 优点：proto 最小
- 缺点：两条下单 + 一次 link 不是原子的；用户逻辑复杂（orphan 未 link
  的腿 / link 前一腿触发）
- 拒绝

### B. 严格 2 腿（mirror BN shape：`price` + `stopPrice` + `stopLimitPrice`）
- 优点：proto 最朴素
- 缺点：我们现有 4 种 TriggerType 已经覆盖 BN 的所有腿形态，直接
  拼成 legs[] 不需要新字段
- 拒绝：allowing N ≥ 2 是 "cheap abstraction，能写 triangular OCO"

### C. Group terminal 时联动 trade-event 回收
- 在 Counter 层增加 OCO 概念
- 缺点：把 OCO 这个纯应用层抽象塞进 Counter，违反 Counter 是账户状态
  机的定位（同 ADR-0035 §Z 的理由）
- 拒绝

### D. 第一腿取消整个 group（含已 TRIGGERED 腿）
- 一些平台支持 "cancel OCO" = 取消整个 group，不管腿的状态
- 缺点：TRIGGERED 腿下的订单已经进 Counter → 要跟着 Cancel 那个内部订
  单 → 路径复杂（订单可能已部分成交）
- 本 MVP 只做 "某腿到 terminal → 其它腿 CANCELED"；显式"取消整个
  group"留作未来 CancelOCO RPC
- 拒绝（当前 scope）

## 理由 (Rationale)

- **结构最小化**：一个 proto RPC + 一个新字段（`oco_group_id`）+ 一个
  级联 helper。复用 Place / Cancel / tryFire / SweepExpired 的既有路径
- **幂等两层**：per-leg `client_trigger_id` + group `client_oco_id`
  两条独立 dedup 轴，对客户端重试友好
- **原子性**：Reserve 全部成功才进内存；失败一腿就回滚已成功的，
  snapshot 永远不会看到"半个 OCO"
- **终态一视同仁**：TRIGGERED / EXPIRED / CANCELED / REJECTED 都走
  cascadeOCOCancelLocked，兄弟腿行为可预测

## 影响 (Consequences)

### 正面

- 用户可以同时下止盈+止损；减少残单 / 手工跟单
- 既有 standalone 触发单行为不变（`OCOGroupID == ""` 跳过 cascade 逻
  辑，零开销）
- PushFeed 的触发单推送只需要带上 `oco_group_id`，客户端就能把腿对齐
  到组（不在本 ADR scope，但设计友好）

### 负面 / 代价

- 级联取消时，engine 持锁遍历 pending。OCO group 量大时微小开销；
  常见场景 group 只含 2-3 腿，可忽略
- OCO dedup 表在 snapshot 里占空间（每 key ≈ 40B JSON）。终端未来要做
  TTL 清理（保留 24h）避免无限增长 —— 当前无清理
- Group cascade 的 best-effort Release 失败时，reservation 会 orphan
  到 Counter 侧；sweeper（MVP-14d）或 operator 需要清理。和单腿场景
  相同的故障模型

### 中性

- 不改变 Counter：Counter 仍然把每腿当独立 client_order_id 看待。OCO
  概念纯粹在 trigger 层

## 实施约束 (Implementation Notes)

### 代码改动

- `api/rpc/trigger/trigger.proto`：
  - 新 RPC `PlaceOCO` + `PlaceOCORequest` / `PlaceOCOResponse`
  - `Trigger.oco_group_id = 18`
- `trigger/internal/engine/engine.go`：
  - 新字段 `Trigger.OCOGroupID` + `Engine.ocoByClient`
  - 新方法 `PlaceOCO` + `cascadeOCOCancelLocked` + helper `releaseAll`
  - Cancel / tryFire / SweepExpired 注入 `cascadeOCOCancelLocked`
  - 新错误 `ErrOCONeedsTwoLegs` / `ErrOCOSymbolMismatch` /
    `ErrOCOSideMismatch` / `ErrOCOUserMismatch`
- `trigger/internal/engine/wire.go`：`ToProto` 带 `OcoGroupId`
- `trigger/internal/snapshot/snapshot.go`：`TriggerSnap.OCOGroupID` +
  `Snapshot.OCOByClient`；`Restore` 调用新 `engine.SetOCOByClient`
- `trigger/internal/service/service.go` + `server.go`：`PlaceOCO` 方
  法 + gRPC handler + error mapping
- `bff/internal/client/trigger.go`：接口加 `PlaceOCO`
- `bff/internal/rest/trigger.go`：`POST /v1/trigger/oco` handler
  + `placeOCOBody` 结构
- `bff/internal/rest/server.go`：路由注册

### 失败场景

| 场景 | 结果 |
|---|---|
| 腿数 < 2 | 400 InvalidArgument + ErrOCONeedsTwoLegs |
| 不同 symbol / side / user_id | 400 + 对应 ErrOCOSymbolMismatch / SideMismatch / UserMismatch |
| 其中一腿余额不足 Reserve 失败 | 回滚其它已 Reserve 的腿 → 整个 Place 返回 FailedPrecondition |
| `client_oco_id` 已存在 | Response 带原 group 的 leg ids + `accepted=false`；不再 Reserve |
| 一腿 trigger 成功，Counter 创建了内部订单，另一腿同时到期 | cascade 只看 PENDING，TRIGGERED 的腿不动；EXPIRED 的腿不动；两者互相不干扰 |
| 级联 Release 网络失败 | Debug log；trigger 侧状态已 CANCELED；reservation 遗留在 Counter 直到 operator 清理 |

### 测试覆盖

- `PlaceOCO_HappyPath` / `DedupByClientOCOID` / mismatched symbol / side / too few legs
- `TriggerOneCascadesToSibling` / `CancelOneCascadesToSibling` /
  `ExpirationCascadesToSibling`

## 未来工作 (Future Work)

- **CancelOCO RPC**：一次取消整个 group，内部若有 TRIGGERED 腿则附带
  Cancel 那个内部订单（需要 Counter 协作）
- **client_oco_id TTL**：ocoByClient 周期清理（几小时～1 天），避免无
  限增长
- **rejection semantics**：某腿 REJECTED 是否应该让整个 group 继续
  PENDING 等用户手动处理？BN 的默认是"REJECT 级联取消"，本 MVP 跟 BN
- **Push 推送 group_id**：让客户端 UI 能把腿对齐回 group 展示
- **多 group 关联**：比如 "OCO of OCOs"（真・triangular）；目前不支持
  嵌套

## 参考 (References)

- [Binance Spot API — New OCO](https://binance-docs.github.io/apidocs/spot/en/#new-oco-trade)
- ADR-0040: Trigger 服务整体设计
- ADR-0041: Counter reservations（orphan 回滚依赖）
- ADR-0043: 过期 sweeper（同一 cascade helper）
- 实现：
  - [api/rpc/trigger/trigger.proto](../../api/rpc/trigger/trigger.proto)
  - [trigger/internal/engine/engine.go](../../trigger/internal/engine/engine.go)
  - [bff/internal/rest/trigger.go](../../bff/internal/rest/trigger.go)
