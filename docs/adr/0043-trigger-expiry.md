# ADR-0043: 触发单过期（TTL）

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0040, 0041, 0042

## 背景 (Context)

ADR-0040 实现的触发单没有过期机制。用户下一个止损后，如果不手动取消，
它会一直挂在 PENDING 状态。几种场景会让这成为问题：

- 用户改用 spot 之外的工具（做合约 / 法币 OTC），本地挂着的 spot 止损
  永远不会用上，但占着 reservation 的资金
- 下错单（stop_price 填了不合理的值），用户忘记取消，几个月后意外触
  发
- 合规 / 审计视角：长期挂单是风险敞口，想要有一个明确的"N 小时后自动
  作废"上限

BN 的 STOP_LOSS 系列也没有显式 TTL 字段，但有 GTD（Good Till Date）这个
tif 语义。考虑到触发单的触发时间不可控，显式 `expires_at_unix_ms`
比 TIF-based 表达更直接。

## 决策 (Decision)

### 1. 新字段 `expires_at_unix_ms` + 新状态 `EXPIRED`

proto 扩展：

```proto
message PlaceTriggerRequest {
  ...
  int64 expires_at_unix_ms = 11;   // 0 = never expires
}

message Trigger {
  ...
  int64 expires_at_unix_ms = 17;
}

enum TriggerStatus {
  ...
  TRIGGER_STATUS_EXPIRED = 5;
}
```

语义：

- `0` = 永不过期（向后兼容 —— 既有客户端不需要改）
- `> 0` = 绝对 wall-clock unix ms；到点后引擎把 PENDING → EXPIRED
- `<= now()` at Place time → `ErrExpiryInPast`（InvalidArgument）

### 2. 引擎扫描器（sweeper）周期性翻转

引擎新 API：

```go
func (e *Engine) SweepExpired(ctx context.Context) int
```

调用方（primary main）按 `--expiry-sweep=5s` 间隔调用。实现要点：

- 加锁遍历 `pending`，找出 `ExpiresAtMs > 0 && <= now` 的条目
- 锁内翻转状态到 EXPIRED + graduateLocked 移到 terminals 区
- 释放锁后 best-effort 调 Counter.ReleaseReservation（幂等，失败只 log）

5 秒的默认粒度对大多数止损场景够用（用户设 24h TTL 不会在意精度到
秒）。配置可到 1s。

### 3. 和 HA 的互动（ADR-0042）

只有 primary 跑 sweeper —— ADR-0042 的 `runPrimary` 里起 goroutine，
primary crash → backup 上位后 sweeper 接手。过期触发可能在 failover 窗
口里延后几秒，属于可接受的业务妥协。

### 4. 客户端视角

- REST `POST /v1/trigger` 增加 `expires_at_unix_ms`（JSON int64，0
  表示无 TTL）
- 响应 / Query / List 的 Trigger 对象携带 `expires_at_unix_ms`
- 状态码字符串新增 `expired`

## 备选方案 (Alternatives Considered)

### A. TIF-based（GTD=Good Till Date）
- 复用订单的 TIF enum 思路
- 缺点：`TIF` 在触发单里是"触发后下内部单的 TIF"，语义已被占用；再
  塞入过期时间会混淆
- 拒绝

### B. 绝对时间 vs 相对 duration（`ttl_ms`）
- 绝对时间（`expires_at_unix_ms`）对客户端要求有准确时钟，但多数客户
  端已经同步 NTP
- 相对 duration（`ttl_ms`）让服务端决定"具体何时过期"，但请求传输延迟
  + 重试会让 TTL 偏移
- BN / FIX 的普遍选择是绝对时间（expireTime）
- 选绝对时间

### C. 过期后自动重新下单（rolling orders）
- 过期后自动续一笔新触发单
- 明显 scope creep，本 ADR 拒绝；rolling 可作未来独立特性

### D. 过期由客户端轮询
- 完全不服务端做，让客户端定期 query + 发 Cancel
- 缺点：客户端必须一直在线；断线期间 stale order 继续占用 reservation
- 拒绝：过期是资源管理需求，服务端兜底更合理

## 理由 (Rationale)

- **最小变更**：一个字段 + 一个状态 + 一个后台 goroutine，BFF 层透传
- **向后兼容**：`0 = never expires` 让既有客户端无感升级
- **消费链路一致**：EXPIRED 走和 CANCELED 一样的 terminal 路径（
  graduate 到 terminals 区、触发 Release），Consumer 不用区别对待
- **运维可观测**：每轮 sweep 如果有条目过期，打一条 Info 日志，不污
  染 PENDING 路径

## 影响 (Consequences)

### 正面

- 用户可以显式设定 "24 小时后不生效就作废"，风险敞口有界
- Reservations 不会永久占用余额；operator 不需要手动清理
- TRIGGERED / EXPIRED 两个终态都能反映到客户端，历史记录可回溯

### 负面 / 代价

- Sweeper 每 5s 扫一次 PENDING。单用户几千个触发单也是毫秒级遍历，上
  十万条时会成为微小开销（锁持有略长）。量级成问题时可以按 symbol
  分区 sweeper
- 客户端需要提供准确 wall-clock（NTP）；偏差大时可能产生"我还没到期
  服务端说过期了"。接受，monitoring 侧如果看到高频 EXPIRED 报警可以
  检查客户端时钟
- Failover 期间 expiry 延后：`min(HA lease TTL, sweeper interval)` 的
  上限。ADR-0042 lease TTL=10s + sweeper=5s → 最多 ~15s 偏差

### 中性

- 没有额外的 counter-journal 事件。EXPIRED 的 bestEffortRelease 走现
  有 ReleaseReservation 路径

## 实施约束 (Implementation Notes)

### 代码改动

- `api/rpc/trigger/trigger.proto`：加 `expires_at_unix_ms` 两处 +
  `TRIGGER_STATUS_EXPIRED`
- `trigger/internal/engine/engine.go`：
  - `Trigger.ExpiresAtMs` 字段
  - `buildTrigger` / `Place` 接受并验证
  - `SweepExpired(ctx) int` 方法
  - `ErrExpiryInPast`
- `trigger/internal/engine/wire.go`：`ToProto` 带 ExpiresAtUnixMs
- `trigger/internal/snapshot/snapshot.go`：`TriggerSnap.ExpiresAtMs`
  序列化
- `trigger/cmd/trigger/main.go`：`--expiry-sweep` flag +
  `runExpirySweeper` goroutine 挂在 primary 里
- `trigger/internal/server/server.go`：错误映射 `ErrExpiryInPast` →
  InvalidArgument
- `bff/internal/rest/trigger.go`：`placeTriggerBody.ExpiresAtUnixMs`
  透传；`triggerStatusLabel` 加 `"expired"`

### 失败场景

| 场景 | 结果 |
|---|---|
| 用户传 `expires_at_unix_ms` 小于 now | 400 InvalidArgument + ErrExpiryInPast |
| Sweeper 运行时 Counter Release 失败 | Debug log，EXPIRED 状态已落；下一次 cancel / operator 手动清理 |
| 同一条 pending 已经被 cancel 了、然后 sweeper 尝试扫 | graduate 已发生，pending map 没有它；sweep 的 range 见不到 |
| 客户端系统时间慢（expires_at 发过来时其实已过期） | 服务端按自己时钟判断，直接 ErrExpiryInPast |
| sweep 运行中 Counter gRPC 挂掉 | RPC err log；trigger 侧仍然是 EXPIRED；reservation 留在 Counter。下一次 cancel / restart 手动处理（和 crash 场景一致） |

### 运维

```
trigger:
  --expiry-sweep   5s       # 默认；0 禁用（测试 / 手动调 SweepExpired 场景）
```

监控指标（未来埋）：`trigger_expired_total`、`trigger_expiry_sweep_duration`。

## 未来工作 (Future Work)

- **Rolling triggers**：EXPIRED 自动续下一条（可选字段
  `renew_after_expired=true`）
- **客户端 TTL 默认**：BFF 层给未传 `expires_at` 的请求兜底，比如默认
  7 天 TTL
- **过期事件流**：对接 Push，让用户 / 审计拿到 `trigger.expired`
  通知

## 参考 (References)

- ADR-0040: Trigger 服务整体设计
- ADR-0041: Counter reservations（过期释放的对象）
- ADR-0042: Trigger HA（primary 上的 sweeper）
- 实现：
  - [api/rpc/trigger/trigger.proto](../../api/rpc/trigger/trigger.proto)
  - [trigger/internal/engine/engine.go](../../trigger/internal/engine/engine.go)
  - [trigger/cmd/trigger/main.go](../../trigger/cmd/trigger/main.go)
  - [bff/internal/rest/trigger.go](../../bff/internal/rest/trigger.go)
