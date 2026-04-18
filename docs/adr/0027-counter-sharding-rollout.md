# ADR-0027: MVP-8 Counter 10-shard 路由落地

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0010, 0018, 0022

## 背景 (Context)

ADR-0010 决定 Counter 分 10 个固定 shard、按 `user_id` hash 路由。但 MVP-0~7
期间只跑了 1 个 shard —— Counter 自己的代码结构早就是 "per-shard" 的（每个
进程一个 `--shard-id`、journal producer id / snapshot 文件 / consumer group 都
按 shard id 命名），真正缺的是：

1. **统一的 hash 函数**：BFF 路由、Counter 自己的 ownership check、未来 Push
   对齐 LB 都要用 *同一个* 稳定 hash，且跨 Go 版本不变。
2. **BFF 从连 1 个 Counter 到连 N 个**：`--counter-shards` 接受 csv。
3. **Counter 自己对 non-owned user 的请求做 defensive 拒绝**：避免配置失误
   (BFF 路由错) 静默污染 shard 状态。

MVP-8 把这三件事做完。真正的多机部署还需要 docker-compose / k8s 编排来拉起 10
个 counter 实例 —— 本 ADR 只涵盖代码层面的 sharding 能力。

## 决策 (Decision)

### 1. 统一 hash 函数：[pkg/shard](../../pkg/shard/shard.go)

```go
func Index(userID string, totalShards int) int {
    return int(xxhash.Sum64String(userID) % uint64(totalShards))
}
func OwnsUser(shardID, totalShards int, userID string) bool
```

- 使用 `github.com/cespare/xxhash/v2`（Go runtime map hash 是 process-随机的，
  不能跨进程用）。
- `TestIndex_Stable` 冻结了 `alice / bob / "" / carol` 的预期 bucket 值作为
  regression trip-wire。哪天 xxhash 库升级改了输出，这个测试会提醒要做
  "coordinated re-shard" 而不是静默把所有 user 重新分桶。

### 2. BFF 多 shard 路由：[bff/internal/client/sharded.go](../../bff/internal/client/sharded.go)

```go
ShardedCounter struct { clients []Counter } // i 对应 shard_id=i
func (s *ShardedCounter) pick(userID) Counter {
    return s.clients[shard.Index(userID, len(s.clients))]
}
```

- 实现 `Counter` 接口，对 REST 层完全透明（`bff/internal/rest` 一行不改）。
- 五个方法（PlaceOrder / CancelOrder / QueryOrder / Transfer / QueryBalance）
  每个 proto request 都有 `UserId` 字段，`pick(req.UserId)` 选目标 client。
- `pick("")` panic —— auth middleware 应该先拦住空 user，走到 counter 前还是
  空就是 BFF 自己 bug，比默默走 shard-0 好。
- 兼容性：main.go 同时接 `--counter-shards=a,b,c`（新）和 `--counter=a:b`
  （单 shard，dev 一行启动），二选一。

### 3. Counter shard ownership guard：service.OwnsUser

```go
type Config struct { ShardID, TotalShards int; ProducerID string }

func (s *Service) OwnsUser(userID string) bool {
    if s.cfg.TotalShards <= 0 { return true } // legacy / single-shard
    return shard.OwnsUser(s.cfg.ShardID, s.cfg.TotalShards, userID)
}
```

在每个 gRPC 入口（`Transfer` / `PlaceOrder` / `CancelOrder` / `QueryBalance`
/ `QueryAccount` / `QueryOrder`）先调 `OwnsUser(req.UserID)`，miss 返回
`ErrWrongShard`，映射到 gRPC `FailedPrecondition`。

trade-event consumer **不加** 显式 guard —— 它依赖现有的 `Orders().Get(id)
== nil` 隐式过滤：只有本 shard 下过单的 user 才有 order，其他 shard 的 user
trade 进来时 orders map 命中不到，settlement 整个跳过。加显式 guard 也可以，
但要每条事件算一次 hash，ROI 不高。记在"未来工作"。

`TotalShards=0` 保留给 dev / 单元测试，走 legacy "claim everything" 分支。

## 备选方案 (Alternatives Considered)

### A. 让 BFF 先查 etcd 里的 shard 路由表，不做硬编码 hash

- 优点：切主 / 重分片时 BFF 自动跟随。
- 缺点：MVP 没有 etcd 主备选举（MVP-12 才做）；现在强引入 etcd 依赖代价大。
- 结论：等 MVP-12 再改；当前 `--counter-shards` csv 静态配置够用。

### B. 用 Go `maphash` 代替 xxhash

- 优点：零外部依赖。
- 缺点：`maphash` 的 Hash 是 process-随机 seed；跨进程一致 hash 要自己维护
  seed，风险不值得。
- 结论：xxhash 有明确的稳定 API 和跨语言实现，换别的 hash 得有同样质量。

### C. Counter 完全信任 BFF 路由，不做 ownership 检查

- 优点：少一行 shard 算式。
- 缺点：配置错一次（shard id 重排 / csv 顺序错）就可能把 user 的 order 写到错
  shard 的 state 里，之后切回来数据对不上。
- 结论：守一层。`FailedPrecondition` 让上游立刻看到问题。

### D. trade-event consumer 也做显式 shard filter

- 优点：更均匀的 CPU 分布（skip 早，不进 sequencer）。
- 缺点：每条事件多一次 hash；现有 implicit filter（Orders() miss）已经把脏数
  据挡住；只有 "没 order 的 user 的垃圾事件" 会多进一步。
- 结论：未来有 perf 需求再加（测试驱动）。

## 理由 (Rationale)

- **纯代码层扩展**：复用现有"每进程一个 shard"结构，只加了 BFF 侧 N-fan-out
  和 service 层 guard，无需改 Kafka topic / 存储布局。
- **hash 稳定性是正确性前提**：每次 Go/库升级都能自动触发冻结值断言，发现 break
  不是 "用户数据随机飘"。
- **BFF 与 Counter 共享同一个 `pkg/shard`**：杜绝"两侧算出不同 shard 导致
  Counter 反手拒绝"的自伤场景。

## 影响 (Consequences)

### 正面

- 可以起 10 个 counter 进程 + BFF 一条命令串起来，每个 user 请求到确定 shard。
- Counter 启动时新增 `--total-shards=10`，一眼能看出部署意图；测试 / 老环境
  传 0 保持 legacy。
- 测试覆盖：`ShardedCounter` 方法级路由 + `OwnsUser` 分叉 + hash 稳定常量。

### 负面 / 代价

- 现在只是"能路由"，**扩容（10 → 20）仍然需要 coordinated re-shard** —— ADR-0010
  已经指明，本 ADR 不解决。
- `--counter-shards` 的 csv 顺序 **就是** shard_id 顺序 —— 错一位会把 user 流量
  集体送到错 shard。部署脚本必须严格按 0..N-1 写，建议后续 k8s StatefulSet 名
  字就用 `counter-shard-0..9`。
- Counter trade-event 处理对 non-owned user 目前靠 implicit `Orders().Get`
  过滤；OrderAccepted 之前的阶段（e.g. transient journal event for another
  user）若哪天新增逻辑会绕过 Orders() miss，要额外加 guard。

### 中性

- 现有单 shard 部署（dev / test）不受影响：`--total-shards=0` 关 guard，行为
  和 MVP-7 完全一致。

## 实施约束 (Implementation Notes)

- hash：`xxhash.Sum64String(userID) % totalShards`。改 hash 需要：
  1. 更新 `TestIndex_Stable` 冻结值，
  2. ADR 标题加 "Superseded by NNNN"，
  3. 做数据迁移（所有 counter shard 先 snapshot，按新 hash 切分 user → 再换进
     程启动）。
- `--counter-shards` 在 bff 里现在没有连通性健康检查 —— `grpc.NewClient` 懒
  连接，第一条请求才发现 endpoint 挂了。未来可以在启动时做一轮 `Ping`。
- 现有 `counter/internal/engine.ShardState` 已经携带 `ShardID`，snapshot 里
  也冻结（`snapshot.ShardID`）。启动时 `ShardID` 不匹配会 fatal：MVP-8 没改
  这个路径，但提醒：如果某天 shard id 重排，要先清 snapshot 文件。
- `ErrWrongShard` → `codes.FailedPrecondition` 意图：让 BFF 的 gRPC 客户端
  把这个错误和普通业务错误区分，监控里能立刻看到"路由不匹配"的数量。

## 未来工作 (Future Work)

- MVP-11：Match 多实例 + etcd 配置驱动 symbol 分配。
- MVP-12：Counter/Match HA，主备切换时 `--counter-shards` 里的 endpoint 随之
  变化，考虑 BFF 动态重连。
- MVP-13：Push 多实例，LB hash 规则与 `pkg/shard.Index` 的 hash 对齐（可以直
  接复用同一函数）。
- 重分片工具：`counter/cmd/reshard`（snapshot → 切 → 输出新 snapshot），
  ADR-0010 已点名。

## 参考 (References)

- ADR-0010: Counter 按 user_id 分 10 个固定 shard
- ADR-0018: Counter Sequencer
- 实现：
  - [pkg/shard/shard.go](../../pkg/shard/shard.go)
  - [bff/internal/client/sharded.go](../../bff/internal/client/sharded.go)
  - [bff/cmd/bff/main.go](../../bff/cmd/bff/main.go)
  - [counter/internal/service/service.go](../../counter/internal/service/service.go)
  - [counter/cmd/counter/main.go](../../counter/cmd/counter/main.go)
