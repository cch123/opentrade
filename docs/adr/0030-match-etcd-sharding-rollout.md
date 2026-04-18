# ADR-0030: MVP-11 Match etcd 驱动 sharding + 热加减 symbol

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0009, 0016, 0019

## 背景 (Context)

ADR-0009 规定 Match 按 symbol 分片、etcd 驱动、支持停机迁移。MVP-1~10 期间
Match 是单实例 + `--symbols` 静态 flag。MVP-11 把 symbol 归属迁到 etcd，并在
runtime 支持 **热加减** symbol（不需要重启进程）。

要回答的问题：

1. etcd 里 key 的布局和 value 的 schema？
2. 启动时如何处理"先 list 后 watch"的时序缝隙？
3. `trading: false` 在 match 侧具体意味着什么？保留 orderbook 还是彻底停？
4. 多个 match 实例看到同一份 etcd 配置，怎么判断谁 own 哪个 symbol？
5. 和现有 `--symbols` 静态模式怎么兼容？

## 决策 (Decision)

### 1. etcd key / value 布局

```
/cex/match/symbols/<SYMBOL>   JSON: {"shard":"match-0","trading":true,"version":"v1"}
```

- `shard`：ownership 标识；**直接用 match 实例的 `--shard-id`**（默认等于
  `--instance-id`，e.g. "match-0"）。ADR-0009 原提到的两层 `match-shard-N → instance_id`
  映射 MVP-11 不实现 —— 当前一个 shard 就是一个 instance，等 HA（MVP-12）上线
  主备才需要区分。
- `trading`：true 才在 match 侧激活；false 等价于"这个 symbol 当前不属于任何
  活跃 match"。
- `version`：纯标签，MVP 只打 log。

路径前缀可配（`--etcd-prefix`），默认 `/cex/match/symbols/`。

### 2. List + Watch，用 revision 避免漏事件

`pkg/etcdcfg` 封装：

```go
List(ctx)  returns (map[symbol]SymbolConfig, revision, error)
Watch(ctx, fromRevision) returns (<-chan Event, error)
```

startup 流程：

```go
snap, rev, _ := src.List(ctx)
for sym, sc := range snap {
    if sc.Owned(shardID) { reg.AddSymbol(sym) }
}
watch, _ := src.Watch(ctx, rev+1)
go applyWatch(watch, reg, shardID)
```

`rev+1` 保证 List 已观察到的 key 不会在 watch 里被重放；List 之后发生的所有
变更都会在 watch 里按序到达。

### 3. `trading: false` 在 match 侧等价于"不 own"

实现上：

| etcd 状态 | match 本地状态 | 动作 |
|---|---|---|
| shard == self && trading == true | 未激活 | AddSymbol（restore snapshot） |
| shard == self && trading == true | 已激活 | no-op |
| shard == self && trading == false | 任意 | RemoveSymbol（drain + final snapshot） |
| shard != self | 已激活 | RemoveSymbol（shard 被迁走） |
| shard != self | 未激活 | no-op |
| Delete | 已激活 | RemoveSymbol |
| Delete | 未激活 | no-op |

ADR-0009 原要求的"trading: false 保留 orderbook 等待撤单"在 MVP-11 **不实现**。
真实停机迁移流程（operator 视角）：

1. 把 symbol 置 `trading: false` —— 源 match 把 orderbook 写 final snapshot 并
   关 worker。后续到来的 `order-event` 仍然发给 Kafka（counter 那一侧此时应该
   先拒单，BFF 也应先下线 symbol 的 UI），落在没 worker 的 symbol 上被 consumer
   当作未知 symbol 丢弃 + 记 log。MVP 接受这个"可能漏掉少量 in-flight 单"的窗口。
2. 改 `shard` 到目标 match 的 id（仍然 `trading: false`）—— etcd 里单键先切
   归属，目标 match 还未激活。
3. 改 `trading: true` —— 目标 match 的 watcher 触发 AddSymbol（restore 从刚刚
   源写的 final snapshot，前提是两边共享 snapshot 存储 / 事先 `rsync`）。
4. 撤下暂停公告。

这比 ADR-0009 的"源在线撤单"简单很多，风险集中在"step 1→3 期间有用户下单
被丢"。由 operator 保证窗口内无新单。snapshot 跨机传输的工具留在 Backlog。

### 4. Ownership 判断：`Owned(shardID)` 单调比较

```go
func (c SymbolConfig) Owned(shardID string) bool {
    return c.Shard == shardID && c.Trading
}
```

两台 match 同时"正确"的前提是 operator 不给同一个 symbol 配冲突 shard。etcd 的
写入是强一致的，不会有 "两个 key 都是 match-0" 的竞态；写 config 错了是人类
错误，match 的任务就是忠实反映 etcd 状态，不做去重裁决。

### 5. `--etcd` 与 `--symbols` 互斥 fallback

- `--etcd=` 空 → 用 `--symbols=BTC-USDT,ETH-USDT` 模式（MVP-1 行为；用于 dev
  和 unit test）。
- `--etcd=host:2379` 非空 → 忽略 `--symbols`，走 etcd 驱动。
- 两者都空 → 启动 validation 失败。

## 备选方案 (Alternatives Considered)

### A. 纯 `--symbols` + 重启迁移
- 优点：零外部依赖。
- 缺点：每次加 / 迁 symbol 需要修改部署脚本 + 重启；撑不了很多 symbol 的体量。
- 结论：保留作 fallback；主路径走 etcd。

### B. `trading: false` 保留 orderbook 等待 in-line cancel
- 优点：忠实复现 ADR-0009 原描述；对 operator 工作流更平滑。
- 缺点：要给 SymbolWorker 加 "paused 模式"（只接 Cancel 不接 Place）；需要
  match 主动广播批量 cancel；counter 还要配合发假的 Cancel 事件。跨服务协作
  工作量翻倍。
- 结论：停机迁移已经在 ADR-0009 里被接受；简化为 "RemoveSymbol + final snapshot"，
  operator 侧承担"先把流量导走"。

### C. 用 `clientv3.Watch(ctx, prefix, WithPrefix())` 直接 watch，List 省掉
- 优点：少一个 RPC。
- 缺点：etcd watch 不保证 revision 起点是"topic head"，用 `WithRev(0)` 只能
  拿到未来事件，启动时就不知道"哪些 symbol 应该已经活着"。必须有 List 来 seed。
- 结论：List + Watch 是 etcd 官方推荐模式。

### D. 多个 match 实例用 consistent hashing 而非手配 shard
- 优点：加实例不用改 etcd。
- 缺点：ADR-0009 已经讨论并拒绝 —— 热门 symbol 独占实例的需求和 hash 冲突。
- 结论：不改。

## 理由 (Rationale)

- **"etcd 反映愿望，match 反映现实"**：match 不做复杂状态机；每次 watch 事件
  都只是一次 "这符合 Owned?" 判断 + 相应 AddSymbol/RemoveSymbol。
- **Registry 分离**：main.go 只做 "etcd → registry" 的翻译，worker lifecycle
  集中在 [registry.Registry](../../match/internal/registry/registry.go)，易测
  易扩展。
- **接口隔离**：[pkg/etcdcfg.Source](../../pkg/etcdcfg/etcdcfg.go) 是 interface；
  生产用 `EtcdSource`，测试用 `MemorySource`（[memory.go](../../pkg/etcdcfg/memory.go)），
  避免起 embedded etcd。

## 影响 (Consequences)

### 正面

- 加 symbol：`etcdctl put /cex/match/symbols/NEW-SYM '{"shard":"match-0","trading":true}'`
  → 对应 match 实例自动 AddSymbol。零运维脚本。
- 迁 symbol：改 `shard` 字段两次（源 false → 目标 true），两边 match 自动
  处理。
- 删 symbol：`etcdctl del`，match 写 final snapshot 并释放内存。
- 测试覆盖：etcdcfg 的 codec + MemorySource、registry 的并发 AddSymbol /
  RemoveSymbol / Close。

### 负面 / 代价

- **In-flight 丢单风险**：stop-the-world 迁移窗口内（step 1→3）送到源 match 的
  order-event 会找不到 worker，被 dispatcher 丢弃 + log。operator 负责"先停
  BFF/counter 接单"。
- **snapshot 跨机分发未解决**：多 match 实例要共享 `--snapshot-dir`（rsync /
  EFS），否则目标 match 启动时 Orders() 是空的，可能对不上源曾有的挂单。
  Backlog 项：snapshot-to-S3。
- **consumer group 没按 partition 子集订阅**：当前 match 实例仍然消费
  `order-event` 全量，只是收到非 owned symbol 时丢弃。多实例时每台都全量消费
  = Kafka 流量放大 N 倍。ADR-0009 提到"每 shard 消费 6-7 个 partition"，这需
  要 partition-level 订阅控制；留给 MVP 后续专项（不影响正确性）。

### 中性

- etcd 挂了 → match 启动失败（list timeout）或运行中失去 watch（log warn，已
  加载的 symbol 继续工作，不退出）。这是符合 ADR-0001 "Kafka 是权威、etcd 是
  配置"的定位。

## 实施约束 (Implementation Notes)

### 启动 + watch loop

- List 带 10s timeout；超时 fatal 退出，由 supervisor 重启。
- Watch channel 关闭（etcd 端 compaction / 连接断） → `applyWatch` 退出，
  当前已加载 symbol 继续工作。MVP 不自动 reconnect watch；要恢复靠进程重启。
- Shutdown 顺序：
  1. 取消 watch ctx（停止处理新事件）
  2. 关 consumer（停止收 order-event）
  3. `registry.Close()` drain 每个 worker + 写 final snapshot
  4. 关 outbox → producer pump 退出
  5. 关 etcd client

### 迁移 runbook（operator 侧）

```
# 假设把 BTC-USDT 从 match-0 迁到 match-1

# 1. 停新单（BFF / counter 下线 symbol）
curl -X POST bff/admin/symbol/disable -d '{"symbol":"BTC-USDT"}'  # 未来实现

# 2. 在源 match-0 上触发 RemoveSymbol：trading: false
etcdctl put /cex/match/symbols/BTC-USDT '{"shard":"match-0","trading":false}'

# 3. 等待源写完 final snapshot（log line "symbol removed"）
#    同步 snapshot 到目标机
rsync match-0:/data/match/BTC-USDT.json match-1:/data/match/

# 4. 切归属 + 开启
etcdctl put /cex/match/symbols/BTC-USDT '{"shard":"match-1","trading":true}'

# 5. 确认目标 match-1 log "symbol added"

# 6. 恢复 BFF
curl -X POST bff/admin/symbol/enable -d '{"symbol":"BTC-USDT"}'  # 未来实现
```

### 测试覆盖

- [pkg/etcdcfg/etcdcfg_test.go](../../pkg/etcdcfg/etcdcfg_test.go)：
  JSON codec、Owned 判定、MemorySource list/watch/revision skip/cancel。
- [match/internal/registry/registry_test.go](../../match/internal/registry/registry_test.go)：
  AddSymbol / Duplicate / RemoveSymbol / Restore 错误 / Close / 并发 + drain。
- main.go 不跑集成测试（etcd embedded 成本高）；集成手动验证：
  ```
  docker compose up -d
  match --etcd=localhost:2379 --shard-id=match-0 ...
  etcdctl put /cex/match/symbols/BTC-USDT '{"shard":"match-0","trading":true}'
  # 观察 log
  etcdctl del /cex/match/symbols/BTC-USDT
  # 观察 RemoveSymbol
  ```

## 未来工作 (Future Work)

- **Partition-level 订阅**：让每个 match 实例只消费 `order-event` 的特定
  partition 子集（`consumer.Assign([]Partition)` 而非 `ConsumerGroup`）。否则
  多实例扇入流量 N×放大。
- **Snapshot 共享存储**：S3 / EFS。ADR-0006 架构里已经标注，MVP-12 做 HA 时
  顺带落地。
- **`trading: false` 的 in-line pause**（Worker 内 drain 模式）：真正做对"零
  丢单 5s 迁移"时再实现；MVP-11 不做。
- **Watch 自动 reconnect**：etcd watch 被 compaction 或连接抖断后自动 re-List
  + re-Watch。
- **BFF / Counter 消费同一份 etcd 配置**：BFF 拒绝 `trading: false` 的 symbol，
  counter 拒绝写 `order-event`。当前只有 match 侧消费。

## 参考 (References)

- ADR-0009: Match 按 symbol 分片
- ADR-0016: Per-symbol 单线程撮合
- ADR-0019: Match Sequencer
- 实现：
  - [pkg/etcdcfg/etcdcfg.go](../../pkg/etcdcfg/etcdcfg.go)
  - [pkg/etcdcfg/memory.go](../../pkg/etcdcfg/memory.go)
  - [match/internal/registry/registry.go](../../match/internal/registry/registry.go)
  - [match/cmd/match/main.go](../../match/cmd/match/main.go)
