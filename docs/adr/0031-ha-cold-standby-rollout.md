# ADR-0031: MVP-12 Counter/Match HA — etcd lease + cold standby

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0001, 0002, 0005, 0006, 0017

## 背景 (Context)

MVP-0 ~ 11 每个 shard 一个进程、单点运行。ADR-0002 规定最终状态是每个 shard
一主一备、etcd lease 选主、Kafka transactional.id fencing；ADR-0006 进一步
规定快照由备节点产出。

MVP-12 的目标是把 Counter 和 Match 的 HA 跑起来：两个进程、一个主、一个备，
主挂了备自动接。整条链路可用的"可用性"真正上线。

动手前要决定几件事：

1. 备节点是**冷备**（等着，切换时从 snapshot + journal catch-up 启动），还是
   **热备**（持续 tail journal 实时同步 state）？
2. Counter 的 Kafka transactional.id fencing 已经在 MVP-3 搭好，要不要顺带把
   Match 也改成 transactional producer？
3. 备节点怎么做周期性 snapshot？冷备根本没有 state 可打。
4. 现有 HA-disabled 部署要不要保留？

## 决策 (Decision)

### 1. Counter + Match 都采用 **冷备（cold standby）**

两个进程用 **同一份** `--instance-id / --shard-id / --election-path`
启动。etcd `concurrency.Election.Campaign` 决定谁是 primary。**只有 primary
跑全部资源**（producers / consumers / gRPC / symbol workers）；backup 的
进程除了 Campaign 循环和 logger 不开任何 I/O。

这意味着：

- backup 不消费 Kafka、不打 snapshot、不监听 gRPC 端口。
- primary 挂掉 → etcd lease 在 `LeaseTTL=10s` 内到期 → backup Campaign 成功 →
  从 snapshot 目录加载 state → 启动 producers / consumers（一旦开始消费即自然
  catch-up 到 topic 尾巴）→ 恢复服务。
- 切换窗口：**10–30 秒**（lease TTL + catch-up）。ADR-0002 已声明"交易系统可
  接受"。

### 2. Counter fencing：Kafka transactional.id（已有）

Counter 的 `TxnProducer`（MVP-3 起）用 `TransactionalID = --instance-id`。
两个 counter 进程用相同 id；谁先调 `BeginTransaction()` 谁 fence 另一个。
MVP-12 不改这条链路，只是把"开 txn producer"延迟到选主成功之后（
[counter/cmd/counter/main.go](../../counter/cmd/counter/main.go) 的
`runPrimary`）。

### 3. Match fencing：不做 Kafka 层，依赖 etcd lease

Match 当前的 `TradeProducer` 是 idempotent + `acks=all`，不是 transactional。
要升级成 transactional 必须重写 producer + 所有 `Publish` 调用的包一层
BeginTransaction/EndTransaction，工作量独立成块（估 ~500 行），超出本 MVP
预算。

MVP-12 **放弃 Match 的 Kafka fencing**，接受如下 split-brain 窗口：

- 老 primary 网络分区、lease 过期但进程未崩、仍在写 trade-event；
- 同时新 primary 升起、也在写 trade-event；
- 两份都进了 Kafka，下游 Counter 会用 `trade_id` 做 dedup，但同一 trade_id
  会消费两次（幂等应用，最终一致）。

真实风险：**同一 symbol 在两个 primary 上并发撮合 → 两份冲突订单结果**。缓解：

- lease TTL 10s，老 primary 超过 10s 未续 lease 时检测 `LostCh()` 并自行退出
  （`runPrimary` 的 ctx cancel 会关闭所有 goroutine）。
- 部署上：k8s 的 liveness probe + `terminationGracePeriodSeconds=5s` 保证
  进程挂就真挂。

Match 的完整 transactional fencing 留给 **MVP-12b**。

### 4. snapshot：primary 周期 + shutdown final

- Counter：MVP-3 起只有 shutdown-time final snapshot。MVP-12 的 HA 下，
  primary 降级（lease 丢）走 `runPrimary` 的 defer 路径，也会写 final
  snapshot。**周期性 snapshot 暂不引入**（会加入 CPU 和 lock 竞争），等未来做
  "备节点 tail journal" 时把快照角色挪到备。
- Match：MVP-1 起就有 periodicSnapshot（默认 60s），本 MVP 保持不变；
  primary cycle 内每 60s 打一次。

ADR-0006 "备打快照" 在 MVP-12 不落地，需要先做热备（tail journal）才能
实现。列 Backlog。

### 5. `--ha-mode` flag：`disabled`（默认）/ `auto`

- `disabled`：跳过 election，直接跑 `runPrimary`。等价于 MVP-11 行为，dev /
  单实例 / 单测使用。
- `auto`：启动 election loop，每轮 Campaign → runPrimary → Demote → 重新
  Campaign。

`disabled` 是**默认值**：确保所有现有 `--symbols=...` / `--shard-id=0` 等
命令行不受影响。

启动校验：`--ha-mode=auto` 强制要求 `--etcd=host:2379`。

### 6. 一个进程同时持有的共享目录

两个 counter 进程需要共享 `--snapshot-dir`（否则 backup 升 primary 后不知道
state）。部署期望：

- Counter/Match 的 snapshot 目录挂在 EFS / NFS / S3（不在本 ADR 实现）；
  MVP 单机测试直接共享同一本地目录（`docker volume`）。
- 后续 ADR 会做 S3 后端（见 ADR-0006 规划）。

## 备选方案 (Alternatives Considered)

### A. 热备：backup 持续 tail journal 实时同步 state
- 优点：切换窗口 <1s、backup 可以实时打快照。
- 缺点：需要写 "journal event → state" 的 apply 逻辑（counter 侧 ~400 行；
  trade-dump 已经实现类似的，但不能直接复用，因为 Counter 的 state 是内存
  ShardState 而不是 MySQL projection）；Match 侧需要两套 consumer group +
  primary/backup 在 outbox 上的 primary-only pump。本次工程量超预算。
- 结论：推迟到 MVP-12b。

### B. Match producer 升级为 transactional
- 优点：和 Counter 对齐的 fencing 语义，无 split-brain。
- 缺点：`TradeProducer` / `Pump` / 测试都要改；每条 trade-event 独立事务 vs
  按 poll batch 打包的权衡也没定型。
- 结论：推迟；etcd lease 的 10s 窗口对 MVP 可接受。

### C. 用 pub-sub 或 health check 做 failover 而不是 etcd lease
- 优点：少一个依赖。
- 缺点：ADR-0002 明确选择 etcd lease + "Kafka 即 SoT" 模型；偏离要重开 ADR。
- 结论：拒绝。

### D. 主备都 serve gRPC，backup 返 FailedPrecondition
- 优点：客户端可以向任一实例发、集群自己路由。
- 缺点：前提是要 backup 知道自己是 backup；这和 "backup 不消费" 的简化冲突。
- 结论：拒绝；LB / BFF 已经是路由层（MVP-8 的 `pkg/shard` 选 shard；某个
  shard 只有一个 endpoint 指向当前 primary）。

### E. `--ha-mode=auto` 作为默认值
- 优点：生产路径一致。
- 缺点：单元测试 / 开发环境会无脑尝试连 etcd 失败。
- 结论：保留 `disabled` 作为默认，生产显式启用。

## 理由 (Rationale)

- **最小实现闭环**：冷备 + etcd lease 只需要加一层 outer loop，`runPrimary`
  完全复用现有代码。ADR-0002 已经说这种模型可行。
- **分期 fencing**：Counter 的 fencing 本来就有（Kafka txn id），Match 的
  fencing 是独立工作量，不阻塞 HA 主路径。
- **测试可落地**：`pkg/election` 只测 Config validation；真 election /
  Campaign / Resign 依赖 etcd，放到手工冒烟脚本。

## 影响 (Consequences)

### 正面

- Counter / Match 的 shard 不再是单点。挂了 10–30s 恢复。
- 部署拓扑简化：一个 shard = 两个进程 + 共享 snapshot 目录 + etcd path。
- 接入点清晰：`--ha-mode=auto` + `--etcd=...` + `--election-path=...`。
- 退化路径健全：`--ha-mode=disabled` 等价于 MVP-11 行为，所有测试和 dev
  脚本不改。

### 负面 / 代价

- **切换窗口 10–30s**：MVP-12 的主要取舍。
- **Match split-brain 可能**：已在上面"Match fencing"一节讲清楚，由 k8s
  liveness probe + etcd lease 兜底；不保证零事务重叠。
- **snapshot 共享目录假设**：单机 dev OK，生产要 NFS / S3；本 ADR 不解决。
- **backup 进程空转**：一个 shard 两个进程，其中一个 idle。资源浪费 50%。
  改善路径是做热备（backup 能做其他事：打快照、服务只读查询）。

### 中性

- `runPrimary` 的粒度：现在是"从零加载 + 启动全部"一次性跑完。如果未来
  runPrimary 内部某个子组件启动失败，会返回、election loop 触发 resign →
  等下一轮。etcd lease 会自动释放。

## 实施约束 (Implementation Notes)

### 代码

- [pkg/election/election.go](../../pkg/election/election.go) 封装
  `concurrency.Session` + `concurrency.Election`：`Campaign` / `Resign` /
  `LostCh` / `Observe` / `Leader` / `Close`。
- [counter/cmd/counter/main.go](../../counter/cmd/counter/main.go) 的
  `runElectionLoop` + `runPrimary` 抽象，同款结构复制到
  [match/cmd/match/main.go](../../match/cmd/match/main.go)。
- Counter / Match 的 `parseFlags` 新增：
  - `--ha-mode=disabled | auto`
  - `--etcd=host:2379,...`
  - `--election-path=/cex/{counter,match}/shard-<id>/leader`
  - `--lease-ttl=10`
  - `--campaign-backoff=2s`

### 切换流程（观察者视角）

1. Primary crash / kill -9 → lease 在最长 `LeaseTTL` 秒内过期。
2. Backup 的 `elec.Campaign` 返回（它正好在阻塞等 lease 释放）。
3. Backup 日志打 "became primary"，开始 restore snapshot、开 txn producer
   (Counter 自然 `InitTransactions` fence)、开 gRPC。
4. BFF 那一侧 `client.Dial` 到 primary 的 gRPC endpoint —— 当前 MVP 的 BFF
   路由表是静态 `--counter-shards` csv（ADR-0027），切主后 endpoint 不变
   （两个 counter 用同一个 endpoint？或者 LB 做）。**生产需要 LB**
   在 counter 进程前放 tcp LB（例如 k8s Service 只指向当前 primary）。
   本 MVP 文档化，不实现。

### 手工冒烟

```
docker compose up -d  # kafka + etcd

# 终端 1
counter --ha-mode=auto --etcd=localhost:2379 \
  --instance-id=counter-shard-0-main \
  --grpc-addr=:8081 --snapshot-dir=./data/counter

# 终端 2（一旦启动就变 backup）
counter --ha-mode=auto --etcd=localhost:2379 \
  --instance-id=counter-shard-0-main \
  --grpc-addr=:8082 --snapshot-dir=./data/counter

# kill 终端 1，观察终端 2 在 10s 内变 primary。
```

### 测试覆盖

- [pkg/election/election_test.go](../../pkg/election/election_test.go)：
  Config validation（Path / Value / Endpoints-or-Client 必需）。
- 真选主 + Campaign / Resign 走手工冒烟；放到 docker-compose 冒烟一并跑
  （ADR-0032 规划）。
- 现有 match / counter 单测 全部以 `--ha-mode=disabled` 心智运行（没改动
  runPrimary 内部逻辑）。

## 未来工作 (Future Work)

- **MVP-12b — Match transactional producer + fencing**：改 `TradeProducer` 为
  `kgo.TransactionalID` + Begin/End 包裹 poll batch。
- **热备（MVP-12c）**：backup tail journal/order-event → 实时同步 state →
  切换延迟 <1s + 备节点打快照（ADR-0006 落地）。
- **Snapshot 共享后端**：S3 / EFS，见 ADR-0006。
- **BFF 动态路由**：BFF 通过 etcd watch shard leader 变化，自动切换上游
  endpoint，取代当前静态 `--counter-shards` csv。当前 MVP 靠外部 LB。
- **灰度升级脚本**：按 shard 一对一对滚动升级（architecture.md §13.2）。

## 参考 (References)

- ADR-0001: Kafka as Source of Truth
- ADR-0002: Counter HA via etcd lease
- ADR-0005: Kafka transactions for dual writes
- ADR-0006: Snapshots by backup node
- ADR-0017: Kafka transactional.id fencing
- 实现：
  - [pkg/election/election.go](../../pkg/election/election.go)
  - [counter/cmd/counter/main.go](../../counter/cmd/counter/main.go)
  - [match/cmd/match/main.go](../../match/cmd/match/main.go)
