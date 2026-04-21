# ADR-0058: Counter 虚拟分片 + 实例锁 + 动态迁移

- 状态: Accepted（phase 1–7 全部落地，见"分阶段实施"表）
- 日期: 2026-04-21 提出；2026-04-22 全部落地
- 决策者: xargin, Claude
- 相关 ADR: 0001（Kafka SoT）、0002（Counter HA via etcd lease）、0003（Counter-Match via Kafka）、0004（counter-journal）、0010（Counter 10 固定 shard，本 ADR 取代）、0017（Kafka transactional.id）、0027（Counter sharding rollout）、0031（Counter cold standby rollout）、0048（Snapshot + offset 原子绑定）、0049（Snapshot protobuf/json）、0050（Match input topic per-symbol）、0057（Asset service + Transfer Saga）

## 术语 (Glossary)

| 本 ADR 字段 | 含义 | 业界对标（仅供读者映射） |
|---|---|---|
| `virtual shard` / `vshard` | 虚拟分片。总数 256，用户通过 `farmhash(user_id) % 256` 稳定绑定，永不改变 | — |
| `physical node` | 承载若干 vshard 的 counter 进程实例 | — |
| `assignment` | vshard → node 的动态映射，维护在 etcd | — |
| `coordinator` | 集群内选出的临时 leader，维护 assignment、处理迁移；不是独立部署单元 | Kafka Controller、ClickHouse Keeper 中的 Leader 角色 |
| `epoch` | vshard 的主权版本号。每次换 owner 递增，用于 fencing 老 owner 的事务 | — |
| `owner` | vshard 当前的 owner node（处理该 vshard 所有读写的进程） | — |
| `instance lease` | 每个 counter node 在 etcd 持的心跳 lease（一台机器一把） | — |
| `cold handoff` | 冷切迁移：老 owner 停流量 → flush → snapshot → 换 owner → 新 owner 恢复 | — |

## 背景 (Context)

### 现状（ADR-0010 / 0027 / 0031）

1. Counter 分 10 固定 shard，按 `farmhash(user_id) % 10` 路由
2. 每个 shard 是 primary + cold standby 两节点（ADR-0031），共 20 节点
3. 切主通过 etcd lease 选主（ADR-0002 / 0031）
4. snapshot 存本地盘，与 Kafka offset 原子绑定（ADR-0048）

### 遇到的问题

1. **粒度粗**：10 shard 意味着单 shard 已经占 1/10 流量；市商等热点用户打爆单 shard 时只能整 shard 迁，代价极大
2. **扩缩容困难**：shard 数一旦定下来，增减机器需要全量 re-shard，运维不可做
3. **cold standby 是浪费**：备机不处理流量，资源利用率 50%
4. **跨 biz_line 划转（ADR-0057）放大问题**：asset-service 加入后 Counter 承接的 transfer saga 调用会让热点更尖锐

### 为什么现在做

1. 未上线，无数据迁移负担（MEMORY: breaking change 无需顾虑）
2. ADR-0057 的 asset-service 即将引入，现在固化 Counter 的分片模型成本最低
3. ADR-0048（snapshot + offset 原子）和 ADR-0049（protobuf）已为"以 snapshot 为权威恢复"打好底

## 决策 (Decision)

### 1. 虚拟分片：256 vshard，hash 永久固定

- 用户到 vshard：`vshard_id = farmhash(user_id) % 256`，永不变更
- vshard 是**快照、订阅、锁、迁移**的最小单位
- 一台 counter node 同时承载若干 vshard（典型 `256 / N_nodes`），动态分配

256 是本 ADR 选定的固定值（见"理由"）。未来如果需要再细分，只能停服重算 hash，所以宁可选大一点。

### 2. Kafka topic 对齐到 vshard 粒度

- `trade-event`（Match → Counter）：**256 partition**，`partition = xxhash64(user_id) % 256`（Match 侧和 Counter 都调 `pkg/shard.Index`，保证两边一致）
  - 结果：1 vshard ↔ 1 partition，恢复时只需 seek 一个 partition
  - Producer 用 `kgo.ManualPartitioner`，`Record.Partition` 由生产者显式指定；不依赖 broker 端的 key hash
- `counter-journal`（Counter 自产）：**256 partition**，同样 key、同样 ManualPartitioner
- `order-event`（Counter → Match）：**不动**，仍按 symbol 分区（ADR-0050）。这条路不需要按 vshard 对齐，因为 Match 按 symbol 分 shard

**Trade 双发（phase 4）**：Match 产一笔 Trade 时，maker 和 taker 的 user 分别落在各自的 vshard。consumer-group 广播模式在 vshard 架构下不存在（每个 worker 只订阅自己的 partition），所以 Match 必须为 **maker 和 taker 分别 emit 一条 TradeEvent** —— 同一个 Trade payload、不同的 `Partition`（一条 keyed 到 maker 的 vshard，一条到 taker 的 vshard）。

- self-trade（maker_user_id == taker_user_id）只发 1 条
- 两条 event 在 match 的 transactional producer 里同一事务内原子 publish
- 下游消费者的 dedup 责任：
  - Counter：每 vshard 只收自己 user 的那份，`(user, symbol) match_seq` guard 继续生效
  - Push / trade-dump：按 user_id / trade_id 去重（现有机制已覆盖）
  - Quote：需要新增"只处理 taker 侧"过滤或按 trade_id 去重（ADR-0058 phase 4b 跟进）

这个决策避免了 vshard 间的 cross-RPC 结算通道，保持 vshard 隔离。代价是 trade-event 事件量 2x（自交易除外）。

### 2a. Partition 路由实现约束（phase 4）

- Match 和 Counter 的生产者都用 `kgo.ManualPartitioner()`
- 所有 trade-event / counter-journal record 必须显式填 `Record.Partition = shard.Index(user_id, vshard_count)`
- `shard.Index` 是 `pkg/shard` 已提供的 xxhash64 实现（ADR-0010 起服役，ADR-0058 延用）
- 生产者侧的 `vshard_count` 通过 flag 传入（典型 256），和 counter 的 `--vshard-count` 保持一致；不一致等同于路由错乱，必须启动期校验

### 3. 实例锁 + 内建 Coordinator + assignment 表

etcd 数据模型：

```
/cex/counter/
  ├── coordinator/leader                ← etcd Election API，选出 coordinator
  ├── nodes/{node-id}                   ← JSON {endpoint, capacity, started_at}
  │                                       绑定 lease TTL=10s（实例锁）
  └── assignment/vshard-{000..255}      ← JSON {owner, epoch, state, target?}
                                          state ∈ {ACTIVE, MIGRATING, HANDOFF_READY}
```

**Coordinator 不是独立进程**——集群内任一 counter node 通过 `coordinator/leader` 选举出来的临时角色；leader 挂了 etcd 秒级选新。leader 切换期间 data plane（路由、消费、生产）继续工作，只有迁移 / failover 被短暂挂起。

职责：
- 监听 `nodes/` 变化（节点上下线）
- 维护 `assignment/` 状态机
- 初始分配（集群首次启动）
- 故障 failover（某 node lease 过期 → 重新分配它名下的 vshard）
- 运维触发的主动迁移（API 调用）

### 4. Fencing：epoch + transactional.id

- 每次 vshard 换 owner `epoch += 1`
- Kafka 生产者 `transactional.id = "counter-vshard-{id}-ep-{epoch}"`
  - 新 epoch 的 producer init 会 fence 掉旧 epoch 的未提交事务（ADR-0017 的机制延伸到 vshard 维度）
- Counter node 在持有 vshard 期间持续 watch 其 assignment，如果发现 `owner != self || epoch > mine`，立即停止处理（自我 fence）

### 5. 共享存储 Snapshot（S3 / S3 兼容）

- 每 vshard 一份 snapshot，key = `{prefix}vshard-{000..255}.pb`
- 继承 ADR-0048：含 per-partition offset，恢复时 seek
- 继承 ADR-0049：protobuf 主 / JSON debug
- 本地盘选项（`--snapshot-backend=fs`）保留给单机开发
- 生产走 S3（AWS S3 / MinIO / 其他兼容实现）

具体实现细节见**本 ADR 的分阶段实施**，不在这里展开。

### 6. BFF 客户端路由

- BFF 启动时 List + Watch `/cex/counter/assignment/` 和 `/cex/counter/nodes/`
- 本地缓存：`vshard_id → node_id → endpoint`
- 请求路由：

```go
vshard := farmhash(userID) % 256
owner  := bff.assignments[vshard].Owner
node   := bff.nodes[owner]
client := bff.pool.Get(node.Endpoint)
client.PlaceOrder(...)
```

迁移窗口内 BFF 可能持有 stale 缓存，此时请求打到老 owner 会收到 `FAILED_PRECONDITION(epoch_mismatch)` 错误，BFF 刷新 cache 后重试。

### 7. 冷切迁移协议（Cold Handoff）

```
t=0  ACTIVE(owner=A, ep=N)
       │
       │ coordinator 决定 A→C 迁移（运维触发或 failover）
       ▼
t=1  MIGRATING(owner=A, ep=N, target=C)
       │
       │ A watch 到 → 停接新请求（拒绝并让 BFF 重试）
       │        → flush transactional producer
       │        → capture ShardSnapshot + offset
       │        → 上传 S3
       │        → CAS 改 state=HANDOFF_READY
       ▼
t=2  HANDOFF_READY(owner=A, ep=N, target=C)
       │
       │ coordinator CAS: owner=C, epoch=N+1, state=ACTIVE, target=<cleared>
       ▼
t=3  ACTIVE(owner=C, ep=N+1)
       │
       │ C watch 到 → 从 S3 拉 snapshot → Restore → seek trade-event partition N
       │        → 以 ep=N+1 的 transactional id 启动 producer → 开消费
       ▼
t=4  data plane on C
```

窗口大小：典型 1-5 秒（取决于 snapshot 大小和 S3 延迟），本 ADR 不追求亚秒级切换。

### 8. 取消 cold standby 模式

ADR-0031 的 primary + cold standby 双节点模式被替换。新模型下所有 counter node 都是 active，都处理流量。某 node 挂了，它承载的 vshard 被 coordinator 重新分配给其他活着的 node（本质等价一次冷切迁移）。

## 备选方案 (Alternatives Considered)

### 【重点】锁粒度三方案对比

这是 ADR-0058 最关键的决策点，单独详写。

#### 方案 A：每 vshard 一把 etcd lease（无中心 coordinator）

**结构**：256 把 lease，每 vshard 一把；node 抢占 lease 即获得 vshard 所有权。

**流程**：
- node 启动 → 读所有 vshard lease 状态 → 按容量抢闲置的
- 负载不均时 node 互相协调（释放 + 抢占）做再平衡
- 迁移 = 老 owner 释放 lease + 新 owner 抢占

**优点**：
- 无 coordinator，无 leader SPOF
- 锁粒度天然对齐 vshard（不需要额外的 assignment 表）

**缺点**：
- **自平衡算法复杂**：否则所有 node 启动时都抢前 N 个 lease，分布严重不均；需要 node 间协商或加随机扰动
- **lease 续约开销大**：每 node 若承载 32 vshard，就要续 32 把 lease（etcd 压力 O(256)）
- **迁移窗口竞争脆弱**：老 owner 释放 lease 的瞬间，多个候选 node 可能同时抢，需要重试
- **可观测性差**：运维要聚合 256 把锁的持有者才能看到分布全景，没有"一张表直接看"的接口

#### 方案 B（**选中**）：实例锁 + assignment 表 + 内建 coordinator

**结构**：每 node 1 把 instance lease；1 把 coordinator lease（leader 选举）；assignment 表记录 `vshard → {owner, epoch, state}`。

**流程**：
- node 启动 → 注册 instance lease → 被动等 coordinator 分配
- coordinator（leader）监听 nodes 变化 + 维护 assignment
- 迁移 = coordinator CAS 更新 assignment，owner node watch 到后按状态机执行

**优点**：
- **lease 数量 O(N+1)**（N 个 instance + 1 个 coordinator），不是 O(256)
- **分配逻辑集中**：一处代码、一处测试，可观测性好（assignment 表就是全景）
- **CAS (epoch) 保证迁移原子性**：不会出现两个 node 同时认为自己是 owner
- **fencing 强**：epoch 单调递增，老 owner 的 Kafka 事务被新 epoch 天然 fence

**缺点**：
- coordinator 是逻辑上的中心角色（但内建选主、不引入独立服务、秒级切换）
- leader 切换期间短暂（< lease TTL）不能处理迁移，但不影响 data plane

#### 方案 C：混合（per-vshard lease + Rendezvous Hashing 确定性分配）

**结构**：256 把 vshard lease，但 node 用 HRW（Rendezvous Hashing）算出"应该是我"的那几把再去抢。

**流程**：
- 每个 node 独立计算 `score(vshard, node) = hash(vshard || node)`
- 按 score 排序决定每个 vshard 的首选 owner
- 扩缩容时 HRW 天然最小化迁移量

**优点**：
- 无中心，负载分布是确定性的
- 扩缩容迁移量最小（HRW 的经典优势）

**缺点**：
- **watch 视图不一致期可能 split-brain**：两个 node 都认为自己应该持有某 vshard，只能靠 lease 过期兜底
- 仍然有 256 把 lease 的 etcd 压力
- fencing 靠 lease TTL，相比 CAS epoch 时序更弱（epoch 是单调序号，TTL 是时间窗口）
- HRW 算法虽然经典但实现和测试比"中心分配"复杂

#### 对比表

| 维度 | A：每 vshard 一把锁 | B：实例锁 + assignment（**选中**） | C：HRW 混合 |
|---|---|---|---|
| etcd lease 数 | `256 + N` | `N + 1` | `256 + N` |
| 需要 coordinator | 否 | 是（内建选主）| 否 |
| 分配算法 | node 间自协调 | coordinator 中心化 | 客户端确定性 |
| 迁移原子性 | 释放-抢占（脆）| **CAS on epoch（强）** | lease 过期（脆）|
| 负载均衡 | 需自协调 | coordinator 一处 | 天然（HRW）|
| split-brain 防御 | lease 过期 | **CAS epoch + fencing** | lease 过期 |
| 可观测性 | 差（要聚合 256 锁）| **好（assignment 表即全景）** | 中 |
| leader SPOF | 无 | 有（秒级切换）| 无 |
| 实现复杂度 | 高 | 中 | 高 |

#### 为什么选 B

1. **迁移频率低**（扩缩容 / 故障），中心化调度不是瓶颈；一致性 > 吞吐
2. **epoch + CAS 的原子性保证比 lease 竞争强**：迁移过程中永远只有一个 owner
3. coordinator 内建选主，不引入独立服务 / SPOF；leader 挂了秒级切新，且 data plane 不受影响
4. **运维可观测性**：`etcdctl get /cex/counter/assignment/ --prefix` 一条命令看全景，对排障至关重要
5. A/C 的"无中心"优势在 256 vshard 这个规模上不明显，复杂度代价却实打实

### 其他备选

**B1. trade-event partition key 选择：按 `hash(user_id) % 256` vs 显式 `partition = vshard_id`**

选前者。两者数学等价，但前者不要求 Match 知道 "user → vshard" 的绑定函数，Match 保持 user_id 输入 / partition 输出的无状态逻辑；后者 Match 要嵌入一份 hash 常量，耦合更重。

**B2. Snapshot 存储：S3 vs EFS vs 本地 + rsync**

选 S3（S3 兼容也可，如 MinIO）。

- **EFS** 备选方案：POSIX 语义，开发友好，但跨 AZ 带宽成本和 S3 相近；如果运维偏好 NFS 可切，接口不变
- **本地 + rsync**：迁移时从旧 owner 拉文件，多一条 node-to-node 链路，且故障 node 的本地盘可能不可访问，不选

**B3. BFF 路由：客户端 watch vs 服务端转发**

选客户端 watch。省一跳 RPC、避免 counter 之间互相转发造成的复杂度。stale cache 靠 epoch 错误触发重试。

**B4. 迁移协议：冷切 vs 热切（双写）**

选冷切。热切需要老 owner + 新 owner 并行处理同一 vshard + 状态合并，分布式系统里极易出错。冷切窗口 1-5 秒可接受（非频繁操作）。

**B5. vshard 数量：64 / 256 / 1024**

选 256。

- 64：和现有 counter-journal 一致，但扩容到 30+ node 时平均每 node 只 2 vshard，粒度不够
- 256：典型部署 5-20 node，每 node 13-50 vshard，粒度合适；固定值也好心算
- 1024：粒度更细但 etcd key 数 × 4，snapshot 文件数 × 4，收益有限

### 被取代的 ADR

- **ADR-0010**（Counter 10 固定 shard）：用 256 vshard 取代
- **ADR-0027**（Counter sharding rollout）：primary/backup 部署流程废弃
- **ADR-0031**（Counter cold standby）：cold standby 模式取消，改为全 active

## 理由 (Rationale)

- **256 vshard 给扩缩容留足空间**：从 5 node 扩到 30 node 不需要 re-shard，只是 coordinator 重新分配
- **实例锁 + assignment 的复杂度 / 价值比最优**：在 256 规模上，中心化调度的复杂度 < 分布式自协调
- **CAS epoch 是关键**：给了"迁移过程中 at-most-one owner"的强保证，是所有分布式锁方案里最简单可靠的
- **Kafka 对齐到 256 partition**：1 vshard = 1 partition，恢复时 seek 一个 partition，offset 语义清晰
- **共享存储是迁移的前置条件**：否则迁移时要从老 node 拉本地文件，失败故障不可恢复

## 影响 (Consequences)

### 正面

- **粒度细**：热点单用户只影响 1/256 流量，可以单独迁走
- **滚动扩缩容**：加减 node 不停服，coordinator 自动再分配
- **资源利用率翻倍**：取消 cold standby，所有节点都是 active
- **snapshot 在共享存储**：迁移、failover、故障恢复统一用一套机制
- **运维可见性**：assignment 表直接看全景

### 负面 / 代价

- **引入 coordinator 角色**：虽然内建选主、不增部署单元，但多了一个需要理解和监控的组件
- **Kafka topic 重建**：trade-event 10 → 256 partition，counter-journal 64 → 256 partition。未上线无实际代价，但未来如果要再改 vshard 数就要再重建一次
- **BFF 路由复杂度上升**：从静态配置到 etcd watch + 缓存 + 刷新
- **S3 依赖**：之前 counter 无对象存储依赖，本 ADR 引入
- **迁移窗口**：1-5 秒不可服务窗口（但这段时间 BFF 重试，用户感知是延迟）

### 中性

- 256 数量不可变（未上线还能改；上线后只能停服重算）
- ADR-0002 的"etcd lease 选主"语义延续，只是从"shard 主权锁"变成"node 存活锁"（instance lease）
- 用户到 vshard 的绑定永久，便于做 per-user 定序（ADR-0018 继续适用）

## 实施约束 (Implementation Notes)

### etcd schema 初始化

```
/cex/counter/
  ├── coordinator/leader              ← etcd Election，value = node_id
  ├── nodes/{node-id}                 ← {endpoint, capacity, started_at_ms}
  │                                     lease TTL=10s
  └── assignment/vshard-000           ← {owner, epoch, state, target?}
      assignment/vshard-001           ← ...
      ...
      assignment/vshard-255
```

首次启动：coordinator 当选后若 `assignment/` 为空，按 HRW 或轮转做初始分配。

### Coordinator 选举

- 用 etcd `concurrency.Election`
- leader 的 lease TTL = 10s
- 多 coordinator 候选正常运行时耗费很低（只持 lease）
- leader 丢 lease → etcd 自动选新（秒级）

### Fencing 细节

- Kafka transactional id = `counter-vshard-{id:03d}-ep-{epoch}`
  - 新 epoch 的 producer `InitProducerId` 会 fence 旧事务
- Counter node 持续 watch 自己持有的 vshard 的 assignment key
  - 发现 `owner != self` 或 `state != ACTIVE`：停止消费 + producer close
- BFF 调用 Counter RPC 带上 BFF 认为的 `(vshard_id, epoch)`
  - Counter 检查 epoch 不匹配时返回 `FAILED_PRECONDITION`
  - BFF 刷新 cache 后重试

### 分阶段实施（全部 Done）

本 ADR 分阶段推进并已全部落地。每阶段独立 merge、独立验证。阶段 3 是 data plane 切换的分水岭：之前仍是 10 固定 shard + snapshot 位置变化；之后切到 vshard 模型。

| 阶段 | 内容 | 状态 | commit |
|---|---|---|---|
| 1 | snapshot 共享存储（fs + s3 后端），shard 模型不动 | ✅ Done | [531fe8a](https://github.com/cch123/opentrade/commit/531fe8a) |
| 2 | etcd assignment schema + coordinator（选主 + 初始分配） | ✅ Done | [531fe8a](https://github.com/cch123/opentrade/commit/531fe8a) |
| 3a | main.go 接 cluster（observer 模式，data plane 不动） | ✅ Done | [531fe8a](https://github.com/cch123/opentrade/commit/531fe8a) |
| 3b-1 | trade-event per-partition consumer + WatchAssignedVShards | ✅ Done | [721cf91](https://github.com/cch123/opentrade/commit/721cf91) |
| 3b-2A | VShardWorker 生命周期 | ✅ Done | [9e9fdd3](https://github.com/cch123/opentrade/commit/9e9fdd3) |
| 3b-2B | Manager + gRPC dispatcher + main.go 切换 | ✅ Done | [a2ba9bc](https://github.com/cch123/opentrade/commit/a2ba9bc) |
| 4a | Match dual-emit + ManualPartitioner；Counter counter-journal 同样 | ✅ Done | [e34ed12](https://github.com/cch123/opentrade/commit/e34ed12) |
| 4b | Quote 侧 match_seq watermark 去重 dual-emit | ✅ Done | [5169f73](https://github.com/cch123/opentrade/commit/5169f73) |
| 4c | Kafka topic pre-create 256 partition（deploy 脚本） | ✅ Done | [2f1474d](https://github.com/cch123/opentrade/commit/2f1474d) |
| 5 | BFF watch etcd + epoch 错误重试 | ✅ Done | [8c8c362](https://github.com/cch123/opentrade/commit/8c8c362) |
| 6 | 冷切迁移协议 + `counter-migrate move` CLI | ✅ Done | [65f2239](https://github.com/cch123/opentrade/commit/65f2239) |
| 7 | 自动 failover（node lease 过期触发重分配） | ✅ Done | [d4daa2f](https://github.com/cch123/opentrade/commit/d4daa2f) |
| 7+ | `counter-migrate plan` / `rebalance` 批量扩容入口 | ✅ Done | [8028296](https://github.com/cch123/opentrade/commit/8028296) |

### 后续 backlog（非本 ADR 范围）

- **端到端集成测试**：etcd + Kafka + MinIO 全链路跑一次 migration / failover
- **Admin 状态观察工具**：`counter-status` 打印集群全景（live nodes / 每 node 承载 vshard 数 / 当前 migrating 列表）
- **Coordinator 自动 rebalance**：扩容频率高时把 `counter-migrate rebalance` 的逻辑搬进 coordinator 做常驻
- **reconcile（MySQL 对账，ADR-0008）**：vshard 级重写，或 node 级扫自己承载的 vshard 集合

### CLI flags（counter）

新增：
- `--vshard-count=256`（固定，启动时校验一致）
- `--node-id=counter-node-A`（每个实例唯一）
- `--snapshot-backend=fs|s3`
- S3 相关：`--snapshot-s3-bucket`, `--snapshot-s3-prefix`, `--snapshot-s3-region`, `--snapshot-s3-endpoint`（endpoint 为 MinIO / localstack 留）

删除：
- `--shard-id`, `--total-shards`（被 assignment 取代）
- primary/backup flag（cold standby 模式取消）

### 已知不做（本 ADR 范围外）

- **热切迁移**：冷切够用
- **自动再平衡**：阶段 6 之前靠运维手动触发；自动化留后续 ADR
- **multipart snapshot 上传**：单 vshard snapshot 预期 < 100MB，PutObject 够
- **snapshot 加密**：MVP 不做，可通过 S3 SSE 配置层解决
- **vshard 数动态调整**：256 固定

## 参考 (References)

- ADR-0001: Kafka 作为事件权威源（snapshot 权威性与 Kafka 互补）
- ADR-0002: Counter HA via etcd lease（实例锁语义延续，从 shard 主权变 node 存活）
- ADR-0010: Counter 10 固定 shard（被本 ADR 取代）
- ADR-0017: Kafka transactional.id 命名（epoch 嵌入 txn id 的机制）
- ADR-0018: Counter Sequencer FIFO（per-user 定序在 vshard 内仍然有效）
- ADR-0027: Counter sharding rollout（primary/backup 部署流程废弃）
- ADR-0031: Counter cold standby rollout（cold standby 模式取消）
- ADR-0048: Snapshot + Kafka offset 原子绑定（offset seek 机制继承）
- ADR-0049: Snapshot protobuf/json（格式继承）
- ADR-0050: Match input topic per-symbol（order-event 方向不受影响）
- ADR-0057: Asset service + Transfer Saga（跨 biz_line 划转在 vshard 架构下仍然有效）
- 讨论：2026-04-21 "counter 256 vshard + 实例锁 + 共享存储 snapshot"
