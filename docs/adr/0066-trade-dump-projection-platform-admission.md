# ADR-0066: trade-dump 角色定位与状态投影平台准入规则

- 状态: Proposed
- 日期: 2026-04-26
- 决策者: xargin, Claude
- 相关 ADR: 0001（Kafka SoT）、0008（trade-dump sidecar）、0023（trade-dump batching）、0028（trade-dump journal projection）、0040（trigger service）、0042（trigger HA cold standby）、0047（trigger long-term history）、0049（snapshot proto/json）、0050（per-symbol order-event topic）、0055（Match orderbook authority）、0058（Counter vshard）、0059 §A.5.5（撮合输出 dispatch）、0060（Counter 异步消费 + TECheckpoint）、0061（trade-dump snapshot pipeline）、0064（Counter 启动 on-demand snapshot RPC）、0065（Asset funding MySQL authority）

## 术语 (Glossary)

| 本 ADR 字段 | 含义 | 业界对标 |
|---|---|---|
| `状态日志` | Kafka topic，每条事件本身就是某个实体在该时刻的最终状态（last-write-wins by id+seq）；apply 一遍即得最终状态 | Kafka Streams changelog topic / Confluent compaction topic |
| `命令流` | Kafka topic，每条事件是动作描述（PlaceOrder / CancelOrder），需要执行业务逻辑才能得到状态 | Event Sourcing 的 command stream |
| `投影 (projection)` | 把 Kafka 事件流转换成另一种存储形式（MySQL 表 / S3 snapshot）的过程 | Materialized view / read model |
| `shadow engine` | 投影服务内部维护的状态副本，用于 apply 状态日志后产出 snapshot | Kafka Streams 的 state store |
| `pipeline` | trade-dump 内的一条端到端处理链：consumer → applier → sink | Kafka Streams 的 topology |
| `准入规则` | 一条新需求要不要塞进 trade-dump 平台的判别条件 | — |
| `状态投影平台` | trade-dump 的新角色定位：消费 Kafka 状态日志 → 维护中间状态 → 输出 MySQL / S3 / RPC | — |

## 背景 (Context)

### 现状：trade-dump 已经偏离 ADR-0008 的原始定位

ADR-0008 把 trade-dump 定位为"trade-event 落 MySQL 的 sidecar"。后续 ADR 逐步给它加职责：

| ADR | 增量职责 |
|---|---|
| 0008 | trade-event → MySQL（trades 表） |
| 0023 | 批量 + commit-order |
| 0028 | counter-journal → MySQL（users / assets / orders 投影） |
| 0047 | trigger-event → MySQL（条件单历史） |
| 0061 | counter-journal → shadow engine → S3 snapshot（替代 Counter 自产） |
| 0064 | gRPC TakeSnapshot RPC（Counter 启动加速） |
| 0065 | asset-journal 不再消费（asset-service 直管 MySQL） |

到本 ADR 落档时，trade-dump 实际是**两条独立 pipeline + 一个 RPC server** 共享同一进程：

```
                     ┌─── Kafka ──────────────────────────────────┐
                     │                                            │
              trade-event       counter-journal       trigger-event
                     │            │       │                  │
                     │  group:    │       │  group:          │
                     │  td-trade  │       │  td-snapshot     │
                     │       group:       │                  │
                     │       td-journal   │                  │
                     │            │       │                  │
   ┌─────────────────┼────────────┼───────┼──────────────────┼────────────┐
   │ trade-dump (one process)     │       │                  │            │
   │   ┌─────────────▼────────────▼─┐   ┌─▼──────────────────▼──────────┐ │
   │   │ SQL pipeline               │   │ Snapshot pipeline             │ │
   │   │  consumer.{Trade,Journal,  │   │  256 × shadow engine          │ │
   │   │   Trigger}                 │   │  (mirrors Counter ShardState) │ │
   │   │  writer.MySQL              │   │  periodic Capture+Save        │ │
   │   │  (idempotent projection)   │   │  → BlobStore (fs|s3)          │ │
   │   └────────────┬───────────────┘   └────┬────────────┬─────────────┘ │
   │                │                        │   ┌────────▼─────────────┐ │
   │                │                        │   │ snapshotrpc :8088    │ │
   │                │                        │   │ TakeSnapshot RPC     │ │
   │                │                        │   │ + housekeeper (TTL)  │ │
   │                │                        │   └────────┬─────────────┘ │
   └────────────────┼────────────────────────┼────────────┼───────────────┘
                    ▼                        ▼            ▼
              ┌──────────┐           ┌──────────────┐  ┌──────────────────┐
              │  MySQL   │           │  S3 / EFS    │  │ Counter recovery │
              │ users/   │           │  vshard-NNN  │  │  hot:  RPC       │
              │ assets/  │           │  .pb /       │  │  cold: blob read │
              │ orders/  │           │  -ondemand   │  │                  │
              │ trades/  │           └──────────────┘  └──────────────────┘
              │ triggers │
              └────┬─────┘
                   ▼
            BFF / Admin
```

### 问题：定位模糊导致后续扩展无章法

最近几次架构讨论暴露了两个症结：

1. **"是不是该把 X 也塞进 trade-dump"无统一判据**

   讨论过的候选：
   - Match 的 snapshot 该不该让 trade-dump 帮忙？
   - Trigger 的 snapshot 该不该收编？
   - 未来 Asset / Push 等服务是否也要塞？

   每次都重新论证一遍，结论难以沉淀。

2. **职责命名错位 + 公共代码错位**

   - 名字 `trade-dump` 暗示只管 trade 落库，但实际跑 5 种 pipeline + RPC
   - `counter/snapshot/` 包是 Counter + trade-dump 共用代码却放在 counter 目录下（已开搬迁任务）
   - `trigger/internal/snapshot/` 自己写了一份 ADR-0049 同款序列化，跟 Counter 实现 95% 重复（已开收编任务）

### 为什么现在做

- ADR-0061 / 0064 把 Counter snapshot 模式跑通，"shadow + S3 + RPC"组合的设计经验已沉淀
- OpenTrade 未上线，breaking change 无顾虑
- 再不定准入规则，下次讨论"X 能不能塞"会再走一遍同样的论证

---

## 决策 (Decision)

### 1. trade-dump 重新定位为：状态投影平台

trade-dump 的角色从"trade-event 落库 sidecar"升级为：

> **OpenTrade 的状态投影平台**：消费 Kafka 状态日志，维护必要的投影状态，输出到 MySQL / 对象存储 / 查询 RPC，供其他服务消费。

它**不**做：
- 不持有任何业务权威状态（撮合 / 账户 / 风控权威性都在原服务）
- 不接收同步业务请求（只接收 read-only / snapshot 类 RPC）
- 不向 Kafka 写状态日志（不参与 EOS 事务，只读消费）
- 不复制原服务的业务规则（撮合 / 风控 / 余额校验）

它**做**：
- 消费符合"状态日志"语义的 Kafka topic
- 维护必要的中间状态（shadow engine / 累加器 / 索引）
- 持久化到 MySQL / 对象存储 / 提供 RPC
- 多集群部署，按 SLA 隔离

### 2. 准入规则：什么能塞进来

#### 准入决策树

```
问 1：这条 Kafka 输入流的每条事件，是否已经包含了"对应实体在该时刻的最终状态"？
       │
       ├─ 是（状态日志） → 问 2
       │
       └─ 否（命令流，需要执行业务逻辑才能得状态） → 拒绝
              例：order-event（PlaceOrder/CancelOrder） → 需要撮合
                  funding-cmd（充值请求）             → 需要风控扣减

问 2：trade-dump 内部 apply 这条流，是否需要执行任何业务规则？
       │
       ├─ 否（仅 upsert / merge by id+seq / 累加） → 接受
       │   例：counter-journal apply → 直接写 balance / order 状态
       │       trade-event 落 MySQL  → INSERT 即可
       │       trigger-event apply  → upsert by trigger_id (LWW by seq)
       │
       └─ 是 → 拒绝（撮合 / 风控 / 信用判断都属此类）

问 3（接受后）：这个 pipeline 的 SLA / 故障域 / 资源使用是否跟现有 pipeline 兼容？
       │
       ├─ 是 → 加进默认 --pipelines
       │
       └─ 否 → 加进 trade-dump 但默认不启用，独立集群部署
```

#### 准入清单（截至本 ADR）

| 服务 / 需求 | 输入流 | 流性质 | 结论 | 备注 |
|---|---|---|---|---|
| Counter snapshot | counter-journal | 状态日志 | ✅ 已收编 | ADR-0061 / 0064 |
| Match (trade-event 历史) | trade-event | 命令结果（已是终态） | ✅ 已收编 | ADR-0023 |
| Match snapshot | order-event | 命令流，需重撮合 | ❌ 拒绝 | 见本 ADR §备选 1 |
| Trigger MySQL 历史 | trigger-event | 状态日志 | ✅ 已收编 | ADR-0047 |
| Trigger snapshot | trigger-event | 状态日志 | 🟡 应收编 | 后续 ADR-0067（建议） |
| Asset funding | — | — | ❌ 拒绝 | ADR-0065 已决：asset-service 直管 MySQL |
| Push 会话状态 | — | — | ❌ 不适用 | active-active 分片，不是状态投影 |

### 3. 部署模型：按 SLA 多集群拆分

trade-dump 通过 `--pipelines` 启动旗标支持选择性启用 pipeline 子集（已实现，ADR-0061 §1）。**SLA 不同的 pipeline 应部署到不同集群**：

```
                ┌────────────── trade-dump binary ──────────────┐
                │  pipelines: sql / snap / future-X / ...       │
                └─────────┬─────────────┬─────────────┬─────────┘
                          │             │             │
              ┌───────────▼──┐ ┌────────▼──────┐ ┌────▼───────────┐
              │ Cluster A    │ │ Cluster B     │ │ Cluster C      │
              │ counter-snap │ │ sql-projection│ │ (future)       │
              │              │ │               │ │                │
              │ --pipelines  │ │ --pipelines   │ │ --pipelines    │
              │   =snap      │ │   =sql        │ │   =X           │
              │              │ │               │ │                │
              │ 副本 ≥ 3     │ │ 副本 ≥ 2      │ │ 按需            │
              │ 关键路径     │ │ 非关键        │ │                │
              │ P1 告警      │ │ P2 告警       │ │                │
              │ 独立 IAM     │ │ 独立 IAM      │ │                │
              │ 独立 S3 path │ │ 独立 MySQL    │ │                │
              └──────────────┘ └───────────────┘ └────────────────┘
```

集群划分原则：
- **关键路径集群**（如 counter-snap）：副本数 ≥ 3，独立 IAM / S3 prefix / 告警等级 P1，资源配额按峰值 ×2
- **非关键路径集群**（如 sql-projection）：副本数 ≥ 2，告警等级 P2，可降级
- **任一 pipeline 故障不能影响其他 pipeline**（独立 consumer group / 故障域，ADR-0061 §6 已约定）
- **集群间不共享 BlobStore prefix / MySQL schema 写权限**（最小权限原则）

### 4. 公共代码归位

为支持多 pipeline + 多集群演化，公共代码应位于 monorepo 顶层 pkg/：

```
pkg/snapshot/             # ADR-0049 格式 / BlobStore / codec（搬迁中）
pkg/projection-runtime/   # 后续：consumer wrapper / writer 接口（视需要）
```

各服务自己的 in-memory state ↔ snapshot 转换留在 `<service>/internal/snapshot/`，**不**搬到 pkg/（避免 pkg/ 变成 service-specific 类型的堆放处）。

### 5. 命名（Future Work，本 ADR 不强制）

`trade-dump` 这个名字与新定位不符。可在以下任一时机重命名：
- 跨集群部署落地时（命令行 / metric label / 部署脚本一起改）
- pkg/projection-runtime 抽离时

候选名：`projection-runtime` / `state-sidecar`。本 ADR 不做强制选择，记录为后续考虑。

---

## 备选方案 (Alternatives Considered)

### 备选 1：把 Match snapshot 也塞进 trade-dump（shadow engine）

让 trade-dump 消费 order-event，跑一份 shadow Match 引擎，per-symbol 维护 orderbook，定期/按需输出 snapshot。

**为什么拒绝**：

- order-event 是命令流，shadow engine 必须**完整复制 Match 撮合逻辑**（红黑树 / 价格层 / cross-matching / STP / IOC / TIF）
- 任一边的 tie-breaker / decimal 精度 / corner case 处理稍有偏差，shadow 与主进程立刻分裂；分裂还查不出来（offset 都对得上）
- Match 升级撮合规则时，trade-dump 必须同步改 + 同步发版，耦合度极高
- 重建成本 ≈ Match 再撮合一遍，**没有外包收益**

Counter 没这个问题：counter-journal apply 是死代码（balance += delta 类），无业务逻辑。

### 备选 2：起独立服务（snapshot-service / projection-service）

把 snapshot pipeline 拆成新进程独立部署。

**为什么拒绝**：

- ADR-0059 §A.5.5 已论证：基础设施复用 vs 拆服务的运维代价（CI / 部署 / 监控 / 告警 / 值班 ×2），团队规模不支持
- `--pipelines` 多集群部署已能拿到独立故障域 / 资源隔离 / SLA 隔离的好处，不必拆服务
- 真要拆，按 `--pipelines=snap` 启动参数即可拆，代码不动

### 备选 3：保持现状不立准入规则，逐 case 评估

**为什么拒绝**：

- 已经反复讨论 Match / Trigger / 未来扩展，无统一判据导致每次重走论证
- 没规则容易滑向"什么都塞"，最后 trade-dump 变成万能模块包袱
- ADR 的价值之一是把判断沉淀成规则，避免讨论复读

### 备选 4：让 trade-dump 接受命令流，但要求原服务把业务逻辑抽成共享 lib

例如 Match 把撮合逻辑抽成 lib，trade-dump import 后跑 shadow。

**为什么拒绝**：

- 撮合逻辑深度依赖 in-memory 数据结构（RB-tree / price level）+ goroutine 模型，"抽成 lib"实际上把 Match 内部架构暴露为公共 API
- 撮合升级时双方代码风险依然存在（lib 升级不同步会分裂）
- 没解决根本问题，只是把耦合从代码复制变成 lib 依赖

---

## 理由 (Rationale)

### 为什么准入用"状态日志 vs 命令流"做判据

这是输入流性质的本质差异：

- **状态日志**的每条事件是 self-contained 的——拿到一条事件就知道该实体的最终状态
- **命令流**的每条事件只描述动作，最终状态需要计算（撮合 / 风控 / 余额校验）

trade-dump 作为投影平台，定位是 "Kafka apply pipeline"，不是 "业务执行 pipeline"。这条边界一旦松掉，trade-dump 会逐步重新实现各业务服务的核心逻辑，最终变成一个全功能后端，违背 ADR-0008 的 sidecar 初心。

### 为什么 Trigger 适合，Match 不适合

二者表面相似（都"per-X 一份 state"），差异在输入：

- Trigger 自己产出 trigger-event 时，事件 payload **是状态变更后的完整 trigger 对象**（[trigger/internal/journal/journal.go:1-9](../../trigger/internal/journal/journal.go) 注释明示："full post-change snapshot on every state transition"）→ apply = upsert by id，零业务逻辑
- Match 的 order-event 是 PlaceOrder / CancelOrder 命令，shadow 要算出 cross 结果

**per-symbol 还是 per-user 不是判据，输入流的完整度才是。**

### 为什么不把所有 snapshot 都搬到 trade-dump

主要顾虑：

1. 不是所有服务的输入都符合准入规则（Match）
2. 短链路服务（如 Quote）状态简单到本地 snapshot 就够，搬过来反而引入跨服务依赖
3. 准入规则的目的是**有原则地接受**，不是"统一收编一切"

### 为什么集群级别要按 SLA 拆而不是同集群跑

`--pipelines` 在同一进程内已经做到 consumer group / 故障域隔离（ADR-0061 §6），但**资源争抢仍存在**：snap pipeline 占满 CPU 时 sql pipeline 受影响。SLA 不同的 pipeline 物理隔离才能保证关键路径的可预测性。

---

## 影响 (Consequences)

### 正面

- **后续扩展有判据**：再有"X 能不能塞 trade-dump"的讨论时，按决策树走一遍即可，不再重复论证
- **trade-dump 内部架构清晰**：所有 pipeline 都符合"状态日志 → projection"模型，没有特例
- **Counter / Trigger 的 snapshot 体系可以共用一套模板**（BlobStore + on-demand RPC + housekeeper + 通用 codec）
- **多集群部署模型成型**：不同 SLA 的 pipeline 物理隔离，故障爆炸半径可控

### 负面 / 代价

- **Match snapshot 仍由 Match 自产**：意味着 Match 升级 snapshot 体系（迁 S3 / cold standby 优化）时不能复用 trade-dump 的基础设施，要在 Match 内独立做一遍
- **准入规则的"边界判定"需要持续维护**：未来如果出现"准状态日志"（每条事件大部分是终态但需要少量计算），需要本 ADR 修订版

### 中性

- **trade-dump 改名暂不强制**：保留命名 hack，但代码内部按新定位组织
- **`--pipelines` 旗标在多集群部署中变成核心运维入口**：旗标稳定性比之前重要，需要纳入命名稳定性考量（未上线期间例外）

---

## 实施约束 (Implementation Notes)

### 前置工作（先行任务）

本 ADR 的实施依赖两个搬迁任务先完成：

1. `counter/snapshot/` → `pkg/snapshot/`（已开任务）—— 公共代码归位 + 清理 service.go 死 offsets 字段
2. `trigger/internal/snapshot/` 收编 `pkg/snapshot.Format` 等公共部分（已开任务）

这两个完成后才能谈 ADR-0067（Trigger snapshot 迁 trade-dump shadow）。

### 后续 ADR 路线图

| 后续 ADR | 主题 | 触发条件 |
|---|---|---|
| ADR-0067（建议） | Trigger snapshot 迁 trade-dump shadow，解耦 NFS 共享 mount | pkg/snapshot 搬迁完成 |
| ADR-0068（建议） | trade-dump 多集群部署拓扑 + IAM / S3 prefix / 告警等级 | 上线前 |
| ADR-0069（建议） | trade-dump 改名 + 目录重组 | 视需要，可与 0068 合并 |

### 不要做的清单

- ❌ 不要在 trade-dump 里写撮合 / 风控 / 余额校验逻辑（命令流路径）
- ❌ 不要让 trade-dump 持有"权威状态"（任何回查必须能从 Kafka 重建）
- ❌ 不要让 trade-dump 接收同步业务请求（只接收 read-only RPC）
- ❌ 不要把 service-specific snapshot 数据结构搬到 pkg/snapshot/（pkg/ 只放真正 service-agnostic 的部分）

---

## 已知不防御场景 (Non-Goals / Known Gaps)

### G1：状态日志事件 schema 漂移

trade-dump shadow apply 与原服务 apply 之间通过 protobuf schema 同步。若原服务 schema 升级且 trade-dump 未同步发版 → shadow 状态错。

**缓解**：proto schema 在 monorepo 内统一，CI 检查兼容性；准入规则只接受 last-write-wins 类语义，schema 错位时降级到"等 trade-dump 发版"，不会污染原服务的权威状态。

### G2：准入规则的灰色地带

某些需求看似命令流但每条事件已附带"执行后状态"（hybrid 流）。此时按事件 payload **是否完整自包含**决定，而不是按 topic 名字。新需求出现 hybrid 流时本 ADR 应修订。

### G3：跨 pipeline 的 SLA 漂移

未来某 pipeline 从非关键路径变成关键路径（例如新业务依赖它）。需要 ADR-0068（部署拓扑）跟踪 SLA 等级变化，及时切到独立集群。

### G4：trade-dump 自身的可用性

本 ADR 不解决 trade-dump 自身挂机时的兜底（Counter 启动 fallback 已由 ADR-0064 提供 blob 读路径，但其他 pipeline 的 fallback 由各服务自行设计）。

---

## 参考 (References)

- ADR-0059 §A.5.5：撮合输出 dispatch 推荐架构（trade-dump 内部新 pipeline 而非独立服务的原始决策）
- ADR-0061：trade-dump snapshot pipeline（首个 shadow engine 落地）
- ADR-0064：Counter on-demand snapshot RPC（hot path 模板）
- Confluent docs：[Kafka Streams stateful operations and changelog topics](https://docs.confluent.io/platform/current/streams/architecture.html#state) — 状态日志 / changelog 概念
- AWS Architecture Center：[Sidecar pattern](https://docs.aws.amazon.com/wellarchitected/latest/framework/sustainability-pillar.html) — 服务边车模式
