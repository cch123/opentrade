# ADR-0067: Trigger snapshot 由 trade-dump shadow 接管（trigger 不再自产 snapshot）

- 状态: Proposed
- 日期: 2026-04-26
- 决策者: xargin, Claude
- 相关 ADR: 0040（trigger 服务）、0042（trigger HA cold standby）、0047（trigger-event 长期历史）、0048（snapshot + offset 原子绑定）、0049（snapshot proto/json）、0058（BlobStore + 共享存储）、0060（Counter 异步消费 + TECheckpoint event 范式）、0061（trade-dump snapshot pipeline，本 ADR mirror counter 路径）、0064（Counter on-demand snapshot RPC，本 ADR mirror）、0066（trade-dump 投影平台准入，本 ADR 是其 §准入清单 trigger 行的落地）

## 术语 (Glossary)

| 本 ADR 字段 | 含义 | 业界对标 / 关联 |
|---|---|---|
| `trigger-shadow` | trade-dump 内部新增的 pipeline，消费 trigger-event 并维护一份 trigger 状态副本 | mirror ADR-0061 counter shadow |
| `TriggerMarketCheckpointEvent` | 新增的 trigger-event 子类型，携带 trigger primary 当前 market-data 消费 offset，使 shadow snapshot 包含 ADR-0048 要求的 offset | mirror ADR-0060 TECheckpointEvent |
| `TakeTriggerSnapshot` RPC | trade-dump 新增的按需 snapshot RPC，trigger 启动时调用拿热路径快照 | mirror ADR-0064 TakeSnapshot |
| `trigger snapshot key` | trade-dump 写 BlobStore 的 key 名，**固定字符串**（如 `trigger`），不带 instance-id | 解决 ADR-0042 副实例接管读不到主实例 snapshot 的问题 |
| `trigger 状态终态` | trigger 当前状态由 (pending triggers, terminal triggers, OCO 客户端 idempotency map, market-data offsets) 完整描述 | — |

## 背景 (Context)

### 现状：trigger 自产 snapshot，违反 ADR-0066 §4

ADR-0058 把 trigger 的 Save/Load 接到了 pkg/snapshot.BlobStore，但 Capture / Save / Restore / Snapshot 数据结构仍然全部在 trigger 自己的代码里 (`trigger/internal/snapshot/`)。这跟 ADR-0066 §4 的"snapshot 核心由 producer 持有，consumer 只 loadSnapshot"模型不一致——counter 已经按这个模型走（ADR-0061，trade-dump 出 snapshot，counter 只 loadSnapshot），trigger 还没。

### 现状：ADR-0042 cold-standby 的 snapshot 共享缺口

trigger snapshot 当前 key 是 `<SnapshotDir>/<InstanceID>`：
- trigger-0（主）写 `/data/trigger-0.pb`
- trigger-1（备）接管时读 `/data/trigger-1.pb`——文件不存在
- 副实例只能从 Kafka market-data 头部全 rescan（ADR-0042 §recovery 已知问题）

ADR-0042 假设两副本走 NFS 共享 mount，但 instance-id-keyed 文件名让"共享"失效。要么：
1. 改 key 成固定字符串（两副本读同一份，但要保证不会同时写）
2. 让 producer 集中写、两副本都只读

按 ADR-0066 路线天然走 (2)：trade-dump 是唯一 producer，写固定 key；两副本只 read。

### ADR-0066 §准入清单的判定

ADR-0066 §准入清单 trigger 那一行：

| 服务 / 需求 | 输入流 | 流性质 | 结论 | 备注 |
|---|---|---|---|---|
| Trigger snapshot | trigger-event | 状态日志 | 🟡 应收编 | 后续 ADR-0067（建议） |

判据走完：
- 问 1（流性质）：trigger-event 每条事件 payload 是状态变更**后**的完整 trigger 对象（[trigger journal 注释](../../trigger/internal/journal/journal.go) 明示 "full post-change snapshot on every state transition"）→ 状态日志 ✓
- 问 2（apply 是否需要业务逻辑）：apply = upsert by `trigger_id`（last-write-wins by `event_seq`），零业务规则 ✓
- 问 3（SLA / 故障域兼容）：trigger snapshot 的 SLA 与 counter snapshot 一致（trigger 启动加速 + cold-standby 接管），可以同集群跑或独立集群按 ADR-0066 §3 切

结论：**接受**，本 ADR 落地。

### 为什么现在做

- ADR-0061 / 0064 已把 counter shadow + on-demand RPC 模板跑通，复制成本低
- ADR-0066 §4 +  本 PR 的 producer-owned 重构已经把 counter 的 layout 规整好了，trigger 走同一套天然贴合
- trigger 上线前调整 cold-standby 路径无包袱

## 决策 (Decision)

### 1. trade-dump 长出 trigger-shadow pipeline

镜像 ADR-0061 counter shadow 的结构，但因为 trigger 模型更简单做以下简化：

| 维度 | counter shadow (ADR-0061) | trigger shadow (本 ADR) |
|---|---|---|
| 分片 | 256 vshard，每 vshard 一个 shadow engine | **单 shadow engine**（trigger 不分片） |
| 输入 topic | counter-journal（事务性 produce，EOS） | trigger-event（普通 produce，无 EOS） |
| 启动 fencing | sentinel produce + InitProducerID（ADR-0064 §2.3） | **不需要**——trigger-event 没有 pending txn，LEO 直接稳定 |
| Apply 内容 | balance 累加 / order 状态 / 各 ledger | 单一动作：`map[trigger_id] = decoded(event.payload)`；status == terminal 的进 terminal 表（带 ADR-0040 `terminal_history_limit` ring） |
| Capture 触发 | 时间 60s ‖ 事件 60000（ADR-0064 §5） | **同款**复用配置语义 |
| 快照大小 | MB 级（账户 + 订单簿 + 预留） | KB 级（pending 通常 < 1k 条） |

### 2. 数据流（含两条独立 Kafka topic）

```
                   trigger primary
                  ┌────────────────────────────────┐
                  │ engine.Engine                  │
                  │  - pending / terminal triggers │
                  │  - OCO idempotency map         │
                  │  - market-data offsets         │
                  └──┬───────────────────┬─────────┘
                     │ produce           │ consume
                     │ trigger-event     │ market-data
                     │ (full post-state) │
                     │ + periodic        │
                     │ TriggerMarket-    │
                     │ CheckpointEvent   │
                     ▼                   ▼
              ┌────────────┐      ┌────────────┐
              │  Kafka:    │      │  Kafka:    │
              │ trigger-   │      │ market-    │
              │ event      │      │ data       │
              └─────┬──────┘      └────────────┘
                    │ consume
                    ▼
        ┌──────────────────────────────────────┐
        │ trade-dump trigger-shadow            │
        │   apply: upsert by trigger_id        │
        │   apply: market offset checkpoint    │
        │   periodic Capture                   │
        │     ↓                                │
        │   pkg/snapshot.BlobStore.Put         │
        │     key = "trigger" (fixed)          │
        │   TakeTriggerSnapshot RPC            │
        └──────────────┬───────────────────────┘
                       │ Get / RPC
                       ▼
                trigger primary OR standby (ADR-0042)
                  startup:
                    1. trade-dump.TakeTriggerSnapshot RPC (hot)
                       fallback: BlobStore.Get("trigger.pb") (cold)
                    2. Restore into engine
                    3. catch-up trigger-event from snap.TriggerEventOffset
                    4. resume market-data from snap.MarketOffsets
```

### 3. trigger snapshot 内容

```go
type TriggerSnapshot struct {
    Version              int
    TakenAtMs            int64
    Pending              []TriggerRecord       // 同现有结构
    Terminals            []TriggerRecord       // 同现有结构（trade-dump 维护 ADR-0040 ring）
    OCOByClient          map[string]string     // 同现有结构
    MarketOffsets        map[int32]int64       // 来自 TriggerMarketCheckpointEvent
    TriggerEventOffset   int64                 // shadow 自己写下的 trigger-event 下次消费位置
}
```

### 4. TriggerMarketCheckpointEvent（trigger-event 新事件）

trigger primary 周期（建议 1 秒或每 N 个 market 事件）写一条到 trigger-event topic：

```proto
message TriggerMarketCheckpointEvent {
  map<int32, int64> market_offsets = 1;  // partition → next-to-consume
  int64 ts_ms = 2;
}
```

shadow apply 时只更新自己持有的 `MarketOffsets` map，不改 trigger 状态。Capture 时把 map 写进 snapshot。

**为什么需要这个**：trigger 的 market-data 消费状态只有 trigger primary 自己知道；不通过事件传给 trade-dump，shadow snapshot 就缺 market offsets，违反 ADR-0048 "snapshot 必须含 offset"。Counter 的 ADR-0060 TECheckpointEvent 就是解决同一问题。

### 5. trigger 启动改造

trigger main.go 当前的 `Capture` / `Save` / `runSnapshotTicker` / `writeSnapshot` 全部删除。`tryRestoreSnapshot` 改成调用新建的 `trigger/internal/snapshot/loadsnapshot.go`（mirror counter 的 loadsnapshot 模式），内部按以下顺序：

1. **Hot 路径**：调 `trade-dump.TakeTriggerSnapshot()` RPC，拿 `(snapshot_key, leo, trigger_event_offset)`
2. 从 BlobStore 读 `<key>.pb`，decode 为 TriggerSnapshot
3. Restore 到 fresh engine
4. 失败 / RPC unavailable / unimplemented → **Cold 路径**：直接 `BlobStore.Get("trigger.pb")` 读最近的 periodic snapshot
5. 起 trigger-event consumer 从 snap.TriggerEventOffset 追上当前 LEO（catch-up，与 counter ADR-0064 §3 同款）
6. 起 market-data consumer 从 snap.MarketOffsets

trigger primary 自己**不再消费** trigger-event 用于 state（trigger-event 现在是 trade-dump 的输入 + trigger startup catch-up 的输入）。

### 6. ADR-0042 cold standby 协同

snapshot key 固定为 `trigger`（不再带 instance-id）：
- trigger-0（主）和 trigger-1（备）通过 etcd 抢 lease
- trade-dump 一直在写 `trigger.pb` / `trigger-ondemand-<ts>.pb`
- 任一副本接管时，`loadsnapshot` 都从同一份 BlobStore 读到主实例运行期间的最新状态
- ADR-0042 §"InstanceID-keyed snapshot 不共享" 的问题彻底消除

trigger 不再需要 NFS 共享 mount——BlobStore（FS 单机或 S3 多机）就够了。

### 7. 准入闭环

按 ADR-0066 §准入决策树：
- 问 1: trigger-event 状态日志 ✓
- 问 2: apply 无业务逻辑 ✓
- 问 3: SLA 兼容（同 counter snapshot 关键路径或独立集群，按 ADR-0066 §3 部署）

## 实施 (Implementation)

### Milestones

| M | 内容 | 范围 |
|---|---|---|
| M1 | proto 加 `TriggerSnapshot` (新结构) + `TriggerMarketCheckpointEvent`；trade-dump shadow 骨架（无 consumer）+ Apply 单测 | api / trade-dump 内部 |
| M2 | trade-dump 起 trigger-event consumer 接 shadow apply；周期 Capture + Save 写 BlobStore | trade-dump |
| M3 | trigger primary 加 TriggerMarketCheckpointEvent 周期 produce | trigger |
| M4 | trade-dump TakeTriggerSnapshot RPC + housekeeper（如有 on-demand 需求；初期可只走 periodic 不上 RPC） | trade-dump |
| M5 | trigger startup 加 hot/cold 路径，旁路新代码与现有 Capture/Save 并行 | trigger |
| M6 | 切流：删除 trigger 自己的 Capture/Save/Load/Snapshot 数据结构 + ticker；trigger snapshot 只剩 loadsnapshot.go | trigger |

每个 M 一个独立 PR；M5 之前老代码不删，rollback 路径清晰。

### Migration / Rollback

- M1–M5 期间：trigger 仍在写自己的 snapshot；trade-dump 同步在写 trigger.pb；任一份都能 Restore
- M5 验证 hot/cold 双路径稳定后再 M6 删旧
- M6 后没有 rollback——trigger 再也不知道怎么 Capture——但 ADR-0061 counter 走过一样的路径（Phase B），有先例

## 备选方案 (Alternatives)

### 备选 1：保持 trigger 自产 snapshot，只解决 ADR-0042 instance-id-keyed 问题

把 trigger 的 snapshot key 从 `<InstanceID>` 改成固定字符串，加文件锁防双写，用 NFS 共享 mount。

**为什么拒绝**：
- ADR-0066 §4 仍违反（snapshot 核心在 trigger）
- 不能复用 ADR-0061/0064 的运维模板（监控、housekeeper、S3 路径）
- 需要在 trigger 内额外处理双写互斥（HA primary 切换瞬间），等价于把 ADR-0042 的 lease 语义在 snapshot 层重做一遍

### 备选 2：trigger snapshot 完全不存，每次冷启动从 trigger-event 头部全 rescan

**为什么拒绝**：
- trigger-event 长期保留（ADR-0047 是长期历史），rescan 时间随项目运行时间线性增长
- ADR-0042 cold-standby 的 RTO 不可控
- 与 counter 模式不一致，运维心智负担大

### 备选 3：让 trigger primary 自己周期写 BlobStore（保留 trigger 写、绕过 trade-dump）

**为什么拒绝**：
- ADR-0066 §准入清单已经把 trigger snapshot 划入"trade-dump 应收编"，本 ADR 是其落地
- 让 trade-dump 集中处理所有 snapshot 是 ADR-0066 的核心收益（统一运维 / 统一 housekeeper / 统一 S3 路径 / 统一 RPC 模板）
- trigger 写 BlobStore 仍然需要单 producer 协调（HA 切换瞬间），不简化反而复杂

## 影响 (Consequences)

### 正面

- trigger snapshot 与 counter snapshot 走同一套 producer-owned 模板，运维一致
- ADR-0042 cold-standby snapshot 共享缺口闭合
- trigger/internal/snapshot 收口为 loadsnapshot.go 一个文件，符合 ADR-0066 §4
- trade-dump 多一个 pipeline 后，多集群部署（ADR-0066 §3）按 SLA 拆分时颗粒度更细

### 代价

- trade-dump 多一条 pipeline + 一份 shadow state；额外内存约等于 active triggers 数 × ~200B（KB 级）
- trigger-event topic 多了 TriggerMarketCheckpointEvent 流量（每秒一条 / 每 N 条 market 事件，可调）
- 需要协调 trigger primary 的 produce 节奏与 trade-dump shadow 的 apply 节奏（与 counter 同款问题，已有解决经验）

### 中性

- trade-dump 的 SLA 等级在 trigger 路径上等价于 counter 路径（均为关键路径）
- M6 切完后 trigger 与 trade-dump 之间是单向依赖（trigger consumer trade-dump producer），与 counter 一致

## 已知不防御场景 (Non-Goals / Known Gaps)

### G1：trade-dump 长时间不可用且 BlobStore 也读不到

trigger 启动会卡在 cold 路径。等价 counter ADR-0064 §G* 中的"trade-dump + S3 同时挂"，超出本 ADR 范围。降级方案：手工触发 trigger-event 头部 rescan 模式（仅 ops 工具，不在主路径）。

### G2：TriggerMarketCheckpointEvent 节奏太低导致 RPO 大

如果 checkpoint 每 N 秒产生一次，重启后最多丢 N 秒的 market-data 进度。这是与 RPO 的 tradeoff，按部署调 frequency。

### G3：shadow 与 trigger primary 状态分裂的检测

trigger-event apply 是确定性 upsert，理论上不会分裂。生产部署仍建议加 ADR-0061 §M5 类型的 diff 工具（snapshot vs 当前 trigger primary state）做监控，但本 ADR 不强制。

## 参考 (References)

- ADR-0061：trade-dump snapshot pipeline（counter 同款路线，本 ADR 直接 mirror）
- ADR-0064：Counter on-demand snapshot RPC（hot path 模板）
- ADR-0066 §准入清单 trigger 行：本 ADR 是其建议的落地 ADR
- ADR-0042 §"InstanceID-keyed snapshot 不共享"：本 ADR 顺带闭合的已知问题
- ADR-0048：snapshot + offset 原子绑定（market offsets 由 TriggerMarketCheckpointEvent 维持）
- ADR-0060 TECheckpointEvent：本 ADR TriggerMarketCheckpointEvent 的对照范式
