# Counter 运维手册 (Runbook)

> 对应 ADR-0060 M8 / ADR-0063。覆盖异步消费 + TECheckpoint + 订单终态删除（含 ADR-0063 inline delete）+ 恢复流程相关的告警、诊断与恢复动作。

## 1. 观测端点

| 端点 | 默认地址 | 用途 |
|---|---|---|
| `/metrics` | `:9101` (`--metrics-addr`) | Prometheus scrape |
| `/healthz` | `:9101/healthz` | liveness (200=ok) |
| gRPC | `:8081` (`--grpc-addr`) | CounterService / AssetHolder RPC |

## 2. 关键 Prometheus 序列

### 2.1 消费 / 推进 (ADR-0060)

| Metric | 类型 | 标签 | 解读 |
|---|---|---|---|
| `counter_pendinglist_size` | Gauge | `vshard` | 每 vshard pendingList 深度。稳态 < 100；> 10k 告警 |
| `counter_checkpoint_publish_total{result=ok\|err}` | Counter | `vshard, result` | TECheckpoint 发布结果。`err` 连续 ≥ 3 → advancer 卡住或 Kafka 异常 |
| `counter_publish_retry_total` | Counter | `op, attempt_range` | `TxnProducer.runTxnWithRetry` 内重试分布。`attempt_range=6+` 出现即高风险 |
| `counter_seq_high_watermark` | Gauge | `vshard` | UserSequencer.CounterSeq 最新值。不应回退 |
| `counter_catchup_events_applied_total` | Counter | `vshard` | Recovery 时 catch-up 应用 event 数。启动时一次性增长是正常 |

> Counter 已不再自产 snapshot（ADR-0061 Phase B），原先的
> `counter_snapshot_duration_seconds` / `counter_seq_high_watermark`
> 已无写入点。snapshot 新鲜度监控迁到 **trade-dump** 侧：
> `trade_dump_snapshot_age_seconds{vshard}` 与 `trade_dump_snapshot_capture_duration_ms{vshard}`。
> 对应告警见 `docs/runbook-trade-dump.md`（后续）。

### 2.2 订单 evict (ADR-0063)

ADR-0063 之后不再有 evictor goroutine / retention 窗口 / `OrderEvictedEvent` / 健康熔断 —— 订单进入终态时由发送方（`emitStatus`）和 apply 侧（`applyOrderStatusEvent`）在同一步内直接 `Orders().Delete(id)`。对应的 `counter_evict_processed_total` / `counter_evict_halted_total` 指标已删除；byID 稳态就只含活跃订单，不需要单独的 evict 进度观测。

### 2.3 通用框架 (pkg/metrics)

- `cex_rpc_requests_total{service="counter", method, code}`
- `cex_rpc_duration_seconds{service="counter", method}`
- `cex_kafka_produce_total{topic, result}`
- `cex_kafka_consume_lag{topic, partition}`

## 3. 告警阈值 (推荐)

```yaml
# ADR-0060 M8 建议基线
- alert: CounterPendingListHigh
  expr: counter_pendinglist_size > 10000
  for: 2m
  labels: {severity: warning}
  annotations:
    summary: "vshard {{ $labels.vshard }} pendingList 深度 > 10k"
    runbook: "docs/runbook-counter.md#4-pendinglist-失控"

- alert: CounterCheckpointPublishErrorBurst
  expr: rate(counter_checkpoint_publish_total{result="err"}[5m]) > 0.1
  for: 3m
  labels: {severity: warning}

- alert: CounterPublishRetryEscalating
  expr: rate(counter_publish_retry_total{attempt_range="6+"}[5m]) > 0
  for: 1m
  labels: {severity: critical}
  annotations:
    runbook: "docs/runbook-counter.md#5-publish-重试持续-5s-即-panic"

# Snapshot freshness is owned by trade-dump's snap pipeline (ADR-0061).
# See docs/runbook-trade-dump.md for trade_dump_snapshot_age_seconds /
# trade_dump_snapshot_* alerting.
#
# ADR-0063 removed the evictor goroutine + OrderEvictedEvent + retention
# window + health-circuit-breaker. Terminal orders are deleted from byID
# inline with the OrderStatusEvent publish, so evict-specific alerts no
# longer exist — monitor byID footprint via the Counter memory / heap
# metrics instead.
```

## 4. pendingList 失控

### 4.1 症状

`counter_pendinglist_size{vshard="X"}` 单调上升，典型节奏数分钟内到 100k+。

### 4.2 可能原因

1. **下游 per-user 卡顿**：某 user 的 fn 在 Kafka Publish 内部卡住，drain goroutine 一直不返回 → 对应 vshard 的 pendingList 无限累积（ADR-0060 Known Gap G6）。应同时触发 `counter_publish_retry_total{attempt_range="6+"}` 告警。
2. **advancer 本身卡死**：runAdvancer 信号 chan 接收阻塞 / panic → `counter_checkpoint_publish_total` 停止增长。
3. **Kafka 集群压力**：整个集群抖动，大部分 Publish 重试但没到 panic 线 → `cex_kafka_produce_total{result="err"}` 增长。

### 4.3 诊断步骤

```bash
# 1. 确认 vshard 所在 pod
kubectl get pods -l app=counter -o wide

# 2. 直接拉 /metrics 看趋势（哪个 vshard，是否仍在涨）
curl http://<pod>:9101/metrics | grep -E 'counter_pendinglist|counter_publish_retry|counter_checkpoint'

# 3. 看 pod 日志里是否有 "journal publish failed; retrying"
kubectl logs <pod> --tail=200 | grep -E 'publish|retry|panic'

# 4. 确认 Kafka broker 健康 — 按 operator 集群 dashboard
```

### 4.4 应急操作

- **Kafka 侧问题**：按 Kafka 告警响应（先稳 broker，再看 Counter）。
- **单 vshard 卡住**：kill 对应 pod，让 ADR-0058 cold handoff 把 vshard 迁走（lease 失效后约 10-30s 新 owner 接管）。
  ```bash
  kubectl delete pod <counter-pod> --grace-period=0
  ```
- **全局 advancer bug**：回滚最近一次发布到上一 build。

## 5. Publish 重试持续 5s 即 panic

### 5.1 症状

Pod 短时间内反复重启，`counter_publish_retry_total{attempt_range="6+"}` 在重启前快速上升；日志最后一行：
```
panic: journal: Publish failed after N attempts over 5s: <kafka error>
```

### 5.2 原因

ADR-0060 §3 的故障边界：Publish 连续 5s 失败触发 panic，让 vshard 通过 ADR-0058 cold handoff 迁到健康节点。

### 5.3 动作

- **Kafka 分区 leader 抖动/长时间不可用**：应同步看 Kafka broker 告警。Counter panic 是预期行为，**无需抑制**，抑制反而会掩盖 Kafka 问题。
- **整个集群 restart loop**（ADR-0060 Known Gap G1）：表明 Kafka 集群长时间不可用，**停止 Counter 写流量（降级 BFF 层）**，恢复 Kafka 后再放流量。

## 6. Snapshot 新鲜度 / 阻塞（迁往 trade-dump）

ADR-0061 Phase B 后 Counter 不再自产 snapshot，对应的
`counter_snapshot_duration_seconds` 已无数据。相关监控 + runbook
在 **trade-dump** 侧：

- 指标：`trade_dump_snapshot_age_seconds{vshard}` /
  `trade_dump_snapshot_capture_duration_ms{vshard}` /
  `trade_dump_snapshot_save_duration_ms{vshard}` /
  `trade_dump_snapshot_apply_error_total{vshard}`
- 告警 + 排障流程：`docs/runbook-trade-dump.md`（后续）

ADR-0063 之后 Counter 侧不再有任何 evict 健康熔断；snapshot-stale
告警在 trade-dump 一侧，真的 snapshot pipeline 挂了，
`trade_dump_snapshot_age_seconds` 增长会驱动响应。

## 7. Recovery (catch-up journal replay)

### 7.1 症状

正常启动：日志 `catchup complete applied=N last_offset=M`，`counter_catchup_events_applied_total` 一次性增长。

### 7.2 异常情况

- `catchup: budget exhausted after N events` → 预算 2min 耗尽。通常是 counter-journal 历史堆积过长 + replay apply 慢。重试一次；重复失败则需 hand-roll 更大的 `defaultCatchUpMaxDuration`。
- `catchup: client closed unexpectedly` → Kafka 连接异常 / auth。排查 broker。
- **snap.journal_offset > counter-journal earliest offset** (ADR-0060 Known Gap G4)：对应 panic，应该在启动时出现明显 assertion 日志。修正 Kafka retention 配置后重启。

## 8. vshard 扩容 / 再平衡 (ADR-0058)

ADR-0058 vshard 模型下，**failover 自动、rebalance 手动**：节点挂了 Coordinator 的 failover loop 自己接管（[coordinator.go](../counter/internal/clustering/coordinator.go) `sweepOrphans`），但**新增节点不会触发自动迁移** —— 必须 operator 跑 `counter-migrate` 工具手动 rebalance。本节是这条手动路径的 step-by-step。

### 8.1 适用场景

| 场景 | 用本节流程？ |
|---|---|
| 加一台 counter 扩容（从 N 台到 N+1） | ✅ 用 `counter-migrate rebalance` |
| 下线一台 counter（先把 vshard 迁走再 kill） | ✅ 用 `counter-migrate rebalance` 或多次 `move` |
| 节点突然死亡 / lease 过期 | ❌ 自动 failover，不要手动介入 |
| 业务高峰 | ❌ 等高峰过 — 迁移过程中目标 vshard 短暂不可写 |
| 单 vshard 倾斜（HRW 已基本均匀，少见） | ✅ 用 `counter-migrate move` 单个搬 |

### 8.2 一次迁移的状态机

```
ACTIVE(owner=A, ep=N)
   │
   ▼ counter-migrate move/rebalance         ← operator 手动触发
   │   StartMigration(vshard, target=B)
   │   (CAS: ACTIVE → MIGRATING)
MIGRATING(owner=A, ep=N, target=B)
   │
   ▼ Manager A 自检循环                       ← 老 owner 自身触发
   │   stop worker → flush TxnProducer →
   │   MarkHandoffReady (CAS)
HANDOFF_READY(owner=A, ep=N, target=B)
   │
   ▼ Coordinator migration loop              ← 当选 leader 的 counter 周期 sweep
   │   CompleteMigration:
   │     owner=B, epoch=N+1, state=ACTIVE
ACTIVE(owner=B, ep=N+1)
   │
   ▼ Manager B 的 WatchAssignedAssignments    ← 新 owner 自身触发
       收到新条目 → 启动 VShardWorker
       load snapshot (trade-dump 产出) →
       catchUpJournal → ready
DONE
```

operator 只手动推第一步（`ACTIVE → MIGRATING`），后面三步是 Manager 和 Coordinator 各自循环里**自动**完成的，所以 operator 视角就是"跑 rebalance 然后等"。

### 8.3 前置条件

- 新节点已经启动并注册到 etcd：`etcdctl --endpoints=$ETCD get --prefix /cex/counter/nodes/` 能看到它
- 监控基线正常：`counter_pendinglist_size`、`counter_checkpoint_publish_total{result=err}`、`counter_publish_retry_total{attempt_range="6+"}` 全部不在告警状态
- trade-dump snapshot pipeline 健康（`trade_dump_snapshot_age_seconds` 在窗口内）—— 新 owner 启动 worker 时要 load 它产出的 snapshot
- 不在业务高峰窗口

### 8.4 步骤

```bash
# 1. dry-run：看 HRW 理想分配 vs 当前实际，输出迁移 plan
counter-migrate plan --etcd=$ETCD

# 输出（示例）：
# vshard | current_owner | hrw_target
# -------|---------------|--------------
# 4      | counter-a-0   | counter-b-0
# 17     | counter-a-0   | counter-b-0
# ... (HRW: 加一台 N=2 后约 ½ vshard 应该迁过去)
#
# rebalance plan: 128 vshards to move

# 2. 确认 plan 合理后执行（串行，一个一个迁）
counter-migrate rebalance --etcd=$ETCD

# 输出（示例）：
# rebalance: starting (128 vshards)
# vshard 4: ACTIVE@counter-a-0 → MIGRATING → HANDOFF_READY → ACTIVE@counter-b-0 ep=2 (3.2s)
# vshard 17: ...
# rebalance complete: 128 migrations applied
```

需要单独移某个 vshard：

```bash
counter-migrate move --etcd=$ETCD --vshard=42 --target=counter-b-0
```

### 8.5 过程中要盯的指标 / 日志

| 指标 / 日志 | 期望 | 异常信号 |
|---|---|---|
| `counter_pendinglist_size{vshard=X}` 老 owner 侧 | 迁移期间停涨，归零，新 owner 侧重新出现 | 持续上涨 = Manager A 没 flush 出去，可能 Kafka 卡 |
| `counter_checkpoint_publish_total{vshard=X}` | 迁移期间老 owner ok 暂停，新 owner 接管后恢复 | err 暴涨 = TxnProducer 上 epoch fence 失败 |
| 老 owner pod 日志 | `vshard X marked HANDOFF_READY` | > 30s 没出现 = flush 卡，看 §8.6 |
| 新 owner pod 日志 | `vshard X worker started, ready` | 没出现 = snapshot load / catchUpJournal 失败 |
| `etcdctl get /cex/counter/assignments/vshard-NNN` | epoch 单调递增，state 过 `MIGRATING → HANDOFF_READY → ACTIVE` | 长时间停在中间状态 = 看 §8.6 |

### 8.6 异常处理

**vshard 卡在 `MIGRATING` 超过 30s**

老 owner 的 Manager 没能 flush。先看老 owner 日志：

```bash
kubectl logs <old-owner-pod> --tail=200 | grep -E 'flush|publish|vshard.*X'
```

常见原因：
1. Kafka 集群抖动 → 看 `cex_kafka_produce_total{result=err}`，先稳 Kafka 再处理（大概率 30s 内自动推进）
2. 老 owner pod 已经死了 → Coordinator 的 failover loop 会通过 `sweepOrphans` 把 vshard force-reassign 到新 owner（epoch+1），但**这是 failover 路径不是 migration 路径**，原本规划的 target 不一定是 HRW 选的；rebalance 完成后再次 plan 一遍补差即可

**vshard 卡在 `HANDOFF_READY`**

Coordinator 的 migration loop 没扫到。先看 coordinator 选举状态：

```bash
etcdctl --endpoints=$ETCD get /cex/counter/coordinator/leader
```

如果 leader 存在但没扫，最简单的解法是让 leader 重启 / 主动 resign —— 新 leader 进 `lead()` 时会跑一次 `sweepHandoffReady`：

```bash
kubectl delete pod <coordinator-leader-pod>
```

**新 owner 启动 VShardWorker 失败**

看新 owner pod 日志：
- `snapshot load: not exist` → trade-dump snapshot pipeline 没产出过这个 vshard 的 snapshot；查 `docs/runbook-trade-dump.md`
- `snapshot load: ...` 其它错 → snapshot 损坏；trade-dump 侧重新生产
- `catchup: budget exhausted` 等 → 看 §7

**`counter-migrate rebalance` 中途失败**

工具是串行的，失败会停下来。当前已迁完的 vshard 不会回滚（已经在新 owner 上跑）。修复底层问题后**重新跑 `plan`**，会自动跳过已迁完的，只显示剩余的；再跑 `rebalance` 接着做。

### 8.7 命令速查

```bash
# 看当前 vshard 分配（按 vshard 排）
etcdctl --endpoints=$ETCD get --prefix /cex/counter/assignments/ | head -40

# 看活节点
etcdctl --endpoints=$ETCD get --prefix /cex/counter/nodes/

# 看当前 coordinator
etcdctl --endpoints=$ETCD get /cex/counter/coordinator/leader

# 看是否需要 rebalance（dry-run）
counter-migrate plan --etcd=$ETCD

# 执行 rebalance（全部需要迁的串行做完）
counter-migrate rebalance --etcd=$ETCD

# 单个 vshard 手术
counter-migrate move --etcd=$ETCD --vshard=N --target=NODE_ID
```

## 9. 常用排障一行

```bash
# 总体状态
curl -s http://<pod>:9101/metrics | \
    grep -E '^counter_(pendinglist|checkpoint|publish_retry|catchup)'

# 特定 vshard
curl -s http://<pod>:9101/metrics | \
    grep -E 'vshard="42"'

# 所有 vshard 的 pendingList 深度（排序）
curl -s http://<pod>:9101/metrics | \
    awk '/counter_pendinglist_size{.*}/ {print $0}' | sort -t= -k2 -nr | head

# etcd 看当前 vshard 分配
etcdctl --endpoints=$ETCD get --prefix /cex/counter/assignments/
```

## 10. 相关 ADR

- [ADR-0048](adr/0048-snapshot-offset-atomicity.md) — snapshot + offset 原子
- [ADR-0058](adr/0058-counter-virtual-shard-and-instance-lock.md) — Counter 虚拟分片 + 冷切；§8 流程的设计依据
- [ADR-0060](adr/0060-counter-async-consumption-and-te-checkpoint.md) — 本 runbook §1-§7 主要依据
- [ADR-0061](adr/0061-trade-dump-snapshot-pipeline.md) — trade-dump 接管 snapshot 生产
- [ADR-0062](adr/0062-order-terminal-eviction-and-idempotency-ring.md) — 订单 evict 旧框架（已 Superseded by ADR-0063）
- [ADR-0063](adr/0063-terminal-is-evict.md) — 终态即 evict（当前模型）

## 11. 变更日志

- 2026-04-22 初稿（ADR-0060 M8 / ADR-0062 M8 落地）
- 2026-04-22 ADR-0063 落地：删 evictor goroutine / OrderEvictedEvent / 健康熔断；终态由 OrderStatusEvent inline delete 承担；§2.2 / §3 / §6 内容收敛
- 2026-04-27 加入 §8 vshard 扩容 / 再平衡章节；同步删除老 shard 模型的 `docs/counter-reshard.md`（ADR-0058 已废弃 `--total-shards`）
