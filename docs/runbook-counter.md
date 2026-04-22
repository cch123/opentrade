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

## 8. 常用排障一行

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

## 9. 相关 ADR

- [ADR-0048](adr/0048-snapshot-offset-atomicity.md) — snapshot + offset 原子
- [ADR-0058](adr/0058-counter-vshard-and-instance-lock.md) — Counter 虚拟分片 + 冷切
- [ADR-0060](adr/0060-counter-async-consumption-and-te-checkpoint.md) — 本 runbook 主要依据
- [ADR-0061](adr/0061-trade-dump-snapshot-pipeline.md) — trade-dump 接管 snapshot 生产
- [ADR-0062](adr/0062-order-terminal-eviction-and-idempotency-ring.md) — 订单 evict 旧框架（已 Superseded by ADR-0063）
- [ADR-0063](adr/0063-terminal-is-evict.md) — 终态即 evict（当前模型）

## 10. 变更日志

- 2026-04-22 初稿（ADR-0060 M8 / ADR-0062 M8 落地）
- 2026-04-22 ADR-0063 落地：删 evictor goroutine / OrderEvictedEvent / 健康熔断；终态由 OrderStatusEvent inline delete 承担；§2.2 / §3 / §6 内容收敛
