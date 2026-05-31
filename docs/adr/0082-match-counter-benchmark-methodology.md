# ADR-0082: Match / Counter 延迟与吞吐 benchmark 方法

- 状态: **Proposed**（2026-05-31 起草；从 `docs/roadmap.md` 的 benchmark 待办提升为独立 ADR）
- 日期: 2026-05-31
- 决策者: xargin, Codex
- 相关 ADR: 0003（Counter↔Match via Kafka）、0016（Match 单 symbol worker）、0018（Counter user sequencer）、0032（Match transactional producer）、0048（snapshot offset atomicity）、0059（Kafka cluster scaling）

## 范围声明（先读这一段）

目标是验证系统是否接近 `20w TPS / 10ms P99`，但本 ADR 不把单一数字当成唯一验收。benchmark 必须能回答：

- 瓶颈在 Counter、Kafka、Match、trade-dump、push 还是网络。
- 延迟分布在不同负载和撮合形态下如何变化。
- 开启事务 producer、snapshot、journal projection 后的成本。

## 背景 (Context)

现有单元测试验证正确性，缺少可重复性能基线。没有统一 benchmark 方法时，局部优化容易得到不可比较的数字。

## 决策 (Decision)

### 1. 建立三层 benchmark

```text
+----------------+     +----------------+     +----------------+
| L1 in-process  | --> | L2 service     | --> | L3 full stack  |
| engine only    |     | grpc+kafka     |     | +mysql+push    |
+----------------+     +----------------+     +----------------+
```

- L1：纯 Go engine / sequencer，不含网络和 Kafka，用于算法上限。
- L2：Counter + Match + Kafka，验证核心交易链路。
- L3：加入 trade-dump、history projection、push，验证端到端系统成本。

### 2. 固定 workload matrix

至少覆盖：

```text
symbols:        1 / 10 / 100
users:          1k / 100k / 1m synthetic
order mix:      limit maker / taker / market / cancel
fill shape:     no-fill / 1:1 fill / 1 taker eats 100 makers
perp mix:       spot only / perp isolated / perp liquidation disabled / perp liquidation enabled
producer mode:  idempotent / transactional
```

每个 case 输出：

- accepted TPS
- trade-event TPS
- p50/p95/p99/p999 latency
- end-to-end journal lag
- Kafka produce/consume lag
- CPU、RSS、GC、alloc/op

`perp liquidation enabled` 至少包含 mark 下跌触发候选扫描、partial liquidation、backstop/ADL mock 三档。强平路径是高压行情下的尾延迟来源，不能只测 disabled 模式。

### 3. 延迟按阶段打点

统一 trace id：

```text
client_send
counter_accept
order_event_produced
match_received
match_emitted_trade
counter_settled
journal_produced
push_delivered optional
```

报告必须同时给 stage latency 和 end-to-end latency，否则无法定位瓶颈。

### 4. 结果归档为机器可读 artifact

每次 benchmark 产出：

```text
bench-results/
  YYYYMMDD-HHMMSS/
    env.json
    workload.json
    metrics.ndjson
    summary.md
    flamegraph optional
```

`summary.md` 只是展示，`metrics.ndjson` 才是后续对比权威。

## 备选方案 (Alternatives Considered)

### A. 只跑 full stack

最接近真实，但定位困难。否决。

### B. 只看平均延迟

交易系统关心尾延迟。必须 p99/p999。否决。

### C. 用生产 Kafka/MySQL 做 benchmark

容易污染环境且不可重复。benchmark 使用隔离环境；生产只跑轻量 canary。

## 影响 (Consequences)

### 正面

- 性能讨论有可复现基线。
- 能区分算法瓶颈和基础设施瓶颈。
- 后续优化可以用同一 workload 比较。

### 负面 / 代价

- 需要 benchmark driver 和统一 metrics schema。
- L3 环境维护成本高。
- 20w TPS 目标需要明确硬件规格，否则数字没有意义。

## 实施约束 (Implementation Notes)

- benchmark driver 必须能固定随机种子。
- 每个 case warmup、run、cooldown 分段。
- 输出环境信息：CPU、内存、Go version、Kafka/MySQL 配置、commit hash。
- CI 只跑小规模 smoke；大规模 benchmark 手动或 nightly。
- 结果回归阈值先用趋势告警，不直接阻塞开发。
- liquidation-enabled case 可以用 deterministic mark script 和 fake liquidity，保证每轮触发数量可复现。

## 参考 (References)

- [ADR-0059: Kafka 集群扩展和 Match 输出分发](./0059-kafka-cluster-scaling-and-match-output-dispatch.md)
