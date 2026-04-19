# Benchmarks / 性能基线

持续跟踪 Match / Counter / 全链路的 TPS + latency。一次性跑完的基准没意义 —— 要能和 **上一版 / 历史值** 对比。

[architecture.md §18.3](../architecture.md) 给了 MVP 目标：**20w TPS / 10ms P99**。本目录记录实际跑出的数和偏差归因。

---

## 什么时候跑

- 决定性的性能优化之前和之后（证明改动有效）
- 每次引入新瓶颈嫌疑的改动（锁 / 新 RPC 跳 / 新 Kafka topic）
- 大版本 release 前
- 硬件 / Go 版本 / Kafka 版本 / 部署拓扑变化时

## 什么时候**不**跑

- bug fix（除非 fix 在热路径）
- 文档 / config / 测试新增
- 单点功能开发（功能稳了再跑一次 regression 基线就够）

## 跑什么

**至少这几条**：

- **Match 单 symbol 撮合吞吐** —— 单机 1 symbol，纯 PlaceOrder，测 orders/s。P50 / P99 event-to-trade latency。
- **Counter 单 shard 撮合吞吐** —— 单 shard，纯 PlaceOrder + 完整 trade-event 回流，测 req/s 和 P99。
- **全链路端到端** —— 从 `POST /v1/order` 到 WS 收到 `FILLED`。测 submit-to-fill 的 P50 / P99。
- **撤单热路径** —— 高并发 CancelOrder（book 上真实订单 + PENDING 短路各一组）。

**环境固定**：同一次对比必须同硬件 / 同 Go 版本 / 同 Kafka 版本 / 同 symbol 数 / 同 partition 数。跨环境数据没有可比性。

## 记录格式

每次跑一篇：`YYYY-MM-DD-<topic>.md`，例：`2026-05-01-match-single-symbol.md`

模板：

```markdown
# <标题>：XX TPS, P99 Xms

- **日期**：YYYY-MM-DD
- **commit**：`<hash>` （关键依赖版本）
- **跑者**：谁
- **目标**：验证 / 对比什么
- **硬件**：CPU model / cores / RAM / disk type
- **Go 版本**：go1.26.x
- **Docker dep 版本**：Kafka 3.8 / MySQL 8.0 / etcd 3.5
- **部署拓扑**：几个 counter shard / match worker / 是否开 HA / snapshot interval

## 负载

- 工具：`wrk` / 自写 loadgen（指向具体 repo 路径）
- 并发：N 连接 × M pipeline
- Payload：具体请求样例（symbol / side / price / qty 分布）
- 持续时间：T 秒

## 结果

| 指标 | 值 |
|---|---|
| orders/s（稳态） | X |
| P50 latency | X ms |
| P99 latency | X ms |
| P99.9 latency | X ms |
| CPU（% of N cores） | X |
| Mem peak | X MB |
| Kafka lag | X |

## 对比上次

| 指标 | 本次 | 上次（日期 / commit） | Δ |
|---|---|---|---|

## 瓶颈分析

pprof CPU / alloc / block / mutex 结果贴重点 stack / flamegraph 截图 / 结论。

## Action items

- [ ] 优化点 1
- [ ] 跟进 ADR / roadmap 条目

## 原始数据

原始 log / pprof 文件位置（不要只贴截图；pprof 要能复现用的）。
```

## 跑完之后

- 回 [roadmap.md 待办](../roadmap.md#填坑--backlog) 的 "Match / Counter 延迟 + 吞吐 benchmark" 条目更新状态
- 如果和 [architecture.md §18.3](../architecture.md) 目标有偏差 > 30%，开 ADR / roadmap 条目跟进
- 大幅回退（vs 上次）触发 postmortem（[postmortems/](../postmortems/)）

## 历史

_(empty — 等第一次正式跑)_
