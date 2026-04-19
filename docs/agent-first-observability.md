# Agent-First 可观测性设计

- 状态: **Draft**（待 review）
- 日期: 2026-04-19
- 作者: xargin, Claude

## 背景

传统可观测性栈（Prometheus + Grafana + Loki/ELK + Jaeger UI）的核心假设是
"人类坐在 dashboard 前查问题"。这决定了：

- metric 要预聚合省基数、label 要精心设计
- 要画几十个 panel 才能覆盖常见场景
- runbook 是 markdown 让人读的
- alert 是 PromQL 规则让人写、让人响应

当主要的 ops 消费者从人变成 LLM agent，这些假设都要重审：

- agent 能读原始 event 并推理，不需要 panel
- agent 能写 ad-hoc 查询，不需要预先"想到"要看什么指标
- runbook 是 agent 调的 tool，不是给人读的文档
- 高基数数据对 agent 反而是优势（信息保留更完整）

## 目标

1. 让 agent 作为排查问题的一线消费者，能在 **一次会话内** 完成
   "某笔订单为什么没成交 / 为什么成交延迟高" 这类根因分析。
2. **最小化新基建**：不起新的分布式系统，尽量复用现有 Kafka/文件/Prom。
3. 保留人类兜底能力：critical path 仍有静态 alert，agent 挂了也不会漏发 pager。
4. 把现在 **不完整** 的 metric / trace 补齐到"agent 能用"的最低标准。

## 非目标

- 不做 Grafana dashboard wall（agent 不看 panel，人也极少看）
- 不建 datalake，不把三种数据灌进同一个存储
- 不替换 zap / Prometheus / Kafka
- 不做 agent 自动修复线上问题（本轮只做 observability）
- 不做大规模接入 Loki / ELK / Tempo 这类重栈

## 现状快照

| 维度 | 现状 | 缺口 |
|------|------|------|
| Log | 全部服务用 `pkg/logx`（zap），字段 schema 已约定 `service/shard/trace_id/user_id/symbol`。stdout → shell 重定向到 `logs/*.log` | dev 环境用 console encoder（彩色），不是 JSONL，DuckDB 直接查会吃力 |
| Audit Log | `pkg/adminaudit` 维护 admin-gateway JSONL，格式稳定、带 fsync | 只覆盖 admin 面，业务面暂无同级结构 |
| Metric | `pkg/metrics` 框架就位（RPC / Kafka / runtime），Handler 返回 `/metrics` | 仅 `counter/sequencer` 和 `push/hub` 实际用了，多数服务未 expose endpoint，也未实际打点 |
| Trace | logx 有 `trace_id` 字段约定 | 无 OTel SDK、无 span、无跨服务传递实现、无 collector |
| 部署 | `deploy/docker/docker-compose.yml`（Kafka + etcd + MySQL + MinIO） | 无 Prometheus / Jaeger / OTel collector 容器 |

## 总体架构

```
  服务（bff / counter / match / push / quote / trade-dump / history / conditional / admin-gateway）
     │           │                │
     │ zap JSON  │ Prom /metrics  │ OTel span
     │ stdout    │                │
     ▼           ▼                ▼
 logs/*.jsonl   Prometheus    OTel collector
    (文件)         (scrape)       │
                                  ├──► Jaeger (本地 UI，可选)
                                  └──► Parquet 文件（归档/DuckDB）

                 ┌─────────────── Agent 查询入口 ───────────────┐
                 │  cmd/obs-mcp  (Go, ~500 行, MCP server)     │
                 │                                              │
                 │   query_events(sql)   ──► DuckDB on files    │
                 │   query_metric(promql)──► Prometheus HTTP    │
                 │   get_trace(trace_id) ──► Jaeger HTTP        │
                 │   tail_log(svc,filter)──► 文件 tail          │
                 │   describe_incident   ──► 三源聚合           │
                 │   runbook_*           ──► 封装的机器可读流程 │
                 └──────────────────────────────────────────────┘
```

**三类数据三种后端，不统一入库**。DuckDB 只是查询引擎，用来对 JSONL/Parquet
文件做 ad-hoc SQL；metric 留 Prom，trace 留 collector/Jaeger。

---

## 阶段 1：补齐基础（让数据"可用"）

这一阶段完全和 agent 无关，做完人也能更舒服地排查。Agent 层依赖这些数据是
"结构化、完整、带 trace_id"。

### 1.1 Log 统一成 JSONL（小改动，优先做）

- `pkg/logx` 增加配置位 `OutputFormat=json`，让 dev 也能按需打 JSON。
- Makefile 里 `run-*` target 默认走 JSON 输出到 `logs/<svc>.jsonl`（旧 `.log`
  保留彩色人眼用，并行两份或加开关）。
- 约定 **强制字段**：
  ```
  ts, level, msg, service
  trace_id   — 有请求上下文的必填
  kind       — 事件分类：rpc / kafka / business / error / audit
  ```
- 约定 **常用可选字段**：`shard, user_id, symbol, order_id, topic, partition, offset, code`
- 在 `pkg/logx` 加一个 lint-friendly helper `Event(ctx, kind, msg, fields...)`，
  逐步收敛业务日志调用点。

**验收**：`cat logs/counter.jsonl | jq '.kind' | sort | uniq -c` 能看到分类分布。

### 1.2 Metric 全服务接入

每个服务启动时 **必须**：

1. 用 `metrics.NewRegistry()` + `metrics.NewFramework(service, reg)`
2. HTTP 暴露 `/metrics` 在独立 admin 端口（比如 `:9{svc_port[-2:]}`，命名规范 ADR 里补）
3. 框架指标自动生效：`cex_rpc_*`、`cex_kafka_*`

**业务 SLI（每服务 3~5 个，不是 50 个）**：

| 服务 | 关键 metric（建议） |
|------|---------------------|
| counter | `cex_counter_order_accept_total{side,type}`, `cex_counter_reservation_active`, `cex_counter_journal_lag` |
| match | `cex_match_trade_total{symbol}`, `cex_match_book_depth{symbol,side}`, `cex_match_engine_tick_duration_seconds` |
| push | `cex_push_ws_connections`, `cex_push_coalesce_queue`, `cex_push_drop_total{reason}` |
| bff | `cex_bff_ws_sessions`, `cex_bff_auth_fail_total{reason}` |
| quote | `cex_quote_snapshot_age_seconds`, `cex_quote_diff_publish_total` |
| trade-dump | `cex_trade_dump_lag_seconds`, `cex_trade_dump_write_bytes` |
| conditional | `cex_cond_active_orders`, `cex_cond_trigger_total{reason}` |
| admin-gateway | `cex_admin_op_total{op,status}` |

**不做**：精心设计 label cardinality，只要别出现 user_id / order_id 这种真·高基数
就行（这些放 log 里）。

**部署**：`deploy/docker/docker-compose.yml` 加 prometheus 容器，scrape 本地服务。

**验收**：`curl :9xxx/metrics | grep cex_` 每个服务至少有框架 5 个 + 业务 3 个。

### 1.3 Trace：引入 OpenTelemetry

现在 `trace_id` 只是 log 字段，没有 span、没有跨服务传递。目标是 **trace_id 贯穿
BFF → Counter → Match → Push**，agent 拿 trace_id 就能看到一笔订单全链路。

**改造点**：

1. `pkg/logx` 旁加 `pkg/obs/trace`：封装 OTel SDK 初始化（stdout exporter + OTLP exporter）。
2. gRPC 两端加 OTel interceptor（`otelgrpc`），span context 自动传。
3. Kafka 消息 header 带 `traceparent`（W3C），produce/consume 两端 inject/extract。
   - 已有的 `pkg/kafka` wrapper 改造点集中。
4. HTTP / WebSocket 入口（bff）在请求进来时 start span，trace_id 写回 log context。
5. OTel collector 部署在 docker-compose，两路导出：
   - Jaeger（本地 UI 看 trace，可选）
   - File exporter（JSON → Parquet 转换脚本 → DuckDB 可查）

**不做**：全链路采样率调优、trace-based alert（留到阶段 3）。起步 100% 采样，量不大。

**验收**：下一单，从 BFF log 拿 `trace_id`，在 Jaeger UI 或 `obs-mcp
get_trace` 能看到完整链路的所有 span。

### 1.4 Audit log 扩展（可选）

目前只有 admin-gateway 一条 JSONL。可以在阶段 2 前，给 counter / match 也出
`data/audit/<svc>.jsonl`，记录关键业务事件（订单状态机转换、撮合成交结果）。

或者 **不做，直接复用 JSONL log 的 `kind=business` 字段**。推荐后者，减少新机制。

---

## 阶段 2：Agent 层（真正的"新基建"，只有 DuckDB + 一个 MCP server）

### 2.1 DuckDB

- 装法：`brew install duckdb`（或 go-duckdb 直接 embed，见下）。
- 不启服务，按需 CLI 或 embed 进 MCP server。
- 预置 SQL view 脚本 `tools/obs/views.sql`：

```sql
CREATE VIEW logs_all AS
  SELECT * FROM read_json_auto('logs/*.jsonl',
                               union_by_name=true,
                               format='newline_delimited');

CREATE VIEW admin_audit AS
  SELECT * FROM read_json_auto('data/audit/admin-*.jsonl',
                               format='newline_delimited');

CREATE VIEW trades AS
  SELECT * FROM read_parquet('data/trade-dump/*.parquet');

CREATE VIEW traces AS
  SELECT * FROM read_parquet('data/otel/traces-*.parquet');
```

### 2.2 `cmd/obs-mcp` — MCP server

一个独立二进制，实现 MCP 协议（stdio transport 即可，给本地 Claude Code 用）。

**Tool 清单**：

| Tool | 输入 | 后端 | 用途 |
|------|------|------|------|
| `query_events` | `sql string` | DuckDB embed | 对 logs_all / admin_audit / trades 查 |
| `query_metric` | `promql, range?` | Prometheus HTTP `/api/v1/query[_range]` | 历史/即时 metric |
| `get_trace` | `trace_id` | Jaeger HTTP `/api/traces/<id>` 或 DuckDB traces | 全链路 span |
| `tail_log` | `service, since, filter?` | 文件直读（jq-like filter） | 实时跟进 |
| `list_services` | 无 | 静态配置 | 告诉 agent 有哪些服务、端口、log 路径 |
| `describe_incident` | `trace_id 或 order_id 或 时间窗口` | 三源聚合 | 一次调用返回"这段时间相关 error log + metric 异常 + trace" |
| `runbook.<name>` | 各异 | 封装好的脚本 | 比如 `runbook.check_kafka_lag()`、`runbook.check_match_determinism(symbol)`、`runbook.snapshot_drift(svc)` |

**实现语言**：Go（项目主栈），MCP SDK 用官方 Go 实现或手写（协议不复杂）。

**部署**：本地 dev 通过 `.claude/settings.json` 注册为 MCP server；线上暂时不上。

### 2.3 Runbook as Tool

把 `docs/postmortems/` 和常见排查套路转成 `runbook.*` tool。每个 runbook：

- 是一个函数，不是 markdown
- 输入：参数（symbol / user_id / 时间窗口）
- 输出：结构化的"状态 + 证据 + 建议下一步"
- 内部：组合调用 `query_events / query_metric / get_trace`

起步做 5~8 个高频的：
- `check_kafka_lag()` — 所有 topic partition lag，超阈值标红
- `check_match_determinism(symbol)` — ADR-002 engine determinism 的 lint 等价运行时检查
- `check_counter_reservation_drift(user_id?)` — 余额 vs reservation 对账
- `snapshot_vs_offset(svc)` — 验证 ADR-0048 的 snapshot-offset 原子性
- `order_lifecycle(order_id)` — 一笔单跨服务时间线
- `symbol_health(symbol)` — BFF/counter/match/push 对该 symbol 的全景
- `ws_session_drop(user_id)` — push 掉线原因

---

## 阶段 3：运营（可选 / 后置）

- **Alert 兜底**：Prometheus 静态 alert 只保留"**必须** 触发 pager"的红线
  （counter 写入失败、match offset 停滞、push ws 大规模掉线）。其他 soft signal
  让 agent 巡检。
- **巡检 agent**：cron 触发一个 agent 每 5~15 分钟跑一轮 runbook.*，异常发 Slack。
- **归档**：`logs/*.jsonl` 超 7 天 gzip 压缩并转 Parquet 到 `data/obs-archive/`。
  DuckDB 一样查（`read_json_auto` + `read_parquet`）。
- **成本**：wide events 比 metric 贵，但本项目规模（本地 + 若干 VPS）成本不敏感。

---

## 实施顺序建议

| 步 | 内容 | 预估 | 阻塞后续？ |
|----|------|------|-----------|
| 1.1 | logx JSONL 输出开关 + 强制字段收敛 | 0.5 天 | ✅ 阶段 2 依赖 |
| 1.2 | 每服务接入 metrics + docker-compose Prom | 1 天 | ❌ 可并行 |
| 1.3 | OTel SDK + collector + Kafka header 传播 | 2~3 天 | ✅ get_trace 依赖 |
| 2.1 | DuckDB view 脚本 | 0.5 天 | — |
| 2.2 | `cmd/obs-mcp` 骨架 + 5 个核心 tool | 2~3 天 | — |
| 2.3 | 8 个 runbook | 每个 0.5 天 | — |

**最小可用（MVP）路径**：1.1 → 2.1 → 2.2（只实现 `query_events / tail_log`）。
这一步就能让 agent 对 JSONL 日志做 SQL 排查，已经比现在强很多。1.2 / 1.3 可以后续增量补。

---

## 开放问题（需要你决策）

1. **MCP server 交付形态**：独立 `cmd/obs-mcp` vs 塞进已有的 tools/ 目录？
2. **docker-compose 增量**：是否把 prometheus + otel-collector + jaeger 加进本地栈？
   （会让 `make dev` 启动变重）
3. **trace 采样**：起步 100% 还是 10%？本项目量不大我倾向 100%。
4. **`runbook.*` 命名空间**：作为 MCP tool 还是作为独立 CLI（`tools/runbook check-kafka-lag`）
   让 agent 用 Bash 调？后者更解耦，但发现性差。
5. **dev log 双输出（彩色 + JSONL）是否可接受**？还是只保留 JSONL + 写一个
   `jless`/`jq` alias 给人看？

---

## 相关 ADR

- ADR-0048 snapshot-offset 原子性（runbook 直接验证它）
- ADR-0052 admin console / audit log（audit JSONL 先例）
- （若本方案接受）后续可能拆出：
  - ADR-0053 OTel 引入与 trace_id 跨服务传递
  - ADR-0054 服务 metric 接入规范与 SLI 目录
  - ADR-0055 obs-mcp 与 agent-first 运维入口
