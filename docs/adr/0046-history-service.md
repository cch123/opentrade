# ADR-0046: History 服务 — 只读查询聚合

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0001, 0008, 0023, 0028, 0040

## 背景 (Context)

MVP-4 ~ MVP-14 的演进里，历史查询散落在两处：

1. **BFF 直连 Counter** 处理 `GET /v1/order/:id`，只能查活跃（仍在
   Counter 内存里的）订单；一旦订单 FILLED / CANCELED 被 Counter 驱
   逐，这条路径直接 404。
2. **没有订单列表 / 成交明细 / 资金流水接口**。客户端要自己看 WS
   推送拼凑；想看"昨天的成交"做不到。

再加上触发单（MVP-14a/b/c/d/e/f）登场后，`orders` 表里多了 client_order_id
`trig-<id>` 前缀的"触发产生的真实订单"；产品侧明确表达了
"挂单 / 已完结 / 触发单"三 tab 的 UI 模式。

可选路线：
- **A. BFF 直接读 MySQL**：最短路径，但 BFF 要加 MySQL 驱动 + 连接池
  + schema 知识，逐渐偏离"网关"定位。schema 演进（将来 orders
  切 ClickHouse、trades 切 parquet）会反向绑住 BFF 发布节奏。
- **B. 独立 history 服务**：BFF 保持薄网关，history 专司读投影。
  代价：多一个部署单元、多一跳 RPC。
- **C. 复用已有服务**：例如 Counter 吃下历史查询 — 但 Counter 已经
  按 user_id 分了 10 shard，跨 shard 聚合不是其职责；trigger
  做触发单历史也要额外建 MySQL 投影。二者都是主写路径，历史查询
  混进去会挤压热路径预算。

## 决策 (Decision)

采纳 **方案 B**：新增 `history/` 模块 + `HistoryService` gRPC，纯只读
读 trade-dump 已有的 MySQL 投影（ADR-0008 / 0023 / 0028），BFF 的
列表类历史接口全部改走它。

### 接口矩阵

| UI tab | BFF REST | 走哪 | 数据源 |
|---|---|---|---|
| 挂单 | `GET /v1/orders?scope=open` | history | `orders` |
| 已完结 | `GET /v1/orders?scope=terminal` | history | `orders` |
| 单笔查询（活跃） | `GET /v1/order/:id` | Counter | Counter in-memory |
| 成交明细 | `GET /v1/trades` | history | `trades` |
| 资金流水 | `GET /v1/account-logs` | history | `account_logs` |
| 触发单列表 | `GET /v1/trigger` | trigger gRPC | trigger in-memory + snapshot |

### 为什么 `GET /v1/order/:id` 不改走 history

单笔查询是活跃订单的 hot path，Counter 内存里的成交进度比 MySQL
实时 1 批。查终态单的场景可以走 `GET /v1/orders?scope=terminal` +
`order_id` 过滤（列表接口足以覆盖）。未来若产品需要统一回退，再
加 "Counter NotFound → history 兜底" 的两跳模式，改动点集中在
BFF 一个 handler。

### 触发单不塞进 HistoryService

触发单有独立状态机（`stop_price` / `trailing_delta_bps` /
`oco_group_id` 等）、自己的服务权威源（trigger，ADR-0040），且
PENDING 触发单根本没落 MySQL，history 读 MySQL 也查不到。查询继续
走 `TriggerService.ListTriggers` / `QueryTrigger`。

触发后产生的真实订单 (`client_order_id="trig-<id>"`) 本来就
落 `orders` 表，`ListOrders` 正常返回；BFF 基于前缀打
`source="trigger"` 标记，产品可以筛出"来自触发单触发的订单"。

触发单长期历史（重启后还能看到三个月前的 TRIGGERED / EXPIRED）是
MVP-16 的工作，需要额外加 `trigger-event` topic + trade-dump
projection，才能让 history 作为权威查询入口。

## 备选方案 (Alternatives Considered)

### 方案 A：BFF 读 MySQL

短期最省事，长期 BFF 被 schema 绑住。一旦 `orders` 要切 ClickHouse
或 `trades` 要做冷热分层，BFF 要跟着改 — 而 BFF 同时承担 REST /
WS 反代 / 限流 / 认证，发布节奏必须稳。拒绝。

### 方案 C：Counter / trigger 自己吐历史

Counter 按 user_id 分 10 shard，跨 shard 做 list + 排序 + 分页要
建一套扇出聚合；而且 Counter 是强主路径，历史查询进去会挤写路径的
goroutine 预算。trigger 同理，且它自己都不持久化 PENDING 态
的长期历史。拒绝。

### 方案 D：把 history 放 trade-dump 进程内

trade-dump 已经连着同一套 MySQL，理论上可以多开一个 gRPC 端口吐读。
但 trade-dump 的可用性模型是"落后几秒可接受"，可以随时重启；history
是面向客户端的实时读 API，SLO 不一样。职责分离，拒绝合并。

## 理由 (Rationale)

- **BFF 保持网关薄形态**：符合 ADR-0029 / 0039 一贯的"BFF 不懂业务"
  路线。
- **schema 演进自由**：`orders` 未来切 ClickHouse / `trades` 切
  S3+parquet，改 history 读层即可，BFF 零感知。
- **横向扩展便宜**：history 无状态，k8s replica + MySQL 只读副本
  就能扩；而 Counter / trigger 扩容还要动 sharding / HA。
- **和 ADR-0001 权威模型兼容**：Kafka 仍是事实源，trade-dump 仍是
  唯一投影入口，history 只读下游表，不反向回写。

## 影响 (Consequences)

### 正面

- 客户端有了统一的历史查询入口：orders / trades / account-logs。
- 三大列表接口一次性上线，产品不用再拿 Counter 活跃单拼凑。
- 冷热分层演进路径清晰：未来可以在 `Store` 接口下面接多个
  backend（MySQL 热 + ClickHouse 温 + S3 冷）而不动 BFF 协议。

### 负面 / 代价

- 多一个部署单元、一跳 RPC；延迟增加 ≤ 1 ms（同数据中心）。
- trade-dump 落库延迟（ADR-0023：MySQL 提交成功后才推 Kafka
  offset）导致 history 比 Kafka 滞后 ≤ 1 批；需要在 REST 文档里
  明确"实时进度走 WS，history 用来事后查"。
- `trades` 表的 user 查询要跨 `idx_user` (maker) 和 `idx_user2`
  (taker) UNION ALL，两倍索引扫 — MVP 可接受；若成为热点再合一
  个 `user_id` 合并索引。

### 中性

- Cursor 分页采用 base64(JSON)，opaque 契约对客户端；未来想切
  numeric offset / keyset 换 struct 就行，协议面零破坏。
- Scope 枚举只给 OPEN / TERMINAL / ALL 三档，细粒度走 `statuses`
  字段；如果将来产品要 FILLED / FAILED 两档再扩。

## 实施约束 (Implementation Notes)

- `history/` 独立 go module，注册进 go.work 和 Makefile MODULES。
- Store 层 SQL 里对 `created_at` / `ts` 做游标 `(col < last OR col = last AND pk < last_pk)` 严格 <，避免游标边界重复行。
- limit 在 store 层 clamp：默认 100，上限 500（MVP-15 §范围）。
- `--mysql-dsn` 建议指向 MySQL 只读副本；历史查询和主写完全隔离，
  MySQL lag 由 trade-dump 自己的 offset 守护（ADR-0023）。
- BFF 的 `--history` flag 为空时三条 REST 返回 503，CI / 本地开发
  可以零依赖运行（沿用 --trigger / --market-brokers 的惯例）。
- Order.reject_reason 在 store 层从 int8 折成字符串 (`invalid_price_tick`
  / `fok_not_filled` 等)，复用 event.RejectReason 枚举；BFF 继续做
  status 字符串（`new` / `filled` / …）翻译，wire 上走 proto enum。
- `GET /v1/orders` 对触发单加 `source="trigger"` 标记，
  判定规则 `client_order_id` 以 `trig-` 前缀开头（ADR-0040）。

## 参考 (References)

- [ADR-0001](0001-kafka-as-source-of-truth.md) — 权威模型
- [ADR-0008](0008-sidecar-persistence-trade-dump.md) — trade-dump 旁路持久化
- [ADR-0023](0023-trade-dump-batching-and-commit-order.md) — trade-dump 提交顺序 / lag 语义
- [ADR-0028](0028-trade-dump-journal-projection.md) — orders / accounts / account_logs 投影
- [ADR-0040](0040-trigger-order-service.md) — 触发单服务（client_order_id `trig-` 前缀）
- [roadmap MVP-15 / MVP-16](../roadmap.md#mvp-15-history)
