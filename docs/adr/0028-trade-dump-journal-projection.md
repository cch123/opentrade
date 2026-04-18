# ADR-0028: trade-dump 投影 counter-journal 到 orders / accounts / account_logs

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0004, 0008, 0023

## 背景 (Context)

MVP-5（ADR-0023）让 `trade-dump` 消费 `trade-event` 写 `trades` 单表。
MySQL schema 里的另外三张表 —— `orders` / `accounts` / `account_logs` —— schema
在 [01-schema.sql](../../deploy/docker/mysql-init/01-schema.sql) 已经建好，但无
人写入。

`counter-journal` topic 含所有账户状态变更（ADR-0004）。MVP-9 让 trade-dump 把它
投影到这三张表，保持 "Counter/Match 不直接写 MySQL" 的架构（ADR-0008）不变。

设计要回答的几件事：

1. **一条事件可能产生多行**：`SettlementEvent` 同时带 `base_balance_after` 和
   `quote_balance_after`，要写两行 `account_logs`。现有 PK `(shard_id, seq_id)`
   会冲突。
2. **orders 表需要 Insert 和 Update 两种写法**：`FreezeEvent` 建新行，
   `OrderStatusEvent` 改已有行。MySQL 没有"幂等 bulk UPDATE"。
3. **乱序保护**：Kafka 至少一次消费 + consumer 重启可能重放老事件，怎么防止
   老值覆盖新状态。
4. **trade-event 和 counter-journal 两个 consumer 的关系**：独立 group、独立
   goroutine，共享 MySQL pool？

## 决策 (Decision)

### 1. account_logs PK 改为 `(shard_id, seq_id, asset)`

原 schema 的 PK `(shard_id, seq_id)` 容不下 SettlementEvent 一条事件对 base +
quote 两个 asset 的两行写入。扩 PK：

```sql
PRIMARY KEY (shard_id, seq_id, asset)
```

- 对 non-Settlement 事件（一条 = 一 asset）完全透明。
- 所有 `INSERT ... ON DUPLICATE KEY UPDATE shard_id = shard_id` 形成幂等 no-op。

### 2. orders 表拆 Insert / Update 两种写法

- `FreezeEvent`（单次，建新行）→ 多行 `INSERT ... ON DUPLICATE KEY UPDATE
  order_id = order_id`。重放一次 Freeze 不会覆盖已经进入 FILLED 的状态。
- `OrderStatusEvent`（多次，改状态）→ 单行 `UPDATE orders SET status=...,
  filled_qty=..., updated_at=... WHERE order_id=?`。不加 seq_id 或 updated_at
  guard —— 单 goroutine 按 Kafka offset 顺序处理，自然时序。
- `CancelRequested` → 不写 MySQL。同时序的 `OrderStatusEvent` 会把订单置为
  PENDING_CANCEL → CANCELED，重复写会制造多余 UPDATE。

### 3. accounts 表用 seq_id guard

```sql
INSERT INTO accounts (...) VALUES (...)
ON DUPLICATE KEY UPDATE
  available = IF(VALUES(seq_id) >= seq_id, VALUES(available), available),
  frozen    = IF(VALUES(seq_id) >= seq_id, VALUES(frozen), frozen),
  seq_id    = IF(VALUES(seq_id) >= seq_id, VALUES(seq_id), seq_id)
```

- counter-journal 是 shard-monotonic（ADR-0004），同 (user, asset) 的新事件
  seq_id 只增不减。guard 防御：
  1. 消费 lag / 重启时的乱序可能性（不应该发生，但我们不靠不出问题）；
  2. 未来多实例 counter 同一 shard 短暂主备双写的极端窗口（ADR-0002 HA 未上线
     前的保护）。

### 4. 两个 consumer，独立 group，共享 MySQL pool

- `trade-dump-trade-<instance>`：消费 trade-event → trades 表（MVP-5）
- `trade-dump-journal-<instance>`：消费 counter-journal → orders / accounts /
  account_logs（MVP-9）
- 同一个 `writer.MySQL`，两个 goroutine 并行 poll + 写；不同 tx 互不干扰。

### 5. shard_id 来源：parse `EventMeta.producer_id`

Counter 的 producer_id 形式是 `counter-shard-<N>-{main|backup}`。trade-dump
侧 parse 出 N 写入 `account_logs.shard_id`。

格式不匹配（例如 producer_id 是 `counter-ha-0` 这种未来命名）→ 跳过
account_logs（accounts 仍然落库，因为不需要 shard_id）。

### 6. 单 goroutine 顺序处理一个 poll batch

和 MVP-5 一致。跨 partition 并行留给将来压测驱动。counter-journal 按 user_id
分 partition，跨 user 并行 OK；但单 goroutine 顺序处理对 MVP 已经够用。

## 备选方案 (Alternatives Considered)

### A. `OrderStatusEvent` 走 upsert + 每列 IF guard
- 优点：严格防乱序。
- 缺点：SQL 膨胀且难读；当前单 consumer 单 goroutine 的时序保证已经足够；
  加了 guard 反而让"一行 UPDATE 没生效"成为一个沉默情况（要额外 log），
  rollback 决策也更复杂。
- 结论：等真实出现乱序问题（例如上多实例并行）再加 guard。

### B. 一条 SettlementEvent 聚合成一行 account_logs（join asset 列表）
- 优点：保持 PK (shard_id, seq_id) 简单。
- 缺点：单行多 asset 破坏"一条 log = 一笔 asset 变动"的语义；MySQL 查询
  `sum(delta) by user, asset` 变麻烦。
- 结论：扩 PK 比挤 row 便宜。

### C. 只消费 `(type, seq_id, asset)` 过滤的 counter-journal 子集
- 优点：每个 table 独立 consumer，失败隔离。
- 缺点：要么 3 个 consumer group（×3 Kafka 流量）要么客户端过滤（多读少用）；
  MVP 一个 journal consumer 同 tx 三表更省网。
- 结论：单 consumer 单 tx。

### D. producer_id 加 schema 字段（显式 shard_id）
- 优点：不依赖字符串 parse。
- 缺点：要改 proto + 所有 Counter emit 路径。
- 结论：现有命名足够规范，parse 可靠；有 trip-wire 测试（`TestShardIDFromProducer`）
  防止将来命名变更悄悄 break。

## 理由 (Rationale)

- **一 tx 三表** 让 orders 插入和 accounts 更新原子：下游查"order + 账户快
  照"时不会看到半步态。
- **单 consumer 保时序 + 幂等 upsert**：MVP 不靠并发，用最少机制。
- **seq_id guard 只加在 accounts**：orders 本身靠顺序时序 OK；accounts 要防
  未来多写场景。

## 影响 (Consequences)

### 正面

- 历史订单 / 余额 / 流水查询从 MySQL 可用（前提：trade-dump 在线过一段时间）。
- 重启 → 从上次 committed offset 继续，幂等 upsert 保证重放安全。
- trade-dump 变两路消费，失败隔离（trade 消费挂不影响 journal 消费推进）。

### 负面 / 代价

- 两 consumer 共享 MySQL pool：单路压力大时会影响另一路；MVP 不做 pool 拆分。
- `account_logs.shard_id` 来自 parse；命名一旦改（future MVP-12 HA 改 producer
  id）要同步更新 parser + test。
- `orders` 里 `frozen_amt` 只在 FreezeEvent 初始化；后续 partial fill 不更新。
  真实冻结量要看 `accounts.frozen` 的快照。MVP 不做 "frozen per order" 明细。

### 中性

- `TransferEvent.TRANSFER_TYPE_FREEZE / UNFREEZE` 两种类型本身也走 accounts
  upsert，但和 PlaceOrder 的 FreezeEvent 并不相同（前者独立流水，后者跟随订单
  生命周期）。journal projection 都能覆盖。

## 实施约束 (Implementation Notes)

- Writer 实现：[trade-dump/internal/writer/mysql_journal.go](../../trade-dump/internal/writer/mysql_journal.go)
  + 纯 projection 函数 [projection.go](../../trade-dump/internal/writer/projection.go)
- Consumer：[trade-dump/internal/consumer/journal.go](../../trade-dump/internal/consumer/journal.go)
- main.go 新增 flag：
  - `--journal-topic=counter-journal`
  - `--journal-group=trade-dump-journal-<instance>`
  - 原 `--group` 改名 `--trade-group` —— **不兼容变更**（deploy 脚本要更新；
    dev 默认值自动 rename）
- schema 变更：`account_logs` PK 调整。现有 dev 环境需要清 mysql volume
  (`docker compose down -v mysql`) 然后重启让 init script 重跑。生产迁移见下。

### 生产迁移 account_logs（本 ADR 不做）

向后演进路径：

```sql
ALTER TABLE account_logs DROP PRIMARY KEY;
ALTER TABLE account_logs ADD PRIMARY KEY (shard_id, seq_id, asset);
```

在 trade-dump 上线新版本前执行。如果老表已经有 Settlement 写入失败留下的脏数据，
先用 `SELECT shard_id, seq_id, COUNT(*) GROUP BY 1,2 HAVING COUNT(*) > 1` 检查。

## 参考 (References)

- ADR-0004: counter-journal topic 定义
- ADR-0008: sidecar persistence
- ADR-0023: trade-dump 批量写入与 offset 提交顺序
- 实现：
  - [trade-dump/internal/writer/projection.go](../../trade-dump/internal/writer/projection.go)
  - [trade-dump/internal/writer/mysql_journal.go](../../trade-dump/internal/writer/mysql_journal.go)
  - [trade-dump/internal/consumer/journal.go](../../trade-dump/internal/consumer/journal.go)
  - [trade-dump/cmd/trade-dump/main.go](../../trade-dump/cmd/trade-dump/main.go)
  - [deploy/docker/mysql-init/01-schema.sql](../../deploy/docker/mysql-init/01-schema.sql)
