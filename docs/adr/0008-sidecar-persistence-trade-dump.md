# ADR-0008: Counter/Match 不直接写 MySQL，通过 trade-dump 旁路持久化

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0001, 0004

## 背景

交易系统需要持久化订单、成交、账户流水到 MySQL 供历史查询。选项：
1. Counter/Match 直接写 MySQL
2. 旁路 trade-dump 服务消费 Kafka 写 MySQL

MySQL 单实例 20w TPS 写入扛不住，需要分库分表；但直接让热路径服务承担 MySQL 写入压力和一致性复杂度不合适。

## 决策

**Counter 和 Match 不直接写 MySQL。** 独立的 `trade-dump` 服务订阅 Kafka（`counter-journal` + `trade-event`），按幂等键写入 MySQL。

## 备选方案

### 方案 A：Counter/Match 直接写 MySQL
- 优点：最终一致性延迟低
- 缺点：
  - 热路径多一个外部依赖
  - MySQL 故障会反压到撮合
  - 分库分表逻辑污染业务代码
  - 双写（内存 + MySQL）一致性复杂

### 方案 B（选中）：旁路 trade-dump
- 优点：
  - 热路径无 MySQL 依赖
  - MySQL 故障只影响历史查询，不影响交易
  - trade-dump 可独立扩缩容，按业务分表
  - Kafka 堆积缓冲，MySQL 恢复后自动追赶
  - CQRS 模式：写到 Kafka，读从 MySQL
- 缺点：
  - 新增一个服务要运维
  - 历史查询有最终一致性延迟（秒级）

## 理由

- **关键路径解耦**：撮合/结算不等 MySQL
- **Kafka 即权威（ADR-0001）**：MySQL 只是 projection，丢失可重建
- **扩容独立**：MySQL 写入瓶颈不影响撮合；trade-dump 可按 topic partition 水平扩展

## 影响

### 正面
- 撮合链路与持久化链路解耦
- MySQL 故障只影响查询，不影响交易
- 按业务需求重建投影方便（下 Kafka，按新 schema 重新 dump）

### 负面
- 历史查询延迟：新成交进 MySQL 约秒级延迟
- "最近 24h" 类查询如果走 MySQL 会看到延迟数据，需要路由到 Counter 内存

### 中性
- MySQL schema 可独立演进，不影响核心撮合

## 实施约束

### 幂等写入

MySQL 表必含 `seq_id` 或 `(topic, partition, offset)` 唯一索引：

```sql
CREATE TABLE trades (
    trade_id BIGINT PRIMARY KEY,
    symbol VARCHAR(32) NOT NULL,
    maker_order_id BIGINT NOT NULL,
    taker_order_id BIGINT NOT NULL,
    price DECIMAL(36, 18) NOT NULL,
    qty DECIMAL(36, 18) NOT NULL,
    taker_side TINYINT NOT NULL,
    ts BIGINT NOT NULL,
    seq_id BIGINT NOT NULL,
    UNIQUE KEY uk_seq (symbol, seq_id)
);
```

写入语句用 `INSERT ... ON DUPLICATE KEY UPDATE id = id`（no-op upsert）保证重复消费无副作用。

### trade-dump 服务结构

- 按 Kafka partition 分配 consumer（同 partition 单线程保序）
- 每个 partition 单独的 MySQL connection pool
- 批量写入（攒几十条一 tx，提高吞吐）
- commit 策略：MySQL 事务 commit 成功后才 commit Kafka offset

### 读路径分层

| 查询 | 路由 |
|---|---|
| 当前余额 | Counter 内存（gRPC） |
| 活跃订单 | Counter 内存 |
| 最近 24h 订单 / 成交 | 先查 Counter 内存（若仍在活跃列表），补查 MySQL |
| 历史订单 / 成交（>24h） | 直接 MySQL |
| 账户流水 | MySQL |

### 对账

- 每小时对比 Counter 内存余额 vs MySQL 聚合 `sum(delta) group by user_id, asset`
- 差异超阈值报警
- 主要用于监控，不作为正常读路径

## 参考

- ADR-0001: Kafka as Source of Truth
- ADR-0004: counter-journal topic
- CQRS 模式
