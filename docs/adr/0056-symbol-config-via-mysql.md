# ADR-0056: Symbol 配置存储从 etcd 迁到独立 MySQL（版本位点轻量 poll）

- 状态: Proposed
- 日期: 2026-04-20
- 决策者: xargin, Claude
- 相关 ADR: 0001（Kafka 作为事件权威源）、0002（Counter HA via etcd lease）、0030（match etcd symbol sharding）、0031（HA cold standby）、0042（conditional HA）、0052（admin console）、0053（symbol 精度治理）、0054（per-symbol 挂单数上限）

## 术语 (Glossary)

| 本 ADR 字段 | 含义 |
|---|---|
| `symbol_config` | 新建 MySQL 表，承载现在 etcd 里 `SymbolConfig` 的全部字段（shard / trading / asset / 精度 / ADR-0054 挂单数上限） |
| `config_version` | 单行单列表，含一个全局 `global_version BIGINT`，每次任意 symbol_config 变更都 +1。服务用它做"变了没"的 O(1) 判定 |
| `symbol_config_history` | 审计表，记录每次 symbol_config 变更的 before/after JSON + actor + 时间 |
| `opentrade_config` | 新增独立 MySQL 库（独立实例或独立 schema，部署期决定） |

## 背景 (Context)

当前（ADR-0030 起）所有 symbol 级配置走 etcd：

- **Config 源**：admin-gateway 写 `/cex/match/symbols/<symbol>`（ADR-0052），KV 内容是 `SymbolConfig` JSON
- **消费方**：match / counter / conditional / BFF 通过 `pkg/etcdcfg.EtcdSource` 的 `List + Watch` 读取并缓存到各自的 symregistry
- **配套用 etcd 的还有**：HA leader election（ADR-0002 / 0031 / 0042）—— 本 ADR 不动这部分

### 为什么要搬

1. **etcd 是 Raft 集群**：MVP 阶段用 3-5 节点太重，运维面（版本升级 / 磁盘整理 / compaction）成本不对称于"每天变几次的 symbol 配置"
2. **缺 schema 强约束**：etcd KV 存 JSON，字段拼写错、类型错、范围错都要靠客户端 `Validate*` 函数兜底。换成 DB column + `CHECK` 约束可以上移到存储层
3. **审计链路薄**：etcd 的 `revision` 只能线性回放，做"这个 symbol 上周谁改了什么"要翻 admin-gateway 的 JSONL audit，和配置本身没原子绑定
4. **和 config 语义契合度低**：etcd 的强一致 / watch / 选主是为"协调"设计的；symbol config 是"低频 CRUD + 推送"的产品面数据，本来就更像一张业务表

### 为什么不走 Kafka compacted topic 路线

本 ADR 讨论阶段曾提出把 `symbol-config` 放到 log-compacted Kafka topic（对齐 ADR-0001 "Kafka 作为事件权威源"，零新基础设施，毫秒级推送）。用户拍板走 MySQL，理由：

- 配置**本质是业务状态**，不是事件流；用关系表天然查得动"当前活跃 symbol 列表"、"precision_version 排序"、"按 quote_asset 筛"等多维查询
- 独立 DB 和现有 `trade-dump` / `history` MySQL **隔离 IO / 隔离 schema migration 风险**
- 运维习惯：DBA 工具链（备份 / 审计 / DDL 变更流程）已成熟，Kafka compacted topic 的"内容管理"工具链单薄

Kafka 路线记录在 "备选方案 A" 留档。

## 决策 (Decision)

### 1. 独立 MySQL 库 + 三张表

部署上**独立库** `opentrade_config`（可先和其他库共用实例起步，随后再拆实例）。

```sql
CREATE DATABASE opentrade_config CHARACTER SET utf8mb4;

CREATE TABLE symbol_config (
    symbol                        VARCHAR(32)   PRIMARY KEY,
    shard                         VARCHAR(32)   NOT NULL,
    trading                       BOOLEAN       NOT NULL DEFAULT false,
    version_tag                   VARCHAR(64),                -- ADR-0030 "version" 字段（灰度标签）
    base_asset                    VARCHAR(16),
    quote_asset                   VARCHAR(16),
    precision_version             BIGINT        NOT NULL DEFAULT 0,
    tiers_json                    JSON,                        -- ADR-0053 tiers
    scheduled_change_json         JSON,                        -- ADR-0053 scheduled_change
    max_open_limit_orders         INT UNSIGNED  NOT NULL DEFAULT 0,   -- ADR-0054
    max_active_conditional_orders INT UNSIGNED  NOT NULL DEFAULT 0,   -- ADR-0054
    updated_at                    TIMESTAMP(6)  NOT NULL
                                  DEFAULT CURRENT_TIMESTAMP(6)
                                  ON UPDATE CURRENT_TIMESTAMP(6),
    CHECK (max_open_limit_orders         <= 10000),
    CHECK (max_active_conditional_orders <= 10000),
    INDEX idx_updated_at (updated_at)
) ENGINE=InnoDB;

CREATE TABLE symbol_config_history (
    id          BIGINT        PRIMARY KEY AUTO_INCREMENT,
    symbol      VARCHAR(32)   NOT NULL,
    actor       VARCHAR(64)   NOT NULL,                     -- admin-gateway audit 的 role/api-key id
    before_json JSON,                                        -- NULL for first insert
    after_json  JSON,                                        -- NULL for delete
    changed_at  TIMESTAMP(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_symbol_changed_at (symbol, changed_at),
    INDEX idx_changed_at        (changed_at)
) ENGINE=InnoDB;

CREATE TABLE config_version (
    id             TINYINT       PRIMARY KEY DEFAULT 1,     -- 单行
    global_version BIGINT        NOT NULL DEFAULT 0,
    updated_at     TIMESTAMP(6)  NOT NULL
                   DEFAULT CURRENT_TIMESTAMP(6)
                   ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;

INSERT INTO config_version(id, global_version) VALUES (1, 0);
```

### 2. Watch 机制 — 版本位点 poll（非全表 poll）

每个服务的 symregistry 启动时：

1. 读 `global_version` + `SELECT * FROM symbol_config` 一次，填充 cache
2. 记录 `lastSeen = global_version`
3. 进入 poll loop（默认 1s 间隔）：

```go
for {
    select {
    case <-ctx.Done(): return
    case <-tick.C:
        v := queryGlobalVersion(ctx)               // < 1ms, 单行
        if v == lastSeen { continue }               // 95%+ 的 tick 走这条
        rows := queryChangedSince(ctx, lastFullAt)  // SELECT * WHERE updated_at > ?
        for _, r := range rows {
            registry.Put(r.Symbol, r.ToConfig())
        }
        // deletes 另走：查 symbol_config_history 里 after_json IS NULL 的条目
        lastSeen = v
        lastFullAt = now
    }
}
```

**设计要点**：
- **热路径 = 读 1 行 BIGINT**：每服务每秒 1 次 × 5-10 服务 = 每秒 <10 个 single-row SELECT，DB 负担可忽略
- version 不变就不碰 symbol_config 表，省掉"1000 symbol × 每秒"的读放大
- 实时性 **~1s**（可调 `--config-poll-interval`）。symbol config 是产品面低频变更（开交易对、切精度、调槽位），秒级完全够

### 3. admin-gateway 写事务

所有变更走 `PUT /admin/symbols/{symbol}` 和对应的 `/precision/*` 端点。handler 在**同一事务**内：

```sql
BEGIN;
  -- 1. 取 before 快照（如果存在）
  SELECT * FROM symbol_config WHERE symbol = :sym FOR UPDATE;
  -- 2. 升/插入
  INSERT INTO symbol_config (...) VALUES (...)
    ON DUPLICATE KEY UPDATE shard = :shard, trading = :trading, ... ;
  -- 3. 审计
  INSERT INTO symbol_config_history(symbol, actor, before_json, after_json)
    VALUES (:sym, :actor, :before, :after);
  -- 4. 水位 +1
  UPDATE config_version SET global_version = global_version + 1 WHERE id = 1;
COMMIT;
```

`actor` 从现有 admin-gateway JSONL audit 里的 role-tagged API-Key id 取（ADR-0052）。JSONL audit 继续写，双保险。

### 4. 代码面改动（最小侵入）

- **新增** `pkg/cfgstore/mysql.go`：实现既有 `etcdcfg.Source` interface（`List` / `Watch`）。只换存储 backend，所有消费方代码不动
- **admin-gateway**：`s.etcd.Put(...)` 替换为 `db.ExecTx(...)`；JSONL audit 保留
- **各服务 main.go**：`etcdcfg.NewEtcdSource(...)` 替换为 `cfgstore.NewMySQLSource(...)`
- **ADR-0054 backlog 第 2 项** 顺手做掉：conditional 的 `SymbolLookup` 也用新 source 接上
- **ADR-0054 backlog 第 3 项** 顺手做掉：`counter/internal/symregistry` 搬到 `pkg/symregistry/`（因为 cfgstore 要提供给 match / counter / conditional 三家共用）

### 5. Leader election 保留在 etcd

HA 选主（ADR-0002 / 0031 / 0042）**不动**。etcd 不退役，只是退化为"只跑 leader election"的轻量使用。理由：

- etcd lease/election 语义成熟，迁移成本不对称于收益
- 未来可单独评估切 MySQL `GET_LOCK` / Redis Redlock / k8s leases，不绑在 config 迁移里

## 备选方案 (Alternatives Considered)

### A. Kafka compacted topic（`symbol-config`）

- key=symbol，value=SymbolConfig proto；log compaction 让 topic 自己成为"最新全量 snapshot"
- admin-gateway PUT 时 produce 到 topic；各服务 consumer 消费即拿到 cache
- **优点**：零新基础设施（Kafka 已在）、毫秒级推送、对齐 ADR-0001、审计靠 retention 天然覆盖
- **缺点**：KV 语义不如关系表灵活（多维筛选、JOIN 审计、DBA 工具链薄弱）
- **结论**：用户拍板 MySQL，记此方案留档。如果未来 MySQL 运维成为痛点可翻回

### B. PostgreSQL LISTEN/NOTIFY

- 触发器 + `pg_notify` 直接推送给订阅服务
- **优点**：推送是 DB-native，毫秒级
- **缺点**：要把 MySQL 迁 Postgres，博弈太大；且 `NOTIFY` payload 大小有限，还是要走 SELECT 拉全量
- **结论**：拒。Postgres 值得做但不是为了这个功能

### C. MySQL binlog → canal → 各服务订阅

- 实时性最好的 MySQL 方案
- **优点**：真实时（<100ms），不需要 poll
- **缺点**：canal 集群自身是基础设施；对"每天变几次的配置"过度设计
- **结论**：拒。版本位点 poll 的 ~1s 延迟完全够用，简单压倒一切

### D. 继续 etcd + 加 schema 校验层

- 在 `pkg/etcdcfg` 里加更多 `Validate*` 函数
- **优点**：零迁移
- **缺点**：etcd 本身的"运维重"问题没解决；DB 的多维查询 / 审计 JOIN 拿不到
- **结论**：拒。本 ADR 的触发动因就是"想去 etcd"

### E. admin-gateway gRPC stream 推送

- admin-gateway 保留一个 long-lived 订阅流，每个服务连上
- **缺点**：admin-gateway 变有状态（要维护 N 个连接 + backlog），违反它当前 stateless 的定位
- **结论**：拒

## 理由 (Rationale)

- **version 位点 poll 是 "LOW 但不蠢"**：热路径读 1 行 BIGINT，等价于 watch 的延迟常数（~1s vs etcd 的 <100ms）但复杂度为零
- **关系表的查询 / 审计能力**：`SELECT * FROM symbol_config_history WHERE symbol='BTC-USDT' ORDER BY changed_at DESC LIMIT 20` 是 etcd 做不到的一等公民查询
- **DBA 工具链复用**：备份、binlog 远端、schema migration、监控面板都是成熟工具
- **独立 DB 隔离**：trade-dump / history 的 IO 峰值不会影响 config 读；反向也一样
- **渐进切换**：换 backend，不换上层接口 —— PR 量可控

## 影响 (Consequences)

### 正面

- etcd 仅剩 leader election 用途，集群可以降配到 3 节点最小规格
- Schema 约束上移到 DB（`CHECK`、`NOT NULL`、类型强制），客户端 `Validate*` 函数降级为辅助
- 审计变 first-class：用 SQL 直接查历史，不用翻日志
- 为后续"VIP 白名单 / 做市商 fee rate / 跨 symbol 批量操作"等扩展留了一张容易加列的表
- 顺手清了 ADR-0054 的 backlog 第 2、3 项（conditional SymbolLookup + symregistry 搬家）

### 负面 / 代价

- **切换期双写风险**：迁移 window 内两边数据源不一致会让部分服务读老、部分读新。MVP 建议"停写 etcd → 迁表 → 启动新 source → 验证 → 删 etcd 配置"顺序，避免双写
- **延迟从 etcd watch 的 <100ms 涨到 ~1s**：symbol config 变更是低频，可接受；但要在 runbook 里写明"切精度生效最多等 1s"
- **新 MySQL 实例/库**：就算共用现有实例，也是新 schema + 新 user + 新备份策略，增加 1 份运维面
- **一次性迁移工作**：代码 ~500 LOC（建表 + `pkg/cfgstore/mysql.go` + admin-gateway 切换 + 服务切换）+ 运维步骤文档；估 1 周

### 中性

- **leader election 还要 etcd**：未来可以再起 ADR 评估 `GET_LOCK` / Redlock / k8s leases
- **`pkg/etcdcfg` 不删**：字段定义（`SymbolConfig` 结构体、`ValidateTiers` 等）保留，改名或留档；只删 `EtcdSource`

## 实施约束 (Implementation Notes)

### 切换步骤（灰度推荐）

1. **P0** 建表 + 写 `pkg/cfgstore/mysql.go`（实现 `Source` interface）+ 单测
2. **P1** admin-gateway 双写：`PUT /admin/symbols` 同时写 etcd + MySQL；审计 JSONL 保留不动
3. **P2** 服务 flag 新增 `--cfg-backend=etcd|mysql`（默认 etcd）；逐个服务切 `--cfg-backend=mysql`，观察 1-2 天对齐
4. **P3** 全服务切完 → admin-gateway 停写 etcd；灰度 1 周无异常后物理清 etcd 里 `/cex/match/symbols/*` key
5. **P4** 代码清理：删双写分支，删 `etcdcfg.EtcdSource` 的 symbol-config 相关分支（lease / election 的 etcd 访问保留）

### Flag

- admin-gateway：`--cfg-mysql-dsn` / `--cfg-mysql-db=opentrade_config`
- 各服务：`--cfg-backend=etcd|mysql`（迁移后删）+ `--cfg-mysql-dsn` + `--config-poll-interval=1s`
- 保留 `--etcd` 但文档里标注"仅用于 leader election"

### 测试

- 单元：`cfgstore.NewMySQLSource` 的 `List` / `Watch` 覆盖：正常路径、version 不动、version 跳变、删除 symbol、并发写
- 集成：admin-gateway PUT → 1s 内 counter / match / conditional cache 看到新值；precision rollout（ADR-0053）端到端
- 迁移测试：P1 双写期，etcd 和 MySQL diff 应始终 = 0

### 失败场景

| 情况 | 结果 |
|---|---|
| MySQL 不可达 | 服务启动失败（fail-fast）；运行中 → poll error 打 WARN，继续用 in-memory cache |
| config_version 意外倒退 | symregistry 依然按 `updated_at > lastFullAt` 读全量差异；位点不前进但 cache 不脏 |
| updated_at 时钟回拨 | 用 BIGINT row_version 列代替 TIMESTAMP 可彻底避（建议实施时采纳） |
| admin-gateway 事务部分提交 | DB 原子性保证，要么全入要么全不入；consumer 看到 version +1 才会重读 |
| 审计表写失败 | 整事务 rollback；PUT 返回 500；不会产生"改了但没审计"的窗口 |

### 迁移 runbook 提要

1. 应用 schema（M1）
2. admin-gateway 升级到双写（M2）
3. 跑脚本一次性把现有 etcd 内容导入 MySQL（idempotent）
4. 灰度切服务（M3）
5. 观察一周，无异常则物理清 etcd 的 symbol-config 前缀（M4）
6. 代码清理删双写分支（M5）

## 参考 (References)

- [ADR-0001](./0001-kafka-as-source-of-truth.md) — 事件权威源（本 ADR 显式拒绝把配置也放进 Kafka，区分 event / state）
- [ADR-0030](./0030-match-etcd-sharding-rollout.md) — 当前 match 从 etcd 读 SymbolConfig 的路径
- [ADR-0052](./0052-admin-console.md) — admin-gateway 的 PUT 入口 + JSONL 审计
- [ADR-0053](./0053-symbol-precision-and-tiered-evolution.md) — tiers_json / scheduled_change_json 字段的业务语义
- [ADR-0054](./0054-per-symbol-order-slots.md) — max_open_limit_orders / max_active_conditional_orders 字段的业务语义
- 实现位置（动工时涉及）：
  - 新增 `pkg/cfgstore/mysql.go`
  - 新增 `pkg/symregistry/`（从 counter/internal 搬家）
  - `admin-gateway/internal/server/server.go` — handlePutSymbol 切事务写
  - counter / match / conditional `cmd/*/main.go` — 切 Source
