# ADR-0065: Funding wallet 以 MySQL 为权威源

- 状态: Proposed
- 日期: 2026-04-23
- 决策者: xargin, Codex
- 相关 ADR: 0011(Counter Transfer 接口), 0046(History 服务), 0057(asset-service + transfer saga)

## 术语 (Glossary)

| 字段 | 含义 |
|---|---|
| `funding wallet` | asset-service 管理的资金账户，用户资金进入 spot/futures/earn 前的中转账户 |
| `trading account` | Counter 当前管理的交易账户，负责下单冻结、成交结算等撮合链路状态 |
| `opentrade_asset` | asset-service 独占 MySQL schema，已经承载 `transfer_ledger` |
| `funding_accounts` | funding wallet 当前余额权威表 |
| `funding_mutations` | funding wallet 的持久幂等和审计表，一次 TransferOut/In/Compensate 一行 |
| `transfer_ledger` | 跨账户划转 saga 状态表，仍由 ADR-0057 定义 |

## 背景 (Context)

ADR-0057 引入 asset-service 后，funding wallet 的最初实现沿用了 Counter 风格：进程内内存状态 + journal replay。这个方向对 Counter 合理，因为交易账户处在撮合热路径，状态变化和 Kafka 事件流强绑定。

但 funding wallet 的问题域不同：

- Asset Service 只管资金账户，交易账户已经由 Counter 管。
- funding wallet 不是撮合热路径，QPS 和延迟要求远低于下单 / 成交结算。
- asset-service 已经因为 `transfer_ledger` 依赖 MySQL，资金账户继续放 MySQL 不引入新的基础设施类型。
- 对资金账户来说，MySQL transaction、row lock、unique key、binlog、备份恢复和人工审计都更自然。

因此没有必要为了统一架构范式而让 Asset Service 发 Kafka 或维护额外 journal。统一资产视图如果未来需要，可以单独设计 CDC / outbox / 事件流；不进入 funding wallet MVP 的正确性路径。

## 决策 (Decision)

### 1. Asset Service 不直接和 Kafka 交互

Asset Service 的职责边界收敛为：

- 管理 `funding` 账户余额；
- 编排跨 biz_line transfer saga；
- 通过 gRPC 调用 Counter 等其他 AssetHolder；
- 对外提供 funding balance / transfer 查询。

Asset Service 不生产 Kafka 事件，不维护 asset-journal，不启动 outbox publisher，也不从 Kafka replay 恢复 funding 余额。

### 2. MySQL 是 funding wallet 的 source of truth

`opentrade_asset.funding_accounts` 是 funding 当前余额的唯一权威源。`QueryFundingBalance` 直接读这张表。

`transfer_ledger` 继续是 saga 状态权威源。funding balance 表只负责 funding 侧余额，不替代 saga ledger。

如果未来要做统一资产视图：

- 可以新增独立 ADR；
- 可以从 `opentrade_asset` 的 binlog / CDC / outbox 产生资产事件；
- 也可以让 History 服务读 asset-service API 或只读副本；
- 但这些都不是本 ADR 的 MVP 范围。

### 3. Schema

新增 / 扩展 `opentrade_asset` schema。`transfer_ledger` 保持 ADR-0057 语义。

```sql
CREATE TABLE IF NOT EXISTS funding_users (
    user_id          VARCHAR(64)     PRIMARY KEY,
    funding_version  BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at_ms    BIGINT          NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS funding_accounts (
    user_id          VARCHAR(64)     NOT NULL,
    asset            VARCHAR(16)     NOT NULL,
    available        DECIMAL(36,18)  NOT NULL DEFAULT 0,
    frozen           DECIMAL(36,18)  NOT NULL DEFAULT 0,
    balance_version  BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at_ms    BIGINT          NOT NULL,
    PRIMARY KEY (user_id, asset),
    KEY idx_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS funding_mutations (
    mutation_id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    transfer_id       VARCHAR(64)     NOT NULL,
    op_type           VARCHAR(32)     NOT NULL, -- transfer_out / transfer_in / compensate
    user_id           VARCHAR(64)     NOT NULL,
    asset             VARCHAR(16)     NOT NULL,
    amount            DECIMAL(36,18)  NOT NULL,
    peer_biz          VARCHAR(32)     NOT NULL DEFAULT '',
    memo              VARCHAR(256)    NOT NULL DEFAULT '',
    status            VARCHAR(32)     NOT NULL, -- CONFIRMED / REJECTED
    reject_reason     VARCHAR(256)    NOT NULL DEFAULT '',
    available_after   DECIMAL(36,18)  NOT NULL DEFAULT 0,
    frozen_after      DECIMAL(36,18)  NOT NULL DEFAULT 0,
    funding_version   BIGINT UNSIGNED NOT NULL DEFAULT 0,
    balance_version   BIGINT UNSIGNED NOT NULL DEFAULT 0,
    created_at_ms     BIGINT          NOT NULL,
    PRIMARY KEY (mutation_id),
    UNIQUE KEY uk_transfer_op (transfer_id, op_type),
    KEY idx_user_created (user_id, created_at_ms),
    KEY idx_transfer (transfer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

说明：

- `funding_users.funding_version` 是 user-level version。
- `funding_accounts.balance_version` 是 per-asset version。
- `funding_mutations` 是 holder 方法的持久幂等表。同一 `(transfer_id, op_type)` 重试返回首次结果；参数不同返回 idempotency conflict。
- `frozen` 字段保留以维持账户模型完整性，但 MVP funding holder 只修改 `available`。

### 4. Funding holder 写入协议

`TransferOut / TransferIn / CompensateTransferOut` 对 funding wallet 的修改必须在一个 MySQL transaction 内完成：

```
BEGIN

1. 查询 funding_mutations WHERE transfer_id=? AND op_type=?
   - 命中且参数一致：返回该行保存的首次结果
   - 命中但参数不一致：返回 idempotency conflict

2. SELECT funding_users WHERE user_id=? FOR UPDATE
   - 不存在则 INSERT funding_users(..., funding_version=0) 后再锁定

3. SELECT funding_accounts WHERE user_id=? AND asset=? FOR UPDATE
   - 不存在则 INSERT zero account 后再锁定

4. 校验业务规则
   - TransferOut: available >= amount
   - TransferIn / Compensate: amount > 0

5. 如果 CONFIRMED:
   - 更新 funding_accounts.available/frozen/balance_version
   - 更新 funding_users.funding_version

6. INSERT funding_mutations
   - CONFIRMED 和 REJECTED 都记录
   - 保存 available_after / frozen_after / version，供幂等重试直接返回

COMMIT
```

规则：

- `TransferOut` 的余额不足是幂等终态。同一 `transfer_id` 重试仍返回第一次的 REJECTED；若用户补足余额后想再转，需要新 `transfer_id`。
- `TransferIn` 和 `CompensateTransferOut` 对同一 `(transfer_id, op_type)` 只入账一次。
- DB commit 成功后，funding 余额立即生效。
- DB commit 失败则余额不变，也不会留下部分 mutation。

### 5. Saga 状态

`transfer_ledger` 仍然按 ADR-0057 管理跨账户划转状态。

funding 侧余额更新和 saga 状态推进的关系：

- asset-service 作为 orchestrator 创建 / 更新 `transfer_ledger`；
- 当 funding 是 from/to holder 时，funding holder 的余额变更写入 `funding_accounts` / `funding_mutations`；
- Counter 仍然管理 trading account，并通过自己的协议持久化交易账户状态；
- 两个 holder 的原子性由 saga 补偿协议保证，不尝试做跨 MySQL + Counter 的 2PC。

### 6. Query path

`AssetService.QueryFundingBalance`：

- 单 asset：`SELECT ... FROM funding_accounts WHERE user_id=? AND asset=?`
- 全资产：`SELECT ... FROM funding_accounts WHERE user_id=? ORDER BY asset`

`AssetService.QueryTransfer`：

- 读 `transfer_ledger`。

Funding 账户流水：

- MVP 可从 `funding_mutations` 查询；
- 如果 History 需要统一入口，可以由 History 服务调用 Asset Service，或读取 `opentrade_asset` 只读副本；
- 不需要为了流水查询引入 Kafka。

### 7. Startup / recovery

asset-service 启动顺序：

```
1. 连接 opentrade_asset MySQL
2. 确认 schema / migration version
3. Recover pending saga from transfer_ledger
4. 打开 gRPC listener
```

不从 Kafka replay funding balance，也不要求 asset-journal retention。

## 备选方案 (Alternatives Considered)

### A. 继续使用 asset-journal replay 恢复 funding

- 优点：接近 Counter 的 journal 范式。
- 缺点：依赖 Kafka retention；恢复时间随历史增长；funding 低频账户为统一范式承担不必要复杂度。
- 结论：拒。

### B. funding wallet snapshot + bounded journal recovery

- 优点：可以降低 replay 时间，仍保持 journal 恢复模型。
- 缺点：要设计 snapshot schema、offset capture、retention invariant；对资金账户过度工程。
- 结论：拒。

### C. transactional outbox 发布 asset-journal

- 优点：未来统一资产视图、异步审计、跨服务订阅会更方便。
- 缺点：当前没有明确消费者必须依赖该事件流；引入 outbox 表、publisher、重试、backlog 告警和下游去重，复杂度超过收益。
- 结论：本 ADR 拒绝。未来若要做统一资产视图，再单独起 ADR。

### D. trade-dump funding_accounts projection 反向恢复 Asset Service

- 优点：项目里已有一个 funding projection 概念。
- 缺点：投影是下游读模型，不是资金权威；会让 Asset Service 反向依赖 trade-dump；仍绕不开幂等和恢复边界。
- 结论：拒。

### E. 2PC / XA 覆盖 Asset MySQL + Counter

- 优点：理论上强一致。
- 缺点：阻塞、复杂、故障处理困难；Counter 当前不是一个 XA resource；与 saga 设计冲突。
- 结论：拒。跨账户划转继续使用 saga + 补偿。

## 理由 (Rationale)

- **职责清楚**：Asset 管 funding account；Counter 管 trading account；二者通过 AssetHolder RPC 和 saga 协作。
- **恢复简单**：Asset Service 重启只依赖自己的 MySQL。
- **幂等持久**：transfer_id 幂等从内存 ring 升级为 DB unique key。
- **运维友好**：资金账户天然适合 MySQL 备份、binlog、审计查询、人工修复。
- **避免无用事件流**：没有统一资产视图之前，不为“可能有用”维护 Kafka/outbox。

## 影响 (Consequences)

### 正面

- Asset Service 不再需要 Kafka client。
- funding 余额重启后天然存在，不依赖 replay。
- Kafka retention 不影响 funding wallet 可恢复性。
- 代码和运维面都比 snapshot/journal/outbox 简单。
- 手工审计可以直接看 `funding_accounts` + `funding_mutations` + `transfer_ledger`。

### 负面 / 代价

- funding holder 写路径从内存操作变成 MySQL transaction，单次延迟更高。
- 失去 funding balance 的 Kafka 事件流；未来统一资产视图需要另做 CDC/outbox。
- History 若要展示 funding 流水，需要读 Asset Service / asset DB，而不是只读 trade-dump DB。

### 中性

- Counter / Match / trade-dump 的现有 Kafka 架构不变。
- `transfer_ledger` 仍是 saga 状态权威。
- `funding_mutations` 是否长期保留属于审计/合规策略，不影响余额正确性。

## 实施约束 (Implementation Notes)

### M1: schema + store

- 新增 `pkg/assetstore` 或 `asset/internal/store`。
- 新增 schema：
  - `funding_users`
  - `funding_accounts`
  - `funding_mutations`
- Store API：
  - `ApplyFundingTransferOut`
  - `ApplyFundingTransferIn`
  - `ApplyFundingCompensate`
  - `QueryFundingBalance`
  - `ListFundingMutations`

### M2: funding holder 切 DB

- `asset/internal/service` 不再以 in-memory `engine.State` 作为 funding 权威。
- `TransferOut/In/Compensate` 调 store transaction。
- 重试命中 `funding_mutations` 返回首次结果。
- 同 transfer_id 不同参数返回明确 idempotency conflict。

### M3: 移除 Kafka / journal 恢复路径

- 删除 asset-service 启动时 replay funding state 的生产路径。
- 删除 asset-journal publisher 对 funding holder 的硬依赖。
- `asset/cmd/asset` 不再要求 Kafka brokers 才能启动 funding holder。
- 已有 `asset-journal` proto / trade-dump projection 若无其他用途，可后续清理；本 ADR 不要求保留。

### M4: 查询与历史

- `QueryFundingBalance` 读 MySQL。
- `QueryTransfer` 继续读 `transfer_ledger`。
- 如需要 funding 流水 API，读 `funding_mutations`。
- History 是否聚合 asset DB，另行决定；不通过 Kafka 绕路。

### 测试要点

- TransferOut 正常扣款：account row 和 mutation row 同事务出现。
- TransferOut 余额不足：mutation REJECTED 持久化，余额不变。
- TransferIn / Compensate 重试：同 `(transfer_id, op_type)` 只入账一次。
- 同 transfer_id 不同参数：返回 idempotency conflict。
- transaction 中途失败：余额和 mutation 都不落部分状态。
- asset-service 重启后 QueryFundingBalance 从 DB 返回正确余额。
- saga 在 DEBITED / COMPENSATING 状态重启后，仍从 `transfer_ledger` 恢复推进。

## 开放问题 (Open Questions)

- funding account 是否需要 frozen 字段？当前保留以对齐账户模型，但 MVP funding holder 只修改 available。
- `funding_mutations` 是否长期全量保留，还是按审计周期归档？资金审计倾向长期保留，具体保留期由运维/合规定。
- 未来统一资产视图如果需要 funding 事件流，是走 MySQL CDC、transactional outbox，还是 History 直接读 asset DB？届时单独起 ADR。

## 参考 (References)

- [ADR-0011](./0011-counter-transfer-interface.md) — Counter trading account 的 Transfer 基础
- [ADR-0046](./0046-history-service.md) — History 只读查询聚合
- [ADR-0057](./0057-asset-service-and-transfer-saga.md) — asset-service 与 transfer saga
