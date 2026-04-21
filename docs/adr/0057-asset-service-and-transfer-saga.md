# ADR-0057: 资产服务（funding wallet）+ 跨子系统划转 Saga 协议

- 状态: Accepted（M1-M4 落地于 commits 20fd0c3 / c19af21 / faaab7a / b30f7d3 / ab343ef；M5 trade-dump 投影 + M6 对账/监控未完成）
- 日期: 2026-04-20
- 决策者: xargin, Claude
- 相关 ADR: 0001（Kafka 作为事件权威源）、0004（counter-journal）、0005（Kafka 事务双写）、0011（Counter 统一 Transfer 接口）、0015（客户端幂等）、0018（Counter UserSequencer）、0028（trade-dump 投影）、0046（History 只读网关）、0048（双层 version）、0049（snapshot protobuf）

## 术语 (Glossary)

| 本 ADR 字段 | 含义 | 业界对标（仅供读者映射） |
|---|---|---|
| `funding wallet` | 资金账户。asset-service **内部的账户模型**（不是独立服务）；用户主动划转的"中转枢纽"，同时参与所有跨 biz_line 的 saga | Binance "Funding Wallet"、OKX "资金账户"、Bybit "Funding Account" |
| `spot account` | 现货账户。Counter 内持有的现货资产（本 ADR 前 Counter 的唯一账户类型） | Binance "Spot Wallet" |
| `biz_line` | 业务线标识字符串。当前取值：`funding` / `spot`，预留 `futures` / `earn` / `margin` | — |
| `AssetHolder` | gRPC 服务接口。每个持有资金的业务服务都实现这个接口，暴露 `TransferOut / TransferIn / CompensateTransferOut` 三个方法 | — |
| `transfer_id` | 客户端发起的全局幂等键（UUID 或客户端单调序）。同一 transfer_id 在 saga 全链路（asset + from + to）都作为幂等去重锚点 | Binance `tranId`、OKX `transId` |
| `transfer_ledger` | asset-service 的 saga 状态表。MySQL 里每条划转一行，记录状态机推进 | — |
| `asset-journal` | 新 Kafka topic。asset-service 的 WAL，记录 funding wallet 的余额变动事件，对齐 counter-journal 的语义 | — |
| `Saga` | 补偿式分布式事务模式。from 扣款成功后 to 入账失败则回滚 from | — |

## 背景 (Context)

### 现状

1. **账户只有一个 biz_line**：Counter 持有的是"现货账户"，且是项目里唯一的账户源（ADR-0011 的 `Transfer` 接口就一个余额表）
2. **充值/提现入口扁平**：ADR-0011 的 `Counter.Transfer(DEPOSIT)` 直接把外部资金打入现货账户，没有"资金账户"这一中转层
3. **未来必然多账户**：合约（futures）、理财（earn）、杠杆（margin）、法币（fiat wallet）是 CEX 产品的常规扩展。每种都需要自己的账户隔离 + 和用户资金的互转
4. **缺跨账户划转协议**：现在没有"从 A 账户转到 B 账户"的 first-class 操作。如果走 `Counter.Transfer(WITHDRAW) + Futures.Transfer(DEPOSIT)` 两次 RPC，中间崩溃会资金悬空（from 扣了 to 没加到），且没有补偿机制

### 用户场景驱动

- 用户充值 USDT → 先进 **funding wallet**
- 用户要交易现货 → 自己从 funding 划转到 spot
- 用户要玩合约（未来）→ 从 funding 划转到 futures
- 各业务线账户不互通，funding 是枢纽

这和 Binance / OKX / Bybit 面向终端用户的"资金账户 + 业务钱包"模型一致（具体实现细节我不清楚，只取其产品形态）。

### 为什么现在做

1. ADR-0011 的 `Counter.Transfer` 混用了"系统层充提" + "账户内操作"两种语义，随着业务线增加会越来越乱，现在拆是最便宜的时机（项目未上线，无兼容负担）
2. saga 协议一旦定下来，未来每加一个业务线只需实现 `AssetHolder` 接口，不用每次都设计一套划转方案
3. 为后续"账户间手续费折算"、"跨账户风控限额"、"统一资产视图"留架构位

## 决策 (Decision)

### 1. 新增服务 `asset/`

asset-service 是**单一服务**，同时承担两个角色（同一进程，共享账户状态）：

1. **funding wallet 的 AssetHolder**：asset-service 自己维护一个 `biz_line=funding` 的账户簿（每个用户一个 `(user_id, asset) → {available, frozen}` 记录），对外暴露 AssetHolder 三方法，和 Counter（`biz_line=spot`）地位对等
2. **Saga Orchestrator**：BFF 把所有跨 biz_line 划转请求打到 asset-service 的 `Transfer` RPC，它调两端（from / to）的 AssetHolder 接口 + 维护 saga 状态机 + 失败补偿

两个角色合并在一个进程里，是因为 funding wallet 几乎出现在每一条 saga 里（另一端是 spot / futures / earn / ...），合并可以省掉"orchestrator ↔ funding holder"之间一次额外的 gRPC 跳。对外的语义上 asset-service 就是"管 funding 账户 + 编排划转"的一个服务，funding wallet 不是独立部署单元。

部署模式：MVP 阶段**单实例**（funding QPS 量级远低于撮合路径），cold standby HA（ADR-0031 模式），Kafka 事务写 `asset-journal`（ADR-0005 模式）。未来若 funding QPS 上去或需要故障隔离再起 ADR 讨论分片策略。

### 2. 统一 `AssetHolder` gRPC 接口

每个持有资金的业务服务都实现这个接口（asset-service 自己也实现，因为 funding wallet 也是 saga 成员）：

```proto
// api/rpc/assetholder/assetholder.proto
service AssetHolder {
  // 从本 holder 扣出 amount，转给 peer_biz。
  // 幂等：同 transfer_id 重试返回首次结果。
  rpc TransferOut(TransferOutRequest) returns (TransferOutResponse);

  // 把 amount 存入本 holder（由对手方已扣出）。
  // 幂等：同 transfer_id 重试返回首次结果。
  rpc TransferIn(TransferInRequest) returns (TransferInResponse);

  // 回滚一次已成功的 TransferOut（补偿）。
  // 幂等：同 transfer_id 可多次调用。
  rpc CompensateTransferOut(CompensateTransferOutRequest) returns (CompensateTransferOutResponse);
}

message TransferOutRequest {
  string user_id     = 1;
  string transfer_id = 2;   // saga 幂等键
  string asset       = 3;
  string amount      = 4;   // decimal string
  string peer_biz    = 5;   // "funding" / "spot" / ...（审计用）
  string memo        = 6;
}

enum TransferOutStatus {
  TRANSFER_OUT_STATUS_UNSPECIFIED = 0;
  TRANSFER_OUT_STATUS_CONFIRMED   = 1;  // 扣款成功
  TRANSFER_OUT_STATUS_REJECTED    = 2;  // 业务拒绝（余额不足、风控等）— 终态
  TRANSFER_OUT_STATUS_DUPLICATED  = 3;  // 幂等命中，返回首次结果
}

message TransferOutResponse {
  TransferOutStatus status          = 1;
  string            reject_reason   = 2;
  string            available_after = 3;
  string            frozen_after    = 4;
}

// TransferIn / CompensateTransferOut 结构同构（省略）
```

**关键设计**：
- 每个 holder 对 saga 的理解**只是"扣钱 / 加钱 / 回滚"三个原子动作**，不需要知道 saga 全貌
- `peer_biz` 只用于审计/日志，不影响业务逻辑
- 所有三个方法**必须幂等**：holder 内部持久化 transfer_id → 结果映射（可以复用现有 Counter 的客户端 dedup 表 ADR-0015）

### 3. Counter 实现 AssetHolder（Spot 作为 holder）

Counter 在现有 gRPC server 上追加三个方法：

- `TransferOut`：内部转为 "从 available 扣 amount"，写 `counter-journal.TransferEvent(type=WITHDRAW, biz_ref_id=<transfer_id>)`
- `TransferIn`：内部转为 "给 available 加 amount"，写 `counter-journal.TransferEvent(type=DEPOSIT, biz_ref_id=<transfer_id>)`
- `CompensateTransferOut`：等价于 `TransferIn` 但带 saga 补偿语义标签（审计区分）

**ADR-0011 的 `Counter.Transfer` 外部职责收窄，但接口暂不删除**：
- `api/rpc/counter/counter.proto` 里的 `Transfer` RPC 先保留，继续承接系统级 / 运维类资金操作；用户发起的跨 biz_line 划转统一改走 asset-service
- Counter **内部**仍有 `svc.Transfer(user_id, asset, amount, ...)` 函数（处理余额加减 + 写 counter-journal + 幂等），作为 AssetHolder 三方法的共享实现基底
- `Counter.Transfer` 不再作为 BFF 用户划转入口；未来如果系统级充提也统一收口到 asset-service，再起新 ADR 删除外部 RPC 并把 ADR-0011 标为 `Superseded`

ADR-0011 在本 ADR 落地后仍保持 `Accepted`，但适用范围收窄为 Counter 内部共享实现 + 少量系统级外部操作。

### 4. asset-service 的 Saga 状态机

```
┌──────┐
│ INIT │ — asset-service 收到用户请求，写 transfer_ledger
└──┬───┘
   │ 调 from.TransferOut
   ├─ REJECTED → FAILED（终态；from 不动，用户看到拒绝）
   │
   ▼
┌─────────┐
│ DEBITED │ — from 扣款成功，资金暂时"悬空"
└──┬──────┘
   │ 调 to.TransferIn
   ├─ REJECTED → COMPENSATING → 调 from.CompensateTransferOut
   │                              ├─ CONFIRMED → COMPENSATED（终态）
   │                              └─ 失败 → 持续重试（带退避）
   ▼
┌───────────┐
│ COMPLETED │ — 终态
└───────────┘
```

**terminal states**：`FAILED` / `COMPLETED` / `COMPENSATED` / `COMPENSATE_STUCK`（补偿重试超阈值，必须人工介入，alarm）。

### 5. `transfer_ledger` 表（saga state 持久化）

asset-service 独享一张 MySQL 表，位于新库 `opentrade_asset`（不用 trade-dump 那套投影库，避免污染）：

```sql
CREATE TABLE transfer_ledger (
    transfer_id VARCHAR(64)  PRIMARY KEY,
    user_id     VARCHAR(64)  NOT NULL,
    from_biz    VARCHAR(32)  NOT NULL,
    to_biz      VARCHAR(32)  NOT NULL,
    asset       VARCHAR(16)  NOT NULL,
    amount      VARCHAR(64)  NOT NULL,          -- decimal string
    state       VARCHAR(32)  NOT NULL,          -- INIT/DEBITED/COMPLETED/...
    reject_reason VARCHAR(256),
    created_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at  TIMESTAMP(6) NOT NULL
                DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6),
    INDEX idx_user_created (user_id, created_at),
    INDEX idx_state_updated (state, updated_at)
) ENGINE=InnoDB;
```

**恢复**：asset-service 启动时 `SELECT * FROM transfer_ledger WHERE state IN ('INIT','DEBITED','COMPENSATING')`，把未终结的 saga 加载到内存 driver 继续推进。

### 6. asset-journal Kafka topic

asset-service 的 WAL，对齐 counter-journal 的语义（ADR-0004）：

```proto
// api/event/asset_journal.proto
message AssetJournalEvent {
  EventMeta meta            = 1;
  uint64    asset_seq_id    = 2;   // ADR-0051 的 typed producer seq
  uint64    funding_version = 3;   // funding wallet 的 user 级 version（ADR-0048 双层 version）

  oneof payload {
    FundingDepositEvent       deposit     = 10;  // 外部充值到 funding
    FundingWithdrawEvent      withdraw    = 11;  // funding 提现到外部
    FundingTransferOutEvent   xfer_out    = 12;  // funding 扣出（saga DEBITED 阶段）
    FundingTransferInEvent    xfer_in     = 13;  // funding 加入（saga 入账）
    FundingCompensateEvent    compensate  = 14;  // 补偿入账
    TransferStateEvent        transfer_state = 15; // saga 状态推进快照，供 trade-dump 投影 transfers
  }
}
```

`transfer_state` 不影响 funding balance，只承载 orchestrator 对 `transfer_ledger` 的状态推进快照（至少包含 `transfer_id / user_id / from_biz / to_biz / asset / amount / state / reject_reason`）。这样 trade-dump 只消费 Kafka topic 就能重建 `transfers` 投影，不需要直连 asset-service 的 MySQL。

**Kafka 事务**：asset-service 写 `transfer_ledger` 状态变更时**不**跨 MySQL + Kafka 事务（复杂度太高）；改用：
1. 先改 `transfer_ledger`（MySQL，本地事务）
2. 再写 `asset-journal`（Kafka 幂等 producer，asset_seq_id 单调）
3. 崩在 2 之前：saga recovery 扫 ledger 重试（事件可能重发，消费方靠 asset_seq_id 去重）

对标 counter-journal 的"事务双写 Kafka"策略（ADR-0005）——这里因为 MySQL 是 saga 权威源、Kafka 是下游投影，不是双写两个 Kafka topic，所以用更简单的"DB-first + Kafka retry"。

### 7. trade-dump 扩展

trade-dump 已经投影 counter-journal → MySQL（ADR-0028）。新增：

- 消费 `asset-journal` → MySQL 表 `funding_accounts(user_id, asset, available, frozen, funding_version)` 和 `funding_account_logs`
- 消费 `asset-journal.transfer_state` → MySQL `transfers` 表供 History 查询（trade-dump 不直连 asset-service 的 MySQL）

### 8. BFF 路由

新增 `POST /v1/transfer`（用户发起跨账户划转）：
```
{ "transfer_id": "uuid", "from_biz": "funding", "to_biz": "spot", "asset": "USDT", "amount": "100" }
```
BFF 转发到 `asset-service.Transfer`。

`POST /v1/admin/transfer`（管理员/系统级）和**链上充值 / 提现回调**的承接方本 ADR 不覆盖，见"开放问题"。

查询侧：
- `GET /v1/transfers` → History 服务（读 MySQL `transfers` 投影）
- `GET /v1/funding-balance` → asset-service.QueryFundingBalance（或 Redis 投影，见本 ADR 未来工作）

## 备选方案 (Alternatives Considered)

### A. Seata-go 做分布式事务

引入阿里 Seata 分布式事务协调器，用 AT / TCC / SAGA 模式之一管划转。

- **优点**：开源、成熟、有状态机 DSL、有 Web 控制台
- **缺点 1**：AT 模式靠 RDBMS undo log 自动回滚；Counter 的账户权威源是**内存 + Kafka journal**，不是 MySQL → AT 完全不适用
- **缺点 2**：TCC 模式本质是手写 try/confirm/cancel 三阶段，等同于本 ADR 的 saga，但多一个 Seata Server 中心化协调者（新的 HA 组件 + 新的运维面）
- **缺点 3**：SAGA 模式提供状态机 DSL，但我们的状态机只有 5-6 个节点，Go 原生代码比外部 DSL 更可读可测
- **缺点 4**：和 ADR-0001 "Kafka 作真值源" 架构基因冲突 —— Seata 的 TC（Transaction Coordinator）想做的事 Kafka journal + transfer_ledger 已经在做
- **结论**：拒。现有基础设施（Kafka + Counter 幂等 Transfer + MySQL）组合出的 saga 比引入 Seata 简单且自洽

### B. 2PC（两阶段提交）

asset-service 作为协调者，第一阶段 prepare 两端都冻结，第二阶段 commit/rollback。

- **优点**：强一致
- **缺点**：阻塞型协议，协调者宕机会卡住参与方；CAP 里牺牲 A
- **结论**：拒。交易所划转可以最终一致（毫秒级），不值得付 2PC 的可用性代价

### C. Counter.Transfer 直接扩展（不新开服务）

在 Counter 里加 `biz_line` 字段，让一个 Counter 同时管 spot + funding + futures，不新开服务。

- **优点**：零新服务
- **缺点 1**：Counter 已经是撮合写路径上的关键组件，往里加业务会放大故障面
- **缺点 2**：Counter 按 user_id 分 10 shard（ADR-0010），funding wallet 完全可以是另一种分片策略（甚至单实例起步）
- **缺点 3**：未来 futures / earn 是独立服务，它们不可能住进 Counter
- **结论**：拒。asset-service 作为独立组件可以不背 Counter 的 HA / sharding / 撮合延迟 SLA

### D. BFF 编排 saga

让 BFF 直接调 from.TransferOut → to.TransferIn，失败 BFF 自己补偿。

- **优点**：不新开服务
- **缺点**：BFF 是无状态网关，承担 saga 状态机违反其定位；BFF 扩容后多实例会重复驱动同一 saga；恢复机制缺失
- **结论**：拒。saga orchestrator 必须是有状态服务

### E. 不做 funding wallet，充值直进 spot

保持 ADR-0011 现状，用户充值 USDT 直接落现货账户。

- **优点**：零架构改动
- **缺点**：业务上走不通 —— 上合约/理财必须有"中转账户"概念，否则用户资金布局和子账户风控完全混乱
- **结论**：拒。这是产品必需，不是架构偏好

## 理由 (Rationale)

1. **分离职责**：Counter 专注撮合路径的账户状态（freeze / unfreeze / settle），asset-service 专注跨账户资金流转；两者都是 AssetHolder 的实现者，地位对等
2. **接口统一压制扩展成本**：加一个业务线（futures / earn / margin）= 实现 AssetHolder 接口 + 在 asset-service 的 biz 注册表加一行路由，**不需要改 saga 逻辑本身**
3. **Saga 语义匹配业务**：跨账户划转失败率极低（只有余额不足这种业务拒绝，和极少的宕机场景），补偿路径简单，不值得上 2PC/Seata
4. **复用现有幂等/事件基础设施**：transfer_id + Counter dedup + Kafka journal 三件套已经在线，不引入新一致性原语
5. **MySQL 做 saga ledger 最顺手**：状态机驱动是"扫表 + 更新"典型关系型负载；放 Kafka journal 里反而要自建状态索引

## 影响 (Consequences)

### 正面

- 为 futures / earn / margin / 法币钱包等业务线预留了干净的接入协议
- 用户资产从"一个现货账户"升级为"funding 中转 + 多业务子账户"，对齐主流 CEX 产品形态
- Counter 卸下了"系统级 Transfer + 用户划转"的职责混用，ADR-0011 的语义变清晰
- 为后续"统一资产视图"（聚合各 holder 余额 + USD 估值）留了数据源（订阅所有 journal topic）
- Saga 协议作为 first-class concept 沉淀，避免每次加业务都重新发明

### 负面 / 代价

- **新服务运维面**：asset-service 自己的 HA、snapshot、监控、告警（虽然可复用 pkg/ 里 counter 的大部分基建）
- **新 MySQL 库 / 表**：`opentrade_asset.transfer_ledger`，一份新备份策略 + DBA 工具链接入
- **BFF 客户端调用点迁移**：用户发起的划转从打 `Counter.Transfer` 切到 `asset-service.Transfer`，request schema 也换（新增 `from_biz` / `to_biz`，语义不同）。项目未上线（参考 feedback_breaking_changes），直接改干净，不做兼容层
- **充值 / 提现 / 管理员调账流程悬空**：本 ADR 落地后 `Counter.Transfer` 对外 RPC 消失，但链上充值回调还没有新家 —— 实施期间要么临时禁用链上回调入口，要么把这部分 admin / link-bridge ADR 和本 ADR 配套一起做
- **Counter 需要实现 AssetHolder**：工作量约 200-400 LOC（三个新 RPC + 幂等层，逻辑可复用现有 Transfer）
- **迁移工作**：把 BFF 的 `POST /v1/transfer` 从打 Counter 改打 asset-service；History 扩展 `GET /v1/transfers`；trade-dump 订阅 asset-journal
- **端到端划转延迟**：用户看到的划转成功从"一次 Counter RPC（<10ms）"变成"asset → from → to → asset 四跳 + ledger 写（~50ms）"，对产品可接受但需 SLA 文档化

### 中性

- asset-service 单实例起步（MVP），分片留到后面；funding wallet 的 QPS 量级远低于 Counter 下单路径
- `CompensateTransferOut` 理论上永远不应触发（to.TransferIn 几乎只在服务宕机时失败）。真触发 = 监控告警级事件，需和对账 job 联动
- ADR-0011 `Counter.Transfer` 保留但语义收窄；未来若彻底搬干净（系统级充提也走 asset-service），可再起 ADR 作废

## 实施约束 (Implementation Notes)

### 分阶段落地

**M1（2 天）** — Proto + pkg 骨架
- `api/rpc/assetholder/assetholder.proto` + `api/rpc/asset/asset.proto` + `api/event/asset_journal.proto`
- `make proto` 生成
- `pkg/transferledger/` MySQL DAO（CRUD + 状态推进）
- Counter 单元测试替身（mock AssetHolder）

**M2（3-4 天）** — Counter 实现 AssetHolder
- `counter/internal/server/assetholder.go`：三个新 RPC，内部复用现有 `svc.Transfer` 逻辑
- 幂等表复用 ADR-0015 的 dedup（以 transfer_id 为键）
- counter-journal 的 TransferEvent 增加 `saga_transfer_id` 字段（proto 可加新字段保持兼容）
- 集成测试：TransferOut → counter-journal 写入正确 → 重试返回 DUPLICATED

**M3（5-7 天）** — asset-service MVP
- `asset/cmd/asset/main.go` 参考 `counter/cmd/counter/main.go` 骨架
- funding wallet 内存账户 + asset-journal Kafka producer（事务 producer, ADR-0032）
- Saga driver：INIT → DEBITED → COMPLETED / COMPENSATED
- 实现 AssetHolder 接口（funding wallet 作为 holder 之一）
- 启动时从 transfer_ledger 恢复未终结 saga
- Snapshot（ADR-0049 protobuf）+ cold standby HA（ADR-0031）

**M4（2 天）** — BFF + History 对接
- BFF `POST /v1/transfer` 切到 asset-service
- `api/rpc/counter/counter.proto` 删除 `Transfer` RPC 及相关消息；BFF 原先打 `Counter.Transfer` 的调用点全部移除或改打 asset-service
- 链上充值 / 提现回调 / 管理员调账入口**本阶段不动**，等后续 ADR 定承接方（见"开放问题"）
- History 新增 `GET /v1/transfers`（读 MySQL `transfers` 投影）

**M5（2 天）** — trade-dump 扩展
- 消费 asset-journal → `funding_accounts` / `funding_account_logs` / `transfers` 投影

**M6（1-2 天）** — 对账 + 监控
- 对账 job：asset.transfer_ledger.COMPLETED ↔ counter-journal.TransferEvent(saga_transfer_id) 配对
- Prometheus metric：`saga_state_count{state=}`、`saga_compensate_total`、`saga_stuck_total`
- Staleness 告警

### Flag

asset-service：
- `--grpc-addr`
- `--kafka-brokers` / `--journal-topic=asset-journal`
- `--mysql-dsn` / `--mysql-db=opentrade_asset`
- `--etcd`（HA election 用）
- `--compensate-max-retries=10` / `--compensate-backoff=1s,2s,5s,...`

### 测试要点

- **单测**：
  - Saga driver 所有状态转移（INIT→DEBITED→COMPLETED；INIT→FAILED；DEBITED→COMPENSATING→COMPENSATED；COMPENSATING 超限→COMPENSATE_STUCK）
  - Counter AssetHolder 三个方法的幂等（同 transfer_id 调 N 次结果一致）
- **集成**：
  - 正常路径：BFF → asset → Counter TransferOut → Counter TransferIn → COMPLETED
  - from 拒绝（余额不足）：INIT → FAILED
  - to 失败 + 补偿成功：INIT → DEBITED → COMPENSATING → COMPENSATED
  - asset-service 在 DEBITED 阶段宕机：重启后从 ledger 恢复继续推进
  - Counter 幂等：TransferOut 重复发（模拟 asset-service 重试）余额只扣一次
- **混沌**：
  - Kafka 断连时 asset-service 行为（应该 block 写，不能只写 ledger 不写 journal）
  - Counter 侧单 shard 挂掉，asset-service 的 saga 超时重试策略

### 失败场景

| 情况 | 结果 |
|---|---|
| from.TransferOut 余额不足 | saga 终态 FAILED，用户看到拒绝，资金零变动 |
| to.TransferIn 失败（业务拒绝，如风控冻结了 to） | 进入 COMPENSATING；from.CompensateTransferOut 把资金退回 |
| CompensateTransferOut 长时间失败 | 超阈值进入 COMPENSATE_STUCK，告警 + 人工介入；客户资金停留在 from（未真正丢失） |
| asset-service 在 ledger 写入后 + Kafka produce 前宕机 | 重启时 saga driver 重读 ledger 继续调 from/to，调用幂等保证无副作用 |
| Counter 在 TransferOut 已扣款 + 未 ack asset-service 时宕机 | asset-service 重试 TransferOut，Counter 幂等命中返回首次 CONFIRMED |
| transfer_id 冲突（极端边界） | MySQL PK 冲突 → asset-service 返回已有 saga 结果（真正的幂等） |
| 用户同时发起两个 transfer（from 余额仅够一个） | 两个 saga 独立推进；后到的 TransferOut 在 Counter 内被拒（余额不足），saga FAILED |

### 和 ADR-0011 的关系

- ADR-0011 的 `Counter.Transfer` **对外 gRPC 接口作废**（proto 中物理删除）；ADR-0011 状态本 ADR 落地时改为 `Superseded by 0057`
- Counter 进程里的 `svc.Transfer(...)` 函数**保留**，作为 Counter AssetHolder 三方法（TransferOut / TransferIn / CompensateTransferOut）的共享实现基底 —— 余额加减 + counter-journal.TransferEvent + 幂等表这套已经稳定的逻辑不重写
- 原本打 `Counter.Transfer` 的调用方（BFF 用户划转、BFF 链上充值回调、admin 调账）全部要迁移或暂挂：
  - 用户划转：切到 asset-service.Transfer（本 ADR M4 做）
  - 链上充值 / 提现 / admin 调账：本 ADR 不定，见"开放问题"

### 命名选择记录

- `AssetHolder` vs `AccountService` vs `WalletProvider`：选了 AssetHolder 因为最直白——"持有资产的一方"，动词 TransferOut/In 明确方向。避免 Account / Wallet 因为在不同业务线里有歧义
- `funding` 作为业务线关键词直接借用 Binance/OKX/Bybit 的用法（见术语表），用户学习成本为零
- `transfer_id` 和现有 ADR-0011 的 `TransferRequest.transfer_id` 同名同义——saga 下沉时就是透传

## 开放问题 (Open Questions)

以下议题本 ADR 不覆盖，留待后续 ADR 讨论。列在这里是为了显式声明 scope 边界，避免实施时把它们悄悄塞进本 ADR：

- **链上充值 / 提现回调承接方**：当前 BFF 在链上事件回调时直接打 `Counter.Transfer(DEPOSIT / WITHDRAW)`。本 ADR 作废该接口后，回调要打谁（asset-service 新增 `AdminDeposit` / `AdminWithdraw` 接口？独立的 "link-bridge" 服务？）由后续 ADR 定。过渡期建议：暂时关闭链上回调入口，或用 feature flag 挂起，直到新承接方就绪
- **管理员 / 客服调账接口**：admin-gateway 给运维用的"手动加/减余额"也要一个新承接方。大概率和上一条同解
- **asset-service 分片策略**：MVP 单实例。未来若 funding QPS 上去或需要故障隔离，分片策略（user_id hash 对齐 ADR-0010 的 10-shard？还是别的切法？）届时起新 ADR
- **funding 对 spot 的默认向导**：用户充值进 funding 后，需不需要 BFF / 前端默认带一个"一键转到 spot"的向导 —— 纯产品决策，非架构 ADR 范围，列在这里只作提醒

## 参考 (References)

- [ADR-0001](./0001-kafka-as-source-of-truth.md) — Kafka 作真值源（asset-journal 对齐）
- [ADR-0004](./0004-counter-journal-topic.md) — counter-journal 的 schema 范式
- [ADR-0005](./0005-kafka-transactions-for-dual-writes.md) — 事务 producer（asset-service 复用模式）
- [ADR-0011](./0011-counter-transfer-interface.md) — Counter 的现有 Transfer 接口（本 ADR 保留其内部用途）
- [ADR-0015](./0015-idempotency-at-counter.md) — 客户端幂等键机制（transfer_id 复用）
- [ADR-0028](./0028-trade-dump-journal-projection.md) — trade-dump 投影模式（asset-journal 投影参考）
- [ADR-0031](./0031-ha-cold-standby-rollout.md) — HA cold standby（asset-service 复用）
- [ADR-0032](./0032-match-transactional-producer.md) — 事务 producer fencing（asset-service 复用）
- [ADR-0046](./0046-history-service.md) — History 网关（新增 `/v1/transfers` 的承载方）
- [ADR-0048](./0048-snapshot-offset-atomicity.md) — 双层 version（funding_version 对齐）
- [ADR-0049](./0049-snapshot-protobuf-with-json-debug.md) — snapshot 格式
- 实现位置（动工时涉及）：
  - 新增 `api/rpc/asset/asset.proto`
  - 新增 `api/rpc/assetholder/assetholder.proto`
  - 新增 `api/event/asset_journal.proto`
  - 新增 `pkg/transferledger/`
  - 新增 `asset/`
  - `counter/internal/server/assetholder.go` — Counter 实现 AssetHolder
  - `bff/internal/rest/transfer.go` — 路由切换
  - `history/` — 新增 `/v1/transfers` 端点
  - `trade-dump/` — 新增 asset-journal 消费 + projection
