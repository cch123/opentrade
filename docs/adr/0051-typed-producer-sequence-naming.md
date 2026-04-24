# ADR-0051: 事件单调序按 producer 命名（counter_seq_id / match_seq_id / quote_seq_id / trigger_seq_id）

- 状态: Accepted
- 日期: 2026-04-19
- 决策者: xargin, Claude
- 相关 ADR: 0004, 0008, 0015, 0018, 0019, 0028, 0047, 0048

## 背景 (Context)

系统里有 4 种独立产生的"单调序列"：

| Producer | Scope | 用途 |
|---|---|---|
| counter `UserSequencer` | per-counter-shard | counter-journal 事件的全局排序；trade-dump 写 `accounts` / `account_logs` 的行级 guard；match 拦 counter 重投递的 dedup key |
| match `SymbolWorker` | per-symbol | trade-event 的全局排序；trade-dump 写 `trades` 的 UNIQUE；counter 拦 match 重投递（`Account.matchSeq`）|
| quote `Engine` | quote 进程 global | market-data 事件的全局排序；BFF 重连补齐 snapshot 时的 sync 锚 |
| trigger service | service global | trigger-event 事件的全局排序；trade-dump 写 `triggers` 的乱序保护辅助 |

但代码里所有 4 种都共享同一个 wire 字段 `EventMeta.seq_id`，对应：

- proto: `EventMeta.seq_id`（4 种 topic 的 envelope 共用）
- Go: `evt.Meta.SeqId` / `Output.SeqID` / `TransferResult.SeqID` / `AccountRow.SeqID` / `AccountLogRow.SeqID` / `TradeRow.SymbolSeqID`（最后一个略有区分）
- MySQL: `accounts.seq_id` / `account_logs.seq_id` / `trades.symbol_seq_id`
- snapshot JSON: `shard_seq` / `seq_id` / `seq` 三种命名同时存在

裸看一处 `seq_id` 引用，无法立刻判断它是谁生产、按什么粒度单调、能不能跨 shard / symbol 比较。具体踩到的痛点：

1. **`accounts.seq_id` 让人疑惑**：是 counter 生成的还是 match 生成的？对账时如果误把它和 `trades.symbol_seq_id` 当作同一空间的值比较，结论全错。
2. **counter 拦 match 重发的 guard 容易解释不清**：代码里 `evt.Meta.SeqId` 同时被 counter-journal handler 和 trade-event handler 读，前者拿到的是"自己 shard 的 seq"，后者拿到的是"对端 match 那条 symbol 流的 seq"——同一字段名两套语义。
3. **trade-dump 的 SQL guard 不自洽**：`IF(VALUES(seq_id) >= seq_id, ...)` 里"谁的 seq"是隐含约定，新人看代码必须翻 ADR-0028 才知道。
4. **snapshot 文件字段名混乱**：counter 用 `shard_seq`，match 用 `seq_id`，quote 用 `seq`，三套互不一致。

## 决策 (Decision)

### 1. 删除共享的 `EventMeta.seq_id`

`api/event/common.proto` 的 `EventMeta` 不再承载序号字段。tag 1 用 `reserved 1; reserved "seq_id";` 占位，防止后续误用。

### 2. 每个事件类型按 producer 自带 typed seq 字段

| Proto 消息 | 新字段 | 语义 |
|---|---|---|
| `CounterJournalEvent` | `counter_seq_id` | counter shard 单调，UserSequencer 生成 |
| `OrderEvent` | `counter_seq_id` | 同上（counter 发给 match 的事件，关联 counter shard）|
| `TradeEvent` | `match_seq_id` | match per-symbol 单调，SymbolWorker 生成 |
| `MarketDataEvent` | `quote_seq_id` | quote 进程 global，原子计数 |
| `TriggerUpdate` | `trigger_seq_id` | trigger 服务 global |

字段名编码了 producer 是谁，任何裸引用都自带语义。

### 3. MySQL 列名同步带 producer 前缀

| 表 | 旧列 | 新列 |
|---|---|---|
| `accounts` | `seq_id` | `counter_seq_id` |
| `account_logs` | `seq_id`（PK 一员） | `counter_seq_id`（PK 一员） |
| `trades` | `symbol_seq_id` + `UNIQUE KEY uk_symbol_seq` | `match_seq_id` + `UNIQUE KEY uk_symbol_match_seq` |

`triggers` 表本来就用 `last_update_ms` 做 guard（ADR-0047），不动。

### 4. Go 字段 / snapshot JSON 对齐

- Go 字段：`SeqID` → `CounterSeqID` / `MatchSeqID` / `QuoteSeqID` / `TriggerSeqID`，按上下文前缀。变量 `seqID` → `counterSeq` / `matchSeq` 等
- counter sequencer：`UserSequencer.shardSeq` / `ShardSeq()` / `SetShardSeq()` → `counterSeq` / `CounterSeq()` / `SetCounterSeq()`
- match sequencer：`SymbolWorker.seqID` / `SeqID()` / `SetSeqID()` → `matchSeq` / `MatchSeq()` / `SetMatchSeq()`
- counter snapshot JSON：`shard_seq` → `counter_seq`（Go field `ShardSeq` → `CounterSeq`）
- match snapshot JSON：`seq_id` → `match_seq_id`（Go field `SeqID` → `MatchSeqID`）
- quote snapshot JSON：`seq` → `quote_seq`（Go field `Seq` → `QuoteSeq`）

### 5. 硬切，不维持兼容

- `EventMeta.seq_id` 直接删；不留 deprecated alias
- MySQL 列直接改名（MVP 期数据可重建，无 ALTER 流程）
- snapshot JSON 字段直接改名（也无线上 snapshot 兼容包袱）

## 备选方案 (Alternatives Considered)

### A. 保持现状，靠注释解释 `EventMeta.seq_id` 的多义性

- 文档在 `common.proto:62-64` 已明示"counter-journal: shard-level monotonic / trade-event: per-symbol monotonic"。但读 `evt.Meta.SeqId` 的人未必去翻 envelope 定义
- 拒：注释解决不了"人脑跨 50 个文件追溯字段语义"的问题

### B. 加 Go 层 accessor wrapper（不动 proto）

- Go 加 `evt.CounterSeqID()` / `evt.MatchSeqID()` 包装 `Meta.SeqId`，业务层只调 typed accessor
- 优点：不动 wire
- 缺点：proto / SQL / snapshot 三层依然只有一个字段名，问题没解决；wrapper 容易被绕过
- 拒：解决了 1/4 的层

### C. EventMeta 改名为 `producer_seq_id`，含义中性

- `EventMeta.producer_seq_id`，靠 topic 上下文辨别"是谁的 seq"
- 优点：单字段、单语义（"producer 内单调"）
- 缺点：依然无法在裸字段名上判断 scope；MySQL 列名跟着叫 `producer_seq_id` 更糟，行内信息量为 0
- 拒：换皮不解决根本问题

### D. 选项 B + 选项 C 组合

- `EventMeta.producer_seq_id` + Go 层 typed accessor
- 比 B/C 各自好，但 proto 字段还是单一名字；新人读 proto 仍需补上下文
- 拒：复杂度比"每事件加 typed 字段"高，受益等同

### E. 最终选：每事件 typed 字段（本 ADR 决策）

- 4 种事件各加自己的 typed seq；`EventMeta.seq_id` 整字段废
- proto / Go / MySQL / snapshot 四层名字一致，互为 self-documenting
- 代价：proto schema 改动覆盖面比 A/B/C 大；MVP 期可接受

## 理由 (Rationale)

- **命名是最便宜的文档**：字段名一旦带了 producer 前缀，任何后来者读代码不用查 ADR 也能立刻判断这条 seq 的归属和单调粒度
- **MVP 是动 schema 的最佳窗口**：`01-schema.sql` 注释里明说"MVP-0 skeleton"，没有线上数据，没有跨版本兼容包袱
- **wire 层和持久化层一致**：proto / MySQL 列 / snapshot 三处用同一套命名，避免"读 SQL 是 counter_seq_id 但读 proto 是 seq_id"的隐性映射成本
- **多种 seq 共存的事实需要被显性化**：之前 `seq_id` 一名遮蔽了"系统里实际跑着 4 种独立单调序"的事实；现在每条流自带名字，新人入手就能看清
- **不留 deprecated 字段**：proto3 的 `reserved` 已经能防止误用；不留 alias 让重构一次到位

## 影响 (Consequences)

### 正面

- 任何 `counter_seq_id` / `match_seq_id` / `quote_seq_id` / `trigger_seq_id` 出现的地方，含义零歧义
- trade-dump SQL guard 自描述：`IF(VALUES(counter_seq_id) >= counter_seq_id, ...)` 一眼看清
- counter 拦 match 重发的 guard 名字 `Account.matchSeq` + `evt.MatchSeqId` 完全对齐
- snapshot 字段命名跨服务统一格式（`<producer>_seq` / `<producer>_seq_id`）

### 负面 / 代价

- **wire breaking**：`EventMeta.seq_id` 字段直接删除（proto3 tag 1 reserved），所有 producer/consumer 必须同步部署；MVP 期内部服务可控，影响小
- **MySQL schema breaking**：列重命名 + PK / UNIQUE KEY 重定义，需要冷启重建表；MVP 期 docker-compose 自带 init script，重启数据卷即可
- **snapshot 文件不兼容**：旧 snapshot 的 `shard_seq` / `seq_id` / `seq` 字段名解码会落到零值；硬切需要清掉旧 snapshot 让服务从 Kafka 头消费一遍
- **代码改动覆盖面广**：182 处引用、50 个文件，机械替换为主但量大

### 中性

- 业务语义无任何变化：哪条 seq 守哪个表、按什么粒度单调，全部和重命名前一致
- 与 ADR-0028（trade-dump projection）的 guard 设计一致，只是字段名更明确
- 与 ADR-0019（match per-symbol seq）/ ADR-0018（counter user-seq）的语义同构，只是 wire 字段从 envelope 提到 payload 同级

## 实施约束 (Implementation Notes)

### Proto 改动

- `event/common.proto`：`EventMeta` 删 `seq_id`；用 `reserved 1; reserved "seq_id";`
- `event/counter_journal.proto`：`CounterJournalEvent` 加 `uint64 counter_seq_id = 3`
- `event/order_event.proto`：`OrderEvent` 加 `uint64 counter_seq_id = 2`
- `event/trade_event.proto`：`TradeEvent` 加 `uint64 match_seq_id = 2`
- `event/trigger_event.proto`：`TriggerUpdate` 加 `uint64 trigger_seq_id = 24`
- `event/market_data.proto`：`MarketDataEvent` 加 `uint64 quote_seq_id = 3`
- `snapshot/match.proto`：`MatchSymbolSnapshot.seq_id` → `match_seq_id`
- `snapshot/counter.proto`：`CounterShardSnapshot.shard_seq` → `counter_seq`
- `snapshot/quote.proto`：`QuoteSnapshot.seq` → `quote_seq`
- `rpc/history/history.proto`：`AccountLog.seq_id` → `counter_seq_id`
- `make proto` regenerate

### MySQL 改动

```sql
-- accounts: seq_id → counter_seq_id（不在 PK 内，直接重命名）
-- account_logs: seq_id → counter_seq_id（PK 内，重定义 PK）
ALTER TABLE accounts CHANGE seq_id counter_seq_id BIGINT UNSIGNED NOT NULL;
ALTER TABLE account_logs DROP PRIMARY KEY,
                        CHANGE seq_id counter_seq_id BIGINT UNSIGNED NOT NULL,
                        ADD PRIMARY KEY (shard_id, counter_seq_id, asset);

-- trades: symbol_seq_id → match_seq_id（在 UNIQUE KEY 内，重定义索引）
ALTER TABLE trades DROP INDEX uk_symbol_seq,
                  CHANGE symbol_seq_id match_seq_id BIGINT UNSIGNED NOT NULL,
                  ADD UNIQUE KEY uk_symbol_match_seq (symbol, match_seq_id);
```

MVP 期实操：`make dev-down -v && make dev-up`（清掉 MySQL volume 让 init script 重跑），不写迁移脚本。

### Go 层重命名清单（按模块）

- **counter**: `UserSequencer.{shardSeq,ShardSeq,SetShardSeq}` → `counterSeq/CounterSeq/SetCounterSeq`；`engine.TransferResult.SeqID` → `CounterSeqID`；`journal.{Transfer,PlaceOrder,Cancel,Settlement,OrderStatus}EventInput.SeqID` → `CounterSeqID`；service 层回调参数 `seqID` → `counterSeq`；`snapshot.ShardSnapshot.ShardSeq` → `CounterSeq`（JSON tag `counter_seq`）
- **match**: `SymbolWorker.{seqID,SeqID,SetSeqID}` → `matchSeq/MatchSeq/SetMatchSeq`；`sequencer.Output.SeqID` → `MatchSeq`；`snapshot.SymbolSnapshot.SeqID` → `MatchSeqID`（JSON tag `match_seq_id`）
- **trade-dump**: `AccountRow.SeqID` → `CounterSeqID`；`AccountLogRow.SeqID` → `CounterSeqID`；`TradeRow.SymbolSeqID` → `MatchSeqID`；mysql_journal.go SQL 全替换
- **history**: `AccountLogsCursor.SeqID` → `CounterSeqID`（JSON tag `q` 保持，cursor 是 opaque）；store.go 查询 SQL 替换列名
- **bff**: `accountLogToJSON` 输出键 `seq_id` → `counter_seq_id`
- **quote**: `Engine.stamp` 改写新字段；`snapshot.Snapshot.Seq` → `QuoteSeq`（JSON tag `quote_seq`）
- **trigger**: `journal.convert` 改写新字段

### 不动的部分

- `Account.matchSeq` map（counter 内存里按 (user, symbol) 维护的 dedup guard）名字本来就清晰，保持
- `AccountSnapshot.LastMatchSeq` / proto `CounterAccount.LastMatchSeq` 已含 "match" 字面量，保持
- ADR 历史文档保持原貌，按照"不重写历史决策"惯例只更新 `architecture.md` / `roadmap.md` / `01-schema.sql` 这些 living docs

### 测试

- 全模块 `make build` / `make test` / `make vet` 一次性通过
- 各模块 snapshot round-trip 测试覆盖新 JSON tag
- counter `match_seq_test.go` 覆盖 `evt.MatchSeqId` 入口的 guard 行为
- trade-dump `mysql_test.go` 覆盖新 SQL 列名

## 参考 (References)

- [ADR-0004](./0004-counter-journal-topic.md) — counter-journal 作为 Counter WAL
- [ADR-0008](./0008-sidecar-persistence-trade-dump.md) — trade-dump 写 MySQL，按 seq 做幂等
- [ADR-0018](./0018-counter-sequencer-fifo.md) — Counter UserSequencer 生产 counter shard seq
- [ADR-0019](./0019-match-sequencer-per-symbol-actor.md) — Match SymbolWorker 生产 match per-symbol seq
- [ADR-0028](./0028-trade-dump-journal-projection.md) — trade-dump 投影 + seq guard
- [ADR-0047](./0047-trigger-long-term-history.md) — trigger 投影用 `last_update_ms` 而非 seq guard
- [ADR-0048](./0048-snapshot-offset-atomicity.md) — snapshot 与 Kafka offset 原子绑定
- 实现:
  - [api/event/common.proto](../../api/event/common.proto)
  - [api/event/counter_journal.proto](../../api/event/counter_journal.proto)
  - [api/event/trade_event.proto](../../api/event/trade_event.proto)
  - [deploy/docker/mysql-init/01-schema.sql](../../deploy/docker/mysql-init/01-schema.sql)
  - [trade-dump/internal/writer/mysql_journal.go](../../trade-dump/internal/writer/mysql_journal.go)
