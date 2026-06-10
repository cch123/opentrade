# Bug Tracking

按时间倒序记录项目里发现并修复 / 待修复的 bug。每条：

- **短描述** — 症状
- **Commit** — 修复落地的 git commit（`git show <id>` 可看细节）
- **根因** — 一句话解释
- **状态** — `fixed` / `open` / `workaround`

新 bug 先写进 `## Open` 段；修完后挪到 `## Fixed`（最新在上）、带上 commit id。仅调研还没动手的可以放在 `## Backlog`。

---

## Open

### 2026-05-31 架构 review

- **[P0] Match trade-event 发布失败会丢撮合输出** — `match/internal/journal/producer.go` 的 `Pump.flush` 在 `PublishBatch` 失败后只打日志，然后清空 batch；此时 `match/internal/sequencer.SymbolWorker` 已经修改 orderbook 并推进输入 offset。结果是 Match 状态前进了，但 Counter / Quote / trade-dump 可能永远收不到对应 `trade-event` / cancel result，形成不可恢复的跨服务状态分叉。
  - 状态：open
  - 代码点：`match/internal/journal/producer.go` `flush` / `PublishBatch`；`match/internal/sequencer/worker.go` `handle` offset advance
  - 修复方向：trade-event publish 失败必须 fail-stop 或保留 batch 重试；不能在没有 durable output 的情况下丢弃输出并继续消费 input。后续可补 ARCH guard：state+offset capture / input offset advance 必须受 output durability 约束。

- **[P0] Counter settlement 没有实现文档承诺的 EOS / offset-state 原子性** — `docs/architecture.md` 描述 trade-event 消费应在 Kafka EOS 事务中 `produce SettlementEvent + sendOffsetsToTransaction + commit` 后再更新内存；实际 `counter/internal/service/trade.go` 先 `ApplyPartySettlement` 改内存，再 best-effort `publisher.Publish`，失败只打日志；异步 handler 还会在 fn error 后推进 pendingList。结果是 Counter 内存、counter-journal、trade-dump shadow snapshot 可能分叉。
  - 状态：open
  - 代码点：`counter/internal/service/trade.go` `buildPartyFn` / `buildSelfTradeFn`；`counter/internal/worker/async_handler.go` error callback；`counter/internal/worker/worker.go` `emitCheckpoint`
  - 修复方向：settlement、unfreeze、status 和 trade-event checkpoint 必须形成同一条可恢复的原子链路；至少 publish 失败应阻断 checkpoint 推进并触发 vshard failover，不能吞掉错误后继续推进 watermark。

- **[P0] Asset transfer saga 缺少“远端结果未知”的状态** — `asset/internal/saga/driver.go` 在 `TransferOut` RPC 超时后直接把 saga 标记为 `FAILED`，在 `TransferIn` RPC 超时后直接进入 compensation。但 holder RPC 可能已经在远端提交成功，只是响应丢失；这会造成“源账户已扣但 ledger 失败”或“目标账户已入账且源账户又被补偿”的错账路径。
  - 状态：open
  - 代码点：`asset/internal/saga/driver.go` `doDebit` / `doCredit`
  - 修复方向：引入 `UNKNOWN` / `CONFIRMING` 类中间态，或给 AssetHolder 增加按 `transfer_id` 查询结果的确认接口；transport timeout 不能直接转成业务失败 / compensation。

- **[P1] Trigger reservation 参与资金冻结但不进 counter-journal** — `Reserve` / `ReleaseReservation` 只改 Counter 内存和 snapshot 副表，明确不 emit counter-journal；但 trigger 在保存 pending trigger 前先 Reserve。非 graceful crash、trade-dump shadow snapshot、on-demand recovery 的边界上，可能出现 reservation 丢失、隐藏冻结、或 trigger 仍 pending 但后续无法 consume reservation。
  - 状态：open
  - 代码点：`counter/internal/service/reservation.go` `Reserve` / `ReleaseReservation`；`pkg/counterstate/reservations.go` `CreateReservation`；`trigger/engine/engine.go` `Place`
  - 修复方向：reservation create / release / consume 应 journal 化，或明确把 reservation 从资金冻结语义中移出；只靠 snapshot 不足以支撑跨服务恢复。

- **[P1] 生产环境仍可默认信任 `X-User-Id`** — BFF 默认 `--auth-mode=header`，`env=prod` 只校验 push trusted header，不禁止 header auth；`pkg/auth.Middleware` 直接信任 `X-User-Id`。如果生产误配置，即可伪造任意用户身份。
  - 状态：open
  - 代码点：`bff/cmd/bff/main.go` `parseFlags` / `validate`；`pkg/auth/middleware.go` `Middleware` / `NewMiddleware`
  - 修复方向：`env=prod` 下禁止 `auth-mode=header` 和 mixed 中的 header fallback，除非显式 dev-only escape hatch；补 ARCH-008 的实现测试。

- **[P1] trade-dump snapshot shadow 遇到 journal apply 错误会跳过记录** — shadow engine 在 apply 前先推进 `nextJournalOffset = kafkaOffset + 1`，pipeline 对 apply error 只打日志后继续消费。坏 journal 记录一旦被跳过，后续 snapshot 会永久缺失该状态，Counter recovery 又信任这个 snapshot。
  - 状态：open
  - 代码点：`trade-dump/internal/snapshot/counter/shadow/engine.go` `Apply`；`trade-dump/internal/snapshot/counter/pipeline/pipeline.go` `handleRecord`
  - 修复方向：apply error 应 fail-stop / 隔离分区并报警，不能推进 snapshot cursor；如果要跳过，必须有 quarantine ledger 和人工确认机制。

- **[P2] 性能目标和当前 Kafka 事务粒度不匹配** — 架构目标写着单实例下单 20w TPS / 单 symbol 撮合 4w TPS，但 Counter 当前每条 `Publish` 都是一个 Kafka transaction，并且同一 `TxnProducer` 用 mutex 串行 Begin / Flush / Commit。这个实现更像 correctness-first MVP，和目标吞吐存在结构性差距。
  - 状态：open
  - 代码点：`counter/internal/journal/txn_producer.go` `Publish` / `runTxn`；`docs/architecture.md` 性能目标
  - 修复方向：先用 ADR-0082 benchmark 量化瓶颈；如果目标仍成立，需要批量事务、流水线化、或重新定义 counter-journal / order-event 的写入粒度。

## Backlog

_(none)_

## Fixed

### 2026-06-10

- **perp-journal 四类用户事件分区键缺失，多分区下打破 per-user 全序** — `perp-counter/internal/journal/convert.go` `journalPartitionKey` 的 switch 漏掉 ADR-0074/0077/0079/0081 新增的 `PositionConfig` / `MarginAdjustment` / `CustomerRiskLimit` / `InvariantBreach` 四类 payload，落入 default 返回 ""（默认 partitioner），同一用户的这些事件与其按 user_id 正确哈希的 OrderStatus/Settlement 等事件可落在不同分区，违反 `perp_journal.proto` 头部声明的 "Partition key: user_id"。消费方影响排查：push 私有流（`push/internal/consumer/perp_private.go` 按 user 路由 WS）在多分区部署下可见保证金调整相对结算乱序；trade-dump 投影四张表均为 `INSERT IGNORE` + `perp_seq_id` 主键，幂等且对到达序不敏感；perp-risk coordinator 不消费这四类 payload；perp-counter 自身恢复走 engine/service snapshot，不回放 perp-journal——均无状态损坏。
  - 状态：fixed
  - commit: [`7438df9`](../../commit/7438df9)
  - 根因：给 payload oneof 新增事件类型时只接了事件构造与下游投影，没有同步 partition-key switch；缺少强制穷举 oneof 的回归测试，遗漏静默退化为默认分区。
  - 修法：补四个 case 按 user_id 取键；表测试扩到全部 13 类 payload（含 risk_pool_settlement 与 user_id=0 的 "" 路径）；新增 `TestJournalPartitionKey_OneofExhaustive` 用 protobuf 反射遍历 payload oneof——任何带 user_id 字段的 payload 若未被 switch 处理即测试失败，机制性防止再漏（已用临时删 case 验证该测试确实报错）。

- **tools/web 编译失败：faucet 仍把 string user 传给 uint64 的 `TransferInRequest.user_id`** — `cd tools/web && go build ./...` 报 `./faucet.go:65:15: cannot use user (variable of type string) as uint64 value in struct literal`。user id 全栈 string→uint64 迁移遗漏了 tools/web；前端默认用户 `"alice"` / `"bob"` 属同一遗留——`pkg/auth.parseUserID` 只接受非零数字 `X-User-Id`，旧默认值会被 BFF 在所有 REST/WS 调用上拒绝。
  - 状态：fixed
  - commit: [`6cc8ca2`](../../commit/6cc8ca2)
  - 根因：tools/web 在 go.work 里但不在 Makefile `MODULES` 列表中，CI 的 `make build` / `make vet` 不编译它，迁移时的断裂一直未被构建暴露。已闭环：`MODULES` 现覆盖全部 go.work 成员（含 tools/tui、tools/web、tools/precision-cli），CI 的 build/vet/test 全量扫描（commit [`7d13537`](../../commit/7d13537)）。
  - 修法：`handleFaucet` 在 API 边界 `strconv.ParseUint` 校验（非数字 / 0 → 400，规则对齐 `pkg/auth.parseUserID`），`faucet.credit` 参数改 `uint64`；index.html 默认用户改 `"1"` / `"2"`，启动时过滤 localStorage 中残留的旧用户名，自定义用户输入加数字校验。

- **journal catch-up 重建的 market-buy-by-quote 订单退化成零量限价单** — `FreezeEvent` 没有 `quote_qty` / `slippage_bps` 字段，跨 snapshot/journal catch-up 边界的 ADR-0035 市价买单（按 quote 预算）被 `applyFreezeEvent` 重建后 `QuoteQty=0 ∧ Qty=0`：catch-up 结束后的 LIVE 成交走 `settleTaker` 的限价买分支（Price=0），`FrozenQuoteDelta=0` 冻结永不消耗，且 `statusAfterFill` 在首笔部分成交就判 FILLED（`filledAfter ≥ Qty(0)`）。
  - 状态：fixed
  - commit: [`4c6d6b4`](../../commit/4c6d6b4)
  - 根因：FreezeEvent 的字段集是限价单时代定的，ADR-0035/0083 给 Order 加的形状字段（`QuoteQty` / `SlippageBps`）只进了 `OrderPlaced`，没进 counter-journal 的 `FreezeEvent`，重建路径丢形状。
  - 修法：`FreezeEvent` 增加 `quote_qty`(13) / `slippage_bps`(14)，`BuildPlaceOrderEvents` 透传，`applyFreezeEvent` 还原；settlement 与 terminal unfreeze 依赖的 `IsMarketBuyByQuote` / `IsMarketBuyByBase` 谓词在重建后成立。附单测 `TestApplyFreezeEvent_MarketBuyByQuoteThenLiveSettlement` / `TestApplyFreezeEvent_RestoresProtectedMarketBuyShape`。

- **journal 回放的 FrozenSpent 用 match price 重算，价格改善的 taker 买单回放后 terminal unfreeze 可把 Frozen 打成负数** — `accumulateFrozenSpent` 对买方限价单按 `evt.Price × evt.Qty` 重算消耗，但 `evt.Price` 是撮合价；LIVE 路径 (`ApplyPartySettlement`) 累计的是 `|FrozenQuoteDelta|` = 委托价 × qty。价格改善成交回放后 `FrozenSpent` 偏小，terminal 释放 `FrozenAmount − FrozenSpent` 超过实际仍冻结的量 → `UnfreezeOnTerminal` 报 "frozen would be negative"。另外 market-buy-by-quote 分支读的是 `delta_quote`（available 侧增量，该形状恒为 0），回放从不累计 FrozenSpent，属同一处的并发缺陷。
  - 状态：fixed
  - commit: [`4c6d6b4`](../../commit/4c6d6b4)
  - 根因：回放侧自己重算消耗公式，与 LIVE 路径的事实数据二次推导不一致；`SettlementEvent` 本来就带 `unfreeze_base` / `unfreeze_quote`（= `|FrozenBaseDelta|` / `|FrozenQuoteDelta|`，event sourcing 的权威值），不该重算。
  - 修法：`accumulateFrozenSpent` 改为直接累加事件的 `unfreeze_base` + `unfreeze_quote`，对全部订单形状统一成立。附 replay-vs-live 等价性单测 `TestApplySettlementEvent_PriceImprovedTakerMatchesLivePath`（含 terminal unfreeze 不为负的断言）。

### 2026-04-19

- **match book 里找不到 order 的 cancel 被静默丢弃，counter 永远停在 PENDING_CANCEL** — counter/match 状态分叉（dev 多次重启 / 快照丢失）后，counter 的 cancel 请求打到 match 找不到订单，match 直接 `return` 不 emit，counter 的 in-flight cancel 永远没有回包，`unfreezeResidual` 永远不执行，frozen 资金卡死。之前只能人工 force-cancel 或清状态。
  - commit: [`78a4c89`](../../commit/78a4c89)
  - 修法：`handleCancel` book miss 路径改为 emit `OutputOrderCancelled` + `FilledQty=0` + 正常分配 match_seq，保持"每个 input 都产出带 seq 的 output"不变量。counter 收到后由 `handleCancelled` 自己判断：order 已 terminal → 只 advance match_seq 短路；仍活着 → `unfreezeResidual` + 转 CANCELED。quote `OnOrderClosed` 对 unknown order 本来就是 no-op。附 1 个单测 `TestWorkerCancelUnknownOrderStillEmits`。

- **取消订单时 open orders 闪烁出更多单又消失** — `pollAccount` 多路径并发（cancel 回调 / WS user event / 2.5s 定时器），更老的 poll 结果覆盖了更新的一次。
  - commit: [`7fc4089`](../../commit/7fc4089)
  - 修法：给每次 pollAccount 加单调 seq，写回前比对，落后的 drop。

- **orderbook 刷新页面就没了** — web 只订阅 WS `depth@` 增量，Quote 每 5s 才推一次 snapshot，页面首屏空窗。
  - commit: [`057b4ef`](../../commit/057b4ef)
  - 修法：boot 时 `GET /v1/depth/<symbol>` 拉 BFF market-cache 的种子（ADR-0038）；`smoke.sh` 给 BFF 加 `--market-brokers`。

- **counter 对自成交 (maker_user == taker_user) 只应用了一半 settlement** — `handleTrade` 两次调 `applyPartyViaSequencer`，同一账户同一 matchSeq，第一次 advance 后第二次被 match_seq 保护直接跳过。
  - commit: [`4ccaf23`](../../commit/4ccaf23)
  - 修法：检测到同用户两边，走 `applySelfTrade` 合并路径，在一次 sequencer.Execute 里顺序应用两边 settlement，末尾一次 AdvanceMatchSeq。附 2 个单测。

- **PENDING_CANCEL 单被前端隐藏导致 frozen 资金成谜** — 之前的修法过滤掉了 pending_cancel，用户看余额被冻着却找不到对应单子。
  - commit: [`e601885`](../../commit/e601885)
  - 修法：显示 pending_cancel 单，行降透明度 + 状态字段显示 `pending_cancel`，cancel 按钮换成 `canceling…` 占位避免重复点击。

- **cancel 后刷新页面又回来了（PENDING_CANCEL 幽灵单）** — BFF 把 internal `PENDING_CANCEL` 折叠成外部 `new`，UI 区分不出"在途取消"和"正常挂单"；叠加 counter/match 状态分叉导致永远完不成 CANCELED 转移。
  - commit: [`9eb713a`](../../commit/9eb713a)
  - 修法：`/v1/order/{id}` 响应加 `internal_status` 字段暴露 8 态原始值，web 能区分 pending_cancel。

- **accidentally committed tui binary** — `tools/tui/tui` 把 10MB Mach-O 一起塞进了 git。
  - commit: [`8f1d21d`](../../commit/8f1d21d)
  - 修法：删文件 + `.gitignore` 加 `tools/*/tui`、`tools/*/web`。

- **history `/v1/orders` 永远 500 — sql: Scan error converting []uint8 to int64** — MySQL 8 里 `UNIX_TIMESTAMP(t)*1000 + MICROSECOND(t) DIV 1000` 返回 DECIMAL，driver 给 Go 的是 []byte，int64 扫描崩。
  - commit: [`5276376`](../../commit/5276376)
  - 修法：表达式外面套 `CAST(... AS SIGNED)` 强制整型。
