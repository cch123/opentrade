# ADR-0078: perp 订单与持仓产品 API

- 状态: **Accepted / Implemented**（2026-05-31 起草；2026-06-11 实现前设计修订并接受，修订内容见「设计修订记录」；同日实现落地，commit `99ad14d` —— perp-counter COID 幂等/amend/batch/cancel-all/pre-check/close-all/force-adjust/block-trade + trigger perp position binding/mark-price 消费/CountActiveTriggers + trade-dump shadow + BFF/admin-gateway 端点，全仓 build/vet/test/test-race 通过）
- 日期: 2026-05-31（修订 2026-06-11）
- 决策者: xargin, Codex
- 相关 ADR: 0014（改单为撤单 + 新建）、0020（订单状态机）、0035（服务端 MARKET）、0062（终态退场 + 幂等环）、0068（perp MVP）、0074（保证金模式）、0077（position_idx）、0081（reduce_only 结算硬约束）、0083（protected market order）

## 范围声明（先读这一段）

本 ADR 定义 perp 产品层 API 的扩展边界：

- amend order
- batch create/cancel
- cancel all / by symbol
- pre-create 试算
- close all position
- block trade
- force add/sub position
- TP/SL/TrailingStop 与 perp 持仓语义集成（含 close_on_trigger）

显式推迟（不在本 ADR 决策范围内）：

- **swap/transfer position**（仓位在账户间转移）：依赖子账户体系，当前系统没有账户层级模型，留待子账户 ADR 一并决策。
- amend fast-path（最坏情况预占的低延迟模式）：见 §2。
- batch `atomic=true`（全有或全无）：见 §3。

本 ADR 不要求一次性实现全部 API，但决定它们如何映射到已有 Counter/Match/trigger 架构。

## 设计修订记录（2026-06-11，实现前修订）

实现前审计发现初稿存在依赖缺口和欠定义语义，按以下 10 条修订。原则：不引入新的真值源、不绕过订单状态机、复用已实现的不变量兜底（ADR-0077 §2 结算端 clamp + `REDUCE_ONLY_INVARIANT_BREACH`）。

1. **close-all 解除对 ADR-0081 的硬依赖**。初稿要求 close-all 复用 ADR-0081 的 close capacity reservation，但 0081 仍是 Proposed 未实现。修订为 **conservative 两阶段**：撤销 scope 内全部在管订单（含 reduce-only，因为没有 capacity 记账时，存量 reduce-only 单会和 close 单超订可关数量）→ 等全部终态 → 按届时 leg size 下 reduce-only protected market 单；close-all 进行期间拒绝该 scope 的新订单。这样 close 单下发时 scope 内没有任何其他活跃订单，不需要 capacity reservation 也不会超订。0081 落地后，系统生成的 close 单改为按 capacity 精确取量（0081 §1 已预留该路径）。
2. **perp 触发单的触发价格基准 = mark price**。初稿未定义价格源。现状是 trigger 服务消费 Quote 的 market-data PublicTrade 流，而 Quote 只消费现货 `trade-event`，**perp symbol 没有任何价格进入 trigger**。修订：perp position-bound trigger 以 `perp-price` topic 的 `MarkTick.mark_price` 为触发基准（防操纵性优于 last price，且与强平判定同源）；trigger 增加该 topic 的有状态消费，offset 必须随 snapshot 原子绑定（扩展 `TriggerMarketCheckpointEvent`，与 ADR-0048/0067 模型一致）。last-price 基准推迟到 Quote 接入 perp 行情后作为可选项。
3. **前置依赖：perp-counter 必须实现 client_order_id 幂等**。perp.proto 注释声称 `client_order_id` 是幂等键，但 perp-counter 实现中没有任何去重。trigger 触发的 crash-replay 安全性（`client_order_id = "trig-<id>"`，重放靠 Counter 去重收敛）和 batch 的 per-item 幂等都建立在这之上。修订为实现前置项：活跃单 COID（client order id）索引 + 终态 COID 幂等环（镜像现货 ADR-0062），随 snapshot 持久化。
4. **batch_id 降级为关联键**。初稿称 batch_id 为"幂等键"，但 batch 结果重放需要持久化整个 batch 结果表，成本不对称。修订：幂等由每个子项的 `client_order_id` 去重承载（见第 3 条）；`batch_id` 只是审计/关联键，原样回显并随子项记入 journal 事件。
5. **amend 语义补全**：`qty` 是**新的总意图量**（对齐 Bybit amend 语义）——新单数量 = 请求 qty − 旧单累计成交量；差值 ≤ 0 则 amend 以 `ALREADY_FILLED` 终态结束、不下新单。只允许改 price/qty（改方向/symbol/position_idx/reduce_only 是新订单不是 amend）；MARKET 单不可 amend；同一订单同时只允许一个在途 amend；显式 CancelOrder 中止在途 amend。新单 order_id 在 AmendOrder 请求时**预分配**并同步返回（也是重放收敛的锚点，见第 6 条）。
6. **事件驱动下单的重放收敛**：amend 的新单与 close-all 的 close 单都由 trade-event 终态事件驱动下发，崩溃重放会重复 dispatch。修订利用 Match 已有的 `REJECT_REASON_DUPLICATE_ORDER_ID` 防御（order id 已在簿内的 Placed 被拒绝）：这两类订单的 order_id 在**请求时预分配并随挂起状态进 snapshot**，重放重复 dispatch 时 Match 回 duplicate 拒绝，perp-counter 对**自己跟踪中的订单**收到 duplicate 拒绝时忽略（不终态化），由重放流中该订单自己的生命周期事件驱动至正确状态。残余窗口：原 dispatch 已在 Match 终态 + 崩溃重放 → 重复入簿造成二次成交——与 ADR-0068 已声明的"order-event/journal 非单事务原子"属同一硬化桶（未来 Match 终态 order-id 环可关闭，见未来工作）。
7. **position_version 与 side_to_close 不上 wire**。position_version 每次成交都会递增，TP/SL 存活期内必然 mismatch，作为 OCC（optimistic concurrency control，乐观并发控制）守卫无可用窗口，删除；side_to_close 恒等于 opposite(order side)，由 ADR-0077 §2 准入矩阵在触发时校验，不单独建字段。`close_on_trigger` 保留字段并记录审计，但 V1 语义为空：perp reduce-only 单不占 IM（initial margin，初始保证金）也不占手续费缓冲，不存在"为保证平仓腾挪保证金"的场景；字段为未来 margin-borrow 语义占位。
8. **admin force add/sub 经济语义定死**：复用 fill 结算原语（加权均价 / realized PnL / 保证金路由与正常成交同一套数学），但只出 `PerpAdminPositionAdjustmentEvent`，不出 settlement/trade 行、不收手续费、不进公开行情。isolated force-add 需要 wallet free balance 足额（先 Reserve 再 ApplyFill），不足即拒绝；force-sub 数量必须 ≤ leg size（禁止翻向）；leg 在清算中拒绝。绕过 SymbolConfig 状态机（admin 修复可能发生在 HALT 的 symbol 上），但仍走精度校验。
9. **block trade V1 为进程内双 user sequencer 协调**。perp-counter 当前单实例部署，初稿的跨 shard 协调器（ADR-0071 模式）没有实施对象。V1：按 user_id 升序获取两个用户的 sequencer 锁（避免死锁），校验与应用在**同一临界区**内完成（没有 TOCTOU——time-of-check-to-time-of-use，检查与使用之间状态已变的竞态——窗口，因此不需要版本戳重校验）；同 user 双边直接拒绝。跨 shard 版本戳协调推迟到 perp-counter 真分片时，沿用初稿引用的 0071 模式。RFQ/审批流是产品层后续，V1 入口为 admin-gateway。
10. **pre-create 字段清理**：`price_protection_result` 依赖未实现的 ADR-0080 准入价格保护，字段保留 deferred 不返回；`snapshot_version` 即 SymbolConfig `config_version`；`max_open_qty` 是按当前 available 与费率的估算值（不是承诺）；新增返回 `fee_buffer`（ADR-0079 §4 taker 费率预留）。

## 背景 (Context)

当前 perp REST 只有最小面：下单、撤单、查单、查仓、查保证金。成熟合约产品需要大量持仓级和批量操作。如果每个 API 都绕过现有订单状态机直接改仓，会破坏 ADR-0014/0020 的可复盘性。

## 决策 (Decision)

### 1. 产品 API 分成三类

```text
+------------------+-----------------------------+-------------------------+
| 类别             | 示例                        | 执行权威                |
+------------------+-----------------------------+-------------------------+
| order commands   | amend/batch/cancel-all      | perp-counter + Match    |
| position commands| close-all/TP-SL             | perp-counter sequencer  |
| admin commands   | force add/sub/block trade   | admin-gateway + audit   |
+------------------+-----------------------------+-------------------------+
```

所有会影响用户仓位或保证金的命令必须进入用户 sequencer。所有会进入 orderbook 的命令必须通过 Match，不允许产品 API 直接改 Match book。

### 2. amend order 仍实现为 cancel + new

沿用 ADR-0014，V1 只实现 conservative 模式：

```text
AmendOrder(user, order_id, new_price, new_qty)
    |
    +-- 用户 sequencer 内校验：归属 / 非终态 / 非清算系统单 /
    |   LIMIT 单 / 无在途 amend / new_qty > 已成交量
    +-- 预分配 new_order_id（同步返回给调用方）
    +-- 登记 pending amend{old_order_id, new_order_id, new_price, new_qty}
    |   （入 snapshot；出 PerpAmendEvent REQUESTED）
    +-- 对旧单 DispatchCancel
    +-- 返回 {amend accepted, old_order_id, new_order_id}

旧单终态事件（CANCELED / FILLED / EXPIRED / REJECTED）到达，同一 sequencer 执行内：
    |
    +-- remaining = new_qty - 旧单累计成交量
    +-- remaining <= 0:
    |     amend 终态 ALREADY_FILLED（出 PerpAmendEvent，不下新单）
    +-- remaining > 0:
          以 new_order_id 走完整 PlaceOrder 准入（catalog 门 / 意图矩阵 /
          杠杆 / 费率 pin / IM+fee 预留）
            |-- 准入拒绝: amend 终态 FAILED(原因)（出 PerpAmendEvent）
            +-- 准入通过: dispatch 新单，amend 终态 COMPLETED（出 PerpAmendEvent）
```

不做 in-place 修改。原因是 Match orderbook 的位置、time priority、reservation、journal 都能保持简单。

cancel 是异步的，旧单在 cancel terminal 前仍可能成交——conservative 模式天然吸收这个竞态：新单数量在旧单终态后才计算，成交多少扣多少，结果确定。语义细则（修订 #5）：

- `new_qty` 是**新的总意图量**，新单实际数量 = `new_qty - old.filled_qty`（旧单终态时点的值）。
- 只允许改 price/qty；TIF、reduce_only、position_idx、杠杆从旧单继承。reduce_only 旧单 amend 后的新单仍是 reduce_only，走正常准入（对侧仓位检查）。
- MARKET 单不可 amend（生命周期太短，没有可改窗口）。
- 一张订单同时最多一个在途 amend，重复请求拒绝 `amend_in_progress`。
- 在途 amend 期间用户显式 CancelOrder：amend 中止（终态 `ABORTED_BY_CANCEL`，不下新单），撤单继续——显式撤单是比 amend 更强的意图。
- 新单准入失败（如保证金不足）= amend 终态 FAILED：旧单已撤、新单未立，与 ADR-0014 客户端两步语义一致，状态可观测不含糊。

**重放收敛**（修订 #6）：pending amend（含预分配的 new_order_id）随 snapshot 持久化；崩溃重放重复触发终态继续逻辑时，重复 dispatch 的新单被 Match 以 `DUPLICATE_ORDER_ID` 拒绝，perp-counter 对跟踪中订单的该拒因**忽略**，订单状态由重放流中它自己的 Accepted/Trade/终态事件收敛。

若后续为了低延迟实现 fast-path（撤旧未终态即下新、按旧单剩余风险 + 新单风险最坏情况预占），必须把 amend 状态写入 journal 并额外测试最坏情况预占；V1 不实现。

### 3. batch API 返回 per-item result，不默认事务化

`BatchPlaceOrders` / `BatchCancelOrders`：

- 前置（修订 #3）：perp-counter 实现 `client_order_id` 幂等——活跃单 COID 索引（命中返回原 order_id + `accepted=false`）+ 终态 COID 幂等环（镜像现货 ADR-0062，环内命中同样返回原 order_id），均入 snapshot。
- `batch_id` 是关联/审计键（修订 #4），原样回显；幂等由子项 `client_order_id` 承载。
- 每个子项独立走 PlaceOrder/CancelOrder 全套准入，按提交顺序执行（同一用户天然在 sequencer 内有序）；默认语义是 best-effort per-item result，部分成功必须可复盘（每个子项的 journal 事件链独立完整）。
- 单批上限 20 项，超出整批拒绝（防 sequencer 长占用）。
- 若未来需要 all-or-nothing，另加 `atomic=true`，只允许同 user + 同 symbol + 小批量，并在 sequencer 内预校验全部 reservation；V1 不实现。

**cancel all / by symbol**（初稿范围项，补语义）：`CancelAllOrders(user, optional symbol)` 在用户 sequencer 内收集全部活跃订单（跳过清算系统单与 SETTLING 后状态的 symbol），逐一 DispatchCancel 并转 PENDING_CANCEL，返回 per-order 受理结果。"by coin" 即不带 symbol 的全量（USDT 单一结算资产）。

### 4. pre-create 是试算，不是锁定

`PreCheckOrder` 与 PlaceOrder 共用同一套准入计算（dry-run 模式：不预留、不触发杠杆配置写穿），返回：

```text
required_initial_margin   // IM
fee_buffer                // ADR-0079 §4 taker 费率预留
effective_leverage
margin_mode / risk_id
max_open_qty              // 估算：available / (price/lev + price*taker_rate)
reject_reason_if_any      // 跑完整准入门后的拒因（空 = 会被受理）
config_version            // 试算所依据的 SymbolConfig 版本
```

它不产生 reservation，也不保证随后真实下单一定成功。真实下单仍重新校验 mark、config version、wallet、active orders。`price_protection_result` 推迟到 ADR-0080 落地（修订 #10）。

### 5. close all position 生成 reduce-only 子单（conservative 两阶段）

```text
CloseAllPositions(user, optional symbol, slippage_bps, client_op_id)
    |
    +-- 用户 sequencer 内：scope 任一 leg 清算中 -> 拒绝 liquidation_in_flight
    |   已有在途 close-all -> 同 client_op_id 返回现状，否则拒绝 close_all_in_progress
    +-- 为 scope 内每个可能的 leg 预分配 close order_id（入 registry + snapshot）
    +-- 出 PerpCloseAllEvent REQUESTED
    +-- 阶段 CANCELING：对 scope 内全部活跃订单 DispatchCancel，
    |   记 pending set；scope 内新订单一律拒绝 close_all_in_progress
    |   （含 trigger 触发单——触发单被拒走 REJECTED 终态，不会反向开仓）
    |
    |   （此间 in-flight fill 照常结算——sizing 在全部终态之后，天然包含它们）
    |
    +-- pending set 清空（全部终态）后，同一 sequencer 执行内进入阶段 PLACING：
    |     对每个非 flat 且不在清算中的 leg：
    |       以预分配 id 下 reduce_only protected MARKET 单（ADR-0083 collar）
    |       （reduce-only 不占 IM/fee buffer，准入不会因保证金失败）
    +-- close 单全部终态 -> close-all 终态 DONE（部分 leg 下单失败则
        DONE_WITH_ERRORS，per-leg 原因入 PerpCloseAllEvent）；guard 解除
```

不能直接按 mark 结算关仓；必须通过 Match 或清晰的 admin/system settlement 路径，避免绕过市场成交和对手方账务。

修订 #1 的关键点：**close 单下发时 scope 内没有任何其他活跃订单且新单被拒**，因此不需要 ADR-0081 capacity reservation 也不会超订可关数量。残余竞态只剩清算与 close 单的重叠（清算在 PLACING 后启动：破产单与 close 单同时减仓）——由已实现的结算端 clamp + `REDUCE_ONLY_INVARIANT_BREACH`（ADR-0077 §2 / ADR-0081 §2 的"显式暴露不静默"路径）兜底。ADR-0081 落地后，close 单按 capacity 精确取量，该兜底收敛为不可达。重放收敛同 §2（close order_id 请求时预分配 + duplicate 拒绝忽略）。

### 6. TP/SL/TrailingStop 归一到 trigger 服务，但绑定 position

perp 的 TP/SL 不是普通触发单的别名。trigger 记录必须绑定：

- `symbol`（perp symbol）
- `position_idx`
- `reduce_only = true`（隐含，触发时强制）
- `close_on_trigger` 可选（V1 仅记录，修订 #7）
- `slippage_bps` 可选（MARKET 变体触发时下 protected market，ADR-0083）

side_to_close 不上 wire（= opposite(order side)，触发时由 ADR-0077 §2 准入矩阵校验）；position_version 删除（修订 #7）。

**触发价格基准 = mark price**（修订 #2）：trigger 服务新增 `perp-price` topic 消费（MarkTick→更新 symbol 价格并跑同一套触发扫描；FundingTick 忽略），per-partition offset 进 `TriggerMarketCheckpointEvent.perp_price_offsets`，随 snapshot 原子绑定、重启 seek（ADR-0048/0067 同构）。spot 与 perp symbol 命名空间不相交，价格表共用无歧义。

**触发执行**：trigger 引擎经 `PerpPlacer` seam 调 perp-counter `PlaceOrder`（`reduce_only=true` + `position_idx` + `client_order_id="trig-<id>"`，crash-replay 由修订 #3 的 COID 幂等收敛）。触发时若仓位不存在或方向不匹配，perp-counter 准入拒绝（`reduce_only_requires_opposite_position` / 意图矩阵拒因），trigger 把这组拒因映射为新终态 **`EXPIRED_POSITION_GONE`**，不反向开仓；其他拒因仍是 REJECTED。

**OCO 复用**：仓位的"TP+SL 成对"就是两条 position-bound trigger 的 OCO（ADR-0044），机制原样复用（同 user/symbol/side 校验天然满足——两条都是平仓侧）。

**TriggerChecker 接通**（ADR-0077 §3 遗留 seam）：trigger 服务暴露 `CountActiveTriggers(user, symbol)`（PENDING 计数，引擎已有 activeTriggers 索引）；perp-counter 经注入的 client 适配器在 SetPositionMode 内查询，**RPC 失败按 fail-closed 处理**（拒绝 mode switch，拒因 `active_triggers_check_unavailable`）。注意这个守卫是 best-effort 的 UX 防线——trigger 下单与 mode switch 之间没有分布式锁，真正的硬保证是触发时准入矩阵 fail-closed：mode 已切换的 orphan trigger 触发时被拒，走 `EXPIRED_POSITION_GONE` 干净终态。

### 7. admin force add/sub position 必须独立审计

`ForceAdjustPosition`（add/sub 合一）只允许 admin-gateway 调用：

- 必须带 reason、ticket、operator。
- 只在用户 sequencer 内 mutation。
- 经济语义（修订 #8）：复用 fill 结算原语——ADD = 按 admin 指定 price 的同向开仓数学（加权均价、isolated 先 Reserve IM 再 ApplyFill，free balance 不足拒绝；cross 走 pool 准入）；SUB = 按指定 price 的减仓数学（realized PnL、保证金按比例释放），数量必须 ≤ leg size，禁止翻向。
- 守卫：leg 清算中拒绝；ONE_WAY/HEDGE 的 position_idx 合法性同 ADR-0077 §2；绕过 SymbolConfig 状态机但保留精度校验。
- 必须发 `PerpAdminPositionAdjustmentEvent`（含 position_after 快照）。
- 不得伪装成普通成交：不出 PerpSettlementEvent、不写 trade 表、不收手续费、不进行情。

### 8. block trade 与普通撮合隔离

block trade 是大宗协议成交，不进入公开 orderbook，但它是**双边成交**，不能当成单用户 internal settlement。V1（修订 #9，单实例部署）：

```text
BlockTrade(block_trade_id, symbol, price, qty,
           buyer{user, position_idx, reduce_only},
           seller{user, position_idx, reduce_only})
    -> admin-gateway 审批/审计入口
    -> perp-counter 按 user_id 升序锁两个用户的 sequencer（防死锁）
       |-- block_trade_id 幂等（重复请求返回首次结果，入 snapshot）
       |-- 同 user 双边 / 无 mark / 价格超出 mark ±band（配置）-> 整笔拒绝
       |-- 每腿独立准入（同一临界区，无 TOCTOU 窗口）：
       |     catalog 精度门 + ADR-0077 §2 意图矩阵 + reduce-only 对侧仓位
       |     与 qty <= leg size + 杠杆解析 + 费率 pin（双边 taker）
       |     + 风险档位 notional cap + IM + fee buffer Reserve
       |-- 任一腿失败：释放已成功腿的 reservation，整笔拒绝
       |-- 两腿都过：ApplyFillWithFee 双腿落仓（与正常成交同一结算数学）
    -> 出 PerpBlockTradeEvent（双腿汇总）+ 每腿 PerpSettlementEvent
       （order_id=0，trade_id="block-<id>"——可复盘地标记非撮合成交）
    -> 不进公开行情（perp-counter 本就不产行情；Match 被绕过 = 天然不公布；
       是否延迟公布是产品策略，挂在未来 RFQ 流程上）
```

跨 shard 形态推迟到 perp-counter 真分片：届时协调器只编排和记录 intent，真正仓位 mutation 仍在各自 user sequencer 内、按 [ADR-0071](./0071-perp-sharded-insurance-and-cross-shard-adl.md) 的版本戳模式执行；任一 leg 版本不匹配或保证金不足，整笔 reject。

## 备选方案 (Alternatives Considered)

### A. 每个高级 API 直接操作仓位

实现快，但绕过订单状态机和 Match，审计不可信。否决。

### B. batch 默认 all-or-nothing

对用户直观，但跨 symbol / 多订单 reservation 复杂，失败回滚成本高。先选 per-item result。

### C. TP/SL 直接放 perp-counter，不复用 trigger

能少一次跨服务，但会复制触发矩阵、TTL、OCO、trailing 逻辑。否决；复用 trigger，但加 perp position binding。

### D. close-all 等 ADR-0081 capacity 落地后再做

时序上干净，但 close-all 是风险处置入口（panic button），不应排在 capacity 优化之后。conservative 两阶段在没有 capacity 的前提下同样不超订（修订 #1），先落地，0081 落地后收紧。

### E. perp 触发用 last price（接入 Quote perp 行情）

产品上更常见的默认，但需要 Quote 消费 perp-trade-event 并产 perp 行情（独立的产品化工作），且 last price 可被薄盘操纵打触发。V1 用 mark price（与强平判定同源），last price 作为后续可选基准。

## 影响 (Consequences)

### 正面

- 产品 API 丰富但不破坏现有事件源模型。
- 用户能得到批量操作和持仓级 TP/SL（含 OCO 成对）。
- admin 强制操作和 block trade 有独立审计路径。
- client_order_id 幂等补齐后，trigger crash-replay 与客户端重试都有确定收敛。

### 负面 / 代价

- BFF/trigger/perp-counter/admin-gateway 都要扩协议。
- batch 和 close-all 需要清晰的 per-item / per-phase 状态模型（registry + snapshot）。
- TP/SL 与 position_idx、reduce_only、mode switch 的交互复杂。
- 事件驱动下单的重放残余窗口（原单已终态 + 崩溃重放 → 二次成交）并入 ADR-0068 既有硬化桶，上线前需关闭（Match 终态 order-id 环或单事务原子化）。

## 实施约束 (Implementation Notes)

- 所有新 API 必须有 `client_op_id` / `client_order_id` / batch 子项 id。
- product API 的结果要能从 journal 重建：amend/close-all/admin 调整/block trade 各有专属 journal 事件（`PerpAmendEvent` / `PerpCloseAllEvent` / `PerpAdminPositionAdjustmentEvent` / `PerpBlockTradeEvent`），不依赖内存临时状态。
- pending amend / close-all registry / 终态 COID 环 / block_trade_id 幂等表全部进 snapshot（ADR-0048 capture barrier 内）。
- close-all 第一版只支持 market protected reduce-only，不支持用户指定复杂路径。
- amend 只实现 conservative；fast-path 必须额外测试 worst-case reservation 后才许实现。
- **ADR-0077 依赖**：TP/SL position binding 落地的同一里程碑，必须把真实的 active-trigger 查询接入 perp-counter 的 `TriggerChecker` seam（ADR-0077 §3），RPC 失败 fail-closed。
- trade-dump / push 的 perp-journal 消费端必须对未知 payload 种类 forward-compat 跳过（新事件先入 journal，投影可后续跟进）。
- 单测覆盖：COID 去重（活跃 + 终态环 + snapshot roundtrip）、batch 部分成功、amend cancel/fill race 与 ALREADY_FILLED、amend/close-all 重放 duplicate 拒绝忽略、TP/SL 仓位消失 → `EXPIRED_POSITION_GONE`、close-all in-flight fill 计入 sizing、close-all 与清算重叠的 breach 兜底、admin force 调整审计字段、block trade 一腿失败整笔 reject 与并发反序锁、mark band 拒绝。

## 未来工作

- ADR-0081 close capacity reservation 落地后：close-all 改为按 capacity 取量；用户 reduce-only 超订从结算 clamp 前移到准入拒绝。
- Match 终态 order-id 环（ADR-0062 的 Match 侧镜像）：关闭事件驱动下单的重放二次成交窗口。
- Quote 接入 perp 行情后：trigger 增加 last-price 触发基准选项。
- perp-counter 分片后：block trade 升级为 ADR-0071 模式的跨 shard 版本戳协调。
- RFQ/审批流产品化（block trade 的用户侧入口）；行情延迟公布策略。
- batch `atomic=true`；amend fast-path。
- swap/transfer position（依赖子账户 ADR）。

## 参考 (References)

- [ADR-0014: 改单实现为撤单 + 新建](./0014-order-modify-as-cancel-new.md)
- [ADR-0045: 触发单 Trailing Stop](./0045-trigger-trailing-stop.md)
- [ADR-0062: 终态退场与幂等环](./0062-order-terminal-eviction-and-idempotency-ring.md)
- [ADR-0067: trigger 快照经 trade-dump shadow](./0067-trigger-snapshot-via-trade-dump-shadow.md)
- [ADR-0077: perp 持仓模式](./0077-perp-position-mode-hedge-both-side.md)
- [ADR-0081: perp reduce_only 结算时硬约束](./0081-perp-reduce-only-settlement-hardening.md)
- [ADR-0083: Match 原生 protected market order](./0083-match-native-protected-market-order.md)
