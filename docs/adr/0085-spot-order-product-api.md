# ADR-0085: 现货订单产品 API（amend + batch）

- 状态: **Proposed**（2026-06-11 起草；ADR-0078 落地后现货/perp 产品面不对称的补齐项）
- 日期: 2026-06-11
- 决策者: xargin, Codex
- 相关 ADR: 0014（改单为撤单 + 新建）、0015（client_order_id 幂等）、0018（UserSequencer）、0020（订单状态机）、0041（Reservation 资金预留）、0054（MAX_OPEN_LIMIT_ORDERS）、0058（虚拟分片）、0060（异步消费 + catch-up）、0061（trade-dump shadow 产快照）、0062（终态退场 + 幂等环）、0064（启动按需快照）、0078（perp 订单与持仓产品 API）

## 范围声明（先读这一段）

ADR-0078 为 perp 落地了 amend / batch / cancel-all / pre-check / close-all / admin 操作面之后，现货 Counter 的产品 API 出现不对称。本 ADR 只决策现货侧**概念适用且确实缺失**的两项：

- **AmendOrder**：cancel + new 的服务端编排（取代 ADR-0014 实施约束中已过时的 BFF wrapper 草图，见 §2）。
- **BatchPlaceOrders / BatchCancelOrders**：per-item best-effort 批量（镜像 ADR-0078 §3）。

现货**已有**、本 ADR 不重做的：

- `client_order_id`（COID）幂等：活跃单索引 + 终态幂等环（ADR-0015/0062）——ADR-0078 修订 #3 在 perp 侧补齐的正是它的镜像，batch 的 per-item 幂等前置在现货天然成立。
- cancel-all：`CancelMyOrders(user, symbol?)`（BFF `DELETE /v1/orders`）。
- admin 撤单面（`AdminCancelOrders`）。

显式推迟（不在本 ADR 决策范围内，理由见 §4）：

- pre-check 试算端点。
- 现货 block trade / OTC（双边余额互换结算，业务形态与 perp block trade 不同）。
- batch `atomic=true`（全有或全无）。
- 仓位类操作（close-all / TP-SL position binding / force adjust）：现货无仓位概念，不适用。

## 背景 (Context)

ADR-0014 决策"改单 = 撤单 + 新建、Match 不做 in-place replace"，并在实施约束里给了一个未来扩展草图："BFF 提供 `POST /v1/order/replace`，内部做两次调用，保证结果语义一致（都成功 / 都失败，中间态报错）"。该草图写于异步撤单链路定型之前，在当前架构下不成立：

1. **撤单是异步的**：`CancelOrder` 受理后订单进入 PENDING_CANCEL，终态（CANCELED/FILLED）经 Match 的 trade-event 回流才确定。BFF 同步等终态只能轮询或订阅，两者都不属于 BFF 的无状态边界。
2. **不等终态就下新单 = 双倍冻结 + 双单并存**：现货冻结的是**全额资金**（quote/base，不是 perp 的 IM——initial margin，初始保证金），旧单冻结要到 cancel 终态才释放。BFF 在受理后立刻下新单，用户需要双倍余额撑过重叠窗口，且新旧两张单同时 live 可同时成交。
3. **BFF 无状态 = 崩溃窗口不可恢复**：两次调用之间 BFF 崩溃，旧单已撤、新单未下，没有任何持久化记录把两者关联起来，客户端只能靠超时自救。

ADR-0078 §2 已为 perp 解决了同一问题（counter 侧 conservative 状态机 + 终态事件续单 + 重放收敛），且 perp-counter 的实现验证了该模式。本 ADR 把它移植到现货 Counter，**移植的难点不在状态机本身，而在恢复模型不同**：perp-counter 自产快照（ADR-0048 模型，amend registry 直接进 snapshot.json）；现货 Counter 的快照由 trade-dump shadow 重放 counter-journal 产出（ADR-0061），启动恢复 = shadow 快照 + journal 尾部 catch-up 重放（ADR-0060 §4 / ADR-0064）。因此现货的 amend 挂起状态必须**经由 counter-journal 事件承载**，而不是直接塞进自产快照。

## 决策 (Decision)

### 1. batch：镜像 ADR-0078 §3，逐项全路径

`BatchPlaceOrders` / `BatchCancelOrders` 加在 CounterService 上：

- 每个子项独立走完整的 PlaceOrder / CancelOrder 路径（shape 校验、精度过滤、COID 去重、槽位上限、冻结、派发），按提交顺序执行；per-item result，部分成功正常。
- `batch_id` 是关联/审计键，原样回显；幂等由子项 `client_order_id` 承载（ADR-0015/0062 现成）。
- 单批上限 20 项，超出整批拒绝。子项的 shape 错误（如非法数量）以 per-item reject 返回，不放弃兄弟项。
- 子项 user_id 必须为 0 或等于头部 user_id（fail-closed），路由按头部 user 的 vshard（ADR-0058 `OwnsUser` 不变）。
- 不事务化：行业批量端点（Binance futures batchOrders / OKX batch-orders / Bybit create-batch）的契约即 per-item 独立 + 部分成功，批量的价值是省往返/省签名/限频计费，不是原子性。`atomic=true` 推迟（同 ADR-0078 备选 B 的否决理由）。
- 固定成本摊销（整批一次 sequencer 进入、整批一个 Kafka 事务派发）为后续优化项，不改协议（见未来工作；与 ADR-0078 的 batch 同一结论）。

### 2. amend：Counter 侧 conservative 状态机（取代 ADR-0014 的 BFF wrapper 草图）

沿用 ADR-0014 的核心决策（amend = cancel + new，Match 不做 in-place replace），编排位置从"客户端/BFF 两步"改为 **Counter 内的挂起状态机**，与 ADR-0078 §2 同构：

```text
AmendOrder(user, order_id, new_price, new_qty)
    |
    +-- user sequencer 内校验：归属 / 非终态 / 非 PENDING_CANCEL /
    |   LIMIT 单（市价单含 quote_qty 市价买不可 amend）/
    |   无在途 amend / new_qty > 已成交量
    +-- 预分配 new_order_id（同步返回；重放收敛锚点）
    +-- 登记 pending amend + 发 AmendEvent(REQUESTED) 入 counter-journal
    +-- 对旧单走撤单路径（CancelRequested 入 journal，cancel 派发 Match）
    +-- 返回 {accepted, old_order_id, new_order_id}

旧单终态事件（CANCELED / FILLED / EXPIRED / REJECTED）随 trade-event 回流，
在同一用户的顺序执行上下文内（异步管线的 per-user 全序保证，ADR-0060）：
    |
    +-- remaining = new_qty - 旧单累计成交量
    +-- remaining <= 0:
    |     AmendEvent(ALREADY_FILLED)，不下新单
    +-- remaining > 0:
          以 new_order_id 走完整 PlaceOrder 路径
          （精度过滤 / COID 去重沿用旧单 COID / 槽位上限 / 从 Available
           全额冻结——旧单冻结已在终态释放，资金时序天然不重叠）
            |-- 冻结不足或校验失败: AmendEvent(FAILED, reason)
            +-- 通过: 派发新单，AmendEvent(COMPLETED)
```

语义细则与 ADR-0078 §2 完全一致，不另立规则：`new_qty` 是**新的总意图量**（新单数量 = new_qty − 旧单已成交）；只允许改 price/qty，TIF 等从旧单继承；同一订单同时最多一个在途 amend；显式 `CancelOrder` 中止在途 amend（`ABORTED_BY_CANCEL`）；对 PENDING_CANCEL 的订单拒绝 `cancel_in_progress`。

现货特有的几点：

- **资金时序是 conservative 的最硬理由**：现货冻结全额资金。先等旧单终态（释放冻结）再为新单冻结，改价/改量永远不要求用户临时持有双倍余额。fast-path（撤旧未终态即下新）在现货意味着最坏情况双倍全额预占，价值/成本比 perp 更差，明确不做。
- **槽位上限（ADR-0054）天然不双占**：旧单先终态（释放 MAX_OPEN_LIMIT_ORDERS 槽位）新单才下，amend 永远不会因"新旧并存"触发槽位拒绝，也不会绕过上限。
- **Reservation 单（ADR-0041）**：经预留下的订单（trigger 内单）其 reservation 在原下单时已消费；续单不复用预留，从 Available 冻结，不足则 `FAILED`。
- **COID 继承**：新单继承旧单 `client_order_id`，活跃索引随之指向新单（幂等环里的旧映射被活跃索引遮蔽）——与 ADR-0078 的行为一致。

**持久化与恢复（与 perp 的关键差异）**：

```text
                          (写路径)
AmendOrder RPC ──► AmendEvent(REQUESTED) ──► counter-journal (Kafka, 持久)
                                                  │
            ┌─────────────────────────────────────┼──────────────────────┐
            ▼ (折叠)                               ▼ (尾部重放)            ▼
   trade-dump shadow                       Counter 启动 catch-up      实时下游
   ApplyCounterJournalEvent                ApplyCounterJournalEvent   (push/审计)
   → ShardState.pendingAmends              → 同一函数、同一 registry
   → CounterShardSnapshot.pending_amends
            │                                      │
            └───────────► 启动恢复 = 快照 + catch-up，两层共用一处 apply 扩展
```

- 挂起 amend 的真值载体是 **counter-journal 的新 payload `AmendEvent`**（REQUESTED / COMPLETED / ALREADY_FILLED / FAILED / ABORTED_BY_CANCEL），registry 放进 `counterstate.ShardState`，对应字段加进 `CounterShardSnapshot`。
- `ApplyCounterJournalEvent`（ADR-0061 M1 的共享 apply 函数）扩展一处，**shadow 折叠与启动 catch-up 同时获得重建能力**——FreezeEvent 跨快照边界重建在途订单的先例（ADR-0060）证明该模式成立，amend registry 走同一条路。
- 恢复保证等级与订单本体相同：热路径（ADR-0064 按需快照对齐各分区 LEO——log-end offset，分区末端位移）下，已落 journal 的 amend 必在恢复状态里；冷路径退化为与在途订单相同的 orphan 窗口（ADR-0068 声明的"派发与本地状态非单事务原子"硬化桶，本 ADR 不扩大也不缩小它）。
- **重放收敛**：复用 ADR-0078 修订 #6——`new_order_id` 在请求时预分配并随 journal/快照持久化，崩溃重放重复派发被 Match 以 `REJECT_REASON_DUPLICATE_ORDER_ID` 拒绝，Counter 对**自己跟踪中订单**的该拒因忽略（现货 `handleRejected` 已携带 reason，可直接分支），订单状态由重放流中它自己的生命周期事件收敛。
- 终态续单在异步消费管线（ADR-0060）中执行的不变量：续单的准入 + 冻结 + 派发必须发生在旧单终态已应用之后，并与该用户的后续事件保持全序——即与终态过渡同一个 per-user 顺序执行单元，不得另起无序路径。

### 3. BFF 端点

- `POST /v1/order/amend`（与 perp 的 `/v1/perp/order/amend` 同形）。
- `POST /v1/orders/batch`、`POST /v1/orders/batch-cancel`。
- `DELETE /v1/orders` 维持现状（cancel-all 已存在）。

### 4. 显式推迟项及理由

- **pre-check**：perp pre-check 的价值在保证金数学（IM / 费用缓冲 / 有效杠杆 / 风险档），现货只剩精度校验 + 余额检查——精度规则可由客户端从 instruments 元数据确定性自查，余额检查本质上是 TOCTOU（time-of-check-to-time-of-use，检查与使用之间状态已变的竞态）快照，试算价值低。等出现真实需求（如组合保证金现货联动）再立项。
- **现货 block trade / OTC**：perp block trade（ADR-0078 §8）复用了保证金准入 + fill 结算原语；现货的双边大宗本质是**协议价的双边余额互换**，不进 orderbook 也不产生冻结语义，更接近 asset-service 转账 saga 的双边形态，需要独立的结算与合规决策，挂 OTC 产品立项。
- **atomic batch**：同 ADR-0078 备选 B 否决理由（跨 symbol 多单冻结的预校验与回滚成本高，行业契约也不要求）。

## 备选方案 (Alternatives Considered)

### A. BFF wrapper replace（ADR-0014 未来扩展草图）

否决，三个结构性缺陷见背景节：异步撤单下要么双倍冻结、要么轮询等终态；BFF 无状态导致崩溃窗口无关联记录；"都成功/都失败"的承诺在两次独立 RPC 上无法兑现。ADR-0014 的核心决策（amend = cancel + new）不受影响，本 ADR 取代的只是其实施约束中的该草图。

### B. Match 原生 ReplaceOrder

ADR-0014 已否决（orderbook in-place modify 复杂度、优先级语义争议），维持。

### C. amend fast-path（撤旧未终态即下新 + 最坏情况预占）

现货全额冻结模型下 = 双倍资金预占，比 perp 的 IM 预占代价更高，且 MVP 无高频做市需求（ADR-0014 既有判断）。不做，未来若做必须按 ADR-0078 §2 同款约束（最坏情况预占 + journal 全程可复盘）。

## 影响 (Consequences)

### 正面

- 现货与 perp 产品面对称：amend / batch 两端语义一致，客户端一套心智。
- amend 资金时序无双倍冻结，崩溃可恢复（journal 承载 + 两层重建共用一处 apply）。
- 复用 ADR-0078 已验证的状态机与重放收敛模式，新风险面小。

### 负面 / 代价

- counter-journal 新 payload + `CounterShardSnapshot` 新字段 + shadow/catch-up apply 扩展——动的是恢复链路的共享件，测试矩阵必须覆盖快照折叠与 catch-up 两条重建路径。
- conservative amend 的延迟 = 撤单 Match 往返 + 下单（与 perp 相同的固有代价）。
- batch 不摊销固定成本（N 次 sequencer 进入 / N 个派发事务），延迟在大批量时线性增长（优化路径已知，见未来工作）。

## 实施约束 (Implementation Notes)

- AmendEvent 状态机与 ADR-0078 的 `PerpAmendEvent` 字段对齐（old/new_order_id、new_price、new_qty、state、reason、placed_qty），分区键 user_id。
- 挂起 amend registry：`counterstate.ShardState` 内、随 `CounterShardSnapshot` 持久化、`ApplyCounterJournalEvent` 单点扩展覆盖 shadow 折叠 + catch-up 重放。
- `handleRejected` 增加 `DUPLICATE_ORDER_ID` 对跟踪中订单的忽略分支（注释引用 ADR-0078 修订 #6）。
- batch 上限 20 为常量；子项按提交顺序执行；批内子项间允许其他同用户操作插队（best-effort 契约明示）。
- 单测清单：amend 撤单/成交竞态（部分成交续余量、全部成交 ALREADY_FILLED）、显式撤单中止、PENDING_CANCEL 拒绝、续单冻结不足 FAILED、COID 继承与活跃索引指向、槽位上限不双占、reservation 单续单从 Available 冻结、**快照折叠重建 registry**、**catch-up 重建 registry**、重放 duplicate 拒绝忽略收敛、batch 部分成功、batch 子项 user 不匹配拒绝、vshard 路由。
- BFF 端点复用现货 place/cancel 的 body 解析；amend 响应回显 old/new_order_id。

## 未来工作

- batch 固定成本摊销：整批一次 sequencer 进入 + 整批单 Kafka 事务派发（协议不变，纯实现优化；与 ADR-0078 侧同批处理）。
- Match 终态 order-id 环（ADR-0078 未来工作项）：关闭事件驱动续单的重放二次成交残余窗口，现货 perp 共享。
- pre-check / 现货 OTC（block trade）：按 §4 的触发条件另行立项。
- atomic batch：如产品确需，限同 user + 同 symbol + 小批量，sequencer 内预校验全部冻结。

## 参考 (References)

- [ADR-0014: 改单实现为"撤单 + 新建"](./0014-order-modify-as-cancel-new.md)
- [ADR-0060: Counter 异步消费与 TE checkpoint](./0060-counter-async-consumption-and-te-checkpoint.md)
- [ADR-0061: trade-dump 快照管线](./0061-trade-dump-snapshot-pipeline.md)
- [ADR-0062: 订单终态退场与幂等环](./0062-order-terminal-eviction-and-idempotency-ring.md)
- [ADR-0064: Counter 启动按需快照](./0064-counter-startup-ondemand-snapshot.md)
- [ADR-0078: perp 订单与持仓产品 API](./0078-perp-order-position-product-api.md)
