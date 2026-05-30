# ADR-0072: 强平扫描改为强平价排序索引 —— 阈值穿越查询取代每 tick 全量评估

- 状态: **Proposed**（2026-05-30 起草；从 ADR-0070 强平扫描的实现成本问题独立成 ADR）
- 日期: 2026-05-30
- 决策者: xargin, Claude
- 相关 ADR: 0068（USDT 线性 perp，§5 mark tick、§8 强平判定）、0070（强平进阶：阶梯 MMR / liq_price 求解 / 在途 guard / LiquidationCheck 复核）、0071（perp-counter 按 user 分片，扫描是 per-shard 行为）、0069（index_stale → 跳过扫描）、0048（snapshot 绑 offset；本 ADR 的索引为派生态、不入 snapshot）、0053（symbol 精度 / tick 标度）

## 范围声明（先读这一段）

本 ADR **只改强平判定的扫描数据结构**，不动强平的语义、不动服务拓扑、不新增服务：

- 现状（[ADR-0068 §8](./0068-usdt-linear-perp.md)，代码 `perp-counter/internal/engine/engine.go:389` `LiquidatablePositions`）：每个 mark tick 对一个 symbol 的**全部仓位**做一次 O(N) 的保证金率评估（`dec.Decimal` 运算），且**全程持 `engine.mu` 读锁**。
- 本 ADR：改为**维护一个按 liq_price（强平价）排序的索引**，mark tick 时只做一次**阈值穿越查询（threshold-crossing query）**——返回被新 mark 越过强平价的那批仓位，复杂度 O(log N + k)（k = 当前越线仓位数，通常接近 0）。索引在仓位变化时增量更新（O(log N)/次）。
- **强平的语义全部不变**：判定仍走 mark price、候选仍进 per-user sequencer 用 `LiquidationCheck` 做 TOCTOU（time-of-check-to-time-of-use，检查与使用之间状态已变的竞态）复核、在途 guard 不变、阶梯 MMR（maintenance margin ratio，维持保证金率）/ 部分强平 / backstop / ADL 全部不动。索引**只是把"找出哪些仓位该平"加速**，不改"找到后怎么平"。
- 仍是 perp-counter 内的事（[ADR-0071](./0071-perp-sharded-insurance-and-cross-shard-adl.md)：检测留在 shard），索引是 **per-shard、per-symbol** 的。

OpenTrade 未上线，breaking change 直接改（同 [ADR-0057](./0057-asset-service-and-transfer-saga.md)）。

## 术语 (Glossary)

| 本 ADR 字段 | 含义 | 业界对标（仅供映射） |
|---|---|---|
| `liq_price`（强平价） | 仓位 margin_ratio 触及 MMR 的 mark 价（标量，仅随仓位变化而变） | BN `liquidationPrice` / Bybit `LiqPriceX` |
| `强平价索引`（liq-price index） | per-(symbol, side) 按 liq_price 排序的有序结构，支持 O(log N) 增删改 + 阈值范围查询 | Bybit Trigger Engine 的 per-symbol open-position cache |
| `阈值穿越查询`（threshold-crossing query） | 给定当前 mark，返回越过强平价的仓位集合（多仓：liq_price ≥ mark；空仓：liq_price ≤ mark） | 各家"价格触发"撮合式风控 |
| `派生态`（derived state） | 可从权威态（仓位）重算的缓存；不入 snapshot，重启时重建 | 物化视图 / secondary index |

## 背景 (Context)

### 现状（已落地，2026-05-30 核实代码）

`engine.go:389 LiquidatablePositions(symbol, mmr)`：

```go
func (e *Engine) LiquidatablePositions(symbol string, mmr dec.Decimal) []LiquidationCandidate {
    e.mu.RLock(); defer e.mu.RUnlock()        // 整轮扫描持读锁
    for user, bySym := range e.positions {    // O(N): 遍历该 symbol 全部仓位
        if perpstate.Isolated(p).Liquidatable(marks, mmr) { ... }  // 每仓 decimal 保证金运算
    }
    sort.Slice(out, ...)                       // 候选排序
}
```

[ADR-0068 §8](./0068-usdt-linear-perp.md) 的 mark tick 链路：markprice 发 `MarkPriceEvent` → perp-counter `HandleMarkPriceEvent` → `SetMark` → `onMarkTick` → `scanLiquidations` → 上面这个全量评估。

### 为什么现在做 —— 三个真实成本

1. **CPU：每 tick O(N) decimal 评估**。`shopspring/decimal`（[ADR-0013](./0013-tech-stack-choices.md)）每次运算堆分配 `big.Int`。在 10^5–10^6 仓位量级，单次扫描就吃满一个核，并持续制造 GC 压力。mark tick 是 1s 级（[ADR-0069](./0069-external-composite-index-price.md)），但乘以仓位数后是稳态的高频开销。
2. **锁竞争：读锁覆盖整轮扫描**。`e.mu.RLock()` 持有到 O(N) 循环结束，期间所有写者（fill / 加减保证金 / 资金费结算，都要 `e.mu.Lock()`）被阻塞。**强平扫描直接拖累下单与成交结算的延迟**——这是比 CPU 更严重的问题。
3. **随分片放大但不消失**：[ADR-0071](./0071-perp-sharded-insurance-and-cross-shard-adl.md) 下扫描是 per-shard，每 shard 只扫 owned 用户；但每 shard 仍是 O(N_shard)/tick。分片摊薄了常数，没改变阶。

### 结构前提 —— liq_price 已是标量

`pkg/perpstate/margin.go:41 LiqPrice(mmr)` 已能把一个逐仓仓位的强平价算成**一个标量**：它只随仓位输入（size / 开仓价 / 分配保证金 / 所在 MMR 档）变化，**与 mark 无关**。这正是可建索引的前提——把"每 tick 对所有仓位算 health"换成"仓位变化时算一次 liq_price 入索引，每 tick 只查被 mark 越线的那几个"。

### 参考实现（已读源）

bybit 的 Trigger Engine 正是这么做的（见 `trading_service/internal/mod/trigger/triggercore/x2_sync_position_change.go`）：维护 per-symbol 的开仓缓存，仓位变化时同步进缓存并**预存 `LiqPriceX`**（结构类型名 `OpenPositionRiskyIsLess` 暗示按风险有序），价格 tick 时用 `CheckWillTriggerLiq(size, side, LiqPriceX, triggerSrcPrice)` 判穿越——**预计算 + 索引，而非每 tick 全量重算**。本 ADR 把同一模式落到 perp-counter 的 engine 里。

## 决策 (Decision)

### 1. 数据结构：per-(symbol, side) 的强平价有序索引

engine 内为每个 symbol 维护两个按 liq_price 排序的有序结构：

```
            多仓 (long)                          空仓 (short)
   liq_price 降序                         liq_price 升序
   ┌──────────────────────┐              ┌──────────────────────┐
   │ liq=105  user A       │  mark 下跌    │ liq= 95  user D       │  mark 上涨
   │ liq=103  user B       │  越过这些    │ liq= 97  user E       │  越过这些
   │ liq=100 ◄── mark=100  │  即触发      │ liq=100 ◄── mark=100  │  即触发
   │ liq= 98  user C       │              │ liq=102  user F       │
   └──────────────────────┘              └──────────────────────┘
   多仓: liq_price ≥ mark 即越线          空仓: liq_price ≤ mark 即越线
```

- key = liq_price（建议标度成 int64，按 symbol tick 取整，[ADR-0053](./0053-symbol-precision-and-tiered-evolution.md)，比较无 decimal 分配），tie-break 用 userID 保证确定性。
- value = 定位信息（userID / side / size），够 `beginLiquidation` 用即可。
- 逐仓下每个 `(user, symbol)` 只有一个仓位（[ADR-0070 §备选方案 D](./0070-perp-liquidation-hardening.md) 无 hedge），故每个仓位只落多仓或空仓一侧；side 翻转（仓位过零）时换边。
- 选型：有序结构需 O(log N) 增删改 + 范围查询——B-tree（如 `google/btree`）或 skip list 皆可（见 §备选方案 C）。

### 2. 写路径：仓位变化时增量更新索引（与仓位 mutation 原子）

凡是改变 liq_price 的 mutation，都在**同一个 `engine.mu` 写临界区内**顺带更新索引：

```
开仓 / fill / 部分强平 / 加减保证金 / 资金费结算 / 接管：
   e.mu.Lock()
     改仓位 (size/margin/realizedPnL...)
     新 liq = Position.LiqPrice(mmrOf(notional))   // 阶梯 MMR 求解, ADR-0070
     索引.upsert(symbol, side, 旧liq→新liq, user)    // O(log N); flat 则 remove; 翻边则换侧
   e.mu.Unlock()
```

- **资金费结算**会同时改一个 symbol 全部仓位的保证金 → 全员 liq_price 变 → 该 symbol 索引整体刷新。但这是每个 funding interval（[ADR-0068](./0068-usdt-linear-perp.md) 默认 8h）一次的 O(N log N)，不是每 tick，可接受（见 §影响）。
- 阶梯 MMR（[ADR-0070](./0070-perp-liquidation-hardening.md)）：liq_price 的求解含分档，复杂度一次性付在"仓位变化"上，不在每 tick。

### 3. 读路径：mark tick 做阈值穿越查询（取代全量循环）

`LiquidatablePositions` 改为：

```
onMarkTick(symbol):
   e.mu.RLock()
     多仓候选 = 索引.long[symbol].rangeGreaterEqual(mark)   // O(log N + k)
     空仓候选 = 索引.short[symbol].rangeLessEqual(mark)
     拷出候选定位信息
   e.mu.RUnlock()                                          // 临界区从 O(N) 缩到 O(log N + k)
   for cand in 候选:  beginLiquidation(cand)               // 仍进 per-user sequencer + LiquidationCheck 复核
```

- 临界区只在范围查询 + 拷贝小候选集期间持有，**不再阻塞 fill 一整轮**。
- 后续完全沿用 ADR-0070：`beginLiquidation` 进 `seq.do(user)`，`LiquidationCheck` 复核、在途 guard 去重、部分/整仓/backstop/ADL 决策树不动。

### 4. mark 跳变（gap）由"每 tick 查当前越线全集"自然覆盖

每 tick 查的是"liq_price 当前被 mark 越过的**全集**"（多仓 liq_price ≥ mark），不是"较上 tick 新增的增量"。所以 mark 从 100 直接跳到 80 时，[80,∞) 区间的多仓**一次全部命中**，不会因为跳变而漏。已在途强平的仓位由现有在途 guard（ADR-0070）跳过，避免重复派发。

## 备选方案 (Alternatives Considered)

### A. 维持每 tick 全量评估（现状）
实现最简单，但 O(N) decimal + 全程持读锁，CPU 与锁竞争随仓位数线性恶化。**否决**。

### B. 强平价排序索引 + 阈值穿越查询（选）
O(log N + k)/tick，临界区极短，预计算 liq_price 复用现成 `LiqPrice`。代价是新增"索引与仓位一致"的不变量与测试面。**选**。

### C. 价格分桶（coarse bucket）vs 有序树/skip list
- 分桶（按价格区间散列）：实现简单，但桶粒度与热点分布难权衡，跳变时要扫多个桶。
- 有序树 / skip list（选）：范围查询天然、跳变一次 range 搞定、增删改 O(log N)。**选有序结构**。
- 排序切片（sorted slice）+ 二分：查询 O(log N) 但增删要 O(N) 搬移，高频仓位变化下劣化。**否决**。

### D. 把扫描移到独立清结算/风控服务
本轮讨论已否决（详见 [ADR-0071 §备选方案 D](./0071-perp-sharded-insurance-and-cross-shard-adl.md) 与对话结论）：扫描 = `f(仓位 × mark)`，仓位权威在 perp-counter；独立扫描服务必须持有仓位副本并与权威同步，引入滞后与一致性负担（unimargin liquidate-server 反例），且**并不减少扫描计算量**，只是搬运 + 加同步。CPU 问题的正解是 perp-counter 内换数据结构（本 ADR），不是拆服务。

## 理由 (Rationale)

1. **正解是数据结构而非拓扑**：瓶颈是"每 tick 全量重算"，把它变成"变化时算一次、每 tick 只查越线者"即根除；拆服务既不降本又添一致性负担。
2. **复用现成标量**：`LiqPrice` 已存在，liq_price 与 mark 无关、只随仓位变，天然适合做有序索引 key。
3. **锁竞争是更大收益**：读临界区从 O(N) 缩到 O(log N + k)，强平扫描不再周期性阻塞下单/成交结算。
4. **语义零变更、风险可控**：索引只加速"找候选"，权威判定仍是 sequencer 内的 `LiquidationCheck` 复核——索引错误至多导致一次多余复核（false positive 无害），真正要守的是"不漏"（见不变量 #20）。
5. **派生态、可重建**：索引不入 snapshot，重启从仓位重建，[ADR-0048](./0048-snapshot-offset-atomicity.md) 的快照模型不动。
6. **有生产先例**：bybit Trigger Engine 同款（预存 LiqPriceX + per-symbol 有序缓存 + 价格穿越判定）。

## 影响 (Consequences)

### 正面
- 每 tick 扫描 O(N) → O(log N + k)，消除每 tick 的 decimal 分配与 GC 压力。
- 读临界区大幅缩短，强平扫描不再阻塞 fill/结算。
- 支撑单 shard 大仓位量级，分片只需横向扩。

### 负面 / 代价
- **新增"索引 ⇄ 仓位一致"的正确性面**：每个改 liq_price 的 mutation 都要原子更新索引（不变量 #18）。这里的 bug 若是 **false negative（越线仓位没进查询结果）= 漏强平**，而 sequencer 的 `LiquidationCheck` 复核**只拦 false positive、拦不住 false negative**（它只复核已浮现的候选）。故"不漏"（#20）是安全攸关、必须重点测的不变量。
- **资金费结算的整体刷新**：每 funding interval 一次 O(N log N) 重算该 symbol 全员 liq_price + 刷新索引；远低于每 tick O(N)，但要确保这次刷新本身不长时间持写锁（可分批）。
- **cross-margin（全仓，future）不适用 per-symbol 标量索引**：全仓 health 是账户级、依赖多个 symbol 的 mark，没有单仓阈值。届时全仓账户走**账户级增量 health**（落在 collateral pool 抽象，[ADR-0070](./0070-perp-liquidation-hardening.md)/[0071](./0071-perp-sharded-insurance-and-cross-shard-adl.md) 的 pool 接口），与本 ADR 的逐仓索引并存。**本 ADR 仅覆盖逐仓**。

### 中性
- 仍每 tick 触发、仍 mark price 判定、仍 per-shard；只是"找候选"的内部实现变了。
- index_stale（[ADR-0069](./0069-external-composite-index-price.md)）时整段查询跳过，与现状一致。

## 实施约束 (Implementation Notes)

### 落地要点
- `pkg/perpstate`：`LiqPrice` 已具备；补 liq_price → int64 标度（按 symbol tick）的转换，供索引 key。
- `engine`：新增 `liqIndex`（`map[symbol]*sideIndex`，sideIndex 含 long/short 两棵有序结构）；在所有改仓 mutation（`ApplyFill` / `ApplyLiquidationFill` / 加减保证金 / `settleFunding` / 部分强平 / 接管）末尾、同一写锁内 upsert/remove/换边。
- `LiquidatablePositions`：函数签名可不变，函数体从全量循环改为对 `liqIndex` 的双侧范围查询。
- 启动/恢复：snapshot restore 后由 `e.positions` 重建 `liqIndex`（派生态，不入 snapshot，不变量 #21）。
- 不动：`scanLiquidations` 之后的一切（`beginLiquidation` / `LiquidationCheck` / 在途 guard / 决策树）。

### 关键不变量（落地时逐条 audit，承接既有编号）
18. **索引-仓位原子一致**：任何改变 liq_price 的仓位 mutation，必须在同一 `engine.mu` 写临界区内更新索引；锁释放时索引必与仓位一致（类比 [ADR-0048](./0048-snapshot-offset-atomicity.md) 的快照-offset 原子绑定）。
19. **索引是加速器、非权威**：阈值查询给出的候选，一律由 per-user sequencer 内的 `LiquidationCheck` 复核后才平（TOCTOU 复核，沿用 [ADR-0070](./0070-perp-liquidation-hardening.md)）。索引导致的 false positive 至多多一次复核，无害。
20. **不漏（completeness，安全攸关）**：在 mark = M 的查询必须返回**所有**满足越线条件（多仓 liq_price ≥ M / 空仓 liq_price ≤ M）的非 flat、未在途仓位。任何 false negative = 漏强平，且复核机制拦不住——这是本 ADR 最高优先级的测试目标。
21. **派生态可重建**：`liqIndex` 不入 snapshot，restore 后从仓位重建。
22. **跳变全覆盖**：查询基于"当前越线全集"而非"较上 tick 增量"，mark 任意跳变都不漏（§决策 4）。

### 测试要点
- **差分测试（核心）**：保留旧的全量 `Liquidatable` 评估作为 oracle；property-based 随机构造仓位集 + 随机 mark，断言 `索引查询结果 == 全量扫描结果`（验证 #20 不漏）。随机穿插 fill/加减保证金/funding/部分平/翻边，每步后再断言一致（验证 #18）。
- 单测：liq_price 标度边界、阶梯 MMR 跨档后 liq_price 变化入索引、side 翻转换边、flat 移除、mark 大跳变一次命中区间全集、在途 guard 去重。
- race（`go test -race`）：并发 fill（写锁更新索引）与 mark tick（读锁查询），验证无数据竞争、无漏无重。
- 基准：N = 10^4 / 10^5 / 10^6 下，每 tick 查询耗时与分配数 vs 现状全量；funding 刷新耗时与写锁持有时长。

### 附录：ASCII 流程图

#### 图 1 — 写路径（仓位变化 → 同写锁内更新索引，原子）

```
fill / 加减保证金 / funding / 部分平 / 接管
        │
        ▼  e.mu.Lock()
   改仓位 → 新 liq = LiqPrice(mmrOf(notional))   // 阶梯 MMR, ADR-0070
   liqIndex.upsert(symbol, side, 旧liq→新liq, user)   // O(log N); flat→remove; 翻边→换侧
        │  e.mu.Unlock()        ← 索引与仓位在锁内同时落定（不变量 #18）
        ▼
```

#### 图 2 — 读路径（mark tick → 阈值穿越查询 → 复核执行）

```
markprice ──MarkTick──► perp-counter shard
   SetMark(symbol, mark)
   onMarkTick(symbol):
     e.mu.RLock()                                          ← 短临界区 O(log N + k)
       long  候选 = liqIndex.long[symbol].range(≥ mark)
       short 候选 = liqIndex.short[symbol].range(≤ mark)
     e.mu.RUnlock()
        │
        ▼  for 候选:  (沿用 ADR-0070, 不变)
     seq.do(user){ LiquidationCheck 复核(仍越线?) → 部分/整仓/backstop → ADL }
        │
        └─ 在途 guard 去重; index_stale(ADR-0069) 时整段跳过
```

## 开放问题 (Open Questions)

- **有序结构选型**：`google/btree` vs 自研 skip list vs 其他；并发读写下的实现复杂度与基准对比（MVP 可先在 `engine.mu` 保护下用单线程结构，不必并发结构）。
- **funding 刷新的写锁占用**：整体刷新是否需要分批 / 增量，避免一次长写锁；或把 funding 对 liq_price 的影响表达成可批量平移的偏移。
- **liq_price 标度精度**：int64 标度的取整方向是否影响"恰好等于"边界的判定（应保守取整，宁可多复核不可漏）。
- **cross-margin 账户级 health 结构**：全仓的账户级增量 health 是另一套结构（依赖多 mark），单独 ADR；本 ADR 只逐仓。

## 参考 (References)

- [ADR-0068](./0068-usdt-linear-perp.md) — §5 mark tick 链路、§8 强平判定（本 ADR 改其扫描实现）。
- [ADR-0070](./0070-perp-liquidation-hardening.md) — 阶梯 MMR / liq_price 求解 / 在途 guard / `LiquidationCheck` 复核（本 ADR 全部沿用，只换"找候选"）。
- [ADR-0071](./0071-perp-sharded-insurance-and-cross-shard-adl.md) — 检测留在 shard（索引是 per-shard）；§备选方案 D 论证不拆扫描服务。
- [ADR-0069](./0069-external-composite-index-price.md) — index_stale 时跳过扫描。
- [ADR-0048](./0048-snapshot-offset-atomicity.md) — 快照模型（索引为派生态、不入快照）。
- 代码现状：`perp-counter/internal/engine/engine.go:389`（`LiquidatablePositions` 全量评估）、`pkg/perpstate/margin.go:41`（`LiqPrice`）、`pkg/perpstate/pool.go:56`（`Liquidatable`）、`perp-counter/internal/service/{mark_price.go,liquidation.go}`（tick → scan → beginLiquidation）。
- 参考实现：bybit `trading_service/internal/mod/trigger/triggercore/x2_sync_position_change.go`（per-symbol 开仓缓存 + 预存 `LiqPriceX` + `CheckWillTriggerLiq` 价格穿越；类型 `OpenPositionRiskyIsLess`）。
- 实现位置（动工时涉及）：`pkg/perpstate/`（liq_price 标度）、`perp-counter/internal/engine/`（liqIndex + mutation 钩子 + 查询）、`perp-counter/internal/service/liquidation.go`（`scanLiquidations` 改查索引）。
