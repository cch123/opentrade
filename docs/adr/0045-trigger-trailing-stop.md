# ADR-0045: 触发单 Trailing Stop

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0040, 0041, 0042, 0043, 0044

## 背景 (Context)

固定 `stop_price` 的止损单（MVP-14a）要求用户在下单时就决定触发价。行
情朝有利方向走时，用户得手动"抬止损" —— 不及时抬就把已有利润让回去。

Trailing stop 是这个问题的标准解法：引擎维护一个 **watermark**（sell 用
所见最大价，buy 用最小价），实时重算的 `effective_stop` 跟着水印走，
只在价格从水印回撤 `trailing_delta` 时触发。下单时固定的只是"允许回撤
多少 bps"，不是一个绝对价。

BN spot 以 `trailingDelta` 参数的形式把它嫁接到 STOP_LOSS / STOP_LOSS_LIMIT
上。本 ADR 给 trigger 服务增加一个显式的 `TRAILING_STOP_LOSS` 类型，
保留后续演化空间。

## 决策 (Decision)

### 1. 新 `TRIGGER_TYPE_TRAILING_STOP_LOSS = 5`（仅 MARKET）

proto 扩展：

```proto
enum TriggerType {
  ...
  TRIGGER_TYPE_TRAILING_STOP_LOSS = 5;
}

message PlaceTriggerRequest {
  ...
  int32 trailing_delta_bps = 12;          // required for trailing, forbidden otherwise
  string activation_price = 13;           // optional
}

message Trigger {
  ...
  int32 trailing_delta_bps = 19;
  string activation_price = 20;
  string trailing_watermark = 21;         // observable
  bool trailing_active = 22;              // observable
}
```

`stop_price` 对 trailing 类型忽略（引擎算）；如果客户端错传非空值，引擎
返回 `ErrStopPriceForbidden`。

MVP 只做 MARKET 变种（触发后 fire MARKET 单）。LIMIT 变种等以后有需求再
加——那时要决定 "trigger 时 limit_price 怎么算"（跟踪、固定、根据 depth
推导），独立 ADR。

### 2. 引擎逻辑

Trigger 多 4 个可变状态字段：`TrailingDeltaBps`、`ActivationPrice`、
`TrailingWatermark`、`TrailingActive`。

每条 PublicTrade 到达 → 引擎在锁内对所有 trailing 的 pending 触发单跑：

```
updateTrailingLocked(c, lastPrice) -> shouldFire bool
  1. activation gate:
       if !c.TrailingActive {
         if ActivationPrice == 0 → TrailingActive = true
         else if sell: lastPrice >= ActivationPrice → TrailingActive = true
              buy:  lastPrice <= ActivationPrice → TrailingActive = true
         if !TrailingActive: return false
       }
  2. watermark:
       if sell: watermark = max(watermark, lastPrice)
       if buy:  watermark = min(watermark, lastPrice)  (init 0 treated as "first time")
  3. effective stop:
       delta = watermark × TrailingDeltaBps / 10000
       sell: stop = watermark - delta; fire if lastPrice <= stop
       buy:  stop = watermark + delta; fire if lastPrice >= stop
```

重要：`watermark / TrailingActive` 是 mutable 内存状态，跟着每笔 trade
推进。因为写在引擎锁内，和 Place / Cancel 互斥；snapshot Capture 也拿
同一把锁，因此 graceful restart 会读到最新 watermark 继续追踪。

### 3. Reservation 不变

Reserve 在 Place 时按 `(qty 或 quote_qty)` 计算，和普通 MARKET
STOP_LOSS 一致（ADR-0041）。触发时 `PlaceOrder(reservation_id, MARKET)`
消费 reservation。trailing 的动态部分只在 "何时触发"，不影响 "触发时要
freeze 多少"。

### 4. 和 OCO / 过期 / HA 的交互

- **OCO**：trailing 腿可以当作 OCO 的一腿。触发走一样的 cascade，兄弟
  腿被 CANCELED。已覆盖。
- **过期**：TTL 过了还没触发 → EXPIRED + release reservation。引擎 sweep
  不关心腿类型。已覆盖。
- **HA**：watermark 在 snapshot 里；primary 切换后 backup 拿到最新 water
  mark，接着消费 market-data。已覆盖。

## 备选方案 (Alternatives Considered)

### A. 复用 STOP_LOSS + `trailing_delta_bps` 为可选字段
- BN 的做法
- 优点：proto 更平；STOP_LOSS 的 LIMIT 变种自动有 trailing 语义
- 缺点：类型枚举本身就承担"这是什么交易行为"的语义，拆开更易读；
  validator 里写"if trailing_delta_bps > 0 then ignore stop_price"也容
  易让人 miss
- 拒绝：新类型更直观

### B. 支持 trailing TAKE_PROFIT
- Take profit 的语义是"价格到达目标" —— 本质上就是 trailing 反向。trailing
  stop-loss 已经覆盖了 "价格朝不利方向回撤即平" 的所有场景。真需要 take
  profit 就是固定价位 TAKE_PROFIT 或一个反向 trailing
- 拒绝：类型爆炸没收益

### C. trailing_delta 用绝对价格
- BN 的 `trailingDelta` 同时支持 bps 和绝对。两种 unit 混用（看字段就能
  识别）增加文档负担
- 统一 bps 更简单：用户按百分比思考回撤比按 "美元" 更自然
- 拒绝

### D. 引擎每 N 秒 recompute watermark（poll 模式）
- 如果引擎没实时响应每笔 PublicTrade，可以定期扫
- 缺点：增加延迟；在剧烈波动时可能错过更深的 high/low
- 拒绝：既有 HandleRecord 已经实时，没有理由改

### E. Watermark 完全在客户端，把 effective_stop 每次重设过来
- 最简实现（服务端零状态）
- 缺点：客户端必须 always-on；断线期间 watermark 不前进
- 拒绝：trailing 的全部价值就是服务端持续追踪

## 理由 (Rationale)

- **行为透明**：`trailing_watermark` + `trailing_active` 两个字段直接
  暴露，客户端 UI 能画水印线
- **snapshot 天然覆盖**：watermark 是普通字段；ADR-0036 同款 snapshot
  路径承担了持久化
- **锁内原子**：updateTrailingLocked 是纯内存操作 + 比较；和 Cancel /
  PlaceOCO 互斥；watermark 不会跨 trade 漏更新
- **和其它 trailing 无歧义**：只一个类型 + MARKET，后续加变种不会冲突

## 影响 (Consequences)

### 正面

- 最常用的专业止损模式落地；交易员不用手动移止损
- 和 OCO / expiry / reservation 正交，四套特性能自由组合
- 从客户端角度看就是多一个 `type="trailing_stop_loss"` + 两个字段，
  零迁移成本

### 负面 / 代价

- 引擎每笔 PublicTrade 对每个 trailing pending 都要 update；千级 trailing
  pending + 万级 trades/s 时轻微 CPU 增加。当前规模可以忽略
- watermark 持续变动增大 snapshot 文件的"脏行" 比例，可能让 diff-based
  备份（未来做）传输量上升；bps 够用，很少在小数点后变动，实际问题不大
- MVP 只支持 MARKET。用户想要"触发后下 LIMIT" 要用固定 stop_loss_limit
  + 手动抬。documented

### 中性

- `ErrStopPriceForbidden`：客户端传了 stop_price 但 type=trailing → 拒
  绝。避免用户"以为两者结合"的误解

## 实施约束 (Implementation Notes)

### 代码改动

- `api/rpc/trigger/trigger.proto`：
  - Enum 加 `TRIGGER_TYPE_TRAILING_STOP_LOSS = 5`
  - PlaceTriggerRequest 加 `trailing_delta_bps` / `activation_price`
  - Trigger 加 `trailing_delta_bps` / `activation_price` /
    `trailing_watermark` / `trailing_active`
- `trigger/internal/engine/engine.go`：
  - Trigger 4 个字段
  - buildTrigger：trailing 分支 + 5 个新 err
  - handleLocked 分叉：trailing 走 `updateTrailingLocked`，其它走
    `ShouldFire`
  - `updateTrailingLocked` 的 3 步：activation → watermark → stop check
- wire.go：ToProto 出口带 4 字段
- snapshot/snapshot.go：TriggerSnap 加 4 字段；Capture / Restore
  覆盖；parse 错误带 id 上下文
- server/server.go：5 个新 err 映射到 InvalidArgument
- bff/internal/rest/trigger.go：
  - `placeTriggerBody` 加 `trailing_delta_bps` / `activation_price`
  - parseTriggerType / triggerTypeLabel 加 `trailing_stop_loss`
  - Place handler + OCO leg 都透传
  - triggerToJSON 暴露四个字段 + oco_group_id

### 失败场景

| 场景 | 结果 |
|---|---|
| trailing type 但 delta ≤ 0 | 400 ErrTrailingDeltaNeeded |
| trailing type 但 delta > 10000 | 400 ErrTrailingDeltaRange |
| non-trailing type 带 delta != 0 | 400 ErrTrailingDeltaForbidden |
| non-trailing type 带 activation_price | 400 ErrActivationPriceShape |
| trailing type 带 non-zero stop_price | 400 ErrStopPriceForbidden |
| activation_price 从未达到 | 永远不触发；到 expiry 走 EXPIRED |
| 价格在 activation 之前回撤到 watermark-delta 的水平 | 因为 activation 没满足，不 fire（trailing 语义：激活才开始追踪） |
| snapshot JSON 有损坏的 activation_price / watermark | Restore 带 id 上下文返回 err，启动失败 |

### 测试覆盖

- 7 个新测试：delta required / range / forbidden for non-trailing /
  stop_price forbidden + sell watermark→fire / buy watermark→fire /
  activation gate + fire

## 未来工作 (Future Work)

- **LIMIT 变种**：`TRAILING_STOP_LOSS_LIMIT` —— 触发时 limit_price 怎么
  选（固定 offset、depth-aware、orderbook best-bid）需要独立决策
- **Absolute delta**：允许 `trailing_delta_abs` 作为 bps 的补充。BN
  pattern
- **Peak / trough 指标推送**：把 `trailing_watermark` 通过 Push 下发
  给 UI，让用户看到动态止损线
- **Trailing take profit**：等需求明确再加
- **Multi-currency watermark**：OCO 里 trailing + 非 trailing 混编时
  cascade 逻辑已经覆盖；真需要多条 trailing 一起跑也已可行
  （`OCO + N × trailing`）

## 参考 (References)

- [Binance Spot API — Trailing Delta](https://binance-docs.github.io/apidocs/spot/en/#trailing-stop-faq)
- ADR-0040: Trigger 服务整体设计
- ADR-0041: Reservations（Reserve 按 qty/quote_qty 算，不依赖 trigger 价）
- ADR-0043 / 0044: expiry / OCO 都和 trailing 正交
- 实现：
  - [api/rpc/trigger/trigger.proto](../../api/rpc/trigger/trigger.proto)
  - [trigger/internal/engine/engine.go](../../trigger/internal/engine/engine.go)
  - [bff/internal/rest/trigger.go](../../bff/internal/rest/trigger.go)
