# ADR-0024: 扩展 trade-event.OrderAccepted 以支持行情重建

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0008, 0019, 0021

## 背景 (Context)

ADR-0021 规定 Quote 消费 `trade-event` 重建 orderbook 并输出 DepthUpdate/DepthSnapshot。
MVP-6 动工时发现原始 `OrderAccepted` payload 只含 `user_id / order_id / symbol`：

```proto
message OrderAccepted {
  string user_id = 1;
  uint64 order_id = 2;
  string symbol = 3;
}
```

Quote 无法从这个事件知道这笔挂单"放在哪个 price level 上 / 还剩多少 qty /
哪一侧"。因此 depth 投影无法实现。

Match 内部 `sequencer.Output`（[match/internal/sequencer/event.go](../../match/internal/sequencer/event.go)）
在 emit `OutputOrderAccepted` 时已经知道这些字段 —— 它们只是没被写进 proto。

## 决策 (Decision)

给 `OrderAccepted` proto 增加三个字段，以 field number 10/11/12 扩展（避免
回退原有的 1/2/3）：

```proto
message OrderAccepted {
  string user_id = 1;
  uint64 order_id = 2;
  string symbol = 3;

  Side side = 10;            // 挂单侧（bid/ask）
  string price = 11;         // 挂单价（decimal）
  string remaining_qty = 12; // 挂单进入 book 后仍然活着的数量
}
```

并相应修改：

- `match/internal/sequencer/Output` 的字段说明：`Price` 和 `TakerRemaining`
  在 `OutputOrderAccepted` 上也被使用（语义为 "resting price / resting qty"）。
- `match/internal/sequencer/worker.go` 的 Accepted emit 填入 `Price: o.Price`、
  `TakerRemaining: o.Remaining`。
- `match/internal/journal/convert.go` 的 `OutputToTradeEvent` 的
  `OutputOrderAccepted` 分支填入三个新字段。

## 备选方案 (Alternatives Considered)

### A. 新增一个独立事件 `OrderRested`
- 优点：proto 语义更分层（Accepted = "match 受理"、Rested = "进 book"）。
- 缺点：新增 payload 和 state 机器增加；所有消费者都要更新 switch。
- 结论：收益不值得成本，单事件足够。

### B. 让 Quote 直接消费 `order-event`（counter → match）
- 优点：`OrderPlaced` payload 已经有完整 price/qty/side。
- 缺点：
  - `order-event` 是 Match 的 *输入*，里面包含已被 STP / post-only /
    duplicate id 拒绝的单；Quote 得重新实现这套过滤逻辑才能对齐 book，
    等于在 Quote 里重跑一遍撮合。
  - 多消费一个 topic 增加运维面。
- 结论：不值得。

### C. 把字段放 `EventMeta` 或其他地方
- 不考虑，强耦合。

## 理由 (Rationale)

- `OrderAccepted` 的语义本身就是 "order rests on the book"，price/qty/side 是该事件
  与生俱来的上下文；原 proto 遗漏属于建模失误。
- 字段 10/11/12 留后 gap 给后续扩展（如果将来要加 tif / order_type），proto
  wire format 向后兼容（新字段默认零值，旧消费者忽略）。
- Match 侧改动只触及两个文件，风险低；所有下游（Counter 的 trade consumer /
  trade-dump）此前都不读这些字段，也无需同步改。

## 影响 (Consequences)

### 正面

- Quote 可以从 trade-event 单流重建 orderbook，无需额外消费 order-event。
- `OrderAccepted` 事件自描述，未来新消费者（例如风控实时监控）能直接用。

### 负面 / 代价

- 事件体积略增（~30 字节/event，limit order 数量级）。可接受。
- 旧 trade-event 历史（MVP-5 之前产出的）里 OrderAccepted 没有这些字段。
  Quote 重启时重放会得到 price="" / remaining_qty="" 的记录；depth 代码需要
  对零值 price / 非正 remaining 做 no-op 处理（已实现，见
  [quote/internal/depth/depth.go](../../quote/internal/depth/depth.go) 对
  `dec.IsPositive` 的检查）。

### 中性

- 订单 **market 单** 从不 rest，从不 emit OrderAccepted —— 新字段对 market 单不适用。

## 实施约束 (Implementation Notes)

- wire-compatible change：字段号 10-12 新增，旧客户端忽略。
- `remaining_qty` 语义是 "静止后仍在 book 的量"，**不是原始下单量**。对 taker
  先部分成交、剩余 rest 的 `TakerPartialOnBook` 场景，它等于 `o.Qty -
  已成交量`。
- 对"零 remaining 进 book"这种矛盾输入（理论不存在），depth 代码按 no-op
  忽略，不 panic。
- 测试：[match/internal/journal/convert_test.go](../../match/internal/journal/convert_test.go)
  的 `TestOutputToTradeEvent_Accepted` 覆盖了新字段的 wire 序列化。

## 参考 (References)

- ADR-0019: Match Sequencer
- ADR-0021: Quote 服务与市场数据扇出
- 实现：[api/event/trade_event.proto](../../api/event/trade_event.proto)、
  [match/internal/sequencer/worker.go](../../match/internal/sequencer/worker.go)、
  [match/internal/journal/convert.go](../../match/internal/journal/convert.go)
