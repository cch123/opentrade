# ADR-0014: 改单实现为"撤单 + 新建"

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0016, 0020

## 背景

支持改单（modify order）有两种实现方式：
1. Match 原生支持 `ReplaceOrder`，保留原订单优先级
2. 客户端自行撤单 + 新建，失去价格时间优先级

## 决策

**改单实现为"撤单 + 新建"**：
- 不提供 `ReplaceOrder` RPC
- 客户端（或 BFF 封装一个便捷 API）先调 `CancelOrder`，再调 `PlaceOrder`
- 新订单获得新的 order_id 和新的价格时间优先级

## 备选方案

### 方案 A：Match 原生 ReplaceOrder
- 优点：
  - 保留原订单优先级（对价格不变、仅改数量的场景有意义）
  - Post-Only 单用户修改数量后仍保持优先级
- 缺点：
  - Match 逻辑复杂度上升（orderbook 需要支持"in-place modify"）
  - 保留优先级的语义边界复杂（改价格必须丢优先级；仅减量可保留；加量视为新单）
  - 业界规则不统一，易产生争议

### 方案 B（选中）：撤 + 新建
- 优点：
  - Match 实现极简
  - 语义清晰无歧义
  - 符合多数主流所 REST API 行为
- 缺点：
  - 频繁改单的做市商失去优先级优势
  - 客户端需要处理"撤单成功但新建失败"的中间态

## 理由

- **Match 简化**：撮合引擎是热路径，越简单越易保证正确性和性能
- **MVP 范围**：高频做市商不是 MVP 的目标用户
- **语义清晰**：用户容易理解"改单 = 取消 + 重下"
- **符合业界做法**：Binance、OKX 的 REST API 都要求撤单 + 新建（部分交易所提供辅助 `cancelReplace` 端点，本质还是两步）

## 影响

### 正面
- Match 不需要支持 modify 逻辑
- 订单状态机简化（无"modified"状态）
- 审计清晰（两条独立记录）

### 负面
- 做市商可能流失（优先级丢失）
- 客户端需要处理部分成功场景

### 中性
- 未来如需"保持优先级"，可以在 BFF 层封装 `cancelReplace` 端点（原子地撤 + 重下），但 Match 层仍是两次操作

## 实施约束

### API 层

- **MVP**：不提供专门的 replace 端点；客户端自行组合
- **未来扩展**：BFF 提供 `POST /v1/order/replace`，内部做两次调用，保证结果语义一致（都成功 / 都失败，中间态报错）

### "仅改数量"不保留优先级

- 即便用户"只改数量不改价格"，也按撤 + 新建处理
- 新订单时间戳为新建时刻，在 orderbook 中重新排队

### 客户端异常处理

- 撤单成功但新下单失败：客户端看到旧订单已撤销，新订单失败 → 需要客户端明确感知（新订单返回 error）
- 撤单失败：可能订单已成交，客户端应刷新订单状态后重试

### 状态机（见 ADR-0020）

无需新增 `MODIFIED` 状态。撤单走 `PENDING_CANCEL → CANCELED`，新建走新的 `∅ → PENDING_NEW → ...`。

## 参考

- ADR-0016: Per-symbol 单线程撮合
- ADR-0020: 订单状态机
- [Binance API: Cancel Replace](https://binance-docs.github.io/apidocs/spot/en/#cancel-an-existing-order-and-send-a-new-order-trade)
- 讨论：2026-04-18 "改单 = 撤 + 新建"
