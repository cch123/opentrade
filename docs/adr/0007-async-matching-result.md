# ADR-0007: 下单 API 返回"已受理"，撮合结果通过 WS 异步推送

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0003, 0005

## 背景

下单 REST API 的响应语义有两种可能：
1. 同步返回最终撮合结果（filled/rejected/partial 等）
2. 同步返回"已受理"（received + order_id），成交通过 WS 或查询接口异步拿

延迟目标：P99 ≤ 10ms。

## 决策

**下单 API 返回"已受理"（或明确拒绝：余额不足、参数非法、去重命中）。** 撮合结果（成交/被撤/被拒）通过 WebSocket 私有频道推送，或通过订单查询接口获取。

## 备选方案

### 方案 A：同步等撮合结果
- 优点：语义直观，客户端逻辑简单
- 缺点：
  - 下单路径延迟 = 冻结 + 撮合 + 结算 → 远超 10ms
  - 若订单无对手方成交（挂单），API 要 hold 直到 IOC 超时或某个策略才返回
  - 服务端需要维护 in-flight request 状态，和 Match 联动困难

### 方案 B（选中）：异步，下单 API 只管受理
- 优点：
  - 下单 P99 延迟可控（3-7ms）
  - 挂单无需 hold 请求
  - 客户端通过 WS 接收实时回报
  - 符合主流交易所（Binance、OKX、Coinbase Pro）的 API 模式
- 缺点：
  - 客户端需要订阅 WS 或做 poll
  - 心智模型上用户需要理解"下单成功"≠"成交"

### 方案 C：Long-poll 混合
- 下单 API 可选 `wait=5s` 参数，在窗口内有结果就返回
- 优点：兼容
- 缺点：实现复杂，对 IOC/FOK 之外意义不大

## 理由

- **延迟预算**：P99 10ms 不足以覆盖完整撮合链路
- **挂单无法同步等**：订单可能在 orderbook 上挂数小时，同步等无意义
- **主流实践**：所有大所 REST 下单都是"受理"语义，成交走 user-data stream
- **MVP 简化**：减少服务端状态维护

## 影响

### 正面
- 下单路径延迟稳定可控
- 服务端无 in-flight request 状态
- 客户端架构清晰（REST 下单 + WS 订阅回报）

### 负面
- 客户端必须实现 WS 订阅或轮询
- 一次性工具（curl）不能一步看到成交结果，需要辅助查询

### 中性
- 后续如要支持 FOK/IOC 的同步等，可用 long-poll 扩展，不影响基础架构

## 实施约束

### 下单 API 响应

```
POST /v1/order
Request: { symbol, side, type, price, qty, client_order_id?, time_in_force }
Response:
  200 { order_id, client_order_id, status: "received", ts }
  400 { code, message }       // 参数非法
  409 { code, message }       // clientOrderId 去重 → 返回已有订单号
  422 { code: "insufficient_balance" }
  429 { code: "rate_limited" }
  503 { code: "service_unavailable" }
```

### WS 私有频道事件

```
{ type: "order_update", order_id, status: "accepted" | "partial" | "filled" | "cancelled" | "rejected", ... }
{ type: "balance_update", asset, available, frozen }
{ type: "trade", trade_id, order_id, price, qty, fee, ts }
```

### 同步等成交（可选扩展，MVP 不做）

- `POST /v1/order?wait_for=filled&timeout=5000`
- 服务端 hold 直到收到 WS 事件或超时
- 仅对 IOC/FOK 有意义，其他单类型不建议

## 参考

- [Binance Spot API: Order Placement](https://binance-docs.github.io/apidocs/spot/en/#new-order-trade)
- ADR-0003, 0005
- 讨论：2026-04-18 "下单响应语义"
