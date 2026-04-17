# ADR-0015: clientOrderId 幂等在 Counter 层处理，仅对活跃订单去重

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0005, 0010, 0018, 0020

## 背景

下单请求需要幂等：客户端重试、网络重传、BFF 重试都可能造成重复请求。幂等的粒度与生命周期有两种选项：

1. 全局窗口（如 24h）：每个 clientOrderId 用过就保留一段时间
2. 活跃订单窗口：只对"仍活跃"的订单做去重，订单终结后同 clientOrderId 可复用

## 决策

**clientOrderId 去重采用"仅覆盖活跃订单"语义：**
- Counter 主内存维护 `(user_id, clientOrderId) → order_id` 索引，**仅包含非终态订单**
- 订单进入终态（FILLED / CANCELED / REJECTED / EXPIRED）时，**从索引中移除对应条目**
- Match 侧做一道 `order_id` 业务幂等作为防御（ADR-0005）

## 备选方案

### 方案 A：24h TTL 全局去重
- 优点：绝对防重复
- 缺点：
  - 需要额外 Redis + TTL 清理逻辑
  - 不符合主流交易所语义（用户完成一单后 clientOrderId 就该能复用）
  - 用户长期做市会快速耗尽 clientOrderId 空间

### 方案 B（选中）：活跃订单窗口
- 优点：
  - 语义对齐 Binance / OKX（"unique among open orders"）
  - 无独立 dedup 表，无 TTL 清理
  - 崩溃恢复简单：从 journal 回放重建索引，终态订单自然不进索引
- 缺点：
  - 索引与订单生命周期耦合，需要在每次状态机转移时维护

## 理由

- **对齐主流**：Binance `newClientOrderId` 文档明确 "unique id among **open** orders"；OKX 同理
- **简化实现**：不需要独立 dedup 表 / Redis / TTL 清理
- **用户体验**：做市商场景可以稳定复用一组 clientOrderId

## 影响

### 正面
- 代码简化
- 无额外存储依赖
- 崩溃恢复天然正确（从 journal 重建）

### 负面
- 索引操作必须和订单状态机转移原子（都在 UserSequencer 内）

### 中性
- PENDING_CANCEL 算活跃态，此时 clientOrderId 仍被占用；用户不能在撤单确认前复用（合理）

## 实施约束

### 数据结构

```go
// counter/internal/engine/state.go
type ShardState struct {
    // order_id → *Order
    orders map[uint64]*Order
    
    // (user_id, clientOrderId) → order_id,仅活跃订单
    activeByCOID map[string]map[string]uint64
    
    // 其他字段...
}
```

### 下单查重流程

```go
func (s *ShardState) PlaceOrder(req *PlaceReq) (*Order, error) {
    if req.ClientOrderID != "" {
        if existingOrderID, ok := s.activeByCOID[req.UserID][req.ClientOrderID]; ok {
            // 命中索引,返回已有订单 (该订单一定是活跃态)
            return s.orders[existingOrderID], nil
        }
    }
    
    // 正常下单流程
    order := s.createOrder(req)
    if req.ClientOrderID != "" {
        s.addToActiveIndex(req.UserID, req.ClientOrderID, order.ID)
    }
    return order, nil
}
```

### 状态转移时的索引维护

在订单状态转移函数里集中处理：

```go
func (s *ShardState) applyTransition(order *Order, newStatus OrderStatus) {
    oldStatus := order.Status
    order.Status = newStatus
    
    // 进入终态时移除索引
    if !oldStatus.IsTerminal() && newStatus.IsTerminal() {
        if order.ClientOrderID != "" {
            s.removeFromActiveIndex(order.UserID, order.ClientOrderID)
        }
    }
    // 其他清理...
}

func (s OrderStatus) IsTerminal() bool {
    switch s {
    case FILLED, CANCELED, REJECTED, EXPIRED:
        return true
    }
    return false
}
```

**注意**：PENDING_NEW、NEW、PARTIALLY_FILLED、PENDING_CANCEL 都是**非终态**，索引保留。

### 恢复流程

从快照 + journal 回放重建 `activeByCOID`：
- 回放每条 event，模拟状态转移
- 终态事件自然把 clientOrderId 从索引剔除
- 结束后索引只含活跃订单

无需额外逻辑。

### Transfer 的幂等（不走这套）

`Transfer` 接口（ADR-0011）没有"订单生命周期"概念，其 `transfer_id` 幂等采用独立的 24h TTL dedup 表（Redis + 内存 LRU），与本 ADR 无关。

### clientOrderId 字段规则

- 长度：≤ 64 字符
- 字符：`[A-Za-z0-9_\-]`
- 用户 ID 隔离：不同用户相同 clientOrderId 不冲突
- 可选字段：不传代表不需要幂等保护

### Match 侧防御（零成本）

- Match orderbook 以 `order_id` 为 key（撤单用）
- OrderPlaced 若 order_id 已存在 → drop + WARN
- 防止 Kafka 重复投递 / EOS 配置异常等极端场景

## 参考

- ADR-0005: Kafka 事务
- ADR-0010: Counter 分片
- ADR-0018: Counter Sequencer
- ADR-0020: 订单状态机
- [Binance newClientOrderId 文档](https://binance-docs.github.io/apidocs/spot/en/#new-order-trade)
- 讨论：2026-04-18 "clientOrderId 只做未完结订单的去重"
