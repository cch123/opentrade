# ADR-0020: 订单状态机（内部 8 态 + 外部 6 态）

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0003, 0004, 0007, 0014, 0015

## 背景

订单从提交到终态经过多个状态。需要明确：
- 有哪些状态
- 状态转移的触发者和事件
- 权威状态归属
- 对外 API 暴露的状态是否包含 pending 中间态

在异步受理架构（ADR-0007）下，"Counter 已受理但 Match 未确认"、"Cancel 已受理但 Match 未确认"这两个中间态真实存在，时长 3-30ms。

## 决策

采用 **Binance 风格**：
- **内部 8 态**：Counter 内存和 journal 中记录完整生命周期，含 `PENDING_NEW` / `PENDING_CANCEL`
- **外部 6 态**：对用户暴露的 API / WS 只显示 `NEW / PARTIALLY_FILLED / FILLED / CANCELED / REJECTED / EXPIRED`
- 内部 pending 态在外部呈现为相邻的非 pending 态（PENDING_NEW → NEW，PENDING_CANCEL → 上一个状态）

## 备选方案

### 方案 A（选中）：Binance 风格隐藏 pending
- 用户看到的 status 简洁
- 符合零售加密交易所主流
- 代价：极罕见场景下用户看到 NEW 瞬间变 REJECTED（Match 层拒单）

### 方案 B：Kraken/FIX 风格暴露 pending
- 用户可观测链路延迟
- 适合机构做市
- 代价：客户端心智更复杂

### 方案 C：默认隐藏 + verbose 参数显示
- 折中
- 代价：双套逻辑

## 理由

- 我们定位零售为主，对齐 Binance 降低客户端学习成本
- 内部保留完整状态机，便于排查链路问题、写对账、做监控
- 未来需要暴露（例如接 FIX 通道）时，映射层改一下即可

## 内部状态定义（Counter 权威，写 journal）

| 状态 | 含义 | 终态 |
|---|---|---|
| `PENDING_NEW` | Counter 已受理，等 Match 入 book 确认 | 否 |
| `NEW` | Match 已入 book（挂单中） | 否 |
| `PARTIALLY_FILLED` | 部分成交 | 否 |
| `FILLED` | 完全成交 | ✓ |
| `PENDING_CANCEL` | 撤单已受理，等 Match 确认 | 否 |
| `CANCELED` | 已撤销 | ✓ |
| `REJECTED` | 被拒 | ✓ |
| `EXPIRED` | 过期（IOC 剩余 / FOK 不成 / TIF 到期） | ✓ |

## 外部状态（API / WS）

| 外部状态 | 映射自的内部状态 |
|---|---|
| `NEW` | `PENDING_NEW`, `NEW` |
| `PARTIALLY_FILLED` | `PARTIALLY_FILLED`，以及 `PENDING_CANCEL`（若此前是 PARTIALLY_FILLED） |
| `FILLED` | `FILLED` |
| `CANCELED` | `CANCELED` |
| `REJECTED` | `REJECTED` |
| `EXPIRED` | `EXPIRED` |

### `PENDING_CANCEL` 外部映射细节

`PENDING_CANCEL` 的外部表现取决于**撤单前的状态**：
- 若撤单前是 NEW → 外部仍显示 `NEW`
- 若撤单前是 PARTIALLY_FILLED → 外部仍显示 `PARTIALLY_FILLED`

实现上 Order 字段需要保留一个 `pre_cancel_status`，或者通过 `filled_qty > 0 ? PARTIALLY_FILLED : NEW` 推断。

## 状态转移图（内部）

```
                     [Counter] PlaceOrder
     ∅ ─────────────────────────────────> PENDING_NEW
     │                                        │
     │ [Counter] 余额不足/参数非法              │ [Match] OrderAccepted
     ▼                                        ▼
  REJECTED (直接返回,无持久状态)           ┌── NEW ──┐
                                            │         │
                 [Match] price tick/STP/    │         │
                         post-only-take  ◄──┤         │ [Match] Trade(partial)
                                            │         ▼
                                        REJECTED      PARTIALLY_FILLED
                                                             │    │
                                   [Match] Trade(final) ─────┤    │
                                              ▼              │    │ [Counter] CancelOrder
                                           FILLED            │    │
                                                             │    ▼
                                                     [Counter] CancelOrder
                                                             │
                                                             ▼
                                                       PENDING_CANCEL ◄── NEW (user cancels early)
                                                             │
                             ┌───────────────────────────────┼──────────────────────────┐
                             ▼                               ▼                          ▼
                 [Match] OrderCancelled            [Match] Trade(fills                [Match] Trade(partial)
                             │                               remaining)             (still PENDING_CANCEL,
                             ▼                               ▼                       filled_qty 累加)
                        CANCELED                          FILLED
              
    [Match] OrderExpired (TIF / IOC / FOK)
              │
              ▼
           EXPIRED
```

## 转移表

| 起始内部态 | 事件 | 终止内部态 | 触发者 | Kafka 事件 |
|---|---|---|---|---|
| ∅ | 下单受理 | PENDING_NEW | Counter 主 | counter-journal: `FreezeEvent`, order-event: `OrderPlaced` |
| ∅ | 下单 Counter 拒绝（余额/参数/dedup） | REJECTED | Counter 主 | 不落 journal，直接返回错误 |
| PENDING_NEW | Match 确认 | NEW | Match | trade-event: `OrderAccepted` |
| PENDING_NEW | Match 拒绝 | REJECTED | Match | trade-event: `OrderRejected(reason)` |
| PENDING_NEW | Match 立即全成 | FILLED | Match | trade-event: 多条 `Trade` |
| PENDING_NEW | IOC 部分/全不成 | PARTIALLY_FILLED/EXPIRED | Match | `Trade` + `OrderExpired` |
| NEW / PART | Match trade 未填满 | PARTIALLY_FILLED | Match | trade-event: `Trade` |
| NEW / PART | Match trade 填满 | FILLED | Match | trade-event: `Trade(final)` |
| NEW / PART | 用户 Cancel | PENDING_CANCEL | Counter 主 | counter-journal: `CancelRequested`, order-event: `OrderCancel` |
| NEW / PART | TIF 到期 | EXPIRED | Match | trade-event: `OrderExpired` |
| PENDING_CANCEL | Match 确认撤 | CANCELED | Match | trade-event: `OrderCancelled(filled_qty)` |
| PENDING_CANCEL | 并发 trade 填满 | FILLED | Match | trade-event: `Trade(final)`，撤单被忽略 |
| PENDING_CANCEL | 并发 trade 部分填 | PENDING_CANCEL | Match | trade-event: `Trade`，filled_qty 累加 |

## 权威状态

- **Counter 主**：持有完整内部状态（8 态），journal 里有完整事件流
- **Match**：只维护 orderbook 上订单的存在与否（live / removed），不区分 pending
- **BFF / 外部查询**：通过 Counter 读内部态 → 映射为外部态返回

## 字段设计（Order 结构）

```go
type Order struct {
    OrderID       uint64
    ClientOrderID string
    UserID        string
    Symbol        string
    Side          Side
    Type          OrderType
    TimeInForce   TIF
    Price         Decimal
    Qty           Decimal
    FilledQty     Decimal
    FrozenAmt     Decimal
    
    Status         OrderStatus  // 内部 8 态
    PreCancelStatus OrderStatus // 仅在 PENDING_CANCEL 时有效,用于外部映射
    
    CreatedAt     int64
    UpdatedAt     int64
    Fee           Decimal
    FeeAsset      string
}

func (o *Order) ExternalStatus() ExternalOrderStatus {
    switch o.Status {
    case PENDING_NEW, NEW:
        return EXT_NEW
    case PENDING_CANCEL:
        if o.PreCancelStatus == PARTIALLY_FILLED {
            return EXT_PARTIALLY_FILLED
        }
        return EXT_NEW
    case PARTIALLY_FILLED:
        return EXT_PARTIALLY_FILLED
    case FILLED:
        return EXT_FILLED
    case CANCELED:
        return EXT_CANCELED
    case REJECTED:
        return EXT_REJECTED
    case EXPIRED:
        return EXT_EXPIRED
    }
    panic("unreachable")
}
```

## proto 定义

**内部**（`api/event/counter_journal.proto`）：
```protobuf
enum InternalOrderStatus {
    INTERNAL_ORDER_STATUS_UNSPECIFIED = 0;
    PENDING_NEW = 1;
    NEW = 2;
    PARTIALLY_FILLED = 3;
    FILLED = 4;
    PENDING_CANCEL = 5;
    CANCELED = 6;
    REJECTED = 7;
    EXPIRED = 8;
}
```

**外部**（`api/rpc/bff.proto`）：
```protobuf
enum OrderStatus {
    ORDER_STATUS_UNSPECIFIED = 0;
    NEW = 1;
    PARTIALLY_FILLED = 2;
    FILLED = 3;
    CANCELED = 4;
    REJECTED = 5;
    EXPIRED = 6;
}
```

## 并发与幂等

- 状态转移由 Counter UserSequencer 串行（ADR-0018）
- trade-event 按 `(symbol, match_seq_id)` 去重
- Match 侧按 `order_id` 去重（ADR-0015）
- 非法转移 → drop + WARN + metric `order_invalid_transition_total{from,to}`

## 实施约束

### `ExternalStatus` 映射在哪执行

- gRPC server 层（BFF / Counter 的对外接口）
- WS 推送层（push service 把 counter-journal 事件翻译成外部事件）
- 测试：每个内部态的外部映射都要有单元测试

### 异常场景处理

- PENDING_NEW → REJECTED：用户先看到 NEW，再收到 REJECTED（罕见，文档中说明）
- PENDING_NEW → FILLED：用户先看到 NEW，再看到 FILLED（市价单/IOC 正常情况）
- PENDING_CANCEL → FILLED：用户发了撤单，但被成交"拦住"，最终看到 FILLED（race，合理行为）

### 测试覆盖

- 所有**合法**转移逐一验证（内部态）
- 所有**非法**转移（如 FILLED → CANCELED）必须被拒绝
- 所有内部 → 外部映射逐一覆盖
- race 场景：PENDING_CANCEL 期间收到 Trade

## 参考

- ADR-0003, 0004, 0005, 0007, 0014, 0015
- [Binance Order Status](https://binance-docs.github.io/apidocs/spot/en/#public-api-definitions)
- [FIX 5.0 Order Status (150)](https://www.onixs.biz/fix-dictionary/5.0/tagNum_150.html)
- 讨论：2026-04-18 订单状态机 / PENDING_NEW 可见性
