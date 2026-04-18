# ADR-0037: Push 端 coalesce + per-conn rate limit

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0022, 0026

## 背景 (Context)

Push 每个连接有一个 `chan []byte` (默认 256 深度) 作为 outbound 队列。当
某条流突然变得很"吵"——典型场景是 K 线 update：每秒可能多次 emit 同一个
`(symbol, interval)` 的最新状态——或客户端突然变慢，队列容易在秒级内被打
满，后续广播就全部被 `TrySend` 丢弃并记日志。结果：用户端看到的 kline 断
断续续，服务端日志被 `"dropped message for slow consumer"` 淹没。

ADR-0022 §广播队列 和 ADR-0026 §实施约束 都提到 "coalesce + rate limit"
是实施阶段需要的，只是 MVP-7 先没做。本 ADR 具体落地。

## 决策 (Decision)

### 1. 可替代流走 coalesce 通道

Conn 多一条"最新值"存储 `coalMap map[string][]byte`。对于
"新 payload 完全 subsume 旧 payload" 的流（目前只有 **KlineUpdate**），
consumer 调 `Hub.BroadcastStreamCoalesce(streamKey, coalesceKey, frame)`，
Hub 再调 `Sink.TrySendCoalesce(coalesceKey, payload)`。

Conn.TrySendCoalesce 的语义：

- 加锁 → `coalMap[coalesceKey] = payload`（无论是否已有值）
- 非阻塞 signal coalesce 通道 `coalSig`
- **总是返回 true**（除非连接已经 closed）

写循环 select 三路：`ctx.Done` / `send` / `coalSig`：

- `send`：一次一帧，速率限制后写 ws
- `coalSig`：snapshot coalMap → for-each 发送。速率超限的条目放回 map
  等下次触发

这样"打爆 kline update" 的场景，outbound 队列本身不会涨；快的连接多推几
次，慢的连接自然只拿到最新那一帧。

**不 coalesce 的流**：

| 流 | 原因 |
|---|---|
| `trade@` | 每笔 fill 一条，都需要送达 |
| `depth@` | 是增量 diff；丢一条 = 缺一个 level，不能简单替换 |
| `depth.snapshot@` | 低频 |
| `kline@...` (Closed) | 状态转换事件，下游按收到与否判断 |
| `user` (私人通道) | 账户变动，每条都必须到 |

### 2. 每连接出站 token bucket

Conn 装一个 `tokenBucket`（`rate` 个/秒 + `burst` 突发）：

- 非阻塞 `allow()` 消费一个 token
- 写循环每发一帧前 `allow`；allow 失败：
  - `send` 路径：丢弃 + `Warn("ws rate-limit drop path=send")`
  - `coalesce` 路径：放回 map 等下次 signal（**不会丢状态**，因为新的
    overwrite 到来 map 里仍是最新）

默认 `--msg-rate=2000` `--msg-burst=4000`。正常客户端（十来条订阅 × 每条
几十 msg/s）远远达不到。触发就是异常：某流疯狂 emit，或连接卡住导致
per-tick 积压。`--msg-rate=0` 关掉 limiter。

## 备选方案 (Alternatives Considered)

### A. 直接加大 send buffer
- 简单，但只是推迟问题。高频流打 24h 就把 1024/conn 也打满
- Coalesce 从根上解决"状态替换型"流

### B. Per-stream 单独队列 + merge 逻辑
- Depth 可能这样做（diff merge），但 MVP 只有 kline 需要；简单 map 够了
- 以后上 depth merge 再扩展

### C. 完全丢弃速率上限，只靠 buffer + drop
- 恶意/wedged 客户端可以把消息打到后端各种资源上（CPU/内存）
- Token bucket 是个廉价上限，默认值保证"正常不触发 + 异常有上限"

### D. 用 `golang.org/x/time/rate`
- 语义相同，但那个包带 wait queue、可 reserve future slots —— MVP 只要
  allow/deny，自己写 20 行更透明
- 不增加一个 module 依赖

## 理由 (Rationale)

- **延迟最少**：快连接看到的行为和之前一致，完全没加 overhead
- **打爆最坏情况**：队列不会因为某个慢客户端无限制增长
- **KlineUpdate 最适合 coalesce**：它的 `open, high, low, close, volume,
  count` 在 bar 未关闭前是累计值，新值包含旧值的所有信息，丢掉中间态对
  下游零损失
- **独立开关**：`--msg-rate=0` 完全禁用 limiter，方便压测场景

## 影响 (Consequences)

### 正面

- 慢客户端再也不会反压其他连接（hub TrySend 对所有人都瞬返）
- 高频 kline 场景的 WS 传输量降到和消费速率等价（每 tick/interval 最多
  一帧）
- 异常上限明确：rate-limit 一触发就有日志

### 负面 / 代价

- API 多一个 Sink 方法 `TrySendCoalesce`；Hub 多一个
  `BroadcastStreamCoalesce`
- coalesce 需要 goroutine 内额外 map + 一个信号 channel —— 每连接开销
  固定（O(active streams) 内存）
- rate-limit 决策是概率性丢弃（不是队列）；运维需要在日志里观察
  `ws rate-limit drop`，必要时调大

### 中性

- `chan []byte` (send) 保留给不可 coalesce 的帧——控制帧、trade、depth
  都继续走这条。老路径 0 变化

## 实施约束 (Implementation Notes)

### 代码改动

- `push/internal/ws/ratelimit.go` — 新增 token bucket (标准库，无外部依赖)
- `push/internal/ws/conn.go`：
  - Conn 多 `rate *tokenBucket` / `coalMap` / `coalSig` 字段
  - 新 `TrySendCoalesce(coalesceKey, payload) bool`
  - 写循环拆出 `flushCoalesce(ctx)` + `writeFrame(ctx, payload)` 两个帮手
- `push/internal/hub/hub.go`：
  - Sink 接口多一个 `TrySendCoalesce`
  - 新 `BroadcastStreamCoalesce(streamKey, coalesceKey, payload)`
- `push/internal/consumer/market_data.go`：
  - `buildStreamFrame` 返回第三个值 `coalescable bool`
  - KlineUpdate → true；其他 false
- `push/cmd/push/main.go`：`--msg-rate` / `--msg-burst` flags + 传进
  ws.Config

### Flag 总览（push）

```
--send-buffer 256          # 既有：每连接 outbound 队列深度
--msg-rate    2000         # 新：每秒上限；0 禁用
--msg-burst   4000         # 新：突发 buffer
--write-timeout 10s        # 既有
```

### 写循环并发语义

- coalMap 受 `coalMu` 保护，写入 (TrySendCoalesce) 与 snapshot-to-pending
  (flushCoalesce 的 lock-swap) 互斥
- 写 ws (`writeFrame`) 不持 `coalMu`，所以新 coalesce 调用可以同时到达
- 如果 flush 被 rate-limit 截断，已拿出的条目会 "re-merge" 回 coalMap：
  如果同 key 已经有新 overwrite，保留新的；否则放回旧的

### 失败场景

| 场景 | 结果 |
|---|---|
| coalSig 已经有信号但 writeLoop 还没执行 → 又来一次 coalesce | map overwrite + signal 是 non-blocking（capacity=1 满就跳过） |
| rate-limit 一直处于 empty + kline 持续 emit | 一直 drop 当前帧、一直 re-queue；bucket 恢复后 flush latest（数据不丢） |
| send buffer 满 + kline update 不停 | 因为 kline 走 coalesce 路，完全不占 send buffer |

## 未来工作 (Future Work)

- **Depth merge**：把 DepthUpdate 也变成可替代流——需要把多条 diff 合成
  一条（level set 合并）。以后做
- **Backpressure 回传**：给极慢客户端发 `CONTROL`，让它主动 throttle 订阅
  或 reconnect
- **Prometheus metrics**：`drop_send_total` / `drop_ratelimit_total` /
  `coalesce_hits_total` per conn

## 参考 (References)

- ADR-0022: Push 分片与 sticky 路由
- ADR-0026: Push WS 协议与 MVP 范围
- 实现：
  - [push/internal/ws/conn.go](../../push/internal/ws/conn.go)
  - [push/internal/ws/ratelimit.go](../../push/internal/ws/ratelimit.go)
  - [push/internal/hub/hub.go](../../push/internal/hub/hub.go)
  - [push/internal/consumer/market_data.go](../../push/internal/consumer/market_data.go)
