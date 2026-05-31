# ADR-0084: match→user 私有推送合并策略

- 状态: **Proposed**（2026-05-31 起草；从 `docs/roadmap.md` 的 private push 合并策略待调研项提升为独立 ADR）
- 日期: 2026-05-31
- 决策者: xargin, Codex
- 相关 ADR: 0026（Push WS 协议）、0037（Push coalesce + rate limit）、0038（BFF 重连补齐快照）、0068（perp 私有流）

## 范围声明（先读这一段）

当前一笔成交在 counter/perp-counter 侧通常产生订单状态事件和结算事件，push 逐条 fanout。一个 taker 吃 100 个 maker 时，单用户可能收到约 200 条 WS 消息。

本 ADR 决定先做**可配置的传输帧层 batching**，不改 journal 语义、不强制合并交易明细。默认对普通连接开启 batch，但协议必须提供 opt-out 给做市 / HFT（high-frequency trading，高频交易）用户；后续交易明细聚合仍保持 opt-in。

## 背景 (Context)

roadmap 已调研多家交易所：

- Binance/BingX 倾向字段合并成 execution report。
- Bybit 使用 `data:[]` 数组和短窗口 batch。
- OKX/Gate/MEXC 分订单频道和明细频道。
- Coinbase 只推订单累计态，fills 走 REST。
- Hyperliquid 支持客户端声明式聚合。

OpenTrade 当前最重要的约束：

- journal 是审计和 recovery 权威，不能为了 WS 省包破坏逐笔事件。
- trade-dump 需要逐笔落库。
- 做市 / 高频用户可能要求逐笔明细。
- C 端用户更关心少包和订单累计态。

## 决策 (Decision)

### 1. 第一阶段选择 push frame batching

保持 upstream event 不变：

```text
counter-journal / perp-journal
        |
        v
push private consumer
        |
        v
per-user short buffer (5-20ms or max N)
        |
        v
WS frame { stream, seq_start, seq_end, data: [...] }
```

默认参数：

```text
batch_enabled = true
batch_window_ms = 10
batch_max_items = 100
batch_max_bytes = 64KiB
```

窗口到期、数量到限、字节到限任一满足即 flush。

### 2. WS frame 增加可恢复的序号范围

```json
{
  "stream": "user",
  "source": "counter-journal",
  "partition": 7,
  "seq_start": 123,
  "seq_end": 140,
  "data": [ ... ]
}
```

序号来源固定为 journal source 的 `(topic, partition, event_seq)`：

- spot `user` stream 使用 `counter-journal` 的 `(partition, counter_seq_id)`。
- perp `perp-user` stream 使用 `perp-journal` 的 `(partition, perp_seq_id)`。
- 一个 WS frame 只能包含同一 `stream/source/partition` 的连续事件；不同 partition 必须拆成不同 frame。
- `seq_start/seq_end` 是该 frame 内同 partition 的最小/最大 seq；若中间不连续，push 必须拆 frame 或标记 gap。

客户端按 `(stream, source, partition)` 维护连续性。检测到 gap 后，重连仍走 ADR-0038 的 snapshot + history 补齐；push-local 连接序号不能作为恢复依据，因为实例重启 / rebalance 后无法从 history 对账。

### 3. 不改变 journal，不合并 OrderStatus 和 Settlement

第一阶段不做：

- counter journal schema breaking change。
- 强制把 OrderStatus + Settlement 合成 TradeUpdate。
- WS-only cumulative。

原因是这些会影响审计、history、做市商消费方。batching 只改变传输帧，不改变事件语义。

### 4. 为后续 opt-in aggregation 预留订阅参数

订阅协议：

```json
{
  "op": "subscribe",
  "streams": ["user"],
  "options": {
    "batch": "default",
    "aggregate_fills": false
  }
}
```

`batch` 可取：

- `"default"`：使用服务端默认，普通连接默认 true。
- `true`：显式开启。
- `false`：显式关闭，适合做市 / HFT 用户，但仍受 per-conn rate limit 保护。

P1 只实现 frame batching。`aggregate_fills=true` 后续另做，需要定义按 `order_id` / `match_round` 聚合的精确语义。

### 5. 慢连接策略按 frame，而不是按 event

当前慢连接丢消息策略要升级：

- 缓冲区按 frame 计数。
- 单 frame 不能超过 `batch_max_bytes`。
- 丢 frame 时记录 `seq_start/seq_end`，客户端能发现 gap。

## 备选方案 (Alternatives Considered)

### A. 协议字段层合并 OrderStatus + Settlement

能把 200 条降到约 100 条，但 breaking journal schema，影响 projection。暂不选。

### B. 传输帧层 batch

侵入最小，保留逐笔语义，延迟可控。选择第一阶段。

### C. 分层频道：orders + fills

产品形态清晰，但需要 BFF/push/history 一起扩。保留后续。

### D. 客户端声明式聚合

灵活，但服务端要维护双视图。预留订阅参数，后续单独 ADR。

### E. WS only cumulative

最省带宽，但不适合做市/高频用户。否决作为默认。

## 影响 (Consequences)

### 正面

- 大幅减少 burst 时 WS frame 数。
- 不改变 journal 和 projection。
- 客户端可用 seq range 检测 gap。

### 负面 / 代价

- 增加 5-20ms 可配置传输延迟。
- 客户端必须处理 `data:[]`。
- push 慢连接和 rate limit 需要按 frame/bytes 重新调参。

## 实施约束 (Implementation Notes)

- batch 必须按 `(connection, stream)` 隔离，不能把不同用户数据混到同 frame。
- 同一用户同 stream 内保持原始事件顺序。
- `seq_start/seq_end` 必须使用 journal source 的 `(topic, partition, event_seq)`，禁止使用 push-local 序号作为 recovery 序号。
- 单测覆盖窗口 flush、max_items flush、max_bytes flush、顺序保持、gap 检测、慢连接丢 frame。
- 先在 perp-user 和 user 两条私有流同时支持，公开行情流沿用 ADR-0037 的 coalesce 策略。

## 参考 (References)

- [ADR-0026: Push WS 协议与 MVP-7 单实例范围](./0026-push-ws-protocol-and-mvp-scope.md)
- [ADR-0037: Push 端 coalesce + per-conn rate limit](./0037-push-coalesce-rate-limit.md)
