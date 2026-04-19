# API 契约索引

外部可见的 API 清单。权威定义在 proto / 代码里，本文是**索引 + 导航**，不做独立维护（否则会漂移）。

## REST（BFF）

入口：`bff/cmd/bff/main.go` → `bff/internal/rest/server.go` 注册。默认监听 `:8080`。

| Method | Path | 用途 | 代码入口 |
|---|---|---|---|
| `POST` | `/v1/transfer` | 存 / 取 / 冻结 / 解冻 | `bff/internal/rest/transfer.go` |
| `GET` | `/v1/account` | 查询当前用户余额 | `bff/internal/rest/account.go` |
| `POST` | `/v1/order` | 下单（limit / market） | `bff/internal/rest/orders.go` |
| `DELETE` | `/v1/order/{order_id}` | 撤单 | `bff/internal/rest/orders.go` |
| `GET` | `/v1/order/{order_id}` | 查单（含 `internal_status` 8 态） | `bff/internal/rest/orders.go` |
| `GET` | `/v1/orders` | 历史订单（cursor 分页，来自 trade-dump） | `bff/internal/rest/history.go` |
| `GET` | `/v1/trades` | 历史成交（cursor 分页） | `bff/internal/rest/history.go` |
| `GET` | `/v1/account-logs` | 账户流水（cursor 分页） | `bff/internal/rest/history.go` |
| `GET` | `/v1/depth/{symbol}` | 深度 snapshot（市场数据重连种子，[ADR-0038](../adr/0038-bff-reconnect-snapshot.md)） | `bff/internal/rest/market.go` |
| `GET` | `/v1/klines/{symbol}` | 最近 N 根 K 线 | `bff/internal/rest/market.go` |
| `POST` | `/v1/conditional` | 下条件单（止损 / 止盈 / trailing） | `bff/internal/rest/conditional.go` |
| `POST` | `/v1/conditional/oco` | 下 OCO | `bff/internal/rest/conditional.go` |
| `DELETE` | `/v1/conditional/{id}` | 撤条件单 | `bff/internal/rest/conditional.go` |
| `GET` | `/v1/conditional/{id}` | 查条件单 | `bff/internal/rest/conditional.go` |
| `GET` | `/v1/conditional` | 列活跃条件单 | `bff/internal/rest/conditional.go` |
| `GET` | `/healthz` | 健康检查 | - |

**Auth**（[ADR-0039](../adr/0039-bff-auth-jwt-apikey.md)）：`--auth-mode=header|jwt|api-key|mixed`。

- `header` —— 读 `X-User-Id`（不校验；默认，仅开发）
- `jwt` —— HS256 共享密钥，`Authorization: Bearer <token>`
- `api-key` —— BN 风格 `X-MBX-APIKEY` + HMAC-SHA256 签名
- `mixed` —— 按 jwt → api-key → header 顺序尝试

## WebSocket

入口：BFF `/ws` 反向代理到 push `/ws`。协议见 [ADR-0026](../adr/0026-ws-subscription-protocol.md)。

**连接**：`ws://bff/ws`（生产应 WSS）。

**客户端 → 服务端**：
```json
{"op":"subscribe","streams":["trade@BTC-USDT","depth@BTC-USDT","user"]}
{"op":"unsubscribe","streams":["..."]}
{"op":"ping"}
```

**服务端 → 客户端**：
```json
{"op":"ack","streams":["..."]}
{"op":"error","message":"..."}
{"stream":"trade@BTC-USDT","data":{...protojson...}}
```

**Stream keys**：
- 公共：`trade@<SYMBOL>`、`depth@<SYMBOL>`、`depth.snapshot@<SYMBOL>`、`kline@<SYMBOL>:<INTERVAL>`
- 私有：`user`（订单状态 + 结算事件，按当前连接的 user_id fanout）

## gRPC（内部，BFF 消费）

不直接对外，客户端只走 REST + WS。内部服务间走 gRPC：

- **Counter**（分 shard，每个 shard 独立监听）—— `PlaceOrder` / `CancelOrder` / `QueryOrder` / `Transfer` / `QueryBalance` / `Reserve` / `ReleaseReservation`。proto: `api/rpc/counter/`
- **Conditional** —— `Place` / `Cancel` / `Query` / `List`。proto: `api/rpc/conditional/`
- **History** —— `ListOrders` / `ListTrades` / `ListAccountLogs`。proto: `api/rpc/history/`

## 增补协议层细节

等到需要对外开放 API 或者多 client 接入时，在本目录下按协议拆：
- `api/rest.md` —— 每个 endpoint 的 request / response schema + 错误码
- `api/websocket.md` —— 详细 stream 规范 + 私有流 payload
- `api/grpc.md` —— gRPC 服务间约定（含 shard 路由、retry、deadline 策略）
- `api/errors.md` —— 错误码表

现在只保留本索引，避免和 proto / 代码漂移。
