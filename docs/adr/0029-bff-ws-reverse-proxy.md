# ADR-0029: BFF WebSocket 反代模式

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0022, 0026

## 背景 (Context)

架构文档（[architecture.md §4](../architecture.md)）规定 BFF 是 REST **+**
WebSocket 网关。MVP-4 先做了 REST，WS 一直挂着。MVP-7（Push）把客户端直连
push 实例跑通后，下一步是把 WS 入口收拢到 BFF：

- 客户端只知道 BFF 一个域名（和 REST 一致）
- Auth 在 BFF 统一做（对齐现有 `X-User-Id` middleware）
- 未来 push 多实例（MVP-13）LB sticky 落在 BFF ↔ push 之间，对客户端透明

## 决策 (Decision)

**BFF 用反向代理模式实现 `/ws`**：BFF 接受客户端 WS 连接，同时对上游 push 发起
一条独立的 WS 连接，两端之间双向透传帧。**BFF 不解析也不修改载荷**。

关键约束：

1. **Auth 在 handshake HTTP 层做**：沿用 `bff/internal/auth.Middleware` 读
   `X-User-Id`。BFF 对上游 dial 时重新 inject `X-User-Id` header。
2. **单实例 upstream**：MVP-10 配一个 `--push-ws` URL（e.g. `ws://push:8081/ws`）。
   多实例 sticky 留给 MVP-13。
3. **一对 WS 连接，无限生命周期**：直到任一方关闭。两个 goroutine 互相拷贝帧。
4. **BFF 不做 rate limit / coalescing**：对 WS 建立连接速率已经受 TCP + auth
   成本约束；per-message 背压由 push 的 send-queue 负责（ADR-0026）。

## 备选方案 (Alternatives Considered)

### A. BFF 独立订阅 Kafka，自己当 hub
- 优点：不依赖 push；BFF 能就近转发。
- 缺点：push 的 hub 功能整块复制到 BFF；失去"Push 只做长连接"的职责划分
  （ADR-0022）。两个 hub 的订阅语义要保持一致成本很高。
- 结论：拒。如果以后想下掉 push，整个合并成 "BFF with WS"，但那是方向性变更，
  不属于 MVP-10。

### B. 用 `httputil.ReverseProxy` + `UpgradeHandler`
- 优点：利用标准库，少写拷贝代码。
- 缺点：`httputil.ReverseProxy` 的 WS 支持在 Go 1.22+ 以 `ErrorHandler` +
  `UpgradeHandler` 的组合达到，和 coder/websocket 混用需要桥接代码；直接用两
  个 `websocket.Conn` 更对称。
- 结论：选对称方案，代码量相近。

### C. 让 push 对客户端直接暴露，BFF 只在 REST 层做 auth
- 优点：少一跳。
- 缺点：客户端要知道 push 地址；auth / CORS / 域名管理分裂；多 push 实例时
  LB 需要配两套（REST 一套 + WS 一套）。
- 结论：和 architecture.md 设定冲突。

### D. BFF 在反代时做 per-connection rate limit
- 优点：防刷。
- 缺点：限流策略耦合到 proxy 代码，MVP 还没到需要的体量。
- 结论：MVP 不做，留给 Backlog。

## 理由 (Rationale)

- **反代不解析载荷** 让 BFF 对 push 的协议变化（stream key / JSON shape）完全
  免疫。协议演进只需改 push 和客户端。
- **两条独立 WS 连接** 模型和 coder/websocket 的 context-first API 对齐，出错
  / shutdown 都通过 ctx cancel 传播。
- **`X-User-Id` 重新注入** 让上游 push 不需要知道 BFF 的 auth 细节；任何
  "LB → BFF → push" 的链路都能保持同一个 auth contract。

## 影响 (Consequences)

### 正面

- 客户端一套域名（BFF）覆盖 REST + WS。
- Push 可以继续部署在 internal network，LB 只对外暴露 BFF。
- 未来 MVP-13 push 多实例时，BFF 这侧只需要把 `UpstreamURL` 换成带 sticky
  的 LB，`pkg/shard.Index(userID, N)` 可以在 BFF 端提前选一个 push 后端（若
  不走 LB）。

### 负面 / 代价

- **多一跳延迟**：客户端 ↔ BFF ↔ push。MVP 估算 <1ms，可接受。
- **连接数翻倍**：BFF 既是 server 又是 client，单机 FD 消耗翻倍。单 BFF 实
  例承载上限约为 push 的一半。生产部署靠 BFF 水平扩容抵消。
- **BFF 对 push 的可观测性弱**：只看帧数，不看类型。后续若要按 stream 做
  metrics，要么改成"解析但不修改"模式，要么在 push 端打指标。

### 中性

- BFF 仍然不懂 WS 消息语义；任何协议 bug 都在 push 侧定位。

## 实施约束 (Implementation Notes)

### 代码

- [bff/internal/ws/proxy.go](../../bff/internal/ws/proxy.go)：`Proxy` 类型，
  `Handler()` 返回挂到 `/ws` 的 `http.HandlerFunc`。
- [bff/cmd/bff/main.go](../../bff/cmd/bff/main.go)：`--push-ws` 为空则 `/ws`
  不注册；非空时用 `auth.Middleware` 包一层 handler。
- 路由：外层 `ServeMux` `/ws` → proxy；`/` → 原 REST server。

### 上游 URL 规范化

支持 `ws://`、`wss://`、`http://`（自动改为 `ws://`）、`https://`（自动改为
`wss://`）。其他 scheme 直接 reject。配置错能在启动时 panic，不留运行时坑。

### 错误语义

- 上游 dial 失败 → HTTP `502 Bad Gateway`（对客户端可见）。
- 客户端 accept 失败 → 关上游，不留半开连接。
- 任一方正常关 → pump 退出 → 关另一方。context cancel 传播保证两端都能收尾。

### 测试

- [proxy_test.go](../../bff/internal/ws/proxy_test.go) 里 `fakeUpstream` 是一个
  记录收到 `X-User-Id` 并 echo 帧的 httptest 服务，验证：
  - Client → upstream 帧转发 + upstream → client 帧回传
  - `X-User-Id` 从 BFF 侧 auth 注入到上游 handshake header
  - 上游不可达时 BFF 返回 502
  - 坏 scheme 在 `New()` 就被拒

### 限制

- 没有 per-connection `idle timeout`（客户端静默连接会一直挂）。coder/websocket
  有 ping 能力但 MVP 未启用，后续和 push 侧的心跳协议一起做。
- 没有 `Origin` 校验；和 push 一致走 `InsecureSkipVerify: true`，上游 LB /
  WAF 负责外网过滤。

## 参考 (References)

- ADR-0022: Push 分片与 sticky WS 路由
- ADR-0026: Push WS 协议与 MVP-7 单实例范围
- 实现：
  - [bff/internal/ws/proxy.go](../../bff/internal/ws/proxy.go)
  - [bff/internal/ws/proxy_test.go](../../bff/internal/ws/proxy_test.go)
  - [bff/cmd/bff/main.go](../../bff/cmd/bff/main.go)
