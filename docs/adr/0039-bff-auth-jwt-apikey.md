# ADR-0039: BFF 认证升级到 JWT + API-Key

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0004（MVP-4 认证占位）

## 背景 (Context)

MVP-4 的认证是"直接读 `X-User-Id` 头"——BFF 完全信任前面的 LB / 网关。上
线生产之前必须替换成可验证的方案：不然任何能访问 BFF 的请求都能伪造 user。

两种主流前端契约：

1. **JWT (RFC 7519)**：浏览器 / App 登录后拿 token，Bearer 请求头携带；BFF
   通过共享密钥或公钥验证签名 + 过期
2. **API-Key (Binance 风格)**：程序化 API 客户端拿 `(key, secret)` 对，在
   请求里带 `X-MBX-APIKEY` + 对 query+body 做 HMAC-SHA256 签名

两种场景都要支持，但是不想强耦合：JWT 用于前端，API-Key 用于机器。

## 决策 (Decision)

### 1. `auth.NewMiddleware(cfg) → http.Handler wrapper`

一个统一入口，根据 `Mode` 派生中间件：

```
header  :  legacy X-User-Id（MVP-4 行为）
jwt     :  Authorization: Bearer <HS256 JWT>；sub → user id
api-key :  X-MBX-APIKEY + timestamp + signature（HMAC-SHA256）
mixed   :  JWT → api-key → header，先匹配者生效（rollout 用）
```

**关键设计**：所有 mode 都**不阻塞请求**——认证失败就只是"user id 没挂到
ctx"，下游 handler `UserID(ctx)` 判断自己要不要 401。这和 MVP-4
`Middleware` 的语义完全兼容，rate-limit / `/healthz` / 公开 market 接口
都不受影响。

### 2. JWT: HS256 only

- 签名算法 HS256（共享密钥），强制校验 `alg` 头，`alg:none` 拒绝
- 必须有 `sub` claim；可选 `exp` / `iat`
- 签名验证用 `hmac.Equal`（constant-time）
- 过期检查放在签名之后：签名错的 token 不泄漏"它是否已过期"

RS256 / ES256 留给未来（对接 Keycloak / Auth0 场景）。

### 3. API-Key: BN-style `X-MBX-APIKEY` + HMAC

签名串：`rawQueryWithoutSignature + "|" + body`

- `rawQueryWithoutSignature`：URL `?` 之后删掉 `signature=xxx` 的原始串
  （保留其他参数在线上的顺序 + URL 编码）
- `|` 分隔符：让空 body 的 GET 和 body 以 `&` 开头的 POST 不会碰撞
- 客户端通过 query param `signature=<hex>` 提交；`timestamp=<ms>` 强制带

验证流程：
1. 查 key → (secret, user_id)
2. 检查 `timestamp` 和服务器时钟偏差 ≤ `RecvWindow` (默认 5s)
3. HMAC 比对

**密钥存储 MVP**：JSON 文件，启动加载到内存（`bff/internal/auth.NewMemoryStore`）。
轮换需要重启 BFF。生产上会换 DB + live reload，留 future work。

### 4. 三个 flag

```
--auth-mode        header | jwt | api-key | mixed    # 默认 header（兼容 MVP-4 环境）
--jwt-secret       <base64-or-utf8 secret>           # jwt / mixed 时必填
--api-keys-file    path/to/keys.json                 # api-key / mixed 可选
```

## 备选方案 (Alternatives Considered)

### A. 直接用 `github.com/golang-jwt/jwt`
- 优点：功能齐全（alg 支持多、claims 校验库）
- 缺点：我们只用 HS256 一种 alg，自己写 60 行可控、无供应链风险、
  和内部 base64 / hmac 工具复用
- 结论：不引入

### B. mTLS 代替 API-Key
- 优点：真正的密钥管理（CA 签发）
- 缺点：客户端 SDK 复杂度大幅上升，浏览器基本没法做
- 结论：留给企业/机构接入，公开 API 还是 HMAC

### C. OAuth2 client_credentials flow
- 优点：标准更成熟，吊销体系完善
- 缺点：引入 IdP 组件、access-token 换签机制、延迟敏感场景不适合
- 结论：MVP 阶段自己管 key + JWT 足够，后续可以在前面挂一个 IdP 发 JWT

### D. JWT 和 API-Key 任选其一
- 前端和 bot 客户端需求差异太大——JWT 短期 token（小时级），API-Key 长期
  凭据（手动轮换）。两者都做才能覆盖
- 结论：两条路径都实现，`mixed` 让一个进程同时接受

### E. 用 query param 携带 JWT（不要 Authorization header）
- 对 WS 友好（headers 在 upgrade 之前有）但泄漏风险高（代理日志 / refer）
- 结论：WS 仍走 Authorization header，代理自行实现

## 理由 (Rationale)

- **无外部依赖**：JWT 验证和 API-Key 签名都靠 stdlib（`crypto/hmac`、
  `crypto/sha256`、`encoding/base64`）。这个模块的攻击面完全可审
- **向后兼容**：默认 `--auth-mode=header` 保留 MVP-4 行为，所有既有测试和
  集成脚本 0 改动；上线时改 flag 就能切换
- **渐进迁移**：`mixed` 模式让生产一段时间里 "新 client 用 JWT / 旧脚本还
  用 header" 并存
- **BN 兼容**：API-Key 签名流程复刻 BN，已有 BN SDK 可以零改动接入

## 影响 (Consequences)

### 正面

- BFF 不再需要完全信任 LB 层就能保证 user_id 真实
- 两种客户端形态都覆盖（浏览器 / App / 机器人）
- 没有引入新依赖

### 负面 / 代价

- API-Key 文件式存储：轮换成本高（需要 BFF 重启），攻击面下降但运维费
  时。`future_work` 有条目跟踪
- JWT 用 HS256 意味着所有 BFF 实例共享 secret；rotate 需要两阶段
  （新老 secret 并存） —— 本 ADR 不做
- `mixed` 模式下 JWT 失败会静默 fall through，debug 时需要看 Debug log
- 新 body-rewindable reader：API-Key 验证要读完 body 做 HMAC，需要把 body
  倒回给下游。代码里 `rewindableBody` 实现

### 中性

- 鉴权失败走 "anonymous"（user id 不注入）而不是 401。handlers 已经通过
  `auth.UserID(ctx)` 自决，所以 `/v1/depth`、`/v1/klines`、`/healthz` 等公
  开接口继续工作

## 实施约束 (Implementation Notes)

### 文件布局

- `bff/internal/auth/jwt.go` — `VerifyHS256` / `SignHS256`（HS256 only）
- `bff/internal/auth/apikey.go` — `VerifyAPIKeyRequest` /
  `SignAPIKeyRequest` / `NewMemoryStore`
- `bff/internal/auth/middleware.go` — `NewMiddleware(Config)`；保留
  legacy `Middleware` 常量供老测试使用

### rest.Server 接口变化

- `rest.Config` 新字段 `AuthMiddleware func(http.Handler) http.Handler`
- `Server.Handler()` 优先用 `Config.AuthMiddleware`，nil 时 fallback 到
  `auth.Middleware`（旧行为）
- `NewServer` 签名不变

### BFF main.go 变化

- 多三个 flag（见上）
- `buildAuthMiddleware(cfg) → (mw, err)`：按 mode 组装；mixed 时 jwt-secret
  空 → 跳过 JWT 尝试；api-keys 文件空 → 跳过 API-Key
- `/ws` 路径也套上同一个 mw（`outer.Handle("/ws", authMW(proxy))`）

### 签名串的坑

API-Key 签名用 `r.URL.RawQuery` 而非 `r.URL.Query().Encode()`：后者会
重新排序 + 规范化 URL encode，和客户端的实际字符串不一致。`queryWithoutSignature`
在字符串层面 drop `signature=...` segment，保留其余字节完全一致。

### 失败 / Edge case

| 情况 | 行为 |
|---|---|
| 没带 `Authorization` + `X-User-Id` | 匿名（公开端点可用，私有端点 401） |
| JWT alg=none | 拒（HS256 校验强制） |
| JWT 签名对但 exp 过 | `ErrExpiredJWT`，user id 不挂；私有端点 401 |
| API-Key timestamp 偏差 > 5s | `ErrAPIKeyStale`，匿名化 |
| API-Key 文件启动加载失败 | Fatal（避免"以为启用其实未启用"的静默） |
| 相同 key 在 file 里重复 | Fatal（ambiguous lookup） |

### 测试覆盖

- `jwt_test.go`：sign/verify 往返、错签、过期、畸形 token、alg:none 拒绝
- `apikey_test.go`：成功路径、签名错、未知 key、过期、缺失 header、
  memory store 文件加载 + 重复拒绝
- `middleware_test.go`：四种 mode 注入 user_id 的断言；mixed 模式
  JWT 优先 header fallback；unknown mode → err

## 未来工作 (Future Work)

- **RS256 / ES256**：接入外部 IdP 时 BFF 改成验签公钥
- **API-Key 存 DB + live reload**：运维无需重启轮换；watch 文件 / 订阅
  Postgres LISTEN
- **JWT secret rotation**：支持新老 secret 同时验证，灰度切换
- **签名 audit log**：每次成功/失败都记录到独立 audit 流，供合规审查
- **Per-key rate limit**：现在 rate limit 是 per-user；API-Key 流量大时
  也按 key 独立限额
- **WS auth 刷新**：长连接 JWT 过期后如何处理（强制重连 / 心跳续签）

## 参考 (References)

- RFC 7519 — JSON Web Token
- [Binance API — Signed endpoints](https://binance-docs.github.io/apidocs/spot/en/#signed-trade-user_data-and-margin-endpoint-security)
- 实现：
  - [bff/internal/auth/jwt.go](../../bff/internal/auth/jwt.go)
  - [bff/internal/auth/apikey.go](../../bff/internal/auth/apikey.go)
  - [bff/internal/auth/middleware.go](../../bff/internal/auth/middleware.go)
  - [bff/cmd/bff/main.go](../../bff/cmd/bff/main.go)
