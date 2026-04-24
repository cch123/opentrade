# 威胁模型 / Security

MVP 阶段的 security posture + 已知 gap。真接入外部流量前要回顾一遍。

**本文不是合规文档**（PCI / ISO / KYC 等另算），只覆盖系统层面的 attack surface + mitigation。

---

## 信任边界

```
外部客户端 ──[TLS 由上游 LB 终结]──> BFF (auth enforced) ──gRPC──> Counter / Trigger / History
                                      │                              │
                                      └──WS 反向代理──> Push         └──Kafka──> Match / Quote / trade-dump
```

- **BFF = 信任边界的 gate**。所有外部 auth 校验只在 BFF 做一次。
- **BFF 后端服务（Counter / Push / gRPC）** 默认相信 BFF 已经验证过 `user_id`。Counter gRPC 不重复 auth。
- **Kafka + MySQL + etcd** 假定在 VPC 内部，未加 TLS / auth（MVP 期），靠网络隔离。生产前必须加。
- **数据面 vs 控制面** 未分离。运维 RPC / snapshot 管理与业务 RPC 走同一端口（未来可能分）。

## Auth 机制（[ADR-0039](./adr/0039-bff-auth-jwt-apikey.md)）

| 模式 | 机制 | 用法 | 风险 |
|---|---|---|---|
| `header` | 读 `X-User-Id`，不校验 | 开发 / 内网 | 任何人可伪造 user_id ——**不要在 internet 暴露** |
| `jwt` | HS256 共享密钥，`Authorization: Bearer` | 服务器签发 token，客户端持有 | 密钥泄露 = 全盘伪造；需要 rotation 流程 |
| `api-key` | `X-MBX-APIKEY` + HMAC-SHA256(query + body) + 时间戳窗口（5s） | 机器人 / 长期集成 | secret 泄露 = 该 key 能被伪造；需要快速 revoke 机制 |
| `mixed` | 依次尝试 jwt → api-key → header | 迁移期 | 受最弱模式约束（启用了 header 就别说有 auth） |

**尚未覆盖**：mTLS / client cert pinning、OIDC、SSO。

## Rate Limiting

- **Push 侧**（[ADR-0037](./adr/0037-push-coalesce-rate-limit.md)）：每连接 token bucket，默认 2000 帧/s、burst 4000；KlineUpdate 走可替代 coalesce 通道；depth/trade 正常队列。超过不断连，丢帧 + 日志。
- **BFF 侧**：REST / WS 尚无显式 rate limit（go-chi middleware 位置预留）。生产前必须加 per-user + per-IP。

## Replay / 幂等

- **Transfer** —— 客户端提供 `transfer_id`，Counter per-user 256 长 ring dedup（ADR-0048 backlog 已做）。
- **Order** —— 客户端提供 `client_order_id`，Counter dedup。
- **gRPC 重试** —— 默认**不**重试；上层业务逻辑决定（因为 idempotency 键由 caller 保证唯一性）。
- **WS** —— at-most-once；断线重连不会 replay，客户端自己 `GET /v1/depth` + `/v1/klines` 拉 snapshot ([ADR-0038](./adr/0038-bff-reconnect-snapshot.md))。

## 已知 gap / TODO

真接入外部流量前必须处理的：

- [ ] **BFF rate limiting** —— per-user + per-IP 限流（REST + WS），当前只有 push 一层
- [ ] **Kafka TLS / SASL** —— broker 间 + client → broker 加密 + 认证
- [ ] **MySQL TLS + 最小权限**账号（当前 `opentrade/opentrade` 拥有全权）
- [ ] **etcd TLS + client cert**
- [ ] **日志脱敏** —— balance / order amount 目前进 structured log；生产应按 sensitivity 分级
- [ ] **snapshot 文件权限** —— 当前 default umask 写入，内含账户余额；应 `chmod 600` + 加密（KMS / 磁盘）
- [ ] **API-Key 管理面** —— 发放 / 吊销 / rotation / audit log 尚无，靠文件热加载（[ADR-0039](./adr/0039-bff-auth-jwt-apikey.md)）
- [ ] **mTLS** —— 服务间 gRPC 未加 cert
- [ ] **审计日志** —— 敏感操作（Transfer / Cancel / Reserve）应有独立 audit sink 不可篡改
- [ ] **DDoS / WAF** —— 依赖上游 LB / CDN，尚无明确对接规范
- [ ] **secret 管理** —— JWT 密钥 / API-Key secrets / DB 密码目前通过 flag / file 传；生产应接 secret manager（Vault / KMS）

## Threat scenarios（条目式，需要细化）

简短场景 + 预期行为。真 attack surface review 要按 STRIDE / kill chain 展开。

- **S1：攻击者伪造 `X-User-Id`** —— 启用了 `header` auth 就能越权。**Mitigation**：生产不用 header 模式
- **S2：JWT 密钥泄露** —— 全盘伪造。**Mitigation**：rotate + versioned kid 支持多 key 并存
- **S3：API-Key secret 泄露** —— 该 key 的所有操作可伪造。**Mitigation**：支持热撤销、最小权限 scope、操作审计
- **S4：Replay 已签名 request（同时间窗口内）** —— API-Key 的 5s 窗口内同 nonce 可重放下单。**Mitigation**：短窗口 + nonce dedup（currently `transfer_id` / `client_order_id` 做业务 dedup，协议层没有）
- **S5：Cancel 他人订单** —— Counter `CancelOrder` 检查 `order.UserID == req.UserID`，不是自己返回 `ErrNotOrderOwner`
- **S6：Journal 污染 / Kafka 消息伪造** —— 当前 broker 内信任；外部攻击者拿到 broker 访问等于拿到系统。**Mitigation**：网络隔离 + SASL/TLS（未做）
- **S7：撮合侧 out-of-band message injection** —— 如果 Match 上有恶意 producer 发伪 `TradeEvent`，能让 Counter 错账。**Mitigation**：transactional producer + consumer 端 match_seq 单调 guard（部分覆盖，producer id 校验未强制）
- **S8：Snapshot 文件篡改** —— 能控制 disk 的攻击者可以改余额。**Mitigation**：生产应加 signing / 加密；当前无

---

**增补**：做 security review 时按 STRIDE 逐项展开；发生安全事故写 postmortem；协议 / 架构变更影响到信任边界时来本文更新一次。
