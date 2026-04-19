# ADR-0052: 管理台 / Admin console（独立 admin-gateway 服务）

- 状态: Accepted
- 日期: 2026-04-19
- 决策者: xargin, Claude
- 相关 ADR: 0030（match etcd symbol sharding）、0039（BFF auth JWT/API-Key）、0011（Counter Transfer 接口）

## 背景 (Context)

MVP-0~16 完成后系统具备完整的交易闭环，但缺少 ops / admin 平面：

1. **币对生命周期**：ADR-0030 runbook 里裸 `etcdctl put /cex/match/symbols/...`，没有 API 入口，也没有写操作的审计 trail
2. **批量撤单**：`bff/internal/rest/orders.go` 只暴露单笔 `POST /v1/order` + `DELETE /v1/order/:id`，风控场景（按 user 强撤）和下架场景（按 symbol drain book）都没法做
3. **Admin auth**：ADR-0039 的 API-Key 只有"用户态"角色；`pkg/auth/apikey.go` 注释明示 `/admin/keys endpoints are future work`，没有 role/RBAC 概念
4. **审计**：无统一的管理操作审计日志（谁在什么时候改了哪个 symbol / 强撤了谁的单）

feature-requests 里用户原话："现在好像还少了管理台，就是一些后台的功能，比如币对上下架，按用户，按 symbol 批量撤单之类的功能？" + 分拆反馈："分拆开，这种是对内的，和 2c 的可不能放在一起"。

## 决策 (Decision)

### 1. 独立 `admin-gateway` 服务（不挂 BFF）

admin-gateway 是与 BFF 并列的独立 Go module + 独立进程：

```
admin-gateway/                 # 新 module
  cmd/admin-gateway/main.go    # 独立启动，默认端口 :8090
  internal/
    server/                    # /admin/* handlers + EtcdSource 接口
    counterclient/             # 小型 Counter client + Sharded fan-out
                               # （不复用 bff/internal/client，保持模块独立）
```

**为什么不复用 BFF**：
- **对外 vs 对内不能放一起**：BFF 是 2C 公网入口，admin 是 ops 内网入口；同进程意味着共用 access log / rate-limiter / middleware chain / goroutine pool，任何一方事故的爆炸半径会波及另一方
- **网段隔离**：admin-gateway 的监听端口只开给 ops 网段（VPN / 内部 LB），BFF 的端口开给公网 LB，运维策略截然不同
- **独立部署 / 扩缩容**：admin 流量稀疏但敏感（一个误操作可能影响全站），不该跟着 BFF 节点扩缩；独立进程让 ops 可以把 admin 部署到专门的机器上
- **Blast radius**：BFF 崩不影响 admin（相反亦然）；admin 这类低频但不能失手的平面，隔离远比复用重要

### 2. RBAC via role-tagged API-Key（共享 `pkg/auth`）

`pkg/auth/apikey.go`（原 `bff/internal/auth`，**挪到 pkg 层**让 BFF 和 admin-gateway 都能复用）的 `apiKeyRecord` 增加 `role` 字段，取值 `user`（默认 / 省略 = 向后兼容）或 `admin`：

```json
{"keys":[
  {"key":"...","secret":"...","user_id":"ops-bot-1","role":"admin"}
]}
```

- `APIKeyStore.Lookup` 返回 `(secret, userID, role, ok)`
- `auth.NewMemoryStore(path, allowedRoles...)` 可传 `auth.RoleAdmin` 白名单：admin-gateway 启动时用这个，**强制 admin key 文件里只能有 admin 条目**（防止 user key 被意外塞进 admin 文件的脚底枪）
- `auth.WithRole(ctx, role)` + `auth.Role(ctx) string` context helper
- `auth.AdminMiddleware(store, logger)` — **仅** API-Key 模式，拒绝 JWT / header fallback
- `auth.RequireAdmin(http.Handler)` — 非 admin role 返回 403

admin-gateway 启动链路：`AdminMiddleware` → `RequireAdmin` → handler。BFF 这边用原来的 `NewMiddleware(Config{Mode: ...})` 路径，对 role 无感（反正它不提供 `/admin/*`）。

### 3. 审计日志 — JSONL 文件（`pkg/adminaudit`）

`pkg/adminaudit`：append-only JSONL writer，每行一个 JSON 事件：

```json
{"ts_unix_ms":...,"admin_id":"ops-bot-1","op":"admin.symbol.put","target":"BTC-USDT","params":{...},"status":"ok","remote_ip":"...","request_id":"..."}
```

- 文件按 admin-gateway 进程单写；`O_APPEND` + `fsync` per-entry；rotation 交给 `logrotate`
- 失败（磁盘满 / 路径不存在）→ ERROR log + 请求仍返回 500（**审计缺失阻塞管理操作**，不允许"静默失败"）
- 未接 Kafka topic / trade-dump projection，刻意推迟：JSONL 对 MVP 级运维审计（少量请求 / 人工 grep / 运维拉日志）已经够用；上 Kafka + MySQL projection 是下一步，等到合规 / 对外审计要求落地再做

`--audit-log` 是启动必填项（`validate()` 拒绝空值），不给"生产忘了配审计"的机会。

### 4. 币对生命周期端点

```
GET    /admin/symbols             # 列所有（etcd prefix scan）
GET    /admin/symbols/{symbol}    # 查单个
PUT    /admin/symbols/{symbol}    # 创建或更新（body: {shard, trading, version}）
DELETE /admin/symbols/{symbol}    # 删除
```

- 实现：在 `pkg/etcdcfg` 的 `EtcdSource` 上加 `Put(ctx, symbol, cfg) (rev, err)` / `Delete(ctx, symbol) (existed, rev, err)`，直接写 `/cex/match/symbols/<symbol>` JSON。`MemorySource` 对称补齐 `PutCtx` / `DeleteCtx`（测试复用）
- 写操作之后审计 log 无条件写（成功或失败都记）
- **trading=false 就是"halt"**：match 侧 watch 已经支持（ADR-0030），admin 侧不需要额外协议

ADR-0030 runbook 里的 `curl -X POST bff/admin/symbol/disable` 占位符本 ADR 用 `PUT /admin/symbols/{symbol}`（body `{shard: ..., trading: false}`）统一实现（RESTful；halt 只是 PUT 一个 `trading:false`）。

### 5. 批量撤单端点

```
POST /admin/cancel-orders
  body: {"user_id": "?", "symbol": "?", "reason": "..."}
```

- 至少要给 `user_id` 或 `symbol` 其一；都给表示 `user_id && symbol` 的交集
- 反馈：`{"cancelled": N, "skipped": M, "shard_results": [{"shard_id":0,"cancelled":N,"skipped":M}, ...]}`

**Counter 侧新 RPC `AdminCancelOrders(AdminCancelOrdersRequest)`**：

```protobuf
rpc AdminCancelOrders(AdminCancelOrdersRequest) returns (AdminCancelOrdersResponse);

message AdminCancelOrdersRequest {
  string user_id = 1;                  // 可选；空 = 该 shard 内所有 users
  string symbol = 2;                   // 可选；空 = 所有 symbols
  string reason = 3;                   // 审计用（BFF/admin 层面）
}
message AdminCancelOrdersResponse {
  uint32 cancelled = 1;                // 本 shard 成功转为 PENDING_CANCEL 的数量
  uint32 skipped = 2;                  // 已 terminal / 已 pending_cancel 跳过数量
  int32 shard_id = 3;
}
```

**Counter service**：遍历 `OrderStore.All()`（clone slice，避免 hold 锁），按 (user, symbol, active) 过滤，逐笔走现有 `CancelOrder` 的 sequencer 路径（每笔走该 user 的 sequencer，保持事件顺序正确）。这是"一笔笔 cancel 的 sugar"，不引入新的状态迁移。

**admin-gateway 侧路由**：
- 有 `user_id` → `shard.Index(user_id, shards)` 算出 owning shard → 单 shard RPC
- 无 `user_id`（symbol-only）→ `counterclient.Sharded.BroadcastAdminCancelOrders` 并发调所有 shard，结果汇总

**Match 侧不新增 RPC**：counter cancel → order-event → match 的路径已经完整，admin 撤单等价于 N 笔正常 cancel。

### 6. 端口 + 部署

| 服务 | 默认端口 | 暴露面 |
|---|---|---|
| BFF | `:8080` | 2C 公网 LB |
| admin-gateway | `:8090` | 内网 / VPN 专用 |

admin-gateway 的 `--counter-shards` 与 BFF 一致（同一套 Counter 实例），但走独立的 gRPC 连接池。etcd / audit log 可与其它 ops 进程共享路径。

### 7. `/admin/healthz` bypass

`/admin/healthz` 挂在 `RequireAdmin` 之外，用于 LB / k8s 探针 —— 其它 `/admin/*` 路由全部走 `AdminMiddleware` + `RequireAdmin`。

## 备选方案 (Alternatives Considered)

### A. BFF-integrated（`/admin/*` 前缀挂 BFF）— 已拒绝

首版本（commit c8b3eb1）曾按此方案实现。user 反馈："分拆开，这种是对内的，和 2c 的可不能放在一起"。

- 优点（曾列举）：复用 BFF 的 auth / rate-limit / access log 基础设施，部署单元少
- 实际拒绝理由：
  - 对外 2C 面和对内 ops 面共用 mux / middleware / goroutine pool，任何一方事故影响另一方
  - Access log 混合，可观测性下降
  - 运维网段策略不同：BFF 要开公网 LB；admin 只开内网 —— 同一进程不能把"一部分路由开给公网，另一部分只开给内网"
  - Blast radius：admin 误操作引起 BFF 重启 = 影响 2C 交易

### B. BFF `/admin/*` 但用独立监听端口 — 已拒绝

同进程但两个 `http.Server` 分别监听 8080 / 8090。

- 优点：省一个部署单元，端口隔离
- 缺点：进程 crash 仍然同生共死；`http.Server` 两套但 goroutine 池 / logger / metrics 全共用；部署策略仍然耦合；权衡下来和方案 A 差距不大
- 不选

### C. Audit 走 Kafka `admin-audit` topic + trade-dump projection — 推迟

和现有 counter-journal / trade-event / conditional-event 的 pattern 对齐。

- 优点：查询能力（history 服务加 `ListAdminAudits`）、与主数据源同构
- 缺点：多一份 proto / 一个 producer / 一个 projection / 一张表；短期 ROI 低
- 何时重新考虑：(1) 合规要求（SOC2 / GDPR）把审计日志当独立数据资产；(2) 多 admin-gateway 实例要统一 audit 检索；(3) 要给 admin UI 做"最近 N 次操作"列表

本 ADR 决定**先 JSONL 文件**，升级路径清晰：`pkg/adminaudit.Logger` 就是一个接口，未来加 `KafkaLogger` 实现，wire 层替换即可。

### D. RBAC 合并 user/admin key 到同一文件 — 已拒绝

`--api-keys-file` 一个文件覆盖所有 role。

- 优点：配置位置集中
- 缺点：admin key 的保存位置 / rotate 频率 / 审计要求 都和 user key 不同；合并让运维心智负担增加
- 选 `--admin-api-keys-file` **独立文件 + 加载时 role 白名单强制**（防"user key 漏进 admin 文件"）

### E. Admin 操作做成 etcd 直写 UI — 拒绝（永久）

- 绕过审计、绕过校验、绕过幂等性 —— 典型"做一半就停"的反模式
- 见 non-goals.md

### F. 把"cancel 所有 X"的批量做成 Counter 内部新状态机 — 拒绝

- 相对于"cancel sugar"是 10x 复杂度，引入新的 snapshot schema + 幂等性设计
- 每笔走 sequencer 对 MVP 场景（N 通常几十到几百）性能足够

## 理由 (Rationale)

**"对内 / 对外混部 = 不能用"** 是本 ADR 的核心约束。第一版按 BFF-integrated 做完后，user 明确拒绝了合并方案。独立 admin-gateway 的成本（一个新 module、一份部署清单、共享 `pkg/auth` 和 `pkg/adminaudit`）远低于运维 + 安全角度的风险。

共享原语（`pkg/auth` / `pkg/adminaudit` / `pkg/etcdcfg`）让代码复用不成为"必须合进程"的借口。admin-gateway 自己的 counter client 是 ~150 行的薄 wrapper，复制代价可接受，换取的是清晰的模块边界。

## 影响 (Consequences)

### 正面

- 币对上下架 / 参数调整有独立 API 入口 + 审计 trail（取代 ADR-0030 的裸 `etcdctl`）
- 风控 / 清算场景的按 user / 按 symbol 批量撤单可以做了
- 内外网完全分离：BFF 崩不影响 admin；admin 误操作不波及 2C
- 为后续 admin UI（web 后台）铺路，UI 直接打 `:8090`，不走 2C LB
- `pkg/auth` 挪到 pkg 层后，push / 其它未来服务也能按需接入 role 机制

### 负面 / 代价

- 多一个 module（`admin-gateway/`）+ 一个进程 + 一份 `go.mod` / `deploy` 清单
- `counterclient` 在 admin-gateway 和 `bff/internal/client` 里有少量代码重复（~100 行 fan-out / dial）—— 为了模块独立接受
- admin key 的 rotate / revoke 仍要重启进程（live-reload 是 ADR-0039 遗留未实现项，本 ADR 不修）

### 中性

- admin-gateway 默认 `:8090`，部署脚本 / docker-compose 需要额外暴露一个端口（本 ADR 不更新 docker-compose，运维时按需加）
- `counter.proto` 新增 `AdminCancelOrders` RPC + `Admin*` messages —— 通用 proto，BFF 也看得见但不调

## 实施约束 (Implementation Notes)

### 目录结构

```
admin-gateway/
  cmd/admin-gateway/main.go        # 独立 main
  internal/
    server/
      server.go                    # /admin/* handlers
      server_test.go
    counterclient/
      counterclient.go             # Counter interface + Dial + Sharded + Broadcast
      counterclient_test.go
  go.mod / go.sum

pkg/auth/                          # 从 bff/internal/auth 挪来
  apikey.go / jwt.go / middleware.go + _test.go
pkg/adminaudit/
  audit.go / audit_test.go
pkg/etcdcfg/                       # 加 Writer (Put/Delete) + ValidateSymbol
  etcdcfg.go / memory.go / _test.go

counter/
  api/rpc/counter/counter.proto    # + AdminCancelOrders RPC
  internal/service/admin.go        # AdminCancelOrders impl
  internal/server/server.go        # gRPC adapter
```

### 启动 flags（admin-gateway）

| Flag | 必填 | 说明 |
|---|---|---|
| `--http-addr` | 否（默认 `:8090`） | HTTP 监听地址 |
| `--counter-shards` | **是** | Counter shard endpoints CSV |
| `--admin-api-keys-file` | **是** | 强制 role=admin 白名单 |
| `--audit-log` | **是** | JSONL 审计文件路径（fail-closed） |
| `--etcd` | 否 | 空 = `/admin/symbols` 返回 503 |
| `--etcd-prefix` | 否 | 默认 `/cex/match/symbols/` |

缺任一必填项 → `validate()` fatal，进程不启动。

### 测试覆盖

- `pkg/auth`：保留原 BFF 测试 + 新增 RequireAdmin / AllowedRoles / AttachesRole
- `pkg/adminaudit`：append / 并发 / reopen / 必填字段 / 目录缺失 fatal
- `pkg/etcdcfg`：PutCtx / DeleteCtx / ValidateSymbol
- `counter/internal/service`：AdminCancelOrders 按过滤器 + wrong-shard + 已 pending_cancel skip
- `admin-gateway/internal/server`：PUT / DELETE / LIST symbol + cancel per-user / cancel fan-out + audit 每条可读回 + etcd 不配 503 + healthz bypass
- `admin-gateway/internal/counterclient`：NewSharded 校验 / broadcast 聚合 + 错误 surface

## 参考 (References)

- ADR-0030 Match etcd sharding rollout — runbook 里的 symbol CRUD 占位
- ADR-0039 BFF auth JWT + API-Key — role 机制的延续
- feature-requests.md 2026-04-19 — 用户原话 + 现状核查 + 分拆反馈
- 第一版（BFF-integrated，已废）：commit c8b3eb1
