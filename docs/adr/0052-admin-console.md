# ADR-0052: 管理台 / Admin console（BFF-integrated）

- 状态: Accepted
- 日期: 2026-04-19
- 决策者: xargin, Claude
- 相关 ADR: 0030（match etcd symbol sharding）、0039（BFF auth JWT/API-Key）、0011（Counter Transfer 接口）

## 背景 (Context)

MVP-0~16 完成后系统具备完整的交易闭环，但缺少 ops / admin 平面：

1. **币对生命周期**：ADR-0030 runbook 里裸 `etcdctl put /cex/match/symbols/...`，没有 API 入口，也没有写操作的审计 trail
2. **批量撤单**：`bff/internal/rest/orders.go` 只暴露单笔 `POST /v1/order` + `DELETE /v1/order/:id`，风控场景（按 user 强撤）和下架场景（按 symbol drain book）都没法做
3. **Admin auth**：ADR-0039 的 API-Key 只有"用户态"角色；[bff/internal/auth/apikey.go:34](../../bff/internal/auth/apikey.go) 注释明示 `/admin/keys endpoints are future work`，没有 role/RBAC 概念
4. **审计**：无统一的管理操作审计日志（谁在什么时候改了哪个 symbol / 强撤了谁的单）

feature-requests 里用户原话："现在好像还少了管理台，就是一些后台的功能，比如币对上下架，按用户，按 symbol 批量撤单之类的功能？"

## 决策 (Decision)

### 1. 复用 BFF，不独立 admin-gateway

所有 admin 端点挂在 BFF 上，前缀 `/admin/*`。MVP 单服务进程 + 独立 role 已经够用，避免多一个服务单元 + 一套 auth 实体 + 一份部署 runbook。未来若流量分离需求浮现（ops 网段隔离、独立 rate-limit 域），再拆。

### 2. RBAC via role-tagged API-Key

`bff/internal/auth/apikey.go` 的 `apiKeyRecord` 增加 `role` 字段，取值 `user`（默认 / 省略 = 向后兼容）或 `admin`：

```json
{"keys":[
  {"key":"...","secret":"...","user_id":"alice","role":"user"},
  {"key":"...","secret":"...","user_id":"ops-bot-1","role":"admin"}
]}
```

- `APIKeyStore.Lookup` 返回 `(secret, userID, role, ok)`
- `auth.WithRole(ctx, role)` + `auth.Role(ctx) (string, error)` context helper
- 新增 `auth.RequireAdmin(http.Handler) http.Handler` 中间件：非 admin role 返回 403

JWT / header mode 默认 role = `user`，不会误授 admin —— 要获得 admin 必须显式走 API-Key 签名 + 使用 admin key 条目。

### 3. 审计日志 — JSONL 文件（非 Kafka）

`pkg/adminaudit`：append-only JSONL writer，每行一个 JSON 事件：

```json
{"ts_unix_ms":...,"admin_id":"ops-bot-1","op":"admin.symbol.put","target":"BTC-USDT","params":{...},"status":"ok","remote_ip":"...","request_id":"..."}
```

- 文件按 BFF 进程单写；`O_APPEND` + `fsync` on Close；rotation 交给 `logrotate`
- 失败（磁盘满 / 路径不存在）→ ERROR log + 请求仍返回 500（**审计缺失阻塞管理操作**，不允许"静默失败"）
- 未接 Kafka topic / trade-dump projection，刻意推迟：JSONL 对 MVP 级运维审计（少量请求 / 人工 grep / 运维拉日志）已经够用；上 Kafka + MySQL projection 是下一步，等到合规 / 对外审计要求落地再做

### 4. 币对生命周期端点

```
GET    /admin/symbols             # 列所有（etcd prefix scan）
GET    /admin/symbols/{symbol}    # 查单个
PUT    /admin/symbols/{symbol}    # 创建或更新（body: {shard, trading, version}）
DELETE /admin/symbols/{symbol}    # 删除
```

- 实现：在 `pkg/etcdcfg` 的 `EtcdSource` 上加 `Put(ctx, symbol, cfg) error` / `Delete(ctx, symbol) error`，直接写 `/cex/match/symbols/<symbol>` JSON。`MemorySource` 对称补齐 ctx-aware 版本（测试复用）
- 写操作之前先审计 log pending，成功后补 `status=ok`；失败 `status=failed` + 错误信息
- **trading=false 就是"halt"**：match 侧 watch 已经支持（ADR-0030），admin 侧不需要额外协议

ADR-0030 runbook 里的 `curl -X POST bff/admin/symbol/disable` 占位符本 ADR 用 `PUT /admin/symbols/{symbol}` 统一实现（RESTful；halt 只是 PUT 一个 `trading:false` 的 body）。

### 5. 批量撤单端点

```
POST /admin/cancel-orders
  body: {"user_id": "?", "symbol": "?", "reason": "..."}
```

- 至少要给 `user_id` 或 `symbol` 其一；都给表示 `user_id && symbol` 的交集
- 反馈：`{"cancelled": N, "shard_results": [{"shard_id":0,"cancelled":N}, ...]}`

**Counter 侧新 RPC `AdminCancelOrders(AdminCancelOrdersRequest)`**：

```protobuf
rpc AdminCancelOrders(AdminCancelOrdersRequest) returns (AdminCancelOrdersResponse);

message AdminCancelOrdersRequest {
  string user_id = 1;                  // 可选；空 = 该 shard 内所有 users
  string symbol = 2;                   // 可选；空 = 所有 symbols
  string reason = 3;                   // 审计用，写进 journal meta
}
message AdminCancelOrdersResponse {
  uint32 cancelled = 1;                // 本 shard 成功转为 PENDING_CANCEL 的数量
  uint32 skipped = 2;                  // 已 terminal / 已 pending_cancel 跳过数量
  int32 shard_id = 3;
}
```

**Counter service**：遍历 `OrderStore.All()`（clone slice，避免 hold 锁），按 (user, symbol, active) 过滤，逐笔走现有 `CancelOrder` 的 sequencer 路径（每笔走该 user 的 sequencer，保持事件顺序正确）。这是"一笔笔 cancel 的 sugar"，不引入新的状态迁移。

**BFF 侧路由**：
- 有 `user_id` → 通过 ShardedCounter `pick(user_id)` 单 shard
- 无 `user_id`（symbol-only 或两者都空）→ ShardedCounter 新增 `BroadcastAdminCancel`，并发调所有 shard，结果汇总

**Match 侧不新增 RPC**：counter cancel → order-event → match 的路径已经完整，admin 撤单等价于 N 笔正常 cancel。

### 6. 新 flag

BFF main：
- `--admin-api-keys-file=PATH`：独立于 `--api-keys-file` 的 admin key 文件；空 = 禁用 admin 端点（返回 404）
- `--etcd=...`：etcd 端点（复用 `pkg/etcdcfg`）；空 = `/admin/symbols` 返回 503
- `--audit-log=PATH`：JSONL 输出文件；空且 admin 启用 = boot 失败（防"生产没审计"）

Counter / match / conditional 不加新 flag。

## 备选方案 (Alternatives Considered)

### A. 独立 admin-gateway 服务

独立进程 + 独立 gRPC/REST + 独立 auth store。

- 优点：网段隔离（ops VPC）；流量 / rate-limit / 日志等级全部分离；ops 下线不影响 trading
- 缺点：多一个部署单元 + 一份 auth 实体 + 一份 routing layer + 一份 observability；MVP 没必要
- 何时重新考虑：当 admin 流量 > 10% trading 流量；或合规要求 ops plane 物理隔离

本 ADR **决定走 (i) 复用 BFF**，理由：当前 admin 流量稀疏（人工触发 + 少量 ops bot），BFF 已经有 auth / rate-limit / access log 的基础设施，复用成本 << 独立服务。未来重评估时，`/admin/*` 前缀可以直接被反代切到独立进程，不需要改协议。

### B. Audit 走 Kafka `admin-audit` topic + trade-dump projection

和现有 counter-journal / trade-event / conditional-event 的 pattern 对齐。

- 优点：查询能力（history 服务加 `ListAdminAudits`）、与主数据源同构
- 缺点：多一份 proto / 一个 producer / 一个 projection / 一张表；短期 ROI 低
- 何时重新考虑：(1) 合规要求（SOC2 / GDPR）把审计日志当独立数据资产；(2) 多 BFF 实例要统一 audit 检索；(3) 要给 admin UI 做"最近 N 次操作"列表

本 ADR 决定**先 JSONL 文件**，因为 MVP 场景下 `grep` / `tail -f` / `logrotate` 已经覆盖。升级路径清晰：`pkg/adminaudit.Logger` 就是一个接口，未来加 KafkaLogger 实现，wire 层替换即可。

### C. RBAC via 独立 admin key file（不合并进 user key file）

```
--api-keys-file=user-keys.json
--admin-api-keys-file=admin-keys.json
```

两个 store 分开加载，admin middleware 只 lookup admin store。

- 优点：物理隔离（admin key 可以放另一个磁盘 / secret manager）；权限切片清晰
- 缺点：两个文件容易不同步；dev 一键部署变复杂

本 ADR 采用 **C**（两个文件，独立 store），原因：admin key 的保存位置 / rotate 频率 / 审计要求 都和 user key 不同；合并进同一文件让运维心智负担增加（"我 rotate 了但是不小心把 admin 的 role 也改了"）。合并成一个文件 + role 字段 是更 elegant 的设计，但运维实践更差 —— 分离赢在运维层面。

---

注：**实现上 C 的"admin 文件"复用 `APIKeyStore` 类型**（一个"带 role 字段的 APIKeyStore"），但 middleware 通过 `RequireAdmin` 守卫强制要求 role=admin。这样代码复用 + 运维分离两得。

### D. 把 admin 操作做成 etcd 直写 UI

给运维一个 etcd 客户端 GUI / `etcdctl` wrapper，管理员直接写 key。

- 优点：零开发
- 缺点：绕过审计（不知道谁写了）、绕过校验（可写非法 JSON / 冲突的 shard 配置）、绕过幂等性（同一操作重放）
- **拒绝**：这是"做完一半就停下"的典型反模式，短期省开发工时但长期埋坑

### E. 在 Counter 里加"cancel 所有 X"的批量状态机

替代"逐笔 cancel"的 sugar 实现。

- 优点：避免 N 次 Kafka 事务；一次原子性
- 缺点：引入新的状态迁移 + snapshot schema 改动 + 幂等性设计；相对于"cancel sugar"是 10x 复杂度
- **拒绝**：每笔走 sequencer 对性能没影响（ops 场景 N 通常几十到几百），但语义干净、不新增审计面

## 理由 (Rationale)

选 **(i) BFF-integrated + C（独立 admin key 文件）+ JSONL 审计 + cancel sugar** 的核心依据：

1. **依赖量最小**：复用现有 BFF / ShardedCounter / etcdcfg / OrderStore，不新增 Kafka topic / proto message type / MySQL 表 / 服务进程
2. **升级路径清晰**：审计可加 KafkaLogger、admin 服务可拆、role 可细化到 resource-level RBAC，都是 "加" 不是 "改"
3. **审计硬性约束**：审计失败阻塞请求，不给"写了 etcd 但没记下"的窗口；JSONL 文件是最简单的可信存储
4. **安全默认**：`--admin-api-keys-file=""` / `--audit-log=""` / `--etcd=""` 默认全 disabled，只有显式配置才打开；dev 环境 / 不需要 admin 的生产环境完全零开销

## 影响 (Consequences)

### 正面

- 币对上下架 / 参数调整 有 API 入口 + 审计 trail（取代 ADR-0030 的裸 `etcdctl`）
- 风控 / 清算场景的按 user / 按 symbol 批量撤单可以做了
- 为后续 admin UI（web 后台）铺路
- role=admin 机制可复用给其他受限操作（例如未来的 Transfer(DEPOSIT/WITHDRAW) 权限收紧）

### 负面 / 代价

- BFF 承担 ops 面的部分责任（access log 里混着 ops 流量；监控仪表盘要分类）
- 审计 JSONL 的检索能力弱（grep / tail）；跨进程聚合只能靠 log 收集管道
- admin key 的 rotate / revoke 仍要重启 BFF（live-reload 是 0039 的遗留未实现项，本 ADR 不修）

### 中性

- 所有新增端点走现有 auth middleware + rate-limit；IP rate limit 对 admin 来说会偏紧（200/s），上线前可能要调
- Counter `AdminCancelOrders` 的 reason 字段落在 order-event 的 meta（未改 schema；reason 进 audit log 而非 counter-journal），保持主数据流 schema 稳定

## 实施约束 (Implementation Notes)

### 目录结构

```
pkg/adminaudit/
  audit.go        # Logger interface + FileLogger (JSONL)
  audit_test.go
bff/internal/rest/
  admin.go        # admin handlers (wired separately from user mux)
  admin_test.go
bff/internal/auth/
  apikey.go       # + Role field
  middleware.go   # + WithRole / Role / RequireAdmin
counter/internal/service/
  admin.go        # AdminCancelOrders
counter/internal/server/
  server.go       # + AdminCancelOrders gRPC
api/rpc/counter/
  counter.proto   # + AdminCancelOrders
pkg/etcdcfg/
  etcdcfg.go      # + EtcdSource.Put / Delete
  memory.go       # + PutCtx / DeleteCtx
```

### 路由挂载

BFF `Server.Handler()` 保持不变（user mux）。admin 路由由 `main.go` 单独 mount：

```go
outer.Handle("/admin/", requireAdmin(authMW(adminMux)))
```

这样 admin mux 走独立 rate limiter（比 user 更宽松）+ 独立 access log tag，不污染 `/v1/*` 的可观测性。

### `AdminCancelOrders` 执行路径

1. server 层 validate (`user_id != "" || symbol != ""`)，否则 `InvalidArgument`
2. service 层遍历 `OrderStore.All()`（已 clone），过滤 active 订单
3. 逐笔调 `Service.CancelOrder`（走 user 的 sequencer）；返回每笔结果
4. 汇总 cancelled / skipped 计数

失败半径：(a) 某一笔 cancel 失败不阻断后续；(b) 整个请求的幂等键是 `reason` + 操作时间戳，调用方重试可能重复撤单（对已 pending_cancel 的幂等）。

### 测试覆盖

- `pkg/adminaudit`：新文件 append 正确、并发写、重新 open 能 append
- `bff/internal/auth`：RequireAdmin 放行 admin role、拒绝 user role、ctx 没有 role 也拒（匿名 = 非 admin）
- `bff/internal/rest/admin_test`：
  - 符号 CRUD 对 MemorySource 生效
  - 审计日志一行对应一个操作
  - cancel-orders 无 user 无 symbol 返回 400
  - fan-out 计数正确（mock ShardedCounter）
- `counter/internal/service`：AdminCancelOrders 按 (user, symbol) 过滤 + sequencer serial 正确
- `pkg/etcdcfg`：MemorySource 新 PutCtx/DeleteCtx 工作；EtcdSource Put/Delete 由 clientv3 的 smoke test 覆盖（embedded etcd 不在本 ADR 引入）

## 参考 (References)

- ADR-0030 Match etcd sharding rollout — runbook 里的 symbol CRUD 占位
- ADR-0039 BFF auth JWT + API-Key — role 机制的延续
- feature-requests.md 2026-04-19 entry — 用户原话 + 现状核查
- [Binance Spot API admin endpoints](https://binance-docs.github.io/apidocs/spot/en/) — `/sapi/v1/account/apiRestrictions` 风格参考（未采用：需要账户层 RBAC，MVP 暂不需要）
