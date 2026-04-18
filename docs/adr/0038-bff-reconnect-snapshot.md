# ADR-0038: BFF 重连补齐快照

- 状态: Accepted
- 日期: 2026-04-18
- 决策者: xargin, Claude
- 相关 ADR: 0021, 0022, 0026, 0029

## 背景 (Context)

ADR-0022 和 ADR-0026 都留了一句话：客户端断线重连后，从 BFF 拉"期间遗漏
的状态"；Push 本身只送"连上之后"的流。这条规则 MVP-7 里只是一句文档——
BFF 并没有这样的 REST endpoint，Push 上游也没做任何 replay。

没有补齐的后果，断线几秒钟的客户端要么自己从交易记录 / 链路历史拼回状
态，要么干脆等下一条实时事件覆盖旧视图。Depth 尤其糟糕：若 DepthUpdate 
是增量 diff，断线期间的一条 diff 就让客户端的 book 永远偏离真相。

## 决策 (Decision)

### 1. BFF 端跑一个 market-data cache

BFF 加一个 goroutine 订阅 `market-data` Kafka topic（consumer group 可
不 commit，重启从 tail 开始——同 Push）。消费端只看两种 payload：

- `DepthSnapshot` → 覆盖写 `cache.depth[symbol]`，latest-wins
- `KlineClosed` → 追加进 `cache.klines[(symbol,interval)]` 的 ring
  buffer（默认 500 条/桶）

其它（DepthUpdate / PublicTrade / KlineUpdate）**不缓存**：
- DepthUpdate 是增量，客户端拿 snapshot 重建即可
- PublicTrade 需要历史就查 MySQL `trades` 表（留 backlog）
- KlineUpdate 是 open bar 的即时状态，下一条 live 事件自然覆盖，无需 replay

### 2. 新增两个公开 REST

```
GET /v1/depth/{symbol}
  200 {"symbol":"BTC-USDT", "bids":[...], "asks":[...]}
  404 若 BFF 还没从 Kafka 收到过该 symbol 的 snapshot
  503 若 BFF 没启用 market cache（--market-brokers 空）

GET /v1/klines/{symbol}?interval=1m&limit=200
  200 {"symbol":"BTC-USDT","interval":"1m","klines":[{...},{...}]}
  400 interval 不在 {1m,5m,15m,1h,1d} / limit 非正
  503 同上
```

两个 endpoint 都对未登录用户开放（只走 IP rate limit）；需要身份的 /v1/order
/ /v1/account 不变。

### 3. 运维开关

BFF 加三个 flag：

```
--market-brokers  ""       # 空则禁用 cache + endpoints（503）
--market-topic    market-data
--market-group    bff-md-<host>  # 默认按 hostname 生成
--kline-buffer    500       # 每 (symbol,interval) 保留条数
```

默认不启用，保留"纯 pass-through BFF"的兼容行为；上线需要显式打开。

## 备选方案 (Alternatives Considered)

### A. BFF 通过 gRPC 向 Quote 请求 snapshot
- 优点：Quote 已经维护 depth 书，数据最权威
- 缺点：Quote 没 gRPC 接口；引入新 RPC 意味着 proto + client 代码 +
  多一个运维维度（Quote 暴露端口、与 BFF 拓扑耦合）
- 结论：直接让 BFF 订阅 market-data 更简洁——Quote 本来就广播状态

### B. BFF 只查 MySQL（trades / 历史 kline）
- 缺点：
  - Depth 没持久化，MySQL 里没有
  - Kline 没投影到 MySQL（MVP 阶段）
  - 小规模断线（几秒）走 MySQL 不经济，上游数据库压力反而大
- 结论：in-memory cache + Kafka 直接喂

### C. BFF 不做 cache，客户端自己订阅 depth.snapshot stream + 本地 dedupe
- 优点：BFF 纯 stateless
- 缺点：客户端首次加载也要等下一个 snapshot tick（默认 5s），首屏延迟翻倍
- 结论：REST 补齐才是"即时可用"，WS 增量才是"持续对账"；两者互补

### D. 把 cache 放 Push 进程
- 缺点：Push 是有状态连接层，挂 HTTP endpoints 会耦合 WS 会话和 REST
  生命周期；REST 应该走 BFF 入口（ADR-0029 明确 BFF 是唯一 HTTP 入口）
- 结论：Push 专注 WS fan-out，不长 HTTP 业务

## 理由 (Rationale)

- **客户端契约简单**：断线后 `GET /v1/depth/{symbol}` 取 snapshot，resubscribe
  WS；KLine 场景先 `GET /v1/klines/...` 把 open bar 前的历史补齐，再接住
  live KlineUpdate
- **BFF 扩展闭合**：新加的 consumer/cache 独立于现有 counter gRPC / WS 代理
  路径，互不影响
- **可关闭**：`--market-brokers ""` 时 endpoints 返回 503，与 MVP-7 语义一
  致；测试 / 离线环境不必起 Kafka cache

## 影响 (Consequences)

### 正面

- 客户端可以在断线时明确向 BFF 询问"我错过了什么"
- Depth 永远可以在几秒内 resync（以 Quote 的 DepthSnapshot ticker 节奏为
  准——MVP 默认 5s）
- BFF 变得更像 Binance `/api/v3/depth` + `/api/v3/klines`，对标熟悉度高

### 负面 / 代价

- BFF 有状态了（cache）——多实例 BFF 时每个实例订全量 market-data，流量
  乘以 N。对 MVP 规模可接受（一台 BFF）
- Kline ring buffer 占 `symbol数 × interval数 × 500` 条；千 symbol × 5
  interval × ~100B/bar ≈ 250MB。可以调低 `--kline-buffer`
- 新 Kafka consumer 多一个运维线：监控 lag、group 管理
- Depth 返回的是 "BFF 最近收到的 snapshot"，略落后 Quote 本体（最多一个
  snapshot interval）；客户端应用 WS 增量时应该按 snapshot ts 对齐（目前
  `DepthSnapshot` proto 有 meta / 序号，客户端能判断）

### 中性

- 响应 JSON 结构和 WS 数据帧一致（都是 protojson 衍生形状），两端可以
  复用 decoder

## 实施约束 (Implementation Notes)

### 新增包

- `bff/internal/marketcache/cache.go` — 线程安全 cache：
  - `PutDepthSnapshot(snap)` / `DepthSnapshot(symbol)`
  - `AppendKlineClosed(symbol, closed)` / `RecentKlines(symbol, interval, limit)`
  - 内部 ring buffer（capacity=cfg.KlineBuffer，默认 500，FIFO overwrite）
- `bff/internal/marketcache/consumer.go` — Kafka 消费 + 派发

### REST 改动

- `rest.Server` 多一个字段 `market *marketcache.Cache`（可 nil）
- 新 handler：`bff/internal/rest/market.go`（handleDepthSnapshot /
  handleKlinesRecent + `parseKlineInterval` + `klineIntervalLabel`）
- 新路由：
  - `GET /v1/depth/{symbol}` → `handleDepthSnapshot`
  - `GET /v1/klines/{symbol}` → `handleKlinesRecent`
  - 走正常的 auth middleware（header 可省）+ rate limit（IP 计数）

### main.go 改动

- 解析 `--market-brokers` / `--market-topic` / `--market-group` /
  `--kline-buffer`
- `startMarketCache(cfg)` 返回 `(Cache, Consumer, error)`；consumer run 在
  后台 goroutine，`sync.WaitGroup` 等退出

### 失败场景

| 场景 | 结果 |
|---|---|
| BFF 启动时 Kafka 不可达 | `kgo.NewClient` 失败 → `Fatal` 阻止启动；和 MVP-12 的 Counter HA 行为一致 |
| 消费中途 Kafka 抖动 | `fetches.IsClientClosed() == false` 下 `PollFetches` 继续重试；错误记 Warn |
| Quote 挂了（没新 DepthSnapshot） | cache 的 snapshot 变旧；客户端能察觉（snapshot 里带 ts），UI 可自行决定要不要降级 |
| `--market-brokers ""` | cache / consumer 都是 nil；endpoint 返回 503 |

## 未来工作 (Future Work)

- **Trades 补齐**：`GET /v1/trades/{symbol}` 查 MySQL `trades` 表（要
  加分页 + 索引覆盖）
- **Depth update merge**：BFF 对 DepthUpdate 累积成完整 book（可选，复杂）
- **Snapshot 的 sequence 对齐**：把 `DepthSnapshot.meta.seq_id` 暴露在
  REST 响应里，客户端用它过滤 WS 里 `seq_id < snap.seq_id` 的增量
- **统一缓存 TTL**：目前 cache 没 TTL；symbol 下架后 map 里还留着——
  一般几千 symbol 内存可忽略，上量后需要清理
- **Prometheus metrics**：cache hits / misses、consumer lag、depth snapshot
  age

## 参考 (References)

- ADR-0021: Quote 服务与 market-data
- ADR-0022: Push 分片与 sticky
- ADR-0026: Push WS 协议与 MVP-7 范围（本 ADR 的动机）
- ADR-0029: BFF WebSocket 反向代理
- 实现：
  - [bff/internal/marketcache/cache.go](../../bff/internal/marketcache/cache.go)
  - [bff/internal/marketcache/consumer.go](../../bff/internal/marketcache/consumer.go)
  - [bff/internal/rest/market.go](../../bff/internal/rest/market.go)
  - [bff/cmd/bff/main.go](../../bff/cmd/bff/main.go)
