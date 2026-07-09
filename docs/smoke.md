# Integration smoke

端到端验证当前现货主链路：

```text
external deposit
  -> Asset funding wallet
  -> BFF /v1/transfer saga (funding -> spot)
  -> Counter spot balance
  -> BFF order -> Match
  -> trade-event -> Counter settlement
  -> counter-journal -> trade-dump -> MySQL
```

外部契约、Proto、恢复逻辑或跨服务数据流变化后，都应跑一次。

## 前置条件

```bash
docker compose -f deploy/docker/docker-compose.yml up -d
```

脚本会检查 Kafka / MySQL、补齐 Kafka topic 分区，并幂等重放现货与
asset-service 的 MySQL schema。Docker 依赖本身仍需提前启动。

## 一键运行

```bash
./deploy/scripts/smoke.sh
```

脚本会构建并启动 Counter、Match、trade-dump、Quote、Push、History、
Asset 和 BFF，日志写到 `./logs`。默认使用数字用户 `1001` / `1002`，
每次运行生成新的 transfer / order id，避免命中旧的幂等记录。

可覆盖用户或保留进程做手工排查：

```bash
BUYER_ID=2001 SELLER_ID=2002 ./deploy/scripts/smoke.sh
KEEP=1 ./deploy/scripts/smoke.sh
```

`KEEP=1` 只保留 Go 服务；Docker 依赖始终由操作者自行管理。

## 脚本实际验证什么

1. 确认 Kafka 与 MySQL 可用，topic 至少覆盖 16 个 vshard。
2. 等 Counter 从 etcd 获得两个测试用户的 vshard ownership。
3. 直接调用 AssetHolder `TransferIn`，模拟外部系统给 funding wallet 入金。
4. 通过 BFF `/v1/transfer` 发起 `funding -> spot` saga，并等待 `COMPLETED`。
5. 用户 1001 先挂 `0.5 BTC @ 50000 USDT` 买单；用户 1002 下同价卖单撮合。
6. 等 `trades` 行数增长，证明 trade-dump 已完成 MySQL 投影。
7. 输出 Counter 实时余额、最新成交和 MySQL account projection。

在干净测试用户上，免手续费后的 spot 余额应为：

```text
1001: BTC available=0.5, USDT available=5000
1002: BTC available=0.5, USDT available=25000
```

重复使用同一用户会累积之前运行的余额；脚本判断成功依赖的是本次新成交，
而不是上述绝对余额。

## 手工排查

### 1. 检查依赖

```bash
docker exec opentrade-kafka \
  kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null
docker exec opentrade-mysql mysqladmin -u root -proot ping
docker exec opentrade-etcd etcdctl endpoint health
```

### 2. 分服务启动

当前完整参数见 [dev-setup.md](./dev-setup.md#分步起每个服务)。关键约束是：

- Counter 必须配置 `--node-id`、`--etcd` 和 `--vshard-count`；旧的
  `--shard-id/--total-shards` 已删除。
- Counter、Match 与 trade-dump 的 `--vshard-count` 必须一致。
- BFF `/v1/transfer` 依赖 Asset 服务；History 负责历史查询。
- Counter 与 trade-dump 应指向同一个 Counter snapshot 目录或对象存储。

### 3. 等 Counter ownership

```bash
curl -X POST \
  http://localhost:8081/opentrade.rpc.counter.CounterService/QueryBalance \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"1001"}'
```

启动初期若返回 `FailedPrecondition`，等 etcd coordinator 完成 assignment 后重试。

### 4. 外部入金，再转入 spot

```bash
# 模拟外部入金到 funding wallet。
curl -X POST \
  http://localhost:19000/opentrade.rpc.assetholder.AssetHolder/TransferIn \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"1001","transfer_id":"manual-deposit-1001-1","asset":"USDT","amount":"30000","peer_biz":"external-deposit"}'

# 当前 /v1/transfer 是 biz-line saga；旧的 type=deposit body 已失效。
curl -X POST http://localhost:8080/v1/transfer \
  -H 'X-User-Id: 1001' \
  -H 'Content-Type: application/json' \
  -d '{"transfer_id":"manual-funding-to-spot-1001-1","from_biz":"funding","to_biz":"spot","asset":"USDT","amount":"30000"}'
```

### 5. 下单并检查投影

```bash
curl -X POST http://localhost:8080/v1/order \
  -H 'X-User-Id: 1001' \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"BTC-USDT","side":"buy","order_type":"limit","tif":"gtc","price":"50000","qty":"0.5","client_order_id":"manual-buy-1"}'

docker exec opentrade-mysql mysql -u opentrade -popentrade opentrade \
  -e 'SELECT * FROM trades ORDER BY ts DESC LIMIT 5'
```

### 6. 观察 WebSocket

```bash
websocat 'ws://localhost:8080/ws' -H 'X-User-Id: 1001'
# 输入：{"op":"subscribe","streams":["trade@BTC-USDT","depth@BTC-USDT"]}
```

## 常见失败

| 症状 | 优先检查 |
|---|---|
| Counter 启动即报 unknown flag | 仍在使用已删除的 `--shard-id/--total-shards` |
| Counter 一直 `FailedPrecondition` | etcd 是否健康、node lease/assignment 是否建立 |
| Counter producer 报 partition 越界 | Kafka topic 分区少于 `--vshard-count` |
| `/v1/transfer` 返回 503 | Asset 未启动，或 BFF 缺少 `--asset=localhost:19000` |
| transfer 失败或补偿 | 查 `asset.log` 与 `counter.log`，确认 funding 先完成入金 |
| 下单 insufficient balance | funding→spot saga 未到 `COMPLETED`，或 user id 不一致 |
| `trades` 一直不增长 | 查 `match.log`、`counter.log`、`trade-dump.log` |
| History 端点 503 | History 未启动，或 BFF 缺少 `--history=localhost:8085` |

## 不覆盖的范围

- 多 Counter 节点迁移 / failover。
- S3 snapshot backend 与跨节点恢复。
- Trigger、永续合约、强平、ADL。
- 压测与延迟基准。
