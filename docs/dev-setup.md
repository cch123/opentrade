# 本地开发环境

如何把 OpenTrade 跑起来、改代码、跑测试。

**定位**：上手指南。和 [smoke.md](./smoke.md)（端到端冒烟）互补 —— smoke 是一键跑完整流程，本文是让你**知道每一步在干嘛**、方便单独起 / 断点调试。

---

## 前置依赖

- **Go 1.26+**（`go.work` 锁定，低版本 build 不过；参见 claude memory `feedback_go_version.md`）
- **Docker + docker compose**（起 Kafka / MySQL / etcd / MinIO）
- **buf**（改 proto 才需要；主线代码里生成的 Go 文件已 checkin）
- **make / bash**（smoke / proto 生成依赖）
- macOS / Linux；Windows 未验证

## 一次性初始化

```bash
git clone <repo>
cd opentrade

# 拉依赖（每个 module 独立）
go work sync

# 拉起 docker deps
cd deploy/docker && docker compose up -d
cd -

# 等 MySQL 初始化完（10s 左右），校验
docker exec opentrade-mysql mysql -u opentrade -popentrade opentrade -e 'SHOW TABLES'
# 应该看到 accounts / account_logs / orders / trades
```

## 一键跑通

```bash
./deploy/scripts/smoke.sh              # build + 起全栈 + 下两笔单 + 校验 MySQL，跑完自动清理
KEEP=1 ./deploy/scripts/smoke.sh       # 同上，但服务留着方便手工 curl
```

详见 [smoke.md](./smoke.md)。

## 分步起每个服务

smoke 脚本里的命令展开版，用于开发 / 调试。每个服务独立进程，logs 自己跟。

```bash
# 数据目录（放 snapshot）
mkdir -p data/counter data/match

# Counter（单节点持有 16 个 vshard；依赖 etcd 做 assignment）
go run ./counter/cmd/counter \
  --node-id=counter-0 \
  --node-endpoint=localhost:8081 \
  --grpc-addr=:8081 \
  --vshard-count=16 \
  --brokers=localhost:9092 \
  --etcd=localhost:2379 \
  --snapshot-backend=fs \
  --snapshot-dir=./data/counter

# Match（默认 symbol=BTC-USDT）
go run ./match/cmd/match \
  --instance-id=match-0 --shard-id=match-0 \
  --symbols=BTC-USDT \
  --brokers=localhost:9092 \
  --vshard-count=16 \
  --snapshot-dir=./data/match

# trade-dump
go run ./trade-dump/cmd/trade-dump \
  --instance-id=trade-dump-0 \
  --brokers=localhost:9092 \
  --pipelines=sql,snap \
  --mysql-dsn='opentrade:opentrade@tcp(localhost:3306)/opentrade?parseTime=true&multiStatements=true' \
  --vshard-count=16 \
  --snapshot-backend=fs \
  --snapshot-dir=./data/counter

# Quote
go run ./quote/cmd/quote --instance-id=quote-0 --brokers=localhost:9092

# Push
go run ./push/cmd/push --instance-id=push-0 --http=:8090 --brokers=localhost:9092

# History（BFF 历史数据需要）
go run ./history/cmd/history \
  --grpc=:8085 \
  --mysql-dsn='opentrade:opentrade@tcp(localhost:3306)/opentrade?parseTime=true&multiStatements=true'

# Asset（funding wallet + funding↔spot transfer saga）
go run ./asset/cmd/asset \
  --instance=asset-0 \
  --grpc=:19000 \
  --mysql-dsn='opentrade:opentrade@tcp(localhost:3306)/opentrade_asset?parseTime=true&multiStatements=true' \
  --peer-holders=spot=localhost:8081

# BFF
go run ./bff/cmd/bff \
  --http-addr=:8080 \
  --counter-shards=localhost:8081 \
  --push-ws=ws://localhost:8090/ws \
  --history=localhost:8085 \
  --asset=localhost:19000 \
  --market-brokers=localhost:9092

# 可选：trigger
go run ./trigger/cmd/trigger \
  --instance-id=trigger-0 \
  --grpc-addr=:8082 \
  --brokers=localhost:9092 \
  --counter-shards=localhost:8081 \
  --snapshot-dir=

# 可选：Web UI（浏览器验证下单流程）
go run ./tools/web --addr=localhost:7070 --bff=http://localhost:8080
```

## 常用操作

**外部入金、转到 spot、下单 / 查余额**（手工验证）：

```bash
# 开发充值：先调用 asset-service 的 funding AssetHolder，模拟外部入金。
curl -X POST http://localhost:19000/opentrade.rpc.assetholder.AssetHolder/TransferIn \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"1001","transfer_id":"funding-in-1001-1","asset":"USDT","amount":"10000","peer_biz":"external-deposit"}'

# 当前 /v1/transfer 是 biz-line 间 saga，不再接受旧的 type=deposit body。
curl -X POST http://localhost:8080/v1/transfer \
  -H 'X-User-Id: 1001' -H 'Content-Type: application/json' \
  -d '{"transfer_id":"funding-to-spot-1001-1","from_biz":"funding","to_biz":"spot","asset":"USDT","amount":"10000"}'

curl -X POST http://localhost:8080/v1/order \
  -H 'X-User-Id: 1001' -H 'Content-Type: application/json' \
  -d '{"symbol":"BTC-USDT","side":"buy","order_type":"limit","tif":"gtc","price":"50000","qty":"0.1","client_order_id":"a1"}'

curl http://localhost:8080/v1/account -H 'X-User-Id: 1001'
```

**看 MySQL projection**：

```bash
docker exec opentrade-mysql mysql -u opentrade -popentrade opentrade \
  -e 'SELECT * FROM accounts ORDER BY user_id, asset'
```

**看 Kafka topic**：

```bash
docker exec opentrade-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec opentrade-kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic counter-journal --from-beginning --max-messages 5
```

## 测试

```bash
# 全量（根目录是 multi-module workspace，统一由 Makefile 遍历）
make test

# 并发密集 module 的 race suite
make test-race

# 单 module
(cd counter && go test ./... -race -v)

# 单测试
(cd counter && go test ./internal/service -run TestSelfTrade -race -v)

# 覆盖率
(cd counter && go test ./... -cover -coverprofile=../coverage.out)
go tool cover -html=coverage.out
```

**race detector 必须开**（`-race`）—— Counter / Match 都是并发密集；没 race 过的改动不合入。

## Proto 改了要重新生成

```bash
# buf.gen.yaml 使用远程 protobuf-go + Connect-Go 插件。
make proto
```

改 proto 一般连带影响多个 module；运行 `make test`，并对并发密集模块补跑 `make test-race`。

## 停机 / 清场

```bash
# 只停服务，保留 docker deps（再次 smoke.sh 可复用）
pkill -f "bin/counter|bin/match|bin/bff|bin/push|bin/quote|bin/trade-dump|bin/trigger|bin/history"

# 完全清场（包含 docker volume 里的 Kafka / MySQL 数据）
cd deploy/docker && docker compose down -v
rm -rf data/ logs/ snapshots/
```

**注意**：`docker compose down -v` 会删 MySQL / Kafka 数据，下次得重新初始化。用于重置状态调试。

## 常见坑

- **BFF 503 from `/v1/orders`** —— 没启 history 或 BFF 没带 `--history=localhost:8085`。
- **`GET /v1/depth/` 空** —— BFF 没带 `--market-brokers=localhost:9092`，market cache 没启动。
- **Counter 启动后不消费** —— etcd 没起、`--node-id` 缺失，或 Counter / Match / trade-dump 的 `--vshard-count` 不一致。
- **Snapshot 恢复后 balance / order 异常** —— 见 [ADR-0048](./adr/0048-snapshot-offset-atomicity.md)，强制保留 snapshot + Kafka 对齐；别单独删 snapshot 保留 topic，或反过来。

---

**增补**：碰到新的 onboarding 坑 / 调试技巧 / 新子服务时回来更新本文。
