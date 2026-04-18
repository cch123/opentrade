# Integration smoke

End-to-end sanity check for OpenTrade: one matching pair of orders walks
through **BFF → Counter → Match → trade-event → Counter settlement →
counter-journal → trade-dump → MySQL**, with WS push observable on the side.

Use this every time the external contract changes (proto, gRPC, schema) or
before committing something that crosses service boundaries.

## Prerequisites

```
(cd deploy/docker && docker compose up -d)   # kafka + etcd + mysql + minio
```

Wait ~10s for mysql to finish running `deploy/docker/mysql-init/01-schema.sql`.
Confirm:

```
docker exec opentrade-mysql mysql -u opentrade -popentrade opentrade \
  -e 'SHOW TABLES'
# expect: account_logs / accounts / orders / trades
```

## One-shot runner

```
./deploy/scripts/smoke.sh
```

The script builds all six binaries into `./bin`, runs them in the
background with logs under `./logs`, deposits funds for two users,
matches a buy + sell on BTC-USDT, prints both balances + the MySQL
`trades` / `accounts` rows, and tears the processes down on exit.

`KEEP=1 ./deploy/scripts/smoke.sh` leaves the services running so you can
curl more from another terminal.

## What you should see

**Balances after the trade** (alice bought 0.5 BTC @ 50000):

```
alice:  BTC available=0.5  frozen=0
        USDT available=-25000 + 10000 = -15000   (will be 0 / fee-free)
bob:    BTC available=0.5    frozen=0  (was 1)
        USDT available=25000 frozen=0
```

For MVP the Counter does not deduct fees, so both balances should move by
exactly 0.5 BTC and 25 000 USDT.

**MySQL trades** — a single row:

```
trade_id         | symbol   | price | qty | maker_user_id | taker_user_id
BTC-USDT:<seq>   | BTC-USDT | 50000 | 0.5 | alice         | bob
```

(`maker` is whoever rested first — here alice.)

**MySQL accounts** — both users show post-trade balances.

## Manual walk-through (debug mode)

If the script blows up, run each step by hand and check the individual
service logs.

1. **Health of deps**
   ```
   docker exec opentrade-kafka \
     kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null
   docker exec opentrade-mysql mysqladmin -u root -proot ping
   ```

2. **Start services** (each in its own terminal is easiest):
   ```
   go run ./counter/cmd/counter -shard-id=0 -total-shards=1 -grpc-addr=:8081 -snapshot-dir=./data/counter
   go run ./match/cmd/match -instance-id=match-0 -shard-id=match-0 -symbols=BTC-USDT -snapshot-dir=./data/match
   go run ./trade-dump/cmd/trade-dump -instance-id=trade-dump-0
   go run ./quote/cmd/quote -instance-id=quote-0
   go run ./push/cmd/push -instance-id=push-0 -http=:8090
   go run ./bff/cmd/bff -http-addr=:8080 -counter-shards=localhost:8081 -push-ws=ws://localhost:8090/ws
   ```

3. **Deposit**:
   ```
   curl -s -X POST http://localhost:8080/v1/transfer \
     -H 'X-User-Id: alice' -H 'Content-Type: application/json' \
     -d '{"transfer_id":"dep-alice","asset":"USDT","amount":"10000","type":"deposit"}' | jq
   ```

4. **Place order** and watch `logs/counter.log`:
   ```
   curl -s -X POST http://localhost:8080/v1/order \
     -H 'X-User-Id: alice' -H 'Content-Type: application/json' \
     -d '{"symbol":"BTC-USDT","side":"buy","order_type":"limit","tif":"gtc","price":"50000","qty":"0.5"}' | jq
   ```
   - Counter logs should show `PlaceOrder` + a `BeginTransaction → commit`.
   - Match logs should show the order being placed on the book.

5. **Watch trade-event land in MySQL** (may take ~1s):
   ```
   docker exec opentrade-mysql mysql -u opentrade -popentrade opentrade \
     -e 'SELECT * FROM trades'
   ```

6. **Subscribe to push over WS**:
   ```
   websocat 'ws://localhost:8080/ws' -H 'X-User-Id: alice'
   # then type: {"op":"subscribe","streams":["trade@BTC-USDT","depth@BTC-USDT"]}
   ```
   You'll see public `trade@BTC-USDT` frames as other users match; private
   `user` frames arrive automatically after an authenticated connection.

## Common failure modes

| symptom | likely cause |
|---|---|
| `PlaceOrder` 返回 `insufficient balance` | deposit 先于 order 顺序，或 total-shards 和 BFF --counter-shards 不匹配导致请求落到错 shard |
| Counter 日志 `ErrWrongShard` | 同上 |
| `trades` 表为空 | trade-event 没产出（match 没撮合）或 trade-dump 连接 MySQL 失败 — 查 trade-dump 日志 |
| `accounts` 表为空 | counter-journal 未被 trade-dump 消费 — 查 `journal` consumer 日志 |
| WS 连接 403 | push 的 `--total-instances > 1` 但 user hash 不属于本实例 — 单实例冒烟要用默认 total-instances=1 |
| Counter 启动时 Kafka 连接失败 | kafka 还没起健康；`docker compose ps` 等 kafka healthy 再启动 |

## Scope

此脚本**不**覆盖：

- HA（`--ha-mode=auto`）—— 需要起两个 counter + 一个 etcd，另开 `docs/ha-smoke.md`（Backlog）。
- 多 shard routing —— 需要起 N counter 进程和调整 BFF `--counter-shards`，另开冒烟。
- 压测 / 延迟基准 —— 另一个 Backlog 项（architecture.md §18.3）。
- Quote 的行情订阅验证 —— push 的 subscribe 流程在手动步骤里可选。
