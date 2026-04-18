#!/usr/bin/env bash
# End-to-end smoke for OpenTrade.
#
# Starts every service against the docker-compose deps (kafka / etcd /
# mysql / minio) and issues two matching orders through BFF. Prints the
# MySQL trades row + balance reports so a human can eyeball correctness.
#
# Prerequisite: deploy/docker/docker-compose.yml is already up.
#   (cd deploy/docker && docker compose up -d)
#
# Usage (from repo root):
#   ./deploy/scripts/smoke.sh        # build everything, run, teardown
#   KEEP=1 ./deploy/scripts/smoke.sh # keep services running for manual poking

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

BIN="./bin"
LOGS="./logs"
DATA="./data"

mkdir -p "$BIN" "$LOGS" "$DATA/counter" "$DATA/match"

say() { printf "\n==> %s\n" "$*"; }

say "building services"
for m in counter match trade-dump quote push bff; do
  (cd "$m" && go build -o "../$BIN/$m" "./cmd/$m")
done

say "starting services (logs under $LOGS/)"
# Counter shard 0 with total-shards=1 (single-shard cluster, every user lands here).
"$BIN/counter" \
  --shard-id=0 --total-shards=1 \
  --grpc-addr=:8081 \
  --snapshot-dir="$DATA/counter" \
  > "$LOGS/counter.log" 2>&1 &
COUNTER_PID=$!

# Match: one symbol, disabled HA, static --symbols.
"$BIN/match" \
  --instance-id=match-0 --shard-id=match-0 \
  --symbols=BTC-USDT \
  --snapshot-dir="$DATA/match" \
  > "$LOGS/match.log" 2>&1 &
MATCH_PID=$!

"$BIN/trade-dump" \
  --instance-id=trade-dump-0 \
  > "$LOGS/trade-dump.log" 2>&1 &
DUMP_PID=$!

"$BIN/quote" \
  --instance-id=quote-0 \
  > "$LOGS/quote.log" 2>&1 &
QUOTE_PID=$!

"$BIN/push" \
  --instance-id=push-0 --http=:8090 \
  > "$LOGS/push.log" 2>&1 &
PUSH_PID=$!

"$BIN/bff" \
  --http-addr=:8080 \
  --counter-shards=localhost:8081 \
  --push-ws=ws://localhost:8090/ws \
  --market-brokers=localhost:9092 \
  > "$LOGS/bff.log" 2>&1 &
BFF_PID=$!

cleanup() {
  [[ "${KEEP:-0}" == "1" ]] && { say "KEEP=1 — leaving services running"; return; }
  say "tearing down"
  kill $COUNTER_PID $MATCH_PID $DUMP_PID $QUOTE_PID $PUSH_PID $BFF_PID 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

sleep 3

# Health probe
for port in 8080 8090; do
  if ! curl -fsS "http://localhost:$port/healthz" >/dev/null 2>&1; then
    # push exposes /healthz; bff does not — ignore 404 on bff.
    :
  fi
done

say "alice deposits 30000 USDT"
# 30000 USDT covers the 25000 freeze for 0.5 BTC @ 50000 with headroom.
curl -fsS -X POST http://localhost:8080/v1/transfer \
  -H "X-User-Id: alice" -H "Content-Type: application/json" \
  -d '{"transfer_id":"dep-alice-usdt","asset":"USDT","amount":"30000","type":"deposit"}' ; echo

say "bob deposits 1 BTC"
curl -fsS -X POST http://localhost:8080/v1/transfer \
  -H "X-User-Id: bob" -H "Content-Type: application/json" \
  -d '{"transfer_id":"dep-bob-btc","asset":"BTC","amount":"1","type":"deposit"}' ; echo

say "alice places limit buy 0.5 BTC @ 50000"
curl -fsS -X POST http://localhost:8080/v1/order \
  -H "X-User-Id: alice" -H "Content-Type: application/json" \
  -d '{"symbol":"BTC-USDT","side":"buy","order_type":"limit","tif":"gtc","price":"50000","qty":"0.5","client_order_id":"alice-1"}' ; echo

say "bob places limit sell 0.5 BTC @ 50000 (should match alice)"
curl -fsS -X POST http://localhost:8080/v1/order \
  -H "X-User-Id: bob" -H "Content-Type: application/json" \
  -d '{"symbol":"BTC-USDT","side":"sell","order_type":"limit","tif":"gtc","price":"50000","qty":"0.5","client_order_id":"bob-1"}' ; echo

sleep 3

say "alice's post-trade balances"
curl -fsS http://localhost:8080/v1/account -H "X-User-Id: alice" ; echo

say "bob's post-trade balances"
curl -fsS http://localhost:8080/v1/account -H "X-User-Id: bob" ; echo

say "mysql trades table"
docker exec opentrade-mysql mysql -u opentrade -popentrade opentrade \
  -e 'SELECT trade_id, symbol, price, qty, maker_user_id, taker_user_id FROM trades'

say "mysql accounts projection"
docker exec opentrade-mysql mysql -u opentrade -popentrade opentrade \
  -e 'SELECT user_id, asset, available, frozen FROM accounts ORDER BY user_id, asset'

say "done"
