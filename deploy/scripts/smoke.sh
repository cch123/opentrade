#!/usr/bin/env bash
# End-to-end smoke for the current OpenTrade spot stack.
#
# Flow under test:
#   external deposit -> asset funding wallet -> BFF transfer saga -> Counter
#   spot wallet -> BFF order -> Match -> Counter settlement -> MySQL projection.
#
# Prerequisite: deploy/docker/docker-compose.yml is already up.
# Usage from the repository root:
#   ./deploy/scripts/smoke.sh
#   KEEP=1 ./deploy/scripts/smoke.sh  # leave Go services running for debugging

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

BUYER_ID="${BUYER_ID:-1001}"
SELLER_ID="${SELLER_ID:-1002}"
RUN_ID="${SMOKE_RUN_ID:-$(date +%s)-$$}"

BIN="./bin"
# A separate directory avoids corrupting logs owned by services launched from
# tools/web and makes a failed run's diagnostics self-contained.
LOGS="${SMOKE_LOG_DIR:-./logs/smoke-$RUN_ID}"
DATA="./data"
COUNTER_SNAPSHOT_DIR="$DATA/counter"
MATCH_SNAPSHOT_DIR="$DATA/match"

KAFKA_BROKERS="localhost:9092"
ETCD_ENDPOINTS="localhost:2379"
MYSQL_DSN="opentrade:opentrade@tcp(localhost:3306)/opentrade?parseTime=true&multiStatements=true"
ASSET_MYSQL_DSN="opentrade:opentrade@tcp(localhost:3306)/opentrade_asset?parseTime=true&multiStatements=true"
VSHARD_COUNT=16

PIDS=()
NAMES=()
LAST_PID=""

mkdir -p "$BIN" "$LOGS" "$COUNTER_SNAPSHOT_DIR" "$MATCH_SNAPSHOT_DIR"

say() { printf "\n==> %s\n" "$*"; }
die() { printf "\nsmoke: %s\n" "$*" >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

require_port_free() {
  local name="$1" port="$2"
  # Every listener started below speaks HTTP (including Connect RPC). Avoid
  # depending on lsof or netcat, neither of which is guaranteed in dev images.
  if curl -sS --connect-timeout 1 --max-time 2 -o /dev/null \
      "http://localhost:$port/" 2>/dev/null; then
    die "$name cannot start: TCP port $port is already in use"
  fi
}

start_service() {
  local name="$1"
  shift
  "$@" > "$LOGS/$name.log" 2>&1 &
  LAST_PID=$!
  PIDS+=("$LAST_PID")
  NAMES+=("$name")
}

show_failure_logs() {
  local name
  (( ${#NAMES[@]} > 0 )) || return 0
  for name in "${NAMES[@]}"; do
    if [[ -f "$LOGS/$name.log" ]]; then
      printf "\n---- %s (last 80 lines, capped at 64 KiB) ----\n" "$name" >&2
      tail -c 65536 "$LOGS/$name.log" 2>/dev/null | tail -n 80 >&2 || true
    fi
  done
}

cleanup() {
  local status=$?
  if (( status != 0 )); then
    show_failure_logs
  fi
  if [[ "${KEEP:-0}" == "1" ]]; then
    say "KEEP=1 — leaving services running: ${PIDS[*]:-none}"
    return
  fi
  if (( ${#PIDS[@]} > 0 )); then
    say "tearing down Go services"
    kill "${PIDS[@]}" 2>/dev/null || true
    local pid
    for pid in "${PIDS[@]}"; do
      wait "$pid" 2>/dev/null || true
    done
  fi
}
trap cleanup EXIT

wait_reachable() {
  local name="$1" pid="$2" url="$3"
  local attempt
  for attempt in $(seq 1 60); do
    # Check the process before the endpoint. Otherwise an already-running
    # service on the same port can make a failed bind look healthy.
    kill -0 "$pid" 2>/dev/null || die "$name exited before becoming reachable (see $LOGS/$name.log)"
    if curl -sS --connect-timeout 1 --max-time 2 -o /dev/null "$url" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  die "$name did not become reachable at $url"
}

wait_healthy() {
  local name="$1" pid="$2" url="$3"
  local attempt
  for attempt in $(seq 1 60); do
    kill -0 "$pid" 2>/dev/null || die "$name exited before becoming healthy (see $LOGS/$name.log)"
    if curl -fsS --connect-timeout 1 --max-time 2 "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  die "$name did not become healthy at $url"
}

wait_counter_owner() {
  local user_id="$1"
  local url="http://localhost:8081/opentrade.rpc.counter.CounterService/QueryBalance"
  local body response attempt
  body=$(printf '{"user_id":"%s"}' "$user_id")
  for attempt in $(seq 1 75); do
    if response=$(curl -fsS --max-time 2 -H 'Content-Type: application/json' --data "$body" "$url" 2>/dev/null); then
      printf '%s\n' "$response"
      return 0
    fi
    kill -0 "$COUNTER_PID" 2>/dev/null || die "counter exited while waiting for vshard ownership"
    sleep 1
  done
  die "counter never became owner for user_id=$user_id"
}

ensure_topic() {
  local topic="$1" target="$2" description current
  if ! description=$(docker exec opentrade-kafka kafka-topics.sh \
      --bootstrap-server localhost:9092 --describe --topic "$topic" 2>/dev/null); then
    docker exec opentrade-kafka kafka-topics.sh \
      --bootstrap-server localhost:9092 --create --topic "$topic" \
      --partitions "$target" --replication-factor 1 >/dev/null
    return
  fi
  current=$(printf '%s\n' "$description" | awk -F '\t' \
    '/^Topic:/{for (i=1;i<=NF;i++) if ($i ~ /^PartitionCount: /) {sub("PartitionCount: ","",$i); print $i; exit}}')
  [[ -n "$current" ]] || die "could not read partition count for Kafka topic $topic"
  if (( current < target )); then
    docker exec opentrade-kafka kafka-topics.sh \
      --bootstrap-server localhost:9092 --alter --topic "$topic" \
      --partitions "$target" >/dev/null
  fi
}

mysql_scalar() {
  docker exec opentrade-mysql mysql -N -s -u opentrade -popentrade opentrade -e "$1" 2>/dev/null
}

post_json() {
  local url="$1" body="$2"
  curl -fsS --max-time 20 -H 'Content-Type: application/json' --data "$body" "$url"
}

funding_deposit() {
  local user_id="$1" asset="$2" amount="$3" transfer_id="$4"
  local body response
  body=$(printf '{"user_id":"%s","transfer_id":"%s","asset":"%s","amount":"%s","peer_biz":"external-deposit","memo":"smoke funding deposit"}' \
    "$user_id" "$transfer_id" "$asset" "$amount")
  response=$(post_json \
    'http://localhost:19000/opentrade.rpc.assetholder.AssetHolder/TransferIn' "$body")
  printf '%s\n' "$response"
  if [[ "$response" != *'TRANSFER_STATUS_CONFIRMED'* && "$response" != *'TRANSFER_STATUS_DUPLICATED'* ]]; then
    die "funding deposit was not confirmed: $response"
  fi
}

transfer_to_spot() {
  local user_id="$1" asset="$2" amount="$3" transfer_id="$4"
  local body response attempt
  body=$(printf '{"transfer_id":"%s","from_biz":"funding","to_biz":"spot","asset":"%s","amount":"%s","memo":"smoke funding to spot"}' \
    "$transfer_id" "$asset" "$amount")
  response=$(curl -fsS --max-time 20 -H "X-User-Id: $user_id" \
    -H 'Content-Type: application/json' --data "$body" http://localhost:8080/v1/transfer)
  printf '%s\n' "$response"
  if [[ "$response" == *'"state":"COMPLETED"'* && "$response" == *'"terminal":true'* ]]; then
    return 0
  fi
  [[ "$response" != *'"terminal":true'* ]] || die "transfer $transfer_id terminated unsuccessfully: $response"
  for attempt in $(seq 1 20); do
    response=$(curl -fsS --max-time 5 -H "X-User-Id: $user_id" \
      "http://localhost:8080/v1/transfer/$transfer_id")
    if [[ "$response" == *'"state":"COMPLETED"'* ]]; then
      printf '%s\n' "$response"
      return 0
    fi
    [[ "$response" != *'"terminal":true'* ]] || die "transfer $transfer_id terminated unsuccessfully: $response"
    sleep 1
  done
  die "transfer $transfer_id did not complete: $response"
}

place_order() {
  local user_id="$1" body="$2" response
  response=$(curl -fsS --max-time 20 -H "X-User-Id: $user_id" \
    -H 'Content-Type: application/json' --data "$body" http://localhost:8080/v1/order)
  printf '%s\n' "$response"
  [[ "$response" == *'"accepted":true'* ]] || die "order was not accepted: $response"
}

require_cmd go
require_cmd curl
require_cmd docker

[[ "$BUYER_ID" =~ ^[1-9][0-9]*$ ]] || die "BUYER_ID must be a positive numeric user id"
[[ "$SELLER_ID" =~ ^[1-9][0-9]*$ ]] || die "SELLER_ID must be a positive numeric user id"
[[ "$BUYER_ID" != "$SELLER_ID" ]] || die "BUYER_ID and SELLER_ID must differ"

say "checking service ports"
require_port_free bff 8080
require_port_free counter 8081
require_port_free history 8085
require_port_free push 8090
require_port_free asset 19000
require_port_free asset-metrics 19090

say "checking docker dependencies"
docker exec opentrade-kafka kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null
docker exec opentrade-mysql mysqladmin ping -u root -proot --silent >/dev/null

# Existing dev volumes may predate asset-service. Re-applying these idempotent
# schemas makes the smoke independent of when the volume was first created.
docker exec -i opentrade-mysql mysql -u root -proot opentrade \
  < deploy/docker/mysql-init/01-schema.sql
docker exec -i opentrade-mysql mysql -u root -proot \
  < deploy/docker/mysql-init/02-asset-schema.sql

say "ensuring Kafka topic widths"
ensure_topic counter-journal "$VSHARD_COUNT"
ensure_topic trade-event "$VSHARD_COUNT"
ensure_topic order-event-BTC-USDT 4
ensure_topic market-data 4

say "building current spot services"
for module in counter match trade-dump quote push history asset bff; do
  (cd "$module" && go build -o "../$BIN/$module" "./cmd/$module")
done

say "starting counter"
start_service counter "$BIN/counter" \
  --node-id=counter-0 \
  --node-endpoint=localhost:8081 \
  --grpc-addr=:8081 \
  --vshard-count="$VSHARD_COUNT" \
  --cluster-root=/cex/counter \
  --brokers="$KAFKA_BROKERS" \
  --etcd="$ETCD_ENDPOINTS" \
  --snapshot-backend=fs \
  --snapshot-dir="$COUNTER_SNAPSHOT_DIR" \
  --env=dev
COUNTER_PID=$LAST_PID
wait_reachable counter "$COUNTER_PID" http://localhost:8081/

say "starting match, projections, push, history, and asset"
start_service match "$BIN/match" \
  --instance-id=match-0 --shard-id=match-0 \
  --symbols=BTC-USDT \
  --brokers="$KAFKA_BROKERS" \
  --vshard-count="$VSHARD_COUNT" \
  --snapshot-dir="$MATCH_SNAPSHOT_DIR" \
  --env=dev
MATCH_PID=$LAST_PID

start_service trade-dump "$BIN/trade-dump" \
  --instance-id=trade-dump-0 \
  --brokers="$KAFKA_BROKERS" \
  --pipelines=sql,snap \
  --mysql-dsn="$MYSQL_DSN" \
  --vshard-count="$VSHARD_COUNT" \
  --snapshot-backend=fs \
  --snapshot-dir="$COUNTER_SNAPSHOT_DIR" \
  --env=dev
DUMP_PID=$LAST_PID

start_service quote "$BIN/quote" \
  --instance-id=quote-0 --brokers="$KAFKA_BROKERS" --env=dev
QUOTE_PID=$LAST_PID

start_service push "$BIN/push" \
  --instance-id=push-0 --http=:8090 --brokers="$KAFKA_BROKERS" --env=dev
PUSH_PID=$LAST_PID

start_service history "$BIN/history" \
  --instance-id=history-0 --grpc=:8085 --mysql-dsn="$MYSQL_DSN" --env=dev
HISTORY_PID=$LAST_PID

start_service asset "$BIN/asset" \
  --instance=asset-0 --grpc=:19000 --metrics-addr=:19090 \
  --mysql-dsn="$ASSET_MYSQL_DSN" \
  --peer-holders=spot=localhost:8081 \
  --env=dev
ASSET_PID=$LAST_PID

wait_healthy push "$PUSH_PID" http://localhost:8090/healthz
wait_reachable history "$HISTORY_PID" http://localhost:8085/
wait_reachable asset "$ASSET_PID" http://localhost:19000/

say "starting BFF"
start_service bff "$BIN/bff" \
  --http-addr=:8080 \
  --counter-shards=localhost:8081 \
  --push-ws=ws://localhost:8090/ws \
  --history=localhost:8085 \
  --asset=localhost:19000 \
  --market-brokers="$KAFKA_BROKERS" \
  --env=dev
BFF_PID=$LAST_PID
wait_healthy bff "$BFF_PID" http://localhost:8080/healthz

say "waiting for Counter vshard ownership"
wait_counter_owner "$BUYER_ID" >/dev/null
wait_counter_owner "$SELLER_ID" >/dev/null

BUYER_DEPOSIT_ID="smoke-deposit-$RUN_ID-$BUYER_ID-USDT"
SELLER_DEPOSIT_ID="smoke-deposit-$RUN_ID-$SELLER_ID-BTC"
BUYER_TRANSFER_ID="smoke-transfer-$RUN_ID-$BUYER_ID-USDT"
SELLER_TRANSFER_ID="smoke-transfer-$RUN_ID-$SELLER_ID-BTC"

say "funding wallets receive external deposits"
funding_deposit "$BUYER_ID" USDT 30000 "$BUYER_DEPOSIT_ID"
funding_deposit "$SELLER_ID" BTC 1 "$SELLER_DEPOSIT_ID"

say "BFF transfers funding balances into spot"
transfer_to_spot "$BUYER_ID" USDT 30000 "$BUYER_TRANSFER_ID"
transfer_to_spot "$SELLER_ID" BTC 1 "$SELLER_TRANSFER_ID"

TRADES_BEFORE=$(mysql_scalar 'SELECT COUNT(*) FROM trades')

say "buyer $BUYER_ID places limit buy 0.5 BTC @ 50000"
place_order "$BUYER_ID" "$(printf '{\"symbol\":\"BTC-USDT\",\"side\":\"buy\",\"order_type\":\"limit\",\"tif\":\"gtc\",\"price\":\"50000\",\"qty\":\"0.5\",\"client_order_id\":\"smoke-buy-%s\"}' "$RUN_ID")"

say "seller $SELLER_ID places matching limit sell"
place_order "$SELLER_ID" "$(printf '{\"symbol\":\"BTC-USDT\",\"side\":\"sell\",\"order_type\":\"limit\",\"tif\":\"gtc\",\"price\":\"50000\",\"qty\":\"0.5\",\"client_order_id\":\"smoke-sell-%s\"}' "$RUN_ID")"

say "waiting for trade-dump MySQL projection"
for _ in $(seq 1 30); do
  TRADES_AFTER=$(mysql_scalar 'SELECT COUNT(*) FROM trades')
  if (( TRADES_AFTER > TRADES_BEFORE )); then
    break
  fi
  sleep 1
done
(( TRADES_AFTER > TRADES_BEFORE )) || die "no new trade reached MySQL"

# The trade row can arrive before Counter consumes the trade-event and emits
# settlement journals. Do not declare success while MySQL still shows the
# placement-time freeze: the smoke contract includes the account projection.
say "waiting for settled MySQL account projection"
ACCOUNTS_READY=0
for _ in $(seq 1 30); do
  SETTLED_ROWS=$(mysql_scalar "SELECT COUNT(*) FROM accounts WHERE \
    (user_id=$BUYER_ID AND asset='BTC' AND available >= 0.5 AND frozen = 0) OR \
    (user_id=$BUYER_ID AND asset='USDT' AND frozen = 0) OR \
    (user_id=$SELLER_ID AND asset='BTC' AND frozen = 0) OR \
    (user_id=$SELLER_ID AND asset='USDT' AND available >= 25000 AND frozen = 0)")
  if (( SETTLED_ROWS == 4 )); then
    ACCOUNTS_READY=1
    break
  fi
  sleep 1
done
(( ACCOUNTS_READY == 1 )) || die "MySQL accounts projection did not reach the settled state"

say "buyer post-trade balances"
curl -fsS http://localhost:8080/v1/account -H "X-User-Id: $BUYER_ID"
printf '\n'

say "seller post-trade balances"
curl -fsS http://localhost:8080/v1/account -H "X-User-Id: $SELLER_ID"
printf '\n'

say "latest MySQL trades for smoke users"
docker exec opentrade-mysql mysql -u opentrade -popentrade opentrade \
  -e "SELECT trade_id, symbol, price, qty, maker_user_id, taker_user_id FROM trades WHERE maker_user_id IN ($BUYER_ID,$SELLER_ID) OR taker_user_id IN ($BUYER_ID,$SELLER_ID) ORDER BY ts DESC LIMIT 5"

say "MySQL accounts projection for smoke users"
docker exec opentrade-mysql mysql -u opentrade -popentrade opentrade \
  -e "SELECT user_id, asset, available, frozen FROM accounts WHERE user_id IN ($BUYER_ID,$SELLER_ID) ORDER BY user_id, asset"

say "done (run_id=$RUN_ID, trades $TRADES_BEFORE -> $TRADES_AFTER)"
