package main

// services.go declares the OpenTrade dev stack: the docker-compose deps and
// every Go service, with the *current* (ADR-0058 / ADR-0057 / ADR-0040) flags
// and ports. This is the single source of truth the orchestrator and the UI
// read from.
//
// Why this file exists at all: deploy/scripts/smoke.sh and docs/dev-setup.md
// drifted out of date — counter dropped --shard-id/--total-shards and now
// requires --node-id + --etcd + --vshard-count, and /v1/transfer became an
// asset-service saga. The specs below are the corrected startup the launcher
// (and the refreshed smoke.sh) drive.

// Ports the launcher binds services to on localhost. Kept here so the UI badge
// list, health probes and inter-service wiring all agree.
const (
	portCounterGRPC = 8081  // counter --grpc-addr (PlaceOrder + AssetHolder)
	portBFFHTTP     = 8080  // bff --http-addr (REST + /ws)
	portPushHTTP    = 8090  // push --http (/ws + /healthz)
	portHistoryGRPC = 8085  // history --grpc
	portTriggerGRPC = 8082  // trigger --grpc-addr (its own default)
	portAssetGRPC   = 19000 // asset --grpc (AssetHolder for funding + saga)
)

// vshardCount is applied identically to counter, match and trade-dump
// (ADR-0058 §2: match's trade-event routing and trade-dump's snapshot
// ownership must match counter). A single counter node wins the coordinator
// election and self-assigns all vshards, so any value works for routing; we
// keep it small for fast single-node startup (each owned vshard runs a
// worker with its own transactional producer).
const vshardCount = 16

// Shared dev infra endpoints (match deploy/docker/docker-compose.yml and
// deploy/scripts/dev-env.sh).
const (
	kafkaBrokers   = "localhost:9092"
	etcdEndpoints  = "localhost:2379"
	mysqlDSN       = "opentrade:opentrade@tcp(localhost:3306)/opentrade?parseTime=true&multiStatements=true"
	mysqlAssetDSN  = "opentrade:opentrade@tcp(localhost:3306)/opentrade_asset?parseTime=true&multiStatements=true"
	composeFile    = "deploy/docker/docker-compose.yml"
	defaultSymbol  = "BTC-USDT"
)

// healthKind selects how the supervisor decides a process is ready to serve.
type healthKind int

const (
	// healthAlive: no listening port (pure Kafka consumer/producer). Ready
	// once the process has stayed up briefly without exiting.
	healthAlive healthKind = iota
	// healthTCP: ready when the TCP port accepts a connection (h2c gRPC
	// services that have no cheap HTTP healthz).
	healthTCP
	// healthHTTP: ready when GET <path> returns 2xx.
	healthHTTP
)

// serviceSpec is one managed Go process. Args are the corrected current flags.
type serviceSpec struct {
	Name string // stable id used in the API, UI and logs
	Desc string // one-line role, shown in the UI

	// Module + main package, relative to repo root, used both to `go build`
	// into ./bin/<Name> and to locate the binary.
	BuildPath string // e.g. "./counter/cmd/counter"

	Args []string // CLI flags passed to the built binary

	// Tier orders startup. Lower tiers come up (and pass their health gate)
	// before higher tiers start, because higher tiers dial lower ones at
	// boot (e.g. bff Fatals if it can't dial counter/history/asset/trigger).
	Tier int

	Health     healthKind
	HealthPort int    // for healthTCP / healthHTTP
	HealthPath string // for healthHTTP

	// Optional marks services the user can toggle off. Spot trading works
	// with the always-on set; asset (funding wallet + transfer saga) and
	// trigger (conditional orders) are opt-in extras.
	Optional bool
}

// stackServices returns the full ordered service set. The slice order is also
// the UI display order.
func stackServices() []serviceSpec {
	return []serviceSpec{
		{
			Name:       "counter",
			Desc:       "accounts · order state machine · spot AssetHolder",
			BuildPath:  "./counter/cmd/counter",
			Tier:       1,
			Health:     healthTCP,
			HealthPort: portCounterGRPC,
			Args: []string{
				"--node-id=counter-0",
				"--node-endpoint=localhost:" + itoa(portCounterGRPC),
				"--grpc-addr=:" + itoa(portCounterGRPC),
				"--vshard-count=" + itoa(vshardCount),
				"--cluster-root=/cex/counter",
				"--brokers=" + kafkaBrokers,
				"--etcd=" + etcdEndpoints,
				"--snapshot-backend=fs",
				"--snapshot-dir=./data/counter",
				"--env=dev",
			},
		},
		{
			Name:      "match",
			Desc:      "per-symbol matching engine (" + defaultSymbol + ")",
			BuildPath: "./match/cmd/match",
			Tier:      2,
			Health:    healthAlive,
			Args: []string{
				"--instance-id=match-0",
				"--shard-id=match-0",
				"--symbols=" + defaultSymbol,
				"--brokers=" + kafkaBrokers,
				"--vshard-count=" + itoa(vshardCount),
				"--snapshot-dir=./data/match",
				"--env=dev",
			},
		},
		{
			Name:      "trade-dump",
			Desc:      "Kafka → MySQL projection + counter snapshots",
			BuildPath: "./trade-dump/cmd/trade-dump",
			Tier:      2,
			Health:    healthAlive,
			Args: []string{
				"--instance-id=trade-dump-0",
				"--brokers=" + kafkaBrokers,
				"--pipelines=sql,snap",
				"--mysql-dsn=" + mysqlDSN,
				"--vshard-count=" + itoa(vshardCount),
				"--snapshot-backend=fs",
				"--snapshot-dir=./data/trade-dump",
				"--env=dev",
			},
		},
		{
			Name:      "quote",
			Desc:      "depth / trades / klines market-data projection",
			BuildPath: "./quote/cmd/quote",
			Tier:      2,
			Health:    healthAlive,
			Args: []string{
				"--instance-id=quote-0",
				"--brokers=" + kafkaBrokers,
				"--env=dev",
			},
		},
		{
			Name:       "push",
			Desc:       "WebSocket fan-out (market + private)",
			BuildPath:  "./push/cmd/push",
			Tier:       2,
			Health:     healthHTTP,
			HealthPort: portPushHTTP,
			HealthPath: "/healthz",
			Args: []string{
				"--instance-id=push-0",
				"--http=:" + itoa(portPushHTTP),
				"--brokers=" + kafkaBrokers,
				"--env=dev",
			},
		},
		{
			Name:       "history",
			Desc:       "read-only order/trade/log history (MySQL)",
			BuildPath:  "./history/cmd/history",
			Tier:       2,
			Health:     healthTCP,
			HealthPort: portHistoryGRPC,
			Args: []string{
				"--instance-id=history-0",
				"--grpc=:" + itoa(portHistoryGRPC),
				"--mysql-dsn=" + mysqlDSN,
				"--env=dev",
			},
		},
		{
			Name:       "asset",
			Desc:       "funding wallet + cross-biz transfer saga",
			BuildPath:  "./asset/cmd/asset",
			Tier:       2,
			Optional:   true,
			Health:     healthTCP,
			HealthPort: portAssetGRPC,
			Args: []string{
				"--instance=asset-0",
				"--grpc=:" + itoa(portAssetGRPC),
				"--metrics-addr=:19090",
				"--mysql-dsn=" + mysqlAssetDSN,
				"--peer-holders=spot=localhost:" + itoa(portCounterGRPC),
				"--env=dev",
			},
		},
		{
			Name:       "trigger",
			Desc:       "stop-loss / take-profit / OCO conditional orders",
			BuildPath:  "./trigger/cmd/trigger",
			Tier:       2,
			Optional:   true,
			Health:     healthTCP,
			HealthPort: portTriggerGRPC,
			Args: []string{
				"--instance-id=trigger-0",
				"--grpc-addr=:" + itoa(portTriggerGRPC),
				"--brokers=" + kafkaBrokers,
				"--market-topic=market-data",
				"--counter-shards=localhost:" + itoa(portCounterGRPC),
				// Empty snapshot-dir → cold-start from the trigger-event head
				// instead of restoring trade-dump's shadow snapshot. Keeps the
				// dev trigger self-contained (no trade-dump coupling).
				"--snapshot-dir=",
				"--env=dev",
			},
		},
		{
			Name:       "bff",
			Desc:       "REST + WebSocket gateway (the API the UI calls)",
			BuildPath:  "./bff/cmd/bff",
			Tier:       3,
			Health:     healthHTTP,
			HealthPort: portBFFHTTP,
			HealthPath: "/healthz",
			Args: []string{
				"--http-addr=:" + itoa(portBFFHTTP),
				"--counter-shards=localhost:" + itoa(portCounterGRPC),
				"--push-ws=ws://localhost:" + itoa(portPushHTTP) + "/ws",
				"--history=localhost:" + itoa(portHistoryGRPC),
				"--asset=localhost:" + itoa(portAssetGRPC),
				"--trigger=localhost:" + itoa(portTriggerGRPC),
				"--market-brokers=" + kafkaBrokers,
				"--env=dev",
			},
		},
	}
}

// dockerDeps are the compose containers the launcher health-gates before
// starting Go services. kafka-init is intentionally excluded: it runs once to
// provision topic partitions and then exits 0, which is success, not failure.
var dockerDeps = []string{"kafka", "etcd", "mysql", "minio"}
