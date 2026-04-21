// Command counter runs the OpenTrade Counter service.
//
// Responsibilities:
//   - Serve the CounterService gRPC: PlaceOrder / CancelOrder / QueryOrder /
//     Transfer / QueryBalance.
//   - Serialize per-user work through the UserSequencer (ADR-0018).
//   - Publish every state-mutating change to the counter-journal Kafka topic
//     (ADR-0004); PlaceOrder / CancelOrder use a Kafka transaction to
//     atomically publish counter-journal + order-event (ADR-0005).
//   - Consume trade-event from Match (at-least-once + idempotency) and apply
//     settlement to accounts + orders.
//   - Persist ShardState + orders + dedup + shard seq to local snapshot on
//     graceful shutdown; auto-restore on next start.
//
// HA (MVP-12, ADR-0031): with --ha-mode=auto Counter competes for the
// shard's leader key in etcd. Only the elected primary opens the Kafka
// transactional producer, runs the trade-event consumer, and accepts gRPC
// traffic. When the lease is lost the primary tears those down and
// re-campaigns. Backups sit idle until they win — this is a cold-standby
// design; ADR-0031 explains why we did not ship a live tailing backup in
// this MVP.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/go-sql-driver/mysql"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	assetholderrpc "github.com/xargin/opentrade/api/gen/rpc/assetholder"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/counter/internal/clustering"
	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/journal"
	"github.com/xargin/opentrade/counter/internal/reconcile"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/counter/internal/server"
	"github.com/xargin/opentrade/counter/internal/service"
	"github.com/xargin/opentrade/counter/internal/snapshot"
	"github.com/xargin/opentrade/counter/internal/symregistry"
	"github.com/xargin/opentrade/pkg/election"
	"github.com/xargin/opentrade/pkg/etcdcfg"
	"github.com/xargin/opentrade/pkg/idgen"
	"github.com/xargin/opentrade/pkg/logx"
)

type Config struct {
	ShardID          int
	TotalShards      int
	InstanceID       string
	GRPCAddr         string
	Brokers          []string
	JournalTopic     string
	OrderEventTopic       string // legacy single topic (ADR-0050 fallback)
	OrderEventTopicPrefix string // ADR-0050; per-symbol topics `<prefix>-<symbol>`
	TradeEventTopic  string
	ConsumerGroup    string
	SnapshotDir      string
	SnapshotInterval time.Duration
	SnapshotFormat   snapshot.Format // ADR-0049

	// Snapshot storage backend (ADR-0058 phase 1). fs = local directory
	// under SnapshotDir; s3 = S3-compatible object store addressed by
	// bucket + prefix.
	SnapshotBackend    string
	SnapshotS3Bucket   string
	SnapshotS3Prefix   string
	SnapshotS3Region   string
	SnapshotS3Endpoint string // override for MinIO/localstack; empty = AWS default

	DedupTTL time.Duration

	// HA (MVP-12).
	HAMode         string // "disabled" (default) | "auto"
	EtcdEndpoints  []string
	ElectionPath   string
	LeaseTTL       int    // seconds; default 10
	CampaignBackoff time.Duration // wait between failed Campaigns

	// Clustering (ADR-0058 phase 3a). When enabled, this counter process
	// registers itself under /cex/counter/nodes/{NodeID}, campaigns for
	// the /coordinator/leader role, and (if elected) writes initial
	// assignments into /cex/counter/assignment/vshard-NNN. The data
	// plane still runs on the legacy shard model — the cluster runs
	// as an observer while later phases migrate consumers / BFF routing
	// onto the vshard view.
	ClusteringMode string // "disabled" (default) | "enabled"
	NodeID         string // node id; default = InstanceID
	NodeEndpoint   string // advertised RPC address; default = GRPCAddr
	VShardCount    int    // total virtual shards (256 per ADR-0058)
	ClusterRoot    string // etcd prefix; default /cex/counter

	// Reconciliation (ADR-0008 §对账). When MySQLDSN is empty the audit loop
	// is disabled and Counter runs as before.
	MySQLDSN        string
	ReconInterval   time.Duration
	ReconBatchSize  int

	// DefaultMaxOpenLimitOrders is the ADR-0054 fallback when a symbol's
	// SymbolConfig.MaxOpenLimitOrders is zero. 0 disables the cap entirely
	// (compatibility mode). Operators should leave the default 100.
	DefaultMaxOpenLimitOrders uint32

	Env      string
	LogLevel string
}

func main() {
	cfg := parseFlags()

	logger, err := logx.New(logx.Config{Service: "counter", Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	logx.SetGlobal(logger)

	if err := cfg.validate(); err != nil {
		logger.Fatal("invalid config", zap.Error(err))
	}

	logger.Info("counter starting",
		zap.Int("shard_id", cfg.ShardID),
		zap.Int("total_shards", cfg.TotalShards),
		zap.String("instance", cfg.InstanceID),
		zap.String("grpc", cfg.GRPCAddr),
		zap.Strings("brokers", cfg.Brokers),
		zap.String("ha_mode", cfg.HAMode),
		zap.String("snapshot_dir", cfg.SnapshotDir))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := newSnapshotStore(rootCtx, cfg, logger)
	if err != nil {
		logger.Fatal("snapshot store init", zap.Error(err))
	}

	// ADR-0058 phase 3a: when clustering is enabled, register this node
	// + campaign for coordinator in parallel with the existing shard
	// data plane. The cluster is observer-only here — no data-plane
	// decisions are driven by the assignment table yet; later phases
	// switch consumers / BFF routing onto the vshard view.
	stopCluster, err := startCluster(rootCtx, cfg, logger)
	if err != nil {
		logger.Fatal("clustering init", zap.Error(err))
	}
	if stopCluster != nil {
		defer stopCluster()
	}

	if cfg.HAMode == "disabled" {
		runPrimary(rootCtx, cfg, store, logger)
		return
	}
	runElectionLoop(rootCtx, cfg, store, logger)
}

// startCluster brings up the clustering subsystem when enabled and
// returns a stop function that must be deferred to block until the
// cluster goroutine has drained (so the node lease is revoked cleanly
// instead of timing out on etcd). When clustering is disabled it
// returns (nil, nil) so the caller's deferred stop is a no-op.
func startCluster(ctx context.Context, cfg Config, logger *zap.Logger) (func(), error) {
	if cfg.ClusteringMode != "enabled" {
		return nil, nil
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("clustering etcd dial: %w", err)
	}
	cluster, err := clustering.New(clustering.Config{
		Client: client,
		Node: clustering.Node{
			ID:          cfg.NodeID,
			Endpoint:    cfg.NodeEndpoint,
			Capacity:    cfg.VShardCount,
			StartedAtMS: time.Now().UnixMilli(),
		},
		VShardCount: cfg.VShardCount,
		LeaseTTL:    cfg.LeaseTTL,
		RootPrefix:  cfg.ClusterRoot,
		Logger:      logger,
	})
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("clustering new: %w", err)
	}
	logger.Info("clustering enabled",
		zap.String("node_id", cfg.NodeID),
		zap.String("endpoint", cfg.NodeEndpoint),
		zap.Int("vshard_count", cfg.VShardCount),
		zap.String("root", cfg.ClusterRoot))

	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := cluster.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("cluster run exited", zap.Error(err))
		}
	}()
	return func() {
		<-done
		_ = client.Close()
	}, nil
}

// newSnapshotStore picks the BlobStore implementation based on
// --snapshot-backend. Credentials for the S3 backend come from the AWS SDK
// default config chain (env vars, shared profile, IAM role), so this
// function never sees them. The S3 endpoint override exists to point at
// MinIO / localstack during dev and integration tests.
func newSnapshotStore(ctx context.Context, cfg Config, logger *zap.Logger) (snapshot.BlobStore, error) {
	switch cfg.SnapshotBackend {
	case "fs":
		logger.Info("snapshot backend: fs", zap.String("dir", cfg.SnapshotDir))
		return snapshot.NewFSBlobStore(cfg.SnapshotDir), nil
	case "s3":
		var opts []func(*awsconfig.LoadOptions) error
		if cfg.SnapshotS3Region != "" {
			opts = append(opts, awsconfig.WithRegion(cfg.SnapshotS3Region))
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("aws config: %w", err)
		}
		client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			if cfg.SnapshotS3Endpoint != "" {
				o.BaseEndpoint = aws.String(cfg.SnapshotS3Endpoint)
				o.UsePathStyle = true // MinIO / path-style buckets
			}
		})
		logger.Info("snapshot backend: s3",
			zap.String("bucket", cfg.SnapshotS3Bucket),
			zap.String("prefix", cfg.SnapshotS3Prefix),
			zap.String("region", cfg.SnapshotS3Region),
			zap.String("endpoint", cfg.SnapshotS3Endpoint))
		return snapshot.NewS3BlobStore(client, cfg.SnapshotS3Bucket, cfg.SnapshotS3Prefix), nil
	default:
		return nil, fmt.Errorf("unknown snapshot backend %q", cfg.SnapshotBackend)
	}
}

// runElectionLoop campaigns for leadership, then runs the primary body for
// the duration of each leadership cycle. It exits when rootCtx is cancelled.
func runElectionLoop(rootCtx context.Context, cfg Config, store snapshot.BlobStore, logger *zap.Logger) {
	elec, err := election.New(election.Config{
		Endpoints: cfg.EtcdEndpoints,
		Path:      cfg.ElectionPath,
		Value:     cfg.InstanceID,
		LeaseTTL:  cfg.LeaseTTL,
	})
	if err != nil {
		logger.Fatal("election init", zap.Error(err))
	}
	defer func() { _ = elec.Close() }()

	for {
		if rootCtx.Err() != nil {
			return
		}
		logger.Info("counter campaigning for leadership",
			zap.String("path", cfg.ElectionPath))
		if err := elec.Campaign(rootCtx); err != nil {
			if rootCtx.Err() != nil {
				return
			}
			logger.Error("campaign failed", zap.Error(err))
			select {
			case <-rootCtx.Done():
				return
			case <-time.After(cfg.CampaignBackoff):
			}
			continue
		}
		logger.Info("counter became primary", zap.String("instance", cfg.InstanceID))

		primaryCtx, cancelPrimary := context.WithCancel(rootCtx)
		watchDone := make(chan struct{})
		go func() {
			defer close(watchDone)
			select {
			case <-elec.LostCh():
				logger.Warn("counter lost leadership — demoting")
				cancelPrimary()
			case <-primaryCtx.Done():
			}
		}()

		runPrimary(primaryCtx, cfg, store, logger)
		cancelPrimary()
		<-watchDone

		// Graceful resign so the backup picks up immediately on clean
		// shutdown. If rootCtx is dead we skip — the session will expire
		// on its own.
		if rootCtx.Err() == nil {
			logger.Info("counter demoted; re-campaigning")
			continue
		}
		resignCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := elec.Resign(resignCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Warn("resign failed", zap.Error(err))
		}
		cancel()
		return
	}
}

// runPrimary brings up the full Counter stack and blocks until ctx is done.
// Called directly in HA-disabled mode and once per leadership cycle in
// auto mode.
func runPrimary(ctx context.Context, cfg Config, store snapshot.BlobStore, logger *zap.Logger) {
	state := engine.NewShardState(cfg.ShardID)
	userSeq := sequencer.New()
	dt := dedup.New(cfg.DedupTTL)

	// Restore returns the per-partition consumer offsets persisted alongside
	// the shard state; nil on cold start or v1 snapshot (ADR-0048).
	restoredOffsets, err := tryRestoreSnapshot(ctx, cfg, store, state, userSeq, dt, logger)
	if err != nil {
		logger.Error("snapshot restore", zap.Error(err))
		return
	}

	// Single transactional producer feeds every counter-journal write so
	// that Transfer, Settlement, PlaceOrder and CancelOrder all benefit from
	// the shard's stable transactional.id fencing (ADR-0017, ADR-0031).
	// Opening the client also implicitly calls InitProducerID on the first
	// BeginTransaction, fencing any previous primary.
	txnProducer, err := journal.NewTxnProducer(ctx, journal.TxnProducerConfig{
		Brokers:         cfg.Brokers,
		ClientID:        cfg.InstanceID + "-txn",
		TransactionalID: cfg.InstanceID,
		JournalTopic:    cfg.JournalTopic,
		OrderEventTopic:       cfg.OrderEventTopic,
		OrderEventTopicPrefix: cfg.OrderEventTopicPrefix,
	}, logger)
	if err != nil {
		logger.Error("txn producer", zap.Error(err))
		return
	}
	defer txnProducer.Close()

	idg, err := idgen.NewGenerator(cfg.ShardID)
	if err != nil {
		logger.Error("idgen", zap.Error(err))
		return
	}

	svc := service.New(service.Config{
		ShardID:                   cfg.ShardID,
		TotalShards:               cfg.TotalShards,
		ProducerID:                cfg.InstanceID,
		DefaultMaxOpenLimitOrders: cfg.DefaultMaxOpenLimitOrders,
	}, state, userSeq, dt, txnProducer, logger)
	svc.SetOrderDeps(txnProducer, idg)

	// ADR-0053 M3: wire per-symbol precision lookup. When etcd is
	// configured (same endpoints as election), spawn a watcher goroutine
	// that keeps the registry in sync with /cex/match/symbols/*. Without
	// etcd endpoints the registry stays nil and PlaceOrder runs in
	// compatibility mode.
	if len(cfg.EtcdEndpoints) > 0 {
		symSrc, err := etcdcfg.NewEtcdSource(etcdcfg.EtcdConfig{
			Endpoints: cfg.EtcdEndpoints,
		})
		if err != nil {
			logger.Warn("symbol registry: etcd dial failed, precision stays in compat mode",
				zap.Error(err))
		} else {
			defer func() { _ = symSrc.Close() }()
			registry := symregistry.New()
			go func() {
				if err := registry.Run(ctx, symSrc); err != nil &&
					!errors.Is(err, context.Canceled) {
					logger.Error("symbol registry watch exited", zap.Error(err))
				}
			}()
			svc.SetSymbolLookup(registry.Get)
			logger.Info("symbol registry wired for precision enforcement",
				zap.Strings("etcd", cfg.EtcdEndpoints))
		}
	} else {
		logger.Info("no etcd endpoints; precision enforcement disabled (compat mode)")
	}

	// Inject the snapshot's trade-event offsets so advanceOffset has a
	// correct baseline. Cold start / v1 snapshot → nil → consumer will
	// fall back to AtStart below.
	svc.SetOffsets(restoredOffsets)
	if len(restoredOffsets) > 0 {
		logger.Info("restoring trade-event offsets from snapshot",
			zap.Int("partitions", len(restoredOffsets)))
	}

	tradeConsumer, err := journal.NewTradeConsumer(journal.TradeConsumerConfig{
		Brokers:        cfg.Brokers,
		ClientID:       cfg.InstanceID + "-trade",
		GroupID:        cfg.ConsumerGroup,
		Topic:          cfg.TradeEventTopic,
		InitialOffsets: restoredOffsets,
	}, svc, logger)
	if err != nil {
		logger.Error("trade consumer", zap.Error(err))
		return
	}
	defer tradeConsumer.Close()

	var tradeWG sync.WaitGroup
	tradeWG.Add(1)
	go func() {
		defer tradeWG.Done()
		if err := tradeConsumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("trade consumer exited", zap.Error(err))
		}
	}()

	// Periodic snapshot. Capture is concurrency-safe against live mutations
	// (each underlying store takes its own RWMutex + returns deep copies).
	// Before reading state we call txnProducer.Flush → Kafka ack barrier so
	// the offsets recorded in the snapshot reflect only fully-committed
	// transactions (ADR-0048 output flush barrier; TxnProducer is sync so
	// this is typically a no-op but keeps the contract explicit).
	snapCtx, cancelSnap := context.WithCancel(context.Background())
	defer cancelSnap()
	var snapWG sync.WaitGroup
	snapWG.Add(1)
	go func() {
		defer snapWG.Done()
		periodicSnapshot(snapCtx, cfg, store, state, userSeq, dt, svc, txnProducer, logger)
	}()

	// Hourly balance audit against MySQL projection (ADR-0008 §对账). Only
	// the primary runs this — backups have stale state by design. Opens its
	// own DB pool so a MySQL outage can't stall the gRPC path.
	reconCancel, reconDone := startReconcile(ctx, cfg, state, logger)
	defer func() {
		if reconCancel != nil {
			reconCancel()
			<-reconDone
		}
	}()

	grpcServer := grpc.NewServer()
	counterrpc.RegisterCounterServiceServer(grpcServer, server.New(svc, logger))
	// Counter also plays the biz_line=spot slot of the asset-service saga
	// protocol (ADR-0057). The AssetHolder server shares the same Service
	// so sequencer / state / dedup are consistent with the CounterService
	// endpoint.
	assetholderrpc.RegisterAssetHolderServer(grpcServer, server.NewAssetHolderServer(svc))
	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Error("listen", zap.Error(err))
		return
	}
	var grpcWG sync.WaitGroup
	grpcWG.Add(1)
	go func() {
		defer grpcWG.Done()
		logger.Info("gRPC listening", zap.String("addr", cfg.GRPCAddr))
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error("grpc Serve", zap.Error(err))
		}
	}()

	<-ctx.Done()
	logger.Info("primary shutting down")

	grpcServer.GracefulStop()
	grpcWG.Wait()

	tradeConsumer.Close()
	tradeWG.Wait()

	cancelSnap()
	snapWG.Wait()

	// Drain in-flight sequencer work before the final snapshot.
	time.Sleep(100 * time.Millisecond)

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownFlushTimeout)
	if err := writeSnapshot(shutdownCtx, cfg, store, state, userSeq, dt, svc, txnProducer); err != nil {
		logger.Error("final snapshot", zap.Error(err))
	} else {
		logger.Info("final snapshot written")
	}
	cancelShutdown()
	logger.Info("primary stopped")
}

// startReconcile opens the MySQL pool + spawns the audit loop on a derived
// context. Returns (cancel, done) so the caller can tear it down. When the
// DSN is empty or the pool fails to open we log and return (nil, nil) so
// the primary continues without auditing.
func startReconcile(parent context.Context, cfg Config, state *engine.ShardState, logger *zap.Logger) (context.CancelFunc, <-chan struct{}) {
	if cfg.MySQLDSN == "" {
		logger.Info("reconcile disabled (no --mysql-dsn)")
		return nil, nil
	}
	db, err := sql.Open("mysql", cfg.MySQLDSN)
	if err != nil {
		logger.Error("reconcile: open mysql", zap.Error(err))
		return nil, nil
	}
	pingCtx, cancelPing := context.WithTimeout(parent, 5*time.Second)
	defer cancelPing()
	if err := db.PingContext(pingCtx); err != nil {
		logger.Error("reconcile: ping mysql", zap.Error(err))
		_ = db.Close()
		return nil, nil
	}
	rec := reconcile.New(reconcile.Config{
		ShardID:   cfg.ShardID,
		Interval:  cfg.ReconInterval,
		BatchSize: cfg.ReconBatchSize,
	}, state, db, logger)
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = db.Close() }()
		_ = rec.Run(ctx)
	}()
	return cancel, done
}

// snapshotFlushTimeout caps how long one snapshot tick may block waiting for
// the Kafka producer to drain (ADR-0048). shutdownFlushTimeout gives the
// final snapshot more headroom since graceful shutdown can coincide with
// broker slowdowns.
const (
	snapshotFlushTimeout = 3 * time.Second
	shutdownFlushTimeout = 10 * time.Second
)

// periodicSnapshot writes a full shard snapshot through the BlobStore on
// each tick. SnapshotInterval <= 0 disables the loop (only the
// shutdown-time snapshot runs, matching pre-MVP-12b behaviour).
func periodicSnapshot(ctx context.Context, cfg Config, store snapshot.BlobStore, state *engine.ShardState, seq *sequencer.UserSequencer, dt *dedup.Table, svc *service.Service, txnProducer *journal.TxnProducer, logger *zap.Logger) {
	if cfg.SnapshotInterval <= 0 {
		return
	}
	ticker := time.NewTicker(cfg.SnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			flushCtx, cancel := context.WithTimeout(ctx, snapshotFlushTimeout)
			err := writeSnapshot(flushCtx, cfg, store, state, seq, dt, svc, txnProducer)
			cancel()
			if err != nil {
				logger.Error("periodic snapshot", zap.Error(err))
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

func parseFlags() Config {
	cfg := Config{
		GRPCAddr:         ":8081",
		JournalTopic:     "counter-journal",
		OrderEventTopic:       "order-event",
		OrderEventTopicPrefix: "order-event",
		TradeEventTopic:  "trade-event",
		SnapshotDir:      "./data/counter",
		SnapshotInterval: 60 * time.Second,
		SnapshotFormat:   snapshot.FormatProto, // ADR-0049
		SnapshotBackend:  "fs",                 // ADR-0058 phase 1
		DedupTTL:         24 * time.Hour,
		HAMode:           "disabled",
		LeaseTTL:         10,
		CampaignBackoff:  2 * time.Second,
		ClusteringMode:   "disabled", // ADR-0058 phase 3a; opt-in per deployment
		VShardCount:      256,        // ADR-0058 §2
		ClusterRoot:      clustering.DefaultRoot,
		ReconInterval:             time.Hour,
		ReconBatchSize:            200,
		DefaultMaxOpenLimitOrders: 100, // ADR-0054
		Env:                       "dev",
		LogLevel:                  "info",
	}
	var brokersStr, etcdStr, snapshotFormatStr string
	flag.IntVar(&cfg.ShardID, "shard-id", 0, "shard id (0..total-shards-1)")
	flag.IntVar(&cfg.TotalShards, "total-shards", 10, "total shard count for user→shard routing (0 disables guard)")
	flag.StringVar(&cfg.InstanceID, "instance-id", "", "instance id (default counter-shard-<N>-main)")
	flag.StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "gRPC listen address")
	flag.StringVar(&brokersStr, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flag.StringVar(&cfg.JournalTopic, "journal-topic", cfg.JournalTopic, "counter-journal topic name")
	flag.StringVar(&cfg.OrderEventTopic, "order-topic", cfg.OrderEventTopic, "legacy single order-event topic (used when --order-topic-prefix is empty; ADR-0050)")
	flag.StringVar(&cfg.OrderEventTopicPrefix, "order-topic-prefix", cfg.OrderEventTopicPrefix, "per-symbol order-event topic prefix — emits to `<prefix>-<symbol>`. Empty falls back to --order-topic (ADR-0050)")
	flag.StringVar(&cfg.TradeEventTopic, "trade-topic", cfg.TradeEventTopic, "trade-event topic name")
	flag.StringVar(&cfg.ConsumerGroup, "group", "", "Kafka consumer group (default counter-shard-<N>)")
	flag.StringVar(&cfg.SnapshotDir, "snapshot-dir", cfg.SnapshotDir, "local directory for snapshots (used when --snapshot-backend=fs)")
	flag.DurationVar(&cfg.SnapshotInterval, "snapshot-interval", cfg.SnapshotInterval, "periodic snapshot cadence while primary (0 disables; only final shutdown snapshot runs)")
	flag.StringVar(&snapshotFormatStr, "snapshot-format", cfg.SnapshotFormat.String(), "snapshot on-disk encoding: proto (default) | json (debug). ADR-0049. Env OPENTRADE_SNAPSHOT_FORMAT overrides.")
	flag.StringVar(&cfg.SnapshotBackend, "snapshot-backend", cfg.SnapshotBackend, "snapshot backend: fs (local dir) | s3 (S3-compatible object store). ADR-0058 phase 1.")
	flag.StringVar(&cfg.SnapshotS3Bucket, "snapshot-s3-bucket", cfg.SnapshotS3Bucket, "S3 bucket (required when --snapshot-backend=s3)")
	flag.StringVar(&cfg.SnapshotS3Prefix, "snapshot-s3-prefix", cfg.SnapshotS3Prefix, "S3 key prefix; trailing slash optional (empty = bucket root)")
	flag.StringVar(&cfg.SnapshotS3Region, "snapshot-s3-region", cfg.SnapshotS3Region, "S3 region (empty = AWS SDK default config chain)")
	flag.StringVar(&cfg.SnapshotS3Endpoint, "snapshot-s3-endpoint", cfg.SnapshotS3Endpoint, "S3 endpoint override for MinIO/localstack (empty = AWS default)")
	flag.DurationVar(&cfg.DedupTTL, "dedup-ttl", cfg.DedupTTL, "transfer_id dedup TTL")
	flag.StringVar(&cfg.HAMode, "ha-mode", cfg.HAMode, "ha mode: disabled | auto (etcd leader election, ADR-0031)")
	flag.StringVar(&etcdStr, "etcd", "", "comma-separated etcd endpoints (required when --ha-mode=auto or --clustering-mode=enabled)")
	flag.StringVar(&cfg.ElectionPath, "election-path", "", "etcd election key (default /cex/counter/shard-<N>/leader)")
	flag.IntVar(&cfg.LeaseTTL, "lease-ttl", cfg.LeaseTTL, "etcd session TTL seconds")
	flag.DurationVar(&cfg.CampaignBackoff, "campaign-backoff", cfg.CampaignBackoff, "wait between failed Campaigns")
	flag.StringVar(&cfg.ClusteringMode, "clustering-mode", cfg.ClusteringMode, "clustering mode: disabled | enabled (ADR-0058 phase 3a; observer-only for now)")
	flag.StringVar(&cfg.NodeID, "node-id", "", "cluster node id; required when --clustering-mode=enabled (default = --instance-id)")
	flag.StringVar(&cfg.NodeEndpoint, "node-endpoint", "", "advertised RPC endpoint for this node in /cex/counter/nodes (default = --grpc-addr)")
	flag.IntVar(&cfg.VShardCount, "vshard-count", cfg.VShardCount, "total virtual shards (ADR-0058 §2; 256 in production)")
	flag.StringVar(&cfg.ClusterRoot, "cluster-root", cfg.ClusterRoot, "etcd root prefix for clustering data (default /cex/counter)")
	flag.StringVar(&cfg.MySQLDSN, "mysql-dsn", "", "MySQL DSN for hourly balance reconcile (empty disables; ADR-0008)")
	flag.DurationVar(&cfg.ReconInterval, "recon-interval", cfg.ReconInterval, "balance audit cadence (primary only; 0 disables even when DSN set)")
	flag.IntVar(&cfg.ReconBatchSize, "recon-batch", cfg.ReconBatchSize, "user ids per reconcile SELECT batch")
	var defaultMaxOpenLimitOrders uint
	flag.UintVar(&defaultMaxOpenLimitOrders, "default-max-open-limit-orders", uint(cfg.DefaultMaxOpenLimitOrders), "ADR-0054 fallback per-(user, symbol) LIMIT cap when SymbolConfig.MaxOpenLimitOrders is zero (0 disables cap; default 100)")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	cfg.Brokers = splitCSV(brokersStr)
	cfg.EtcdEndpoints = splitCSV(etcdStr)
	cfg.DefaultMaxOpenLimitOrders = uint32(defaultMaxOpenLimitOrders)
	// --snapshot-format + OPENTRADE_SNAPSHOT_FORMAT env override (ADR-0049).
	if envFmt := os.Getenv("OPENTRADE_SNAPSHOT_FORMAT"); envFmt != "" {
		snapshotFormatStr = envFmt
	}
	if snapshotFormatStr != "" {
		f, err := snapshot.ParseFormat(snapshotFormatStr)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		cfg.SnapshotFormat = f
	}
	if cfg.InstanceID == "" {
		cfg.InstanceID = fmt.Sprintf("counter-shard-%d-main", cfg.ShardID)
	}
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = fmt.Sprintf("counter-shard-%d", cfg.ShardID)
	}
	if cfg.ElectionPath == "" {
		cfg.ElectionPath = fmt.Sprintf("/cex/counter/shard-%d/leader", cfg.ShardID)
	}
	// Clustering defaults: if the operator enables clustering but
	// doesn't set node-id / node-endpoint explicitly, fall back to the
	// instance id / gRPC address. These are the right identifiers for
	// the typical single-binary deployment.
	if cfg.NodeID == "" {
		cfg.NodeID = cfg.InstanceID
	}
	if cfg.NodeEndpoint == "" {
		cfg.NodeEndpoint = cfg.GRPCAddr
	}
	return cfg
}

func (c *Config) validate() error {
	if c.ShardID < 0 {
		return fmt.Errorf("shard-id must be >= 0")
	}
	if c.TotalShards < 0 {
		return fmt.Errorf("total-shards must be >= 0")
	}
	if c.TotalShards > 0 && c.ShardID >= c.TotalShards {
		return fmt.Errorf("shard-id %d must be < total-shards %d", c.ShardID, c.TotalShards)
	}
	if c.GRPCAddr == "" {
		return fmt.Errorf("grpc-addr required")
	}
	if len(c.Brokers) == 0 {
		return fmt.Errorf("at least one broker required")
	}
	switch c.HAMode {
	case "disabled", "auto":
	default:
		return fmt.Errorf("ha-mode must be disabled or auto, got %q", c.HAMode)
	}
	if c.HAMode == "auto" && len(c.EtcdEndpoints) == 0 {
		return fmt.Errorf("--etcd required when --ha-mode=auto")
	}
	switch c.SnapshotBackend {
	case "fs":
		if c.SnapshotDir == "" {
			return fmt.Errorf("--snapshot-dir required when --snapshot-backend=fs")
		}
	case "s3":
		if c.SnapshotS3Bucket == "" {
			return fmt.Errorf("--snapshot-s3-bucket required when --snapshot-backend=s3")
		}
	default:
		return fmt.Errorf("--snapshot-backend must be fs or s3, got %q", c.SnapshotBackend)
	}
	switch c.ClusteringMode {
	case "disabled":
	case "enabled":
		if len(c.EtcdEndpoints) == 0 {
			return fmt.Errorf("--etcd required when --clustering-mode=enabled")
		}
		if c.VShardCount <= 0 {
			return fmt.Errorf("--vshard-count must be > 0 when --clustering-mode=enabled")
		}
	default:
		return fmt.Errorf("--clustering-mode must be disabled or enabled, got %q", c.ClusteringMode)
	}
	return nil
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Snapshot helpers
// ---------------------------------------------------------------------------

// snapshotKey returns the BlobStore key (stem, no extension) for this
// shard's snapshot. The store adds its own directory / prefix; the format
// extension is appended by snapshot.Save based on cfg.SnapshotFormat.
func snapshotKey(cfg Config) string {
	return fmt.Sprintf("shard-%d", cfg.ShardID)
}

// tryRestoreSnapshot loads state + per-partition offsets from the store.
// Returns the offsets map so main can seed both the Service and the Kafka
// consumer. A v1 snapshot restores state but returns nil offsets — the
// consumer falls back to AtStart for one rescan, covered by idempotency
// guards.
func tryRestoreSnapshot(ctx context.Context, cfg Config, store snapshot.BlobStore, state *engine.ShardState, seq *sequencer.UserSequencer, dt *dedup.Table, logger *zap.Logger) (map[int32]int64, error) {
	key := snapshotKey(cfg)
	snap, err := snapshot.Load(ctx, store, key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			logger.Info("no snapshot found, starting fresh", zap.Int("shard_id", cfg.ShardID))
			return nil, nil
		}
		return nil, fmt.Errorf("load %s: %w", key, err)
	}
	if err := snapshot.Restore(cfg.ShardID, state, seq, dt, snap); err != nil {
		return nil, fmt.Errorf("restore: %w", err)
	}
	offsets := snapshot.OffsetsSliceToMap(snap.Offsets)
	logger.Info("restored from snapshot",
		zap.Int("shard_id", cfg.ShardID),
		zap.Int("version", snap.Version),
		zap.Uint64("counter_seq", snap.CounterSeq),
		zap.Int("accounts", len(snap.Accounts)),
		zap.Int("orders", len(snap.Orders)),
		zap.Int("partitions", len(offsets)))
	return offsets, nil
}

// writeSnapshot captures state + offsets after flushing the Kafka producer
// so persisted offsets are guaranteed ≤ committed output position
// (ADR-0048 output flush barrier).
func writeSnapshot(ctx context.Context, cfg Config, store snapshot.BlobStore, state *engine.ShardState, seq *sequencer.UserSequencer, dt *dedup.Table, svc *service.Service, txnProducer *journal.TxnProducer) error {
	if err := txnProducer.Flush(ctx); err != nil {
		return fmt.Errorf("flush before snapshot: %w", err)
	}
	snap := snapshot.Capture(cfg.ShardID, state, seq, dt, svc.Offsets(), time.Now().UnixMilli())
	return snapshot.Save(ctx, store, snapshotKey(cfg), snap, cfg.SnapshotFormat)
}
