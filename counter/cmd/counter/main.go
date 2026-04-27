// Command counter runs the OpenTrade Counter service under the
// ADR-0058 vshard model.
//
// The process registers itself as a node in etcd, participates in
// coordinator election, and receives vshard assignments. For every
// vshard it owns it runs a VShardWorker — state + sequencer + dedup +
// transactional producer (epoch-fenced) + per-partition trade-event
// consumer + snapshot loop. The gRPC dispatcher routes every request
// to the vshard that owns the user (stable hash) or replies
// FailedPrecondition for foreign users so the BFF refreshes routing
// and retries.
//
// The legacy shard mode (--shard-id / --total-shards / --ha-mode) and
// the per-shard MySQL reconcile loop are removed. Reconcile will
// return as a node-level operator task in a follow-up ADR.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/xargin/opentrade/api/gen/rpc/assetholder/assetholderrpcconnect"
	"github.com/xargin/opentrade/api/gen/rpc/counter/counterrpcconnect"
	"github.com/xargin/opentrade/counter/internal/clustering"
	"github.com/xargin/opentrade/counter/internal/metrics"
	"github.com/xargin/opentrade/counter/internal/server"
	"github.com/xargin/opentrade/counter/internal/symregistry"
	"github.com/xargin/opentrade/counter/internal/tradedumpclient"
	"github.com/xargin/opentrade/counter/internal/worker"
	"github.com/xargin/opentrade/pkg/connectx"
	"github.com/xargin/opentrade/pkg/etcdcfg"
	"github.com/xargin/opentrade/pkg/logx"
	sharedmetrics "github.com/xargin/opentrade/pkg/metrics"
	"github.com/xargin/opentrade/pkg/snapshot"
)

// Config captures every runtime knob. ADR-0058 removed shard-id /
// total-shards / ha-mode / instance-id / consumer-group / election-*
// and the MySQL reconcile triple.
type Config struct {
	NodeID       string
	NodeEndpoint string
	GRPCAddr     string
	VShardCount  int
	ClusterRoot  string

	// Kafka
	Brokers               []string
	JournalTopic          string
	TradeEventTopic       string
	OrderEventTopic       string
	OrderEventTopicPrefix string

	// etcd (cluster control plane + symbol registry)
	EtcdEndpoints []string
	LeaseTTL      int

	// Snapshot store Counter reads from on startup (ADR-0061 / ADR-
	// 0064). Trade-dump's shadow pipeline is the sole producer; Counter
	// only loads from this store and never writes to it. Backend choice
	// here must point at the same fs dir / S3 bucket+prefix that
	// trade-dump writes.
	SnapshotBackend    string
	SnapshotDir        string
	SnapshotS3Bucket   string
	SnapshotS3Prefix   string
	SnapshotS3Region   string
	SnapshotS3Endpoint string

	// Business
	DedupTTL                  time.Duration
	DefaultMaxOpenLimitOrders uint32

	// Observability (ADR-0060 M8 + ADR-0062 M8)
	MetricsAddr string

	// ADR-0064 on-demand startup (hot path for faster Counter
	// recovery). Empty endpoint / legacy StartupMode disables the
	// path entirely — Counter falls back to ADR-0060 §4.2 load +
	// catchUpJournal transparently, which is the only safe mode
	// before trade-dump's TakeSnapshot surface is deployed.
	StartupMode       string        // "auto" (default) | "on-demand" | "legacy"
	TradeDumpEndpoint string        // host:port; empty disables on-demand regardless of mode
	OnDemandTimeout   time.Duration // per-attempt budget (sentinel + RPC + download); 0 → worker default (3s)

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
		zap.String("node_id", cfg.NodeID),
		zap.String("endpoint", cfg.NodeEndpoint),
		zap.String("grpc", cfg.GRPCAddr),
		zap.Int("vshard_count", cfg.VShardCount),
		zap.Strings("brokers", cfg.Brokers),
		zap.Strings("etcd", cfg.EtcdEndpoints),
		zap.String("cluster_root", cfg.ClusterRoot),
		zap.String("snapshot_backend", cfg.SnapshotBackend))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := newSnapshotStore(rootCtx, cfg, logger)
	if err != nil {
		logger.Fatal("snapshot store init", zap.Error(err))
	}

	// ADR-0060 M8 / ADR-0062 M8: one Prometheus registry per process,
	// shared by the framework metrics and the Counter-specific series.
	// Served on --metrics-addr. Empty address disables the endpoint
	// but still constructs the counter struct so emission is a no-op
	// without extra nil guards downstream.
	promReg := sharedmetrics.NewRegistry()
	_ = sharedmetrics.NewFramework("counter", promReg)
	counterMetrics := metrics.NewCounter(promReg)

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logger.Fatal("etcd dial", zap.Error(err))
	}
	defer func() { _ = etcdClient.Close() }()

	symbolLookup := startSymbolRegistry(rootCtx, cfg, logger)

	cluster, err := clustering.New(clustering.Config{
		Client: etcdClient,
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
		logger.Fatal("clustering init", zap.Error(err))
	}

	var wg sync.WaitGroup

	wg.Add(1)
	clusterDone := make(chan struct{})
	go func() {
		defer wg.Done()
		defer close(clusterDone)
		if err := cluster.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("cluster run exited", zap.Error(err))
		}
	}()

	// ADR-0064 on-demand startup wiring. Dial the trade-dump
	// endpoint once per process so every VShardWorker shares a
	// pooled connection. An empty endpoint (or an unparsable
	// startup mode) disables on-demand — the template below still
	// gets a nil OnDemandClient and legacy startup takes over.
	startupMode, err := worker.ParseStartupMode(cfg.StartupMode)
	if err != nil {
		logger.Fatal("invalid --startup-mode", zap.Error(err))
	}
	var tdClient *tradedumpclient.Client
	if cfg.TradeDumpEndpoint != "" {
		tdClient, err = tradedumpclient.Dial(rootCtx, cfg.TradeDumpEndpoint, logger)
		if err != nil {
			// Dial failures should NOT keep Counter from booting —
			// ADR-0064 §4 contract is that the legacy path always
			// works without on-demand. Log prominently and degrade.
			logger.Warn("trade-dump dial failed; on-demand disabled",
				zap.String("endpoint", cfg.TradeDumpEndpoint),
				zap.Error(err))
			tdClient = nil
		} else {
			defer func() { _ = tdClient.Close() }()
			logger.Info("on-demand startup enabled",
				zap.String("endpoint", cfg.TradeDumpEndpoint),
				zap.String("mode", startupMode.String()),
				zap.Duration("timeout", cfg.OnDemandTimeout))
		}
	} else if startupMode == worker.StartupModeOnDemand {
		// Explicit on-demand requirement with no endpoint is a
		// deployment error — fail fast rather than silently falling
		// back to legacy, which would defeat the operator's intent.
		logger.Fatal("--startup-mode=on-demand requires --trade-dump-endpoint")
	} else {
		logger.Info("on-demand startup disabled",
			zap.String("mode", startupMode.String()),
			zap.String("endpoint", cfg.TradeDumpEndpoint))
	}

	mgr, err := worker.NewManager(cluster, worker.WorkerTemplate{
		NodeID:                    cfg.NodeID,
		VShardCount:               cfg.VShardCount,
		Brokers:                   cfg.Brokers,
		JournalTopic:              cfg.JournalTopic,
		TradeEventTopic:           cfg.TradeEventTopic,
		OrderEventTopic:           cfg.OrderEventTopic,
		OrderEventTopicPrefix:     cfg.OrderEventTopicPrefix,
		Store:                     store,
		DedupTTL:                  cfg.DedupTTL,
		DefaultMaxOpenLimitOrders: cfg.DefaultMaxOpenLimitOrders,
		SymbolLookup:              symbolLookup,
		Metrics:                   counterMetrics,
		StartupMode:               startupMode,
		OnDemandClient:            tdClient,
		OnDemandTimeout:           cfg.OnDemandTimeout,
	}, logger)
	if err != nil {
		logger.Fatal("worker manager init", zap.Error(err))
	}

	wg.Add(1)
	mgrDone := make(chan struct{})
	go func() {
		defer wg.Done()
		defer close(mgrDone)
		if err := mgr.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("worker manager exited", zap.Error(err))
		}
	}()

	rpcMux := http.NewServeMux()
	counterPath, counterHandler := counterrpcconnect.NewCounterServiceHandler(server.New(mgr, logger))
	rpcMux.Handle(counterPath, counterHandler)
	// Counter's AssetHolder surface (ADR-0057) shares the same Router so
	// each user's asset-service calls land on the same vshard as their
	// order + balance traffic.
	holderPath, holderHandler := assetholderrpcconnect.NewAssetHolderHandler(server.NewAssetHolderServer(mgr))
	rpcMux.Handle(holderPath, holderHandler)
	rpcSrv := connectx.NewH2CServer(cfg.GRPCAddr, rpcMux)

	wg.Add(1)
	grpcDone := make(chan struct{})
	go func() {
		defer wg.Done()
		defer close(grpcDone)
		logger.Info("gRPC (Connect/h2c) listening", zap.String("addr", cfg.GRPCAddr))
		if err := rpcSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("rpc Serve", zap.Error(err))
		}
	}()

	// ADR-0060 M8 / ADR-0062 M8: Prometheus /metrics + /healthz on a
	// separate HTTP port so ops scrape doesn't compete with gRPC.
	var metricsSrv *http.Server
	metricsDone := make(chan struct{})
	if cfg.MetricsAddr != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", sharedmetrics.Handler(promReg))
		metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		})
		metricsSrv = &http.Server{
			Addr:              cfg.MetricsAddr,
			Handler:           metricsMux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(metricsDone)
			logger.Info("metrics HTTP listening", zap.String("addr", cfg.MetricsAddr))
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("metrics server exited", zap.Error(err))
			}
		}()
	} else {
		close(metricsDone)
	}

	<-rootCtx.Done()
	logger.Info("counter shutting down")

	if metricsSrv != nil {
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
		_ = metricsSrv.Shutdown(shutdownCtx)
		cancelShutdown()
		<-metricsDone
	}

	// Stop gRPC first so no new RPC arrives while workers are draining.
	rpcShutdownCtx, rpcShutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := rpcSrv.Shutdown(rpcShutdownCtx); err != nil {
		logger.Warn("rpc shutdown", zap.Error(err))
	}
	rpcShutdownCancel()
	<-grpcDone

	// Manager → cluster: stopping the manager lets every VShardWorker
	// flush its transactional producer (so trade-dump's shadow sees
	// every committed event, ADR-0061); then the cluster loop exits
	// and releases the node lease + coordinator role gracefully.
	// Counter does not write a final snapshot — trade-dump's snapshot
	// pipeline is the sole producer per ADR-0061 Phase B.
	<-mgrDone
	<-clusterDone

	wg.Wait()
	logger.Info("counter stopped")
}

// startSymbolRegistry wires the ADR-0053 per-symbol precision lookup
// when etcd is reachable. Returns nil when the registry couldn't
// start; workers treat a nil lookup as "compat mode" and skip
// precision enforcement rather than refusing orders.
func startSymbolRegistry(ctx context.Context, cfg Config, logger *zap.Logger) func(string) (etcdcfg.SymbolConfig, bool) {
	symSrc, err := etcdcfg.NewEtcdSource(etcdcfg.EtcdConfig{
		Endpoints: cfg.EtcdEndpoints,
	})
	if err != nil {
		logger.Warn("symbol registry: etcd dial failed, precision stays in compat mode",
			zap.Error(err))
		return nil
	}
	registry := symregistry.New()
	go func() {
		defer func() { _ = symSrc.Close() }()
		if err := registry.Run(ctx, symSrc); err != nil &&
			!errors.Is(err, context.Canceled) {
			logger.Error("symbol registry watch exited", zap.Error(err))
		}
	}()
	logger.Info("symbol registry wired for precision enforcement",
		zap.Strings("etcd", cfg.EtcdEndpoints))
	return registry.Get
}

// newSnapshotStore opens the BlobStore handle Counter uses to LOAD
// snapshots produced by trade-dump's shadow pipeline (ADR-0061 / ADR-
// 0064). Counter is read-only on this store; trade-dump is the sole
// producer, so the fs dir / S3 bucket+prefix configured here must
// match what trade-dump writes. Picks fs vs s3 based on
// --snapshot-backend; S3 credentials come from the AWS SDK default
// config chain (env vars, shared profile, IAM role) so no AK/SK ever
// crosses the CLI.
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

// ---------------------------------------------------------------------------
// Flags
// ---------------------------------------------------------------------------

func parseFlags() Config {
	cfg := Config{
		GRPCAddr:                  ":8081",
		VShardCount:               256, // ADR-0058 §2
		ClusterRoot:               clustering.DefaultRoot,
		JournalTopic:              "counter-journal",
		TradeEventTopic:           "trade-event",
		OrderEventTopic:           "order-event",
		OrderEventTopicPrefix:     "order-event",
		LeaseTTL:                  10,
		SnapshotBackend:           "fs",
		SnapshotDir:               "./data/counter",
		DedupTTL:                  24 * time.Hour,
		DefaultMaxOpenLimitOrders: 100,
		MetricsAddr:               ":9101",
		Env:                       "dev",
		LogLevel:                  "info",
	}
	var brokersStr, etcdStr string

	flag.StringVar(&cfg.NodeID, "node-id", "", "cluster node id (required)")
	flag.StringVar(&cfg.NodeEndpoint, "node-endpoint", "", "advertised RPC endpoint for this node in /cex/counter/nodes (default = --grpc-addr)")
	flag.StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "gRPC listen address")
	flag.IntVar(&cfg.VShardCount, "vshard-count", cfg.VShardCount, "total virtual shards (ADR-0058 §2; 256 in production)")
	flag.StringVar(&cfg.ClusterRoot, "cluster-root", cfg.ClusterRoot, "etcd root prefix for clustering data (default /cex/counter)")

	flag.StringVar(&brokersStr, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flag.StringVar(&cfg.JournalTopic, "journal-topic", cfg.JournalTopic, "counter-journal topic name")
	flag.StringVar(&cfg.TradeEventTopic, "trade-topic", cfg.TradeEventTopic, "trade-event topic name")
	flag.StringVar(&cfg.OrderEventTopic, "order-topic", cfg.OrderEventTopic, "legacy single order-event topic (used when --order-topic-prefix is empty; ADR-0050)")
	flag.StringVar(&cfg.OrderEventTopicPrefix, "order-topic-prefix", cfg.OrderEventTopicPrefix, "per-symbol order-event topic prefix — emits to `<prefix>-<symbol>`. Empty falls back to --order-topic (ADR-0050)")

	flag.StringVar(&etcdStr, "etcd", "", "comma-separated etcd endpoints (required)")
	flag.IntVar(&cfg.LeaseTTL, "lease-ttl", cfg.LeaseTTL, "etcd session TTL seconds (node lease + coordinator election)")

	flag.StringVar(&cfg.SnapshotBackend, "snapshot-backend", cfg.SnapshotBackend, "shared snapshot backend Counter loads from on startup: fs (local dir) | s3 (S3-compatible object store). Must point at the same store trade-dump writes (ADR-0061 / ADR-0064).")
	flag.StringVar(&cfg.SnapshotDir, "snapshot-dir", cfg.SnapshotDir, "directory holding snapshots trade-dump produces; counter reads from here (used when --snapshot-backend=fs)")
	flag.StringVar(&cfg.SnapshotS3Bucket, "snapshot-s3-bucket", cfg.SnapshotS3Bucket, "S3 bucket (required when --snapshot-backend=s3)")
	flag.StringVar(&cfg.SnapshotS3Prefix, "snapshot-s3-prefix", cfg.SnapshotS3Prefix, "S3 key prefix; trailing slash optional (empty = bucket root)")
	flag.StringVar(&cfg.SnapshotS3Region, "snapshot-s3-region", cfg.SnapshotS3Region, "S3 region (empty = AWS SDK default config chain)")
	flag.StringVar(&cfg.SnapshotS3Endpoint, "snapshot-s3-endpoint", cfg.SnapshotS3Endpoint, "S3 endpoint override for MinIO/localstack (empty = AWS default)")

	flag.DurationVar(&cfg.DedupTTL, "dedup-ttl", cfg.DedupTTL, "transfer_id dedup TTL")
	var defaultMaxOpenLimitOrders uint
	flag.UintVar(&defaultMaxOpenLimitOrders, "default-max-open-limit-orders", uint(cfg.DefaultMaxOpenLimitOrders), "ADR-0054 fallback per-(user, symbol) LIMIT cap when SymbolConfig.MaxOpenLimitOrders is zero (0 disables cap; default 100)")

	flag.StringVar(&cfg.MetricsAddr, "metrics-addr", cfg.MetricsAddr, "HTTP /metrics listen address (empty disables; ADR-0060 M8 / ADR-0062 M8)")

	// ADR-0064 on-demand startup flags (startup hot path — see ADR §3).
	flag.StringVar(&cfg.StartupMode, "startup-mode", cfg.StartupMode, "startup recovery mode: auto (default — fall back to legacy on on-demand error) | on-demand (fatal on on-demand error; use for staging verification). Unset --trade-dump-endpoint to take the legacy path entirely. ADR-0064 M4.")
	flag.StringVar(&cfg.TradeDumpEndpoint, "trade-dump-endpoint", cfg.TradeDumpEndpoint, "host:port of trade-dump's TakeSnapshot gRPC server; empty takes the ADR-0060 §4.2 legacy recovery path. ADR-0064.")
	flag.DurationVar(&cfg.OnDemandTimeout, "on-demand-timeout", cfg.OnDemandTimeout, "per-attempt budget for the full on-demand path (sentinel produce + RPC + blob download). 0 → worker default (3s). ADR-0064.")

	flag.StringVar(&cfg.Env, "env", cfg.Env, "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	cfg.Brokers = splitCSV(brokersStr)
	cfg.EtcdEndpoints = splitCSV(etcdStr)
	cfg.DefaultMaxOpenLimitOrders = uint32(defaultMaxOpenLimitOrders)

	if cfg.NodeEndpoint == "" {
		cfg.NodeEndpoint = cfg.GRPCAddr
	}
	return cfg
}

func (c *Config) validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("--node-id required")
	}
	if c.GRPCAddr == "" {
		return fmt.Errorf("--grpc-addr required")
	}
	if c.VShardCount <= 0 {
		return fmt.Errorf("--vshard-count must be > 0")
	}
	if len(c.Brokers) == 0 {
		return fmt.Errorf("at least one --brokers entry required")
	}
	if len(c.EtcdEndpoints) == 0 {
		return fmt.Errorf("--etcd required")
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
