// Command trade-dump runs the OpenTrade persistence sidecar (ADR-0008).
//
// Two pipelines, selectable via --pipelines:
//
//   - sql   (ADR-0008 / 0023 / 0028 / 0047 / 0057): consume
//     trade-event + counter-journal + conditional-event +
//     asset-journal and idempotently project onto MySQL.
//   - snap  (ADR-0061): consume counter-journal per vshard,
//     maintain a ShadowEngine, and write per-vshard snapshots to
//     a blob store (fs / s3). Counter recovery seeks to the
//     JournalOffset embedded in these snapshots on restart.
//
// Default: both pipelines run in-process.
package main

import (
	"context"
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
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	tradedumprpc "github.com/xargin/opentrade/api/gen/rpc/tradedump"
	snapshotpkg "github.com/xargin/opentrade/counter/snapshot"
	"github.com/xargin/opentrade/pkg/logx"
	"github.com/xargin/opentrade/trade-dump/internal/consumer"
	"github.com/xargin/opentrade/trade-dump/internal/snapshot/pipeline"
	"github.com/xargin/opentrade/trade-dump/internal/snapshotrpc"
	"github.com/xargin/opentrade/trade-dump/internal/writer"
)

type Config struct {
	InstanceID string
	Brokers    []string

	// Pipelines controls which projections run. Valid set
	// members: "sql", "snap". Empty slice errors in validate().
	Pipelines []string

	// -- SQL pipeline (ADR-0023 / 0028 / 0047 / 0057) --
	TradeTopic       string
	TradeGroup       string
	JournalTopic     string
	JournalGroup     string
	ConditionalTopic string
	ConditionalGroup string
	AssetTopic       string
	AssetGroup       string

	MySQLDSN            string
	MySQLMaxOpenConns   int
	MySQLMaxIdleConns   int
	MySQLConnMaxLife    time.Duration
	MySQLInsertChunkMax int

	// -- Snapshot pipeline (ADR-0061) --
	VShardCount         int
	SnapshotBackend     string // fs | s3
	SnapshotDir         string
	SnapshotS3Bucket    string
	SnapshotS3Prefix    string
	SnapshotS3Region    string
	SnapshotS3Endpoint  string
	SnapshotFormat      snapshotpkg.Format
	SnapshotInterval    time.Duration
	SnapshotEventCount  uint64
	SnapshotSaveTimeout time.Duration
	SnapshotOwnerIndex  int
	SnapshotOwnerCount  int

	// -- gRPC server (ADR-0064) --
	// GRPCAddr is the TCP address for trade-dump's on-demand
	// snapshot gRPC surface. Empty string disables the server.
	// Default :8088 (chosen to avoid clashing with counter :8081,
	// admin-gateway :8090).
	GRPCAddr string

	// OnDemandConcurrency bounds concurrent in-flight TakeSnapshot
	// requests across all vshards on this instance (ADR-0064 §2.2
	// whole-host restart protection). Default 16.
	OnDemandConcurrency int

	// OnDemandWaitApplyTimeout bounds the per-request wait for
	// shadow to apply up to the partition's LEO. Default 2s.
	// Counter's overall RPC deadline is 3s so this leaves ~1s for
	// LEO query + Capture + upload.
	OnDemandWaitApplyTimeout time.Duration

	// OnDemandWorkTimeout bounds the detached TakeSnapshot worker's total
	// runtime after singleflight starts it. Default 8s.
	OnDemandWorkTimeout time.Duration

	// OnDemandTTL is the max age an on-demand snapshot blob-store
	// object may retain before the housekeeper deletes it
	// (ADR-0064 §2.5). Default 1h.
	OnDemandTTL time.Duration

	// OnDemandSweepInterval is the housekeeper's sweep cadence
	// (ADR-0064 §2.5). Default 5min.
	OnDemandSweepInterval time.Duration

	Env      string
	LogLevel string
}

func main() {
	cfg := parseFlags()

	logger, err := logx.New(logx.Config{Service: "trade-dump", Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	logx.SetGlobal(logger)

	if err := cfg.validate(); err != nil {
		logger.Fatal("invalid config", zap.Error(err))
	}

	logger.Info("trade-dump starting",
		zap.String("instance", cfg.InstanceID),
		zap.Strings("brokers", cfg.Brokers),
		zap.Strings("pipelines", cfg.Pipelines))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	wantSQL := hasPipeline(cfg.Pipelines, "sql")
	wantSnap := hasPipeline(cfg.Pipelines, "snap")

	var wg sync.WaitGroup

	// ---------------- SQL pipeline ----------------
	var (
		mysqlWriter *writer.MySQL
		tradeCons   *consumer.TradeConsumer
		journalCons *consumer.JournalConsumer
		condCons    *consumer.ConditionalConsumer
		assetCons   *consumer.AssetConsumer
	)
	if wantSQL {
		mysqlWriter, err = writer.NewMySQL(writer.MySQLConfig{
			DSN:             cfg.MySQLDSN,
			MaxOpenConns:    cfg.MySQLMaxOpenConns,
			MaxIdleConns:    cfg.MySQLMaxIdleConns,
			ConnMaxLifetime: cfg.MySQLConnMaxLife,
			ChunkSize:       cfg.MySQLInsertChunkMax,
		})
		if err != nil {
			logger.Fatal("mysql open", zap.Error(err))
		}
		defer func() { _ = mysqlWriter.Close() }()

		tradeCons, err = consumer.New(consumer.Config{
			Brokers:  cfg.Brokers,
			ClientID: cfg.InstanceID + "-trade",
			GroupID:  cfg.TradeGroup,
			Topic:    cfg.TradeTopic,
		}, mysqlWriter, logger)
		if err != nil {
			logger.Fatal("trade consumer init", zap.Error(err))
		}
		defer tradeCons.Close()

		journalCons, err = consumer.NewJournal(consumer.JournalConfig{
			Brokers:  cfg.Brokers,
			ClientID: cfg.InstanceID + "-journal",
			GroupID:  cfg.JournalGroup,
			Topic:    cfg.JournalTopic,
		}, mysqlWriter, logger)
		if err != nil {
			logger.Fatal("journal consumer init", zap.Error(err))
		}
		defer journalCons.Close()

		if cfg.ConditionalTopic != "" {
			condCons, err = consumer.NewConditional(consumer.ConditionalConfig{
				Brokers:  cfg.Brokers,
				ClientID: cfg.InstanceID + "-cond",
				GroupID:  cfg.ConditionalGroup,
				Topic:    cfg.ConditionalTopic,
			}, mysqlWriter, logger)
			if err != nil {
				logger.Fatal("conditional consumer init", zap.Error(err))
			}
			defer condCons.Close()
			logger.Info("conditional consumer enabled",
				zap.String("topic", cfg.ConditionalTopic),
				zap.String("group", cfg.ConditionalGroup))
		}
		if cfg.AssetTopic != "" {
			assetCons, err = consumer.NewAsset(consumer.AssetConfig{
				Brokers:  cfg.Brokers,
				ClientID: cfg.InstanceID + "-asset",
				GroupID:  cfg.AssetGroup,
				Topic:    cfg.AssetTopic,
			}, mysqlWriter, logger)
			if err != nil {
				logger.Fatal("asset consumer init", zap.Error(err))
			}
			defer assetCons.Close()
			logger.Info("asset consumer enabled",
				zap.String("topic", cfg.AssetTopic),
				zap.String("group", cfg.AssetGroup))
		}

		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := tradeCons.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("trade consumer exited", zap.Error(err))
				stop()
			}
		}()
		go func() {
			defer wg.Done()
			if err := journalCons.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("journal consumer exited", zap.Error(err))
				stop()
			}
		}()
		if condCons != nil {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := condCons.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
					logger.Error("conditional consumer exited", zap.Error(err))
					stop()
				}
			}()
		}
		if assetCons != nil {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := assetCons.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
					logger.Error("asset consumer exited", zap.Error(err))
					stop()
				}
			}()
		}
	} else {
		logger.Info("sql pipeline disabled (--pipelines)")
	}

	// ---------------- Snapshot pipeline ----------------
	var snapPipe *pipeline.Pipeline
	if wantSnap {
		store, err := newSnapshotStore(rootCtx, cfg, logger)
		if err != nil {
			logger.Fatal("snapshot store init", zap.Error(err))
		}
		snapPipe, err = pipeline.New(pipeline.Config{
			Brokers:            cfg.Brokers,
			ClientID:           cfg.InstanceID + "-snap",
			JournalTopic:       cfg.JournalTopic,
			VShardCount:        cfg.VShardCount,
			OwnedVShards:       ownedSnapshotVShards(cfg.VShardCount, cfg.SnapshotOwnerIndex, cfg.SnapshotOwnerCount),
			Store:              store,
			SnapshotFormat:     cfg.SnapshotFormat,
			SnapshotInterval:   cfg.SnapshotInterval,
			SnapshotEventCount: cfg.SnapshotEventCount,
			SaveTimeout:        cfg.SnapshotSaveTimeout,
			Logger:             logger,
		})
		if err != nil {
			logger.Fatal("snapshot pipeline init", zap.Error(err))
		}
		if err := snapPipe.Start(rootCtx); err != nil {
			logger.Fatal("snapshot pipeline start", zap.Error(err))
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := snapPipe.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("snapshot pipeline exited", zap.Error(err))
				stop()
			}
		}()
		logger.Info("snapshot pipeline enabled",
			zap.Int("vshard_count", cfg.VShardCount),
			zap.Int("owner_index", cfg.SnapshotOwnerIndex),
			zap.Int("owner_count", cfg.SnapshotOwnerCount),
			zap.Ints("owned_vshards", snapPipe.OwnedVShards()),
			zap.String("backend", cfg.SnapshotBackend),
			zap.Duration("interval", cfg.SnapshotInterval),
			zap.Uint64("event_count", cfg.SnapshotEventCount))
	} else {
		logger.Info("snapshot pipeline disabled (--pipelines)")
	}

	// ---------------- gRPC server (ADR-0064 on-demand snapshot) ----------------
	//
	// Started only when the snap pipeline is running: the handler
	// reaches into the pipeline's per-vshard shadow engines, so
	// sql-only replicas would at best return Unimplemented and at
	// worst (if Counter LB'd onto them) silently defeat the latency
	// win by forcing fallback. Empty --grpc-addr disables the
	// server even on snap replicas (single-process dev / tests).
	//
	// Dependency wiring (M1c):
	//   - Shadow: the live pipeline (owns 256 engines, ShadowEngine
	//     returns the engine keyed by counter-journal partition).
	//   - Admin: a dedicated kadm.Client backed by its own kgo
	//     client so LEO queries don't contend with the pipeline's
	//     consumer hot path.
	//   - BlobStore: shared with the pipeline so periodic + on-
	//     demand snapshots live in one key-space (housekeeper
	//     M1d scans it for cleanup).
	//   - KeyPrefix: matches the SnapshotKeyFormat root (no prefix
	//     in the default pipeline config — on-demand keys live
	//     alongside "vshard-NNN.pb" as "vshard-NNN-ondemand-*.pb").
	var grpcSrv *grpc.Server
	grpcDone := make(chan struct{})
	var grpcAdminClient *kgo.Client
	switch {
	case !wantSnap:
		close(grpcDone)
		logger.Info("grpc server disabled (snap pipeline off — on-demand snapshot requires shadow engine)")
	case cfg.GRPCAddr == "":
		close(grpcDone)
		logger.Info("grpc server disabled (--grpc-addr empty)")
	case snapPipe == nil:
		// Defensive: snap pipeline init failed earlier. We'd have
		// fatal'd already, but keep the invariant explicit.
		close(grpcDone)
		logger.Warn("grpc server disabled (snap pipeline not constructed)")
	default:
		// Dedicated admin client — separate from pipeline's consumer
		// so ListEndOffsets traffic is isolated. Closed during
		// shutdown after GracefulStop drains handler goroutines.
		grpcAdminClient, err = kgo.NewClient(
			kgo.SeedBrokers(cfg.Brokers...),
			kgo.ClientID(cfg.InstanceID+"-ondemand-admin"),
		)
		if err != nil {
			logger.Fatal("grpc admin kgo.NewClient", zap.Error(err))
		}
		admin := snapshotrpc.NewKafkaAdminClient(kadm.NewClient(grpcAdminClient))

		store, err := newSnapshotStore(rootCtx, cfg, logger)
		if err != nil {
			logger.Fatal("grpc snapshot store init", zap.Error(err))
		}

		lis, err := net.Listen("tcp", cfg.GRPCAddr)
		if err != nil {
			logger.Fatal("grpc listen", zap.Error(err), zap.String("addr", cfg.GRPCAddr))
		}
		grpcSrv = grpc.NewServer()
		tradedumprpc.RegisterTradeDumpSnapshotServer(grpcSrv, snapshotrpc.New(snapshotrpc.Config{
			Logger:       logger,
			Shadow:       snapPipe,
			Admin:        admin,
			BlobStore:    store,
			JournalTopic: cfg.JournalTopic,
			// KeyPrefix stays empty: the BlobStore impl (FS or
			// S3) already namespaces its keys internally — S3 via
			// its configured Prefix, FS via its baseDir.
			// Piping cfg.SnapshotS3Prefix in here would double-
			// apply it (codex review catch) so on-demand objects
			// would land outside the housekeeper's List prefix
			// and accumulate forever.
			KeyPrefix:        "",
			Concurrency:      cfg.OnDemandConcurrency,
			WaitApplyTimeout: cfg.OnDemandWaitApplyTimeout,
			WorkTimeout:      cfg.OnDemandWorkTimeout,
			SnapshotFormat:   cfg.SnapshotFormat,
		}))
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(grpcDone)
			logger.Info("grpc listening",
				zap.String("addr", cfg.GRPCAddr),
				zap.Int("concurrency", cfg.OnDemandConcurrency),
				zap.Duration("wait_apply_timeout", cfg.OnDemandWaitApplyTimeout))
			if err := grpcSrv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				logger.Error("grpc Serve", zap.Error(err))
				stop()
			}
		}()

		// ADR-0064 §2.5 M1d: on-demand snapshot housekeeper.
		// Deletes stale `vshard-NNN-ondemand-*` blob-store keys
		// older than OnDemandTTL. Driven off the shared blob store
		// and its BlobLister capability. If the configured store
		// impl doesn't implement BlobLister (future backend), we
		// log and skip rather than crash — periodic keys still
		// churn naturally via S3 Lifecycle / operator cleanup.
		if lister, ok := store.(snapshotpkg.BlobLister); ok {
			hk := snapshotrpc.NewHousekeeper(snapshotrpc.Housekeeper{
				Lister:       lister,
				OnDemandGlob: "vshard-",
				TTL:          cfg.OnDemandTTL,
				ScanInterval: cfg.OnDemandSweepInterval,
				Logger:       logger,
			})
			wg.Add(1)
			go func() {
				defer wg.Done()
				hk.Run(rootCtx)
			}()
			logger.Info("on-demand housekeeper started",
				zap.Duration("ttl", cfg.OnDemandTTL),
				zap.Duration("sweep_interval", cfg.OnDemandSweepInterval))
		} else {
			logger.Warn("on-demand housekeeper disabled (BlobStore does not implement BlobLister)")
		}
	}

	<-rootCtx.Done()
	logger.Info("shutdown initiated")
	if grpcSrv != nil {
		grpcSrv.GracefulStop()
		<-grpcDone
		// Close the dedicated admin client after Serve returns so
		// any in-flight ListEndOffsets from draining handlers
		// completes against a live client.
		if grpcAdminClient != nil {
			grpcAdminClient.Close()
		}
	}
	if tradeCons != nil {
		tradeCons.Close()
	}
	if journalCons != nil {
		journalCons.Close()
	}
	if condCons != nil {
		condCons.Close()
	}
	if assetCons != nil {
		assetCons.Close()
	}
	if snapPipe != nil {
		snapPipe.Close()
	}
	wg.Wait()
	logger.Info("trade-dump shutdown complete")
}

func hasPipeline(set []string, want string) bool {
	for _, p := range set {
		if p == want {
			return true
		}
	}
	return false
}

func parseFlags() Config {
	cfg := Config{
		InstanceID:        "trade-dump-0",
		TradeTopic:        "trade-event",
		JournalTopic:      "counter-journal",
		ConditionalTopic:  "conditional-event",
		AssetTopic:        "asset-journal",
		MySQLDSN:          "opentrade:opentrade@tcp(127.0.0.1:3306)/opentrade?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=true",
		MySQLMaxOpenConns: 16,
		MySQLMaxIdleConns: 4,
		MySQLConnMaxLife:  30 * time.Minute,
		VShardCount:       256, // ADR-0058 §2
		SnapshotBackend:   "fs",
		SnapshotDir:       "./data/trade-dump-snap",
		SnapshotFormat:    snapshotpkg.FormatProto,
		// ADR-0064 §5: periodic snapshot relaxed from ADR-0061's
		// 10s/10000 to 60s/60000 now that on-demand carries the
		// hot-path recovery cursor. Operators wanting tighter
		// fallback RPO can override via the flags below.
		SnapshotInterval:         60 * time.Second,
		SnapshotEventCount:       60000,
		SnapshotSaveTimeout:      30 * time.Second,
		SnapshotOwnerIndex:       0,
		SnapshotOwnerCount:       1,
		GRPCAddr:                 ":8088",
		OnDemandConcurrency:      16,
		OnDemandWaitApplyTimeout: 2 * time.Second,
		OnDemandWorkTimeout:      8 * time.Second,
		OnDemandTTL:              1 * time.Hour,
		OnDemandSweepInterval:    5 * time.Minute,
		Env:                      "dev",
		LogLevel:                 "info",
	}
	var brokersStr, pipelinesStr, snapFmtStr string
	flag.StringVar(&cfg.InstanceID, "instance-id", cfg.InstanceID, "instance id (client id prefix)")
	flag.StringVar(&brokersStr, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flag.StringVar(&pipelinesStr, "pipelines", "sql,snap", "comma-separated list of pipelines to run: sql, snap")

	// SQL pipeline flags
	flag.StringVar(&cfg.TradeTopic, "trade-topic", cfg.TradeTopic, "trade-event topic name")
	flag.StringVar(&cfg.TradeGroup, "trade-group", "", "trade-event consumer group (default trade-dump-trade-{instance-id})")
	flag.StringVar(&cfg.JournalTopic, "journal-topic", cfg.JournalTopic, "counter-journal topic name (shared by sql + snap pipelines)")
	flag.StringVar(&cfg.JournalGroup, "journal-group", "", "counter-journal consumer group for sql pipeline (default trade-dump-journal-{instance-id})")
	flag.StringVar(&cfg.ConditionalTopic, "conditional-topic", cfg.ConditionalTopic, "conditional-event topic name (empty disables; ADR-0047)")
	flag.StringVar(&cfg.ConditionalGroup, "conditional-group", "", "conditional-event consumer group (default trade-dump-cond-{instance-id})")
	flag.StringVar(&cfg.AssetTopic, "asset-topic", cfg.AssetTopic, "asset-journal topic name (empty disables; ADR-0057)")
	flag.StringVar(&cfg.AssetGroup, "asset-group", "", "asset-journal consumer group (default trade-dump-asset-{instance-id})")
	flag.StringVar(&cfg.MySQLDSN, "mysql-dsn", cfg.MySQLDSN, "MySQL DSN")
	flag.IntVar(&cfg.MySQLMaxOpenConns, "mysql-max-open", cfg.MySQLMaxOpenConns, "MySQL max open connections")
	flag.IntVar(&cfg.MySQLMaxIdleConns, "mysql-max-idle", cfg.MySQLMaxIdleConns, "MySQL max idle connections")
	flag.DurationVar(&cfg.MySQLConnMaxLife, "mysql-conn-life", cfg.MySQLConnMaxLife, "MySQL connection max lifetime")
	flag.IntVar(&cfg.MySQLInsertChunkMax, "mysql-chunk", cfg.MySQLInsertChunkMax, "rows per multi-row INSERT chunk (default 500)")

	// Snapshot pipeline flags
	flag.IntVar(&cfg.VShardCount, "vshard-count", cfg.VShardCount, "total virtual shards (ADR-0058; 256 in production)")
	flag.StringVar(&cfg.SnapshotBackend, "snapshot-backend", cfg.SnapshotBackend, "snapshot backend: fs | s3 (ADR-0058 phase 1)")
	flag.StringVar(&cfg.SnapshotDir, "snapshot-dir", cfg.SnapshotDir, "local snapshot dir (used when --snapshot-backend=fs)")
	flag.StringVar(&cfg.SnapshotS3Bucket, "snapshot-s3-bucket", cfg.SnapshotS3Bucket, "S3 bucket (required when --snapshot-backend=s3)")
	flag.StringVar(&cfg.SnapshotS3Prefix, "snapshot-s3-prefix", cfg.SnapshotS3Prefix, "S3 key prefix; trailing slash optional")
	flag.StringVar(&cfg.SnapshotS3Region, "snapshot-s3-region", cfg.SnapshotS3Region, "S3 region (empty = AWS SDK default chain)")
	flag.StringVar(&cfg.SnapshotS3Endpoint, "snapshot-s3-endpoint", cfg.SnapshotS3Endpoint, "S3 endpoint override for MinIO / localstack")
	flag.StringVar(&snapFmtStr, "snapshot-format", cfg.SnapshotFormat.String(), "snapshot encoding: proto (default) | json (ADR-0049)")
	flag.DurationVar(&cfg.SnapshotInterval, "snapshot-interval", cfg.SnapshotInterval, "time-window trigger per vshard (ADR-0061 §4.1; default relaxed to 60s by ADR-0064 §5 since on-demand carries the recovery cursor)")
	var snapEventCount uint
	flag.UintVar(&snapEventCount, "snapshot-event-count", uint(cfg.SnapshotEventCount), "event-window trigger per vshard (default 60000 under ADR-0064 §5; was 10000 under ADR-0061)")
	flag.DurationVar(&cfg.SnapshotSaveTimeout, "snapshot-save-timeout", cfg.SnapshotSaveTimeout, "max wall-time per blob-store Save")
	flag.IntVar(&cfg.SnapshotOwnerIndex, "snapshot-owner-index", cfg.SnapshotOwnerIndex, "zero-based snap replica index; owns vshards where vshard % owner-count == owner-index")
	flag.IntVar(&cfg.SnapshotOwnerCount, "snapshot-owner-count", cfg.SnapshotOwnerCount, "total snap replicas sharing vshards; 1 means this instance owns all vshards")

	// gRPC server flags (ADR-0064)
	flag.StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "gRPC listen address for on-demand snapshot (ADR-0064); empty string disables")
	flag.IntVar(&cfg.OnDemandConcurrency, "ondemand-concurrency", cfg.OnDemandConcurrency, "max concurrent on-demand TakeSnapshot requests (ADR-0064 §2.2)")
	flag.DurationVar(&cfg.OnDemandWaitApplyTimeout, "ondemand-wait-apply-timeout", cfg.OnDemandWaitApplyTimeout, "per-request budget for shadow to apply up to LEO before DeadlineExceeded (ADR-0064 §2.5)")
	flag.DurationVar(&cfg.OnDemandWorkTimeout, "ondemand-work-timeout", cfg.OnDemandWorkTimeout, "total budget for detached on-demand snapshot work, including LEO query and blob upload")
	flag.DurationVar(&cfg.OnDemandTTL, "ondemand-ttl", cfg.OnDemandTTL, "max age of on-demand snapshot blob-store objects before housekeeper deletes (ADR-0064 §2.5)")
	flag.DurationVar(&cfg.OnDemandSweepInterval, "ondemand-sweep-interval", cfg.OnDemandSweepInterval, "housekeeper sweep cadence (ADR-0064 §2.5)")

	flag.StringVar(&cfg.Env, "env", cfg.Env, "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	cfg.Brokers = splitCSV(brokersStr)
	cfg.Pipelines = splitCSV(pipelinesStr)
	cfg.SnapshotEventCount = uint64(snapEventCount)
	if cfg.TradeGroup == "" {
		cfg.TradeGroup = "trade-dump-trade-" + cfg.InstanceID
	}
	if cfg.JournalGroup == "" {
		cfg.JournalGroup = "trade-dump-journal-" + cfg.InstanceID
	}
	if cfg.ConditionalGroup == "" {
		cfg.ConditionalGroup = "trade-dump-cond-" + cfg.InstanceID
	}
	if cfg.AssetGroup == "" {
		cfg.AssetGroup = "trade-dump-asset-" + cfg.InstanceID
	}
	if envFmt := os.Getenv("OPENTRADE_SNAPSHOT_FORMAT"); envFmt != "" {
		snapFmtStr = envFmt
	}
	if snapFmtStr != "" {
		f, err := snapshotpkg.ParseFormat(snapFmtStr)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		cfg.SnapshotFormat = f
	}
	return cfg
}

func (c *Config) validate() error {
	if c.InstanceID == "" {
		return fmt.Errorf("instance-id is required")
	}
	if len(c.Brokers) == 0 {
		return fmt.Errorf("at least one broker required")
	}
	if len(c.Pipelines) == 0 {
		return fmt.Errorf("at least one pipeline required (--pipelines=sql,snap)")
	}
	for _, p := range c.Pipelines {
		if p != "sql" && p != "snap" {
			return fmt.Errorf("unknown pipeline %q (valid: sql, snap)", p)
		}
	}
	if hasPipeline(c.Pipelines, "sql") && c.MySQLDSN == "" {
		return fmt.Errorf("mysql-dsn required when sql pipeline enabled")
	}
	if hasPipeline(c.Pipelines, "snap") {
		if c.VShardCount <= 0 {
			return fmt.Errorf("vshard-count > 0 required for snap pipeline")
		}
		if c.SnapshotOwnerCount <= 0 {
			return fmt.Errorf("snapshot-owner-count must be > 0")
		}
		if c.SnapshotOwnerIndex < 0 || c.SnapshotOwnerIndex >= c.SnapshotOwnerCount {
			return fmt.Errorf("snapshot-owner-index must be in [0,%d), got %d", c.SnapshotOwnerCount, c.SnapshotOwnerIndex)
		}
		if len(ownedSnapshotVShards(c.VShardCount, c.SnapshotOwnerIndex, c.SnapshotOwnerCount)) == 0 {
			return fmt.Errorf("snapshot owner %d/%d owns no vshards (vshard-count=%d)", c.SnapshotOwnerIndex, c.SnapshotOwnerCount, c.VShardCount)
		}
		if c.SnapshotBackend == "s3" && c.SnapshotS3Bucket == "" {
			return fmt.Errorf("snapshot-s3-bucket required when --snapshot-backend=s3")
		}
	}
	return nil
}

func ownedSnapshotVShards(vshardCount, ownerIndex, ownerCount int) []int {
	if ownerCount <= 1 {
		out := make([]int, vshardCount)
		for v := 0; v < vshardCount; v++ {
			out[v] = v
		}
		return out
	}
	out := make([]int, 0, (vshardCount+ownerCount-1)/ownerCount)
	for v := 0; v < vshardCount; v++ {
		if v%ownerCount == ownerIndex {
			out = append(out, v)
		}
	}
	return out
}

// newSnapshotStore mirrors counter/cmd/counter/main.go.newSnapshotStore
// — same backend semantics, so a shared S3 bucket key-space stays
// consistent between Counter's reader and trade-dump's writer.
func newSnapshotStore(ctx context.Context, cfg Config, logger *zap.Logger) (snapshotpkg.BlobStore, error) {
	switch cfg.SnapshotBackend {
	case "fs":
		logger.Info("snapshot backend: fs", zap.String("dir", cfg.SnapshotDir))
		return snapshotpkg.NewFSBlobStore(cfg.SnapshotDir), nil
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
				o.UsePathStyle = true
			}
		})
		logger.Info("snapshot backend: s3",
			zap.String("bucket", cfg.SnapshotS3Bucket),
			zap.String("prefix", cfg.SnapshotS3Prefix),
			zap.String("region", cfg.SnapshotS3Region),
			zap.String("endpoint", cfg.SnapshotS3Endpoint))
		return snapshotpkg.NewS3BlobStore(client, cfg.SnapshotS3Bucket, cfg.SnapshotS3Prefix), nil
	default:
		return nil, fmt.Errorf("unknown snapshot backend %q", cfg.SnapshotBackend)
	}
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
