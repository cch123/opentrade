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
	VShardCount        int
	SnapshotBackend    string // fs | s3
	SnapshotDir        string
	SnapshotS3Bucket   string
	SnapshotS3Prefix   string
	SnapshotS3Region   string
	SnapshotS3Endpoint string
	SnapshotFormat     snapshotpkg.Format
	SnapshotInterval   time.Duration
	SnapshotEventCount uint64
	SnapshotSaveTimeout time.Duration

	// -- gRPC server (ADR-0064) --
	// GRPCAddr is the TCP address for trade-dump's on-demand
	// snapshot gRPC surface. Empty string disables the server.
	// Default :8088 (chosen to avoid clashing with counter :8081,
	// admin-gateway :8090).
	GRPCAddr string

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
			zap.String("backend", cfg.SnapshotBackend),
			zap.Duration("interval", cfg.SnapshotInterval),
			zap.Uint64("event_count", cfg.SnapshotEventCount))
	} else {
		logger.Info("snapshot pipeline disabled (--pipelines)")
	}

	// ---------------- gRPC server (ADR-0064 on-demand snapshot) ----------------
	//
	// Only started when the snap pipeline is running. Rationale
	// (codex review catch, ADR-0064 M1b):
	//
	//   - The TakeSnapshot handler fundamentally depends on the
	//     shadow engine (ADR-0061). An sql-only trade-dump replica
	//     has no shadow state to capture, so any client call would
	//     at best return Unimplemented (M1b) or a stub error (M1c+).
	//   - If sql-only replicas bind the same advertised port, a
	//     Counter client load-balanced onto one would always take
	//     the legacy fallback path even when a snap-capable replica
	//     is healthy. That silently defeats the latency win.
	//   - sql-only replicas should also not fail hard just because
	//     `:8088` happens to be held by something else on the host.
	//
	// Empty --grpc-addr disables the server even on snap replicas
	// (for single-process dev / tests that don't need the RPC).
	var grpcSrv *grpc.Server
	grpcDone := make(chan struct{})
	switch {
	case !wantSnap:
		close(grpcDone)
		logger.Info("grpc server disabled (snap pipeline off — on-demand snapshot requires shadow engine)")
	case cfg.GRPCAddr == "":
		close(grpcDone)
		logger.Info("grpc server disabled (--grpc-addr empty)")
	default:
		lis, err := net.Listen("tcp", cfg.GRPCAddr)
		if err != nil {
			logger.Fatal("grpc listen", zap.Error(err), zap.String("addr", cfg.GRPCAddr))
		}
		grpcSrv = grpc.NewServer()
		tradedumprpc.RegisterTradeDumpSnapshotServer(grpcSrv, snapshotrpc.New(snapshotrpc.Config{
			Logger: logger,
		}))
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer close(grpcDone)
			logger.Info("grpc listening", zap.String("addr", cfg.GRPCAddr))
			if err := grpcSrv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				logger.Error("grpc Serve", zap.Error(err))
				stop()
			}
		}()
	}

	<-rootCtx.Done()
	logger.Info("shutdown initiated")
	if grpcSrv != nil {
		grpcSrv.GracefulStop()
		<-grpcDone
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
		InstanceID:          "trade-dump-0",
		TradeTopic:          "trade-event",
		JournalTopic:        "counter-journal",
		ConditionalTopic:    "conditional-event",
		AssetTopic:          "asset-journal",
		MySQLDSN:            "opentrade:opentrade@tcp(127.0.0.1:3306)/opentrade?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=true",
		MySQLMaxOpenConns:   16,
		MySQLMaxIdleConns:   4,
		MySQLConnMaxLife:    30 * time.Minute,
		VShardCount:         256, // ADR-0058 §2
		SnapshotBackend:     "fs",
		SnapshotDir:         "./data/trade-dump-snap",
		SnapshotFormat:      snapshotpkg.FormatProto,
		SnapshotInterval:    10 * time.Second,
		SnapshotEventCount:  10000,
		SnapshotSaveTimeout: 30 * time.Second,
		GRPCAddr:            ":8088",
		Env:                 "dev",
		LogLevel:            "info",
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
	flag.DurationVar(&cfg.SnapshotInterval, "snapshot-interval", cfg.SnapshotInterval, "time-window trigger per vshard (ADR-0061 §4.1)")
	var snapEventCount uint
	flag.UintVar(&snapEventCount, "snapshot-event-count", uint(cfg.SnapshotEventCount), "event-window trigger per vshard")
	flag.DurationVar(&cfg.SnapshotSaveTimeout, "snapshot-save-timeout", cfg.SnapshotSaveTimeout, "max wall-time per blob-store Save")

	// gRPC server flags (ADR-0064)
	flag.StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "gRPC listen address for on-demand snapshot (ADR-0064); empty string disables")

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
		if c.SnapshotBackend == "s3" && c.SnapshotS3Bucket == "" {
			return fmt.Errorf("snapshot-s3-bucket required when --snapshot-backend=s3")
		}
	}
	return nil
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
