// Command conditional runs the OpenTrade conditional-order service.
//
// MVP-14a scope (ADR-0040):
//   - Accepts STOP_LOSS / STOP_LOSS_LIMIT / TAKE_PROFIT / TAKE_PROFIT_LIMIT
//     via ConditionalService gRPC.
//   - Subscribes to market-data and fires Counter.PlaceOrder when the
//     last PublicTrade price crosses the stop threshold.
//   - Periodic local JSON snapshot + per-partition offset so a restart
//     resumes from the saved offset (not topic tail).
//
// Funds are not reserved at placement; the inner order can fail with
// INSUFFICIENT_BALANCE at trigger time, which we surface as REJECTED.
// MVP-14b will add proper reservation.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	condrpc "github.com/xargin/opentrade/api/gen/rpc/conditional"
	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/conditional/internal/consumer"
	"github.com/xargin/opentrade/conditional/internal/counterclient"
	"github.com/xargin/opentrade/conditional/internal/engine"
	"github.com/xargin/opentrade/conditional/internal/server"
	"github.com/xargin/opentrade/conditional/internal/service"
	"github.com/xargin/opentrade/conditional/internal/snapshot"
	"github.com/xargin/opentrade/pkg/idgen"
	"github.com/xargin/opentrade/pkg/logx"
)

type Config struct {
	InstanceID           string
	GRPCAddr             string
	Brokers              []string
	MarketTopic          string
	ConsumerGroup        string
	CounterShards        []string
	SnapshotDir          string
	SnapshotInterval     time.Duration
	TerminalHistoryLimit int

	// idgen shard id — snowflake layout (ADR: counter/cmd/counter uses
	// ShardID for this). For conditional we give it its own shard id
	// range by default to avoid id collisions with counter order ids
	// even though the namespaces are disjoint today.
	IDGenShard int

	Env      string
	LogLevel string
}

func main() {
	cfg := parseFlags()

	logger, err := logx.New(logx.Config{Service: "conditional", Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	logx.SetGlobal(logger)

	if err := cfg.validate(); err != nil {
		logger.Fatal("invalid config", zap.Error(err))
	}
	logger.Info("conditional starting",
		zap.String("instance", cfg.InstanceID),
		zap.String("grpc", cfg.GRPCAddr),
		zap.Strings("brokers", cfg.Brokers),
		zap.Strings("counter_shards", cfg.CounterShards))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	counterConns, counterClients, err := dialCounterShards(rootCtx, cfg.CounterShards)
	if err != nil {
		logger.Fatal("dial counter shards", zap.Error(err))
	}
	defer func() {
		for i, c := range counterConns {
			if err := c.Close(); err != nil {
				logger.Debug("close counter conn", zap.Int("shard", i), zap.Error(err))
			}
		}
	}()
	placer, err := counterclient.NewSharded(counterClients)
	if err != nil {
		logger.Fatal("sharded counter", zap.Error(err))
	}

	idg, err := idgen.NewGenerator(cfg.IDGenShard)
	if err != nil {
		logger.Fatal("idgen", zap.Error(err))
	}

	eng := engine.New(engine.Config{
		TerminalHistoryLimit: cfg.TerminalHistoryLimit,
	}, idg, placer, placer, logger)

	initialOffsets, err := tryRestoreSnapshot(cfg, eng, logger)
	if err != nil {
		logger.Fatal("snapshot restore", zap.Error(err))
	}

	mdCons, err := consumer.New(consumer.Config{
		Brokers:        cfg.Brokers,
		ClientID:       cfg.InstanceID,
		GroupID:        cfg.ConsumerGroup,
		Topic:          cfg.MarketTopic,
		InitialOffsets: initialOffsets,
	}, eng.HandleRecord, logger)
	if err != nil {
		logger.Fatal("market-data consumer", zap.Error(err))
	}
	defer mdCons.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := mdCons.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("consumer exited", zap.Error(err))
			stop()
		}
	}()

	snapCtx, cancelSnap := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		runSnapshotTicker(snapCtx, cfg, eng, logger)
	}()

	svc := service.New(eng)
	grpcSrv := grpc.NewServer()
	condrpc.RegisterConditionalServiceServer(grpcSrv, server.New(svc, nil))

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Fatal("listen", zap.Error(err))
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("gRPC listening", zap.String("addr", cfg.GRPCAddr))
		if err := grpcSrv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error("grpc serve", zap.Error(err))
		}
	}()

	<-rootCtx.Done()
	logger.Info("shutdown initiated")
	grpcSrv.GracefulStop()
	mdCons.Close()
	cancelSnap()
	wg.Wait()
	if err := writeSnapshot(cfg, eng); err != nil {
		logger.Error("final snapshot", zap.Error(err))
	} else if cfg.SnapshotDir != "" {
		logger.Info("final snapshot written", zap.String("path", snapshotPath(cfg)))
	}
	logger.Info("conditional shutdown complete")
}

// ---------------------------------------------------------------------------
// Snapshot helpers
// ---------------------------------------------------------------------------

func snapshotPath(cfg Config) string {
	return filepath.Join(cfg.SnapshotDir, cfg.InstanceID+".json")
}

func tryRestoreSnapshot(cfg Config, eng *engine.Engine, logger *zap.Logger) (map[int32]int64, error) {
	if cfg.SnapshotDir == "" {
		logger.Info("snapshot disabled (empty --snapshot-dir); cold start")
		return nil, nil
	}
	path := snapshotPath(cfg)
	snap, err := snapshot.Load(path)
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", path, err)
	}
	if snap == nil {
		logger.Info("no snapshot found; cold start", zap.String("path", path))
		return nil, nil
	}
	if err := snapshot.Restore(eng, snap); err != nil {
		return nil, fmt.Errorf("engine restore: %w", err)
	}
	logger.Info("conditional state restored",
		zap.String("path", path),
		zap.Int64("taken_at_ms", snap.TakenAtMs),
		zap.Int("pending", len(snap.Pending)),
		zap.Int("terminals", len(snap.Terminals)),
		zap.Int("partitions", len(snap.Offsets)))
	return snap.Offsets, nil
}

func writeSnapshot(cfg Config, eng *engine.Engine) error {
	if cfg.SnapshotDir == "" {
		return nil
	}
	snap := snapshot.Capture(eng)
	snap.TakenAtMs = time.Now().UnixMilli()
	return snapshot.Save(snapshotPath(cfg), snap)
}

func runSnapshotTicker(ctx context.Context, cfg Config, eng *engine.Engine, logger *zap.Logger) {
	if cfg.SnapshotDir == "" || cfg.SnapshotInterval <= 0 {
		return
	}
	ticker := time.NewTicker(cfg.SnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := writeSnapshot(cfg, eng); err != nil {
				logger.Error("snapshot", zap.Error(err))
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Counter dial
// ---------------------------------------------------------------------------

func dialCounterShards(ctx context.Context, endpoints []string) ([]*grpc.ClientConn, []counterrpc.CounterServiceClient, error) {
	conns := make([]*grpc.ClientConn, 0, len(endpoints))
	clients := make([]counterrpc.CounterServiceClient, 0, len(endpoints))
	for i, ep := range endpoints {
		conn, c, err := counterclient.Dial(ctx, ep)
		if err != nil {
			for _, prior := range conns {
				_ = prior.Close()
			}
			return nil, nil, fmt.Errorf("shard %d (%s): %w", i, ep, err)
		}
		conns = append(conns, conn)
		clients = append(clients, c)
	}
	return conns, clients, nil
}

// ---------------------------------------------------------------------------
// Flags
// ---------------------------------------------------------------------------

func parseFlags() Config {
	cfg := Config{
		InstanceID:           "conditional-0",
		GRPCAddr:             ":8082",
		MarketTopic:          "market-data",
		SnapshotDir:          "./data/conditional",
		SnapshotInterval:     30 * time.Second,
		TerminalHistoryLimit: 1000,
		IDGenShard:           900, // deliberately out of counter's 0..99 range
		Env:                  "dev",
		LogLevel:             "info",
	}
	var brokersStr, shardsStr string
	flag.StringVar(&cfg.InstanceID, "instance-id", cfg.InstanceID, "instance id (grpc client id / default group suffix)")
	flag.StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "gRPC listen address")
	flag.StringVar(&brokersStr, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flag.StringVar(&cfg.MarketTopic, "market-topic", cfg.MarketTopic, "market-data topic name")
	flag.StringVar(&cfg.ConsumerGroup, "group", "", "Kafka consumer group (default conditional-{instance-id})")
	flag.StringVar(&shardsStr, "counter-shards", "localhost:8081",
		"comma-separated Counter gRPC endpoints, in shard-id order (shard 0 first)")
	flag.StringVar(&cfg.SnapshotDir, "snapshot-dir", cfg.SnapshotDir, "directory for engine-state snapshots (empty disables)")
	flag.DurationVar(&cfg.SnapshotInterval, "snapshot-interval", cfg.SnapshotInterval, "snapshot cadence; 0 disables the ticker (still writes on shutdown)")
	flag.IntVar(&cfg.TerminalHistoryLimit, "terminal-history", cfg.TerminalHistoryLimit, "max retained terminal records per engine for ListConditionals(include_inactive)")
	flag.IntVar(&cfg.IDGenShard, "idgen-shard", cfg.IDGenShard, "snowflake shard id for conditional ids (avoid collisions with counter)")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	cfg.Brokers = splitCSV(brokersStr)
	cfg.CounterShards = splitCSV(shardsStr)
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "conditional-" + cfg.InstanceID
	}
	return cfg
}

func (c *Config) validate() error {
	if c.InstanceID == "" {
		return fmt.Errorf("instance-id required")
	}
	if c.GRPCAddr == "" {
		return fmt.Errorf("grpc-addr required")
	}
	if len(c.Brokers) == 0 {
		return fmt.Errorf("at least one Kafka broker required")
	}
	if len(c.CounterShards) == 0 {
		return fmt.Errorf("at least one counter shard required")
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
