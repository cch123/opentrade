// Command counter runs the OpenTrade Counter service.
//
// Responsibilities (MVP-2):
//   - Serve the CounterService gRPC (Transfer / QueryBalance for now).
//   - Serialize per-user work through the UserSequencer (ADR-0018).
//   - Publish every state-mutating change to the counter-journal Kafka topic
//     (ADR-0004) before committing it to memory (ADR-0001 write-ahead order).
//   - Persist ShardState + dedup + shard seq to local snapshot on graceful
//     shutdown; auto-restore on next start.
//
// Configuration is via flags + OPENTRADE_* env vars. Later MVPs add etcd
// lease election (MVP-8) and trade-event consumption (MVP-3).
package main

import (
	"context"
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

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/journal"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/counter/internal/server"
	"github.com/xargin/opentrade/counter/internal/service"
	"github.com/xargin/opentrade/counter/internal/snapshot"
	"github.com/xargin/opentrade/pkg/logx"
)

type Config struct {
	ShardID     int
	InstanceID  string // defaults to "counter-shard-<N>-main"
	GRPCAddr    string // ":8081"
	Brokers     []string
	JournalTopic string
	SnapshotDir string
	DedupTTL    time.Duration
	Env         string
	LogLevel    string
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
		zap.String("instance", cfg.InstanceID),
		zap.String("grpc", cfg.GRPCAddr),
		zap.Strings("brokers", cfg.Brokers),
		zap.String("snapshot_dir", cfg.SnapshotDir))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- Build core components ----------------------------------------------

	state := engine.NewShardState(cfg.ShardID)
	userSeq := sequencer.New()
	dt := dedup.New(cfg.DedupTTL)

	if err := tryRestoreSnapshot(cfg, state, userSeq, dt, logger); err != nil {
		logger.Fatal("snapshot restore", zap.Error(err))
	}

	producer, err := journal.NewJournalProducer(journal.ProducerConfig{
		Brokers:    cfg.Brokers,
		ClientID:   cfg.InstanceID,
		ProducerID: cfg.InstanceID,
		Topic:      cfg.JournalTopic,
	}, logger)
	if err != nil {
		logger.Fatal("journal producer", zap.Error(err))
	}
	defer producer.Close()

	svc := service.New(service.Config{
		ShardID:    cfg.ShardID,
		ProducerID: cfg.InstanceID,
	}, state, userSeq, dt, producer, logger)

	// --- gRPC server --------------------------------------------------------

	grpcServer := grpc.NewServer()
	counterrpc.RegisterCounterServiceServer(grpcServer, server.New(svc, logger))

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Fatal("listen", zap.Error(err))
	}
	var grpcWG sync.WaitGroup
	grpcWG.Add(1)
	go func() {
		defer grpcWG.Done()
		logger.Info("gRPC listening", zap.String("addr", cfg.GRPCAddr))
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("grpc Serve", zap.Error(err))
		}
	}()

	// --- Wait for shutdown --------------------------------------------------

	<-rootCtx.Done()
	logger.Info("shutdown initiated")

	grpcServer.GracefulStop()
	grpcWG.Wait()

	// Snapshot before exit. New gRPC requests are rejected (server stopped)
	// but the UserSequencer may have in-flight tasks; we wait briefly.
	time.Sleep(100 * time.Millisecond)

	if err := writeSnapshot(cfg, state, userSeq, dt); err != nil {
		logger.Error("final snapshot", zap.Error(err))
	} else {
		logger.Info("final snapshot written")
	}

	logger.Info("counter shutdown complete")
}

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

func parseFlags() Config {
	cfg := Config{
		GRPCAddr:     ":8081",
		JournalTopic: "counter-journal",
		SnapshotDir:  "./data/counter",
		DedupTTL:     24 * time.Hour,
		Env:          "dev",
		LogLevel:     "info",
	}
	var brokersStr string
	flag.IntVar(&cfg.ShardID, "shard-id", 0, "shard id (0..N-1)")
	flag.StringVar(&cfg.InstanceID, "instance-id", "", "instance id (default counter-shard-<N>-main)")
	flag.StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "gRPC listen address")
	flag.StringVar(&brokersStr, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flag.StringVar(&cfg.JournalTopic, "journal-topic", cfg.JournalTopic, "counter-journal topic name")
	flag.StringVar(&cfg.SnapshotDir, "snapshot-dir", cfg.SnapshotDir, "local directory for snapshots")
	flag.DurationVar(&cfg.DedupTTL, "dedup-ttl", cfg.DedupTTL, "transfer_id dedup TTL")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	cfg.Brokers = splitCSV(brokersStr)
	if cfg.InstanceID == "" {
		cfg.InstanceID = fmt.Sprintf("counter-shard-%d-main", cfg.ShardID)
	}
	return cfg
}

func (c *Config) validate() error {
	if c.ShardID < 0 {
		return fmt.Errorf("shard-id must be >= 0")
	}
	if c.GRPCAddr == "" {
		return fmt.Errorf("grpc-addr required")
	}
	if len(c.Brokers) == 0 {
		return fmt.Errorf("at least one broker required")
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

func snapshotPath(cfg Config) string {
	return filepath.Join(cfg.SnapshotDir, fmt.Sprintf("shard-%d.json", cfg.ShardID))
}

func tryRestoreSnapshot(cfg Config, state *engine.ShardState, seq *sequencer.UserSequencer, dt *dedup.Table, logger *zap.Logger) error {
	path := snapshotPath(cfg)
	snap, err := snapshot.Load(path)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Info("no snapshot found, starting fresh", zap.Int("shard_id", cfg.ShardID))
			return nil
		}
		return fmt.Errorf("load %s: %w", path, err)
	}
	if err := snapshot.Restore(cfg.ShardID, state, seq, dt, snap); err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	logger.Info("restored from snapshot",
		zap.Int("shard_id", cfg.ShardID),
		zap.Uint64("shard_seq", snap.ShardSeq),
		zap.Int("accounts", len(snap.Accounts)))
	return nil
}

func writeSnapshot(cfg Config, state *engine.ShardState, seq *sequencer.UserSequencer, dt *dedup.Table) error {
	snap := snapshot.Capture(cfg.ShardID, state, seq, dt, time.Now().UnixMilli())
	return snapshot.Save(snapshotPath(cfg), snap)
}
