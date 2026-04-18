// Command counter runs the OpenTrade Counter service.
//
// Responsibilities (MVP-3):
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
// Later MVPs add etcd lease election (MVP-8) and Kafka EOS consumer semantics.
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
	"github.com/xargin/opentrade/pkg/idgen"
	"github.com/xargin/opentrade/pkg/logx"
)

type Config struct {
	ShardID         int
	TotalShards     int    // 0 disables the shard ownership guard (legacy / single-shard)
	InstanceID      string // defaults to counter-shard-<N>-main
	GRPCAddr        string
	Brokers         []string
	JournalTopic    string
	OrderEventTopic string
	TradeEventTopic string
	ConsumerGroup   string // trade-event consumer group (default "counter-shard-<N>")
	SnapshotDir     string
	DedupTTL        time.Duration
	Env             string
	LogLevel        string
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
		zap.String("snapshot_dir", cfg.SnapshotDir))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- Core components -----------------------------------------------------

	state := engine.NewShardState(cfg.ShardID)
	userSeq := sequencer.New()
	dt := dedup.New(cfg.DedupTTL)

	if err := tryRestoreSnapshot(cfg, state, userSeq, dt, logger); err != nil {
		logger.Fatal("snapshot restore", zap.Error(err))
	}

	// Non-transactional producer (Transfer path + Settlement events).
	journalProducer, err := journal.NewJournalProducer(journal.ProducerConfig{
		Brokers:    cfg.Brokers,
		ClientID:   cfg.InstanceID + "-journal",
		ProducerID: cfg.InstanceID,
		Topic:      cfg.JournalTopic,
	}, logger)
	if err != nil {
		logger.Fatal("journal producer", zap.Error(err))
	}
	defer journalProducer.Close()

	// Transactional producer (PlaceOrder / CancelOrder path, ADR-0005/0017).
	txnProducer, err := journal.NewTxnProducer(rootCtx, journal.TxnProducerConfig{
		Brokers:         cfg.Brokers,
		ClientID:        cfg.InstanceID + "-txn",
		TransactionalID: cfg.InstanceID,
		JournalTopic:    cfg.JournalTopic,
		OrderEventTopic: cfg.OrderEventTopic,
	}, logger)
	if err != nil {
		logger.Fatal("txn producer", zap.Error(err))
	}
	defer txnProducer.Close()

	// Snowflake id generator for order ids; seeded with shard id as machine id.
	idg, err := idgen.NewGenerator(cfg.ShardID)
	if err != nil {
		logger.Fatal("idgen", zap.Error(err))
	}

	svc := service.New(service.Config{
		ShardID:     cfg.ShardID,
		TotalShards: cfg.TotalShards,
		ProducerID:  cfg.InstanceID,
	}, state, userSeq, dt, journalProducer, logger)
	svc.SetOrderDeps(txnProducer, idg)

	// --- Trade consumer ------------------------------------------------------

	tradeConsumer, err := journal.NewTradeConsumer(journal.TradeConsumerConfig{
		Brokers:  cfg.Brokers,
		ClientID: cfg.InstanceID + "-trade",
		GroupID:  cfg.ConsumerGroup,
		Topic:    cfg.TradeEventTopic,
	}, svc, logger)
	if err != nil {
		logger.Fatal("trade consumer", zap.Error(err))
	}
	defer tradeConsumer.Close()

	var tradeWG sync.WaitGroup
	tradeWG.Add(1)
	go func() {
		defer tradeWG.Done()
		if err := tradeConsumer.Run(rootCtx); err != nil && err != context.Canceled {
			logger.Error("trade consumer exited", zap.Error(err))
		}
	}()

	// --- gRPC server ---------------------------------------------------------

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

	// --- Shutdown ------------------------------------------------------------

	<-rootCtx.Done()
	logger.Info("shutdown initiated")

	grpcServer.GracefulStop()
	grpcWG.Wait()

	tradeConsumer.Close()
	tradeWG.Wait()

	// Brief wait for in-flight sequencer tasks to drain before snapshotting.
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
		GRPCAddr:        ":8081",
		JournalTopic:    "counter-journal",
		OrderEventTopic: "order-event",
		TradeEventTopic: "trade-event",
		SnapshotDir:     "./data/counter",
		DedupTTL:        24 * time.Hour,
		Env:             "dev",
		LogLevel:        "info",
	}
	var brokersStr string
	flag.IntVar(&cfg.ShardID, "shard-id", 0, "shard id (0..total-shards-1)")
	flag.IntVar(&cfg.TotalShards, "total-shards", 10, "total shard count for user→shard routing (0 disables guard)")
	flag.StringVar(&cfg.InstanceID, "instance-id", "", "instance id (default counter-shard-<N>-main)")
	flag.StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "gRPC listen address")
	flag.StringVar(&brokersStr, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flag.StringVar(&cfg.JournalTopic, "journal-topic", cfg.JournalTopic, "counter-journal topic name")
	flag.StringVar(&cfg.OrderEventTopic, "order-topic", cfg.OrderEventTopic, "order-event topic name")
	flag.StringVar(&cfg.TradeEventTopic, "trade-topic", cfg.TradeEventTopic, "trade-event topic name")
	flag.StringVar(&cfg.ConsumerGroup, "group", "", "Kafka consumer group (default counter-shard-<N>)")
	flag.StringVar(&cfg.SnapshotDir, "snapshot-dir", cfg.SnapshotDir, "local directory for snapshots")
	flag.DurationVar(&cfg.DedupTTL, "dedup-ttl", cfg.DedupTTL, "transfer_id dedup TTL")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	cfg.Brokers = splitCSV(brokersStr)
	if cfg.InstanceID == "" {
		cfg.InstanceID = fmt.Sprintf("counter-shard-%d-main", cfg.ShardID)
	}
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = fmt.Sprintf("counter-shard-%d", cfg.ShardID)
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
		zap.Int("accounts", len(snap.Accounts)),
		zap.Int("orders", len(snap.Orders)))
	return nil
}

func writeSnapshot(cfg Config, state *engine.ShardState, seq *sequencer.UserSequencer, dt *dedup.Table) error {
	snap := snapshot.Capture(cfg.ShardID, state, seq, dt, time.Now().UnixMilli())
	return snapshot.Save(snapshotPath(cfg), snap)
}
