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

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/journal"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/counter/internal/server"
	"github.com/xargin/opentrade/counter/internal/service"
	"github.com/xargin/opentrade/counter/internal/snapshot"
	"github.com/xargin/opentrade/pkg/election"
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
	OrderEventTopic  string
	TradeEventTopic  string
	ConsumerGroup    string
	SnapshotDir      string
	SnapshotInterval time.Duration
	DedupTTL         time.Duration

	// HA (MVP-12).
	HAMode         string // "disabled" (default) | "auto"
	EtcdEndpoints  []string
	ElectionPath   string
	LeaseTTL       int    // seconds; default 10
	CampaignBackoff time.Duration // wait between failed Campaigns

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

	if cfg.HAMode == "disabled" {
		runPrimary(rootCtx, cfg, logger)
		return
	}
	runElectionLoop(rootCtx, cfg, logger)
}

// runElectionLoop campaigns for leadership, then runs the primary body for
// the duration of each leadership cycle. It exits when rootCtx is cancelled.
func runElectionLoop(rootCtx context.Context, cfg Config, logger *zap.Logger) {
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

		runPrimary(primaryCtx, cfg, logger)
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
func runPrimary(ctx context.Context, cfg Config, logger *zap.Logger) {
	state := engine.NewShardState(cfg.ShardID)
	userSeq := sequencer.New()
	dt := dedup.New(cfg.DedupTTL)

	if err := tryRestoreSnapshot(cfg, state, userSeq, dt, logger); err != nil {
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
		OrderEventTopic: cfg.OrderEventTopic,
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
		ShardID:     cfg.ShardID,
		TotalShards: cfg.TotalShards,
		ProducerID:  cfg.InstanceID,
	}, state, userSeq, dt, txnProducer, logger)
	svc.SetOrderDeps(txnProducer, idg)

	tradeConsumer, err := journal.NewTradeConsumer(journal.TradeConsumerConfig{
		Brokers:  cfg.Brokers,
		ClientID: cfg.InstanceID + "-trade",
		GroupID:  cfg.ConsumerGroup,
		Topic:    cfg.TradeEventTopic,
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
	// (each underlying store takes its own RWMutex + returns deep copies)
	// and records ShardSeq before scanning state, so restore replays any
	// in-flight divergence via the idempotent trade / transfer handlers.
	snapCtx, cancelSnap := context.WithCancel(context.Background())
	defer cancelSnap()
	var snapWG sync.WaitGroup
	snapWG.Add(1)
	go func() {
		defer snapWG.Done()
		periodicSnapshot(snapCtx, cfg, state, userSeq, dt, logger)
	}()

	grpcServer := grpc.NewServer()
	counterrpc.RegisterCounterServiceServer(grpcServer, server.New(svc, logger))
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

	if err := writeSnapshot(cfg, state, userSeq, dt); err != nil {
		logger.Error("final snapshot", zap.Error(err))
	} else {
		logger.Info("final snapshot written")
	}
	logger.Info("primary stopped")
}

// periodicSnapshot writes a full shard snapshot to SnapshotDir on each tick.
// SnapshotInterval <= 0 disables the loop (only the shutdown-time snapshot
// runs, matching pre-MVP-12b behaviour).
func periodicSnapshot(ctx context.Context, cfg Config, state *engine.ShardState, seq *sequencer.UserSequencer, dt *dedup.Table, logger *zap.Logger) {
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
			if err := writeSnapshot(cfg, state, seq, dt); err != nil {
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
		OrderEventTopic:  "order-event",
		TradeEventTopic:  "trade-event",
		SnapshotDir:      "./data/counter",
		SnapshotInterval: 60 * time.Second,
		DedupTTL:         24 * time.Hour,
		HAMode:           "disabled",
		LeaseTTL:         10,
		CampaignBackoff:  2 * time.Second,
		Env:              "dev",
		LogLevel:         "info",
	}
	var brokersStr, etcdStr string
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
	flag.DurationVar(&cfg.SnapshotInterval, "snapshot-interval", cfg.SnapshotInterval, "periodic snapshot cadence while primary (0 disables; only final shutdown snapshot runs)")
	flag.DurationVar(&cfg.DedupTTL, "dedup-ttl", cfg.DedupTTL, "transfer_id dedup TTL")
	flag.StringVar(&cfg.HAMode, "ha-mode", cfg.HAMode, "ha mode: disabled | auto (etcd leader election, ADR-0031)")
	flag.StringVar(&etcdStr, "etcd", "", "comma-separated etcd endpoints (required when --ha-mode=auto)")
	flag.StringVar(&cfg.ElectionPath, "election-path", "", "etcd election key (default /cex/counter/shard-<N>/leader)")
	flag.IntVar(&cfg.LeaseTTL, "lease-ttl", cfg.LeaseTTL, "etcd session TTL seconds")
	flag.DurationVar(&cfg.CampaignBackoff, "campaign-backoff", cfg.CampaignBackoff, "wait between failed Campaigns")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	cfg.Brokers = splitCSV(brokersStr)
	cfg.EtcdEndpoints = splitCSV(etcdStr)
	if cfg.InstanceID == "" {
		cfg.InstanceID = fmt.Sprintf("counter-shard-%d-main", cfg.ShardID)
	}
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = fmt.Sprintf("counter-shard-%d", cfg.ShardID)
	}
	if cfg.ElectionPath == "" {
		cfg.ElectionPath = fmt.Sprintf("/cex/counter/shard-%d/leader", cfg.ShardID)
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
