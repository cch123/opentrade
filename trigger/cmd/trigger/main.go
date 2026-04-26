// Command trigger runs the OpenTrade trigger-order service.
//
// MVP-14a scope (ADR-0040):
//   - Accepts STOP_LOSS / STOP_LOSS_LIMIT / TAKE_PROFIT / TAKE_PROFIT_LIMIT
//     via TriggerService gRPC.
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

	counterrpc "github.com/xargin/opentrade/api/gen/rpc/counter"
	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
	"github.com/xargin/opentrade/pkg/election"
	"github.com/xargin/opentrade/pkg/idgen"
	"github.com/xargin/opentrade/pkg/logx"
	snapshotpkg "github.com/xargin/opentrade/pkg/snapshot"
	"github.com/xargin/opentrade/trigger/engine"
	"github.com/xargin/opentrade/trigger/internal/consumer"
	"github.com/xargin/opentrade/trigger/internal/counterclient"
	"github.com/xargin/opentrade/trigger/internal/journal"
	"github.com/xargin/opentrade/trigger/internal/server"
	"github.com/xargin/opentrade/trigger/internal/service"
	"github.com/xargin/opentrade/trigger/internal/snapshot"
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
	SnapshotFormat       snapshotpkg.Format // ADR-0049
	TerminalHistoryLimit int
	// ExpirySweepInterval tunes how often the primary scans pending
	// triggers for expired ones (ADR-0043). 0 disables.
	ExpirySweepInterval time.Duration

	// JournalTopic (ADR-0047) names the trigger-event audit topic.
	// Empty disables journaling — useful for CI / local dev and for
	// backward compatibility with MVP-14 deployments.
	JournalTopic string

	// idgen shard id — snowflake layout (ADR: counter/cmd/counter uses
	// ShardID for this). For trigger we give it its own shard id
	// range by default to avoid id collisions with counter order ids
	// even though the namespaces are disjoint today.
	IDGenShard int

	// HA (MVP-14c / ADR-0042). "disabled" = single-process behaviour;
	// "auto" campaigns for leadership via etcd and only serves traffic as
	// primary. Backups sit idle and re-campaign when the primary resigns
	// / dies — same cold-standby shape as Counter/Match (ADR-0031).
	HAMode          string
	EtcdEndpoints   []string
	ElectionPath    string
	LeaseTTL        int
	CampaignBackoff time.Duration

	// DefaultMaxActiveTriggerOrders is the ADR-0054 fallback cap per
	// (user, symbol) when a SymbolConfig.MaxActiveTriggerOrders is
	// zero. Zero disables the cap (compat mode).
	DefaultMaxActiveTriggerOrders uint32

	// EtcdSymbolPrefix, when non-empty, starts a watcher that wires
	// etcd-backed SymbolConfig lookups into the engine so ADR-0054 caps
	// can be tuned per-symbol. Defaults to the shared prefix used by
	// match / counter (ADR-0030).
	EtcdSymbolPrefix string

	Env      string
	LogLevel string
}

func main() {
	cfg := parseFlags()

	logger, err := logx.New(logx.Config{Service: "trigger", Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	logx.SetGlobal(logger)

	if err := cfg.validate(); err != nil {
		logger.Fatal("invalid config", zap.Error(err))
	}
	logger.Info("trigger starting",
		zap.String("instance", cfg.InstanceID),
		zap.String("grpc", cfg.GRPCAddr),
		zap.Strings("brokers", cfg.Brokers),
		zap.Strings("counter_shards", cfg.CounterShards),
		zap.String("ha_mode", cfg.HAMode))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.HAMode == "disabled" {
		runPrimary(rootCtx, cfg, logger)
		return
	}
	runElectionLoop(rootCtx, cfg, logger)
}

// runElectionLoop campaigns for leadership and runs the primary stack for
// the duration of each leadership cycle. On lost leadership the primary
// stack tears down, we re-campaign. Identical cold-standby model to
// Counter/Match (ADR-0031); see ADR-0042 for why trigger gets HA.
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
		logger.Info("trigger campaigning for leadership",
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
		logger.Info("trigger became primary", zap.String("instance", cfg.InstanceID))

		primaryCtx, cancelPrimary := context.WithCancel(rootCtx)
		watchDone := make(chan struct{})
		go func() {
			defer close(watchDone)
			select {
			case <-elec.LostCh():
				logger.Warn("trigger lost leadership — demoting")
				cancelPrimary()
			case <-primaryCtx.Done():
			}
		}()

		runPrimary(primaryCtx, cfg, logger)
		cancelPrimary()
		<-watchDone

		if rootCtx.Err() == nil {
			logger.Info("trigger demoted; re-campaigning")
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

// runPrimary brings up the full trigger stack (gRPC server +
// market-data consumer + snapshot ticker) and blocks until ctx is done.
// Called directly in HA-disabled mode; called once per leadership cycle
// under --ha-mode=auto.
func runPrimary(ctx context.Context, cfg Config, logger *zap.Logger) {
	counterConns, counterClients, err := dialCounterShards(ctx, cfg.CounterShards)
	if err != nil {
		logger.Error("dial counter shards", zap.Error(err))
		return
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
		logger.Error("sharded counter", zap.Error(err))
		return
	}

	idg, err := idgen.NewGenerator(cfg.IDGenShard)
	if err != nil {
		logger.Error("idgen", zap.Error(err))
		return
	}

	eng := engine.New(engine.Config{
		TerminalHistoryLimit:          cfg.TerminalHistoryLimit,
		DefaultMaxActiveTriggerOrders: cfg.DefaultMaxActiveTriggerOrders,
		// SymbolLookup left nil for MVP — only the default cap applies.
		// Per-symbol override via etcd SymbolConfig is backlog (see
		// ADR-0054 backlog memory).
	}, idg, placer, placer, logger)

	var jProducer *journal.Producer
	if cfg.JournalTopic != "" {
		p, err := journal.New(journal.Config{
			Brokers:    cfg.Brokers,
			Topic:      cfg.JournalTopic,
			ProducerID: "trigger-" + cfg.InstanceID,
			Logger:     logger,
		})
		if err != nil {
			logger.Error("journal producer init", zap.Error(err))
			return
		}
		jProducer = p
		eng.SetJournal(p)
		logger.Info("trigger journal enabled", zap.String("topic", cfg.JournalTopic))
	} else {
		logger.Info("trigger journal disabled (empty --journal-topic)")
	}

	var snapStore snapshotpkg.BlobStore
	if cfg.SnapshotDir != "" {
		snapStore = snapshotpkg.NewFSBlobStore(cfg.SnapshotDir)
	}

	initialOffsets, err := tryRestoreSnapshot(ctx, snapStore, cfg, eng, logger)
	if err != nil {
		logger.Error("snapshot restore", zap.Error(err))
		return
	}

	mdCons, err := consumer.New(consumer.Config{
		Brokers:        cfg.Brokers,
		ClientID:       cfg.InstanceID,
		GroupID:        cfg.ConsumerGroup,
		Topic:          cfg.MarketTopic,
		InitialOffsets: initialOffsets,
	}, eng.HandleRecord, logger)
	if err != nil {
		logger.Error("market-data consumer", zap.Error(err))
		return
	}
	defer mdCons.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := mdCons.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("consumer exited", zap.Error(err))
		}
	}()

	snapCtx, cancelSnap := context.WithCancel(context.Background())
	defer cancelSnap()
	wg.Add(1)
	go func() {
		defer wg.Done()
		runSnapshotTicker(snapCtx, snapStore, cfg, eng, logger)
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		runExpirySweeper(ctx, cfg, eng, logger)
	}()

	svc := service.New(eng)
	grpcSrv := grpc.NewServer()
	condrpc.RegisterTriggerServiceServer(grpcSrv, server.New(svc, nil))

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Error("listen", zap.Error(err))
		return
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("gRPC listening", zap.String("addr", cfg.GRPCAddr))
		if err := grpcSrv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error("grpc serve", zap.Error(err))
		}
	}()

	<-ctx.Done()
	logger.Info("primary shutting down")
	grpcSrv.GracefulStop()
	mdCons.Close()
	cancelSnap()
	wg.Wait()
	finalCtx, finalCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := writeSnapshot(finalCtx, snapStore, cfg, eng); err != nil {
		logger.Error("final snapshot", zap.Error(err))
	} else if snapStore != nil {
		logger.Info("final snapshot written", zap.String("path", snapshotPath(cfg)))
	}
	finalCancel()
	if jProducer != nil {
		drainCtx, drainCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = jProducer.Close(drainCtx)
		drainCancel()
	}
	logger.Info("primary stopped")
}

// ---------------------------------------------------------------------------
// Snapshot helpers
// ---------------------------------------------------------------------------

// snapshotKey is the BlobStore key stem (no format extension); Save
// appends `.pb` / `.json` per cfg.SnapshotFormat (ADR-0049). Used as the
// log path too — combined with cfg.SnapshotDir it locates the file on
// disk for ops.
func snapshotKey(cfg Config) string { return cfg.InstanceID }

// snapshotPath returns the human-readable on-disk path for log lines.
func snapshotPath(cfg Config) string {
	return filepath.Join(cfg.SnapshotDir, snapshotKey(cfg))
}

func tryRestoreSnapshot(ctx context.Context, store snapshotpkg.BlobStore, cfg Config, eng *engine.Engine, logger *zap.Logger) (map[int32]int64, error) {
	if store == nil {
		logger.Info("snapshot disabled (empty --snapshot-dir); cold start")
		return nil, nil
	}
	key := snapshotKey(cfg)
	path := snapshotPath(cfg)
	snap, err := snapshot.Load(ctx, store, key)
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
	logger.Info("trigger state restored",
		zap.String("path", path),
		zap.Int64("taken_at_ms", snap.TakenAtMs),
		zap.Int("pending", len(snap.Pending)),
		zap.Int("terminals", len(snap.Terminals)),
		zap.Int("partitions", len(snap.Offsets)))
	return snap.Offsets, nil
}

func writeSnapshot(ctx context.Context, store snapshotpkg.BlobStore, cfg Config, eng *engine.Engine) error {
	if store == nil {
		return nil
	}
	snap := snapshot.Capture(eng)
	snap.TakenAtMs = time.Now().UnixMilli()
	return snapshot.Save(ctx, store, snapshotKey(cfg), snap, cfg.SnapshotFormat)
}

// runExpirySweeper sweeps PENDING triggers whose ExpiresAtMs has
// passed, flipping them to EXPIRED and releasing their reservations.
// Interval 0 disables entirely (matches --expiry-sweep=0; useful for
// deterministic tests that drive SweepExpired manually).
func runExpirySweeper(ctx context.Context, cfg Config, eng *engine.Engine, logger *zap.Logger) {
	if cfg.ExpirySweepInterval <= 0 {
		return
	}
	ticker := time.NewTicker(cfg.ExpirySweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n := eng.SweepExpired(ctx); n > 0 && logger != nil {
				logger.Info("expiry sweep", zap.Int("expired", n))
			}
		}
	}
}

func runSnapshotTicker(ctx context.Context, store snapshotpkg.BlobStore, cfg Config, eng *engine.Engine, logger *zap.Logger) {
	if store == nil || cfg.SnapshotInterval <= 0 {
		return
	}
	ticker := time.NewTicker(cfg.SnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := writeSnapshot(ctx, store, cfg, eng); err != nil {
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
		InstanceID:                    "trigger-0",
		GRPCAddr:                      ":8082",
		MarketTopic:                   "market-data",
		SnapshotDir:                   "./data/trigger",
		SnapshotInterval:              30 * time.Second,
		SnapshotFormat:                snapshotpkg.FormatProto, // ADR-0049
		TerminalHistoryLimit:          1000,
		ExpirySweepInterval:           5 * time.Second,
		IDGenShard:                    900, // deliberately out of counter's 0..99 range
		HAMode:                        "disabled",
		LeaseTTL:                      10,
		CampaignBackoff:               2 * time.Second,
		DefaultMaxActiveTriggerOrders: 10, // ADR-0054
		EtcdSymbolPrefix:              "",
		Env:                           "dev",
		LogLevel:                      "info",
	}
	var brokersStr, shardsStr, etcdStr, snapshotFormatStr string
	flag.StringVar(&cfg.InstanceID, "instance-id", cfg.InstanceID, "instance id (grpc client id / default group suffix)")
	flag.StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "gRPC listen address")
	flag.StringVar(&brokersStr, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flag.StringVar(&cfg.MarketTopic, "market-topic", cfg.MarketTopic, "market-data topic name")
	flag.StringVar(&cfg.JournalTopic, "journal-topic", cfg.JournalTopic, "trigger-event audit topic name (empty disables journaling; ADR-0047)")
	flag.StringVar(&cfg.ConsumerGroup, "group", "", "Kafka consumer group (default trigger-{instance-id})")
	flag.StringVar(&shardsStr, "counter-shards", "localhost:8081",
		"comma-separated Counter gRPC endpoints, in shard-id order (shard 0 first)")
	flag.StringVar(&cfg.SnapshotDir, "snapshot-dir", cfg.SnapshotDir, "directory for engine-state snapshots (empty disables)")
	flag.DurationVar(&cfg.SnapshotInterval, "snapshot-interval", cfg.SnapshotInterval, "snapshot cadence; 0 disables the ticker (still writes on shutdown)")
	flag.StringVar(&snapshotFormatStr, "snapshot-format", cfg.SnapshotFormat.String(), "snapshot on-disk encoding: proto (default) | json (debug). ADR-0049. Env OPENTRADE_SNAPSHOT_FORMAT overrides.")
	flag.IntVar(&cfg.TerminalHistoryLimit, "terminal-history", cfg.TerminalHistoryLimit, "max retained terminal records per engine for ListTriggers(include_inactive)")
	flag.DurationVar(&cfg.ExpirySweepInterval, "expiry-sweep", cfg.ExpirySweepInterval, "cadence for sweeping expired PENDING triggers (ADR-0043); 0 disables")
	flag.IntVar(&cfg.IDGenShard, "idgen-shard", cfg.IDGenShard, "snowflake shard id for trigger ids (avoid collisions with counter)")
	flag.StringVar(&cfg.HAMode, "ha-mode", cfg.HAMode, "ha mode: disabled | auto (etcd leader election, ADR-0042)")
	flag.StringVar(&etcdStr, "etcd", "", "comma-separated etcd endpoints (required when --ha-mode=auto)")
	flag.StringVar(&cfg.ElectionPath, "election-path", "", "etcd election key (default /cex/trigger/leader)")
	flag.IntVar(&cfg.LeaseTTL, "lease-ttl", cfg.LeaseTTL, "etcd session TTL seconds")
	flag.DurationVar(&cfg.CampaignBackoff, "campaign-backoff", cfg.CampaignBackoff, "wait between failed Campaigns")
	var defaultMaxActiveCond uint
	flag.UintVar(&defaultMaxActiveCond, "default-max-active-trigger-orders", uint(cfg.DefaultMaxActiveTriggerOrders), "ADR-0054 fallback per-(user, symbol) pending trigger cap when SymbolConfig.MaxActiveTriggerOrders is zero (0 disables; default 10)")
	flag.StringVar(&cfg.EtcdSymbolPrefix, "etcd-symbol-prefix", cfg.EtcdSymbolPrefix, "etcd prefix for SymbolConfig lookups (ADR-0054 per-symbol cap override); empty disables lookup")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	cfg.Brokers = splitCSV(brokersStr)
	cfg.CounterShards = splitCSV(shardsStr)
	cfg.EtcdEndpoints = splitCSV(etcdStr)
	cfg.DefaultMaxActiveTriggerOrders = uint32(defaultMaxActiveCond)
	// --snapshot-format + OPENTRADE_SNAPSHOT_FORMAT env override (ADR-0049).
	if envFmt := os.Getenv("OPENTRADE_SNAPSHOT_FORMAT"); envFmt != "" {
		snapshotFormatStr = envFmt
	}
	if snapshotFormatStr != "" {
		f, err := snapshotpkg.ParseFormat(snapshotFormatStr)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		cfg.SnapshotFormat = f
	}
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "trigger-" + cfg.InstanceID
	}
	if cfg.ElectionPath == "" {
		cfg.ElectionPath = "/cex/trigger/leader"
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
