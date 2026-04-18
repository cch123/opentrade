// Command match runs the OpenTrade matching engine.
//
// Responsibilities:
//   - Read symbol → shard mapping from etcd (ADR-0009 / ADR-0030) and
//     dynamically add or remove per-symbol workers as operators rewrite the
//     config. Legacy --symbols flag is retained as a fallback when --etcd is
//     empty (dev one-liners and tests).
//   - For each active symbol: restore from the latest on-disk snapshot,
//     start a SymbolWorker, register with the journal Dispatcher.
//   - Consume order-event from Kafka, dispatch to per-symbol workers.
//   - Publish sequencer outputs to trade-event.
//   - Periodically snapshot every live worker; write a final snapshot when
//     a symbol is removed (migration runbook, see ADR-0030).
//
// HA (MVP-12, ADR-0031): with --ha-mode=auto Match competes for the shard's
// leader key in etcd. The elected primary runs the whole stack above;
// losers sit idle. This is cold-standby — Match does not ship a live
// tailing backup in MVP-12. Kafka transactional fencing for Match is
// deferred (producer stays idempotent); operators rely on etcd lease +
// primary self-exit, with the known split-brain window documented in
// ADR-0031.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/match/internal/engine"
	"github.com/xargin/opentrade/match/internal/journal"
	"github.com/xargin/opentrade/match/internal/registry"
	"github.com/xargin/opentrade/match/internal/sequencer"
	"github.com/xargin/opentrade/match/internal/snapshot"
	"github.com/xargin/opentrade/pkg/election"
	"github.com/xargin/opentrade/pkg/etcdcfg"
	"github.com/xargin/opentrade/pkg/logx"
)

type Config struct {
	InstanceID string
	ShardID    string

	Brokers          []string
	OrderTopic       string
	TradeTopic       string
	ConsumerGroup    string
	SnapshotDir      string
	SnapshotInterval time.Duration

	EtcdEndpoints   []string
	EtcdPrefix      string
	EtcdDialTimeout time.Duration

	Symbols []string

	// HA (MVP-12).
	HAMode          string
	ElectionPath    string
	LeaseTTL        int
	CampaignBackoff time.Duration

	Env      string
	LogLevel string
}

func main() {
	cfg := parseFlags()

	logger, err := logx.New(logx.Config{Service: "match", Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	logx.SetGlobal(logger)

	if err := cfg.validate(); err != nil {
		logger.Fatal("invalid config", zap.Error(err))
	}

	logger.Info("match starting",
		zap.String("instance", cfg.InstanceID),
		zap.String("shard_id", cfg.ShardID),
		zap.Strings("brokers", cfg.Brokers),
		zap.Strings("etcd", cfg.EtcdEndpoints),
		zap.Strings("static_symbols", cfg.Symbols),
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

// runElectionLoop campaigns for leadership then runs the primary body for
// each leadership cycle. Exits when rootCtx is cancelled.
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
		logger.Info("match campaigning for leadership",
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
		logger.Info("match became primary", zap.String("instance", cfg.InstanceID))

		primaryCtx, cancelPrimary := context.WithCancel(rootCtx)
		watchDone := make(chan struct{})
		go func() {
			defer close(watchDone)
			select {
			case <-elec.LostCh():
				logger.Warn("match lost leadership — demoting")
				cancelPrimary()
			case <-primaryCtx.Done():
			}
		}()

		runPrimary(primaryCtx, cfg, logger)
		cancelPrimary()
		<-watchDone

		if rootCtx.Err() == nil {
			logger.Info("match demoted; re-campaigning")
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

// runPrimary brings up the whole matching pipeline and blocks until ctx is
// done. Invoked directly in HA-disabled mode and once per leadership cycle
// in auto mode.
func runPrimary(ctx context.Context, cfg Config, logger *zap.Logger) {
	// --- Shared pipeline --------------------------------------------------

	dispatcher := journal.NewDispatcher()
	outbox := make(chan *sequencer.Output, 4096)

	workersCtx, cancelWorkers := context.WithCancel(context.Background())
	defer cancelWorkers()

	// Producer comes up before the registry so the registry's Snapshot
	// callback (invoked from RemoveSymbol) can call producer.FlushAndWait
	// before capturing state (ADR-0048 output flush barrier).
	//
	// HA auto → use a stable TransactionalID so the shard's Kafka producer
	// epoch fences any older primary still alive under the same id
	// (ADR-0031 §Match fencing). Disabled mode keeps the legacy idempotent
	// producer for dev / single-node tests.
	txnID := ""
	if cfg.HAMode == "auto" {
		txnID = cfg.InstanceID
	}
	producer, err := journal.NewTradeProducer(journal.ProducerConfig{
		Brokers:         cfg.Brokers,
		ClientID:        cfg.InstanceID,
		ProducerID:      cfg.InstanceID,
		Topic:           cfg.TradeTopic,
		TransactionalID: txnID,
	}, logger)
	if err != nil {
		logger.Error("trade producer", zap.Error(err))
		return
	}
	defer producer.Close()

	pumpCtx, cancelPump := context.WithCancel(context.Background())
	defer cancelPump()
	var pumpWG sync.WaitGroup
	pumpWG.Add(1)
	go func() {
		defer pumpWG.Done()
		producer.Pump(pumpCtx, outbox)
	}()

	reg, err := registry.New(workersCtx, registry.Config{
		Dispatcher: dispatcher,
		Factory: func(symbol string) *sequencer.SymbolWorker {
			return sequencer.NewSymbolWorker(sequencer.Config{
				Symbol:  symbol,
				Inbox:   2048,
				STPMode: engine.STPNone,
			}, outbox)
		},
		Restore: func(w *sequencer.SymbolWorker) error {
			return tryRestoreSnapshot(w, cfg, logger)
		},
		Snapshot: func(w *sequencer.SymbolWorker) {
			// RemoveSymbol path: the worker goroutine is already stopped,
			// so Pump may have been quiet for a moment — FlushAndWait
			// still forces any trailing transaction to commit before we
			// capture. Use a longer timeout because shutdown may coincide
			// with broker slowdowns.
			sctx, cancel := context.WithTimeout(context.Background(), shutdownFlushTimeout)
			defer cancel()
			if err := writeSnapshot(sctx, w, producer, cfg, time.Now().UnixMilli()); err != nil {
				logger.Error("final snapshot",
					zap.String("symbol", w.Symbol()), zap.Error(err))
			}
		},
		Logger: logger,
	})
	if err != nil {
		logger.Error("registry", zap.Error(err))
		return
	}

	// --- Symbol ownership: etcd or static flag ----------------------------

	var (
		etcdSrc     *etcdcfg.EtcdSource
		watchCancel context.CancelFunc
		watchWG     sync.WaitGroup
	)
	if len(cfg.EtcdEndpoints) > 0 {
		etcdSrc, err = etcdcfg.NewEtcdSource(etcdcfg.EtcdConfig{
			Endpoints:   cfg.EtcdEndpoints,
			DialTimeout: cfg.EtcdDialTimeout,
			Prefix:      cfg.EtcdPrefix,
		})
		if err != nil {
			logger.Error("etcd source", zap.Error(err))
			return
		}
		defer func() { _ = etcdSrc.Close() }()

		listCtx, listCancel := context.WithTimeout(ctx, 10*time.Second)
		snap, rev, err := etcdSrc.List(listCtx)
		listCancel()
		if err != nil {
			logger.Error("etcd list", zap.Error(err))
			return
		}
		for sym, sc := range snap {
			if !sc.Owned(cfg.ShardID) {
				continue
			}
			if err := reg.AddSymbol(sym); err != nil {
				logger.Error("add symbol at startup",
					zap.String("symbol", sym), zap.Error(err))
			}
		}

		watchCtx, cancel := context.WithCancel(ctx)
		watchCancel = cancel
		watchCh, err := etcdSrc.Watch(watchCtx, rev+1)
		if err != nil {
			logger.Error("etcd watch", zap.Error(err))
			cancel()
			return
		}
		watchWG.Add(1)
		go func() {
			defer watchWG.Done()
			applyWatch(watchCtx, watchCh, reg, cfg.ShardID, logger)
		}()
	} else {
		for _, s := range cfg.Symbols {
			if err := reg.AddSymbol(s); err != nil {
				logger.Error("add static symbol",
					zap.String("symbol", s), zap.Error(err))
			}
		}
	}

	// --- Consumer ---------------------------------------------------------

	// Merge per-symbol snapshot offsets to the consumer's per-partition
	// seek map. Partitions shared by multiple symbols rewind to the
	// slowest owner so no symbol misses events after seek (ADR-0048 §4).
	initialOffsets := mergeRestoredOffsets(reg)
	if len(initialOffsets) > 0 {
		logger.Info("restoring consumer offsets from snapshots",
			zap.Int("partitions", len(initialOffsets)))
	}

	consumer, err := journal.NewOrderConsumer(journal.ConsumerConfig{
		Brokers:        cfg.Brokers,
		ClientID:       cfg.InstanceID,
		GroupID:        cfg.ConsumerGroup,
		Topic:          cfg.OrderTopic,
		InitialOffsets: initialOffsets,
	}, dispatcher, logger)
	if err != nil {
		logger.Error("order consumer", zap.Error(err))
		return
	}
	defer consumer.Close()

	var consumerWG sync.WaitGroup
	consumerWG.Add(1)
	go func() {
		defer consumerWG.Done()
		if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("consumer exited", zap.Error(err))
		}
	}()

	// --- Periodic snapshot ------------------------------------------------

	snapCtx, cancelSnap := context.WithCancel(context.Background())
	defer cancelSnap()
	go periodicSnapshot(snapCtx, reg, producer, cfg, logger)

	// --- Wait for shutdown signal -----------------------------------------

	<-ctx.Done()
	logger.Info("primary shutting down")

	if watchCancel != nil {
		watchCancel()
	}
	watchWG.Wait()

	consumer.Close()
	consumerWG.Wait()

	// RemoveSymbol drains workers and writes a final snapshot. Close() does
	// the same for every remaining symbol.
	reg.Close()
	close(outbox)

	pumpWG.Wait()
	cancelSnap()

	logger.Info("primary stopped")
}

// applyWatch consumes etcd events and tells the registry what to do.
// Ownership rules (ADR-0030):
//
//	Put    && Owned(shard) && !active   → AddSymbol
//	Put    && !Owned && active          → RemoveSymbol (trading: false or shard moved)
//	Delete && active                    → RemoveSymbol
func applyWatch(ctx context.Context, watchCh <-chan etcdcfg.Event, reg *registry.Registry, shardID string, logger *zap.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-watchCh:
			if !ok {
				logger.Warn("etcd watch channel closed")
				return
			}
			switch ev.Type {
			case etcdcfg.EventPut:
				switch {
				case ev.Config.Owned(shardID) && !reg.HasSymbol(ev.Symbol):
					if err := reg.AddSymbol(ev.Symbol); err != nil {
						logger.Error("watch add symbol",
							zap.String("symbol", ev.Symbol), zap.Error(err))
					}
				case !ev.Config.Owned(shardID) && reg.HasSymbol(ev.Symbol):
					if err := reg.RemoveSymbol(ev.Symbol); err != nil {
						logger.Error("watch remove symbol",
							zap.String("symbol", ev.Symbol), zap.Error(err))
					}
				}
			case etcdcfg.EventDelete:
				if reg.HasSymbol(ev.Symbol) {
					if err := reg.RemoveSymbol(ev.Symbol); err != nil {
						logger.Error("watch delete symbol",
							zap.String("symbol", ev.Symbol), zap.Error(err))
					}
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

func parseFlags() Config {
	cfg := Config{
		OrderTopic:       "order-event",
		TradeTopic:       "trade-event",
		SnapshotDir:      "./data/match",
		SnapshotInterval: 60 * time.Second,
		EtcdPrefix:       etcdcfg.DefaultPrefix,
		EtcdDialTimeout:  5 * time.Second,
		HAMode:           "disabled",
		LeaseTTL:         10,
		CampaignBackoff:  2 * time.Second,
		Env:              "dev",
		LogLevel:         "info",
	}
	var (
		brokersStr string
		etcdStr    string
		symbolsStr string
	)
	flag.StringVar(&cfg.InstanceID, "instance-id", "match-0", "instance id (client id / consumer group suffix / producer id)")
	flag.StringVar(&cfg.ShardID, "shard-id", "", "shard id (match etcd SymbolConfig.Shard); defaults to --instance-id")
	flag.StringVar(&brokersStr, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flag.StringVar(&etcdStr, "etcd", "", "comma-separated etcd endpoints; used for symbol config and HA election")
	flag.StringVar(&cfg.EtcdPrefix, "etcd-prefix", cfg.EtcdPrefix, "etcd key prefix for symbol configs")
	flag.DurationVar(&cfg.EtcdDialTimeout, "etcd-dial-timeout", cfg.EtcdDialTimeout, "etcd dial timeout")
	flag.StringVar(&symbolsStr, "symbols", "", "comma-separated static symbol list (used when --etcd is empty)")
	flag.StringVar(&cfg.OrderTopic, "order-topic", cfg.OrderTopic, "order-event topic name")
	flag.StringVar(&cfg.TradeTopic, "trade-topic", cfg.TradeTopic, "trade-event topic name")
	flag.StringVar(&cfg.ConsumerGroup, "group", "", "Kafka consumer group (default match-{instance-id})")
	flag.StringVar(&cfg.SnapshotDir, "snapshot-dir", cfg.SnapshotDir, "local directory for snapshots")
	flag.DurationVar(&cfg.SnapshotInterval, "snapshot-interval", cfg.SnapshotInterval, "how often to snapshot each symbol")
	flag.StringVar(&cfg.HAMode, "ha-mode", cfg.HAMode, "ha mode: disabled | auto (etcd leader election, ADR-0031)")
	flag.StringVar(&cfg.ElectionPath, "election-path", "", "etcd election key (default /cex/match/shard-<id>/leader)")
	flag.IntVar(&cfg.LeaseTTL, "lease-ttl", cfg.LeaseTTL, "etcd session TTL seconds")
	flag.DurationVar(&cfg.CampaignBackoff, "campaign-backoff", cfg.CampaignBackoff, "wait between failed Campaigns")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	cfg.Brokers = splitCSV(brokersStr)
	cfg.EtcdEndpoints = splitCSV(etcdStr)
	cfg.Symbols = splitCSV(symbolsStr)
	if cfg.ShardID == "" {
		cfg.ShardID = cfg.InstanceID
	}
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "match-" + cfg.InstanceID
	}
	if cfg.ElectionPath == "" {
		cfg.ElectionPath = "/cex/match/shard-" + sanitize(cfg.ShardID) + "/leader"
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
	if len(c.EtcdEndpoints) == 0 && len(c.Symbols) == 0 {
		return fmt.Errorf("either --etcd or --symbols is required")
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

func snapshotPath(cfg Config, symbol string) string {
	return filepath.Join(cfg.SnapshotDir, fmt.Sprintf("%s.json", sanitize(symbol)))
}

func sanitize(s string) string {
	return strings.NewReplacer("/", "_", ":", "_").Replace(s)
}

func tryRestoreSnapshot(w *sequencer.SymbolWorker, cfg Config, logger *zap.Logger) error {
	path := snapshotPath(cfg, w.Symbol())
	snap, err := snapshot.Load(path)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Info("no snapshot found, starting fresh", zap.String("symbol", w.Symbol()))
			return nil
		}
		return fmt.Errorf("load %s: %w", path, err)
	}
	if err := snapshot.Restore(w, snap); err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	logger.Info("restored from snapshot",
		zap.String("symbol", w.Symbol()),
		zap.Uint64("seq_id", snap.SeqID),
		zap.Int("orders", len(snap.Orders)),
		zap.Int("partitions", len(snap.Offsets)))
	return nil
}

// snapshotTimeout caps how long a single Capture is allowed to block the
// Pump flush barrier. 3s / 10s are copied from the ADR-0048 failure-scenario
// table so tuning them is a one-place change.
const (
	snapshotFlushTimeout = 3 * time.Second
	shutdownFlushTimeout = 10 * time.Second
)

// writeSnapshot performs one ADR-0048 Capture cycle for w. It flushes the
// producer's outbox first so the Kafka commit state is consistent with the
// offsets about to be written, then reads the worker state under its lock.
//
// On flush failure the snapshot is skipped (not written with stale offsets)
// and the caller sees the error; next periodic tick will retry.
func writeSnapshot(ctx context.Context, w *sequencer.SymbolWorker, producer *journal.TradeProducer, cfg Config, ts int64) error {
	flushCtx, cancel := context.WithTimeout(ctx, snapshotFlushTimeout)
	defer cancel()
	if err := producer.FlushAndWait(flushCtx); err != nil {
		return fmt.Errorf("flush before snapshot: %w", err)
	}
	snap := snapshot.Capture(w, ts)
	return snapshot.Save(snapshotPath(cfg, w.Symbol()), snap)
}

func periodicSnapshot(ctx context.Context, reg *registry.Registry, producer *journal.TradeProducer, cfg Config, logger *zap.Logger) {
	if cfg.SnapshotInterval <= 0 {
		return
	}
	ticker := time.NewTicker(cfg.SnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			for _, w := range reg.Workers() {
				if err := writeSnapshot(ctx, w, producer, cfg, t.UnixMilli()); err != nil {
					logger.Error("periodic snapshot",
						zap.String("symbol", w.Symbol()), zap.Error(err))
				}
			}
		}
	}
}

// mergeRestoredOffsets collects per-partition offsets from every active
// worker, taking the MIN per partition. A partition shared by multiple
// symbols (shared-topic deployments, ADR-0048 §4) must be rewound to the
// slowest consumer's position so none of them miss events after seek.
func mergeRestoredOffsets(reg *registry.Registry) map[int32]int64 {
	merged := make(map[int32]int64)
	for _, w := range reg.Workers() {
		for p, off := range w.Offsets() {
			if existing, ok := merged[p]; !ok || off < existing {
				merged[p] = off
			}
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}
