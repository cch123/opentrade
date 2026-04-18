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
	"github.com/xargin/opentrade/pkg/etcdcfg"
	"github.com/xargin/opentrade/pkg/logx"
)

type Config struct {
	InstanceID string // e.g. "match-0"
	ShardID    string // matches SymbolConfig.Shard in etcd; defaults to InstanceID

	Brokers          []string
	OrderTopic       string
	TradeTopic       string
	ConsumerGroup    string
	SnapshotDir      string
	SnapshotInterval time.Duration

	// etcd-driven symbol ownership (preferred).
	EtcdEndpoints   []string
	EtcdPrefix      string
	EtcdDialTimeout time.Duration

	// Static symbol list (fallback when --etcd is empty).
	Symbols []string

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
		zap.String("snapshot_dir", cfg.SnapshotDir))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- Shared pipeline --------------------------------------------------

	dispatcher := journal.NewDispatcher()
	outbox := make(chan *sequencer.Output, 4096)

	workersCtx, cancelWorkers := context.WithCancel(context.Background())
	defer cancelWorkers()

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
			if err := writeSnapshot(w, cfg, time.Now().UnixMilli()); err != nil {
				logger.Error("final snapshot",
					zap.String("symbol", w.Symbol()), zap.Error(err))
			}
		},
		Logger: logger,
	})
	if err != nil {
		logger.Fatal("registry", zap.Error(err))
	}

	// --- Producer pump ----------------------------------------------------

	producer, err := journal.NewTradeProducer(journal.ProducerConfig{
		Brokers:    cfg.Brokers,
		ClientID:   cfg.InstanceID,
		ProducerID: cfg.InstanceID,
		Topic:      cfg.TradeTopic,
	}, logger)
	if err != nil {
		logger.Fatal("trade producer", zap.Error(err))
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
			logger.Fatal("etcd source", zap.Error(err))
		}
		defer func() { _ = etcdSrc.Close() }()

		listCtx, listCancel := context.WithTimeout(rootCtx, 10*time.Second)
		snap, rev, err := etcdSrc.List(listCtx)
		listCancel()
		if err != nil {
			logger.Fatal("etcd list", zap.Error(err))
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

		watchCtx, cancel := context.WithCancel(rootCtx)
		watchCancel = cancel
		watchCh, err := etcdSrc.Watch(watchCtx, rev+1)
		if err != nil {
			logger.Fatal("etcd watch", zap.Error(err))
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

	consumer, err := journal.NewOrderConsumer(journal.ConsumerConfig{
		Brokers:  cfg.Brokers,
		ClientID: cfg.InstanceID,
		GroupID:  cfg.ConsumerGroup,
		Topic:    cfg.OrderTopic,
	}, dispatcher, logger)
	if err != nil {
		logger.Fatal("order consumer", zap.Error(err))
	}
	defer consumer.Close()

	var consumerWG sync.WaitGroup
	consumerWG.Add(1)
	go func() {
		defer consumerWG.Done()
		if err := consumer.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("consumer exited", zap.Error(err))
		}
	}()

	// --- Periodic snapshot ------------------------------------------------

	snapCtx, cancelSnap := context.WithCancel(context.Background())
	defer cancelSnap()
	go periodicSnapshot(snapCtx, reg, cfg, logger)

	// --- Wait for shutdown signal -----------------------------------------

	<-rootCtx.Done()
	logger.Info("shutdown initiated")

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

	logger.Info("match shutdown complete")
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
	flag.StringVar(&etcdStr, "etcd", "", "comma-separated etcd endpoints; empty falls back to --symbols")
	flag.StringVar(&cfg.EtcdPrefix, "etcd-prefix", cfg.EtcdPrefix, "etcd key prefix for symbol configs")
	flag.DurationVar(&cfg.EtcdDialTimeout, "etcd-dial-timeout", cfg.EtcdDialTimeout, "etcd dial timeout")
	flag.StringVar(&symbolsStr, "symbols", "", "comma-separated static symbol list (used when --etcd is empty)")
	flag.StringVar(&cfg.OrderTopic, "order-topic", cfg.OrderTopic, "order-event topic name")
	flag.StringVar(&cfg.TradeTopic, "trade-topic", cfg.TradeTopic, "trade-event topic name")
	flag.StringVar(&cfg.ConsumerGroup, "group", "", "Kafka consumer group (default match-{instance-id})")
	flag.StringVar(&cfg.SnapshotDir, "snapshot-dir", cfg.SnapshotDir, "local directory for snapshots")
	flag.DurationVar(&cfg.SnapshotInterval, "snapshot-interval", cfg.SnapshotInterval, "how often to snapshot each symbol")
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
		zap.Int("orders", len(snap.Orders)))
	return nil
}

func writeSnapshot(w *sequencer.SymbolWorker, cfg Config, ts int64) error {
	snap := snapshot.Capture(w, nil, ts)
	return snapshot.Save(snapshotPath(cfg, w.Symbol()), snap)
}

func periodicSnapshot(ctx context.Context, reg *registry.Registry, cfg Config, logger *zap.Logger) {
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
				if err := writeSnapshot(w, cfg, t.UnixMilli()); err != nil {
					logger.Error("periodic snapshot",
						zap.String("symbol", w.Symbol()), zap.Error(err))
				}
			}
		}
	}
}
