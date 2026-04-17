// Command match runs the OpenTrade matching engine.
//
// Responsibilities (MVP-1):
//   - For each configured symbol, spin up a SymbolWorker (per-symbol goroutine).
//   - Attempt to restore each worker from its latest on-disk snapshot.
//   - Consume order-event Kafka topic and dispatch to workers by symbol.
//   - Publish sequencer outputs to trade-event topic.
//   - Periodically snapshot each worker to local disk (S3 in later MVPs).
//
// Configuration is supplied via flags and environment variables (OPENTRADE_*).
package main

import (
	"context"
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
	"github.com/xargin/opentrade/match/internal/sequencer"
	"github.com/xargin/opentrade/match/internal/snapshot"
	"github.com/xargin/opentrade/pkg/logx"
)

type Config struct {
	InstanceID       string        // "match-0"
	Brokers          []string      // Kafka broker list
	Symbols          []string      // symbols owned by this instance
	OrderTopic       string        // default "order-event"
	TradeTopic       string        // default "trade-event"
	ConsumerGroup    string        // default "match-{InstanceID}"
	SnapshotDir      string        // ./data/match by default
	SnapshotInterval time.Duration // default 60s
	Env              string        // dev / prod
	LogLevel         string        // info / debug / warn / error
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
		zap.Strings("symbols", cfg.Symbols),
		zap.Strings("brokers", cfg.Brokers),
		zap.String("snapshot_dir", cfg.SnapshotDir))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- Build workers + outbox ---------------------------------------------

	dispatcher := journal.NewDispatcher()
	outbox := make(chan *sequencer.Output, 4096)

	workerCtx, cancelWorkers := context.WithCancel(context.Background())
	defer cancelWorkers()

	var workerWG sync.WaitGroup
	workers := make([]*sequencer.SymbolWorker, 0, len(cfg.Symbols))
	for _, sym := range cfg.Symbols {
		w := sequencer.NewSymbolWorker(sequencer.Config{
			Symbol:  sym,
			Inbox:   2048,
			STPMode: engine.STPNone,
		}, outbox)

		// Try restore from snapshot.
		if err := tryRestoreSnapshot(w, cfg, logger); err != nil {
			logger.Fatal("snapshot restore", zap.String("symbol", sym), zap.Error(err))
		}

		dispatcher.Register(w)
		workerWG.Add(1)
		go func(w *sequencer.SymbolWorker) {
			defer workerWG.Done()
			w.Run(workerCtx)
		}(w)
		workers = append(workers, w)
		logger.Info("worker started", zap.String("symbol", sym), zap.Uint64("seq_id", w.SeqID()))
	}

	// --- Producer pump -------------------------------------------------------

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

	// --- Consumer ------------------------------------------------------------

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
		if err := consumer.Run(rootCtx); err != nil && err != context.Canceled {
			logger.Error("consumer exited", zap.Error(err))
		}
	}()

	// --- Periodic snapshot ---------------------------------------------------

	snapCtx, cancelSnap := context.WithCancel(context.Background())
	defer cancelSnap()
	go periodicSnapshot(snapCtx, workers, cfg, logger)

	// --- Wait for shutdown signal --------------------------------------------

	<-rootCtx.Done()
	logger.Info("shutdown initiated")

	// Order: stop consumer → drain workers → stop pump → snapshot.
	consumer.Close()
	consumerWG.Wait()

	for _, w := range workers {
		w.Close()
	}
	workerWG.Wait()
	close(outbox)

	pumpWG.Wait()
	cancelSnap()

	// Final snapshot before exit.
	for _, w := range workers {
		if err := writeSnapshot(w, cfg, time.Now().UnixMilli()); err != nil {
			logger.Error("final snapshot", zap.String("symbol", w.Symbol()), zap.Error(err))
		}
	}

	logger.Info("match shutdown complete")
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
		Env:              "dev",
		LogLevel:         "info",
	}
	var (
		brokersStr string
		symbolsStr string
	)
	flag.StringVar(&cfg.InstanceID, "instance-id", "match-0", "instance id (used as client id / consumer group / producer id)")
	flag.StringVar(&brokersStr, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flag.StringVar(&symbolsStr, "symbols", "BTC-USDT", "comma-separated symbols owned by this instance")
	flag.StringVar(&cfg.OrderTopic, "order-topic", cfg.OrderTopic, "order-event topic name")
	flag.StringVar(&cfg.TradeTopic, "trade-topic", cfg.TradeTopic, "trade-event topic name")
	flag.StringVar(&cfg.ConsumerGroup, "group", "", "Kafka consumer group (default match-{instance-id})")
	flag.StringVar(&cfg.SnapshotDir, "snapshot-dir", cfg.SnapshotDir, "local directory for snapshots")
	flag.DurationVar(&cfg.SnapshotInterval, "snapshot-interval", cfg.SnapshotInterval, "how often to snapshot each symbol")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	cfg.Brokers = splitCSV(brokersStr)
	cfg.Symbols = splitCSV(symbolsStr)
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
	if len(c.Symbols) == 0 {
		return fmt.Errorf("at least one symbol required")
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
	// Replace characters not friendly to filesystems.
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

func periodicSnapshot(ctx context.Context, workers []*sequencer.SymbolWorker, cfg Config, logger *zap.Logger) {
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
			for _, w := range workers {
				if err := writeSnapshot(w, cfg, t.UnixMilli()); err != nil {
					logger.Error("periodic snapshot", zap.String("symbol", w.Symbol()), zap.Error(err))
				}
			}
		}
	}
}
