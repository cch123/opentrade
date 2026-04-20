// Command quote runs the OpenTrade market-data (quote) service.
//
// MVP-6: consumes trade-event from the beginning on every start (ADR-0025),
// projects Trade / OrderAccepted / OrderCancelled / OrderExpired into
// PublicTrade + Kline + Depth messages, and publishes them to the
// market-data Kafka topic. A periodic ticker emits DepthSnapshot for every
// active symbol.
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

	"github.com/xargin/opentrade/pkg/logx"
	"github.com/xargin/opentrade/quote/internal/consumer"
	"github.com/xargin/opentrade/quote/internal/engine"
	"github.com/xargin/opentrade/quote/internal/kline"
	"github.com/xargin/opentrade/quote/internal/producer"
	"github.com/xargin/opentrade/quote/internal/snapshot"
)

type Config struct {
	InstanceID       string
	Brokers          []string
	TradeTopic       string
	MarketDataTopic  string
	ConsumerGroup    string
	SnapshotInterval time.Duration

	// Engine-state snapshot (ADR-0036). Separate cadence from the public
	// DepthSnapshot ticker: we want disk writes to be less frequent than
	// the wire-level snapshots Push consumers rely on.
	StateSnapshotDir      string
	StateSnapshotInterval time.Duration
	StateSnapshotFormat   snapshot.Format // ADR-0049

	Env      string
	LogLevel string
}

func main() {
	cfg := parseFlags()

	logger, err := logx.New(logx.Config{Service: "quote", Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	logx.SetGlobal(logger)

	if err := cfg.validate(); err != nil {
		logger.Fatal("invalid config", zap.Error(err))
	}

	logger.Info("quote starting",
		zap.String("instance", cfg.InstanceID),
		zap.Strings("brokers", cfg.Brokers),
		zap.String("trade_topic", cfg.TradeTopic),
		zap.String("market_topic", cfg.MarketDataTopic),
		zap.Duration("snapshot_interval", cfg.SnapshotInterval))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eng := engine.New(engine.Config{
		ProducerID: cfg.InstanceID,
		Intervals:  kline.DefaultIntervals(),
	}, logger)

	// Restore engine state if a snapshot exists (ADR-0036). Absence is not
	// an error — cold start rescans trade-event from earliest.
	initialOffsets, err := tryRestore(cfg, eng, logger)
	if err != nil {
		logger.Fatal("snapshot restore", zap.Error(err))
	}

	prod, err := producer.New(producer.Config{
		Brokers:  cfg.Brokers,
		ClientID: cfg.InstanceID,
		Topic:    cfg.MarketDataTopic,
	}, logger)
	if err != nil {
		logger.Fatal("producer init", zap.Error(err))
	}
	defer prod.Close()

	cons, err := consumer.New(consumer.Config{
		Brokers:        cfg.Brokers,
		ClientID:       cfg.InstanceID,
		GroupID:        cfg.ConsumerGroup,
		Topic:          cfg.TradeTopic,
		InitialOffsets: initialOffsets,
	}, eng.HandleRecord, prod, logger)
	if err != nil {
		logger.Fatal("consumer init", zap.Error(err))
	}
	defer cons.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := cons.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("consumer exited", zap.Error(err))
			stop()
		}
	}()

	// ADR-0055: depth snapshots are emitted directly by Match as OrderBook
	// Full frames; Quote no longer schedules a depth ticker.

	// Engine-state persistence ticker (ADR-0036). Cancelled with a separate
	// context so we can still write a final snapshot on shutdown after the
	// rest of the pipeline has stopped.
	stateCtx, cancelState := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		runStateSnapshotTicker(stateCtx, cfg, eng, logger)
	}()

	<-rootCtx.Done()
	logger.Info("shutdown initiated")
	cons.Close()
	cancelState()
	wg.Wait()
	if err := writeStateSnapshot(cfg, eng); err != nil {
		logger.Error("final state snapshot", zap.Error(err))
	} else if cfg.StateSnapshotDir != "" {
		logger.Info("final state snapshot written", zap.String("path", stateSnapshotPath(cfg)))
	}
	logger.Info("quote shutdown complete")
}

// stateSnapshotPath is the absolute path of the engine-state file.
func stateSnapshotPath(cfg Config) string {
	// ADR-0049: basePath without extension; Save/Load append the right ext.
	return filepath.Join(cfg.StateSnapshotDir, cfg.InstanceID)
}

// tryRestore loads the engine-state snapshot (if any) and returns the
// per-partition offsets the consumer should start from. A missing file
// yields (nil, nil) and the consumer falls back to AtStart.
func tryRestore(cfg Config, eng *engine.Engine, logger *zap.Logger) (map[int32]int64, error) {
	if cfg.StateSnapshotDir == "" {
		logger.Info("state snapshot disabled (empty --state-snapshot-dir); cold start")
		return nil, nil
	}
	path := stateSnapshotPath(cfg)
	snap, err := snapshot.Load(path)
	if err != nil {
		return nil, fmt.Errorf("load %s: %w", path, err)
	}
	if snap == nil {
		logger.Info("no state snapshot found; cold start", zap.String("path", path))
		return nil, nil
	}
	if err := eng.Restore(snap); err != nil {
		return nil, fmt.Errorf("engine restore: %w", err)
	}
	logger.Info("engine state restored",
		zap.String("path", path),
		zap.Int64("taken_at_ms", snap.TakenAtMs),
		zap.Int("symbols", len(snap.Symbols)),
		zap.Int("partitions", len(snap.Offsets)))
	return snap.Offsets, nil
}

// writeStateSnapshot persists the engine's current state to disk atomically.
// No-op when the feature is disabled or the engine is empty.
func writeStateSnapshot(cfg Config, eng *engine.Engine) error {
	if cfg.StateSnapshotDir == "" {
		return nil
	}
	snap := eng.Capture()
	snap.TakenAtMs = time.Now().UnixMilli()
	return snapshot.Save(stateSnapshotPath(cfg), snap, cfg.StateSnapshotFormat)
}

// runStateSnapshotTicker persists engine state on a cadence. Interval <=0
// disables, leaving only the final shutdown write.
func runStateSnapshotTicker(ctx context.Context, cfg Config, eng *engine.Engine, logger *zap.Logger) {
	if cfg.StateSnapshotDir == "" || cfg.StateSnapshotInterval <= 0 {
		return
	}
	ticker := time.NewTicker(cfg.StateSnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := writeStateSnapshot(cfg, eng); err != nil {
				logger.Error("state snapshot", zap.Error(err))
			}
		}
	}
}

func parseFlags() Config {
	cfg := Config{
		InstanceID:            "quote-0",
		TradeTopic:            "trade-event",
		MarketDataTopic:       "market-data",
		SnapshotInterval:      5 * time.Second,
		StateSnapshotDir:      "./data/quote",
		StateSnapshotInterval: 30 * time.Second,
		StateSnapshotFormat:   snapshot.FormatProto, // ADR-0049
		Env:                   "dev",
		LogLevel:              "info",
	}
	var brokersStr, snapshotFormatStr string
	flag.StringVar(&cfg.InstanceID, "instance-id", cfg.InstanceID, "instance id (client id / consumer group suffix)")
	flag.StringVar(&brokersStr, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flag.StringVar(&cfg.TradeTopic, "trade-topic", cfg.TradeTopic, "trade-event topic name")
	flag.StringVar(&cfg.MarketDataTopic, "market-topic", cfg.MarketDataTopic, "market-data topic name")
	flag.StringVar(&cfg.ConsumerGroup, "group", "", "Kafka consumer group id (default quote-{instance-id})")
	flag.DurationVar(&cfg.SnapshotInterval, "snapshot-interval", cfg.SnapshotInterval, "how often to emit DepthSnapshot on market-data; 0 disables")
	flag.StringVar(&cfg.StateSnapshotDir, "state-snapshot-dir", cfg.StateSnapshotDir, "directory for engine-state snapshots (empty disables persistence)")
	flag.DurationVar(&cfg.StateSnapshotInterval, "state-snapshot-interval", cfg.StateSnapshotInterval, "how often to persist engine state to disk; 0 disables (only final snapshot on shutdown)")
	flag.StringVar(&snapshotFormatStr, "snapshot-format", cfg.StateSnapshotFormat.String(), "snapshot on-disk encoding: proto (default) | json (debug). ADR-0049. Env OPENTRADE_SNAPSHOT_FORMAT overrides.")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	cfg.Brokers = splitCSV(brokersStr)
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "quote-" + cfg.InstanceID
	}
	// --snapshot-format + OPENTRADE_SNAPSHOT_FORMAT env override (ADR-0049).
	if envFmt := os.Getenv("OPENTRADE_SNAPSHOT_FORMAT"); envFmt != "" {
		snapshotFormatStr = envFmt
	}
	if snapshotFormatStr != "" {
		f, err := snapshot.ParseFormat(snapshotFormatStr)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		cfg.StateSnapshotFormat = f
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
