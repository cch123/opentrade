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
)

type Config struct {
	InstanceID       string
	Brokers          []string
	TradeTopic       string
	MarketDataTopic  string
	ConsumerGroup    string
	SnapshotInterval time.Duration
	Env              string
	LogLevel         string
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
		Brokers:  cfg.Brokers,
		ClientID: cfg.InstanceID,
		GroupID:  cfg.ConsumerGroup,
		Topic:    cfg.TradeTopic,
	}, eng.Handle, prod, logger)
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

	wg.Add(1)
	go func() {
		defer wg.Done()
		runSnapshotTicker(rootCtx, cfg.SnapshotInterval, eng, prod, logger)
	}()

	<-rootCtx.Done()
	logger.Info("shutdown initiated")
	cons.Close()
	wg.Wait()
	logger.Info("quote shutdown complete")
}

func runSnapshotTicker(ctx context.Context, interval time.Duration, eng *engine.Engine, prod *producer.MarketDataProducer, logger *zap.Logger) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			snaps := eng.SnapshotAll()
			if len(snaps) == 0 {
				continue
			}
			if err := prod.PublishBatch(ctx, snaps); err != nil {
				logger.Error("snapshot publish", zap.Error(err))
			}
		}
	}
}

func parseFlags() Config {
	cfg := Config{
		InstanceID:       "quote-0",
		TradeTopic:       "trade-event",
		MarketDataTopic:  "market-data",
		SnapshotInterval: 5 * time.Second,
		Env:              "dev",
		LogLevel:         "info",
	}
	var brokersStr string
	flag.StringVar(&cfg.InstanceID, "instance-id", cfg.InstanceID, "instance id (client id / consumer group suffix)")
	flag.StringVar(&brokersStr, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flag.StringVar(&cfg.TradeTopic, "trade-topic", cfg.TradeTopic, "trade-event topic name")
	flag.StringVar(&cfg.MarketDataTopic, "market-topic", cfg.MarketDataTopic, "market-data topic name")
	flag.StringVar(&cfg.ConsumerGroup, "group", "", "Kafka consumer group id (default quote-{instance-id})")
	flag.DurationVar(&cfg.SnapshotInterval, "snapshot-interval", cfg.SnapshotInterval, "how often to emit DepthSnapshot; 0 disables")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	cfg.Brokers = splitCSV(brokersStr)
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "quote-" + cfg.InstanceID
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
