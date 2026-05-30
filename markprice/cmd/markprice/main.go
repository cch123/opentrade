// Command markprice produces the mark-price topic (ADR-0068 §5): it tails the
// spot + perp market-data OrderBook streams for an index (spot mid) and basis
// (perp mid), folds them into a manipulation-resistant mark via internal/calc,
// and emits MarkTick (high-frequency) + FundingTick (at each funding boundary)
// for perp-counter. A single tick goroutine owns the Calc; the market-data
// consumer feeds a concurrent-safe mid book.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/markprice/internal/calc"
	"github.com/xargin/opentrade/markprice/internal/journal"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/logx"
)

// Config holds the markprice CLI flags.
type Config struct {
	InstanceID          string
	Brokers             string
	SpotSymbol          string
	PerpSymbol          string
	SpotMarketDataTopic string
	PerpMarketDataTopic string
	MarkTopic           string
	Alpha               string
	BasisCap            string
	FundingRateCap      string
	FundingInterval     string
	TickInterval        string
	Env                 string
	LogLevel            string
}

func main() {
	var cfg Config
	flag.StringVar(&cfg.InstanceID, "instance-id", "markprice-0", "instance id (client id / producer id)")
	flag.StringVar(&cfg.Brokers, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flag.StringVar(&cfg.SpotSymbol, "spot-symbol", "BTC-USDT", "spot symbol used as the index source")
	flag.StringVar(&cfg.PerpSymbol, "perp-symbol", "BTC-USDT-PERP", "perp symbol to publish marks for")
	flag.StringVar(&cfg.SpotMarketDataTopic, "spot-market-data-topic", "market-data", "spot OrderBook market-data topic (index source)")
	flag.StringVar(&cfg.PerpMarketDataTopic, "perp-market-data-topic", "perp-market-data", "perp OrderBook market-data topic (basis source)")
	flag.StringVar(&cfg.MarkTopic, "mark-topic", "mark-price", "mark-price topic to produce to")
	flag.StringVar(&cfg.Alpha, "ema-alpha", "0.1", "EMA smoothing for the basis, (0,1]")
	flag.StringVar(&cfg.BasisCap, "basis-cap", "0", "clamp on |mark-index| (absolute USDT); 0 = none")
	flag.StringVar(&cfg.FundingRateCap, "funding-rate-cap", "0.0075", "clamp on |funding_rate| per interval")
	flag.StringVar(&cfg.FundingInterval, "funding-interval", "8h", "funding settlement interval (aligned to UTC)")
	flag.StringVar(&cfg.TickInterval, "tick-interval", "1s", "how often to emit a MarkTick")
	flag.StringVar(&cfg.Env, "env", "dev", "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", "info", "log level")
	flag.Parse()

	logger, err := logx.New(logx.Config{Service: "markprice", Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	logx.SetGlobal(logger)

	tickInterval, err := time.ParseDuration(cfg.TickInterval)
	if err != nil || tickInterval <= 0 {
		logger.Fatal("invalid --tick-interval", zap.String("v", cfg.TickInterval), zap.Error(err))
	}
	fundingInterval, err := time.ParseDuration(cfg.FundingInterval)
	if err != nil || fundingInterval <= 0 {
		logger.Fatal("invalid --funding-interval", zap.String("v", cfg.FundingInterval), zap.Error(err))
	}
	brokers := splitCSV(cfg.Brokers)
	if len(brokers) == 0 {
		logger.Fatal("at least one --brokers endpoint required")
	}

	c := calc.New(calc.Config{
		Alpha:          dec.New(cfg.Alpha),
		BasisCap:       dec.New(cfg.BasisCap),
		FundingRateCap: dec.New(cfg.FundingRateCap),
	})
	book := journal.NewMidBook()

	consumer, err := journal.NewMarketDataConsumer(journal.MarketDataConsumerConfig{
		Brokers:  brokers,
		ClientID: cfg.InstanceID,
		Topics:   []string{cfg.SpotMarketDataTopic, cfg.PerpMarketDataTopic},
	}, book, logger)
	if err != nil {
		logger.Fatal("market-data consumer", zap.Error(err))
	}
	defer consumer.Close()

	producer, err := journal.NewMarkProducer(journal.MarkProducerConfig{
		Brokers:    brokers,
		ClientID:   cfg.InstanceID,
		ProducerID: cfg.InstanceID,
		Topic:      cfg.MarkTopic,
	}, logger)
	if err != nil {
		logger.Fatal("mark producer", zap.Error(err))
	}
	defer producer.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("markprice starting (ADR-0068 §5)",
		zap.String("spot_symbol", cfg.SpotSymbol), zap.String("perp_symbol", cfg.PerpSymbol),
		zap.String("mark_topic", cfg.MarkTopic), zap.Duration("tick", tickInterval),
		zap.Duration("funding_interval", fundingInterval))

	go func() {
		if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error("market-data consumer exited", zap.Error(err))
			stop()
		}
	}()

	runTickLoop(ctx, cfg, tickInterval, fundingInterval, book, c, producer, logger)

	logger.Info("markprice shutting down")
	_ = logger.Sync()
}

// runTickLoop owns the Calc (single goroutine). Each tick it reads the latest
// spot/perp mids, folds a MarkTick, and on crossing a funding boundary settles
// the round with one FundingTick. Blocks until ctx is cancelled.
func runTickLoop(ctx context.Context, cfg Config, tick, fundingInterval time.Duration,
	book *journal.MidBook, c *calc.Calc, producer *journal.MarkProducer, logger *zap.Logger) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	// Seed the boundary with the current interval so we settle only when we
	// cross into a NEW interval (no spurious settlement at startup).
	lastBoundary := time.Now().UTC().Truncate(fundingInterval)
	var lastMark dec.Decimal

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			spotMid, ok := book.Mid(cfg.SpotSymbol)
			if !ok {
				continue // no index yet — wait for the first spot Full
			}
			perpMid, okPerp := book.Mid(cfg.PerpSymbol)
			if !okPerp {
				perpMid = spotMid // no perp book yet → zero basis, mark == index
			}
			mark, fundingEst := c.Tick(spotMid, perpMid)
			lastMark = mark
			now := time.Now()
			if err := producer.PublishMarkTick(ctx, cfg.PerpSymbol, mark, spotMid, fundingEst, now.UnixMilli()); err != nil && ctx.Err() == nil {
				logger.Warn("publish mark tick", zap.Error(err))
			}

			if curBoundary := now.UTC().Truncate(fundingInterval); curBoundary.After(lastBoundary) {
				rate := c.FundingRate()
				roundID := curBoundary.Unix()
				if err := producer.PublishFundingTick(ctx, cfg.PerpSymbol, roundID, rate, lastMark, now.UnixMilli()); err != nil && ctx.Err() == nil {
					logger.Warn("publish funding tick", zap.Error(err))
				} else {
					logger.Info("funding round settled",
						zap.String("round_id", journal.FundingRoundID(cfg.PerpSymbol, roundID)),
						zap.String("rate", rate.String()))
				}
				lastBoundary = curBoundary
			}
		}
	}
}

// splitCSV splits a comma-separated flag value, trimming blanks.
func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
