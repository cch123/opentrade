// Command markprice produces the mark-price topic (ADR-0068 §5): it consumes
// the spot market-data stream for an index, folds the perp basis into a
// manipulation-resistant mark, and emits MarkTick / FundingTick for
// perp-counter. This is the M4 skeleton — the mark/funding computation
// (internal/calc) is ready and tested; the Kafka consume (spot market-data)
// and produce (mark-price) loop is wired with the broker adapters.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/markprice/internal/calc"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/logx"
)

// Config holds the markprice CLI flags.
type Config struct {
	Brokers         string
	SpotSymbol      string
	PerpSymbol      string
	MarkTopic       string
	Alpha           string
	BasisCap        string
	FundingRateCap  string
	FundingInterval string
	Env             string
	LogLevel        string
}

func main() {
	var cfg Config
	flag.StringVar(&cfg.Brokers, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flag.StringVar(&cfg.SpotSymbol, "spot-symbol", "BTC-USDT", "spot symbol used as the index source")
	flag.StringVar(&cfg.PerpSymbol, "perp-symbol", "BTC-USDT-PERP", "perp symbol to publish marks for")
	flag.StringVar(&cfg.MarkTopic, "mark-topic", "mark-price", "mark-price topic to produce to")
	flag.StringVar(&cfg.Alpha, "ema-alpha", "0.1", "EMA smoothing for the basis, (0,1]")
	flag.StringVar(&cfg.BasisCap, "basis-cap", "0", "clamp on |mark-index| (absolute USDT); 0 = none")
	flag.StringVar(&cfg.FundingRateCap, "funding-rate-cap", "0.0075", "clamp on |funding_rate| per interval")
	flag.StringVar(&cfg.FundingInterval, "funding-interval", "8h", "funding settlement interval")
	flag.StringVar(&cfg.Env, "env", "dev", "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", "info", "log level")
	flag.Parse()

	logger, err := logx.New(logx.Config{Service: "markprice", Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	logx.SetGlobal(logger)

	calcCfg := calc.Config{
		Alpha:          dec.New(cfg.Alpha),
		BasisCap:       dec.New(cfg.BasisCap),
		FundingRateCap: dec.New(cfg.FundingRateCap),
	}
	c := calc.New(calcCfg)
	_ = c // fed by the spot market-data consume loop once Kafka is wired

	logger.Info("markprice starting (ADR-0068 M4 skeleton)",
		zap.String("spot_symbol", cfg.SpotSymbol), zap.String("perp_symbol", cfg.PerpSymbol),
		zap.String("mark_topic", cfg.MarkTopic), zap.String("funding_interval", cfg.FundingInterval))
	logger.Warn("M4 scope: mark/funding calc ready (internal/calc); spot market-data consume + mark-price produce pending Kafka wiring")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	logger.Info("markprice shutting down")
	_ = logger.Sync()
}
