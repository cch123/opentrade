// Command markprice produces the mark-price topic (ADR-0068 §5): it tails the
// spot + perp market-data OrderBook streams and emits MarkTick (high-frequency)
// + FundingTick (at each funding boundary) for perp-counter. The mark is the
// spot index + a capped EMA basis; the funding rate is the Binance method —
// a premium index built from the perp DEPTH-WEIGHTED impact prices (not
// top-of-book) vs the index, TWAP'd over the interval, plus a clamped interest
// term. A single tick goroutine owns the Calc; the market-data consumer feeds a
// concurrent-safe depth book.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/perp-pricing/internal/calc"
	indexprice "github.com/xargin/opentrade/perp-pricing/internal/index"
	"github.com/xargin/opentrade/perp-pricing/internal/journal"
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
	ImpactNotional      string
	InterestRateDaily   string
	PremiumBand         string
	IndexConfig         string
	IndexSourceMaxAge   string
	IndexQuorum         int
	IndexDeviationBand  string
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
	flag.StringVar(&cfg.ImpactNotional, "impact-notional", "20000", "impact margin notional (quote) the funding premium index is depth-weighted over (Binance method)")
	flag.StringVar(&cfg.InterestRateDaily, "funding-interest-rate-daily", "0.0003", "daily interest-rate component of the funding rate (Binance default 0.03%/day)")
	flag.StringVar(&cfg.PremiumBand, "premium-band", "0.0005", "± band clamping (interest - avg_premium) in the funding rate (Binance ±0.05%); 0 = pure premium")
	flag.StringVar(&cfg.IndexConfig, "index-config", "", "ADR-0069 per-symbol composite index JSON; empty uses only self:<spot-symbol>")
	flag.StringVar(&cfg.IndexSourceMaxAge, "index-source-max-age", "5s", "max age before an index source is stale")
	flag.IntVar(&cfg.IndexQuorum, "index-quorum", 2, "minimum live index sources required for a fresh index; self-only dev mode caps this to 1")
	flag.StringVar(&cfg.IndexDeviationBand, "index-deviation-band", "0.05", "median deviation band for outlier rejection; 0 disables")
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
	indexMaxAge, err := time.ParseDuration(cfg.IndexSourceMaxAge)
	if err != nil || indexMaxAge <= 0 {
		logger.Fatal("invalid --index-source-max-age", zap.String("v", cfg.IndexSourceMaxAge), zap.Error(err))
	}
	brokers := splitCSV(cfg.Brokers)
	if len(brokers) == 0 {
		logger.Fatal("at least one --brokers endpoint required")
	}

	c := calc.New(calc.Config{
		Alpha:         dec.New(cfg.Alpha),
		BasisCap:      dec.New(cfg.BasisCap),
		InterestDaily: dec.New(cfg.InterestRateDaily),
		IntervalMin:   int64(fundingInterval / time.Minute),
		PremiumBand:   dec.New(cfg.PremiumBand),
		FundingCap:    dec.New(cfg.FundingRateCap),
	})
	impactNotional := dec.New(cfg.ImpactNotional)
	book := journal.NewBook()
	indexCfg, selfSourceName, err := loadIndexConfig(cfg, indexMaxAge)
	if err != nil {
		logger.Fatal("index config", zap.Error(err))
	}
	indexEval, err := indexprice.NewEvaluator(indexCfg)
	if err != nil {
		logger.Fatal("index evaluator", zap.Error(err))
	}
	indexBook := indexprice.NewSourceBook()

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
	indexprice.RunExternalSources(ctx, indexBook, indexCfg.Sources, logger)

	logger.Info("markprice starting (ADR-0068 §5)",
		zap.String("spot_symbol", cfg.SpotSymbol), zap.String("perp_symbol", cfg.PerpSymbol),
		zap.String("mark_topic", cfg.MarkTopic), zap.Duration("tick", tickInterval),
		zap.Duration("funding_interval", fundingInterval),
		zap.Int("index_sources", len(indexCfg.Sources)), zap.Int("index_quorum", indexCfg.Quorum),
		zap.Duration("index_source_max_age", indexCfg.SourceMaxAge))

	go func() {
		if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error("market-data consumer exited", zap.Error(err))
			stop()
		}
	}()

	runTickLoop(ctx, cfg, tickInterval, fundingInterval, impactNotional,
		book, indexBook, indexEval, indexCfg, selfSourceName, c, producer, logger)

	logger.Info("markprice shutting down")
	_ = logger.Sync()
}

// runTickLoop owns the Calc (single goroutine). Each tick it reads the latest
// spot index + perp mid (for the mark) and the perp depth-weighted impact
// prices (for the funding premium sample), emits a MarkTick, and on crossing a
// funding boundary settles the round with one FundingTick. Blocks until ctx is
// cancelled.
func runTickLoop(ctx context.Context, cfg Config, tick, fundingInterval time.Duration, impactNotional dec.Decimal,
	book *journal.Book, indexBook *indexprice.SourceBook, indexEval *indexprice.Evaluator, indexCfg indexprice.Config,
	selfSourceName string, c *calc.Calc, producer *journal.MarkProducer, logger *zap.Logger) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	// Seed the boundary with the current interval so we settle only when we
	// cross into a NEW interval (no spurious settlement at startup).
	lastBoundary := time.Now().UTC().Truncate(fundingInterval)
	var lastMark dec.Decimal
	var lastStaleLogBoundary time.Time
	var seenIndexState bool
	var lastIndexStale, lastIndexDegraded bool

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			if spotMid, tsMs, ok := book.MidAt(cfg.SpotSymbol); ok {
				indexBook.Upsert(selfSourceName, spotMid, tsMs)
			}
			idx := indexEval.Eval(now, indexBook.Snapshot(indexCfg.Sources))
			if !idx.HasIndex {
				continue // no last-good index yet — any mark would be fabricated
			}
			if !seenIndexState || idx.Stale != lastIndexStale || idx.Degraded != lastIndexDegraded {
				level := logger.Info
				if idx.Stale || idx.Degraded {
					level = logger.Warn
				}
				level("index source state changed",
					zap.Bool("stale", idx.Stale), zap.Bool("degraded", idx.Degraded),
					zap.Int("live_sources", idx.LiveCount), zap.Int("used_sources", idx.UsedCount),
					zap.Int("dropped_sources", idx.DroppedCount), zap.Int("quorum", indexCfg.Quorum))
				seenIndexState = true
				lastIndexStale, lastIndexDegraded = idx.Stale, idx.Degraded
			}
			indexPx := idx.Index
			perpMid, okPerp := book.Mid(cfg.PerpSymbol)
			if !okPerp {
				perpMid = indexPx // no perp book yet → zero basis, mark == index
			}
			mark := c.Mark(indexPx, perpMid)
			// Funding premium sample from the perp impact prices vs the index
			// (skipped when the perp book is one-sided / absent). ADR-0069 also
			// skips samples while stale so a frozen index cannot contaminate the
			// next settled funding round.
			if !idx.Stale {
				if impactBid, impactAsk, okImp := book.ImpactPrices(cfg.PerpSymbol, impactNotional); okImp {
					c.SamplePremium(impactBid, impactAsk, indexPx)
				}
			}
			fundingEst := c.ForecastFundingRate()
			lastMark = mark
			if err := producer.PublishMarkTick(ctx, cfg.PerpSymbol, mark, indexPx, fundingEst, now.UnixMilli(), idx.Stale, idx.Degraded); err != nil && ctx.Err() == nil {
				logger.Warn("publish mark tick", zap.Error(err))
			}

			if curBoundary := now.UTC().Truncate(fundingInterval); curBoundary.After(lastBoundary) {
				if idx.Stale {
					if !curBoundary.Equal(lastStaleLogBoundary) {
						logger.Warn("funding round deferred because index is stale",
							zap.String("round_id", journal.FundingRoundID(cfg.PerpSymbol, curBoundary.Unix())),
							zap.Int("live_sources", idx.LiveCount), zap.Int("quorum", indexCfg.Quorum))
						lastStaleLogBoundary = curBoundary
					}
					continue
				}
				rate := c.SettleFundingRate()
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

type rawIndexSymbolConfig struct {
	Quorum        int              `json:"quorum"`
	SourceMaxAge  int64            `json:"source_max_age_ms"`
	DeviationBand string           `json:"deviation_band"`
	Sources       []rawIndexSource `json:"sources"`
}

type rawIndexSource struct {
	Name   string          `json:"name"`
	Weight json.RawMessage `json:"weight"`
}

func loadIndexConfig(cfg Config, defaultMaxAge time.Duration) (indexprice.Config, string, error) {
	selfName := "self:" + cfg.SpotSymbol
	out := indexprice.Config{
		Quorum:        cfg.IndexQuorum,
		SourceMaxAge:  defaultMaxAge,
		DeviationBand: dec.New(cfg.IndexDeviationBand),
		Sources: []indexprice.SourceConfig{{
			Name: selfName, Weight: dec.FromInt(1), Self: true,
		}},
	}
	if cfg.IndexConfig == "" {
		// Empty config is the local/dev self-source mode. It must produce a
		// degraded-but-fresh index so existing single-node smoke tests still
		// emit marks; production should pass --index-config and keep quorum > 1.
		if out.Quorum > len(out.Sources) {
			out.Quorum = len(out.Sources)
		}
		return out, selfName, out.Validate()
	}
	body, err := os.ReadFile(cfg.IndexConfig)
	if err != nil {
		return out, selfName, err
	}
	var bySymbol map[string]rawIndexSymbolConfig
	if err := json.Unmarshal(body, &bySymbol); err != nil {
		return out, selfName, err
	}
	raw, ok := bySymbol[cfg.PerpSymbol]
	if !ok {
		return out, selfName, fmt.Errorf("missing index config for %s", cfg.PerpSymbol)
	}
	if raw.Quorum > 0 {
		out.Quorum = raw.Quorum
	}
	if raw.SourceMaxAge > 0 {
		out.SourceMaxAge = time.Duration(raw.SourceMaxAge) * time.Millisecond
	}
	if raw.DeviationBand != "" {
		out.DeviationBand = dec.New(raw.DeviationBand)
	}
	out.Sources = out.Sources[:0]
	for _, src := range raw.Sources {
		weight, err := parseRawWeight(src.Weight)
		if err != nil {
			return out, selfName, fmt.Errorf("source %s weight: %w", src.Name, err)
		}
		if strings.HasPrefix(src.Name, "self:") {
			selfName = src.Name
		}
		out.Sources = append(out.Sources, indexprice.SourceConfig{
			Name: src.Name, Weight: weight, Self: strings.HasPrefix(src.Name, "self:"),
		})
	}
	return out, selfName, out.Validate()
}

func parseRawWeight(raw json.RawMessage) (dec.Decimal, error) {
	if len(raw) == 0 {
		return dec.Zero, fmt.Errorf("missing")
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return dec.Parse(s)
	}
	return dec.Parse(string(raw))
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
