// Command markprice produces the perp-price topic (ADR-0068 §5): it tails the
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
	SpotSymbols         string
	PerpSymbol          string
	PerpSymbols         string
	SpotMarketDataTopic string
	PerpMarketDataTopic string
	MarkTopic           string
	Alpha               string
	BasisCap            string
	FundingRateCap      string
	FundingInterval     string
	FundingIntervals    string
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
	flag.StringVar(&cfg.SpotSymbols, "spot-symbols", "", "per-symbol spot mappings: PERP=SPOT[,PERP=SPOT...]")
	flag.StringVar(&cfg.PerpSymbol, "perp-symbol", "BTC-USDT-PERP", "perp symbol to publish marks for")
	flag.StringVar(&cfg.PerpSymbols, "perp-symbols", "", "comma-separated perp symbols; empty uses --perp-symbol")
	flag.StringVar(&cfg.SpotMarketDataTopic, "spot-market-data-topic", "market-data", "spot OrderBook market-data topic (index source)")
	flag.StringVar(&cfg.PerpMarketDataTopic, "perp-market-data-topic", "perp-market-data", "perp OrderBook market-data topic (basis source)")
	flag.StringVar(&cfg.MarkTopic, "perp-price-topic", "perp-price", "perp-price topic to produce to")
	flag.StringVar(&cfg.Alpha, "ema-alpha", "0.1", "EMA smoothing for the basis, (0,1]")
	flag.StringVar(&cfg.BasisCap, "basis-cap", "0", "clamp on |mark-index| (absolute USDT); 0 = none")
	flag.StringVar(&cfg.FundingRateCap, "funding-rate-cap", "0.0075", "clamp on |funding_rate| per interval")
	flag.StringVar(&cfg.FundingInterval, "funding-interval", "8h", "funding settlement interval (aligned to UTC)")
	flag.StringVar(&cfg.FundingIntervals, "funding-intervals", "", "per-symbol funding intervals: PERP=8h[,PERP=4h...]")
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

	impactNotional := dec.New(cfg.ImpactNotional)
	book := journal.NewBook()
	runtimes, err := buildSymbolRuntimes(cfg, fundingInterval, indexMaxAge, time.Now())
	if err != nil {
		logger.Fatal("symbol runtime config", zap.Error(err))
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
	indexprice.RunExternalSources(ctx, indexBook, collectIndexSources(runtimes), logger)

	logger.Info("markprice starting (ADR-0068 §5)",
		zap.Strings("perp_symbols", runtimeSymbols(runtimes)),
		zap.Strings("funding_intervals", runtimeFundingIntervals(runtimes)),
		zap.String("mark_topic", cfg.MarkTopic), zap.Duration("tick", tickInterval),
		zap.Duration("default_funding_interval", fundingInterval))

	go func() {
		if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error("market-data consumer exited", zap.Error(err))
			stop()
		}
	}()

	runTickLoop(ctx, tickInterval, impactNotional, book, indexBook, runtimes, producer, logger)

	logger.Info("markprice shutting down")
	_ = logger.Sync()
}

type markPublisher interface {
	PublishMarkTick(ctx context.Context, symbol string, mark, index, fundingEst dec.Decimal, tsMs int64, indexStale, indexDegraded bool) error
	PublishFundingTick(ctx context.Context, symbol string, roundID int64, rate, mark dec.Decimal, tsMs int64) error
}

// symbolRuntime is the per-symbol state ADR-0068 requires for funding: the
// premium accumulator, interest interval, index evaluator, and boundary cursor
// must not be shared across symbols because exchanges can list contracts with
// different funding cadences.
type symbolRuntime struct {
	PerpSymbol      string
	SpotSymbol      string
	FundingInterval time.Duration
	Calc            *calc.Calc
	IndexCfg        indexprice.Config
	IndexEval       *indexprice.Evaluator
	SelfSourceName  string

	LastBoundary         time.Time
	LastMark             dec.Decimal
	LastStaleLogBoundary time.Time
	SeenIndexState       bool
	LastIndexStale       bool
	LastIndexDegraded    bool
}

// runTickLoop owns all per-symbol Calc instances (single goroutine). Each tick
// walks every configured symbol and advances its own mark/funding state; this
// keeps symbols with different funding intervals deterministic without adding
// cross-goroutine ordering questions.
func runTickLoop(ctx context.Context, tick time.Duration, impactNotional dec.Decimal,
	book *journal.Book, indexBook *indexprice.SourceBook, runtimes []*symbolRuntime,
	producer markPublisher, logger *zap.Logger) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			for _, rt := range runtimes {
				runSymbolTick(ctx, now, impactNotional, book, indexBook, rt, producer, logger)
			}
		}
	}
}

func runSymbolTick(ctx context.Context, now time.Time, impactNotional dec.Decimal,
	book *journal.Book, indexBook *indexprice.SourceBook, rt *symbolRuntime,
	producer markPublisher, logger *zap.Logger) {
	if spotMid, tsMs, ok := book.MidAt(rt.SpotSymbol); ok {
		indexBook.Upsert(rt.SelfSourceName, spotMid, tsMs)
	}
	idx := rt.IndexEval.Eval(now, indexBook.Snapshot(rt.IndexCfg.Sources))
	if !idx.HasIndex {
		return // no last-good index yet — any mark would be fabricated
	}
	if !rt.SeenIndexState || idx.Stale != rt.LastIndexStale || idx.Degraded != rt.LastIndexDegraded {
		level := logger.Info
		if idx.Stale || idx.Degraded {
			level = logger.Warn
		}
		level("index source state changed",
			zap.String("symbol", rt.PerpSymbol),
			zap.Bool("stale", idx.Stale), zap.Bool("degraded", idx.Degraded),
			zap.Int("live_sources", idx.LiveCount), zap.Int("used_sources", idx.UsedCount),
			zap.Int("dropped_sources", idx.DroppedCount), zap.Int("quorum", rt.IndexCfg.Quorum))
		rt.SeenIndexState = true
		rt.LastIndexStale, rt.LastIndexDegraded = idx.Stale, idx.Degraded
	}
	indexPx := idx.Index
	perpMid, okPerp := book.Mid(rt.PerpSymbol)
	if !okPerp {
		perpMid = indexPx // no perp book yet → zero basis, mark == index
	}
	mark := rt.Calc.Mark(indexPx, perpMid)
	// Funding premium sample from the perp impact prices vs the index (skipped
	// when the perp book is one-sided / absent). ADR-0069 also skips samples
	// while stale so a frozen index cannot contaminate that symbol's accumulator.
	if !idx.Stale {
		if impactBid, impactAsk, okImp := book.ImpactPrices(rt.PerpSymbol, impactNotional); okImp {
			rt.Calc.SamplePremium(impactBid, impactAsk, indexPx)
		}
	}
	fundingEst := rt.Calc.ForecastFundingRate()
	rt.LastMark = mark
	if err := producer.PublishMarkTick(ctx, rt.PerpSymbol, mark, indexPx, fundingEst, now.UnixMilli(), idx.Stale, idx.Degraded); err != nil && ctx.Err() == nil {
		logger.Warn("publish mark tick", zap.String("symbol", rt.PerpSymbol), zap.Error(err))
	}

	if curBoundary := now.UTC().Truncate(rt.FundingInterval); curBoundary.After(rt.LastBoundary) {
		if idx.Stale {
			if !curBoundary.Equal(rt.LastStaleLogBoundary) {
				logger.Warn("funding round deferred because index is stale",
					zap.String("round_id", journal.FundingRoundID(rt.PerpSymbol, curBoundary.Unix())),
					zap.Int("live_sources", idx.LiveCount), zap.Int("quorum", rt.IndexCfg.Quorum))
				rt.LastStaleLogBoundary = curBoundary
			}
			return
		}
		rate := rt.Calc.SettleFundingRate()
		roundID := curBoundary.Unix()
		if err := producer.PublishFundingTick(ctx, rt.PerpSymbol, roundID, rate, rt.LastMark, now.UnixMilli()); err != nil && ctx.Err() == nil {
			logger.Warn("publish funding tick", zap.String("symbol", rt.PerpSymbol), zap.Error(err))
		} else {
			logger.Info("funding round settled",
				zap.String("round_id", journal.FundingRoundID(rt.PerpSymbol, roundID)),
				zap.String("rate", rate.String()))
		}
		rt.LastBoundary = curBoundary
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

func buildSymbolRuntimes(cfg Config, defaultFundingInterval, indexMaxAge time.Duration, now time.Time) ([]*symbolRuntime, error) {
	symbols := splitCSV(cfg.PerpSymbols)
	if len(symbols) == 0 {
		symbols = splitCSV(cfg.PerpSymbol)
	}
	if len(symbols) == 0 {
		return nil, fmt.Errorf("at least one perp symbol required")
	}
	symbolSet := map[string]struct{}{}
	for _, sym := range symbols {
		symbolSet[sym] = struct{}{}
	}
	spotOverrides, err := parseStringAssignments(cfg.SpotSymbols)
	if err != nil {
		return nil, fmt.Errorf("spot-symbols: %w", err)
	}
	fundingOverrides, err := parseDurationAssignments(cfg.FundingIntervals)
	if err != nil {
		return nil, fmt.Errorf("funding-intervals: %w", err)
	}
	for sym := range spotOverrides {
		if _, ok := symbolSet[sym]; !ok {
			return nil, fmt.Errorf("spot-symbols references unknown perp symbol %s", sym)
		}
	}
	for sym := range fundingOverrides {
		if _, ok := symbolSet[sym]; !ok {
			return nil, fmt.Errorf("funding-intervals references unknown perp symbol %s", sym)
		}
	}

	out := make([]*symbolRuntime, 0, len(symbols))
	for _, perpSymbol := range symbols {
		spotSymbol := spotSymbolFor(perpSymbol, cfg.SpotSymbol, spotOverrides, len(symbols) == 1)
		fundingInterval := defaultFundingInterval
		if override, ok := fundingOverrides[perpSymbol]; ok {
			fundingInterval = override
		}
		indexCfg, selfSourceName, err := loadIndexConfig(cfg, perpSymbol, spotSymbol, indexMaxAge)
		if err != nil {
			return nil, err
		}
		indexEval, err := indexprice.NewEvaluator(indexCfg)
		if err != nil {
			return nil, err
		}
		// Each symbol gets its own Calc because funding premium TWAP and the
		// interest-per-interval term depend on that symbol's settlement cadence.
		c := calc.New(calc.Config{
			Alpha:         dec.New(cfg.Alpha),
			BasisCap:      dec.New(cfg.BasisCap),
			InterestDaily: dec.New(cfg.InterestRateDaily),
			IntervalMin:   int64(fundingInterval / time.Minute),
			PremiumBand:   dec.New(cfg.PremiumBand),
			FundingCap:    dec.New(cfg.FundingRateCap),
		})
		out = append(out, &symbolRuntime{
			PerpSymbol:      perpSymbol,
			SpotSymbol:      spotSymbol,
			FundingInterval: fundingInterval,
			Calc:            c,
			IndexCfg:        indexCfg,
			IndexEval:       indexEval,
			SelfSourceName:  selfSourceName,
			LastBoundary:    now.UTC().Truncate(fundingInterval),
		})
	}
	return out, nil
}

func loadIndexConfig(cfg Config, perpSymbol, spotSymbol string, defaultMaxAge time.Duration) (indexprice.Config, string, error) {
	selfName := "self:" + spotSymbol
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
	raw, ok := bySymbol[perpSymbol]
	if !ok {
		return out, selfName, fmt.Errorf("missing index config for %s", perpSymbol)
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

func collectIndexSources(runtimes []*symbolRuntime) []indexprice.SourceConfig {
	seen := map[string]indexprice.SourceConfig{}
	for _, rt := range runtimes {
		for _, src := range rt.IndexCfg.Sources {
			if _, ok := seen[src.Name]; !ok {
				seen[src.Name] = src
			}
		}
	}
	out := make([]indexprice.SourceConfig, 0, len(seen))
	for _, src := range seen {
		out = append(out, src)
	}
	return out
}

func runtimeSymbols(runtimes []*symbolRuntime) []string {
	out := make([]string, 0, len(runtimes))
	for _, rt := range runtimes {
		out = append(out, rt.PerpSymbol)
	}
	return out
}

func runtimeFundingIntervals(runtimes []*symbolRuntime) []string {
	out := make([]string, 0, len(runtimes))
	for _, rt := range runtimes {
		out = append(out, rt.PerpSymbol+"="+rt.FundingInterval.String())
	}
	return out
}

func spotSymbolFor(perpSymbol, legacySpot string, overrides map[string]string, single bool) string {
	if spot, ok := overrides[perpSymbol]; ok {
		return spot
	}
	if single && legacySpot != "" {
		return legacySpot
	}
	return strings.TrimSuffix(perpSymbol, "-PERP")
}

func parseStringAssignments(raw string) (map[string]string, error) {
	out := map[string]string{}
	for _, part := range splitCSV(raw) {
		k, v, ok := strings.Cut(part, "=")
		if !ok || strings.TrimSpace(k) == "" || strings.TrimSpace(v) == "" {
			return nil, fmt.Errorf("invalid assignment %q, want KEY=VALUE", part)
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return out, nil
}

func parseDurationAssignments(raw string) (map[string]time.Duration, error) {
	rawMap, err := parseStringAssignments(raw)
	if err != nil {
		return nil, err
	}
	out := map[string]time.Duration{}
	for sym, v := range rawMap {
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("%s has invalid duration %q", sym, v)
		}
		out[sym] = d
	}
	return out, nil
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
