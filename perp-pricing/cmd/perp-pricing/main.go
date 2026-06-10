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
	"sort"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/perp-pricing/internal/calc"
	indexprice "github.com/xargin/opentrade/perp-pricing/internal/index"
	"github.com/xargin/opentrade/perp-pricing/internal/journal"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/logx"
	"github.com/xargin/opentrade/pkg/perpcfg"
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
	CatalogDSN          string
	CatalogPollInterval string
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
	flag.StringVar(&cfg.CatalogDSN, "catalog-dsn", "", "MySQL DSN of the ADR-0075 perp symbol catalog; set, the catalog drives the symbol set + per-symbol funding/pricing params and stamps config_version onto every tick (flags become the dev fallback)")
	flag.StringVar(&cfg.CatalogPollInterval, "catalog-poll-interval", "1s", "catalog anchor poll cadence")
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

	// ADR-0075: with a catalog the symbol set + per-symbol funding/pricing
	// params come from MySQL and every tick is stamped with its
	// config_version; flags remain the dev fallback.
	var (
		catalog  *perpcfg.Cache
		runtimes []*symbolRuntime
	)
	if cfg.CatalogDSN != "" {
		pollInterval, err := time.ParseDuration(cfg.CatalogPollInterval)
		if err != nil || pollInterval <= 0 {
			logger.Fatal("invalid --catalog-poll-interval", zap.String("v", cfg.CatalogPollInterval), zap.Error(err))
		}
		store, err := perpcfg.OpenMySQLStore(perpcfg.MySQLConfig{DSN: cfg.CatalogDSN})
		if err != nil {
			logger.Fatal("perp catalog store", zap.Error(err))
		}
		defer func() { _ = store.Close() }()
		catalog = perpcfg.NewCache(perpcfg.CacheConfig{
			Store: store, PollInterval: pollInterval, Logger: logger,
		})
		loadCtx, cancelLoad := context.WithTimeout(context.Background(), 10*time.Second)
		err = catalog.Load(loadCtx)
		cancelLoad()
		if err != nil {
			logger.Fatal("perp catalog initial load", zap.Error(err))
		}
		now := time.Now()
		for _, sym := range catalog.Symbols() {
			view, ok := catalog.Active(sym)
			if !ok {
				continue // listed but not yet effective — sync() picks it up later
			}
			rt, err := catalogRuntime(view, impactNotional, now)
			if err != nil {
				logger.Fatal("catalog runtime", zap.String("symbol", sym), zap.Error(err))
			}
			runtimes = append(runtimes, rt)
		}
	} else {
		var err error
		runtimes, err = buildSymbolRuntimes(cfg, fundingInterval, indexMaxAge, time.Now())
		if err != nil {
			logger.Fatal("symbol runtime config", zap.Error(err))
		}
	}
	mgr := newRuntimeManager(runtimes, catalog, impactNotional, logger)
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
	if catalog != nil {
		go catalog.Run(ctx)
	}
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

	runTickLoop(ctx, tickInterval, book, indexBook, mgr, producer, logger)

	logger.Info("markprice shutting down")
	_ = logger.Sync()
}

type markPublisher interface {
	PublishMarkTick(ctx context.Context, symbol string, mark, index, fundingEst dec.Decimal, tsMs int64, indexStale, indexDegraded bool, cfgVersion uint64) error
	PublishFundingTick(ctx context.Context, symbol string, roundID int64, rate, mark dec.Decimal, tsMs int64, cfgVersion uint64) error
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
	ImpactNotional  dec.Decimal
	ConfigVersion   uint64 // ADR-0075: stamped onto every tick; 0 = flag-driven

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
// cross-goroutine ordering questions. With an ADR-0075 catalog the loop also
// reconciles the runtime set against the catalog before each tick — runtimes
// are single-owner state, so config swaps happen in this goroutine only.
func runTickLoop(ctx context.Context, tick time.Duration,
	book *journal.Book, indexBook *indexprice.SourceBook, mgr *runtimeManager,
	producer markPublisher, logger *zap.Logger) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			mgr.sync(now)
			for _, rt := range mgr.ordered() {
				runSymbolTick(ctx, now, book, indexBook, rt, producer, logger)
			}
		}
	}
}

// runtimeManager owns the symbol → runtime set. In legacy (flag) mode the set
// is fixed at startup; in catalog mode sync() reconciles it with the active
// catalog configs every tick.
type runtimeManager struct {
	bySym          map[string]*symbolRuntime
	catalog        *perpcfg.Cache
	impactFallback dec.Decimal // flag default when a config carries 0
	startedSources map[string]struct{}
	logger         *zap.Logger
}

func newRuntimeManager(runtimes []*symbolRuntime, catalog *perpcfg.Cache, impactFallback dec.Decimal, logger *zap.Logger) *runtimeManager {
	m := &runtimeManager{
		bySym:          map[string]*symbolRuntime{},
		catalog:        catalog,
		impactFallback: impactFallback,
		startedSources: map[string]struct{}{},
		logger:         logger,
	}
	for _, rt := range runtimes {
		m.bySym[rt.PerpSymbol] = rt
		for _, srcCfg := range rt.IndexCfg.Sources {
			m.startedSources[srcCfg.Name] = struct{}{}
		}
	}
	return m
}

func (m *runtimeManager) ordered() []*symbolRuntime {
	syms := make([]string, 0, len(m.bySym))
	for s := range m.bySym {
		syms = append(syms, s)
	}
	sort.Strings(syms)
	out := make([]*symbolRuntime, 0, len(syms))
	for _, s := range syms {
		out = append(out, m.bySym[s])
	}
	return out
}

// sync reconciles runtimes with the catalog: new symbols get a runtime
// (hot listing), a changed active version rebuilds the runtime (the premium
// accumulator restarts and the in-progress funding round re-anchors at the
// new interval — one round is deliberately skipped rather than settled from
// mixed-parameter samples), and symbols with no effective config are
// dropped. Index-source ADDITIONS need a process restart for their fetchers;
// weight/quorum/staleness changes apply live through the rebuilt evaluator.
func (m *runtimeManager) sync(now time.Time) {
	if m.catalog == nil {
		return
	}
	seen := map[string]struct{}{}
	for _, sym := range m.catalog.Symbols() {
		view, ok := m.catalog.Active(sym)
		if !ok {
			continue
		}
		seen[sym] = struct{}{}
		cur := m.bySym[sym]
		if cur != nil && cur.ConfigVersion == view.Cfg.ConfigVersion {
			continue
		}
		rt, err := catalogRuntime(view, m.impactFallback, now)
		if err != nil {
			m.logger.Warn("perp pricing runtime build failed",
				zap.String("symbol", sym), zap.Error(err))
			continue
		}
		for _, srcCfg := range rt.IndexCfg.Sources {
			if _, started := m.startedSources[srcCfg.Name]; !started && !srcCfg.Self {
				m.logger.Warn("new external index source requires a perp-pricing restart to start its fetcher",
					zap.String("symbol", sym), zap.String("source", srcCfg.Name))
			}
		}
		if cur != nil {
			m.logger.Info("perp pricing runtime rebuilt",
				zap.String("symbol", sym),
				zap.Uint64("old_version", cur.ConfigVersion),
				zap.Uint64("new_version", rt.ConfigVersion))
		} else {
			m.logger.Info("perp pricing runtime added",
				zap.String("symbol", sym), zap.Uint64("version", rt.ConfigVersion))
		}
		m.bySym[sym] = rt
	}
	for sym := range m.bySym {
		if _, ok := seen[sym]; !ok {
			m.logger.Warn("perp pricing runtime dropped (no effective catalog config)",
				zap.String("symbol", sym))
			delete(m.bySym, sym)
		}
	}
}

// catalogRuntime builds a fresh runtime from one catalog view (ADR-0075).
func catalogRuntime(view perpcfg.View, impactFallback dec.Decimal, now time.Time) (*symbolRuntime, error) {
	cfg := view.Cfg
	spot := cfg.Pricing.SpotSymbol
	if spot == "" {
		spot = view.Spec.BaseAsset + "-" + view.Spec.QuoteAsset
	}
	interval := time.Duration(cfg.Funding.IntervalSeconds) * time.Second
	idxCfg, selfName := indexConfigFromCatalog(cfg.Pricing, spot)
	if err := idxCfg.Validate(); err != nil {
		return nil, err
	}
	eval, err := indexprice.NewEvaluator(idxCfg)
	if err != nil {
		return nil, err
	}
	impact := cfg.Pricing.ImpactNotional
	if impact.Sign() <= 0 {
		impact = impactFallback
	}
	c := calc.New(calc.Config{
		Alpha:         cfg.Pricing.MarkEmaAlpha,
		BasisCap:      cfg.Pricing.MarkBasisCap,
		InterestDaily: cfg.Funding.InterestRate,
		IntervalMin:   int64(interval / time.Minute),
		PremiumBand:   cfg.Funding.Clamp,
		FundingCap:    cfg.Funding.Cap,
		FundingFloor:  cfg.Funding.Floor,
	})
	return &symbolRuntime{
		PerpSymbol:      view.Spec.Symbol,
		SpotSymbol:      spot,
		FundingInterval: interval,
		Calc:            c,
		IndexCfg:        idxCfg,
		IndexEval:       eval,
		SelfSourceName:  selfName,
		ImpactNotional:  impact,
		ConfigVersion:   cfg.ConfigVersion,
		LastBoundary:    now.UTC().Truncate(interval),
	}, nil
}

// indexConfigFromCatalog maps PricingParams onto the ADR-0069 evaluator
// config. No sources = self-only dev mode (quorum 1, degraded-but-fresh).
func indexConfigFromCatalog(p perpcfg.PricingParams, spotSymbol string) (indexprice.Config, string) {
	selfName := "self:" + spotSymbol
	out := indexprice.Config{
		Quorum:        p.IndexQuorum,
		SourceMaxAge:  time.Duration(p.IndexMaxAgeMs) * time.Millisecond,
		DeviationBand: p.IndexDeviationBand,
	}
	if out.SourceMaxAge <= 0 {
		out.SourceMaxAge = 5 * time.Second
	}
	if len(p.IndexSources) == 0 {
		out.Sources = []indexprice.SourceConfig{{Name: selfName, Weight: dec.FromInt(1), Self: true}}
		out.Quorum = 1
		return out, selfName
	}
	for _, src := range p.IndexSources {
		self := strings.HasPrefix(src.Name, "self:")
		if self {
			selfName = src.Name
		}
		out.Sources = append(out.Sources, indexprice.SourceConfig{
			Name: src.Name, Weight: src.Weight, Self: self,
		})
	}
	if out.Quorum <= 0 {
		out.Quorum = 2
		if out.Quorum > len(out.Sources) {
			out.Quorum = len(out.Sources)
		}
	}
	return out, selfName
}

func runSymbolTick(ctx context.Context, now time.Time,
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
		if impactBid, impactAsk, okImp := book.ImpactPrices(rt.PerpSymbol, rt.ImpactNotional); okImp {
			rt.Calc.SamplePremium(impactBid, impactAsk, indexPx)
		}
	}
	fundingEst := rt.Calc.ForecastFundingRate()
	rt.LastMark = mark
	if err := producer.PublishMarkTick(ctx, rt.PerpSymbol, mark, indexPx, fundingEst, now.UnixMilli(), idx.Stale, idx.Degraded, rt.ConfigVersion); err != nil && ctx.Err() == nil {
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
		if err := producer.PublishFundingTick(ctx, rt.PerpSymbol, roundID, rate, rt.LastMark, now.UnixMilli(), rt.ConfigVersion); err != nil && ctx.Err() == nil {
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

	// Tolerant parse: tests construct Config directly with the zero value;
	// the production flag default is 20000.
	legacyImpact, _ := dec.Parse(cfg.ImpactNotional)
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
			ImpactNotional:  legacyImpact,
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
