// Command perp-counter is the account-truth service for USDT-margined linear
// perpetuals (ADR-0068 §2, A1: independent service alongside the spot Counter).
// It serves the Connect/h2c gRPC read+write paths and, when --brokers is set,
// is wired into Match over Kafka end to end: PlaceOrder/Cancel dispatch
// order-event to Match's perp deployment; perp-trade-event flows back into
// position settlement; perp-price drives unrealized PnL, funding, and
// liquidation; and state is snapshotted with bound offsets for recovery
// (ADR-0068 §1/§2/§5/§7/§8, ADR-0048).
//
// HA (ADR-0031 cold-standby): with --ha-mode=auto the instance competes for the
// shard's etcd leader key; only the primary runs the pipeline, losers idle.
// Failover safety rests on the snapshot+offset binding (the new primary restores
// then replays idempotently) and the transactional producer fencing
// (--transactional-id, ADR-0032). --ha-mode=disabled runs a single instance.
//
// With --brokers empty the service runs with no-op sinks (dev: read paths + the
// margin gate are live, nothing fills). The M7 access surface (BFF/push/
// trade-dump/history, asset-service futures holder) is a later milestone.
package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/api/gen/rpc/assetholder/assetholderrpcconnect"
	"github.com/xargin/opentrade/api/gen/rpc/perp/perprpcconnect"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/perp-counter/internal/journal"
	"github.com/xargin/opentrade/perp-counter/internal/perppricing"
	"github.com/xargin/opentrade/perp-counter/internal/server"
	"github.com/xargin/opentrade/perp-counter/internal/service"
	"github.com/xargin/opentrade/perp-counter/internal/snapshot"
	"github.com/xargin/opentrade/perp-counter/internal/tradeevent"
	"github.com/xargin/opentrade/pkg/connectx"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/election"
	"github.com/xargin/opentrade/pkg/idgen"
	"github.com/xargin/opentrade/pkg/logx"
	"github.com/xargin/opentrade/pkg/perpcfg"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// Config holds the perp-counter CLI flags.
type Config struct {
	InstanceID         string
	GRPCAddr           string
	DefaultMMR         string
	MaxLeverage        string
	RiskTiers          string
	LiqFeeRate         string
	TargetMarginBuffer string
	AutoAddTrigger     string
	AutoAddTarget      string
	AutoAddMaxPerEvent string
	BackstopAccount    uint64
	BackstopAfterTicks int
	VShardCount        int
	RiskCoordinator    bool
	IDGenShard         int
	Env                string
	LogLevel           string

	// Kafka (ADR-0068 §1/§2/§5). Empty Brokers = no-op sinks (dev).
	Brokers               string
	OrderEventTopicPrefix string
	JournalTopic          string
	TradeTopic            string
	ConsumerGroup         string
	TransactionalID       string
	MarkPriceTopic        string
	MarkPriceGroup        string

	// ADR-0075 SymbolConfig catalog. Empty DSN = legacy flag-driven config
	// (dev); set, the catalog is the admission authority and perp-counter
	// fails startup if the initial load fails (fail-closed).
	CatalogDSN          string
	CatalogPollInterval time.Duration
	CatalogMaxStaleness time.Duration

	// Snapshot persistence (ADR-0048 / ADR-0068 §5 invariant #5).
	SnapshotPath     string
	SnapshotInterval time.Duration

	// HA (ADR-0031 cold-standby).
	HAMode          string
	EtcdEndpoints   string
	ElectionPath    string
	LeaseTTL        int
	CampaignBackoff time.Duration
}

// deps are the parsed, process-lifetime dependencies passed to each primary
// cycle.
type deps struct {
	mmr            dec.Decimal
	maxLev         dec.Decimal
	liqFeeRate     dec.Decimal
	targetBuffer   dec.Decimal
	autoAddTrigger dec.Decimal
	autoAddTarget  dec.Decimal
	autoAddMax     dec.Decimal
	riskTiers      []perpstate.RiskTier
	idg            *idgen.Generator
}

func main() {
	cfg := parseFlags()

	logger, err := logx.New(logx.Config{Service: "perp-counter", Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	logx.SetGlobal(logger)

	mmr, err := dec.Parse(cfg.DefaultMMR)
	if err != nil {
		logger.Fatal("invalid --default-mmr", zap.Error(err))
	}
	maxLev, err := dec.Parse(cfg.MaxLeverage)
	if err != nil {
		logger.Fatal("invalid --max-leverage", zap.Error(err))
	}
	liqFeeRate, err := dec.Parse(cfg.LiqFeeRate)
	if err != nil {
		logger.Fatal("invalid --liq-fee-rate", zap.Error(err))
	}
	targetBuffer, err := dec.Parse(cfg.TargetMarginBuffer)
	if err != nil {
		logger.Fatal("invalid --target-margin-buffer", zap.Error(err))
	}
	autoAddTrigger, err := dec.Parse(cfg.AutoAddTrigger)
	if err != nil {
		logger.Fatal("invalid --auto-add-trigger-buffer", zap.Error(err))
	}
	autoAddTarget, err := dec.Parse(cfg.AutoAddTarget)
	if err != nil {
		logger.Fatal("invalid --auto-add-target-buffer", zap.Error(err))
	}
	autoAddMax, err := dec.Parse(cfg.AutoAddMaxPerEvent)
	if err != nil {
		logger.Fatal("invalid --auto-add-max-per-event", zap.Error(err))
	}
	riskTiers, err := parseRiskTiers(cfg.RiskTiers)
	if err != nil {
		logger.Fatal("invalid --risk-tiers", zap.Error(err))
	}
	if err := validateConfig(cfg); err != nil {
		logger.Fatal("invalid config", zap.Error(err))
	}
	idg, err := idgen.NewGenerator(cfg.IDGenShard)
	if err != nil {
		logger.Fatal("idgen", zap.Error(err))
	}
	d := deps{mmr: mmr, maxLev: maxLev, liqFeeRate: liqFeeRate, targetBuffer: targetBuffer,
		autoAddTrigger: autoAddTrigger, autoAddTarget: autoAddTarget, autoAddMax: autoAddMax,
		riskTiers: riskTiers, idg: idg}

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	etcd := splitCSV(cfg.EtcdEndpoints)
	if cfg.HAMode != "auto" || len(etcd) == 0 {
		runPrimary(rootCtx, cfg, d, logger)
		return
	}
	runElectionLoop(rootCtx, cfg, d, etcd, logger)
}

func validateConfig(cfg Config) error {
	if cfg.VShardCount > 1 && !cfg.RiskCoordinator {
		return errors.New("ADR-0071 guard: multi-vshard perp-counter requires --risk-coordinator-enabled so global insurance/ADL are not decided from shard-local state")
	}
	return nil
}

// runElectionLoop campaigns for the shard's leader key and runs the primary
// body for each leadership cycle (ADR-0031, mirrors match). Exits when rootCtx
// is cancelled.
func runElectionLoop(rootCtx context.Context, cfg Config, d deps, etcd []string, logger *zap.Logger) {
	elec, err := election.New(election.Config{
		Endpoints: etcd, Path: cfg.ElectionPath, Value: cfg.InstanceID, LeaseTTL: cfg.LeaseTTL,
	})
	if err != nil {
		logger.Fatal("election init", zap.Error(err))
	}
	defer func() { _ = elec.Close() }()

	for {
		if rootCtx.Err() != nil {
			return
		}
		logger.Info("campaigning for leadership", zap.String("path", cfg.ElectionPath))
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
		logger.Info("became primary", zap.String("instance", cfg.InstanceID))

		primaryCtx, cancelPrimary := context.WithCancel(rootCtx)
		watchDone := make(chan struct{})
		go func() {
			defer close(watchDone)
			select {
			case <-elec.LostCh():
				logger.Warn("lost leadership — demoting")
				cancelPrimary()
			case <-primaryCtx.Done():
			}
		}()

		runPrimary(primaryCtx, cfg, d, logger)
		cancelPrimary()
		<-watchDone

		if rootCtx.Err() == nil {
			logger.Info("demoted; re-campaigning")
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

// runPrimary brings up the whole pipeline and blocks until ctx is done. Invoked
// directly in HA-disabled mode and once per leadership cycle in auto mode. On
// promotion it restores from the snapshot; on shutdown/demotion it writes a
// final snapshot so the next owner resumes from a fresh point.
func runPrimary(ctx context.Context, cfg Config, d deps, logger *zap.Logger) {
	eng := engine.New()

	// ADR-0075: load the symbol catalog before anything serves. A failed
	// initial load aborts startup — running with an empty cache would
	// fail-closed every admission while hiding the store outage.
	var catalog *perpcfg.Cache
	if cfg.CatalogDSN != "" {
		store, err := perpcfg.OpenMySQLStore(perpcfg.MySQLConfig{DSN: cfg.CatalogDSN})
		if err != nil {
			logger.Error("perp catalog store", zap.Error(err))
			return
		}
		defer func() { _ = store.Close() }()
		catalog = perpcfg.NewCache(perpcfg.CacheConfig{
			Store:        store,
			PollInterval: cfg.CatalogPollInterval,
			MaxStaleness: cfg.CatalogMaxStaleness,
			Logger:       logger,
		})
		loadCtx, cancelLoad := context.WithTimeout(ctx, 10*time.Second)
		err = catalog.Load(loadCtx)
		cancelLoad()
		if err != nil {
			logger.Error("perp catalog initial load", zap.Error(err))
			return
		}
		go catalog.Run(ctx)
		logger.Info("perp catalog loaded (ADR-0075)",
			zap.Strings("symbols", catalog.Symbols()),
			zap.Duration("poll", cfg.CatalogPollInterval),
			zap.Duration("max_staleness", cfg.CatalogMaxStaleness))
	}

	// Restore engine state; the service order store + bound offsets are
	// restored after the service is built (ADR-0048).
	var restored *snapshot.PerpSnapshot
	if cfg.SnapshotPath != "" {
		snap, ok, err := snapshot.Load(cfg.SnapshotPath)
		if err != nil {
			logger.Error("load snapshot", zap.String("path", cfg.SnapshotPath), zap.Error(err))
			return
		}
		if ok {
			eng.Restore(snap.Engine)
			restored = &snap
			logger.Info("restored engine state", zap.Int64("ts_unix_ms", snap.TsUnixMs))
		}
	}

	var (
		dispatch service.Dispatcher
		jrnl     service.Journal
		producer *journal.Producer
		err      error
	)
	brokers := splitCSV(cfg.Brokers)
	if len(brokers) > 0 {
		producer, err = journal.NewProducer(journal.ProducerConfig{
			Brokers:               brokers,
			ClientID:              cfg.InstanceID,
			OrderEventTopicPrefix: cfg.OrderEventTopicPrefix,
			JournalTopic:          cfg.JournalTopic,
			TransactionalID:       cfg.TransactionalID,
		}, logger)
		if err != nil {
			logger.Error("perp producer", zap.Error(err))
			return
		}
		defer producer.Close()
		dispatch, jrnl = producer, producer
	} else {
		logger.Warn("no --brokers: running with no-op sinks (PlaceOrder reserves margin but nothing dispatches/fills)")
	}

	svc := service.New(eng, dispatch, jrnl, d.idg.Next, service.Config{
		ShardID: cfg.IDGenShard, ProducerID: cfg.InstanceID,
		MaxLeverage: d.maxLev, MMR: d.mmr, RiskTiers: d.riskTiers,
		LiquidationFeeRate: d.liqFeeRate, TargetMarginBuffer: d.targetBuffer,
		BackstopAccount: cfg.BackstopAccount, BackstopAfterTicks: cfg.BackstopAfterTicks,
		RiskCoordinatorEnabled: cfg.RiskCoordinator,
		AutoAddTriggerBuffer:   d.autoAddTrigger, AutoAddTargetBuffer: d.autoAddTarget,
		AutoAddMaxPerEvent: d.autoAddMax,
		Catalog:            catalog,
	})
	if catalog != nil {
		// Rebuild the precomputed liq-price index whenever any symbol's
		// active config version changes (publish or effective boundary).
		go svc.RunCatalogRefresh(ctx, cfg.CatalogPollInterval)
	}
	if restored != nil {
		svc.Restore(restored.Service)
		logger.Info("restored service state",
			zap.Int("orders", len(restored.Service.Orders)),
			zap.Int("offset_partitions", len(restored.Service.Offsets)))
	}

	var (
		consumer     *tradeevent.Consumer
		markConsumer *perppricing.Consumer
	)
	if len(brokers) > 0 {
		consumer, err = tradeevent.NewConsumer(tradeevent.ConsumerConfig{
			Brokers:        brokers,
			ClientID:       cfg.InstanceID,
			GroupID:        cfg.ConsumerGroup,
			Topic:          cfg.TradeTopic,
			InitialOffsets: svc.ConsumedOffsets(), // seek to the snapshot's bound offsets
		}, svc, logger)
		if err != nil {
			logger.Error("perp trade consumer", zap.Error(err))
			return
		}
		defer consumer.Close()

		markConsumer, err = perppricing.NewConsumer(perppricing.ConsumerConfig{
			Brokers:  brokers,
			ClientID: cfg.InstanceID + "-mark",
			GroupID:  cfg.MarkPriceGroup,
			Topic:    cfg.MarkPriceTopic,
		}, svc, logger)
		if err != nil {
			logger.Error("perp-price consumer", zap.Error(err))
			return
		}
		defer markConsumer.Close()
	}

	mux := http.NewServeMux()
	rpcPath, handler := perprpcconnect.NewPerpServiceHandler(server.New(eng, svc, d.mmr))
	mux.Handle(rpcPath, handler)
	// biz_line=futures AssetHolder (ADR-0057): funding→futures margin deposits.
	holderPath, holderHandler := assetholderrpcconnect.NewAssetHolderHandler(server.NewAssetHolderServer(svc))
	mux.Handle(holderPath, holderHandler)
	if cfg.RiskCoordinator {
		server.RegisterRiskHandlers(mux, svc)
	}
	httpSrv := connectx.NewH2CServer(cfg.GRPCAddr, mux)

	logger.Info("perp-counter primary up (ADR-0068)",
		zap.String("grpc", cfg.GRPCAddr), zap.Strings("brokers", brokers),
		zap.String("trade_topic", cfg.TradeTopic), zap.String("mark_topic", cfg.MarkPriceTopic),
		zap.Bool("transactional", cfg.TransactionalID != ""), zap.String("ha", cfg.HAMode),
		zap.Int("vshard_count", cfg.VShardCount), zap.Bool("risk_coordinator", cfg.RiskCoordinator))

	if cfg.SnapshotPath != "" {
		if err := snapshot.EnsureDir(cfg.SnapshotPath); err != nil {
			logger.Error("snapshot dir", zap.Error(err))
			return
		}
		go runSnapshotLoop(ctx, cfg, svc, producer, logger)
	}

	srvErr := make(chan error, 1)
	go func() {
		logger.Info("gRPC (Connect/h2c) listening", zap.String("addr", cfg.GRPCAddr))
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			srvErr <- err
		}
	}()
	var consumerWG sync.WaitGroup
	if consumer != nil {
		consumerWG.Add(1)
		go func() {
			defer consumerWG.Done()
			if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("trade consumer exited", zap.Error(err))
			}
		}()
	}
	if markConsumer != nil {
		consumerWG.Add(1)
		go func() {
			defer consumerWG.Done()
			if err := markConsumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("perp-price consumer exited", zap.Error(err))
			}
		}()
	}

	select {
	case <-ctx.Done():
	case err := <-srvErr:
		logger.Error("grpc serve", zap.Error(err))
	}

	logger.Info("primary shutting down")
	if consumer != nil {
		consumer.Close()
	}
	if markConsumer != nil {
		markConsumer.Close()
	}
	consumerWG.Wait()

	// Final snapshot once the consumers have stopped mutating state.
	if cfg.SnapshotPath != "" {
		if err := saveSnapshot(cfg, svc, producer); err != nil {
			logger.Error("final snapshot", zap.Error(err))
		} else {
			logger.Info("wrote final snapshot", zap.String("path", cfg.SnapshotPath))
		}
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown", zap.Error(err))
	}
}

// runSnapshotLoop periodically captures + persists state (ADR-0048). Each tick
// flushes the producer under the service capture barrier, so the bound offsets
// never run ahead of durably-emitted journal / order-event output.
func runSnapshotLoop(ctx context.Context, cfg Config, svc *service.Service, producer *journal.Producer, logger *zap.Logger) {
	if cfg.SnapshotInterval <= 0 {
		return
	}
	ticker := time.NewTicker(cfg.SnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := saveSnapshot(cfg, svc, producer); err != nil {
				logger.Error("periodic snapshot", zap.Error(err))
			}
		}
	}
}

// saveSnapshot captures the engine + service image (flushing the producer first)
// and atomically writes it to disk.
func saveSnapshot(cfg Config, svc *service.Service, producer *journal.Producer) error {
	var flush func() error
	if producer != nil {
		flush = func() error {
			fctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return producer.Flush(fctx)
		}
	}
	engSnap, svcSnap, err := svc.Capture(flush)
	if err != nil {
		return err
	}
	return snapshot.Save(cfg.SnapshotPath, snapshot.PerpSnapshot{
		TsUnixMs: time.Now().UnixMilli(), Engine: engSnap, Service: svcSnap,
	})
}

func parseFlags() Config {
	var cfg Config
	flag.StringVar(&cfg.InstanceID, "instance-id", "perp-counter-0", "instance id (client id / producer id / consumer group suffix)")
	flag.StringVar(&cfg.GRPCAddr, "grpc-addr", ":8086", "gRPC (Connect/h2c) listen address")
	flag.StringVar(&cfg.DefaultMMR, "default-mmr", "0.005",
		"maintenance margin rate for liquidation + the derived liq price (ADR-0068; per-symbol override is M6)")
	flag.StringVar(&cfg.MaxLeverage, "max-leverage", "125", "max leverage accepted at PlaceOrder (0 = no cap)")
	flag.StringVar(&cfg.RiskTiers, "risk-tiers", "",
		"ADR-0070 risk tiers as cap:mmr:max_leverage:liq_fee_rate CSV; cap=0 means open-ended")
	flag.StringVar(&cfg.LiqFeeRate, "liq-fee-rate", "0", "fallback liquidation fee rate credited to insurance")
	flag.StringVar(&cfg.TargetMarginBuffer, "target-margin-buffer", "0", "partial liquidation target buffer added above tier MMR")
	flag.StringVar(&cfg.AutoAddTrigger, "auto-add-trigger-buffer", "0", "ADR-0074 auto-add fires at MMR+buffer ratio (0 = service default 0.005)")
	flag.StringVar(&cfg.AutoAddTarget, "auto-add-target-buffer", "0", "ADR-0074 auto-add tops margin up to MMR+buffer ratio (0 = service default 0.01)")
	flag.StringVar(&cfg.AutoAddMaxPerEvent, "auto-add-max-per-event", "0", "platform cap per auto-add transfer (0 = uncapped)")
	flag.Uint64Var(&cfg.BackstopAccount, "backstop-account", 0, "system user id that receives internal backstop inventory")
	flag.IntVar(&cfg.BackstopAfterTicks, "backstop-after-ticks", 2, "mark ticks to wait before escalating an in-flight liquidation to backstop")
	flag.IntVar(&cfg.VShardCount, "vshard-count", 1, "perp-counter user vshard count; values >1 require --risk-coordinator-enabled (ADR-0071)")
	flag.BoolVar(&cfg.RiskCoordinator, "risk-coordinator-enabled", false, "disable shard-local ADL decisions because perp-risk owns global insurance/ADL (ADR-0071)")
	flag.IntVar(&cfg.IDGenShard, "idgen-shard", 0, "snowflake shard id for perp order ids (avoid collisions with counter)")
	flag.StringVar(&cfg.Env, "env", "dev", "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", "info", "log level")

	flag.StringVar(&cfg.Brokers, "brokers", "", "comma-separated Kafka brokers; empty runs with no-op sinks (no order dispatch / trade consume)")
	flag.StringVar(&cfg.OrderEventTopicPrefix, "order-event-topic-prefix", "order-event",
		"per-symbol order-event topic prefix (ADR-0050); a perp order routes to `<prefix>-<symbol>`")
	flag.StringVar(&cfg.JournalTopic, "journal-topic", "perp-journal", "perp-journal WAL topic (ADR-0068 §2)")
	flag.StringVar(&cfg.TradeTopic, "trade-topic", "perp-trade-event", "perp trade-event topic consumed from Match (ADR-0068 §0 physical isolation)")
	flag.StringVar(&cfg.ConsumerGroup, "group", "perp-counter", "Kafka consumer group for perp-trade-event (stable across instances so partitions balance)")
	flag.StringVar(&cfg.TransactionalID, "transactional-id", "", "stable Kafka transactional id for producer fencing (ADR-0032); empty = idempotent (dev). Set per shard in HA mode.")
	flag.StringVar(&cfg.MarkPriceTopic, "perp-price-topic", "perp-price", "perp-price topic consumed from perp-pricing (ADR-0068 §5)")
	flag.StringVar(&cfg.MarkPriceGroup, "perp-price-group", "perp-counter-mark", "Kafka consumer group for the perp-price stream")
	flag.StringVar(&cfg.CatalogDSN, "catalog-dsn", "", "MySQL DSN of the ADR-0075 perp symbol catalog; empty = legacy flag-driven config (dev)")
	flag.DurationVar(&cfg.CatalogPollInterval, "catalog-poll-interval", time.Second, "catalog anchor poll cadence")
	flag.DurationVar(&cfg.CatalogMaxStaleness, "catalog-max-staleness", 30*time.Second, "reject new orders when the catalog cache has not synced for this long (fail-closed)")
	flag.StringVar(&cfg.SnapshotPath, "snapshot-path", "./data/perp-counter/snapshot.json", "snapshot file path (state + bound offsets, ADR-0048); empty disables")
	flag.DurationVar(&cfg.SnapshotInterval, "snapshot-interval", 60*time.Second, "how often to snapshot state + offsets")

	flag.StringVar(&cfg.HAMode, "ha-mode", "disabled", "ha mode: disabled | auto (etcd leader election, ADR-0031)")
	flag.StringVar(&cfg.EtcdEndpoints, "etcd", "", "comma-separated etcd endpoints (required for --ha-mode=auto)")
	flag.StringVar(&cfg.ElectionPath, "election-path", "/cex/perp-counter/leader", "etcd election key (ADR-0031)")
	flag.IntVar(&cfg.LeaseTTL, "lease-ttl", 10, "etcd session TTL seconds")
	flag.DurationVar(&cfg.CampaignBackoff, "campaign-backoff", 2*time.Second, "wait between failed campaigns")
	flag.Parse()
	return cfg
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

func parseRiskTiers(raw string) ([]perpstate.RiskTier, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	parts := splitCSV(raw)
	tiers := make([]perpstate.RiskTier, 0, len(parts))
	for _, part := range parts {
		fields := strings.Split(part, ":")
		if len(fields) != 4 {
			return nil, errors.New("each tier must be cap:mmr:max_leverage:liq_fee_rate")
		}
		cap, err := dec.Parse(strings.TrimSpace(fields[0]))
		if err != nil {
			return nil, err
		}
		mmr, err := dec.Parse(strings.TrimSpace(fields[1]))
		if err != nil {
			return nil, err
		}
		maxLev, err := dec.Parse(strings.TrimSpace(fields[2]))
		if err != nil {
			return nil, err
		}
		fee, err := dec.Parse(strings.TrimSpace(fields[3]))
		if err != nil {
			return nil, err
		}
		// Keep validation local to CLI parsing so tests can still construct edge
		// models directly, while production flags fail before the service starts.
		if cap.Sign() < 0 || mmr.Sign() < 0 || maxLev.Sign() < 0 || fee.Sign() < 0 {
			return nil, errors.New("tier values must be non-negative")
		}
		tiers = append(tiers, perpstate.RiskTier{
			TierMaxNotional: cap, MaintMarginRatio: mmr, MaxLeverage: maxLev, LiqFeeRate: fee,
		})
	}
	return tiers, nil
}
