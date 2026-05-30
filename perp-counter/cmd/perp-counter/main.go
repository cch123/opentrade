// Command perp-counter is the account-truth service for USDT-margined linear
// perpetuals (ADR-0068 §2, A1: independent service alongside the spot
// Counter). The Connect/h2c gRPC server serves the read + write paths, and —
// when --brokers is set — perp-counter is wired into Match over Kafka:
// PlaceOrder/Cancel dispatch order-event records to Match's perp deployment and
// the perp-trade-event stream flows back into position settlement (ADR-0068 §1).
//
// mark-price consumption (markprice → perp-counter), the liquidation execution
// flow, snapshot persistence + HA, and the M7 access surface (BFF/push/
// trade-dump/history) are later milestones. With --brokers empty the service
// runs with no-op sinks (dev: read paths + the margin gate are live, nothing
// fills).
package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/api/gen/rpc/perp/perprpcconnect"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/perp-counter/internal/journal"
	"github.com/xargin/opentrade/perp-counter/internal/server"
	"github.com/xargin/opentrade/perp-counter/internal/service"
	"github.com/xargin/opentrade/perp-counter/internal/snapshot"
	"github.com/xargin/opentrade/pkg/connectx"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/idgen"
	"github.com/xargin/opentrade/pkg/logx"
)

// Config holds the perp-counter CLI flags.
type Config struct {
	InstanceID  string
	GRPCAddr    string
	DefaultMMR  string
	MaxLeverage string
	IDGenShard  int
	Env         string
	LogLevel    string

	// Kafka (ADR-0068 §1/§2/§5). Empty Brokers = no-op sinks (dev).
	Brokers               string
	OrderEventTopicPrefix string
	JournalTopic          string
	TradeTopic            string
	ConsumerGroup         string
	TransactionalID       string
	MarkPriceTopic        string
	MarkPriceGroup        string

	// Snapshot persistence (ADR-0048 / ADR-0068 §5 invariant #5).
	SnapshotPath     string
	SnapshotInterval time.Duration
}

func main() {
	var cfg Config
	flag.StringVar(&cfg.InstanceID, "instance-id", "perp-counter-0", "instance id (client id / producer id / consumer group suffix)")
	flag.StringVar(&cfg.GRPCAddr, "grpc-addr", ":8086", "gRPC (Connect/h2c) listen address")
	flag.StringVar(&cfg.DefaultMMR, "default-mmr", "0.005",
		"default maintenance margin rate for the derived liq price (ADR-0068; per-symbol override is M6)")
	flag.StringVar(&cfg.MaxLeverage, "max-leverage", "125", "max leverage accepted at PlaceOrder (0 = no cap)")
	flag.IntVar(&cfg.IDGenShard, "idgen-shard", 0, "snowflake shard id for perp order ids (avoid collisions with counter)")
	flag.StringVar(&cfg.Env, "env", "dev", "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", "info", "log level")

	flag.StringVar(&cfg.Brokers, "brokers", "", "comma-separated Kafka brokers; empty runs with no-op sinks (no order dispatch / trade consume)")
	flag.StringVar(&cfg.OrderEventTopicPrefix, "order-event-topic-prefix", "order-event",
		"per-symbol order-event topic prefix (ADR-0050); a perp order routes to `<prefix>-<symbol>`")
	flag.StringVar(&cfg.JournalTopic, "journal-topic", "perp-journal", "perp-journal WAL topic (ADR-0068 §2)")
	flag.StringVar(&cfg.TradeTopic, "trade-topic", "perp-trade-event", "perp trade-event topic consumed from Match (ADR-0068 §0 physical isolation)")
	flag.StringVar(&cfg.ConsumerGroup, "group", "perp-counter", "Kafka consumer group for perp-trade-event (stable across instances so partitions balance)")
	flag.StringVar(&cfg.TransactionalID, "transactional-id", "", "stable Kafka transactional id for producer fencing (ADR-0032); empty = idempotent (dev)")
	flag.StringVar(&cfg.MarkPriceTopic, "mark-price-topic", "mark-price", "mark-price topic consumed from markprice (ADR-0068 §5)")
	flag.StringVar(&cfg.MarkPriceGroup, "mark-price-group", "perp-counter-mark", "Kafka consumer group for the mark-price stream")
	flag.StringVar(&cfg.SnapshotPath, "snapshot-path", "./data/perp-counter/snapshot.json", "snapshot file path (state + bound offsets, ADR-0048); empty disables")
	flag.DurationVar(&cfg.SnapshotInterval, "snapshot-interval", 60*time.Second, "how often to snapshot state + offsets")
	flag.Parse()

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
	idg, err := idgen.NewGenerator(cfg.IDGenShard)
	if err != nil {
		logger.Fatal("idgen", zap.Error(err))
	}

	eng := engine.New()

	// Restore engine state from the last snapshot (ADR-0048). The service order
	// store + bound offsets are restored after the service is built, below.
	var restored *snapshot.PerpSnapshot
	if cfg.SnapshotPath != "" {
		snap, ok, err := snapshot.Load(cfg.SnapshotPath)
		if err != nil {
			logger.Fatal("load snapshot", zap.String("path", cfg.SnapshotPath), zap.Error(err))
		}
		if ok {
			eng.Restore(snap.Engine)
			restored = &snap
			logger.Info("restored engine state from snapshot",
				zap.String("path", cfg.SnapshotPath), zap.Int64("ts_unix_ms", snap.TsUnixMs))
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- Kafka wiring (ADR-0068 §1/§2) -----------------------------------
	// When --brokers is set, the producer dispatches order-event to Match and
	// emits the perp-journal WAL; the consumer feeds perp-trade-event back into
	// settlement. Empty brokers keep the no-op sinks so dev runs without Kafka.
	var (
		dispatch service.Dispatcher
		jrnl     service.Journal
		producer *journal.Producer
		consumer *journal.TradeConsumer
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
			logger.Fatal("perp producer", zap.Error(err))
		}
		defer producer.Close()
		dispatch, jrnl = producer, producer
	} else {
		logger.Warn("no --brokers: running with no-op sinks (PlaceOrder reserves margin but nothing dispatches/fills)")
	}

	svc := service.New(eng, dispatch, jrnl, idg.Next, service.Config{
		ShardID: cfg.IDGenShard, ProducerID: cfg.InstanceID, MaxLeverage: maxLev, MMR: mmr,
	})
	if restored != nil {
		svc.Restore(restored.Service)
		logger.Info("restored service state from snapshot",
			zap.Int("orders", len(restored.Service.Orders)),
			zap.Int("offset_partitions", len(restored.Service.Offsets)))
	}

	var markConsumer *journal.MarkPriceConsumer
	if len(brokers) > 0 {
		consumer, err = journal.NewTradeConsumer(journal.TradeConsumerConfig{
			Brokers:        brokers,
			ClientID:       cfg.InstanceID,
			GroupID:        cfg.ConsumerGroup,
			Topic:          cfg.TradeTopic,
			InitialOffsets: svc.ConsumedOffsets(), // seek to the snapshot's bound offsets
		}, svc, logger)
		if err != nil {
			logger.Fatal("perp trade consumer", zap.Error(err))
		}
		defer consumer.Close()

		markConsumer, err = journal.NewMarkPriceConsumer(journal.MarkPriceConsumerConfig{
			Brokers:  brokers,
			ClientID: cfg.InstanceID + "-mark",
			GroupID:  cfg.MarkPriceGroup,
			Topic:    cfg.MarkPriceTopic,
		}, svc, logger)
		if err != nil {
			logger.Fatal("mark-price consumer", zap.Error(err))
		}
		defer markConsumer.Close()
	}

	mux := http.NewServeMux()
	path, handler := perprpcconnect.NewPerpServiceHandler(server.New(eng, svc, mmr))
	mux.Handle(path, handler)
	httpSrv := connectx.NewH2CServer(cfg.GRPCAddr, mux)

	logger.Info("perp-counter starting (ADR-0068)",
		zap.String("grpc", cfg.GRPCAddr), zap.String("default_mmr", cfg.DefaultMMR),
		zap.Strings("brokers", brokers), zap.String("order_topic_prefix", cfg.OrderEventTopicPrefix),
		zap.String("trade_topic", cfg.TradeTopic), zap.String("journal_topic", cfg.JournalTopic),
		zap.Bool("transactional", cfg.TransactionalID != ""))
	logger.Warn("scope: order-event/trade-event + mark-price/funding/liquidation + snapshot wired; cold-standby HA and M7 (BFF/push/trade-dump/history) pending later milestones")

	if cfg.SnapshotPath != "" {
		if err := snapshot.EnsureDir(cfg.SnapshotPath); err != nil {
			logger.Fatal("snapshot dir", zap.Error(err))
		}
		go runSnapshotLoop(ctx, cfg, svc, producer, logger)
	}

	go func() {
		logger.Info("gRPC (Connect/h2c) listening", zap.String("addr", cfg.GRPCAddr))
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("grpc serve", zap.Error(err))
			stop()
		}
	}()

	if consumer != nil {
		go func() {
			logger.Info("perp-trade-event consumer starting",
				zap.String("topic", cfg.TradeTopic), zap.String("group", cfg.ConsumerGroup))
			if err := consumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("trade consumer exited", zap.Error(err))
				stop()
			}
		}()
	}
	if markConsumer != nil {
		go func() {
			logger.Info("mark-price consumer starting",
				zap.String("topic", cfg.MarkPriceTopic), zap.String("group", cfg.MarkPriceGroup))
			if err := markConsumer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("mark-price consumer exited", zap.Error(err))
				stop()
			}
		}()
	}

	<-ctx.Done()
	logger.Info("perp-counter shutting down")
	if consumer != nil {
		consumer.Close()
	}
	if markConsumer != nil {
		markConsumer.Close()
	}
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
		logger.Error("shutdown", zap.Error(err))
	}
	_ = logger.Sync()
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
