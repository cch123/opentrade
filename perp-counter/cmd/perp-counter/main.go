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

	var markConsumer *journal.MarkPriceConsumer
	if len(brokers) > 0 {
		consumer, err = journal.NewTradeConsumer(journal.TradeConsumerConfig{
			Brokers:  brokers,
			ClientID: cfg.InstanceID,
			GroupID:  cfg.ConsumerGroup,
			Topic:    cfg.TradeTopic,
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
	logger.Warn("scope: order-event/trade-event wired; mark-price consume, liquidation execution, snapshot+HA, and M7 (BFF/push/trade-dump/history) pending later milestones")

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
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown", zap.Error(err))
	}
	_ = logger.Sync()
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
