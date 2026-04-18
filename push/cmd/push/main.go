// Command push runs the OpenTrade WebSocket push service.
//
// MVP-7 is a single-instance gateway that:
//   - accepts authenticated WS connections (X-User-Id header, matching BFF),
//   - subscribes connections to public streams and the implicit "user" stream,
//   - consumes market-data for public fan-out,
//   - consumes counter-journal for per-user private delivery.
//
// Multi-instance sticky routing (ADR-0022) and client snapshot-on-reconnect
// are deferred to a later MVP; see ADR-0026.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/pkg/logx"
	"github.com/xargin/opentrade/push/internal/consumer"
	"github.com/xargin/opentrade/push/internal/hub"
	"github.com/xargin/opentrade/push/internal/ws"
)

type Config struct {
	InstanceID       string
	InstanceOrdinal  int // numeric shard position 0..TotalInstances-1 (ADR-0033)
	TotalInstances   int // number of push instances in the cluster; 1 disables sticky
	HTTPAddr         string
	Brokers          []string
	MarketDataTopic  string
	CounterJournalTopic string
	MarketGroupID    string
	PrivateGroupID   string
	SendBuffer       int
	WriteTimeout     time.Duration

	// Per-connection outbound rate limit (ADR-0037). Defaults are generous
	// enough that a normal client never trips them; an abusive client or a
	// wedged write loop gets dropped frames with a log line.
	MessageRate  float64
	MessageBurst float64

	Env              string
	LogLevel         string
}

func main() {
	cfg := parseFlags()

	logger, err := logx.New(logx.Config{Service: "push", Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	logx.SetGlobal(logger)

	if err := cfg.validate(); err != nil {
		logger.Fatal("invalid config", zap.Error(err))
	}

	logger.Info("push starting",
		zap.String("instance", cfg.InstanceID),
		zap.Int("ordinal", cfg.InstanceOrdinal),
		zap.Int("total_instances", cfg.TotalInstances),
		zap.String("http", cfg.HTTPAddr),
		zap.Strings("brokers", cfg.Brokers),
		zap.String("market_topic", cfg.MarketDataTopic),
		zap.String("counter_topic", cfg.CounterJournalTopic))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	h := hub.New(logger)

	mdCons, err := consumer.NewMarketData(consumer.MarketDataConfig{
		Brokers: cfg.Brokers, ClientID: cfg.InstanceID,
		GroupID: cfg.MarketGroupID, Topic: cfg.MarketDataTopic,
	}, h, logger)
	if err != nil {
		logger.Fatal("market-data consumer init", zap.Error(err))
	}
	defer mdCons.Close()

	privCons, err := consumer.NewPrivate(consumer.PrivateConfig{
		Brokers: cfg.Brokers, ClientID: cfg.InstanceID,
		InstanceOrdinal: cfg.InstanceOrdinal,
		TotalInstances:  cfg.TotalInstances,
		GroupID: cfg.PrivateGroupID, Topic: cfg.CounterJournalTopic,
	}, h, logger)
	if err != nil {
		logger.Fatal("private consumer init", zap.Error(err))
	}
	defer privCons.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", ws.Handler(rootCtx, h, ws.Config{
		SendBuffer:      cfg.SendBuffer,
		WriteTimeout:    cfg.WriteTimeout,
		MessageRate:     cfg.MessageRate,
		MessageBurst:    cfg.MessageBurst,
		InstanceOrdinal: cfg.InstanceOrdinal,
		TotalInstances:  cfg.TotalInstances,
	}, logger))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: mux,
	}

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		if err := mdCons.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("market-data consumer exited", zap.Error(err))
			stop()
		}
	}()
	go func() {
		defer wg.Done()
		if err := privCons.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("private consumer exited", zap.Error(err))
			stop()
		}
	}()
	go func() {
		defer wg.Done()
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server exited", zap.Error(err))
			stop()
		}
	}()

	<-rootCtx.Done()
	logger.Info("shutdown initiated")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	mdCons.Close()
	privCons.Close()
	wg.Wait()
	logger.Info("push shutdown complete")
}

func parseFlags() Config {
	cfg := Config{
		InstanceID:          "push-0",
		InstanceOrdinal:     0,
		TotalInstances:      1,
		HTTPAddr:            ":8081",
		MarketDataTopic:     "market-data",
		CounterJournalTopic: "counter-journal",
		SendBuffer:          256,
		WriteTimeout:        10 * time.Second,
		MessageRate:         2000,
		MessageBurst:        4000,
		Env:                 "dev",
		LogLevel:            "info",
	}
	var brokersStr string
	flag.StringVar(&cfg.InstanceID, "instance-id", cfg.InstanceID, "instance id (client id / default group suffix)")
	flag.IntVar(&cfg.InstanceOrdinal, "instance-ordinal", cfg.InstanceOrdinal, "numeric position in the push cluster 0..total-1 (ADR-0033)")
	flag.IntVar(&cfg.TotalInstances, "total-instances", cfg.TotalInstances, "total push instances; 1 disables sticky filtering")
	flag.StringVar(&cfg.HTTPAddr, "http", cfg.HTTPAddr, "HTTP bind address (serves /ws and /healthz)")
	flag.StringVar(&brokersStr, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flag.StringVar(&cfg.MarketDataTopic, "market-topic", cfg.MarketDataTopic, "market-data topic")
	flag.StringVar(&cfg.CounterJournalTopic, "counter-topic", cfg.CounterJournalTopic, "counter-journal topic")
	flag.StringVar(&cfg.MarketGroupID, "market-group", "", "market-data consumer group (default push-md-{instance-id})")
	flag.StringVar(&cfg.PrivateGroupID, "private-group", "", "counter-journal consumer group (default push-priv-{instance-id})")
	flag.IntVar(&cfg.SendBuffer, "send-buffer", cfg.SendBuffer, "per-connection outbound queue depth")
	flag.DurationVar(&cfg.WriteTimeout, "write-timeout", cfg.WriteTimeout, "per-write deadline on WS")
	flag.Float64Var(&cfg.MessageRate, "msg-rate", cfg.MessageRate, "per-connection outbound rate (msgs/s); 0 disables")
	flag.Float64Var(&cfg.MessageBurst, "msg-burst", cfg.MessageBurst, "per-connection outbound burst capacity (msgs)")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	cfg.Brokers = splitCSV(brokersStr)
	if cfg.MarketGroupID == "" {
		cfg.MarketGroupID = "push-md-" + cfg.InstanceID
	}
	if cfg.PrivateGroupID == "" {
		cfg.PrivateGroupID = "push-priv-" + cfg.InstanceID
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
	if c.HTTPAddr == "" {
		return fmt.Errorf("http address required")
	}
	if c.TotalInstances < 1 {
		return fmt.Errorf("total-instances must be >= 1")
	}
	if c.InstanceOrdinal < 0 || c.InstanceOrdinal >= c.TotalInstances {
		return fmt.Errorf("instance-ordinal %d must be in [0, %d)", c.InstanceOrdinal, c.TotalInstances)
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
