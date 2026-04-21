// Command trade-dump runs the OpenTrade persistence sidecar (ADR-0008),
// consuming trade-event + counter-journal + conditional-event +
// asset-journal from Kafka and idempotently projecting them onto MySQL.
//
//   - trade-event       → trades                                 (ADR-0023, MVP-5)
//   - counter-journal   → orders / accounts / account_logs       (ADR-0028, MVP-9)
//   - conditional-event → conditionals                           (ADR-0047)
//   - asset-journal     → funding_accounts / funding_account_logs /
//                         transfers                               (ADR-0057 M5)
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
	"github.com/xargin/opentrade/trade-dump/internal/consumer"
	"github.com/xargin/opentrade/trade-dump/internal/writer"
)

type Config struct {
	InstanceID string
	Brokers    []string

	TradeTopic         string
	TradeGroup         string
	JournalTopic       string
	JournalGroup       string
	ConditionalTopic   string
	ConditionalGroup   string
	AssetTopic         string
	AssetGroup         string

	MySQLDSN            string
	MySQLMaxOpenConns   int
	MySQLMaxIdleConns   int
	MySQLConnMaxLife    time.Duration
	MySQLInsertChunkMax int

	Env      string
	LogLevel string
}

func main() {
	cfg := parseFlags()

	logger, err := logx.New(logx.Config{Service: "trade-dump", Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	logx.SetGlobal(logger)

	if err := cfg.validate(); err != nil {
		logger.Fatal("invalid config", zap.Error(err))
	}

	logger.Info("trade-dump starting",
		zap.String("instance", cfg.InstanceID),
		zap.Strings("brokers", cfg.Brokers),
		zap.String("trade_topic", cfg.TradeTopic),
		zap.String("trade_group", cfg.TradeGroup),
		zap.String("journal_topic", cfg.JournalTopic),
		zap.String("journal_group", cfg.JournalGroup))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mysql, err := writer.NewMySQL(writer.MySQLConfig{
		DSN:             cfg.MySQLDSN,
		MaxOpenConns:    cfg.MySQLMaxOpenConns,
		MaxIdleConns:    cfg.MySQLMaxIdleConns,
		ConnMaxLifetime: cfg.MySQLConnMaxLife,
		ChunkSize:       cfg.MySQLInsertChunkMax,
	})
	if err != nil {
		logger.Fatal("mysql open", zap.Error(err))
	}
	defer func() { _ = mysql.Close() }()

	tradeCons, err := consumer.New(consumer.Config{
		Brokers:  cfg.Brokers,
		ClientID: cfg.InstanceID + "-trade",
		GroupID:  cfg.TradeGroup,
		Topic:    cfg.TradeTopic,
	}, mysql, logger)
	if err != nil {
		logger.Fatal("trade consumer init", zap.Error(err))
	}
	defer tradeCons.Close()

	journalCons, err := consumer.NewJournal(consumer.JournalConfig{
		Brokers:  cfg.Brokers,
		ClientID: cfg.InstanceID + "-journal",
		GroupID:  cfg.JournalGroup,
		Topic:    cfg.JournalTopic,
	}, mysql, logger)
	if err != nil {
		logger.Fatal("journal consumer init", zap.Error(err))
	}
	defer journalCons.Close()

	var condCons *consumer.ConditionalConsumer
	if cfg.ConditionalTopic != "" {
		condCons, err = consumer.NewConditional(consumer.ConditionalConfig{
			Brokers:  cfg.Brokers,
			ClientID: cfg.InstanceID + "-cond",
			GroupID:  cfg.ConditionalGroup,
			Topic:    cfg.ConditionalTopic,
		}, mysql, logger)
		if err != nil {
			logger.Fatal("conditional consumer init", zap.Error(err))
		}
		defer condCons.Close()
		logger.Info("conditional consumer enabled",
			zap.String("topic", cfg.ConditionalTopic),
			zap.String("group", cfg.ConditionalGroup))
	} else {
		logger.Info("conditional consumer disabled (empty --conditional-topic)")
	}

	var assetCons *consumer.AssetConsumer
	if cfg.AssetTopic != "" {
		assetCons, err = consumer.NewAsset(consumer.AssetConfig{
			Brokers:  cfg.Brokers,
			ClientID: cfg.InstanceID + "-asset",
			GroupID:  cfg.AssetGroup,
			Topic:    cfg.AssetTopic,
		}, mysql, logger)
		if err != nil {
			logger.Fatal("asset consumer init", zap.Error(err))
		}
		defer assetCons.Close()
		logger.Info("asset consumer enabled",
			zap.String("topic", cfg.AssetTopic),
			zap.String("group", cfg.AssetGroup))
	} else {
		logger.Info("asset consumer disabled (empty --asset-topic)")
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := tradeCons.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("trade consumer exited", zap.Error(err))
			stop()
		}
	}()
	go func() {
		defer wg.Done()
		if err := journalCons.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("journal consumer exited", zap.Error(err))
			stop()
		}
	}()
	if condCons != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := condCons.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("conditional consumer exited", zap.Error(err))
				stop()
			}
		}()
	}
	if assetCons != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := assetCons.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("asset consumer exited", zap.Error(err))
				stop()
			}
		}()
	}

	<-rootCtx.Done()
	logger.Info("shutdown initiated")
	tradeCons.Close()
	journalCons.Close()
	if condCons != nil {
		condCons.Close()
	}
	if assetCons != nil {
		assetCons.Close()
	}
	wg.Wait()
	logger.Info("trade-dump shutdown complete")
}

func parseFlags() Config {
	cfg := Config{
		InstanceID:        "trade-dump-0",
		TradeTopic:        "trade-event",
		JournalTopic:      "counter-journal",
		ConditionalTopic:  "conditional-event",
		AssetTopic:        "asset-journal",
		MySQLDSN:          "opentrade:opentrade@tcp(127.0.0.1:3306)/opentrade?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=true",
		MySQLMaxOpenConns: 16,
		MySQLMaxIdleConns: 4,
		MySQLConnMaxLife:  30 * time.Minute,
		Env:               "dev",
		LogLevel:          "info",
	}
	var brokersStr string
	flag.StringVar(&cfg.InstanceID, "instance-id", cfg.InstanceID, "instance id (client id prefix)")
	flag.StringVar(&brokersStr, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flag.StringVar(&cfg.TradeTopic, "trade-topic", cfg.TradeTopic, "trade-event topic name")
	flag.StringVar(&cfg.TradeGroup, "trade-group", "", "trade-event consumer group (default trade-dump-trade-{instance-id})")
	flag.StringVar(&cfg.JournalTopic, "journal-topic", cfg.JournalTopic, "counter-journal topic name")
	flag.StringVar(&cfg.JournalGroup, "journal-group", "", "counter-journal consumer group (default trade-dump-journal-{instance-id})")
	flag.StringVar(&cfg.ConditionalTopic, "conditional-topic", cfg.ConditionalTopic, "conditional-event topic name (empty disables projection; ADR-0047)")
	flag.StringVar(&cfg.ConditionalGroup, "conditional-group", "", "conditional-event consumer group (default trade-dump-cond-{instance-id})")
	flag.StringVar(&cfg.AssetTopic, "asset-topic", cfg.AssetTopic, "asset-journal topic name (empty disables projection; ADR-0057)")
	flag.StringVar(&cfg.AssetGroup, "asset-group", "", "asset-journal consumer group (default trade-dump-asset-{instance-id})")
	flag.StringVar(&cfg.MySQLDSN, "mysql-dsn", cfg.MySQLDSN, "MySQL DSN")
	flag.IntVar(&cfg.MySQLMaxOpenConns, "mysql-max-open", cfg.MySQLMaxOpenConns, "MySQL max open connections")
	flag.IntVar(&cfg.MySQLMaxIdleConns, "mysql-max-idle", cfg.MySQLMaxIdleConns, "MySQL max idle connections")
	flag.DurationVar(&cfg.MySQLConnMaxLife, "mysql-conn-life", cfg.MySQLConnMaxLife, "MySQL connection max lifetime")
	flag.IntVar(&cfg.MySQLInsertChunkMax, "mysql-chunk", cfg.MySQLInsertChunkMax, "rows per multi-row INSERT chunk (default 500)")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	cfg.Brokers = splitCSV(brokersStr)
	if cfg.TradeGroup == "" {
		cfg.TradeGroup = "trade-dump-trade-" + cfg.InstanceID
	}
	if cfg.JournalGroup == "" {
		cfg.JournalGroup = "trade-dump-journal-" + cfg.InstanceID
	}
	if cfg.ConditionalGroup == "" {
		cfg.ConditionalGroup = "trade-dump-cond-" + cfg.InstanceID
	}
	if cfg.AssetGroup == "" {
		cfg.AssetGroup = "trade-dump-asset-" + cfg.InstanceID
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
	if c.MySQLDSN == "" {
		return fmt.Errorf("mysql-dsn required")
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
