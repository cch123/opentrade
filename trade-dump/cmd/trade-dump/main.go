// Command trade-dump runs the OpenTrade persistence sidecar (ADR-0008),
// consuming trade-event from Kafka and idempotently writing Trade payloads to
// MySQL.
//
// MVP-5 scope: trade-event only, single trades table. counter-journal
// projection (orders / account_logs) is deferred to a later MVP.
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
	InstanceID    string
	Brokers       []string
	TradeTopic    string
	ConsumerGroup string

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
		zap.String("topic", cfg.TradeTopic),
		zap.String("group", cfg.ConsumerGroup))

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

	c, err := consumer.New(consumer.Config{
		Brokers:  cfg.Brokers,
		ClientID: cfg.InstanceID,
		GroupID:  cfg.ConsumerGroup,
		Topic:    cfg.TradeTopic,
	}, mysql, logger)
	if err != nil {
		logger.Fatal("consumer init", zap.Error(err))
	}
	defer c.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := c.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("consumer exited", zap.Error(err))
			stop() // trigger overall shutdown — supervisor will restart us
		}
	}()

	<-rootCtx.Done()
	logger.Info("shutdown initiated")
	c.Close()
	wg.Wait()
	logger.Info("trade-dump shutdown complete")
}

func parseFlags() Config {
	cfg := Config{
		InstanceID:        "trade-dump-0",
		TradeTopic:        "trade-event",
		MySQLDSN:          "opentrade:opentrade@tcp(127.0.0.1:3306)/opentrade?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=true",
		MySQLMaxOpenConns: 16,
		MySQLMaxIdleConns: 4,
		MySQLConnMaxLife:  30 * time.Minute,
		Env:               "dev",
		LogLevel:          "info",
	}
	var brokersStr string
	flag.StringVar(&cfg.InstanceID, "instance-id", cfg.InstanceID, "instance id (client id / consumer group suffix)")
	flag.StringVar(&brokersStr, "brokers", "localhost:9092", "comma-separated Kafka brokers")
	flag.StringVar(&cfg.TradeTopic, "trade-topic", cfg.TradeTopic, "trade-event topic name")
	flag.StringVar(&cfg.ConsumerGroup, "group", "", "Kafka consumer group (default trade-dump-{instance-id})")
	flag.StringVar(&cfg.MySQLDSN, "mysql-dsn", cfg.MySQLDSN, "MySQL DSN")
	flag.IntVar(&cfg.MySQLMaxOpenConns, "mysql-max-open", cfg.MySQLMaxOpenConns, "MySQL max open connections")
	flag.IntVar(&cfg.MySQLMaxIdleConns, "mysql-max-idle", cfg.MySQLMaxIdleConns, "MySQL max idle connections")
	flag.DurationVar(&cfg.MySQLConnMaxLife, "mysql-conn-life", cfg.MySQLConnMaxLife, "MySQL connection max lifetime")
	flag.IntVar(&cfg.MySQLInsertChunkMax, "mysql-chunk", cfg.MySQLInsertChunkMax, "rows per multi-row INSERT chunk (default 500)")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	cfg.Brokers = splitCSV(brokersStr)
	if cfg.ConsumerGroup == "" {
		cfg.ConsumerGroup = "trade-dump-" + cfg.InstanceID
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
