// Command history runs the OpenTrade HistoryService read gateway
// (MVP-15, ADR-0046 draft). It reads the trade-dump MySQL projection
// and exposes cursor-paged queries over gRPC; it consumes no Kafka and
// holds no business state.
//
// Lag: trade-dump commits MySQL before advancing Kafka offsets
// (ADR-0023), so rows lag real-time by at most one batch. Clients that
// need live fill progress keep using the Push `user` stream (ADR-0007).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	historypb "github.com/xargin/opentrade/api/gen/rpc/history"
	"github.com/xargin/opentrade/history/internal/mysqlstore"
	"github.com/xargin/opentrade/history/internal/server"
	"github.com/xargin/opentrade/pkg/logx"
	"go.uber.org/zap"
)

type Config struct {
	InstanceID       string
	GRPCAddr         string
	MySQLDSN         string
	MySQLMaxOpen     int
	MySQLMaxIdle     int
	MySQLConnMaxLife time.Duration
	QueryTimeout     time.Duration
	Env              string
	LogLevel         string
}

func main() {
	cfg := parseFlags()

	logger, err := logx.New(logx.Config{Service: "history", Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	logx.SetGlobal(logger)

	if err := cfg.validate(); err != nil {
		logger.Fatal("invalid config", zap.Error(err))
	}

	logger.Info("history starting",
		zap.String("instance", cfg.InstanceID),
		zap.String("grpc", cfg.GRPCAddr))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := mysqlstore.NewStore(mysqlstore.Config{
		DSN:             cfg.MySQLDSN,
		MaxOpenConns:    cfg.MySQLMaxOpen,
		MaxIdleConns:    cfg.MySQLMaxIdle,
		ConnMaxLifetime: cfg.MySQLConnMaxLife,
		QueryTimeout:    cfg.QueryTimeout,
	})
	if err != nil {
		logger.Fatal("mysql open", zap.Error(err))
	}
	defer func() { _ = store.Close() }()

	pingCtx, pingCancel := context.WithTimeout(rootCtx, 5*time.Second)
	if err := store.Ping(pingCtx); err != nil {
		pingCancel()
		logger.Fatal("mysql ping", zap.Error(err))
	}
	pingCancel()

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Fatal("grpc listen", zap.Error(err))
	}
	httpSrv := &http.Server{Handler: historypb.NewHistoryServiceHTTPHandler(server.New(store))}

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("rpc serving", zap.String("addr", cfg.GRPCAddr))
		serveErr <- httpSrv.Serve(lis)
	}()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("rpc serve", zap.Error(err))
		}
	case <-rootCtx.Done():
		logger.Info("shutdown initiated")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = httpSrv.Shutdown(shutdownCtx)
		cancel()
	}
	logger.Info("history shutdown complete")
}

func parseFlags() Config {
	cfg := Config{
		InstanceID:       "history-0",
		GRPCAddr:         ":8085",
		MySQLDSN:         "opentrade:opentrade@tcp(127.0.0.1:3306)/opentrade?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=true",
		MySQLMaxOpen:     16,
		MySQLMaxIdle:     4,
		MySQLConnMaxLife: 30 * time.Minute,
		QueryTimeout:     2 * time.Second,
		Env:              "dev",
		LogLevel:         "info",
	}
	flag.StringVar(&cfg.InstanceID, "instance-id", cfg.InstanceID, "instance id")
	flag.StringVar(&cfg.GRPCAddr, "grpc", cfg.GRPCAddr, "gRPC listen addr")
	flag.StringVar(&cfg.MySQLDSN, "mysql-dsn", cfg.MySQLDSN, "MySQL DSN (read replica recommended)")
	flag.IntVar(&cfg.MySQLMaxOpen, "mysql-max-open", cfg.MySQLMaxOpen, "MySQL max open connections")
	flag.IntVar(&cfg.MySQLMaxIdle, "mysql-max-idle", cfg.MySQLMaxIdle, "MySQL max idle connections")
	flag.DurationVar(&cfg.MySQLConnMaxLife, "mysql-conn-life", cfg.MySQLConnMaxLife, "MySQL conn max lifetime")
	flag.DurationVar(&cfg.QueryTimeout, "query-timeout", cfg.QueryTimeout, "per-query timeout")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()
	return cfg
}

func (c *Config) validate() error {
	if c.InstanceID == "" {
		return fmt.Errorf("instance-id required")
	}
	if c.GRPCAddr == "" {
		return fmt.Errorf("grpc addr required")
	}
	if c.MySQLDSN == "" {
		return fmt.Errorf("mysql-dsn required")
	}
	return nil
}
