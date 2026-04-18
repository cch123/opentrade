// Command bff runs the OpenTrade BFF (REST + gRPC gateway).
//
// MVP-4 scope:
//   - REST handlers: /v1/order (POST/DELETE/GET), /v1/transfer (POST),
//     /v1/account (GET), /healthz.
//   - Auth: trust X-User-Id header (MVP placeholder).
//   - Sliding-window rate limiting per user + per IP.
//   - Upstream: single Counter shard via gRPC (MVP-9 adds routing across
//     shards).
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/bff/internal/client"
	"github.com/xargin/opentrade/bff/internal/rest"
	"github.com/xargin/opentrade/pkg/logx"
)

type Config struct {
	HTTPAddr         string
	CounterEndpoint  string
	UserRateLimit    int
	UserRateWindow   time.Duration
	IPRateLimit      int
	IPRateWindow     time.Duration
	ReadHeaderTimeout time.Duration
	ShutdownGrace    time.Duration
	Env              string
	LogLevel         string
}

func main() {
	cfg := parseFlags()

	logger, err := logx.New(logx.Config{Service: "bff", Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	logx.SetGlobal(logger)

	if err := cfg.validate(); err != nil {
		logger.Fatal("invalid config", zap.Error(err))
	}

	logger.Info("bff starting",
		zap.String("http", cfg.HTTPAddr),
		zap.String("counter", cfg.CounterEndpoint))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// gRPC client (lazy — Dial doesn't connect immediately).
	conn, counter, err := client.Dial(rootCtx, cfg.CounterEndpoint)
	if err != nil {
		logger.Fatal("dial counter", zap.Error(err))
	}
	defer func() { _ = conn.Close() }()

	srv := rest.NewServer(rest.Config{
		Addr:           cfg.HTTPAddr,
		UserRateLimit:  cfg.UserRateLimit,
		UserRateWindow: cfg.UserRateWindow,
		IPRateLimit:    cfg.IPRateLimit,
		IPRateWindow:   cfg.IPRateWindow,
	}, counter, logger)

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}

	go func() {
		logger.Info("http listening", zap.String("addr", cfg.HTTPAddr))
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http serve", zap.Error(err))
		}
	}()

	<-rootCtx.Done()
	logger.Info("shutdown initiated")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown", zap.Error(err))
	}
	logger.Info("bff shutdown complete")
}

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

func parseFlags() Config {
	cfg := Config{
		HTTPAddr:          ":8080",
		CounterEndpoint:   "localhost:8081",
		UserRateLimit:     50,
		UserRateWindow:    time.Second,
		IPRateLimit:       200,
		IPRateWindow:      time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		ShutdownGrace:     5 * time.Second,
		Env:               "dev",
		LogLevel:          "info",
	}
	flag.StringVar(&cfg.HTTPAddr, "http-addr", cfg.HTTPAddr, "HTTP listen address")
	flag.StringVar(&cfg.CounterEndpoint, "counter", cfg.CounterEndpoint, "Counter gRPC endpoint")
	flag.IntVar(&cfg.UserRateLimit, "user-rate", cfg.UserRateLimit, "requests per user per window")
	flag.DurationVar(&cfg.UserRateWindow, "user-window", cfg.UserRateWindow, "user rate window")
	flag.IntVar(&cfg.IPRateLimit, "ip-rate", cfg.IPRateLimit, "requests per IP per window")
	flag.DurationVar(&cfg.IPRateWindow, "ip-window", cfg.IPRateWindow, "IP rate window")
	flag.DurationVar(&cfg.ShutdownGrace, "shutdown-grace", cfg.ShutdownGrace, "graceful shutdown timeout")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()
	return cfg
}

func (c *Config) validate() error {
	if c.HTTPAddr == "" {
		return fmt.Errorf("http-addr required")
	}
	if c.CounterEndpoint == "" {
		return fmt.Errorf("counter endpoint required")
	}
	return nil
}
