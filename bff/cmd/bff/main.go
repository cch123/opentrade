// Command bff runs the OpenTrade BFF (REST + gRPC gateway).
//
// MVP-8 scope:
//   - REST handlers: /v1/order (POST/DELETE/GET), /v1/transfer (POST),
//     /v1/account (GET), /healthz.
//   - Auth: trust X-User-Id header (MVP placeholder).
//   - Sliding-window rate limiting per user + per IP.
//   - Upstream: N Counter shard endpoints, routed by user_id hash
//     (ADR-0010 + ADR-0027).
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/xargin/opentrade/bff/internal/auth"
	"github.com/xargin/opentrade/bff/internal/client"
	"github.com/xargin/opentrade/bff/internal/rest"
	bffws "github.com/xargin/opentrade/bff/internal/ws"
	"github.com/xargin/opentrade/pkg/logx"
)

type Config struct {
	HTTPAddr          string
	CounterShards     []string
	PushWSURL         string // upstream push /ws endpoint; empty disables the WS proxy
	WSProxyDialTimeout time.Duration
	UserRateLimit     int
	UserRateWindow    time.Duration
	IPRateLimit       int
	IPRateWindow      time.Duration
	ReadHeaderTimeout time.Duration
	ShutdownGrace     time.Duration
	Env               string
	LogLevel          string
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
		zap.Strings("counter_shards", cfg.CounterShards))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conns, clients, err := dialAllShards(rootCtx, cfg.CounterShards)
	if err != nil {
		logger.Fatal("dial counter shards", zap.Error(err))
	}
	defer func() {
		for i, c := range conns {
			if err := c.Close(); err != nil {
				logger.Debug("close shard conn",
					zap.Int("shard", i), zap.Error(err))
			}
		}
	}()
	counter, err := client.NewSharded(clients)
	if err != nil {
		logger.Fatal("build sharded counter", zap.Error(err))
	}

	srv := rest.NewServer(rest.Config{
		Addr:           cfg.HTTPAddr,
		UserRateLimit:  cfg.UserRateLimit,
		UserRateWindow: cfg.UserRateWindow,
		IPRateLimit:    cfg.IPRateLimit,
		IPRateWindow:   cfg.IPRateWindow,
	}, counter, logger)

	outer := http.NewServeMux()
	if cfg.PushWSURL != "" {
		proxy, err := bffws.New(bffws.Config{
			UpstreamURL: cfg.PushWSURL,
			DialTimeout: cfg.WSProxyDialTimeout,
		}, logger)
		if err != nil {
			logger.Fatal("ws proxy init", zap.Error(err))
		}
		outer.Handle("/ws", auth.Middleware(proxy.Handler()))
		logger.Info("ws reverse-proxy enabled", zap.String("upstream", cfg.PushWSURL))
	}
	outer.Handle("/", srv.Handler())

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           outer,
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

// dialAllShards walks endpoints left-to-right and dials each as a separate
// grpc.ClientConn, returning them alongside a matching Counter slice. Caller
// owns both slices' lifetime.
func dialAllShards(ctx context.Context, endpoints []string) ([]*grpc.ClientConn, []client.Counter, error) {
	conns := make([]*grpc.ClientConn, 0, len(endpoints))
	clients := make([]client.Counter, 0, len(endpoints))
	for i, ep := range endpoints {
		conn, c, err := client.Dial(ctx, ep)
		if err != nil {
			for _, prior := range conns {
				_ = prior.Close()
			}
			return nil, nil, fmt.Errorf("shard %d (%s): %w", i, ep, err)
		}
		conns = append(conns, conn)
		clients = append(clients, c)
	}
	return conns, clients, nil
}

// ---------------------------------------------------------------------------
// Config helpers
// ---------------------------------------------------------------------------

func parseFlags() Config {
	cfg := Config{
		HTTPAddr:           ":8080",
		WSProxyDialTimeout: 5 * time.Second,
		UserRateLimit:      50,
		UserRateWindow:     time.Second,
		IPRateLimit:        200,
		IPRateWindow:       time.Second,
		ReadHeaderTimeout:  5 * time.Second,
		ShutdownGrace:      5 * time.Second,
		Env:                "dev",
		LogLevel:           "info",
	}
	var (
		shardsCSV string
		// legacy single-shard flag for dev one-liner convenience
		legacyCounter string
	)
	flag.StringVar(&cfg.HTTPAddr, "http-addr", cfg.HTTPAddr, "HTTP listen address")
	flag.StringVar(&shardsCSV, "counter-shards", "",
		"comma-separated Counter gRPC shard endpoints, in shard-id order (shard 0, shard 1, ...)")
	flag.StringVar(&legacyCounter, "counter", "localhost:8081",
		"single-shard convenience: used when --counter-shards is empty")
	flag.StringVar(&cfg.PushWSURL, "push-ws", "", "push /ws endpoint to reverse-proxy (e.g. ws://push:8081/ws); empty disables /ws")
	flag.DurationVar(&cfg.WSProxyDialTimeout, "ws-dial-timeout", cfg.WSProxyDialTimeout, "WS proxy upstream dial timeout")
	flag.IntVar(&cfg.UserRateLimit, "user-rate", cfg.UserRateLimit, "requests per user per window")
	flag.DurationVar(&cfg.UserRateWindow, "user-window", cfg.UserRateWindow, "user rate window")
	flag.IntVar(&cfg.IPRateLimit, "ip-rate", cfg.IPRateLimit, "requests per IP per window")
	flag.DurationVar(&cfg.IPRateWindow, "ip-window", cfg.IPRateWindow, "IP rate window")
	flag.DurationVar(&cfg.ShutdownGrace, "shutdown-grace", cfg.ShutdownGrace, "graceful shutdown timeout")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	if shardsCSV != "" {
		cfg.CounterShards = splitCSV(shardsCSV)
	} else if legacyCounter != "" {
		cfg.CounterShards = []string{legacyCounter}
	}
	return cfg
}

func (c *Config) validate() error {
	if c.HTTPAddr == "" {
		return fmt.Errorf("http-addr required")
	}
	if len(c.CounterShards) == 0 {
		return fmt.Errorf("at least one counter shard endpoint required")
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
