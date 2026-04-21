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

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/xargin/opentrade/bff/internal/client"
	"github.com/xargin/opentrade/bff/internal/clusterview"
	"github.com/xargin/opentrade/bff/internal/marketcache"
	"github.com/xargin/opentrade/bff/internal/rest"
	bffws "github.com/xargin/opentrade/bff/internal/ws"
	"github.com/xargin/opentrade/pkg/auth"
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

	// Market-data cache for reconnect replay (ADR-0038). Empty brokers
	// disables the cache and the /v1/depth + /v1/klines endpoints return
	// 503.
	MarketBrokers  []string
	MarketTopic    string
	MarketGroupID  string
	KlineBuffer    int

	// Auth (ADR-0039). AuthMode in {"header","jwt","api-key","mixed"}.
	// Default "header" = legacy X-User-Id trust-the-header scheme.
	AuthMode     string
	JWTSecret    string
	APIKeysFile  string

	// Conditional service endpoint (ADR-0040). Empty disables the
	// /v1/conditional endpoints with 503.
	ConditionalAddr string

	// History service endpoint (ADR-0046). Empty disables the
	// /v1/orders, /v1/trades, /v1/account-logs endpoints with 503.
	HistoryAddr string

	// Asset service endpoint (ADR-0057). Empty disables the
	// /v1/transfer, /v1/transfer/{id}, /v1/funding-balance endpoints
	// with 503.
	AssetAddr string

	// Counter routing mode (ADR-0058 phase 5). "disabled" keeps the
	// legacy --counter-shards list; "enabled" ignores it and routes
	// each request by hashing user_id into the 256-vshard table watched
	// from etcd.
	ClusteringMode string
	EtcdEndpoints  []string
	VShardCount    int
	ClusterRoot    string

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

	counter, closeCounter, err := buildCounter(rootCtx, cfg, logger)
	if err != nil {
		logger.Fatal("build counter client", zap.Error(err))
	}
	defer closeCounter()

	mdCache, mdConsumer, err := startMarketCache(rootCtx, cfg, logger)
	if err != nil {
		logger.Fatal("start market-data cache", zap.Error(err))
	}
	defer func() {
		if mdConsumer != nil {
			mdConsumer.Close()
		}
	}()

	authMW, err := buildAuthMiddleware(cfg, logger)
	if err != nil {
		logger.Fatal("auth middleware", zap.Error(err))
	}

	condConn, condClient, err := maybeDialConditional(rootCtx, cfg.ConditionalAddr, logger)
	if err != nil {
		logger.Fatal("dial conditional", zap.Error(err))
	}
	defer func() {
		if condConn != nil {
			_ = condConn.Close()
		}
	}()

	histConn, histClient, err := maybeDialHistory(rootCtx, cfg.HistoryAddr, logger)
	if err != nil {
		logger.Fatal("dial history", zap.Error(err))
	}
	defer func() {
		if histConn != nil {
			_ = histConn.Close()
		}
	}()

	assetConn, assetClient, err := maybeDialAsset(rootCtx, cfg.AssetAddr, logger)
	if err != nil {
		logger.Fatal("dial asset", zap.Error(err))
	}
	defer func() {
		if assetConn != nil {
			_ = assetConn.Close()
		}
	}()

	srv := rest.NewServer(rest.Config{
		Addr:           cfg.HTTPAddr,
		UserRateLimit:  cfg.UserRateLimit,
		UserRateWindow: cfg.UserRateWindow,
		IPRateLimit:    cfg.IPRateLimit,
		IPRateWindow:   cfg.IPRateWindow,
		AuthMiddleware: authMW,
	}, counter, assetClient, mdCache, condClient, histClient, logger)

	outer := http.NewServeMux()
	if cfg.PushWSURL != "" {
		proxy, err := bffws.New(bffws.Config{
			UpstreamURL: cfg.PushWSURL,
			DialTimeout: cfg.WSProxyDialTimeout,
		}, logger)
		if err != nil {
			logger.Fatal("ws proxy init", zap.Error(err))
		}
		outer.Handle("/ws", authMW(proxy.Handler()))
		logger.Info("ws reverse-proxy enabled", zap.String("upstream", cfg.PushWSURL))
	}

	outer.Handle("/", srv.Handler())

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           outer,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}

	var bgWG sync.WaitGroup
	if mdConsumer != nil {
		bgWG.Add(1)
		go func() {
			defer bgWG.Done()
			if err := mdConsumer.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("market-data cache run exited", zap.Error(err))
			}
		}()
	}

	go func() {
		logger.Info("http listening", zap.String("addr", cfg.HTTPAddr))
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http serve", zap.Error(err))
		}
	}()

	<-rootCtx.Done()
	bgWG.Wait()
	logger.Info("shutdown initiated")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown", zap.Error(err))
	}
	logger.Info("bff shutdown complete")
}

// maybeDialConditional dials the conditional service when addr is set.
// Returns (nil, nil, nil) when disabled so the REST layer falls back to
// 503 on /v1/conditional endpoints. Errors are fatal so misconfigured
// prod doesn't silently lose conditional-order traffic.
func maybeDialConditional(ctx context.Context, addr string, logger *zap.Logger) (*grpc.ClientConn, client.Conditional, error) {
	if addr == "" {
		logger.Info("conditional endpoint disabled (empty --conditional)")
		return nil, nil, nil
	}
	conn, c, err := client.DialConditional(ctx, addr)
	if err != nil {
		return nil, nil, fmt.Errorf("conditional dial %s: %w", addr, err)
	}
	logger.Info("conditional endpoint configured", zap.String("addr", addr))
	return conn, c, nil
}

// maybeDialHistory dials the history service when addr is set. Returns
// (nil, nil, nil) when disabled; the REST layer then 503s on
// /v1/orders, /v1/trades, /v1/account-logs (ADR-0046).
func maybeDialHistory(ctx context.Context, addr string, logger *zap.Logger) (*grpc.ClientConn, client.History, error) {
	if addr == "" {
		logger.Info("history endpoint disabled (empty --history)")
		return nil, nil, nil
	}
	conn, c, err := client.DialHistory(ctx, addr)
	if err != nil {
		return nil, nil, fmt.Errorf("history dial %s: %w", addr, err)
	}
	logger.Info("history endpoint configured", zap.String("addr", addr))
	return conn, c, nil
}

// maybeDialAsset dials the asset service when addr is set. Returns
// (nil, nil, nil) when disabled; the REST layer then 503s on the
// ADR-0057 endpoints (/v1/transfer, /v1/funding-balance, ...).
func maybeDialAsset(ctx context.Context, addr string, logger *zap.Logger) (*grpc.ClientConn, client.Asset, error) {
	if addr == "" {
		logger.Info("asset endpoint disabled (empty --asset)")
		return nil, nil, nil
	}
	conn, c, err := client.DialAsset(ctx, addr)
	if err != nil {
		return nil, nil, fmt.Errorf("asset dial %s: %w", addr, err)
	}
	logger.Info("asset endpoint configured", zap.String("addr", addr))
	return conn, c, nil
}

// buildAuthMiddleware turns the flag set into a configured auth middleware.
// Loads the API-key file when api-key or mixed mode is selected.
func buildAuthMiddleware(cfg Config, logger *zap.Logger) (func(http.Handler) http.Handler, error) {
	mode := auth.Mode(cfg.AuthMode)
	authCfg := auth.Config{
		Mode:      mode,
		JWTSecret: []byte(cfg.JWTSecret),
		Logger:    logger,
	}
	if mode == auth.ModeAPIKey || mode == auth.ModeMixed {
		store, err := auth.NewMemoryStore(cfg.APIKeysFile)
		if err != nil {
			return nil, fmt.Errorf("api keys: %w", err)
		}
		authCfg.APIKeyStore = store
	}
	mw, err := auth.NewMiddleware(authCfg)
	if err != nil {
		return nil, err
	}
	logger.Info("auth middleware ready",
		zap.String("mode", cfg.AuthMode),
		zap.Bool("jwt_configured", cfg.JWTSecret != ""),
		zap.Bool("api_keys_loaded", cfg.APIKeysFile != ""))
	return mw, nil
}

// startMarketCache optionally wires a market-data consumer into a
// marketcache.Cache. Returns (nil, nil, nil) when disabled (empty brokers);
// a non-nil consumer must be Close()'d on shutdown, Run()'d in a goroutine.
func startMarketCache(_ context.Context, cfg Config, logger *zap.Logger) (*marketcache.Cache, *marketcache.Consumer, error) {
	if len(cfg.MarketBrokers) == 0 {
		logger.Info("market-data cache disabled (empty --market-brokers)")
		return nil, nil, nil
	}
	cache := marketcache.New(marketcache.Config{KlineBuffer: cfg.KlineBuffer})
	cons, err := marketcache.NewConsumer(marketcache.ConsumerConfig{
		Brokers:  cfg.MarketBrokers,
		ClientID: "bff-md",
		GroupID:  cfg.MarketGroupID,
		Topic:    cfg.MarketTopic,
	}, cache, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("marketcache consumer: %w", err)
	}
	logger.Info("market-data cache enabled",
		zap.Strings("brokers", cfg.MarketBrokers),
		zap.String("topic", cfg.MarketTopic),
		zap.String("group", cfg.MarketGroupID),
		zap.Int("kline_buffer", cfg.KlineBuffer))
	return cache, cons, nil
}

// buildCounter picks between the legacy static-shard routing and the
// ADR-0058 vshard routing based on --clustering-mode. Returns the
// Counter impl plus a close function the caller MUST defer — close
// tears down every cached gRPC connection and, for the vshard path,
// stops the etcd watcher goroutine.
func buildCounter(ctx context.Context, cfg Config, logger *zap.Logger) (client.Counter, func(), error) {
	if cfg.ClusteringMode == "enabled" {
		return buildVShardCounter(ctx, cfg, logger)
	}
	conns, clients, err := dialAllShards(ctx, cfg.CounterShards)
	if err != nil {
		return nil, nil, err
	}
	c, err := client.NewSharded(clients)
	if err != nil {
		for _, conn := range conns {
			_ = conn.Close()
		}
		return nil, nil, err
	}
	closer := func() {
		for i, conn := range conns {
			if err := conn.Close(); err != nil {
				logger.Debug("close shard conn",
					zap.Int("shard", i), zap.Error(err))
			}
		}
	}
	return c, closer, nil
}

// buildVShardCounter wires etcd → clusterview.Watcher → VShardCounter
// for ADR-0058 phase 5 routing. The watcher goroutine owns the etcd
// client; shutdown closes the counter (drops cached gRPC conns), waits
// for the watcher loop to exit, then closes etcd.
func buildVShardCounter(ctx context.Context, cfg Config, logger *zap.Logger) (client.Counter, func(), error) {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("etcd dial: %w", err)
	}
	watcher, err := clusterview.New(clusterview.Config{
		Client:      etcdCli,
		RootPrefix:  cfg.ClusterRoot,
		VShardCount: cfg.VShardCount,
		Logger:      logger,
	})
	if err != nil {
		_ = etcdCli.Close()
		return nil, nil, fmt.Errorf("watcher: %w", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := watcher.Run(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("cluster watcher exited", zap.Error(err))
		}
	}()
	c, err := client.NewVShardCounter(watcher)
	if err != nil {
		cancel()
		<-done
		_ = etcdCli.Close()
		return nil, nil, fmt.Errorf("vshard counter: %w", err)
	}
	closer := func() {
		if err := c.Close(); err != nil {
			logger.Warn("close vshard counter", zap.Error(err))
		}
		cancel()
		<-done
		_ = etcdCli.Close()
	}
	logger.Info("clustering-mode=enabled: routing counter via etcd vshard table",
		zap.Strings("etcd", cfg.EtcdEndpoints),
		zap.Int("vshard_count", cfg.VShardCount),
		zap.String("cluster_root", cfg.ClusterRoot))
	return c, closer, nil
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
		MarketTopic:        "market-data",
		KlineBuffer:        500,
		AuthMode:           "header",
		ClusteringMode:     "disabled", // ADR-0058 phase 5; opt-in
		VShardCount:        256,
		ClusterRoot:        "/cex/counter",
		Env:                "dev",
		LogLevel:           "info",
	}
	var (
		shardsCSV     string
		marketBrokers string
		etcdCSV       string
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
	flag.StringVar(&marketBrokers, "market-brokers", "", "Kafka brokers for the market-data reconnect cache (empty disables /v1/depth + /v1/klines; ADR-0038)")
	flag.StringVar(&cfg.MarketTopic, "market-topic", cfg.MarketTopic, "market-data topic name (default: market-data)")
	flag.StringVar(&cfg.MarketGroupID, "market-group", "", "consumer group id for the market-data cache (default bff-md-{host})")
	flag.IntVar(&cfg.KlineBuffer, "kline-buffer", cfg.KlineBuffer, "max KlineClosed entries kept per (symbol, interval)")
	flag.StringVar(&cfg.AuthMode, "auth-mode", cfg.AuthMode, "auth mode: header | jwt | api-key | mixed (ADR-0039)")
	flag.StringVar(&cfg.JWTSecret, "jwt-secret", "", "HS256 secret for --auth-mode=jwt|mixed (empty = jwt disabled in mixed mode)")
	flag.StringVar(&cfg.APIKeysFile, "api-keys-file", "", "JSON file listing {key,secret,user_id} entries for --auth-mode=api-key|mixed")
	flag.StringVar(&cfg.ConditionalAddr, "conditional", "", "Conditional service gRPC endpoint (empty disables /v1/conditional; ADR-0040)")
	flag.StringVar(&cfg.HistoryAddr, "history", "", "History service gRPC endpoint (empty disables /v1/orders,/v1/trades,/v1/account-logs; ADR-0046)")
	flag.StringVar(&cfg.AssetAddr, "asset", "", "Asset service gRPC endpoint (empty disables /v1/transfer,/v1/funding-balance; ADR-0057)")
	flag.StringVar(&cfg.ClusteringMode, "clustering-mode", cfg.ClusteringMode, "counter routing mode: disabled (use --counter-shards) | enabled (watch etcd for ADR-0058 vshard routing)")
	flag.StringVar(&etcdCSV, "etcd", "", "comma-separated etcd endpoints (required when --clustering-mode=enabled)")
	flag.IntVar(&cfg.VShardCount, "vshard-count", cfg.VShardCount, "ADR-0058 vshard count (must match counter --vshard-count)")
	flag.StringVar(&cfg.ClusterRoot, "cluster-root", cfg.ClusterRoot, "etcd root prefix for counter cluster data (default /cex/counter)")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	if shardsCSV != "" {
		cfg.CounterShards = splitCSV(shardsCSV)
	} else if legacyCounter != "" {
		cfg.CounterShards = []string{legacyCounter}
	}
	cfg.EtcdEndpoints = splitCSV(etcdCSV)
	cfg.MarketBrokers = splitCSV(marketBrokers)
	if len(cfg.MarketBrokers) > 0 && cfg.MarketGroupID == "" {
		host, _ := os.Hostname()
		if host == "" {
			host = "local"
		}
		cfg.MarketGroupID = "bff-md-" + host
	}
	return cfg
}

func (c *Config) validate() error {
	if c.HTTPAddr == "" {
		return fmt.Errorf("http-addr required")
	}
	switch c.ClusteringMode {
	case "disabled":
		if len(c.CounterShards) == 0 {
			return fmt.Errorf("at least one counter shard endpoint required when --clustering-mode=disabled")
		}
	case "enabled":
		if len(c.EtcdEndpoints) == 0 {
			return fmt.Errorf("--etcd required when --clustering-mode=enabled")
		}
		if c.VShardCount <= 0 {
			return fmt.Errorf("--vshard-count must be > 0 when --clustering-mode=enabled")
		}
	default:
		return fmt.Errorf("--clustering-mode must be disabled or enabled, got %q", c.ClusteringMode)
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
