// Command admin-gateway is the ops-only HTTP entry point for OpenTrade
// (ADR-0052). Separate process from BFF so internal admin traffic never
// shares a mux, auth middleware, rate-limiter, or access log with 2C
// user-facing requests.
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
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/xargin/opentrade/admin-gateway/internal/counterclient"
	"github.com/xargin/opentrade/admin-gateway/internal/rollout"
	"github.com/xargin/opentrade/admin-gateway/internal/server"
	"github.com/xargin/opentrade/pkg/adminaudit"
	"github.com/xargin/opentrade/pkg/auth"
	"github.com/xargin/opentrade/pkg/etcdcfg"
	"github.com/xargin/opentrade/pkg/logx"
)

// Config bundles runtime knobs.
type Config struct {
	HTTPAddr          string
	CounterShards     []string
	ReadHeaderTimeout time.Duration
	ShutdownGrace     time.Duration
	RequestTimeout    time.Duration

	// Admin auth — required; empty = refuse to start. This service has
	// exactly one auth surface (admin-role API-Key, BN-style signed
	// requests). No JWT, no header-trust, no user-role fallback.
	AdminAPIKeysFile string

	// Audit log — required; empty = refuse to start. Every mutating
	// request must be auditable (ADR-0052).
	AuditLogPath string

	// Etcd — optional; empty disables /admin/symbols (503). Present
	// deploys should always set this; absence is a dev-only mode.
	EtcdEndpoints []string
	EtcdPrefix    string

	// ADR-0053 M4: precision rollout executor. Active only when etcd is
	// configured. Zero disables (dev only); must be >= 1s otherwise.
	RolloutScanInterval time.Duration

	Env      string
	LogLevel string
}

func main() {
	cfg := parseFlags()

	logger, err := logx.New(logx.Config{Service: "admin-gateway", Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	logx.SetGlobal(logger)

	if err := cfg.validate(); err != nil {
		logger.Fatal("invalid config", zap.Error(err))
	}

	logger.Info("admin-gateway starting",
		zap.String("http", cfg.HTTPAddr),
		zap.Strings("counter_shards", cfg.CounterShards),
		zap.String("audit_log", cfg.AuditLogPath),
		zap.Bool("etcd_configured", len(cfg.EtcdEndpoints) > 0))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Counter shard clients — admin needs the full shard list to fan
	// out symbol-only cancels, same xxhash routing as BFF for user-
	// scoped cancels.
	conns, shardClients, err := dialAllShards(rootCtx, cfg.CounterShards)
	if err != nil {
		logger.Fatal("dial counter shards", zap.Error(err))
	}
	defer func() {
		for i, c := range conns {
			if err := c.Close(); err != nil {
				logger.Debug("close shard conn", zap.Int("shard", i), zap.Error(err))
			}
		}
	}()
	sharded, err := counterclient.NewSharded(shardClients)
	if err != nil {
		logger.Fatal("build sharded counter", zap.Error(err))
	}

	// Admin API-Key store: role=admin only, anything else is a config
	// error at boot (catches the "user keys accidentally pasted into
	// admin file" footgun).
	store, err := auth.NewMemoryStore(cfg.AdminAPIKeysFile, auth.RoleAdmin)
	if err != nil {
		logger.Fatal("admin api keys", zap.Error(err))
	}
	adminMW, err := auth.AdminMiddleware(store, logger)
	if err != nil {
		logger.Fatal("admin middleware", zap.Error(err))
	}

	// Audit log: open + fsync per entry. Open failure is fatal — no
	// audit = no admin plane.
	audit, err := adminaudit.Open(cfg.AuditLogPath)
	if err != nil {
		logger.Fatal("audit log open", zap.Error(err))
	}
	defer func() {
		if err := audit.Close(); err != nil {
			logger.Warn("audit close", zap.Error(err))
		}
	}()

	// Optional etcd connection for /admin/symbols CRUD.
	var etcdSource *etcdcfg.EtcdSource
	var etcdShim server.EtcdSource
	if len(cfg.EtcdEndpoints) > 0 {
		prefix := cfg.EtcdPrefix
		if prefix == "" {
			prefix = etcdcfg.DefaultPrefix
		}
		src, err := etcdcfg.NewEtcdSource(etcdcfg.EtcdConfig{
			Endpoints: cfg.EtcdEndpoints,
			Prefix:    prefix,
		})
		if err != nil {
			logger.Fatal("etcd source", zap.Error(err))
		}
		etcdSource = src
		etcdShim = newEtcdShim(src)
	}
	defer func() {
		if etcdSource != nil {
			_ = etcdSource.Close()
		}
	}()

	// ADR-0053 M4: precision rollout executor. Runs only when etcd is
	// configured (no etcd → no Tiers → no schedule → nothing to execute).
	if etcdSource != nil && cfg.RolloutScanInterval > 0 {
		runner, err := rollout.New(rollout.Config{
			Etcd:         etcdSource,
			Audit:        audit,
			Logger:       logger,
			ScanInterval: cfg.RolloutScanInterval,
		})
		if err != nil {
			logger.Fatal("rollout runner", zap.Error(err))
		}
		go func() {
			if err := runner.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("rollout runner exited", zap.Error(err))
			}
		}()
		logger.Info("precision rollout executor started",
			zap.Duration("scan_interval", cfg.RolloutScanInterval))
	} else if etcdSource != nil {
		logger.Info("precision rollout disabled (--rollout-scan-interval=0)")
	}

	adminSrv, err := server.New(server.Config{
		Counter:        sharded,
		Etcd:           etcdShim,
		Audit:          audit,
		Logger:         logger,
		RequestTimeout: cfg.RequestTimeout,
	})
	if err != nil {
		logger.Fatal("admin server", zap.Error(err))
	}

	// Route chain: /admin/healthz bypasses auth (readiness probes); the
	// rest goes through AdminMiddleware + RequireAdmin.
	adminHandler := adminSrv.Handler()
	outer := http.NewServeMux()
	outer.Handle("/admin/healthz", adminHandler) // bypass
	outer.Handle("/admin/", adminMW(auth.RequireAdmin(adminHandler)))

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           outer,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
	}
	go func() {
		logger.Info("http listening", zap.String("addr", cfg.HTTPAddr))
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
	logger.Info("admin-gateway shutdown complete")
}

// dialAllShards opens one grpc.ClientConn per shard endpoint in order.
func dialAllShards(ctx context.Context, endpoints []string) ([]*grpc.ClientConn, []counterclient.Counter, error) {
	conns := make([]*grpc.ClientConn, 0, len(endpoints))
	clients := make([]counterclient.Counter, 0, len(endpoints))
	for i, ep := range endpoints {
		conn, c, err := counterclient.Dial(ctx, ep)
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

// newEtcdShim adapts *etcdcfg.EtcdSource to server.EtcdSource. EtcdSource
// already has Put / Delete / List with matching signatures, so this is a
// direct pass-through.
type etcdSrcShim struct{ s *etcdcfg.EtcdSource }

func newEtcdShim(s *etcdcfg.EtcdSource) server.EtcdSource { return etcdSrcShim{s: s} }
func (s etcdSrcShim) List(ctx context.Context) (map[string]etcdcfg.SymbolConfig, int64, error) {
	return s.s.List(ctx)
}
func (s etcdSrcShim) Put(ctx context.Context, symbol string, cfg etcdcfg.SymbolConfig) (int64, error) {
	return s.s.Put(ctx, symbol, cfg)
}
func (s etcdSrcShim) Delete(ctx context.Context, symbol string) (bool, int64, error) {
	return s.s.Delete(ctx, symbol)
}

// ---------------------------------------------------------------------------
// Flag parsing + validation
// ---------------------------------------------------------------------------

func parseFlags() Config {
	cfg := Config{
		HTTPAddr:            ":8090",
		ReadHeaderTimeout:   5 * time.Second,
		ShutdownGrace:       5 * time.Second,
		RequestTimeout:      5 * time.Second,
		RolloutScanInterval: 5 * time.Second,
		Env:                 "dev",
		LogLevel:            "info",
	}
	var shardsCSV, etcdCSV string
	flag.StringVar(&cfg.HTTPAddr, "http-addr", cfg.HTTPAddr, "HTTP listen address (default :8090 — distinct from BFF :8080)")
	flag.StringVar(&shardsCSV, "counter-shards", "",
		"comma-separated Counter gRPC shard endpoints in shard-id order (required)")
	flag.StringVar(&cfg.AdminAPIKeysFile, "admin-api-keys-file", "", "JSON file of admin-role API-Key entries (required)")
	flag.StringVar(&cfg.AuditLogPath, "audit-log", "", "JSONL audit log file (required)")
	flag.StringVar(&etcdCSV, "etcd", "", "comma-separated etcd endpoints for /admin/symbols CRUD (empty disables symbol endpoints)")
	flag.StringVar(&cfg.EtcdPrefix, "etcd-prefix", "", "etcd key prefix for symbol configs (default /cex/match/symbols/)")
	flag.DurationVar(&cfg.RolloutScanInterval, "rollout-scan-interval", cfg.RolloutScanInterval,
		"ADR-0053 M4 precision rollout scan tick (0 disables; min 1s otherwise)")
	flag.DurationVar(&cfg.RequestTimeout, "request-timeout", cfg.RequestTimeout, "per-admin-request timeout")
	flag.DurationVar(&cfg.ShutdownGrace, "shutdown-grace", cfg.ShutdownGrace, "graceful shutdown timeout")
	flag.StringVar(&cfg.Env, "env", cfg.Env, "dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "log level")
	flag.Parse()

	cfg.CounterShards = splitCSV(shardsCSV)
	cfg.EtcdEndpoints = splitCSV(etcdCSV)
	return cfg
}

func (c *Config) validate() error {
	if c.HTTPAddr == "" {
		return fmt.Errorf("http-addr required")
	}
	if len(c.CounterShards) == 0 {
		return fmt.Errorf("--counter-shards is required (admin batch cancel needs at least one shard)")
	}
	if c.AdminAPIKeysFile == "" {
		return fmt.Errorf("--admin-api-keys-file is required (no anonymous admin access)")
	}
	if c.AuditLogPath == "" {
		return fmt.Errorf("--audit-log is required (no unaudited admin ops)")
	}
	if c.RolloutScanInterval > 0 && c.RolloutScanInterval < time.Second {
		return fmt.Errorf("--rollout-scan-interval must be 0 (disabled) or >= 1s")
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
