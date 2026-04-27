// Command asset runs the OpenTrade asset-service (ADR-0057).
//
// Responsibilities shipped in this binary:
//
//   - Serve the AssetHolder gRPC (TransferOut / TransferIn /
//     CompensateTransferOut) for the biz_line=funding account book.
//   - Serve AssetService.Transfer (the saga orchestrator entrypoint)
//     plus QueryTransfer / QueryFundingBalance read paths.
//   - Persist funding balances + saga state to opentrade_asset MySQL;
//     recover pending sagas on startup before opening the gRPC listener.
//
// Not yet wired (tracked in ADR-0057 follow-ups):
//
//   - HA cold-standby via etcd lease (M3c)
//   - biz_line discovery (currently flag-driven via --peer-<biz>=host:port)
//
// Single-instance for MVP; partitioning / sharding is deferred per
// ADR-0057 open question.
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

	_ "github.com/go-sql-driver/mysql" // MySQL driver for pkg/transferledger

	"go.uber.org/zap"

	"github.com/xargin/opentrade/api/gen/rpc/asset/assetrpcconnect"
	"github.com/xargin/opentrade/api/gen/rpc/assetholder/assetholderrpcconnect"
	"github.com/xargin/opentrade/asset/internal/holder"
	assetmetrics "github.com/xargin/opentrade/asset/internal/metrics"
	"github.com/xargin/opentrade/asset/internal/saga"
	"github.com/xargin/opentrade/asset/internal/server"
	"github.com/xargin/opentrade/asset/internal/service"
	"github.com/xargin/opentrade/asset/internal/store"
	"github.com/xargin/opentrade/pkg/connectx"
	"github.com/xargin/opentrade/pkg/logx"
	"github.com/xargin/opentrade/pkg/metrics"
	"github.com/xargin/opentrade/pkg/transferledger"
)

type Config struct {
	InstanceID  string
	GRPCAddr    string
	MetricsAddr string
	Env         string
	LogLevel    string

	// Reconciler tick interval (ADR-0057 M6). 0 = default (30s).
	ReconcileInterval time.Duration

	// MySQL for funding wallet + transfer_ledger.
	MySQLDSN string

	// Biz-line routing: `biz:addr` pairs parsed from --peer-holders.
	// "funding" is always wired to the in-process Service and should
	// NOT appear here.
	PeerHolders map[string]string
}

func main() {
	cfg := parseFlags()

	logger, err := logx.New(logx.Config{Service: "asset", Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	logx.SetGlobal(logger)

	if err := cfg.validate(); err != nil {
		logger.Fatal("invalid config", zap.Error(err))
	}

	logger.Info("asset starting",
		zap.String("instance", cfg.InstanceID),
		zap.String("grpc", cfg.GRPCAddr),
		zap.Any("peer_holders", cfg.PeerHolders))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Prometheus registry — one shared across framework + saga metrics.
	promReg := metrics.NewRegistry()
	_ = metrics.NewFramework("asset", promReg)
	sagaMetrics := assetmetrics.NewSaga(promReg)

	fundingStore, err := store.New(store.Config{DSN: cfg.MySQLDSN})
	if err != nil {
		logger.Fatal("funding store init", zap.Error(err))
	}
	defer func() { _ = fundingStore.Close() }()
	if err := fundingStore.Ping(rootCtx); err != nil {
		logger.Fatal("funding store ping", zap.Error(err))
	}

	svc := service.New(fundingStore, logger)

	ledger, err := transferledger.NewLedger(transferledger.Config{DSN: cfg.MySQLDSN})
	if err != nil {
		logger.Fatal("transfer_ledger init", zap.Error(err))
	}
	defer func() { _ = ledger.Close() }()

	registry := holder.NewRegistry()
	registry.Register("funding", holder.NewLocalFundingClient(svc))
	peerHTTP, err := registerPeerHolders(registry, cfg.PeerHolders)
	if err != nil {
		logger.Fatal("peer holders init", zap.Error(err))
	}
	defer func() {
		if peerHTTP != nil {
			peerHTTP.CloseIdleConnections()
		}
	}()

	driver := saga.New(saga.Config{}, ledger, registry, logger, sagaMetrics)
	orch := saga.NewOrchestrator(saga.OrchestratorConfig{}, ledger, driver, logger, sagaMetrics)

	// Recover pending sagas BEFORE opening the gRPC listener so no fresh
	// Transfer can race a resumed driver on the same id.
	recoverCtx, cancel := context.WithTimeout(rootCtx, 60*time.Second)
	if err := orch.Recover(recoverCtx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Warn("saga recover", zap.Error(err))
	}
	cancel()

	var reconciler *saga.Reconciler
	var reconcilerWG sync.WaitGroup
	reconciler = saga.NewReconciler(saga.ReconcilerConfig{Interval: cfg.ReconcileInterval},
		ledger, sagaMetrics, logger)
	reconcilerWG.Add(1)
	go func() {
		defer reconcilerWG.Done()
		if err := reconciler.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("reconciler exited", zap.Error(err))
		}
	}()

	// Metrics HTTP endpoint. Separate listener from gRPC so Prometheus
	// can scrape even when the gRPC side is saturated / blocked.
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", metrics.Handler(promReg))
	metricsMux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	metricsSrv := &http.Server{
		Addr:    cfg.MetricsAddr,
		Handler: metricsMux,
	}
	var metricsWG sync.WaitGroup
	if cfg.MetricsAddr != "" {
		metricsWG.Add(1)
		go func() {
			defer metricsWG.Done()
			logger.Info("metrics HTTP listening", zap.String("addr", cfg.MetricsAddr))
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("metrics server exited", zap.Error(err))
			}
		}()
	}

	rpcMux := http.NewServeMux()
	holderPath, holderHandler := assetholderrpcconnect.NewAssetHolderHandler(server.NewAssetHolderServer(svc))
	rpcMux.Handle(holderPath, holderHandler)
	assetPath, assetHandler := assetrpcconnect.NewAssetServiceHandler(server.NewAssetServer(svc, orch))
	rpcMux.Handle(assetPath, assetHandler)
	rpcSrv := connectx.NewH2CServer(cfg.GRPCAddr, rpcMux)

	var grpcWG sync.WaitGroup
	grpcWG.Add(1)
	go func() {
		defer grpcWG.Done()
		logger.Info("gRPC (Connect/h2c) listening", zap.String("addr", cfg.GRPCAddr))
		if err := rpcSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("rpc Serve", zap.Error(err))
		}
	}()

	<-rootCtx.Done()
	logger.Info("asset shutting down")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := rpcSrv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("rpc shutdown", zap.Error(err))
	}
	shutdownCancel()
	grpcWG.Wait()
	reconcilerWG.Wait()
	if cfg.MetricsAddr != "" {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = metricsSrv.Shutdown(shutdownCtx)
		cancel()
		metricsWG.Wait()
	}
	logger.Info("asset shutdown complete")
}

// registerPeerHolders wires each peer biz_line to a Connect-based
// AssetHolder client and registers it. Returns the shared *http.Client
// so the caller can drop idle connections on shutdown. "funding" is
// skipped because asset-service is itself the funding holder — a
// misconfiguration that targets it is a hard error.
func registerPeerHolders(reg *holder.Registry, peers map[string]string) (*http.Client, error) {
	if len(peers) == 0 {
		return nil, nil
	}
	httpClient := connectx.NewH2CClient()
	for biz, addr := range peers {
		if biz == "funding" {
			return httpClient, fmt.Errorf("holder: 'funding' cannot be a peer (this service IS the funding holder)")
		}
		reg.Register(biz, holder.NewGRPCClientFromHTTP(httpClient, connectx.BaseURL(addr)))
	}
	return httpClient, nil
}

func parseFlags() Config {
	var (
		instance       = flag.String("instance", "asset-main", "instance id; shows up in logs")
		grpcAddr       = flag.String("grpc", ":19000", "gRPC listen address")
		metricsAddr    = flag.String("metrics-addr", ":19090", "Prometheus scrape endpoint (empty disables /metrics HTTP)")
		env            = flag.String("env", "dev", "environment tag (dev/staging/prod)")
		level          = flag.String("log-level", "info", "log level: debug | info | warn | error")
		mysqlDSN       = flag.String("mysql-dsn", "", "MySQL DSN for opentrade_asset funding wallet + transfer_ledger")
		ledgerDSN      = flag.String("ledger-dsn", "", "deprecated alias for --mysql-dsn")
		peerFlag       = flag.String("peer-holders", "", "biz_line peer list, e.g. 'spot=counter-0:18000,spot=counter-1:18000,futures=futures:19500'. Keys may repeat; last one wins.")
		reconcileEvery = flag.Duration("reconcile-interval", saga.DefaultReconcileInterval, "saga reconciler tick interval (refreshes saga_state_count gauge)")
	)
	flag.Parse()

	dsn := *mysqlDSN
	if dsn == "" {
		dsn = *ledgerDSN
	}
	return Config{
		InstanceID:        *instance,
		GRPCAddr:          *grpcAddr,
		MetricsAddr:       *metricsAddr,
		Env:               *env,
		LogLevel:          *level,
		MySQLDSN:          dsn,
		PeerHolders:       parsePeerList(*peerFlag),
		ReconcileInterval: *reconcileEvery,
	}
}

// parsePeerList turns "spot=addr1,futures=addr2" into
// {"spot":"addr1","futures":"addr2"}. Malformed entries are skipped
// with no ceremony — main's validate() catches an empty map later if
// needed.
func parsePeerList(s string) map[string]string {
	if s == "" {
		return nil
	}
	out := make(map[string]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}
		biz, addr := strings.TrimSpace(kv[0]), strings.TrimSpace(kv[1])
		if biz == "" || addr == "" {
			continue
		}
		out[biz] = addr
	}
	return out
}

func (c Config) validate() error {
	if c.GRPCAddr == "" {
		return errors.New("grpc addr required")
	}
	if c.MySQLDSN == "" {
		return errors.New("mysql dsn required")
	}
	return nil
}
