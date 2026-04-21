// Command asset runs the OpenTrade asset-service (ADR-0057).
//
// Responsibilities shipped in this binary:
//
//   - Serve the AssetHolder gRPC (TransferOut / TransferIn /
//     CompensateTransferOut) for the biz_line=funding account book.
//   - Serve AssetService.Transfer (the saga orchestrator entrypoint)
//     plus QueryTransfer / QueryFundingBalance read paths.
//   - Persist saga state to MySQL via pkg/transferledger; recover
//     pending sagas on startup before opening the gRPC listener.
//   - Publish FundingTransferOut/In/Compensate and SagaStateChange
//     envelopes to the asset-journal Kafka topic for trade-dump to
//     project into MySQL.
//
// Not yet wired (tracked in ADR-0057 follow-ups):
//
//   - Snapshot of in-memory funding state + HA cold-standby via etcd
//     lease (M3c)
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
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQL driver for pkg/transferledger

	"go.uber.org/zap"

	assetrpc "github.com/xargin/opentrade/api/gen/rpc/asset"
	assetholderrpc "github.com/xargin/opentrade/api/gen/rpc/assetholder"
	"github.com/xargin/opentrade/asset/internal/engine"
	"github.com/xargin/opentrade/asset/internal/holder"
	"github.com/xargin/opentrade/asset/internal/journal"
	assetmetrics "github.com/xargin/opentrade/asset/internal/metrics"
	"github.com/xargin/opentrade/asset/internal/saga"
	"github.com/xargin/opentrade/asset/internal/server"
	"github.com/xargin/opentrade/asset/internal/service"
	"github.com/xargin/opentrade/pkg/logx"
	"github.com/xargin/opentrade/pkg/metrics"
	"github.com/xargin/opentrade/pkg/transferledger"
)

type Config struct {
	InstanceID   string
	GRPCAddr     string
	MetricsAddr  string
	Brokers      []string
	JournalTopic string
	Env          string
	LogLevel     string

	// Reconciler tick interval (ADR-0057 M6). 0 = default (30s).
	ReconcileInterval time.Duration

	// MySQL for transfer_ledger.
	LedgerDSN string

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
		zap.Strings("brokers", cfg.Brokers),
		zap.String("journal_topic", cfg.JournalTopic),
		zap.Any("peer_holders", cfg.PeerHolders))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Prometheus registry — one shared across framework + saga metrics.
	// framework metrics cover RPC / Kafka shapes; sagaMetrics covers
	// ADR-0057 M6 telemetry.
	promReg := metrics.NewRegistry()
	_ = metrics.NewFramework("asset", promReg)
	sagaMetrics := assetmetrics.NewSaga(promReg)

	state := engine.NewState()

	pub, err := journal.NewKafkaPublisher(journal.KafkaConfig{
		Brokers:  cfg.Brokers,
		Topic:    cfg.JournalTopic,
		ClientID: cfg.InstanceID,
		Logger:   logger,
	})
	if err != nil {
		logger.Fatal("journal producer init", zap.Error(err))
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := pub.Close(shutdownCtx); err != nil {
			logger.Warn("journal close", zap.Error(err))
		}
	}()

	svc := service.New(service.Config{ProducerID: cfg.InstanceID}, state, pub, logger)

	// Saga layer (ADR-0057 §4). Wired only when a ledger DSN is given;
	// without a DSN asset-service falls back to "funding holder only"
	// mode — useful for local dev / integration tests where MySQL is
	// not available.
	var orch *saga.Orchestrator
	var registry *holder.Registry
	var ledger *transferledger.Ledger
	var peerConns []io.Closer

	if cfg.LedgerDSN != "" {
		var err error
		ledger, err = transferledger.NewLedger(transferledger.Config{DSN: cfg.LedgerDSN})
		if err != nil {
			logger.Fatal("transfer_ledger init", zap.Error(err))
		}
		defer func() { _ = ledger.Close() }()

		registry = holder.NewRegistry()
		registry.Register("funding", holder.NewLocalFundingClient(svc))
		peerConns, err = registerPeerHolders(registry, cfg.PeerHolders)
		if err != nil {
			logger.Fatal("peer holders init", zap.Error(err))
		}
		defer func() {
			for _, c := range peerConns {
				_ = c.Close()
			}
		}()

		driver := saga.New(saga.Config{ProducerID: cfg.InstanceID}, ledger, registry, pub, logger, sagaMetrics)
		orch = saga.NewOrchestrator(saga.OrchestratorConfig{}, ledger, driver, logger, sagaMetrics)

		// Recover pending sagas BEFORE opening the gRPC listener so
		// no fresh Transfer can race a resumed driver on the same id.
		recoverCtx, cancel := context.WithTimeout(rootCtx, 60*time.Second)
		if err := orch.Recover(recoverCtx); err != nil && !errors.Is(err, context.Canceled) {
			logger.Warn("saga recover", zap.Error(err))
		}
		cancel()
	} else {
		logger.Warn("ledger DSN empty — running in funding-holder-only mode (no saga orchestrator)")
	}

	// Reconciler runs only when we have a ledger (no ledger = no state
	// to aggregate). Prometheus scrapes will still see transitions
	// counters from Driver without a reconciler running.
	var reconciler *saga.Reconciler
	var reconcilerWG sync.WaitGroup
	if ledger != nil {
		reconciler = saga.NewReconciler(saga.ReconcilerConfig{Interval: cfg.ReconcileInterval},
			ledger, sagaMetrics, logger)
		reconcilerWG.Add(1)
		go func() {
			defer reconcilerWG.Done()
			if err := reconciler.Run(rootCtx); err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("reconciler exited", zap.Error(err))
			}
		}()
	}

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

	assetHolderHandler := assetholderrpc.NewAssetHolderHTTPHandler(server.NewAssetHolderServer(svc))
	assetHandler := assetrpc.NewAssetServiceHTTPHandler(server.NewAssetServer(svc, orch))
	rpcSrv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/opentrade.rpc.assetholder.AssetHolder/"):
			assetHolderHandler.ServeHTTP(w, r)
		case strings.HasPrefix(r.URL.Path, "/opentrade.rpc.asset.AssetService/"):
			assetHandler.ServeHTTP(w, r)
		default:
			http.NotFound(w, r)
		}
	})}

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Fatal("listen", zap.Error(err))
	}

	var grpcWG sync.WaitGroup
	grpcWG.Add(1)
	go func() {
		defer grpcWG.Done()
		logger.Info("RPC listening", zap.String("addr", cfg.GRPCAddr))
		if err := rpcSrv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("rpc Serve", zap.Error(err))
		}
	}()

	<-rootCtx.Done()
	logger.Info("asset shutting down")
	shutdownCtx, cancelRPC := context.WithTimeout(context.Background(), 5*time.Second)
	_ = rpcSrv.Shutdown(shutdownCtx)
	cancelRPC()
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

// registerPeerHolders dials each peer biz_line and registers the
// resulting Client. Returns the list of opened conns so the caller can
// Close() them on shutdown. "funding" is skipped because asset-service
// is itself the funding holder — a misconfiguration that targets it is
// a hard error.
func registerPeerHolders(reg *holder.Registry, peers map[string]string) ([]io.Closer, error) {
	if len(peers) == 0 {
		return nil, nil
	}
	var conns []io.Closer
	for biz, addr := range peers {
		if biz == "funding" {
			return conns, fmt.Errorf("holder: 'funding' cannot be a peer (this service IS the funding holder)")
		}
		c, err := holder.NewGRPCClient(addr)
		if err != nil {
			return conns, fmt.Errorf("dial peer %s (%s): %w", biz, addr, err)
		}
		conns = append(conns, c)
		reg.Register(biz, c)
	}
	return conns, nil
}

func parseFlags() Config {
	var (
		instance       = flag.String("instance", "asset-main", "instance id; shows up in logs + journal ProducerID")
		grpcAddr       = flag.String("grpc", ":19000", "gRPC listen address")
		metricsAddr    = flag.String("metrics-addr", ":19090", "Prometheus scrape endpoint (empty disables /metrics HTTP)")
		brokers        = flag.String("brokers", "localhost:9092", "comma-separated Kafka bootstrap brokers")
		topic          = flag.String("journal-topic", journal.DefaultTopic, "asset-journal Kafka topic")
		env            = flag.String("env", "dev", "environment tag (dev/staging/prod)")
		level          = flag.String("log-level", "info", "log level: debug | info | warn | error")
		ledgerDSN      = flag.String("ledger-dsn", "", "MySQL DSN for opentrade_asset.transfer_ledger; empty = funding-holder-only mode")
		peerFlag       = flag.String("peer-holders", "", "biz_line peer list, e.g. 'spot=counter-0:18000,spot=counter-1:18000,futures=futures:19500'. Keys may repeat; last one wins.")
		reconcileEvery = flag.Duration("reconcile-interval", saga.DefaultReconcileInterval, "saga reconciler tick interval (refreshes saga_state_count gauge)")
	)
	flag.Parse()

	return Config{
		InstanceID:        *instance,
		GRPCAddr:          *grpcAddr,
		MetricsAddr:       *metricsAddr,
		Brokers:           splitCSV(*brokers),
		JournalTopic:      *topic,
		Env:               *env,
		LogLevel:          *level,
		LedgerDSN:         *ledgerDSN,
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

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (c Config) validate() error {
	if c.GRPCAddr == "" {
		return errors.New("grpc addr required")
	}
	if len(c.Brokers) == 0 {
		return errors.New("at least one Kafka broker required")
	}
	if c.JournalTopic == "" {
		return errors.New("journal topic required")
	}
	// ledger DSN + peer holders: optional. A deployment can start in
	// funding-holder-only mode and add saga config later.
	return nil
}
