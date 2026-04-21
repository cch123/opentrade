// Command asset runs the OpenTrade asset-service (ADR-0057).
//
// Responsibilities shipped in this binary:
//
//   - Serve the AssetHolder gRPC (TransferOut / TransferIn /
//     CompensateTransferOut) for the biz_line=funding account book.
//   - Serve the AssetService gRPC read path (QueryFundingBalance); the
//     saga orchestrator path (Transfer + QueryTransfer) lands in M3b.
//   - Publish FundingTransferOut/In/Compensate envelopes to the
//     asset-journal Kafka topic so trade-dump can project into MySQL.
//
// Not yet wired (tracked in ADR-0057 follow-ups):
//
//   - Saga orchestrator + transfer_ledger recovery (M3b)
//   - Snapshot of in-memory funding state + HA cold-standby via etcd
//     lease (M3c)
//   - gRPC clients to Counter / Futures AssetHolders (M3b, plus biz_line
//     routing table)
//
// Single-instance for MVP; partitioning / sharding is deferred per
// ADR-0057 open question.
package main

import (
	"context"
	"errors"
	"flag"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	assetrpc "github.com/xargin/opentrade/api/gen/rpc/asset"
	assetholderrpc "github.com/xargin/opentrade/api/gen/rpc/assetholder"
	"github.com/xargin/opentrade/asset/internal/engine"
	"github.com/xargin/opentrade/asset/internal/journal"
	"github.com/xargin/opentrade/asset/internal/server"
	"github.com/xargin/opentrade/asset/internal/service"
	"github.com/xargin/opentrade/pkg/logx"
)

type Config struct {
	InstanceID   string
	GRPCAddr     string
	Brokers      []string
	JournalTopic string
	Env          string
	LogLevel     string
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
		zap.String("journal_topic", cfg.JournalTopic))

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	grpcServer := grpc.NewServer()
	assetholderrpc.RegisterAssetHolderServer(grpcServer, server.NewAssetHolderServer(svc))
	assetrpc.RegisterAssetServiceServer(grpcServer, server.NewAssetServer(svc))

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Fatal("listen", zap.Error(err))
	}

	var grpcWG sync.WaitGroup
	grpcWG.Add(1)
	go func() {
		defer grpcWG.Done()
		logger.Info("gRPC listening", zap.String("addr", cfg.GRPCAddr))
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			logger.Error("grpc Serve", zap.Error(err))
		}
	}()

	<-rootCtx.Done()
	logger.Info("asset shutting down")
	grpcServer.GracefulStop()
	grpcWG.Wait()
	logger.Info("asset shutdown complete")
}

func parseFlags() Config {
	var (
		instance = flag.String("instance", "asset-main", "instance id; shows up in logs + journal ProducerID")
		grpcAddr = flag.String("grpc", ":19000", "gRPC listen address")
		brokers  = flag.String("brokers", "localhost:9092", "comma-separated Kafka bootstrap brokers")
		topic    = flag.String("journal-topic", journal.DefaultTopic, "asset-journal Kafka topic")
		env      = flag.String("env", "dev", "environment tag (dev/staging/prod)")
		level    = flag.String("log-level", "info", "log level: debug | info | warn | error")
	)
	flag.Parse()

	return Config{
		InstanceID:   *instance,
		GRPCAddr:     *grpcAddr,
		Brokers:      splitCSV(*brokers),
		JournalTopic: *topic,
		Env:          *env,
		LogLevel:     *level,
	}
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
	return nil
}
