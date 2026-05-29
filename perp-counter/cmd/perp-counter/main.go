// Command perp-counter is the account-truth service for USDT-margined linear
// perpetuals (ADR-0068 §2, A1: independent service alongside the spot
// Counter). This is the M1 skeleton: the Connect/h2c gRPC server stands up
// and the read paths (QueryPositions / QueryMargin) are live against the
// engine. PlaceOrder/Cancel (Match dispatch + per-user sequencer, M3),
// Kafka journaling, mark-price consumption (M4), funding (M5), liquidation
// (M6), HA and snapshot persistence are wired in later milestones.
package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/api/gen/rpc/perp/perprpcconnect"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/perp-counter/internal/server"
	"github.com/xargin/opentrade/perp-counter/internal/service"
	"github.com/xargin/opentrade/pkg/connectx"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/idgen"
	"github.com/xargin/opentrade/pkg/logx"
)

// Config holds the perp-counter CLI flags.
type Config struct {
	GRPCAddr    string
	DefaultMMR  string
	MaxLeverage string
	IDGenShard  int
	Env         string
	LogLevel    string
}

func main() {
	var cfg Config
	flag.StringVar(&cfg.GRPCAddr, "grpc-addr", ":8086", "gRPC (Connect/h2c) listen address")
	flag.StringVar(&cfg.DefaultMMR, "default-mmr", "0.005",
		"default maintenance margin rate for the derived liq price (ADR-0068; per-symbol override is M6)")
	flag.StringVar(&cfg.MaxLeverage, "max-leverage", "125", "max leverage accepted at PlaceOrder (0 = no cap)")
	flag.IntVar(&cfg.IDGenShard, "idgen-shard", 0, "snowflake shard id for perp order ids (avoid collisions with counter)")
	flag.StringVar(&cfg.Env, "env", "dev", "environment: dev | prod")
	flag.StringVar(&cfg.LogLevel, "log-level", "info", "log level")
	flag.Parse()

	logger, err := logx.New(logx.Config{Service: "perp-counter", Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	logx.SetGlobal(logger)

	mmr, err := dec.Parse(cfg.DefaultMMR)
	if err != nil {
		logger.Fatal("invalid --default-mmr", zap.Error(err))
	}

	maxLev, err := dec.Parse(cfg.MaxLeverage)
	if err != nil {
		logger.Fatal("invalid --max-leverage", zap.Error(err))
	}
	idg, err := idgen.NewGenerator(cfg.IDGenShard)
	if err != nil {
		logger.Fatal("idgen", zap.Error(err))
	}

	eng := engine.New()
	// M3: Match dispatch + perp-journal are not wired yet (no Kafka), so the
	// service runs with no-op sinks — PlaceOrder reserves margin and records
	// the order, but nothing fills until the producer/consumer land. The
	// pre-trade margin gate, reduce_only check, and query paths are live.
	svc := service.New(eng, nil, nil, idg.Next, service.Config{
		ShardID: cfg.IDGenShard, ProducerID: "perp-shard-0-main", MaxLeverage: maxLev,
	})

	mux := http.NewServeMux()
	path, handler := perprpcconnect.NewPerpServiceHandler(server.New(eng, svc, mmr))
	mux.Handle(path, handler)
	httpSrv := connectx.NewH2CServer(cfg.GRPCAddr, mux)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("perp-counter starting (ADR-0068 M1 skeleton)",
		zap.String("grpc", cfg.GRPCAddr), zap.String("default_mmr", cfg.DefaultMMR))
	logger.Warn("M1 scope: read paths live; PlaceOrder/Cancel pending M3; Kafka/markprice/funding/liquidation/HA pending later milestones")

	go func() {
		logger.Info("gRPC (Connect/h2c) listening", zap.String("addr", cfg.GRPCAddr))
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("grpc serve", zap.Error(err))
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("perp-counter shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown", zap.Error(err))
	}
	_ = logger.Sync()
}
