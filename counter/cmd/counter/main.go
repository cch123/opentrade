// Command counter runs the OpenTrade Counter service.
//
// MVP-0: scaffold only — the main function wires logger + config and blocks on
// a signal. Business wiring arrives in MVP-2.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/xargin/opentrade/pkg/logx"
	"go.uber.org/zap"
)

// Config is the Counter service configuration.
type Config struct {
	Service string `yaml:"service"`
	LogLevel string `yaml:"log_level"`
	Env      string `yaml:"env"`
	ShardID  int    `yaml:"shard_id"`
}

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "", "path to YAML config file")
	flag.Parse()

	cfg := Config{Service: "counter", LogLevel: "info", Env: "dev"}
	// TODO(MVP-2): config.Load(cfgPath, &cfg)
	_ = cfgPath

	logger, err := logx.New(logx.Config{Service: cfg.Service, Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	logx.SetGlobal(logger)

	logger.Info("counter starting", zap.Int("shard_id", cfg.ShardID))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// TODO(MVP-2): bootstrap engine, journal producer, trade-event consumer,
	// gRPC server, etcd election, etc.

	<-ctx.Done()
	logger.Info("counter shutting down")
}
