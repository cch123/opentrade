// Command match runs the OpenTrade Match (matching engine) service.
//
// MVP-0: scaffold only. MVP-1 adds Kafka order-event consumer + orderbook +
// trade-event producer + snapshots.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/xargin/opentrade/pkg/logx"
)

type Config struct {
	Service  string `yaml:"service"`
	LogLevel string `yaml:"log_level"`
	Env      string `yaml:"env"`
	ShardID  string `yaml:"shard_id"` // e.g. "match-shard-0"
}

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "", "path to YAML config file")
	flag.Parse()

	cfg := Config{Service: "match", LogLevel: "info", Env: "dev"}
	_ = cfgPath

	logger, err := logx.New(logx.Config{Service: cfg.Service, Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	logx.SetGlobal(logger)

	logger.Info("match starting")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()
	logger.Info("match shutting down")
}
