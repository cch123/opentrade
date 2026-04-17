// Command quote runs the OpenTrade market-data / quote service.
//
// MVP-0: scaffold only. MVP-6 adds trade-event consumer + depth/kline/trades
// producers on the market-data topic.
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
}

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "", "path to YAML config file")
	flag.Parse()

	cfg := Config{Service: "quote", LogLevel: "info", Env: "dev"}
	_ = cfgPath

	logger, err := logx.New(logx.Config{Service: cfg.Service, Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	logx.SetGlobal(logger)

	logger.Info("quote starting")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()
	logger.Info("quote shutting down")
}
