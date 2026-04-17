// Command trade-dump runs the OpenTrade persistence sidecar, consuming
// counter-journal / trade-event and writing them to MySQL idempotently.
//
// MVP-0: scaffold only. MVP-7 adds consumers + MySQL writers.
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
	MySQLDSN string `yaml:"mysql_dsn"`
}

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "", "path to YAML config file")
	flag.Parse()

	cfg := Config{Service: "trade-dump", LogLevel: "info", Env: "dev"}
	_ = cfgPath

	logger, err := logx.New(logx.Config{Service: cfg.Service, Level: cfg.LogLevel, Env: cfg.Env})
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	logx.SetGlobal(logger)

	logger.Info("trade-dump starting")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	<-ctx.Done()
	logger.Info("trade-dump shutting down")
}
