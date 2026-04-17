// Package logx provides a thin wrapper around zap with opinionated defaults
// for OpenTrade services.
//
// Every service should initialize a root logger via New and derive sub-loggers
// via With. Fields that are conventional throughout the system:
//
//	service    — service name (counter / match / bff / ...)
//	shard      — shard id, when applicable
//	trace_id   — request/event trace id
//	user_id    — user id, when applicable
//	symbol     — trading symbol, when applicable
package logx

import (
	"os"
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config configures the root logger.
type Config struct {
	Service string // service name
	Level   string // debug / info / warn / error
	Env     string // dev / prod — controls encoding
}

// New creates a root logger. In dev it uses console encoding with colors; in
// prod it uses JSON.
func New(cfg Config) (*zap.Logger, error) {
	level, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		level = zapcore.InfoLevel
	}

	var enc zapcore.Encoder
	encCfg := zap.NewProductionEncoderConfig()
	encCfg.TimeKey = "ts"
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encCfg.EncodeDuration = zapcore.StringDurationEncoder
	if cfg.Env == "dev" {
		encCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
		enc = zapcore.NewConsoleEncoder(encCfg)
	} else {
		enc = zapcore.NewJSONEncoder(encCfg)
	}

	core := zapcore.NewCore(enc, zapcore.Lock(os.Stdout), level)
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
	if cfg.Service != "" {
		logger = logger.With(zap.String("service", cfg.Service))
	}
	return logger, nil
}

// --- global helpers (optional) ----------------------------------------------

var global atomic.Pointer[zap.Logger]

// SetGlobal installs l as the package-level logger used by L / S.
func SetGlobal(l *zap.Logger) {
	global.Store(l)
}

// L returns the global logger; falls back to a nop logger if not set.
func L() *zap.Logger {
	if l := global.Load(); l != nil {
		return l
	}
	return zap.NewNop()
}

// S is a convenience returning a sugared global logger.
func S() *zap.SugaredLogger { return L().Sugar() }
