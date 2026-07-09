package main

import (
	"strings"
	"testing"
	"time"
)

func validConfigForTest() Config {
	return Config{
		HTTPAddr:          ":8080",
		CounterShards:     []string{"127.0.0.1:8081"},
		ReadHeaderTimeout: time.Second,
		ReadTimeout:       time.Second,
		WriteTimeout:      time.Second,
		IdleTimeout:       time.Second,
		AuthMode:          "header",
		ClusteringMode:    "disabled",
		Env:               "dev",
	}
}

func TestConfigValidateRejectsUnsignedProductionAuth(t *testing.T) {
	for _, mode := range []string{"header", "mixed"} {
		t.Run(mode, func(t *testing.T) {
			cfg := validConfigForTest()
			cfg.Env = "prod"
			cfg.AuthMode = mode
			if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "unsafe") {
				t.Fatalf("validate() error = %v, want unsafe production auth error", err)
			}
		})
	}
}

func TestConfigValidateAcceptsSignedProductionAuth(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
	}{
		{name: "jwt", edit: func(cfg *Config) {
			cfg.AuthMode = "jwt"
			cfg.JWTSecret = "test-secret"
		}},
		{name: "api-key", edit: func(cfg *Config) {
			cfg.AuthMode = "api-key"
			cfg.APIKeysFile = "/run/secrets/api-keys.json"
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfigForTest()
			cfg.Env = "prod"
			tt.edit(&cfg)
			if err := cfg.validate(); err != nil {
				t.Fatalf("validate() error = %v", err)
			}
		})
	}
}

func TestConfigValidateRequiresAuthMaterialAndTimeouts(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
	}{
		{name: "jwt secret", edit: func(cfg *Config) { cfg.AuthMode = "jwt" }},
		{name: "api key file", edit: func(cfg *Config) { cfg.AuthMode = "api-key" }},
		{name: "read timeout", edit: func(cfg *Config) { cfg.ReadTimeout = 0 }},
		{name: "unknown environment", edit: func(cfg *Config) { cfg.Env = "production" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfigForTest()
			tt.edit(&cfg)
			if err := cfg.validate(); err == nil {
				t.Fatal("validate() succeeded, want error")
			}
		})
	}
}
