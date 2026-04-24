// Package rest hosts BFF's HTTP API. It translates REST requests to Counter
// gRPC calls and enforces rate limiting + auth middleware.
package rest

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/pkg/auth"
	"github.com/xargin/opentrade/bff/internal/client"
	"github.com/xargin/opentrade/bff/internal/marketcache"
	"github.com/xargin/opentrade/bff/internal/ratelimit"
)

// Config bundles runtime knobs.
type Config struct {
	Addr           string
	UserRateLimit  int
	UserRateWindow time.Duration
	IPRateLimit    int
	IPRateWindow   time.Duration
	RequestTimeout time.Duration

	// AuthMiddleware overrides the middleware that extracts the
	// authenticated user id into the request context. Nil falls back to
	// the legacy X-User-Id header scheme (MVP-4 default).
	AuthMiddleware func(http.Handler) http.Handler
}

// Server is the BFF HTTP server.
type Server struct {
	cfg         Config
	counter     client.Counter
	asset       client.Asset
	trigger client.Trigger
	history     client.History
	market      *marketcache.Cache
	logger      *zap.Logger

	userLimiter *ratelimit.SlidingWindow
	ipLimiter   *ratelimit.SlidingWindow
}

// NewServer wires handlers. counter may be nil during tests that substitute
// a fake via a dedicated helper. market, trigger, history and asset
// are optional: nil disables their endpoints with 503 (ADR-0038 /
// ADR-0040 / ADR-0046 / ADR-0057).
func NewServer(cfg Config, counter client.Counter, asset client.Asset, market *marketcache.Cache, trigger client.Trigger, history client.History, logger *zap.Logger) *Server {
	return &Server{
		cfg:         cfg,
		counter:     counter,
		asset:       asset,
		trigger: trigger,
		history:     history,
		market:      market,
		logger:      logger,
		userLimiter: ratelimit.New(cfg.UserRateLimit, cfg.UserRateWindow),
		ipLimiter:   ratelimit.New(cfg.IPRateLimit, cfg.IPRateWindow),
	}
}

// Handler returns the top-level http.Handler with middleware chain applied.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/order", s.handlePlaceOrder)
	mux.HandleFunc("DELETE /v1/order/{order_id}", s.handleCancelOrder)
	mux.HandleFunc("DELETE /v1/orders", s.handleCancelMyOrders)
	mux.HandleFunc("GET /v1/order/{order_id}", s.handleQueryOrder)
	mux.HandleFunc("POST /v1/transfer", s.handleTransfer)
	mux.HandleFunc("GET /v1/transfer/{transfer_id}", s.handleQueryTransfer)
	mux.HandleFunc("GET /v1/funding-balance", s.handleQueryFundingBalance)
	mux.HandleFunc("GET /v1/account", s.handleQueryBalance)
	mux.HandleFunc("GET /v1/depth/{symbol}", s.handleDepthSnapshot)
	mux.HandleFunc("GET /v1/klines/{symbol}", s.handleKlinesRecent)
	mux.HandleFunc("POST /v1/trigger", s.handlePlaceTrigger)
	mux.HandleFunc("POST /v1/trigger/oco", s.handlePlaceOCO)
	mux.HandleFunc("DELETE /v1/trigger/{id}", s.handleCancelTrigger)
	mux.HandleFunc("GET /v1/trigger/{id}", s.handleQueryTrigger)
	mux.HandleFunc("GET /v1/trigger", s.handleListTriggers)
	mux.HandleFunc("GET /v1/orders", s.handleListOrders)
	mux.HandleFunc("GET /v1/trades", s.handleListTrades)
	mux.HandleFunc("GET /v1/account-logs", s.handleListAccountLogs)
	mux.HandleFunc("GET /v1/transfers", s.handleListTransfers)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	authMW := s.cfg.AuthMiddleware
	if authMW == nil {
		authMW = auth.Middleware
	}
	return chain(
		mux,
		recoverMW(s.logger),
		accessLog(s.logger),
		authMW,
		s.rateLimitMW,
	)
}

// ---------------------------------------------------------------------------
// Middleware helpers
// ---------------------------------------------------------------------------

func chain(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	// Apply in reverse order so the first argument runs first.
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

func recoverMW(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					logger.Error("panic in handler",
						zap.String("path", r.URL.Path),
						zap.Any("panic", rec))
					writeError(w, http.StatusInternalServerError, "internal error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func accessLog(logger *zap.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
			next.ServeHTTP(sw, r)
			logger.Info("request",
				zap.String("method", r.Method),
				zap.String("path", r.URL.Path),
				zap.Int("status", sw.code),
				zap.Duration("duration", time.Since(start)),
				zap.String("ip", clientIP(r)))
		})
	}
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (s *Server) rateLimitMW(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := clientIP(r)
		if !s.ipLimiter.Allow(ip) {
			writeError(w, http.StatusTooManyRequests, "ip rate limit exceeded")
			return
		}
		if uid, err := auth.UserID(r.Context()); err == nil {
			if !s.userLimiter.Allow(uid) {
				writeError(w, http.StatusTooManyRequests, "user rate limit exceeded")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// clientIP strips port from RemoteAddr. If a trusted reverse-proxy header
// configuration is added later, preferring X-Forwarded-For can slot in here.
func clientIP(r *http.Request) string {
	host := r.RemoteAddr
	if i := strings.LastIndex(host, ":"); i > 0 {
		host = host[:i]
	}
	return host
}

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{
		"error": msg,
	})
}

func readJSON(r *http.Request, out any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(out)
}
