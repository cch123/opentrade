// Package auth houses BFF's request authentication middleware.
//
// Three modes (ADR-0039):
//
//   - Header: trust `X-User-Id` verbatim. MVP / dev convenience; never
//     enable on the open internet.
//   - JWT: verify HS256 Bearer tokens; `sub` is the user id.
//   - APIKey: Binance-style HMAC-signed requests; `user_id` comes from the
//     key store.
//   - Mixed: try JWT → API-Key → Header in order. Useful during rollout
//     so existing tools keep working while new clients move to signed auth.
package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
)

// HeaderUserID is the request header carrying the authenticated user id in
// the legacy trust-the-header scheme.
const HeaderUserID = "X-User-Id"

// HeaderAuthorization is the standard bearer-token header.
const HeaderAuthorization = "Authorization"

type ctxKey int

const ctxKeyUserID ctxKey = 1

// ErrMissingUserID is returned when a handler that requires a user id does
// not find one in the request context.
var ErrMissingUserID = errors.New("auth: missing user id")

// Mode selects which authentication scheme the middleware enforces.
type Mode string

const (
	ModeHeader Mode = "header"
	ModeJWT    Mode = "jwt"
	ModeAPIKey Mode = "api-key"
	ModeMixed  Mode = "mixed"
)

// Config bundles everything NewMiddleware needs. Fields irrelevant to the
// selected Mode may be left zero.
type Config struct {
	Mode        Mode
	JWTSecret   []byte      // required for jwt / mixed
	APIKeyStore APIKeyStore // required for api-key / mixed
	// Now overrides time.Now — tests only.
	Now    func() time.Time
	Logger *zap.Logger
}

// Middleware is the MVP-4 header-only middleware kept for pre-MVP tests
// that want the old behaviour without configuring a Mode.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if uid := r.Header.Get(HeaderUserID); uid != "" {
			r = r.WithContext(WithUserID(r.Context(), uid))
		}
		next.ServeHTTP(w, r)
	})
}

// NewMiddleware returns a middleware that applies the Config's Mode on
// every request. A missing user id does not short-circuit the chain —
// handlers that require auth call UserID(ctx) and decide (same contract
// as the legacy Middleware).
func NewMiddleware(cfg Config) (func(http.Handler) http.Handler, error) {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	switch cfg.Mode {
	case "", ModeHeader:
		return wrap(authHeader), nil
	case ModeJWT:
		if len(cfg.JWTSecret) == 0 {
			return nil, fmt.Errorf("auth: jwt mode requires JWTSecret")
		}
		return wrap(authJWT(cfg)), nil
	case ModeAPIKey:
		if cfg.APIKeyStore == nil {
			return nil, fmt.Errorf("auth: api-key mode requires APIKeyStore")
		}
		return wrap(authAPIKey(cfg)), nil
	case ModeMixed:
		fns := []authFn{}
		if len(cfg.JWTSecret) > 0 {
			fns = append(fns, authJWT(cfg))
		}
		if cfg.APIKeyStore != nil {
			fns = append(fns, authAPIKey(cfg))
		}
		fns = append(fns, authHeader)
		return wrap(authFirstMatch(fns...)), nil
	}
	return nil, fmt.Errorf("auth: unknown mode %q", cfg.Mode)
}

// authFn extracts a user id from the request. It returns "" when this
// scheme doesn't apply to the request (caller moves on to the next).
type authFn func(r *http.Request) string

func wrap(fn authFn) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if uid := fn(r); uid != "" {
				r = r.WithContext(WithUserID(r.Context(), uid))
			}
			next.ServeHTTP(w, r)
		})
	}
}

func authHeader(r *http.Request) string {
	return r.Header.Get(HeaderUserID)
}

func authJWT(cfg Config) authFn {
	return func(r *http.Request) string {
		raw := r.Header.Get(HeaderAuthorization)
		if !strings.HasPrefix(raw, "Bearer ") {
			return ""
		}
		token := strings.TrimPrefix(raw, "Bearer ")
		if token == "" {
			return ""
		}
		uid, err := VerifyHS256(token, cfg.JWTSecret, cfg.Now())
		if err != nil {
			cfg.Logger.Debug("jwt verify failed",
				zap.String("path", r.URL.Path), zap.Error(err))
			return ""
		}
		return uid
	}
}

func authAPIKey(cfg Config) authFn {
	return func(r *http.Request) string {
		if r.Header.Get(HeaderAPIKey) == "" {
			return ""
		}
		uid, err := VerifyAPIKeyRequest(r, cfg.APIKeyStore, cfg.Now())
		if err != nil {
			cfg.Logger.Debug("api-key verify failed",
				zap.String("path", r.URL.Path), zap.Error(err))
			return ""
		}
		return uid
	}
}

func authFirstMatch(fns ...authFn) authFn {
	return func(r *http.Request) string {
		for _, fn := range fns {
			if uid := fn(r); uid != "" {
				return uid
			}
		}
		return ""
	}
}

// WithUserID attaches a user id to ctx.
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, ctxKeyUserID, userID)
}

// UserID retrieves the user id attached by Middleware, or ErrMissingUserID.
func UserID(ctx context.Context) (string, error) {
	v, ok := ctx.Value(ctxKeyUserID).(string)
	if !ok || v == "" {
		return "", ErrMissingUserID
	}
	return v, nil
}
