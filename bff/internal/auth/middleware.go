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

const (
	ctxKeyUserID ctxKey = 1
	ctxKeyRole   ctxKey = 2
)

// ErrMissingUserID is returned when a handler that requires a user id does
// not find one in the request context.
var ErrMissingUserID = errors.New("auth: missing user id")

// ErrForbidden is returned when an authenticated user's role does not
// authorize them for the requested endpoint.
var ErrForbidden = errors.New("auth: forbidden")

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

// AdminMiddleware builds an auth middleware that only recognises admin-role
// API-Keys. JWT / header schemes are ignored: admin access always requires
// a signed API-Key request, so a stolen JWT can't escalate privileges
// (ADR-0052).
func AdminMiddleware(store APIKeyStore, logger *zap.Logger) (func(http.Handler) http.Handler, error) {
	if store == nil {
		return nil, fmt.Errorf("auth: admin middleware requires APIKeyStore")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	cfg := Config{Mode: ModeAPIKey, APIKeyStore: store, Logger: logger, Now: time.Now}
	return wrap(authAPIKey(cfg)), nil
}

// authResult is the (user_id, role) a chain element extracted from the
// request. Empty user_id means the scheme did not apply and the next
// scheme (in mixed mode) should be tried.
type authResult struct {
	userID string
	role   string
}

// authFn extracts auth details from the request. Empty result.userID means
// this scheme doesn't apply (caller moves on to the next).
type authFn func(r *http.Request) authResult

func wrap(fn authFn) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			res := fn(r)
			if res.userID != "" {
				ctx := WithUserID(r.Context(), res.userID)
				role := res.role
				if role == "" {
					role = RoleUser
				}
				ctx = WithRole(ctx, role)
				r = r.WithContext(ctx)
			}
			next.ServeHTTP(w, r)
		})
	}
}

func authHeader(r *http.Request) authResult {
	return authResult{userID: r.Header.Get(HeaderUserID), role: RoleUser}
}

func authJWT(cfg Config) authFn {
	return func(r *http.Request) authResult {
		raw := r.Header.Get(HeaderAuthorization)
		if !strings.HasPrefix(raw, "Bearer ") {
			return authResult{}
		}
		token := strings.TrimPrefix(raw, "Bearer ")
		if token == "" {
			return authResult{}
		}
		uid, err := VerifyHS256(token, cfg.JWTSecret, cfg.Now())
		if err != nil {
			cfg.Logger.Debug("jwt verify failed",
				zap.String("path", r.URL.Path), zap.Error(err))
			return authResult{}
		}
		return authResult{userID: uid, role: RoleUser}
	}
}

func authAPIKey(cfg Config) authFn {
	return func(r *http.Request) authResult {
		if r.Header.Get(HeaderAPIKey) == "" {
			return authResult{}
		}
		uid, role, err := VerifyAPIKeyRequest(r, cfg.APIKeyStore, cfg.Now())
		if err != nil {
			cfg.Logger.Debug("api-key verify failed",
				zap.String("path", r.URL.Path), zap.Error(err))
			return authResult{}
		}
		return authResult{userID: uid, role: role}
	}
}

func authFirstMatch(fns ...authFn) authFn {
	return func(r *http.Request) authResult {
		for _, fn := range fns {
			if res := fn(r); res.userID != "" {
				return res
			}
		}
		return authResult{}
	}
}

// WithUserID attaches a user id to ctx.
func WithUserID(ctx context.Context, userID string) context.Context {
	return context.WithValue(ctx, ctxKeyUserID, userID)
}

// WithRole attaches a role to ctx. Intended for middleware use.
func WithRole(ctx context.Context, role string) context.Context {
	return context.WithValue(ctx, ctxKeyRole, role)
}

// UserID retrieves the user id attached by Middleware, or ErrMissingUserID.
func UserID(ctx context.Context) (string, error) {
	v, ok := ctx.Value(ctxKeyUserID).(string)
	if !ok || v == "" {
		return "", ErrMissingUserID
	}
	return v, nil
}

// Role retrieves the role attached by Middleware. Unauthenticated requests
// return RoleUser so handlers don't have to special-case anonymous — admin
// access is gated by RequireAdmin, which checks for RoleAdmin explicitly.
func Role(ctx context.Context) string {
	v, ok := ctx.Value(ctxKeyRole).(string)
	if !ok || v == "" {
		return RoleUser
	}
	return v
}

// RequireAdmin wraps next so that only requests whose context carries
// RoleAdmin reach the handler. Everything else (user role, anonymous)
// returns 403.
func RequireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if Role(r.Context()) != RoleAdmin {
			http.Error(w, "admin role required", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}
