// Package auth houses BFF's request authentication middleware. MVP-4 uses a
// trivial header-based identity (X-User-Id); production-grade auth (JWT /
// API-Key / signatures) is wired in a later MVP.
package auth

import (
	"context"
	"errors"
	"net/http"
)

// HeaderUserID is the request header carrying the authenticated user id in
// the MVP-4 trust-the-header scheme.
const HeaderUserID = "X-User-Id"

type ctxKey int

const ctxKeyUserID ctxKey = 1

// ErrMissingUserID is returned when a handler that requires a user id does
// not find one in the request context.
var ErrMissingUserID = errors.New("auth: missing user id")

// Middleware extracts X-User-Id and puts it on the request context. If the
// header is missing, the handler chain still runs — downstream handlers that
// need a user id should call UserID(ctx) and respond 401 on miss.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if uid := r.Header.Get(HeaderUserID); uid != "" {
			r = r.WithContext(WithUserID(r.Context(), uid))
		}
		next.ServeHTTP(w, r)
	})
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
