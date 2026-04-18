package auth

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"
)

// terminalHandler peeks the user id off the context and writes it as the
// response body so tests can assert authentication outcomes by observing
// the HTTP layer.
var terminalHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	if uid, err := UserID(r.Context()); err == nil {
		_, _ = io.WriteString(w, uid)
		return
	}
	_, _ = io.WriteString(w, "-")
})

func newHandler(t *testing.T, cfg Config) http.Handler {
	t.Helper()
	cfg.Logger = zap.NewNop()
	mw, err := NewMiddleware(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return mw(terminalHandler)
}

func TestMiddleware_Header(t *testing.T) {
	h := newHandler(t, Config{Mode: ModeHeader})
	r := httptest.NewRequest("GET", "/x", nil)
	r.Header.Set(HeaderUserID, "alice")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	if rr.Body.String() != "alice" {
		t.Errorf("body = %q", rr.Body.String())
	}
}

func TestMiddleware_JWT(t *testing.T) {
	secret := []byte("secret-32-bytes-is-enough-for-now")
	now := time.Unix(1_700_000_000, 0)
	tok, _ := SignHS256("alice", secret, now, now.Add(time.Hour))
	h := newHandler(t, Config{
		Mode:      ModeJWT,
		JWTSecret: secret,
		Now:       func() time.Time { return now.Add(5 * time.Minute) },
	})
	r := httptest.NewRequest("GET", "/x", nil)
	r.Header.Set(HeaderAuthorization, "Bearer "+tok)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	if rr.Body.String() != "alice" {
		t.Errorf("body = %q", rr.Body.String())
	}
}

func TestMiddleware_JWT_BadTokenResolvesAnonymous(t *testing.T) {
	secret := []byte("secret-32-bytes-is-enough-for-now")
	h := newHandler(t, Config{Mode: ModeJWT, JWTSecret: secret})
	r := httptest.NewRequest("GET", "/x", nil)
	r.Header.Set(HeaderAuthorization, "Bearer not-a-real-jwt")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	if rr.Body.String() != "-" {
		t.Errorf("body = %q (bad jwt must fall through to anon, not crash)", rr.Body.String())
	}
}

func TestMiddleware_JWT_MissingSecretRejectsConfig(t *testing.T) {
	if _, err := NewMiddleware(Config{Mode: ModeJWT}); err == nil {
		t.Fatal("expected error for jwt without secret")
	}
}

func TestMiddleware_Mixed_PrefersJWT(t *testing.T) {
	secret := []byte("mixed-secret-bytes-32-chars-abcde")
	now := time.Unix(1_700_000_000, 0)
	tok, _ := SignHS256("jwt-user", secret, now, now.Add(time.Hour))
	h := newHandler(t, Config{
		Mode:      ModeMixed,
		JWTSecret: secret,
		Now:       func() time.Time { return now },
	})
	// Client provides BOTH a Bearer JWT and a legacy X-User-Id header.
	// JWT should win.
	r := httptest.NewRequest("GET", "/x", nil)
	r.Header.Set(HeaderAuthorization, "Bearer "+tok)
	r.Header.Set(HeaderUserID, "header-user")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	if rr.Body.String() != "jwt-user" {
		t.Errorf("body = %q, want jwt-user", rr.Body.String())
	}
}

func TestMiddleware_Mixed_FallsBackToHeader(t *testing.T) {
	secret := []byte("some-secret-32-chars-or-more-aaaa")
	h := newHandler(t, Config{Mode: ModeMixed, JWTSecret: secret})
	r := httptest.NewRequest("GET", "/x", nil)
	r.Header.Set(HeaderUserID, "legacy")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	if rr.Body.String() != "legacy" {
		t.Errorf("body = %q", rr.Body.String())
	}
}

func TestMiddleware_UnknownModeFails(t *testing.T) {
	if _, err := NewMiddleware(Config{Mode: "gnmo"}); err == nil {
		t.Fatal("expected error for unknown mode")
	}
}
