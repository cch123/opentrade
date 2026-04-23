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

func TestMiddleware_Mixed_BadJWTDoesNotFallBackToHeader(t *testing.T) {
	secret := []byte("some-secret-32-chars-or-more-aaaa")
	h := newHandler(t, Config{Mode: ModeMixed, JWTSecret: secret})
	r := httptest.NewRequest("GET", "/x", nil)
	r.Header.Set(HeaderAuthorization, "Bearer not-a-real-jwt")
	r.Header.Set(HeaderUserID, "spoofed")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	if rr.Body.String() != "-" {
		t.Errorf("body = %q, want anonymous after bad jwt", rr.Body.String())
	}
}

func TestMiddleware_Mixed_BadAPIKeyDoesNotFallBackToHeader(t *testing.T) {
	store := &fakeStore{m: map[string]struct{ s, u, role string }{
		"K": {s: "S", u: "api-user", role: RoleUser},
	}}
	now := time.Unix(1_700_000_000, 0)
	h := newHandler(t, Config{
		Mode:        ModeMixed,
		APIKeyStore: store,
		Now:         func() time.Time { return now },
	})
	r := httptest.NewRequest("GET", "/x?timestamp="+itoa(now.UnixMilli())+"&signature=deadbeef", nil)
	r.Header.Set(HeaderAPIKey, "K")
	r.Header.Set(HeaderUserID, "spoofed")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	if rr.Body.String() != "-" {
		t.Errorf("body = %q, want anonymous after bad api key", rr.Body.String())
	}
}

func TestMiddleware_UnknownModeFails(t *testing.T) {
	if _, err := NewMiddleware(Config{Mode: "gnmo"}); err == nil {
		t.Fatal("expected error for unknown mode")
	}
}

func TestMiddleware_APIKey_AttachesRole(t *testing.T) {
	store := &fakeStore{m: map[string]struct{ s, u, role string }{
		"K": {s: "S", u: "ops", role: RoleAdmin},
	}}
	now := time.Unix(1_700_000_000, 0)
	rawQ := "timestamp=" + itoa(now.UnixMilli())
	sig := SignAPIKeyRequest([]byte("S"), rawQ, nil)

	// Terminal handler records both user id and role.
	var gotUser, gotRole string
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if uid, err := UserID(r.Context()); err == nil {
			gotUser = uid
		}
		gotRole = Role(r.Context())
	})
	mw, err := NewMiddleware(Config{
		Mode:        ModeAPIKey,
		APIKeyStore: store,
		Now:         func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/admin/ping?"+rawQ+"&signature="+sig, nil)
	r.Header.Set(HeaderAPIKey, "K")
	mw(h).ServeHTTP(httptest.NewRecorder(), r)

	if gotUser != "ops" || gotRole != RoleAdmin {
		t.Fatalf("user=%q role=%q", gotUser, gotRole)
	}
}

func TestRequireAdmin_BlocksUserRole(t *testing.T) {
	h := RequireAdmin(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	// No role in ctx → defaults to RoleUser → 403.
	r := httptest.NewRequest("GET", "/admin/x", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("anon: got %d, want 403", rr.Code)
	}

	// Explicit user role → 403.
	r2 := httptest.NewRequest("GET", "/admin/x", nil)
	r2 = r2.WithContext(WithRole(r2.Context(), RoleUser))
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, r2)
	if rr2.Code != http.StatusForbidden {
		t.Fatalf("user: got %d, want 403", rr2.Code)
	}

	// Admin role → 200.
	r3 := httptest.NewRequest("GET", "/admin/x", nil)
	r3 = r3.WithContext(WithRole(r3.Context(), RoleAdmin))
	rr3 := httptest.NewRecorder()
	h.ServeHTTP(rr3, r3)
	if rr3.Code != http.StatusOK {
		t.Fatalf("admin: got %d, want 200", rr3.Code)
	}
}
