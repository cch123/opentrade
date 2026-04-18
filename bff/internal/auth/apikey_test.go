package auth

import (
	"bytes"
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type fakeStore struct{ m map[string]struct{ s, u string } }

func (f *fakeStore) Lookup(k string) ([]byte, string, bool) {
	if v, ok := f.m[k]; ok {
		return []byte(v.s), v.u, true
	}
	return nil, "", false
}

func TestVerifyAPIKey_Success(t *testing.T) {
	store := &fakeStore{m: map[string]struct{ s, u string }{
		"K": {s: "S", u: "alice"},
	}}
	now := time.Unix(1_700_000_000, 0)
	body := []byte(`{"symbol":"BTC-USDT"}`)
	rawQ := "symbol=BTC-USDT&timestamp=" + itoa(now.UnixMilli())
	sig := SignAPIKeyRequest([]byte("S"), rawQ, body)
	r := httptest.NewRequest("POST", "/v1/order?"+rawQ+"&signature="+sig, bytes.NewReader(body))
	r.Header.Set(HeaderAPIKey, "K")

	uid, err := VerifyAPIKeyRequest(r, store, now)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if uid != "alice" {
		t.Errorf("uid = %q", uid)
	}
	// Body must still be readable by downstream handlers.
	got, _ := io.ReadAll(r.Body)
	if string(got) != string(body) {
		t.Errorf("body not restored: %q", got)
	}
}

func TestVerifyAPIKey_BadSig(t *testing.T) {
	store := &fakeStore{m: map[string]struct{ s, u string }{"K": {s: "S", u: "u1"}}}
	now := time.Unix(1_700_000_000, 0)
	r := httptest.NewRequest("GET", "/x?timestamp="+itoa(now.UnixMilli())+"&signature=deadbeef", nil)
	r.Header.Set(HeaderAPIKey, "K")
	_, err := VerifyAPIKeyRequest(r, store, now)
	if !errors.Is(err, ErrAPIKeyBadSig) {
		t.Errorf("err = %v", err)
	}
}

func TestVerifyAPIKey_UnknownKey(t *testing.T) {
	store := &fakeStore{m: map[string]struct{ s, u string }{}}
	r := httptest.NewRequest("GET", "/x?timestamp=1&signature=abc", nil)
	r.Header.Set(HeaderAPIKey, "nope")
	_, err := VerifyAPIKeyRequest(r, store, time.Now())
	if !errors.Is(err, ErrAPIKeyUnknown) {
		t.Errorf("err = %v", err)
	}
}

func TestVerifyAPIKey_Stale(t *testing.T) {
	store := &fakeStore{m: map[string]struct{ s, u string }{"K": {s: "S", u: "u1"}}}
	now := time.Unix(1_700_000_000, 0)
	old := now.Add(-time.Hour)
	rawQ := "timestamp=" + itoa(old.UnixMilli())
	sig := SignAPIKeyRequest([]byte("S"), rawQ, nil)
	r := httptest.NewRequest("GET", "/x?"+rawQ+"&signature="+sig, nil)
	r.Header.Set(HeaderAPIKey, "K")
	if _, err := VerifyAPIKeyRequest(r, store, now); !errors.Is(err, ErrAPIKeyStale) {
		t.Errorf("err = %v, want ErrAPIKeyStale", err)
	}
}

func TestVerifyAPIKey_Missing(t *testing.T) {
	r := httptest.NewRequest("GET", "/x", nil)
	if _, err := VerifyAPIKeyRequest(r, &fakeStore{m: map[string]struct{ s, u string }{}}, time.Now()); !errors.Is(err, ErrAPIKeyMissing) {
		t.Errorf("err = %v", err)
	}
}

func TestMemoryStore_FileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	content := `{"keys":[{"key":"K1","secret":"S1","user_id":"u1"},{"key":"K2","secret":"S2","user_id":"u2"}]}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := NewMemoryStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if secret, uid, ok := store.Lookup("K1"); !ok || string(secret) != "S1" || uid != "u1" {
		t.Errorf("K1: %q %q %v", secret, uid, ok)
	}
	if _, _, ok := store.Lookup("nope"); ok {
		t.Error("unexpected lookup success")
	}
}

func TestMemoryStore_DuplicateKeysRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keys.json")
	content := `{"keys":[{"key":"K","secret":"a","user_id":"u"},{"key":"K","secret":"b","user_id":"v"}]}`
	_ = os.WriteFile(path, []byte(content), 0o600)
	if _, err := NewMemoryStore(path); err == nil {
		t.Fatal("expected duplicate-key error")
	}
}

func TestMemoryStore_EmptyPathIsEmptyStore(t *testing.T) {
	store, err := NewMemoryStore("")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, ok := store.Lookup("any"); ok {
		t.Error("empty store must miss everything")
	}
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := false
	if v < 0 {
		neg = true
		v = -v
	}
	var b [20]byte
	pos := len(b)
	for v > 0 {
		pos--
		b[pos] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
