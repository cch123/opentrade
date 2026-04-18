package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// API-Key auth — Binance-style: every authenticated request carries
// `X-MBX-APIKEY` plus a `timestamp` (ms epoch) and `signature` (hex
// HMAC-SHA256) query parameter. The signature covers the raw query string
// minus the signature itself, concatenated with the raw request body.
//
// This lets us reuse the same flow for GET (body empty) and POST, and is
// the convention any Binance SDK already knows how to sign.
//
// Keys live in a JSON file loaded at boot:
//
//	{"keys": [
//	  {"key": "...hex...", "secret": "...hex...", "user_id": "alice"},
//	  ...
//	]}
//
// MVP: file is read at startup, rotation requires a BFF restart. A
// live-reload watcher + /admin/keys endpoints are future work.

// RecvWindow is the tolerance (in ms) between the client's declared
// timestamp and the server's clock. Anything more skewed than this
// is treated as a replay / clock-drift attempt.
const RecvWindow = 5 * time.Second

// APIKeyStore resolves an api-key to its (secret, user_id) tuple. Any
// implementation must be safe for concurrent reads.
type APIKeyStore interface {
	Lookup(key string) (secret []byte, userID string, ok bool)
}

// -----------------------------------------------------------------------------
// In-memory store (JSON file backed)
// -----------------------------------------------------------------------------

type apiKeyRecord struct {
	Key    string `json:"key"`
	Secret string `json:"secret"`
	UserID string `json:"user_id"`
}

type apiKeysFile struct {
	Keys []apiKeyRecord `json:"keys"`
}

type memoryStore struct {
	mu sync.RWMutex
	m  map[string]struct {
		secret []byte
		user   string
	}
}

// NewMemoryStore loads an APIKeyStore from a JSON file. Duplicate keys in
// the file are an error (ambiguous resolution). Returns an empty store for
// an empty path so the flag `--api-keys-file=""` means "no api-key auth".
func NewMemoryStore(path string) (APIKeyStore, error) {
	s := &memoryStore{m: make(map[string]struct {
		secret []byte
		user   string
	})}
	if path == "" {
		return s, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("auth: open api keys %s: %w", path, err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("auth: read api keys: %w", err)
	}
	var parsed apiKeysFile
	if err := json.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("auth: decode api keys: %w", err)
	}
	for _, r := range parsed.Keys {
		if r.Key == "" || r.Secret == "" || r.UserID == "" {
			return nil, fmt.Errorf("auth: api key entry missing field: %+v", r)
		}
		if _, dup := s.m[r.Key]; dup {
			return nil, fmt.Errorf("auth: duplicate api key %q", r.Key)
		}
		s.m[r.Key] = struct {
			secret []byte
			user   string
		}{secret: []byte(r.Secret), user: r.UserID}
	}
	return s, nil
}

// Lookup implements APIKeyStore.
func (s *memoryStore) Lookup(key string) ([]byte, string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[key]
	if !ok {
		return nil, "", false
	}
	return v.secret, v.user, true
}

// -----------------------------------------------------------------------------
// Signature verification
// -----------------------------------------------------------------------------

// HeaderAPIKey is the header carrying the client's api-key id.
const HeaderAPIKey = "X-MBX-APIKEY"

var (
	// ErrAPIKeyMissing: no X-MBX-APIKEY header.
	ErrAPIKeyMissing = errors.New("auth: missing api key")
	// ErrAPIKeyUnknown: key not found in store.
	ErrAPIKeyUnknown = errors.New("auth: unknown api key")
	// ErrAPIKeyBadSig: signature did not match / was absent.
	ErrAPIKeyBadSig = errors.New("auth: bad signature")
	// ErrAPIKeyStale: timestamp drift > RecvWindow.
	ErrAPIKeyStale = errors.New("auth: stale timestamp")
)

// VerifyAPIKeyRequest authenticates an incoming request against store. On
// success returns the authenticated user_id. The request body is read in
// full; caller should restore it afterward if downstream handlers need it.
//
// The signing string is `rawQueryWithoutSignature + "|" + body`. The
// separator keeps empty-body GETs unambiguously distinct from a POST whose
// body starts with `&`.
func VerifyAPIKeyRequest(r *http.Request, store APIKeyStore, now time.Time) (string, error) {
	key := r.Header.Get(HeaderAPIKey)
	if key == "" {
		return "", ErrAPIKeyMissing
	}
	secret, userID, ok := store.Lookup(key)
	if !ok {
		return "", ErrAPIKeyUnknown
	}
	q := r.URL.Query()
	sigHex := q.Get("signature")
	tsStr := q.Get("timestamp")
	if sigHex == "" || tsStr == "" {
		return "", fmt.Errorf("%w: timestamp + signature required", ErrAPIKeyBadSig)
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return "", fmt.Errorf("%w: bad timestamp: %v", ErrAPIKeyBadSig, err)
	}
	diff := now.UnixMilli() - ts
	if diff < -int64(RecvWindow/time.Millisecond) || diff > int64(RecvWindow/time.Millisecond) {
		return "", ErrAPIKeyStale
	}

	// Build the signing string: drop `signature`, keep all other params
	// in their on-the-wire order via RawQuery filtering.
	signingQuery := queryWithoutSignature(r.URL.RawQuery)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return "", fmt.Errorf("%w: body read: %v", ErrAPIKeyBadSig, err)
	}
	// Restore body for downstream handlers.
	_ = r.Body.Close()
	r.Body = &rewindableBody{data: body}

	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signingQuery))
	mac.Write([]byte("|"))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(sigHex)) {
		return "", ErrAPIKeyBadSig
	}
	return userID, nil
}

// queryWithoutSignature strips the `signature=` key/value pair from raw,
// preserving the original ordering of the remaining pairs. We do this at
// the string level (rather than via url.Values) because URL-encoded
// clients may pass parameters that don't round-trip through Go's parser
// (e.g. `+` vs `%20`), and reproducing their exact signing input is the
// only way to verify the HMAC.
func queryWithoutSignature(raw string) string {
	out := make([]byte, 0, len(raw))
	start := 0
	for i := 0; i <= len(raw); i++ {
		if i == len(raw) || raw[i] == '&' {
			seg := raw[start:i]
			if !(len(seg) >= len("signature=") && seg[:len("signature=")] == "signature=") {
				if len(out) > 0 {
					out = append(out, '&')
				}
				out = append(out, seg...)
			}
			start = i + 1
		}
	}
	return string(out)
}

// rewindableBody lets downstream handlers re-read the request body after
// VerifyAPIKeyRequest consumed it for signing.
type rewindableBody struct {
	data []byte
	off  int
}

func (r *rewindableBody) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	return n, nil
}

func (r *rewindableBody) Close() error { return nil }

// SignAPIKeyRequest is the mirror of VerifyAPIKeyRequest for tests or client
// tooling. Returns the signature hex string to place in the `signature`
// query param.
func SignAPIKeyRequest(secret []byte, rawQueryWithoutSig string, body []byte) string {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(rawQueryWithoutSig))
	mac.Write([]byte("|"))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
