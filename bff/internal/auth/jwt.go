package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// JWT helpers — minimal HS256 verifier (RFC 7519) implemented against the
// stdlib so BFF doesn't pull an external JWT library. RS256 and ES256 are
// out of scope for MVP and live on the ADR-0039 future-work list.

var (
	// ErrInvalidJWT is returned for any structural / signature / expiry
	// problem with the supplied token.
	ErrInvalidJWT = errors.New("auth: invalid jwt")

	// ErrExpiredJWT is a leaf of ErrInvalidJWT kept separately so callers
	// can distinguish "token shape is ok but it's stale" from other
	// failures when logging.
	ErrExpiredJWT = errors.New("auth: jwt expired")
)

// jwtHeader is the subset of fields we care about in the token header.
// `typ` is ignored; `alg` must be HS256 for MVP.
type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// jwtClaims is the subset of registered claims we enforce.
type jwtClaims struct {
	Sub string `json:"sub"`           // authenticated user id
	Exp int64  `json:"exp,omitempty"` // seconds since epoch
	Iat int64  `json:"iat,omitempty"` // seconds since epoch (informational)
}

// VerifyHS256 checks token's signature against secret, ensures the header
// says HS256, and returns the `sub` claim (user id). An expired token
// (exp <= now) returns ErrExpiredJWT; any other problem wraps
// ErrInvalidJWT.
func VerifyHS256(token string, secret []byte, now time.Time) (string, error) {
	if secret == nil || len(secret) == 0 {
		return "", fmt.Errorf("%w: empty secret", ErrInvalidJWT)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("%w: expected 3 segments, got %d", ErrInvalidJWT, len(parts))
	}
	headerB, payloadB, sig := parts[0], parts[1], parts[2]

	headerBytes, err := base64.RawURLEncoding.DecodeString(headerB)
	if err != nil {
		return "", fmt.Errorf("%w: header base64: %v", ErrInvalidJWT, err)
	}
	var hdr jwtHeader
	if err := json.Unmarshal(headerBytes, &hdr); err != nil {
		return "", fmt.Errorf("%w: header json: %v", ErrInvalidJWT, err)
	}
	if hdr.Alg != "HS256" {
		return "", fmt.Errorf("%w: alg=%q, want HS256", ErrInvalidJWT, hdr.Alg)
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(payloadB)
	if err != nil {
		return "", fmt.Errorf("%w: payload base64: %v", ErrInvalidJWT, err)
	}
	var claims jwtClaims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return "", fmt.Errorf("%w: payload json: %v", ErrInvalidJWT, err)
	}
	if claims.Sub == "" {
		return "", fmt.Errorf("%w: missing sub", ErrInvalidJWT)
	}

	// Constant-time signature check against `header.payload`.
	signingInput := headerB + "." + payloadB
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signingInput))
	expected := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(sig)) {
		return "", fmt.Errorf("%w: signature mismatch", ErrInvalidJWT)
	}

	// Expiry last: a bad signature should not leak whether a token would
	// have expired or not.
	if claims.Exp > 0 && now.Unix() >= claims.Exp {
		return "", ErrExpiredJWT
	}
	return claims.Sub, nil
}

// SignHS256 emits a compact HS256 JWT with the given claims. This is a
// helper for tests and for any operator tooling that wants to mint
// short-lived tokens; production token issuance belongs to a dedicated
// IdP, not BFF.
func SignHS256(subject string, secret []byte, issuedAt, expiresAt time.Time) (string, error) {
	if subject == "" {
		return "", errors.New("auth: subject required")
	}
	if len(secret) == 0 {
		return "", errors.New("auth: secret required")
	}
	headerBytes, _ := json.Marshal(jwtHeader{Alg: "HS256", Typ: "JWT"})
	claims := jwtClaims{Sub: subject, Iat: issuedAt.Unix(), Exp: expiresAt.Unix()}
	payloadBytes, _ := json.Marshal(claims)
	header := base64.RawURLEncoding.EncodeToString(headerBytes)
	payload := base64.RawURLEncoding.EncodeToString(payloadBytes)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(header + "." + payload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return header + "." + payload + "." + sig, nil
}
