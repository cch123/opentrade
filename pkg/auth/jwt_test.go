package auth

import (
	"errors"
	"testing"
	"time"
)

func TestSignVerifyHS256_RoundTrip(t *testing.T) {
	secret := []byte("very-secret-32-bytes-for-testing-only")
	now := time.Unix(1_700_000_000, 0)
	tok, err := SignHS256("alice", secret, now, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	sub, err := VerifyHS256(tok, secret, now.Add(5*time.Minute))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if sub != "alice" {
		t.Errorf("sub = %q", sub)
	}
}

func TestVerifyHS256_WrongSecretFails(t *testing.T) {
	secret := []byte("aaa")
	now := time.Unix(1_700_000_000, 0)
	tok, _ := SignHS256("alice", secret, now, now.Add(time.Hour))
	if _, err := VerifyHS256(tok, []byte("bbb"), now); err == nil {
		t.Fatal("expected error")
	} else if !errors.Is(err, ErrInvalidJWT) {
		t.Errorf("err = %v, want ErrInvalidJWT leaf", err)
	}
}

func TestVerifyHS256_ExpiredFails(t *testing.T) {
	secret := []byte("secret")
	now := time.Unix(1_700_000_000, 0)
	tok, _ := SignHS256("alice", secret, now, now.Add(time.Minute))
	if _, err := VerifyHS256(tok, secret, now.Add(time.Hour)); !errors.Is(err, ErrExpiredJWT) {
		t.Errorf("err = %v, want ErrExpiredJWT", err)
	}
}

func TestVerifyHS256_MalformedShape(t *testing.T) {
	for _, tok := range []string{"", "not.a.jwt", "onlyone", "two.parts", "a.b.c.d"} {
		if _, err := VerifyHS256(tok, []byte("x"), time.Now()); err == nil {
			t.Errorf("expected error for token %q", tok)
		}
	}
}

func TestVerifyHS256_RejectsAlgNone(t *testing.T) {
	// RFC warns against accepting alg:none. Our implementation pins HS256;
	// constructing a hand-rolled alg:none token must fail.
	// header: {"alg":"none","typ":"JWT"} payload: {"sub":"x"}
	tok := "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiJ4In0."
	if _, err := VerifyHS256(tok, []byte("irrelevant"), time.Now()); err == nil {
		t.Fatal("alg:none must be rejected")
	}
}
