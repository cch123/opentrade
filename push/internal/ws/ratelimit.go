package ws

import (
	"sync"
	"time"
)

// tokenBucket is a minimal standalone token-bucket limiter used per Conn to
// cap outbound messages-per-second at the wire. We roll our own rather than
// pull golang.org/x/time/rate in because the semantics we need are trivial
// (no reservation, no wait queue — just allow/deny) and a vendored dep for
// one call site is overkill.
//
// Zero value is unusable; always call newTokenBucket. Safe for concurrent
// callers.
type tokenBucket struct {
	mu     sync.Mutex
	tokens float64
	last   time.Time
	rate   float64 // tokens per second
	burst  float64 // max accumulated tokens
}

// newTokenBucket returns a bucket pre-filled to burst. rate ≤ 0 disables the
// limiter (allow always returns true).
func newTokenBucket(ratePerSec, burst float64) *tokenBucket {
	if ratePerSec <= 0 {
		return &tokenBucket{rate: 0}
	}
	if burst <= 0 {
		burst = ratePerSec // 1s burst window by default
	}
	return &tokenBucket{
		tokens: burst,
		last:   time.Now(),
		rate:   ratePerSec,
		burst:  burst,
	}
}

// allow consumes one token. Returns true if a token was available, false if
// the caller must drop the event.
func (tb *tokenBucket) allow() bool {
	if tb.rate <= 0 {
		return true
	}
	tb.mu.Lock()
	defer tb.mu.Unlock()
	now := time.Now()
	tb.tokens += now.Sub(tb.last).Seconds() * tb.rate
	if tb.tokens > tb.burst {
		tb.tokens = tb.burst
	}
	tb.last = now
	if tb.tokens >= 1 {
		tb.tokens--
		return true
	}
	return false
}
