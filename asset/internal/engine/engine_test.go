package engine

import (
	"errors"
	"sync"
	"testing"

	"github.com/xargin/opentrade/pkg/dec"
)

func mustDec(t *testing.T, s string) dec.Decimal {
	t.Helper()
	d, err := dec.Parse(s)
	if err != nil {
		t.Fatalf("dec.Parse(%q): %v", s, err)
	}
	return d
}

func TestApplyTransferIn_CreditsAndBumpsVersions(t *testing.T) {
	st := NewState()
	res, err := st.ApplyTransferIn(TransferRequest{
		UserID: "u1", TransferID: "t1", Asset: "USDT", Amount: mustDec(t, "100"),
	})
	if err != nil {
		t.Fatalf("in: %v", err)
	}
	if res.Duplicated {
		t.Errorf("first call should not be duplicated")
	}
	if res.BalanceAfter.Available.String() != "100" {
		t.Errorf("available = %q", res.BalanceAfter.Available.String())
	}
	if res.BalanceAfter.Version != 1 {
		t.Errorf("asset version = %d, want 1", res.BalanceAfter.Version)
	}
	if res.FundingVersion != 1 {
		t.Errorf("funding version = %d, want 1", res.FundingVersion)
	}
}

func TestApplyTransferIn_Idempotent(t *testing.T) {
	st := NewState()
	req := TransferRequest{UserID: "u1", TransferID: "t1", Asset: "USDT", Amount: mustDec(t, "100")}

	if _, err := st.ApplyTransferIn(req); err != nil {
		t.Fatalf("first: %v", err)
	}
	res, err := st.ApplyTransferIn(req)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if !res.Duplicated {
		t.Error("second call should be Duplicated=true")
	}
	// Balance must still reflect the single credit.
	if res.BalanceAfter.Available.String() != "100" {
		t.Errorf("available = %q, want 100", res.BalanceAfter.Available.String())
	}
	if res.BalanceAfter.Version != 1 {
		t.Errorf("asset version leaked bump on dedup hit: %d", res.BalanceAfter.Version)
	}
}

func TestApplyTransferOut_Confirmed(t *testing.T) {
	st := NewState()
	_, _ = st.ApplyTransferIn(TransferRequest{UserID: "u1", TransferID: "seed", Asset: "USDT", Amount: mustDec(t, "200")})

	res, err := st.ApplyTransferOut(TransferRequest{UserID: "u1", TransferID: "out-1", Asset: "USDT", Amount: mustDec(t, "75")})
	if err != nil {
		t.Fatalf("out: %v", err)
	}
	if res.BalanceAfter.Available.String() != "125" {
		t.Errorf("available = %q, want 125", res.BalanceAfter.Available.String())
	}
	if res.BalanceAfter.Version != 2 {
		t.Errorf("asset version = %d, want 2", res.BalanceAfter.Version)
	}
	if res.FundingVersion != 2 {
		t.Errorf("funding version = %d, want 2", res.FundingVersion)
	}
}

func TestApplyTransferOut_InsufficientBalance(t *testing.T) {
	st := NewState()
	_, _ = st.ApplyTransferIn(TransferRequest{UserID: "u1", TransferID: "seed", Asset: "USDT", Amount: mustDec(t, "10")})

	_, err := st.ApplyTransferOut(TransferRequest{UserID: "u1", TransferID: "out-1", Asset: "USDT", Amount: mustDec(t, "100")})
	if !errors.Is(err, ErrInsufficientAvailable) {
		t.Fatalf("err = %v, want ErrInsufficientAvailable", err)
	}
	// Rejection must NOT remember the id, so the caller can retry with
	// corrected params.
	acc := st.Account("u1")
	acc.mu.Lock()
	defer acc.mu.Unlock()
	if acc.seen("out-1") {
		t.Error("rejected transfer should not be remembered")
	}
}

func TestApplyCompensate_EquivalentToIn(t *testing.T) {
	st := NewState()
	res, err := st.ApplyCompensate(TransferRequest{UserID: "u1", TransferID: "c1", Asset: "USDT", Amount: mustDec(t, "50")})
	if err != nil {
		t.Fatalf("compensate: %v", err)
	}
	if res.BalanceAfter.Available.String() != "50" {
		t.Errorf("available = %q", res.BalanceAfter.Available.String())
	}
}

func TestValidate_MissingFields(t *testing.T) {
	st := NewState()
	cases := []struct {
		name string
		req  TransferRequest
		err  error
	}{
		{"user", TransferRequest{TransferID: "t", Asset: "USDT", Amount: mustDec(t, "10")}, ErrMissingUserID},
		{"transfer", TransferRequest{UserID: "u1", Asset: "USDT", Amount: mustDec(t, "10")}, ErrMissingTransferID},
		{"asset", TransferRequest{UserID: "u1", TransferID: "t", Amount: mustDec(t, "10")}, ErrMissingAsset},
		{"amount=0", TransferRequest{UserID: "u1", TransferID: "t", Asset: "USDT", Amount: mustDec(t, "0")}, ErrInvalidAmount},
	}
	for _, tc := range cases {
		_, err := st.ApplyTransferIn(tc.req)
		if !errors.Is(err, tc.err) {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.err)
		}
	}
}

// TestRingEvicts inserts 300 distinct transfer_ids and asserts that the
// oldest ones age out (ring size = 256).
func TestRingEvicts(t *testing.T) {
	st := NewState()
	for i := 0; i < 300; i++ {
		_, err := st.ApplyTransferIn(TransferRequest{
			UserID:     "u1",
			TransferID: "t-" + pad(i),
			Asset:      "USDT",
			Amount:     mustDec(t, "1"),
		})
		if err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
	}
	acc := st.Account("u1")
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if len(acc.recentSeen) != recentRingSize {
		t.Errorf("ring size = %d, want %d", len(acc.recentSeen), recentRingSize)
	}
	if acc.seen("t-" + pad(0)) {
		t.Error("oldest id should have been evicted")
	}
	if !acc.seen("t-" + pad(299)) {
		t.Error("newest id should be present")
	}
}

func pad(n int) string {
	s := []byte("0000")
	for i := len(s) - 1; i >= 0 && n > 0; i-- {
		s[i] = byte('0' + n%10)
		n /= 10
	}
	return string(s)
}

// TestConcurrentDifferentUsers ensures per-user locking doesn't serialize
// across users.
func TestConcurrentDifferentUsers(t *testing.T) {
	st := NewState()
	var wg sync.WaitGroup
	const N = 50
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			uid := "u-" + pad(i)
			_, err := st.ApplyTransferIn(TransferRequest{
				UserID: uid, TransferID: "seed", Asset: "USDT", Amount: mustDec(t, "10"),
			})
			if err != nil {
				t.Errorf("apply %s: %v", uid, err)
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < N; i++ {
		uid := "u-" + pad(i)
		if st.Account(uid).Balance("USDT").Available.String() != "10" {
			t.Errorf("%s: not credited", uid)
		}
	}
}
