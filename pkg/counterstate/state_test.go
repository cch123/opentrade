package counterstate

import (
	"errors"
	"fmt"
	"testing"

	"github.com/xargin/opentrade/pkg/dec"
)

func mkTransfer(user uint64, asset, amount string, t TransferType) TransferRequest {
	return TransferRequest{
		TransferID: fmt.Sprintf("tx-%d-%s", user, asset),
		UserID:     user,
		Asset:      asset,
		Amount:     dec.New(amount),
		Type:       t,
	}
}

func TestDepositCreatesAccount(t *testing.T) {
	s := NewShardState(0)
	bal, err := s.ApplyTransfer(mkTransfer(1001, "USDT", "100", TransferDeposit))
	if err != nil {
		t.Fatalf("deposit: %v", err)
	}
	if bal.Available.String() != "100" || bal.Frozen.String() != "0" {
		t.Fatalf("balance = %+v, want avail=100 frozen=0", bal)
	}
	if got := s.Balance(1001, "USDT"); got.Available.String() != "100" {
		t.Fatalf("after deposit state = %+v", got)
	}
}

func TestWithdrawInsufficient(t *testing.T) {
	s := NewShardState(0)
	_, err := s.ApplyTransfer(mkTransfer(1001, "USDT", "100", TransferWithdraw))
	if !errors.Is(err, ErrInsufficientAvailable) {
		t.Fatalf("err = %v, want ErrInsufficientAvailable", err)
	}
	// State must remain at zero.
	if !s.Balance(1001, "USDT").IsEmpty() {
		t.Fatalf("state mutated despite withdraw failure: %+v", s.Balance(1001, "USDT"))
	}
}

func TestFreezeAndUnfreezeRoundTrip(t *testing.T) {
	s := NewShardState(0)
	_, _ = s.ApplyTransfer(mkTransfer(1001, "USDT", "100", TransferDeposit))
	bal, err := s.ApplyTransfer(mkTransfer(1001, "USDT", "40", TransferFreeze))
	if err != nil {
		t.Fatal(err)
	}
	if bal.Available.String() != "60" || bal.Frozen.String() != "40" {
		t.Fatalf("post-freeze = %+v", bal)
	}
	bal, err = s.ApplyTransfer(mkTransfer(1001, "USDT", "30", TransferUnfreeze))
	if err != nil {
		t.Fatal(err)
	}
	if bal.Available.String() != "90" || bal.Frozen.String() != "10" {
		t.Fatalf("post-unfreeze = %+v", bal)
	}
}

func TestFreezeInsufficient(t *testing.T) {
	s := NewShardState(0)
	_, _ = s.ApplyTransfer(mkTransfer(1001, "USDT", "10", TransferDeposit))
	_, err := s.ApplyTransfer(mkTransfer(1001, "USDT", "20", TransferFreeze))
	if !errors.Is(err, ErrInsufficientAvailable) {
		t.Fatalf("err = %v, want ErrInsufficientAvailable", err)
	}
	// State must remain untouched.
	if got := s.Balance(1001, "USDT"); got.Available.String() != "10" || got.Frozen.String() != "0" {
		t.Fatalf("state leaked changes: %+v", got)
	}
}

func TestUnfreezeInsufficient(t *testing.T) {
	s := NewShardState(0)
	_, err := s.ApplyTransfer(mkTransfer(1001, "USDT", "5", TransferUnfreeze))
	if !errors.Is(err, ErrInsufficientFrozen) {
		t.Fatalf("err = %v, want ErrInsufficientFrozen", err)
	}
}

func TestZeroOrNegativeAmount(t *testing.T) {
	s := NewShardState(0)
	_, err := s.ApplyTransfer(mkTransfer(1001, "USDT", "0", TransferDeposit))
	if !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("zero: err = %v, want ErrInvalidAmount", err)
	}
	_, err = s.ApplyTransfer(mkTransfer(1001, "USDT", "-1", TransferDeposit))
	if !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("negative: err = %v, want ErrInvalidAmount", err)
	}
}

func TestUnknownTransferType(t *testing.T) {
	s := NewShardState(0)
	req := TransferRequest{UserID: 1001, Asset: "USDT", Amount: dec.New("1"), Type: 99}
	if _, err := s.ApplyTransfer(req); !errors.Is(err, ErrUnknownTransferType) {
		t.Fatalf("err = %v, want ErrUnknownTransferType", err)
	}
}

func TestMultipleAssetsIndependent(t *testing.T) {
	s := NewShardState(0)
	_, _ = s.ApplyTransfer(mkTransfer(1001, "USDT", "100", TransferDeposit))
	_, _ = s.ApplyTransfer(mkTransfer(1001, "BTC", "0.5", TransferDeposit))
	if s.Balance(1001, "USDT").Available.String() != "100" {
		t.Fatal("USDT lost")
	}
	if s.Balance(1001, "BTC").Available.String() != "0.5" {
		t.Fatal("BTC lost")
	}
}

func TestMultipleUsersIndependent(t *testing.T) {
	s := NewShardState(0)
	_, _ = s.ApplyTransfer(mkTransfer(1001, "USDT", "10", TransferDeposit))
	_, _ = s.ApplyTransfer(mkTransfer(1002, "USDT", "20", TransferDeposit))
	if s.Balance(1001, "USDT").Available.String() != "10" {
		t.Fatal("u1 mutated by u2")
	}
	if s.Balance(1002, "USDT").Available.String() != "20" {
		t.Fatal("u2 mutated by u1")
	}
}
