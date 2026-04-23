package service

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap"

	"github.com/xargin/opentrade/asset/internal/engine"
	"github.com/xargin/opentrade/asset/internal/store"
	"github.com/xargin/opentrade/pkg/dec"
)

type fakeFundingStore struct {
	state *engine.State
}

func newFakeFundingStore() *fakeFundingStore {
	return &fakeFundingStore{state: engine.NewState()}
}

func (f *fakeFundingStore) TransferOut(_ context.Context, req store.Request) (store.Result, error) {
	res, err := f.state.ApplyTransferOut(toEngineReq(req))
	if err != nil {
		if errors.Is(err, engine.ErrInsufficientAvailable) {
			return store.Result{Status: store.StatusRejected, RejectReason: err}, nil
		}
		return store.Result{Status: store.StatusRejected, RejectReason: err}, nil
	}
	return fromEngineResult(res), nil
}

func (f *fakeFundingStore) TransferIn(_ context.Context, req store.Request) (store.Result, error) {
	res, err := f.state.ApplyTransferIn(toEngineReq(req))
	if err != nil {
		return store.Result{Status: store.StatusRejected, RejectReason: err}, nil
	}
	return fromEngineResult(res), nil
}

func (f *fakeFundingStore) Compensate(_ context.Context, req store.Request) (store.Result, error) {
	return f.TransferIn(context.Background(), req)
}

func (f *fakeFundingStore) QueryFundingBalance(_ context.Context, userID, asset string) ([]store.FundingBalance, error) {
	acc := f.state.Account(userID)
	if asset != "" {
		return []store.FundingBalance{{Asset: asset, Balance: acc.Balance(asset)}}, nil
	}
	all := acc.Copy()
	out := make([]store.FundingBalance, 0, len(all))
	for a, b := range all {
		out = append(out, store.FundingBalance{Asset: a, Balance: b})
	}
	return out, nil
}

func toEngineReq(req store.Request) engine.TransferRequest {
	return engine.TransferRequest{
		UserID:     req.UserID,
		TransferID: req.TransferID,
		Asset:      req.Asset,
		Amount:     req.Amount,
	}
}

func fromEngineResult(res engine.Result) store.Result {
	status := store.StatusConfirmed
	if res.Duplicated {
		status = store.StatusDuplicated
	}
	return store.Result{
		Status:         status,
		BalanceAfter:   res.BalanceAfter,
		FundingVersion: res.FundingVersion,
	}
}

func mustDec(t *testing.T, s string) dec.Decimal {
	t.Helper()
	d, err := dec.Parse(s)
	if err != nil {
		t.Fatalf("dec.Parse: %v", err)
	}
	return d
}

func newSvc(t *testing.T) *Service {
	t.Helper()
	return New(newFakeFundingStore(), zap.NewNop())
}

func holder(userID, transferID, asset, amount string, t *testing.T) HolderRequest {
	return HolderRequest{
		UserID:     userID,
		TransferID: transferID,
		Asset:      asset,
		Amount: engine.TransferRequest{
			UserID:     userID,
			TransferID: transferID,
			Asset:      asset,
			Amount:     mustDec(t, amount),
		},
	}
}

func TestTransferIn_Confirmed(t *testing.T) {
	svc := newSvc(t)

	req := holder("u1", "saga-1", "USDT", "100", t)
	req.PeerBiz = "spot"
	req.Memo = "hi"
	res, err := svc.TransferIn(context.Background(), req)
	if err != nil {
		t.Fatalf("in: %v", err)
	}
	if res.Status != StatusConfirmed {
		t.Fatalf("status = %v", res.Status)
	}
	if res.BalanceAfter.Available.String() != "100" {
		t.Errorf("available = %q", res.BalanceAfter.Available.String())
	}
	if res.FundingVersion != 1 {
		t.Errorf("funding_version = %d", res.FundingVersion)
	}
}

func TestTransferOut_Rejected(t *testing.T) {
	svc := newSvc(t)

	res, err := svc.TransferOut(context.Background(), holder("u1", "saga-out", "USDT", "100", t))
	if err != nil {
		t.Fatalf("out: %v", err)
	}
	if res.Status != StatusRejected {
		t.Fatalf("status = %v, want Rejected", res.Status)
	}
	if !errors.Is(res.RejectReason, engine.ErrInsufficientAvailable) {
		t.Errorf("reject = %v", res.RejectReason)
	}
}

func TestTransferIn_Duplicated(t *testing.T) {
	svc := newSvc(t)
	req := holder("u1", "saga-1", "USDT", "50", t)

	if _, err := svc.TransferIn(context.Background(), req); err != nil {
		t.Fatalf("first: %v", err)
	}
	res, err := svc.TransferIn(context.Background(), req)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if res.Status != StatusDuplicated {
		t.Fatalf("status = %v, want Duplicated", res.Status)
	}
}

func TestCompensate_CreditsFunding(t *testing.T) {
	svc := newSvc(t)
	req := holder("u1", "saga-c", "USDT", "40", t)
	req.PeerBiz = "spot"
	req.CompensateCause = "peer_in_rejected"

	res, err := svc.Compensate(context.Background(), req)
	if err != nil {
		t.Fatalf("compensate: %v", err)
	}
	if res.Status != StatusConfirmed {
		t.Fatalf("status = %v", res.Status)
	}
	if res.BalanceAfter.Available.String() != "40" {
		t.Errorf("available = %s", res.BalanceAfter.Available)
	}
}

func TestQueryFundingBalance(t *testing.T) {
	svc := newSvc(t)
	_, _ = svc.TransferIn(context.Background(), holder("u1", "t-usdt", "USDT", "100", t))
	_, _ = svc.TransferIn(context.Background(), holder("u1", "t-btc", "BTC", "0.5", t))

	all, err := svc.QueryFundingBalance(context.Background(), "u1", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("all = %d, want 2", len(all))
	}
	one, err := svc.QueryFundingBalance(context.Background(), "u1", "USDT")
	if err != nil {
		t.Fatal(err)
	}
	if len(one) != 1 || one[0].Balance.Available.String() != "100" {
		t.Errorf("one = %+v", one)
	}

	miss, err := svc.QueryFundingBalance(context.Background(), "stranger", "USDT")
	if err != nil {
		t.Fatal(err)
	}
	if len(miss) != 1 || !miss[0].Balance.Available.IsZero() {
		t.Errorf("miss = %+v", miss)
	}
}
