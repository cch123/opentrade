package holder

import (
	"context"
	"testing"

	"github.com/xargin/opentrade/asset/internal/engine"
	"github.com/xargin/opentrade/asset/internal/service"
	"github.com/xargin/opentrade/asset/internal/store"
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

func (f *fakeFundingStore) Compensate(ctx context.Context, req store.Request) (store.Result, error) {
	return f.TransferIn(ctx, req)
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

func newLocal(t *testing.T) *LocalFundingClient {
	t.Helper()
	svc := service.New(newFakeFundingStore(), nil)
	return NewLocalFundingClient(svc)
}

func TestLocal_TransferIn_Confirmed(t *testing.T) {
	c := newLocal(t)
	res, err := c.TransferIn(context.Background(), Request{
		UserID: "u1", TransferID: "t1", Asset: "USDT", Amount: "100", PeerBiz: "spot",
	})
	if err != nil {
		t.Fatalf("in: %v", err)
	}
	if res.Status != StatusConfirmed {
		t.Errorf("status = %v", res.Status)
	}
}

func TestLocal_TransferOut_Insufficient(t *testing.T) {
	c := newLocal(t)
	res, err := c.TransferOut(context.Background(), Request{
		UserID: "u1", TransferID: "t1", Asset: "USDT", Amount: "100", PeerBiz: "spot",
	})
	if err != nil {
		t.Fatalf("out: %v", err)
	}
	if res.Status != StatusRejected {
		t.Fatalf("status = %v", res.Status)
	}
	if res.Reason != ReasonInsufficientBalance {
		t.Errorf("reason = %v", res.Reason)
	}
}

func TestLocal_Idempotent(t *testing.T) {
	c := newLocal(t)
	req := Request{UserID: "u1", TransferID: "t1", Asset: "USDT", Amount: "10", PeerBiz: "spot"}
	if _, err := c.TransferIn(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	res, err := c.TransferIn(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != StatusDuplicated {
		t.Errorf("status = %v, want DUPLICATED", res.Status)
	}
}

func TestLocal_ValidationRejects(t *testing.T) {
	c := newLocal(t)
	bad := []Request{
		{TransferID: "t", Asset: "USDT", Amount: "1"},
		{UserID: "u1", Asset: "USDT", Amount: "1"},
		{UserID: "u1", TransferID: "t", Amount: "1"},
		{UserID: "u1", TransferID: "t", Asset: "USDT", Amount: "0"},
		{UserID: "u1", TransferID: "t", Asset: "USDT", Amount: "abc"},
	}
	for i, r := range bad {
		res, err := c.TransferIn(context.Background(), r)
		if err != nil {
			t.Errorf("case %d: err = %v", i, err)
		}
		if res.Status != StatusRejected || res.Reason != ReasonAmountInvalid {
			t.Errorf("case %d: res = %+v", i, res)
		}
	}
}

func TestRegistry_GetAndKnown(t *testing.T) {
	r := NewRegistry()
	c := newLocal(t)
	r.Register("funding", c)

	got, err := r.Get("funding")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got == nil {
		t.Error("nil client")
	}
	if _, err := r.Get("futures"); err != ErrUnknownBizLine {
		t.Errorf("unknown err = %v", err)
	}
	known := r.Known()
	if len(known) != 1 || known[0] != "funding" {
		t.Errorf("known = %v", known)
	}
}

func TestRegistry_RegisterNil_Panics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("expected panic on nil client")
		}
	}()
	NewRegistry().Register("funding", nil)
}
