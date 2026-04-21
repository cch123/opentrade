package holder

import (
	"context"
	"testing"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/asset/internal/engine"
	"github.com/xargin/opentrade/asset/internal/service"
)

type nopPub struct{ seq uint64 }

func (p *nopPub) Publish(context.Context, string, *eventpb.AssetJournalEvent) error { return nil }
func (p *nopPub) NextSeq() uint64                                                    { p.seq++; return p.seq }
func (p *nopPub) Close(context.Context) error                                         { return nil }

func newLocal(t *testing.T) *LocalFundingClient {
	t.Helper()
	state := engine.NewState()
	svc := service.New(service.Config{ProducerID: "asset-test"}, state, &nopPub{}, zap.NewNop())
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
