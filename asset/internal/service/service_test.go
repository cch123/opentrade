package service

import (
	"context"
	"errors"
	"sync"
	"testing"

	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/asset/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

type fakePub struct {
	mu      sync.Mutex
	events  []*eventpb.AssetJournalEvent
	seq     uint64
	publish error // if non-nil, Publish returns this
}

func (f *fakePub) Publish(_ context.Context, _ string, evt *eventpb.AssetJournalEvent) error {
	if f.publish != nil {
		return f.publish
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, evt)
	return nil
}

func (f *fakePub) NextSeq() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seq++
	return f.seq
}

func (f *fakePub) Close(context.Context) error { return nil }

func mustDec(t *testing.T, s string) dec.Decimal {
	t.Helper()
	d, err := dec.Parse(s)
	if err != nil {
		t.Fatalf("dec.Parse: %v", err)
	}
	return d
}

func newSvc(t *testing.T) (*Service, *fakePub) {
	t.Helper()
	state := engine.NewState()
	pub := &fakePub{}
	return New(Config{ProducerID: "asset-main"}, state, pub, zap.NewNop()), pub
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

func TestTransferIn_Confirmed_PublishesEvent(t *testing.T) {
	svc, pub := newSvc(t)

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

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.events) != 1 {
		t.Fatalf("events = %d", len(pub.events))
	}
	xin := pub.events[0].GetXferIn()
	if xin == nil {
		t.Fatalf("event payload not XferIn: %+v", pub.events[0])
	}
	if xin.TransferId != "saga-1" || xin.Amount != "100" || xin.PeerBiz != "spot" {
		t.Errorf("event = %+v", xin)
	}
	if pub.events[0].FundingVersion != 1 {
		t.Errorf("funding_version on envelope = %d", pub.events[0].FundingVersion)
	}
	if pub.events[0].AssetSeqId != 1 {
		t.Errorf("asset_seq_id = %d", pub.events[0].AssetSeqId)
	}
}

func TestTransferOut_Rejected_NoPublish(t *testing.T) {
	svc, pub := newSvc(t)

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
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.events) != 0 {
		t.Errorf("rejected transfer should not publish: %d events", len(pub.events))
	}
}

func TestTransferIn_Duplicated(t *testing.T) {
	svc, pub := newSvc(t)
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
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.events) != 1 {
		t.Errorf("duplicate should not re-publish: %d events", len(pub.events))
	}
}

func TestTransferIn_PublishFailureDoesNotCommit(t *testing.T) {
	svc, pub := newSvc(t)
	pub.publish = errors.New("kafka down")
	req := holder("u1", "saga-1", "USDT", "50", t)

	if _, err := svc.TransferIn(context.Background(), req); err == nil {
		t.Fatal("expected publish error")
	}
	if got := svc.QueryFundingBalance("u1", "USDT")[0].Balance.Available; !got.IsZero() {
		t.Fatalf("balance committed despite failed publish: %s", got)
	}

	pub.publish = nil
	res, err := svc.TransferIn(context.Background(), req)
	if err != nil {
		t.Fatalf("retry after failed publish: %v", err)
	}
	if res.Status != StatusConfirmed {
		t.Fatalf("retry status = %v, want confirmed", res.Status)
	}
	if got := svc.QueryFundingBalance("u1", "USDT")[0].Balance.Available.String(); got != "50" {
		t.Fatalf("balance after retry = %s, want 50", got)
	}
}

func TestCompensate_EmitsCompensateKind(t *testing.T) {
	svc, pub := newSvc(t)
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
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.events) != 1 {
		t.Fatalf("events = %d", len(pub.events))
	}
	c := pub.events[0].GetCompensate()
	if c == nil {
		t.Fatalf("payload not Compensate")
	}
	if c.CompensateCause != "peer_in_rejected" {
		t.Errorf("compensate_cause = %q", c.CompensateCause)
	}
}

func TestQueryFundingBalance(t *testing.T) {
	svc, _ := newSvc(t)
	_, _ = svc.TransferIn(context.Background(), holder("u1", "t-usdt", "USDT", "100", t))
	_, _ = svc.TransferIn(context.Background(), holder("u1", "t-btc", "BTC", "0.5", t))

	all := svc.QueryFundingBalance("u1", "")
	if len(all) != 2 {
		t.Fatalf("all = %d, want 2", len(all))
	}
	one := svc.QueryFundingBalance("u1", "USDT")
	if len(one) != 1 || one[0].Balance.Available.String() != "100" {
		t.Errorf("one = %+v", one)
	}

	// Unknown user returns zero-valued balance, not an error.
	miss := svc.QueryFundingBalance("stranger", "USDT")
	if len(miss) != 1 || !miss[0].Balance.Available.IsZero() {
		t.Errorf("miss = %+v", miss)
	}
}
