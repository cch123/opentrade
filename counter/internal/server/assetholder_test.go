package server

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	assetholderrpc "github.com/xargin/opentrade/api/gen/rpc/assetholder"
	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/internal/engine"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/counter/internal/service"
)

func newHolderPair(t *testing.T) (*AssetHolderServer, *fakePub) {
	t.Helper()
	state := engine.NewShardState(0)
	seq := sequencer.New()
	dt := dedup.New(time.Hour)
	pub := &fakePub{}
	svc := service.New(service.Config{ShardID: 0, ProducerID: "counter-shard-0-main"},
		state, seq, dt, pub, zap.NewNop())
	return NewAssetHolderServer(NewSingleServiceRouter(svc)), pub
}

// seedDeposit pre-funds a user so subsequent TransferOut has balance.
func seedDeposit(t *testing.T, h *AssetHolderServer, userID, asset, amount string) {
	t.Helper()
	resp, err := h.TransferIn(context.Background(), &assetholderrpc.TransferInRequest{
		UserId: userID, TransferId: "seed-" + userID + "-" + asset + "-" + amount,
		Asset: asset, Amount: amount, PeerBiz: "funding",
	})
	if err != nil {
		t.Fatalf("seed TransferIn: %v", err)
	}
	if resp.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED {
		t.Fatalf("seed status = %v", resp.Status)
	}
}

// ---------------------------------------------------------------------------
// TransferIn
// ---------------------------------------------------------------------------

func TestTransferIn_Confirmed(t *testing.T) {
	h, pub := newHolderPair(t)

	resp, err := h.TransferIn(context.Background(), &assetholderrpc.TransferInRequest{
		UserId:     "u1",
		TransferId: "saga-1",
		Asset:      "USDT",
		Amount:     "100",
		PeerBiz:    "funding",
		Memo:       "test",
	})
	if err != nil {
		t.Fatalf("TransferIn: %v", err)
	}
	if resp.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED {
		t.Fatalf("status = %v", resp.Status)
	}
	if resp.AvailableAfter != "100" {
		t.Errorf("available_after = %q", resp.AvailableAfter)
	}

	// Verify the journal event was published with saga_transfer_id.
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.events) != 1 {
		t.Fatalf("events = %d, want 1", len(pub.events))
	}
	xfer := pub.events[0].GetTransfer()
	if xfer == nil {
		t.Fatalf("event payload not TransferEvent: %+v", pub.events[0])
	}
	if xfer.SagaTransferId != "saga-1" {
		t.Errorf("saga_transfer_id = %q, want saga-1", xfer.SagaTransferId)
	}
	if xfer.Type != eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT {
		t.Errorf("type = %v, want DEPOSIT", xfer.Type)
	}
	if xfer.BizRefId != "funding" {
		t.Errorf("biz_ref_id = %q, want funding", xfer.BizRefId)
	}
}

func TestTransferIn_Idempotent(t *testing.T) {
	h, pub := newHolderPair(t)

	req := &assetholderrpc.TransferInRequest{
		UserId: "u1", TransferId: "saga-1", Asset: "USDT",
		Amount: "100", PeerBiz: "funding",
	}

	first, err := h.TransferIn(context.Background(), req)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if first.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED {
		t.Fatalf("first status = %v", first.Status)
	}

	second, err := h.TransferIn(context.Background(), req)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_DUPLICATED {
		t.Fatalf("second status = %v, want DUPLICATED", second.Status)
	}

	// Journal must have exactly one event despite two RPC calls.
	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.events) != 1 {
		t.Fatalf("events = %d, want 1 (idempotency should skip second publish)", len(pub.events))
	}
}

// ---------------------------------------------------------------------------
// TransferOut
// ---------------------------------------------------------------------------

func TestTransferOut_Confirmed(t *testing.T) {
	h, pub := newHolderPair(t)
	seedDeposit(t, h, "u1", "USDT", "500")

	resp, err := h.TransferOut(context.Background(), &assetholderrpc.TransferOutRequest{
		UserId: "u1", TransferId: "saga-out-1", Asset: "USDT",
		Amount: "150", PeerBiz: "funding",
	})
	if err != nil {
		t.Fatalf("TransferOut: %v", err)
	}
	if resp.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED {
		t.Fatalf("status = %v", resp.Status)
	}
	if resp.AvailableAfter != "350" {
		t.Errorf("available_after = %q, want 350", resp.AvailableAfter)
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()
	// seed + withdraw = 2 events
	if len(pub.events) != 2 {
		t.Fatalf("events = %d, want 2", len(pub.events))
	}
	last := pub.events[len(pub.events)-1].GetTransfer()
	if last.Type != eventpb.TransferEvent_TRANSFER_TYPE_WITHDRAW {
		t.Errorf("type = %v, want WITHDRAW", last.Type)
	}
	if last.SagaTransferId != "saga-out-1" {
		t.Errorf("saga_transfer_id = %q", last.SagaTransferId)
	}
}

func TestTransferOut_InsufficientBalance(t *testing.T) {
	h, _ := newHolderPair(t)
	seedDeposit(t, h, "u1", "USDT", "10")

	resp, err := h.TransferOut(context.Background(), &assetholderrpc.TransferOutRequest{
		UserId: "u1", TransferId: "saga-out-bad", Asset: "USDT",
		Amount: "100", PeerBiz: "funding",
	})
	if err != nil {
		t.Fatalf("TransferOut: %v", err)
	}
	if resp.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_REJECTED {
		t.Fatalf("status = %v, want REJECTED", resp.Status)
	}
	if resp.RejectReason != assetholderrpc.RejectReason_REJECT_REASON_INSUFFICIENT_BALANCE {
		t.Errorf("reject_reason = %v, want INSUFFICIENT_BALANCE", resp.RejectReason)
	}
}

// ---------------------------------------------------------------------------
// CompensateTransferOut
// ---------------------------------------------------------------------------

func TestCompensate_CreditsAndTags(t *testing.T) {
	h, pub := newHolderPair(t)

	resp, err := h.CompensateTransferOut(context.Background(), &assetholderrpc.CompensateTransferOutRequest{
		UserId:          "u1",
		TransferId:      "saga-compensate-1",
		Asset:           "USDT",
		Amount:          "100",
		PeerBiz:         "funding",
		CompensateCause: "peer_in_timeout",
	})
	if err != nil {
		t.Fatalf("Compensate: %v", err)
	}
	if resp.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED {
		t.Fatalf("status = %v", resp.Status)
	}
	if resp.AvailableAfter != "100" {
		t.Errorf("available_after = %q", resp.AvailableAfter)
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.events) != 1 {
		t.Fatalf("events = %d", len(pub.events))
	}
	ev := pub.events[0].GetTransfer()
	if ev.SagaTransferId != "saga-compensate-1" {
		t.Errorf("saga_transfer_id = %q", ev.SagaTransferId)
	}
	// Memo must carry the compensate marker so audit can distinguish
	// compensations from normal credits.
	wantMemo := "compensate: peer=funding cause=peer_in_timeout"
	if ev.Memo != wantMemo {
		t.Errorf("memo = %q, want %q", ev.Memo, wantMemo)
	}
	if ev.Type != eventpb.TransferEvent_TRANSFER_TYPE_DEPOSIT {
		t.Errorf("type = %v, want DEPOSIT (compensate rides the deposit leg)", ev.Type)
	}
}

// ---------------------------------------------------------------------------
// Argument / shard guards
// ---------------------------------------------------------------------------

func TestHolder_NilRequest(t *testing.T) {
	h, _ := newHolderPair(t)
	cases := []struct {
		name string
		call func() error
	}{
		{"out", func() error {
			_, err := h.TransferOut(context.Background(), nil)
			return err
		}},
		{"in", func() error {
			_, err := h.TransferIn(context.Background(), nil)
			return err
		}},
		{"compensate", func() error {
			_, err := h.CompensateTransferOut(context.Background(), nil)
			return err
		}},
	}
	for _, tc := range cases {
		err := tc.call()
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: code = %s, want InvalidArgument", tc.name, status.Code(err))
		}
	}
}

func TestHolder_InvalidAmount(t *testing.T) {
	h, _ := newHolderPair(t)
	cases := []string{"", "not-a-number", "0", "-1"}
	for _, amt := range cases {
		_, err := h.TransferIn(context.Background(), &assetholderrpc.TransferInRequest{
			UserId: "u1", TransferId: "saga-" + amt, Asset: "USDT",
			Amount: amt, PeerBiz: "funding",
		})
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("amount=%q: code = %s, want InvalidArgument", amt, status.Code(err))
		}
	}
}

func TestHolder_MissingFields(t *testing.T) {
	h, _ := newHolderPair(t)
	cases := []*assetholderrpc.TransferInRequest{
		{TransferId: "saga-1", Asset: "USDT", Amount: "10"}, // missing user_id
		{UserId: "u1", Asset: "USDT", Amount: "10"},          // missing transfer_id
		{UserId: "u1", TransferId: "saga-1", Amount: "10"},   // missing asset
	}
	for i, req := range cases {
		_, err := h.TransferIn(context.Background(), req)
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("case %d: code = %s, want InvalidArgument", i, status.Code(err))
		}
	}
}

func TestHolder_WrongShard(t *testing.T) {
	// Build a Service bound to shard 0 of a 2-shard cluster, so half of
	// user_ids don't belong to this shard and must get FailedPrecondition.
	state := engine.NewShardState(0)
	seq := sequencer.New()
	dt := dedup.New(time.Hour)
	pub := &fakePub{}
	svc := service.New(service.Config{ShardID: 0, TotalShards: 2, ProducerID: "counter-shard-0-main"},
		state, seq, dt, pub, zap.NewNop())
	h := NewAssetHolderServer(NewSingleServiceRouter(svc))

	// Find a user_id that does NOT belong to shard 0. Trial a short list;
	// 2 shards means hash parity, so at least one of these will be owned
	// by shard 1.
	var foreignUser string
	for _, u := range []string{"u0", "u1", "u2", "u3", "u4", "u5"} {
		if !svc.OwnsUser(u) {
			foreignUser = u
			break
		}
	}
	if foreignUser == "" {
		t.Fatal("could not find a user outside shard 0 in the trial set")
	}

	_, err := h.TransferIn(context.Background(), &assetholderrpc.TransferInRequest{
		UserId: foreignUser, TransferId: "saga-shard", Asset: "USDT",
		Amount: "10", PeerBiz: "funding",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("code = %s, want FailedPrecondition", status.Code(err))
	}
}
