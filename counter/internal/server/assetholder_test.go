package server

import (
	"context"
	"strconv"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.uber.org/zap"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	assetholderrpc "github.com/xargin/opentrade/api/gen/rpc/assetholder"
	"github.com/xargin/opentrade/counter/internal/dedup"
	"github.com/xargin/opentrade/counter/internal/sequencer"
	"github.com/xargin/opentrade/counter/internal/service"
	"github.com/xargin/opentrade/pkg/counterstate"
	"github.com/xargin/opentrade/pkg/dec"
	countersnapshot "github.com/xargin/opentrade/pkg/snapshot/counter"
)

func newHolderPair(t *testing.T) (*AssetHolderServer, *fakePub) {
	t.Helper()
	state := counterstate.NewShardState(0)
	h, pub := newHolderForState(t, state)
	return h, pub
}

func newHolderForState(t *testing.T, state *counterstate.ShardState) (*AssetHolderServer, *fakePub) {
	t.Helper()
	seq := sequencer.New()
	dt := dedup.New(time.Hour)
	pub := &fakePub{}
	svc := service.New(service.Config{ShardID: 0, ProducerID: "counter-shard-0-main"},
		state, seq, dt, pub, zap.NewNop())
	return NewAssetHolderServer(NewSingleServiceRouter(svc)), pub
}

// seedDeposit pre-funds a user so subsequent TransferOut has balance.
func seedDeposit(t *testing.T, h *AssetHolderServer, userID uint64, asset, amount string) {
	t.Helper()
	userLabel := strconv.FormatUint(userID, 10)
	resp, err := h.TransferIn(context.Background(), connect.NewRequest(&assetholderrpc.TransferInRequest{
		UserId: userID, TransferId: "seed-" + userLabel + "-" + asset + "-" + amount,
		Asset: asset, Amount: amount, PeerBiz: "funding",
	}))
	if err != nil {
		t.Fatalf("seed TransferIn: %v", err)
	}
	if resp.Msg.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED {
		t.Fatalf("seed status = %v", resp.Msg.Status)
	}
}

// ---------------------------------------------------------------------------
// TransferIn
// ---------------------------------------------------------------------------

func TestTransferIn_Confirmed(t *testing.T) {
	h, pub := newHolderPair(t)

	resp, err := h.TransferIn(context.Background(), connect.NewRequest(&assetholderrpc.TransferInRequest{
		UserId:     1001,
		TransferId: "saga-1",
		Asset:      "USDT",
		Amount:     "100",
		PeerBiz:    "funding",
		Memo:       "test",
	}))
	if err != nil {
		t.Fatalf("TransferIn: %v", err)
	}
	if resp.Msg.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED {
		t.Fatalf("status = %v", resp.Msg.Status)
	}
	if resp.Msg.AvailableAfter != "100" {
		t.Errorf("available_after = %q", resp.Msg.AvailableAfter)
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
	if want := holderDedupKey(holderOperationTransferIn, "saga-1"); xfer.TransferId != want {
		t.Errorf("transfer_id = %q, want internal key %q", xfer.TransferId, want)
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

	build := func() *connect.Request[assetholderrpc.TransferInRequest] {
		return connect.NewRequest(&assetholderrpc.TransferInRequest{
			UserId: 1001, TransferId: "saga-1", Asset: "USDT",
			Amount: "100", PeerBiz: "funding",
		})
	}

	first, err := h.TransferIn(context.Background(), build())
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if first.Msg.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED {
		t.Fatalf("first status = %v", first.Msg.Status)
	}

	second, err := h.TransferIn(context.Background(), build())
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second.Msg.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_DUPLICATED {
		t.Fatalf("second status = %v, want DUPLICATED", second.Msg.Status)
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
	seedDeposit(t, h, 1001, "USDT", "500")

	resp, err := h.TransferOut(context.Background(), connect.NewRequest(&assetholderrpc.TransferOutRequest{
		UserId: 1001, TransferId: "saga-out-1", Asset: "USDT",
		Amount: "150", PeerBiz: "funding",
	}))
	if err != nil {
		t.Fatalf("TransferOut: %v", err)
	}
	if resp.Msg.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED {
		t.Fatalf("status = %v", resp.Msg.Status)
	}
	if resp.Msg.AvailableAfter != "350" {
		t.Errorf("available_after = %q, want 350", resp.Msg.AvailableAfter)
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
	seedDeposit(t, h, 1001, "USDT", "10")

	resp, err := h.TransferOut(context.Background(), connect.NewRequest(&assetholderrpc.TransferOutRequest{
		UserId: 1001, TransferId: "saga-out-bad", Asset: "USDT",
		Amount: "100", PeerBiz: "funding",
	}))
	if err != nil {
		t.Fatalf("TransferOut: %v", err)
	}
	if resp.Msg.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_REJECTED {
		t.Fatalf("status = %v, want REJECTED", resp.Msg.Status)
	}
	if resp.Msg.RejectReason != assetholderrpc.RejectReason_REJECT_REASON_INSUFFICIENT_BALANCE {
		t.Errorf("reject_reason = %v, want INSUFFICIENT_BALANCE", resp.Msg.RejectReason)
	}
}

// ---------------------------------------------------------------------------
// CompensateTransferOut
// ---------------------------------------------------------------------------

func TestCompensate_CreditsAndTags(t *testing.T) {
	h, pub := newHolderPair(t)

	resp, err := h.CompensateTransferOut(context.Background(), connect.NewRequest(&assetholderrpc.CompensateTransferOutRequest{
		UserId:          1001,
		TransferId:      "saga-compensate-1",
		Asset:           "USDT",
		Amount:          "100",
		PeerBiz:         "funding",
		CompensateCause: "peer_in_timeout",
	}))
	if err != nil {
		t.Fatalf("Compensate: %v", err)
	}
	if resp.Msg.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED {
		t.Fatalf("status = %v", resp.Msg.Status)
	}
	if resp.Msg.AvailableAfter != "100" {
		t.Errorf("available_after = %q", resp.Msg.AvailableAfter)
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
	if want := holderDedupKey(holderOperationCompensateTransferOut, "saga-compensate-1"); ev.TransferId != want {
		t.Errorf("transfer_id = %q, want internal key %q", ev.TransferId, want)
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

func TestCompensate_SameTransferIDRestoresBalanceExactlyOnce(t *testing.T) {
	state := counterstate.NewShardState(0)
	h, pub := newHolderForState(t, state)
	seedDeposit(t, h, 1001, "USDT", "500")

	transferID := "saga-out-compensate-same-id"
	outReq := connect.NewRequest(&assetholderrpc.TransferOutRequest{
		UserId: 1001, TransferId: transferID, Asset: "USDT",
		Amount: "150", PeerBiz: "funding",
	})
	out, err := h.TransferOut(context.Background(), outReq)
	if err != nil {
		t.Fatalf("TransferOut: %v", err)
	}
	if out.Msg.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED || out.Msg.AvailableAfter != "350" {
		t.Fatalf("TransferOut = status %v balance %s, want CONFIRMED/350", out.Msg.Status, out.Msg.AvailableAfter)
	}

	compensate := func() *connect.Response[assetholderrpc.CompensateTransferOutResponse] {
		t.Helper()
		resp, err := h.CompensateTransferOut(context.Background(), connect.NewRequest(&assetholderrpc.CompensateTransferOutRequest{
			UserId: 1001, TransferId: transferID, Asset: "USDT",
			Amount: "150", PeerBiz: "funding", CompensateCause: "peer_in_failed",
		}))
		if err != nil {
			t.Fatalf("CompensateTransferOut: %v", err)
		}
		return resp
	}

	first := compensate()
	if first.Msg.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED || first.Msg.AvailableAfter != "500" {
		t.Fatalf("first compensate = status %v balance %s, want CONFIRMED/500", first.Msg.Status, first.Msg.AvailableAfter)
	}
	second := compensate()
	if second.Msg.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_DUPLICATED {
		t.Fatalf("second compensate status = %v, want DUPLICATED", second.Msg.Status)
	}
	if got := state.Balance(1001, "USDT").Available.String(); got != "500" {
		t.Fatalf("balance after duplicate compensation = %s, want 500", got)
	}

	pub.mu.Lock()
	defer pub.mu.Unlock()
	if len(pub.events) != 3 { // seed + debit + one compensation
		t.Fatalf("events = %d, want 3", len(pub.events))
	}
	outEvent := pub.events[1].GetTransfer()
	compensateEvent := pub.events[2].GetTransfer()
	if outEvent.TransferId == compensateEvent.TransferId {
		t.Fatalf("out and compensate shared internal dedup key %q", outEvent.TransferId)
	}
	if outEvent.SagaTransferId != transferID || compensateEvent.SagaTransferId != transferID {
		t.Fatalf("external saga IDs changed: out=%q compensate=%q", outEvent.SagaTransferId, compensateEvent.SagaTransferId)
	}
}

func TestAssetHolderDedup_LegacySnapshotAndQualifiedJournalReplay(t *testing.T) {
	const (
		userID       = uint64(1001)
		transferID   = "legacy-saga-id"
		inUserID     = uint64(1002)
		inTransferID = "legacy-in-saga-id"
	)

	// Pre-fix snapshots stored only the raw saga ID after TransferOut. Restore
	// one through the real snapshot path to pin rolling-upgrade compatibility.
	legacy := counterstate.NewShardState(0)
	legacy.Account(userID).PutForRestore("USDT", counterstate.Balance{Available: dec.New("350"), Version: 1})
	legacy.Account(userID).RememberTransfer(transferID)
	legacy.Account(inUserID).PutForRestore("USDT", counterstate.Balance{Available: dec.New("100"), Version: 1})
	legacy.Account(inUserID).RememberTransfer(inTransferID)
	snap := countersnapshot.CaptureFromState(0, legacy, 1, nil, 0, time.Now().UnixMilli())
	restored := counterstate.NewShardState(0)
	if err := countersnapshot.RestoreState(0, restored, snap); err != nil {
		t.Fatalf("RestoreState: %v", err)
	}
	h, pub := newHolderForState(t, restored)

	// Out keeps the old raw key as a lookup-only fallback, so an old request
	// cannot debit twice after upgrade.
	out, err := h.TransferOut(context.Background(), connect.NewRequest(&assetholderrpc.TransferOutRequest{
		UserId: userID, TransferId: transferID, Asset: "USDT", Amount: "150", PeerBiz: "funding",
	}))
	if err != nil {
		t.Fatalf("legacy TransferOut retry: %v", err)
	}
	if out.Msg.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_DUPLICATED {
		t.Fatalf("legacy TransferOut retry status = %v, want DUPLICATED", out.Msg.Status)
	}
	in, err := h.TransferIn(context.Background(), connect.NewRequest(&assetholderrpc.TransferInRequest{
		UserId: inUserID, TransferId: inTransferID, Asset: "USDT", Amount: "100", PeerBiz: "funding",
	}))
	if err != nil {
		t.Fatalf("legacy TransferIn retry: %v", err)
	}
	if in.Msg.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_DUPLICATED {
		t.Fatalf("legacy TransferIn retry status = %v, want DUPLICATED", in.Msg.Status)
	}
	if got := restored.Balance(inUserID, "USDT").Available.String(); got != "100" {
		t.Fatalf("legacy TransferIn retry balance = %s, want 100", got)
	}

	// Compensation must ignore that ambiguous legacy key because it belongs
	// to the original debit, then persist its own qualified key.
	compReq := func() *connect.Request[assetholderrpc.CompensateTransferOutRequest] {
		return connect.NewRequest(&assetholderrpc.CompensateTransferOutRequest{
			UserId: userID, TransferId: transferID, Asset: "USDT", Amount: "150",
			PeerBiz: "funding", CompensateCause: "upgrade_recovery",
		})
	}
	comp, err := h.CompensateTransferOut(context.Background(), compReq())
	if err != nil {
		t.Fatalf("legacy compensation: %v", err)
	}
	if comp.Msg.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED || comp.Msg.AvailableAfter != "500" {
		t.Fatalf("legacy compensation = status %v balance %s, want CONFIRMED/500", comp.Msg.Status, comp.Msg.AvailableAfter)
	}

	pub.mu.Lock()
	eventCount := len(pub.events)
	if eventCount != 1 {
		pub.mu.Unlock()
		t.Fatalf("events = %d, want only compensation", eventCount)
	}
	compEvent := pub.events[0]
	pub.mu.Unlock()

	// Catch-up replay remembers TransferEvent.transfer_id verbatim. A retry on
	// the recovered process must therefore dedup without another credit.
	replayed := counterstate.NewShardState(0)
	if err := counterstate.ApplyCounterJournalEvent(replayed, compEvent); err != nil {
		t.Fatalf("ApplyCounterJournalEvent: %v", err)
	}
	replayedHolder, replayPub := newHolderForState(t, replayed)
	retry, err := replayedHolder.CompensateTransferOut(context.Background(), compReq())
	if err != nil {
		t.Fatalf("replayed compensation retry: %v", err)
	}
	if retry.Msg.Status != assetholderrpc.TransferStatus_TRANSFER_STATUS_DUPLICATED {
		t.Fatalf("replayed compensation retry status = %v, want DUPLICATED", retry.Msg.Status)
	}
	if got := replayed.Balance(userID, "USDT").Available.String(); got != "500" {
		t.Fatalf("replayed balance after duplicate = %s, want 500", got)
	}
	replayPub.mu.Lock()
	replayEventCount := len(replayPub.events)
	replayPub.mu.Unlock()
	if replayEventCount != 0 {
		t.Fatalf("replayed duplicate published %d events", replayEventCount)
	}
}

// ---------------------------------------------------------------------------
// Argument / shard guards
// ---------------------------------------------------------------------------

func TestHolder_NilMsg(t *testing.T) {
	h, _ := newHolderPair(t)
	cases := []struct {
		name string
		call func() error
	}{
		{"out", func() error {
			_, err := h.TransferOut(context.Background(), connect.NewRequest((*assetholderrpc.TransferOutRequest)(nil)))
			return err
		}},
		{"in", func() error {
			_, err := h.TransferIn(context.Background(), connect.NewRequest((*assetholderrpc.TransferInRequest)(nil)))
			return err
		}},
		{"compensate", func() error {
			_, err := h.CompensateTransferOut(context.Background(), connect.NewRequest((*assetholderrpc.CompensateTransferOutRequest)(nil)))
			return err
		}},
	}
	for _, tc := range cases {
		err := tc.call()
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("%s: code = %s, want InvalidArgument", tc.name, connect.CodeOf(err))
		}
	}
}

func TestHolder_InvalidAmount(t *testing.T) {
	h, _ := newHolderPair(t)
	cases := []string{"", "not-a-number", "0", "-1"}
	for _, amt := range cases {
		_, err := h.TransferIn(context.Background(), connect.NewRequest(&assetholderrpc.TransferInRequest{
			UserId: 1001, TransferId: "saga-" + amt, Asset: "USDT",
			Amount: amt, PeerBiz: "funding",
		}))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("amount=%q: code = %s, want InvalidArgument", amt, connect.CodeOf(err))
		}
	}
}

func TestHolder_MissingFields(t *testing.T) {
	h, _ := newHolderPair(t)
	cases := []*assetholderrpc.TransferInRequest{
		{TransferId: "saga-1", Asset: "USDT", Amount: "10"}, // missing user_id
		{UserId: 1001, Asset: "USDT", Amount: "10"},         // missing transfer_id
		{UserId: 1001, TransferId: "saga-1", Amount: "10"},  // missing asset
	}
	for i, req := range cases {
		_, err := h.TransferIn(context.Background(), connect.NewRequest(req))
		if connect.CodeOf(err) != connect.CodeInvalidArgument {
			t.Errorf("case %d: code = %s, want InvalidArgument", i, connect.CodeOf(err))
		}
	}
}

func TestHolder_WrongShard(t *testing.T) {
	// Build a Service bound to shard 0 of a 2-shard cluster, so half of
	// user_ids don't belong to this shard and must get FailedPrecondition.
	state := counterstate.NewShardState(0)
	seq := sequencer.New()
	dt := dedup.New(time.Hour)
	pub := &fakePub{}
	svc := service.New(service.Config{ShardID: 0, TotalShards: 2, ProducerID: "counter-shard-0-main"},
		state, seq, dt, pub, zap.NewNop())
	h := NewAssetHolderServer(NewSingleServiceRouter(svc))

	// Find a user_id that does NOT belong to shard 0. Trial a short list;
	// 2 shards means hash parity, so at least one of these will be owned
	// by shard 1.
	var foreignUser uint64
	for _, u := range []uint64{1000, 1001, 1002, 1003, 1004, 1005} {
		if !svc.OwnsUser(u) {
			foreignUser = u
			break
		}
	}
	if foreignUser == 0 {
		t.Fatal("could not find a user outside shard 0 in the trial set")
	}

	_, err := h.TransferIn(context.Background(), connect.NewRequest(&assetholderrpc.TransferInRequest{
		UserId: foreignUser, TransferId: "saga-shard", Asset: "USDT",
		Amount: "10", PeerBiz: "funding",
	}))
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("code = %s, want FailedPrecondition", connect.CodeOf(err))
	}
}
