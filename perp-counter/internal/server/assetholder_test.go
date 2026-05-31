package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	assetholderrpc "github.com/xargin/opentrade/api/gen/rpc/assetholder"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/perp-counter/internal/service"
)

func newHolder() (*AssetHolderServer, *engine.Engine) {
	eng := engine.New()
	svc := service.New(eng, nil, nil, func() uint64 { return 1 }, service.Config{ProducerID: "p"})
	return NewAssetHolderServer(svc), eng
}

func TestAssetHolder_TransferInThenOut(t *testing.T) {
	h, eng := newHolder()
	ctx := context.Background()

	in, err := h.TransferIn(ctx, connect.NewRequest(&assetholderrpc.TransferInRequest{
		UserId: 1001, TransferId: "tx1", Asset: "USDT", Amount: "250"}))
	if err != nil {
		t.Fatalf("transfer in: %v", err)
	}
	if in.Msg.GetStatus() != assetholderrpc.TransferStatus_TRANSFER_STATUS_CONFIRMED {
		t.Fatalf("status = %v, want confirmed", in.Msg.GetStatus())
	}
	if in.Msg.GetAvailableAfter() != "250" {
		t.Fatalf("available_after = %s, want 250", in.Msg.GetAvailableAfter())
	}

	out, err := h.TransferOut(ctx, connect.NewRequest(&assetholderrpc.TransferOutRequest{
		UserId: 1001, TransferId: "tx2", Asset: "USDT", Amount: "100"}))
	if err != nil {
		t.Fatalf("transfer out: %v", err)
	}
	if out.Msg.GetAvailableAfter() != "150" {
		t.Fatalf("available_after = %s, want 150", out.Msg.GetAvailableAfter())
	}
	if eng.WalletOf(1001).Available.String() != "150" {
		t.Fatalf("wallet = %s, want 150", eng.WalletOf(1001).Available)
	}
}

func TestAssetHolder_RejectAndDuplicate(t *testing.T) {
	h, _ := newHolder()
	ctx := context.Background()

	// Over-withdraw on an empty wallet → REJECTED / INSUFFICIENT_BALANCE.
	rej, err := h.TransferOut(ctx, connect.NewRequest(&assetholderrpc.TransferOutRequest{
		UserId: 1001, TransferId: "tx1", Asset: "USDT", Amount: "5"}))
	if err != nil {
		t.Fatalf("transfer out: %v", err)
	}
	if rej.Msg.GetStatus() != assetholderrpc.TransferStatus_TRANSFER_STATUS_REJECTED ||
		rej.Msg.GetRejectReason() != assetholderrpc.RejectReason_REJECT_REASON_INSUFFICIENT_BALANCE {
		t.Fatalf("want rejected/insufficient, got %v/%v", rej.Msg.GetStatus(), rej.Msg.GetRejectReason())
	}

	// Idempotent credit.
	h.TransferIn(ctx, connect.NewRequest(&assetholderrpc.TransferInRequest{
		UserId: 1001, TransferId: "dep", Asset: "USDT", Amount: "10"}))
	dup, _ := h.TransferIn(ctx, connect.NewRequest(&assetholderrpc.TransferInRequest{
		UserId: 1001, TransferId: "dep", Asset: "USDT", Amount: "10"}))
	if dup.Msg.GetStatus() != assetholderrpc.TransferStatus_TRANSFER_STATUS_DUPLICATED {
		t.Fatalf("repeat status = %v, want duplicated", dup.Msg.GetStatus())
	}
}

func TestAssetHolder_Validation(t *testing.T) {
	h, _ := newHolder()
	ctx := context.Background()
	bad := []*assetholderrpc.TransferInRequest{
		{UserId: 0, TransferId: "t", Asset: "USDT", Amount: "1"},
		{UserId: 1001, TransferId: "t", Asset: "USDT", Amount: "abc"},
		{UserId: 1001, TransferId: "", Asset: "USDT", Amount: "1"},
		{UserId: 1001, TransferId: "t", Asset: "", Amount: "1"},
		{UserId: 1001, TransferId: "t", Asset: "USDT", Amount: "0"},
		{UserId: 1001, TransferId: "t", Asset: "USDT", Amount: "-5"},
	}
	for i, req := range bad {
		if _, err := h.TransferIn(ctx, connect.NewRequest(req)); err == nil {
			t.Errorf("case %d: expected InvalidArgument, got nil", i)
		}
	}
}
