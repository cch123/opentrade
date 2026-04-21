package journal

import (
	"testing"

	"github.com/xargin/opentrade/asset/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

func TestBuild_TransferOut(t *testing.T) {
	evt := Build(BuildInput{
		Kind:           KindTransferOut,
		AssetSeqID:     42,
		TsUnixMS:       1_700_000_000_000,
		ProducerID:     "asset-main",
		FundingVersion: 7,
		UserID:         "u1",
		TransferID:     "saga-1",
		Asset:          "USDT",
		Amount:         "100",
		PeerBiz:        "spot",
		Memo:           "hi",
		BalanceAfter: engine.Balance{
			Available: mustDec(t, "400"),
			Frozen:    mustDec(t, "0"),
			Version:   3,
		},
	})
	if evt.AssetSeqId != 42 {
		t.Errorf("asset_seq_id = %d", evt.AssetSeqId)
	}
	if evt.FundingVersion != 7 {
		t.Errorf("funding_version = %d", evt.FundingVersion)
	}
	out := evt.GetXferOut()
	if out == nil {
		t.Fatalf("payload not XferOut: %+v", evt.Payload)
	}
	if out.TransferId != "saga-1" || out.Amount != "100" || out.PeerBiz != "spot" {
		t.Errorf("out payload = %+v", out)
	}
	if out.BalanceAfter.Available != "400" || out.BalanceAfter.Version != 3 {
		t.Errorf("balance_after = %+v", out.BalanceAfter)
	}
}

func TestBuild_TransferIn(t *testing.T) {
	evt := Build(BuildInput{
		Kind:       KindTransferIn,
		AssetSeqID: 1,
		UserID:     "u1",
		TransferID: "saga-2",
		Asset:      "BTC",
		Amount:     "0.5",
		PeerBiz:    "spot",
		BalanceAfter: engine.Balance{
			Available: mustDec(t, "0.5"),
			Frozen:    mustDec(t, "0"),
			Version:   1,
		},
	})
	in := evt.GetXferIn()
	if in == nil {
		t.Fatalf("payload not XferIn")
	}
	if in.TransferId != "saga-2" || in.Amount != "0.5" {
		t.Errorf("in payload = %+v", in)
	}
}

func TestBuild_Compensate(t *testing.T) {
	evt := Build(BuildInput{
		Kind:            KindCompensate,
		AssetSeqID:      99,
		UserID:          "u1",
		TransferID:      "saga-3",
		Asset:           "USDT",
		Amount:          "30",
		PeerBiz:         "spot",
		CompensateCause: "peer_in_rejected",
		BalanceAfter: engine.Balance{
			Available: mustDec(t, "130"),
			Version:   5,
		},
	})
	c := evt.GetCompensate()
	if c == nil {
		t.Fatalf("payload not Compensate")
	}
	if c.CompensateCause != "peer_in_rejected" {
		t.Errorf("compensate_cause = %q", c.CompensateCause)
	}
}

func TestBuild_DefaultTs(t *testing.T) {
	evt := Build(BuildInput{Kind: KindTransferIn, UserID: "u", Asset: "USDT", Amount: "1"})
	if evt.Meta.TsUnixMs == 0 {
		t.Error("ts should default to now")
	}
}

func mustDec(t *testing.T, s string) dec.Decimal {
	t.Helper()
	d, err := dec.Parse(s)
	if err != nil {
		t.Fatalf("dec.Parse(%q): %v", s, err)
	}
	return d
}
