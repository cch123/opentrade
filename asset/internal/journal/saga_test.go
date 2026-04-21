package journal

import "testing"

func TestBuildSagaStateChange(t *testing.T) {
	evt := BuildSagaStateChange(SagaStateChangeInput{
		AssetSeqID:   42,
		TsUnixMS:     1_700_000_000_000,
		ProducerID:   "asset-main",
		TransferID:   "saga-1",
		UserID:       "u1",
		FromBiz:      "funding",
		ToBiz:        "spot",
		Asset:        "USDT",
		Amount:       "100",
		OldState:     "INIT",
		NewState:     "DEBITED",
		RejectReason: "",
	})
	if evt.AssetSeqId != 42 {
		t.Errorf("asset_seq_id = %d", evt.AssetSeqId)
	}
	if evt.FundingVersion != 0 {
		t.Errorf("funding_version = %d, want 0 for saga_state events", evt.FundingVersion)
	}
	p := evt.GetSagaState()
	if p == nil {
		t.Fatal("payload not SagaState")
	}
	if p.NewState != "DEBITED" || p.OldState != "INIT" {
		t.Errorf("states = %q → %q", p.OldState, p.NewState)
	}
	if p.FromBiz != "funding" || p.ToBiz != "spot" {
		t.Errorf("routing = %q → %q", p.FromBiz, p.ToBiz)
	}
}
