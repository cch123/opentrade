package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/perp-risk/internal/shardrpc"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perprisk"
	"github.com/xargin/opentrade/pkg/perpstate"
	"go.uber.org/zap"
)

func TestHandler_TakeoverLotQueriesCandidatesAndDispatchesTask(t *testing.T) {
	var gotReq perprisk.CandidateRequest
	var gotTask perprisk.ADLTaskWire
	shard := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case perprisk.RiskCandidatesPath:
			if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
				t.Fatal(err)
			}
			_ = json.NewEncoder(w).Encode(perprisk.CandidateResponse{Candidates: []perprisk.ADLCandidateWire{{
				UserID: 2001, Symbol: "BTC-USDT-PERP", Side: uint8(perpstate.SideSell),
				Size: "1", Score: "10", SacrificePerQty: "5", PosSeq: 77, PositionVersion: 3,
			}}})
		case perprisk.RiskTaskPath:
			if err := json.NewDecoder(r.Body).Decode(&gotTask); err != nil {
				t.Fatal(err)
			}
			_ = json.NewEncoder(w).Encode(perprisk.ADLTaskResponse{Applied: true, FactQty: "1", RealizedPnL: "5"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer shard.Close()

	coord := perprisk.New()
	h := &handler{
		coord: coord, shards: []string{shard.URL},
		rpc: shardrpc.New(0), logger: zap.NewNop(),
	}
	_ = coord.ApplyDelta(perprisk.InsuranceDelta{Coin: "USDT", Delta: dec.New("100")})
	evt := &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Takeover{Takeover: &eventpb.PerpTakeoverEvent{
		UserId: 1001, Symbol: "BTC-USDT-PERP", LotId: "lot-99", LiqOrderId: 99,
		BankruptcyPrice: "90", ClosedQty: "1", TakenOverQty: "1", TakeoverPrice: "90",
		TakenOverBalance: "-5", TakeoverNotional: "20", InventorySide: eventpb.Side_SIDE_BUY,
	}}}
	applied, err := h.ApplyJournalEventAt(evt, 0, 12)
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("takeover event should apply")
	}
	if got := coord.Fund("USDT"); got.Cmp(dec.New("80")) != 0 {
		t.Fatalf("fund after working-capital borrow = %s, want 80", got)
	}
	if gotReq.Symbol != "BTC-USDT-PERP" || gotReq.AdlPrice != "90" || gotReq.ExcludeUser != 1001 {
		t.Fatalf("unexpected candidate request: %+v", gotReq)
	}
	if gotTask.LotID != "lot-99" || gotTask.UserID != 2001 || gotTask.PosSeq != 77 || gotTask.PositionVersion != 3 || gotTask.AdlRound != 1 || gotTask.Qty != "1" {
		t.Fatalf("unexpected ADL task: %+v", gotTask)
	}
	if got := coord.Snapshot().InFlightADL; len(got) != 1 || got[0].LotID != "lot-99" || got[0].AdlRound != 1 {
		t.Fatalf("in-flight ADL snapshot = %+v", got)
	}
	applied, err = h.ApplyJournalEventAt(&eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Adl{Adl: &eventpb.PerpAdlEvent{
		UserId: 2001, Symbol: "BTC-USDT-PERP", LotId: "lot-99", AdlRound: 1,
		Price: "90", FactQty: "1", RealizedPnl: "5",
	}}}, 0, 13)
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("ADL event should apply to the lot")
	}
	if got := coord.Snapshot().InFlightADL; len(got) != 0 {
		t.Fatalf("ADL event should clear in-flight task, got %+v", got)
	}
}

func TestHandler_TakeoverBorrowsWorkingCapital(t *testing.T) {
	coord := perprisk.New()
	_ = coord.ApplyDelta(perprisk.InsuranceDelta{Coin: "USDT", Delta: dec.New("100")})
	h := &handler{coord: coord, rpc: shardrpc.New(0), logger: zap.NewNop()}

	evt := &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Takeover{Takeover: &eventpb.PerpTakeoverEvent{
		UserId: 1001, Symbol: "BTC-USDT-PERP", LotId: "lot-9", LiqOrderId: 9,
		BankruptcyPrice: "90", ClosedQty: "1", TakenOverBalance: "-5", TakeoverNotional: "20",
		InventorySide: eventpb.Side_SIDE_BUY,
	}}}
	applied, err := h.ApplyJournalEventAt(evt, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("takeover event should fold")
	}
	if got := coord.Fund("USDT"); got.Cmp(dec.New("80")) != 0 {
		t.Fatalf("fund after takeover borrow = %s, want 80", got)
	}
	snap := coord.Snapshot()
	if len(snap.Loans) != 1 || snap.Loans[0].RefID != "takeover:lot-9" || snap.Loans[0].Principal != "20" {
		t.Fatalf("loan snapshot wrong: %+v", snap.Loans)
	}
	if len(snap.Lots) != 1 || snap.Lots[0].LotID != "lot-9" || snap.Lots[0].WorkingCapital != "20" {
		t.Fatalf("lot snapshot wrong: %+v", snap.Lots)
	}
}

func TestRiskHTTP_WorkingCapitalRepay(t *testing.T) {
	coord := perprisk.New()
	_ = coord.ApplyDelta(perprisk.InsuranceDelta{Coin: "USDT", Delta: dec.New("100")})
	if _, err := coord.BorrowWorkingCapital(perprisk.BorrowRequest{
		Coin: "USDT", Symbol: "BTC-USDT-PERP", Day: "2026-05-31",
		RefID: "takeover:9", Amount: dec.New("20"),
	}); err != nil {
		t.Fatal(err)
	}
	h := &handler{coord: coord, rpc: shardrpc.New(0), logger: zap.NewNop()}
	srv := riskHTTPServer(":0", h, zap.NewNop())
	httpSrv := httptest.NewServer(srv.Handler)
	defer httpSrv.Close()

	body, _ := json.Marshal(perprisk.WorkingCapitalRepayRequest{RefID: "takeover:9", Amount: "7"})
	resp, err := http.Post(httpSrv.URL+perprisk.WorkingCapitalRepayPath, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("repay status = %s", resp.Status)
	}
	if got := coord.Fund("USDT"); got.Cmp(dec.New("87")) != 0 {
		t.Fatalf("fund after repay = %s, want 87", got)
	}
}
