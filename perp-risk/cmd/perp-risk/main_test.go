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

func TestHandler_FoldsDeficitQueriesCandidatesAndDispatchesTask(t *testing.T) {
	var gotReq perprisk.CandidateRequest
	var gotTask perprisk.ADLTaskWire
	shard := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case perprisk.RiskCandidatesPath:
			if err := json.NewDecoder(r.Body).Decode(&gotReq); err != nil {
				t.Fatal(err)
			}
			_ = json.NewEncoder(w).Encode(perprisk.CandidateResponse{Candidates: []perprisk.ADLCandidateWire{{
				UserID: "winner", Symbol: "BTC-USDT-PERP", Side: uint8(perpstate.SideSell),
				Size: "1", Score: "10", SacrificePerQty: "5", PosSeq: 77, PositionVersion: 3,
			}}})
		case perprisk.RiskTaskPath:
			if err := json.NewDecoder(r.Body).Decode(&gotTask); err != nil {
				t.Fatal(err)
			}
			_ = json.NewEncoder(w).Encode(perprisk.ADLTaskResponse{Applied: true})
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
	evt := &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Liquidation{Liquidation: &eventpb.PerpLiquidationEvent{
		UserId: "loser", Symbol: "BTC-USDT-PERP", LiqOrderId: 99,
		BankruptcyPrice: "90", InsuranceDelta: "-5",
	}}}
	applied, err := h.ApplyJournalEventAt(evt, 0, 12)
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("liquidation event should fold")
	}
	if got := coord.Fund("USDT"); got.Cmp(dec.New("-5")) != 0 {
		t.Fatalf("fund = %s, want -5", got)
	}
	if gotReq.Symbol != "BTC-USDT-PERP" || gotReq.AdlPrice != "90" || gotReq.ExcludeUser != "loser" {
		t.Fatalf("unexpected candidate request: %+v", gotReq)
	}
	if gotTask.UserID != "winner" || gotTask.PosSeq != 77 || gotTask.PositionVersion != 3 || gotTask.AdlRound != 1 || gotTask.Qty != "1" {
		t.Fatalf("unexpected ADL task: %+v", gotTask)
	}
}

func TestHandler_TakeoverBorrowsWorkingCapital(t *testing.T) {
	coord := perprisk.New()
	_ = coord.ApplyDelta(perprisk.InsuranceDelta{Coin: "USDT", Delta: dec.New("100")})
	h := &handler{coord: coord, rpc: shardrpc.New(0), logger: zap.NewNop()}

	evt := &eventpb.PerpJournalEvent{Payload: &eventpb.PerpJournalEvent_Takeover{Takeover: &eventpb.PerpTakeoverEvent{
		UserId: "loser", Symbol: "BTC-USDT-PERP", LiqOrderId: 9,
		BankruptcyPrice: "90", ClosedQty: "1", InsuranceDelta: "-5", TakeoverNotional: "20",
	}}}
	applied, err := h.ApplyJournalEventAt(evt, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	if !applied {
		t.Fatal("takeover event should fold")
	}
	if got := coord.Fund("USDT"); got.Cmp(dec.New("75")) != 0 {
		t.Fatalf("fund after takeover fold + borrow = %s, want 75", got)
	}
	snap := coord.Snapshot()
	if len(snap.Loans) != 1 || snap.Loans[0].RefID != "takeover:9" || snap.Loans[0].Principal != "20" {
		t.Fatalf("loan snapshot wrong: %+v", snap.Loans)
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
