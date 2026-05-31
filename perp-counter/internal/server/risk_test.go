package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/perp-counter/internal/service"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perprisk"
	"github.com/xargin/opentrade/pkg/perpstate"
)

type riskNoopDispatch struct{}

func (riskNoopDispatch) DispatchOrder(string, *eventpb.OrderEvent) error  { return nil }
func (riskNoopDispatch) DispatchCancel(string, *eventpb.OrderEvent) error { return nil }

type riskJournal struct {
	events []*eventpb.PerpJournalEvent
}

func (j *riskJournal) Emit(e *eventpb.PerpJournalEvent) { j.events = append(j.events, e) }

func TestRiskHandlers_CandidatesAndTask(t *testing.T) {
	const winnerUser uint64 = 2001
	eng := engine.New()
	journal := &riskJournal{}
	var id uint64
	svc := service.New(eng, riskNoopDispatch{}, journal, func() uint64 { id++; return id }, service.Config{
		MaxLeverage: dec.New("100"), RiskCoordinatorEnabled: true, ProducerID: "perp-shard-0",
	})
	openServerRiskPosition(eng, winnerUser, "BTC-USDT-PERP", perpstate.SideSell, "100", "1", "10")
	eng.SetMark("BTC-USDT-PERP", dec.New("85"))

	mux := http.NewServeMux()
	RegisterRiskHandlers(mux, svc)
	server := httptest.NewServer(mux)
	defer server.Close()

	body, _ := json.Marshal(perprisk.CandidateRequest{Symbol: "BTC-USDT-PERP", AdlPrice: "90"})
	resp, err := http.Post(server.URL+perprisk.RiskCandidatesPath, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var candidates perprisk.CandidateResponse
	if err := json.NewDecoder(resp.Body).Decode(&candidates); err != nil {
		t.Fatal(err)
	}
	if len(candidates.Candidates) != 1 || candidates.Candidates[0].UserID != winnerUser {
		t.Fatalf("unexpected candidates: %+v", candidates.Candidates)
	}
	if candidates.Candidates[0].PositionVersion == 0 {
		t.Fatalf("candidate must carry position_version: %+v", candidates.Candidates[0])
	}

	task := perprisk.ADLTaskWire{
		LotID: "lot-http", UserID: winnerUser, Symbol: "BTC-USDT-PERP", Side: uint8(perpstate.SideSell),
		Qty: "1", Price: "90", PosSeq: candidates.Candidates[0].PosSeq,
		PositionVersion: candidates.Candidates[0].PositionVersion, AdlRound: 1,
	}
	body, _ = json.Marshal(task)
	resp, err = http.Post(server.URL+perprisk.RiskTaskPath, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var taskResp perprisk.ADLTaskResponse
	if err := json.NewDecoder(resp.Body).Decode(&taskResp); err != nil {
		t.Fatal(err)
	}
	if !taskResp.Applied {
		t.Fatal("task should apply with matching pos_seq")
	}
	if _, ok := eng.PositionOf(winnerUser, "BTC-USDT-PERP"); ok {
		t.Fatal("ADL task should close the one-lot winner")
	}
}

func openServerRiskPosition(e *engine.Engine, user uint64, symbol string, side perpstate.Side, price, qty, lev string) {
	p := dec.New(price)
	q := dec.New(qty)
	l := dec.New(lev)
	im := perpstate.InitMargin(p, q, l)
	e.Deposit(user, im)
	e.Reserve(user, im)
	e.ApplyFill(user, symbol, l, perpstate.Fill{Side: side, Price: p, Qty: q})
}
