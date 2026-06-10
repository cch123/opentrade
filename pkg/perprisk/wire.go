package perprisk

import (
	"fmt"

	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

const (
	RiskCandidatesPath      = "/internal/perp-risk/adl-candidates"
	RiskTaskPath            = "/internal/perp-risk/adl-task"
	WorkingCapitalRepayPath = "/internal/perp-risk/working-capital-repay"
)

// CandidateRequest is the internal RPC request perp-risk sends to each
// perp-counter shard. The request is price-scoped because the amount a winner
// can surrender depends on the ADL/bankruptcy price being used for this round.
type CandidateRequest struct {
	Symbol      string `json:"symbol"`
	AdlPrice    string `json:"adl_price"`
	ExcludeUser uint64 `json:"exclude_user,omitempty"`
}

type CandidateResponse struct {
	Candidates []ADLCandidateWire `json:"candidates"`
}

type ADLCandidateWire struct {
	UserID          uint64 `json:"user_id"`
	Symbol          string `json:"symbol"`
	PositionIdx     uint8  `json:"position_idx,omitempty"` // ADR-0077 leg
	Side            uint8  `json:"side"`
	Size            string `json:"size"`
	Score           string `json:"score"`
	SacrificePerQty string `json:"sacrifice_per_qty"`
	PosSeq          uint64 `json:"pos_seq"`
	PositionVersion uint64 `json:"position_version"`
}

type ADLTaskWire struct {
	LotID           string `json:"lot_id"`
	UserID          uint64 `json:"user_id"`
	Symbol          string `json:"symbol"`
	PositionIdx     uint8  `json:"position_idx,omitempty"` // ADR-0077 leg
	Side            uint8  `json:"side"`
	Qty             string `json:"qty"`
	Price           string `json:"price"`
	PosSeq          uint64 `json:"pos_seq"`
	PositionVersion uint64 `json:"position_version"`
	AdlRound        uint64 `json:"adl_round"`
}

type ADLTaskResponse struct {
	Applied     bool   `json:"applied"`
	FactQty     string `json:"fact_qty,omitempty"`
	RealizedPnL string `json:"realized_pnl,omitempty"`
}

type ADLTaskResult struct {
	Applied     bool
	FactQty     dec.Decimal
	RealizedPnL dec.Decimal
}

type WorkingCapitalRepayRequest struct {
	RefID  string `json:"ref_id"`
	Amount string `json:"amount"`
}

type WorkingCapitalRepayResponse struct {
	Applied bool `json:"applied"`
}

func CandidateToWire(c ADLCandidate) ADLCandidateWire {
	return ADLCandidateWire{
		UserID: c.UserID, Symbol: c.Symbol, PositionIdx: c.PositionIdx, Side: uint8(c.Side),
		Size: c.Size.String(), Score: c.Score.String(),
		SacrificePerQty: c.SacrificePerQty.String(), PosSeq: c.PosSeq,
		PositionVersion: c.PositionVersion,
	}
}

func CandidateFromWire(w ADLCandidateWire) (ADLCandidate, error) {
	size, err := dec.Parse(w.Size)
	if err != nil {
		return ADLCandidate{}, fmt.Errorf("candidate size: %w", err)
	}
	score, err := dec.Parse(w.Score)
	if err != nil {
		return ADLCandidate{}, fmt.Errorf("candidate score: %w", err)
	}
	sacrifice, err := dec.Parse(w.SacrificePerQty)
	if err != nil {
		return ADLCandidate{}, fmt.Errorf("candidate sacrifice_per_qty: %w", err)
	}
	return ADLCandidate{
		UserID: w.UserID, Symbol: w.Symbol, PositionIdx: w.PositionIdx, Side: perpstate.Side(w.Side),
		Size: size, Score: score, SacrificePerQty: sacrifice,
		PosSeq: w.PosSeq, PositionVersion: w.PositionVersion,
	}, nil
}

func TaskToWire(t ADLTask) ADLTaskWire {
	return ADLTaskWire{
		LotID: t.LotID, UserID: t.UserID, Symbol: t.Symbol, PositionIdx: t.PositionIdx, Side: uint8(t.Side),
		Qty: t.Qty.String(), Price: t.Price.String(),
		PosSeq: t.PosSeq, PositionVersion: t.PositionVersion, AdlRound: t.AdlRound,
	}
}

func TaskFromWire(w ADLTaskWire) (ADLTask, error) {
	qty, err := dec.Parse(w.Qty)
	if err != nil {
		return ADLTask{}, fmt.Errorf("task qty: %w", err)
	}
	price, err := dec.Parse(w.Price)
	if err != nil {
		return ADLTask{}, fmt.Errorf("task price: %w", err)
	}
	return ADLTask{
		LotID: w.LotID, UserID: w.UserID, Symbol: w.Symbol, PositionIdx: w.PositionIdx, Side: perpstate.Side(w.Side),
		Qty: qty, Price: price, PosSeq: w.PosSeq,
		PositionVersion: w.PositionVersion, AdlRound: w.AdlRound,
	}, nil
}
