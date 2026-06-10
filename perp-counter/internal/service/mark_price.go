package service

// mark_price.go is the consume side of markprice → perp-counter (ADR-0068 §5).
// MarkTick updates the per-symbol mark the queries + liquidation judgement run
// off; FundingTick settles one funding round across every position in the
// symbol (ADR-0068 §7), each under the owning user's sequencer (invariant #1)
// so a user's funding stays totally ordered with their fills.

import (
	"strconv"
	"strings"

	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

// HandlePerpPriceEvent routes a decoded mark-price record. MarkTick sets the
// symbol mark (and, once wired, triggers the liquidation scan); FundingTick
// settles the round.
func (s *Service) HandlePerpPriceEvent(evt *eventpb.PerpPriceEvent) {
	if evt == nil {
		return
	}
	// Share the capture barrier with the trade path: funding / liquidation
	// mutate positions + the liquidation registry, which the snapshot reads.
	s.snapshotMu.RLock()
	defer s.snapshotMu.RUnlock()
	symbol := evt.GetSymbol()
	switch p := evt.Payload.(type) {
	case *eventpb.PerpPriceEvent_Tick:
		mark, err := dec.Parse(p.Tick.GetMarkPrice())
		if err != nil || mark.Sign() <= 0 {
			return
		}
		s.eng.SetMark(symbol, mark)
		// ADR-0069 freezes the index to a last-good value when quorum is lost.
		// That mark is still useful for display, but liquidation is irreversible
		// and must not be driven from a stale index.
		if !p.Tick.GetIndexStale() {
			s.onMarkTick(symbol)
		}
	case *eventpb.PerpPriceEvent_Funding:
		f := p.Funding
		rate, err := dec.Parse(f.GetFundingRate())
		if err != nil {
			return
		}
		roundID, ok := parseFundingRound(f.GetFundingRoundId())
		if !ok {
			return // can't guard idempotency without a round id — skip
		}
		s.settleFunding(symbol, f.GetFundingRoundId(), roundID, rate)
	}
}

// settleFunding fans the round out across every user holding a position in
// symbol, each under its own sequencer (invariant #1). The per-position
// funding_round_seen watermark (in SettleFundingUser) makes a redelivered tick
// a no-op.
func (s *Service) settleFunding(symbol, roundIDStr string, roundID int64, rate dec.Decimal) {
	for _, user := range s.eng.UsersWithPosition(symbol) {
		s.seq.do(user, func() {
			res, ok := s.eng.SettleFundingUser(user, symbol, roundID, rate)
			if !ok {
				return
			}
			s.emitFunding(symbol, roundIDStr, rate, res)
		})
	}
}

// emitFunding writes a PerpFundingEvent for one position's funding settlement.
// Caller holds the user's seq lock, so the position snapshot read here reflects
// the just-applied state.
func (s *Service) emitFunding(symbol, roundIDStr string, rate dec.Decimal, res engine.FundingResult) {
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_Funding{Funding: &eventpb.PerpFundingEvent{
			UserId: res.UserID, Symbol: symbol, FundingRoundId: roundIDStr,
			FundingRate: rate.String(), MarkPrice: s.eng.MarkOf(symbol).String(),
			Payment:       res.Payment.String(),
			PositionAfter: s.positionSnap(res.UserID, symbol),
		}},
	})
}

// onMarkTick is the per-mark-tick risk pass (ADR-0068 §8 / ADR-0074 §7):
// auto-add tops distressed isolated positions up FIRST, then the liquidation
// scan runs — its per-user sequencer re-check sees the topped-up state, so a
// saved position never gets a stale forced close. The execution flow
// (scan → cancel orders → bankruptcy order → insurance) is wired in
// liquidation.go.
func (s *Service) onMarkTick(symbol string) {
	s.runAutoAdd(symbol)
	s.scanLiquidations(symbol)
}

// parseFundingRound extracts the unix-seconds round id from a funding_round_id
// of the form "<symbol>:<seconds>" (mark_price.proto). ok=false on a malformed
// id (the settlement is skipped rather than guessing a watermark).
func parseFundingRound(id string) (int64, bool) {
	i := strings.LastIndex(id, ":")
	if i < 0 || i == len(id)-1 {
		return 0, false
	}
	v, err := strconv.ParseInt(id[i+1:], 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}
