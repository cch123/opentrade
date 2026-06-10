package engine

// project.go is the ADR-0075 §3 risk_reprice_policy dry-run: evaluate every
// position in a symbol under a CANDIDATE risk model (the tier table an admin
// wants to publish as IMMEDIATE) and report the blast radius before the
// publish is allowed through.

import (
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// RiskProjection is the dry-run outcome for one symbol on this shard.
type RiskProjection struct {
	// Affected counts accounts whose maintenance requirement would INCREASE
	// under the candidate table (the set the reprice policy must budget
	// for via max_affected_accounts).
	Affected uint64
	// Liquidatable counts accounts whose position (isolated) or account
	// pool (cross) would breach maintenance outright at current marks —
	// the would-be batch liquidation set.
	Liquidatable uint64
	// Scanned counts accounts holding any non-flat position in symbol.
	Scanned uint64
}

// ProjectRiskTiers runs the dry-run under the engine read lock. The candidate
// model replaces the symbol's model for every evaluation; other symbols in a
// cross pool keep their live resolution.
func (e *Engine) ProjectRiskTiers(symbol string, candidate perpstate.RiskModel) RiskProjection {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var out RiskProjection
	candStd := perpstate.StandardRisk{
		Model: e.risk,
		ModelFor: func(p *perpstate.Position) perpstate.RiskModel {
			if p.Symbol == symbol {
				return candidate
			}
			return e.crossModelForLocked(p)
		},
	}
	for user, bySym := range e.positions {
		p := bySym[symbol]
		if p == nil || p.IsFlat() {
			continue
		}
		out.Scanned++
		mark := e.marks[p.Symbol]
		notional := p.Notional(mark)
		curModel, _ := e.riskModelForPositionLocked(p)
		curMMR := curModel.EffectiveMMR(notional, p.RiskID)
		candMMR := candidate.EffectiveMMR(notional, p.RiskID)
		if candMMR.Cmp(curMMR) > 0 {
			out.Affected++
		}
		if mark.Sign() <= 0 {
			continue // no mark → no health verdict; counted as scanned only
		}
		if p.Mode == perpstate.MarginCross {
			w := e.wallets[user]
			if w == nil {
				w = &Wallet{Available: zero, Reserved: zero, CrossReserved: zero}
			}
			h := candStd.Eval(perpstate.Cross(w.Available.Add(w.CrossReserved), e.crossPositionsLocked(user)), e.marks)
			if h.Liquidatable() {
				out.Liquidatable++
			}
			continue
		}
		marks := map[string]dec.Decimal{symbol: mark}
		if perpstate.Isolated(p).Liquidatable(marks, candidate.EffectiveMMRFunc(p.RiskID)) {
			out.Liquidatable++
		}
	}
	return out
}
