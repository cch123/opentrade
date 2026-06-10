package perprisk

import (
	"errors"

	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpstate"
)

// LotStatus is the coordinator-owned lifecycle for a system-taken-over
// liquidation inventory lot (ADR-0073). String values keep JSON snapshots
// readable during incident review.
type LotStatus string

const (
	LotStatusInit            LotStatus = "Init"
	LotStatusUnwinding       LotStatus = "Unwinding"
	LotStatusAdlInProcessing LotStatus = "AdlInProcessing"
	LotStatusSettling        LotStatus = "Settling"
	LotStatusDone            LotStatus = "Done"
)

// TakenOverLot is the authoritative state for one liquidated inventory slice.
// Backstop/system-account positions may still be used for execution, but this
// lot is the only object that owns leaves_qty, settlement, and idempotency.
type TakenOverLot struct {
	LotID             string
	UserID            uint64
	Symbol            string
	PositionIdx       uint8 // ADR-0077: the liquidated leg (replay must not reduce the wrong leg)
	Side              perpstate.Side
	TotalQty          dec.Decimal
	LeavesQty         dec.Decimal
	TakeoverPrice     dec.Decimal
	TriggerMarkPrice  dec.Decimal
	TakenOverBalance  dec.Decimal
	PositionVersion   uint64
	Status            LotStatus
	RealizedPnL       dec.Decimal
	CumFee            dec.Decimal
	WorkingCapitalRef string
	WorkingCapital    dec.Decimal
	CreatedUnixMs     int64
	UpdatedUnixMs     int64
}

// RiskPoolSettlement is the final accounting result for a completed lot. It is
// intentionally separate from ADL fills: ADL only consumes inventory; this
// settlement is what changes the fund balance.
type RiskPoolSettlement struct {
	LotID               string
	Symbol              string
	Coin                string
	WorkingCapitalRef   string
	TakenOverBalance    dec.Decimal
	LiqAdlRealizedPnL   dec.Decimal
	CumFee              dec.Decimal
	WorkingCapitalDrawn dec.Decimal
	BorrowedBalance     dec.Decimal
	FinalPoolDelta      dec.Decimal
}

func NewTakenOverLot(l TakenOverLot) (TakenOverLot, error) {
	if l.LotID == "" {
		return TakenOverLot{}, errors.New("perprisk: lot_id required")
	}
	if l.UserID == 0 || l.Symbol == "" {
		return TakenOverLot{}, errors.New("perprisk: lot user_id and symbol required")
	}
	if l.TotalQty.Sign() <= 0 {
		return TakenOverLot{}, errors.New("perprisk: lot total_qty must be positive")
	}
	if l.TakeoverPrice.Sign() <= 0 {
		return TakenOverLot{}, errors.New("perprisk: lot takeover_price must be positive")
	}
	if l.LeavesQty.Sign() == 0 {
		l.LeavesQty = l.TotalQty
	}
	if l.LeavesQty.Sign() < 0 || l.LeavesQty.Cmp(l.TotalQty) > 0 {
		return TakenOverLot{}, errors.New("perprisk: lot leaves_qty out of range")
	}
	if l.Status == "" {
		l.Status = LotStatusInit
	}
	return l, nil
}

// WithWorkingCapital records the actual RiskPool draw. The amount may be less
// than requested because ADR-0073 inherits ADR-0071's global and per-symbol
// quota clamps; the lot must preserve the fact, not the request.
func (l TakenOverLot) WithWorkingCapital(ref string, amount dec.Decimal) TakenOverLot {
	l.WorkingCapitalRef = ref
	l.WorkingCapital = amount
	if l.Status == LotStatusInit {
		l.Status = LotStatusUnwinding
	}
	return l
}

// ApplyLotFill reduces leaves_qty by factQty. It accepts only fact quantities
// because dispatched order/ADL requests are not authoritative inventory changes.
func ApplyLotFill(l TakenOverLot, factQty, realizedPnL, fee dec.Decimal, status LotStatus) (TakenOverLot, error) {
	if l.LotID == "" {
		return TakenOverLot{}, errors.New("perprisk: lot_id required")
	}
	if l.Status == LotStatusDone {
		return TakenOverLot{}, errors.New("perprisk: lot already done")
	}
	if factQty.Sign() <= 0 {
		return l, nil
	}
	if factQty.Cmp(l.LeavesQty) > 0 {
		return TakenOverLot{}, errors.New("perprisk: lot fill exceeds leaves_qty")
	}
	l.LeavesQty = l.LeavesQty.Sub(factQty)
	l.RealizedPnL = l.RealizedPnL.Add(realizedPnL)
	l.CumFee = l.CumFee.Add(fee)
	if l.LeavesQty.Sign() == 0 {
		l.Status = LotStatusSettling
	} else if status != "" {
		l.Status = status
	}
	return l, nil
}

func ApplyLotADL(l TakenOverLot, factQty, realizedPnL dec.Decimal) (TakenOverLot, error) {
	return ApplyLotFill(l, factQty, realizedPnL, zero, LotStatusAdlInProcessing)
}

// SettleTakenOverLot calculates the only authoritative RiskPool fund movement
// for a lot. Calling it on a Done lot returns ok=false so replay cannot credit
// or debit the fund twice.
func SettleTakenOverLot(l TakenOverLot, coin string) (TakenOverLot, RiskPoolSettlement, bool, error) {
	if l.LotID == "" {
		return TakenOverLot{}, RiskPoolSettlement{}, false, errors.New("perprisk: lot_id required")
	}
	if l.Status == LotStatusDone {
		return l, RiskPoolSettlement{}, false, nil
	}
	if l.LeavesQty.Sign() != 0 {
		return TakenOverLot{}, RiskPoolSettlement{}, false, errors.New("perprisk: cannot settle lot with leaves_qty")
	}
	if coin == "" {
		coin = coinFromLinearPerp(l.Symbol)
	}
	netRecovery := l.TakenOverBalance.Add(l.RealizedPnL).Sub(l.CumFee)
	borrowed := l.WorkingCapital.Sub(netRecovery)
	settlement := RiskPoolSettlement{
		LotID: l.LotID, Symbol: l.Symbol, Coin: coin,
		WorkingCapitalRef: l.WorkingCapitalRef,
		TakenOverBalance:  l.TakenOverBalance, LiqAdlRealizedPnL: l.RealizedPnL,
		CumFee: l.CumFee, WorkingCapitalDrawn: l.WorkingCapital,
		BorrowedBalance: borrowed, FinalPoolDelta: borrowed.Neg(),
	}
	l.Status = LotStatusDone
	return l, settlement, true, nil
}
