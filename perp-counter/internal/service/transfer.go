package service

// transfer.go is perp-counter's side of the asset-service saga (ADR-0057): the
// futures wallet is a biz_line=futures AssetHolder. funding→futures deposits
// (TransferIn) and futures→funding withdrawals (TransferOut) land here as saga
// legs. Every transfer runs under the user's sequencer (invariant #1) and the
// snapshot barrier; the engine dedups on transfer_id (idempotent per the
// AssetHolder contract) and a confirmed move emits a PerpMarginEvent.

import (
	eventpb "github.com/xargin/opentrade/api/gen/event"
	"github.com/xargin/opentrade/perp-counter/internal/engine"
	"github.com/xargin/opentrade/pkg/dec"
)

// TransferStatus mirrors the AssetHolder TransferStatus enum.
type TransferStatus uint8

const (
	// TransferConfirmed: the balance moved.
	TransferConfirmed TransferStatus = iota + 1
	// TransferRejected: business reject (insufficient balance). Terminal.
	TransferRejected
	// TransferDuplicated: idempotency hit; fields mirror the first execution.
	TransferDuplicated
)

// TransferResult is the outcome the AssetHolder handler maps to the wire.
type TransferResult struct {
	Status         TransferStatus
	AvailableAfter dec.Decimal
	ReservedAfter  dec.Decimal
	RejectReason   string
}

// FuturesTransferIn credits the futures wallet (funding→futures deposit).
func (s *Service) FuturesTransferIn(user, transferID, asset string, amt dec.Decimal) TransferResult {
	return s.futuresTransfer(user, transferID, asset, amt, true, eventpb.PerpMarginEvent_KIND_TRANSFER_IN)
}

// FuturesTransferOut debits free margin (futures→funding withdrawal).
func (s *Service) FuturesTransferOut(user, transferID, asset string, amt dec.Decimal) TransferResult {
	return s.futuresTransfer(user, transferID, asset, amt, false, eventpb.PerpMarginEvent_KIND_TRANSFER_OUT)
}

// FuturesCompensateTransferOut reverses a confirmed TransferOut by crediting the
// amount back. asset-service uses a distinct transfer_id for the compensate leg
// (the holder just dedups on whatever id it receives, like counter).
func (s *Service) FuturesCompensateTransferOut(user, transferID, asset string, amt dec.Decimal) TransferResult {
	return s.futuresTransfer(user, transferID, asset, amt, true, eventpb.PerpMarginEvent_KIND_TRANSFER_IN)
}

func (s *Service) futuresTransfer(user, transferID, asset string, amt dec.Decimal, in bool, kind eventpb.PerpMarginEvent_Kind) TransferResult {
	s.snapshotMu.RLock()
	defer s.snapshotMu.RUnlock()
	var res TransferResult
	s.seq.do(user, func() {
		var (
			out engine.TransferOutcome
			dup bool
		)
		if in {
			out, dup = s.eng.TransferIn(user, transferID, amt)
		} else {
			out, dup = s.eng.TransferOut(user, transferID, amt)
		}
		res = TransferResult{
			AvailableAfter: out.AvailableAfter, ReservedAfter: out.ReservedAfter,
			RejectReason: out.RejectReason,
		}
		switch {
		case dup:
			res.Status = TransferDuplicated
		case out.Status == engine.TransferConfirmed:
			res.Status = TransferConfirmed
			s.emitMargin(user, kind, asset, amt, out, transferID)
		default:
			res.Status = TransferRejected
		}
	})
	return res
}

// emitMargin journals a confirmed futures-wallet balance change (ADR-0068 §2).
func (s *Service) emitMargin(user string, kind eventpb.PerpMarginEvent_Kind, asset string, amt dec.Decimal, out engine.TransferOutcome, refID string) {
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_Margin{Margin: &eventpb.PerpMarginEvent{
			UserId: user, Kind: kind, Asset: asset, Amount: amt.String(),
			AvailableAfter: out.AvailableAfter.String(), ReservedAfter: out.ReservedAfter.String(),
			RefId: refID,
		}},
	})
}
