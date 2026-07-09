package service

// transfer.go is perp-counter's side of the asset-service saga (ADR-0057): the
// futures wallet is a biz_line=futures AssetHolder. funding→futures deposits
// (TransferIn) and futures→funding withdrawals (TransferOut) land here as saga
// legs. Every transfer runs under the user's sequencer (invariant #1) and the
// snapshot barrier; the engine dedups on (operation, transfer_id) (idempotent
// per the AssetHolder contract) and a confirmed move emits a PerpMarginEvent.

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
func (s *Service) FuturesTransferIn(user uint64, transferID, asset string, amt dec.Decimal) TransferResult {
	return s.futuresTransfer(user, transferID, asset, amt, (*engine.Engine).TransferIn, eventpb.PerpMarginEvent_KIND_TRANSFER_IN)
}

// FuturesTransferOut debits free margin (futures→funding withdrawal).
func (s *Service) FuturesTransferOut(user uint64, transferID, asset string, amt dec.Decimal) TransferResult {
	return s.futuresTransfer(user, transferID, asset, amt, (*engine.Engine).TransferOut, eventpb.PerpMarginEvent_KIND_TRANSFER_OUT)
}

// FuturesCompensateTransferOut reverses a confirmed TransferOut by crediting the
// amount back. The AssetHolder contract requires the original transfer_id;
// operation-aware engine keys keep the debit and its compensating credit in
// separate idempotency spaces.
func (s *Service) FuturesCompensateTransferOut(user uint64, transferID, asset string, amt dec.Decimal) TransferResult {
	return s.futuresTransfer(user, transferID, asset, amt, (*engine.Engine).CompensateTransferOut, eventpb.PerpMarginEvent_KIND_TRANSFER_IN)
}

type transferFunc func(*engine.Engine, uint64, string, dec.Decimal) (engine.TransferOutcome, bool)

func (s *Service) futuresTransfer(user uint64, transferID, asset string, amt dec.Decimal, transfer transferFunc, kind eventpb.PerpMarginEvent_Kind) TransferResult {
	s.snapshotMu.RLock()
	defer s.snapshotMu.RUnlock()
	var res TransferResult
	s.seq.do(user, func() {
		out, dup := transfer(s.eng, user, transferID, amt)
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
func (s *Service) emitMargin(user uint64, kind eventpb.PerpMarginEvent_Kind, asset string, amt dec.Decimal, out engine.TransferOutcome, refID string) {
	s.journal.Emit(&eventpb.PerpJournalEvent{
		Meta: s.meta(), PerpSeqId: s.nextPerpSeq(),
		Payload: &eventpb.PerpJournalEvent_Margin{Margin: &eventpb.PerpMarginEvent{
			UserId: user, Kind: kind, Asset: asset, Amount: amt.String(),
			AvailableAfter: out.AvailableAfter.String(), ReservedAfter: out.ReservedAfter.String(),
			RefId: refID,
		}},
	})
}
