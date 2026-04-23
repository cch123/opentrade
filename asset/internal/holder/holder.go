// Package holder abstracts "remote AssetHolder" so the saga orchestrator
// (ADR-0057 §4) doesn't care whether a biz_line is backed by an in-process
// service (funding) or a gRPC dial-out (spot / futures / ...).
//
// The interface is deliberately thin: three methods mirror the proto RPCs
// 1:1 and carry only string/decimal payloads. All translation from proto
// enums happens at the gRPC client boundary so internal code sees a
// uniform enum.
package holder

// Status is the unified outcome enum used by every HolderClient. Values
// mirror both api/rpc/assetholder.TransferStatus and
// asset/internal/service.Status so translation is trivial in either
// direction.
type Status int

const (
	StatusUnspecified Status = 0
	StatusConfirmed   Status = 1
	StatusRejected    Status = 2
	StatusDuplicated  Status = 3
)

// RejectReason mirrors api/rpc/assetholder.RejectReason. Saga driver
// persists the string form of this into transfer_ledger.reject_reason
// for observability; the specific enum value is rarely consulted.
type RejectReason int

const (
	ReasonUnspecified         RejectReason = 0
	ReasonInsufficientBalance RejectReason = 1
	ReasonUnknownUser         RejectReason = 2
	ReasonUnknownAsset        RejectReason = 3
	ReasonAssetFrozen         RejectReason = 4
	ReasonAmountInvalid       RejectReason = 5
	ReasonInternal            RejectReason = 99
)

// String returns a stable wire name for the reason, used in
// transfer_ledger.reject_reason.
func (r RejectReason) String() string {
	switch r {
	case ReasonUnspecified:
		return ""
	case ReasonInsufficientBalance:
		return "insufficient_balance"
	case ReasonUnknownUser:
		return "unknown_user"
	case ReasonUnknownAsset:
		return "unknown_asset"
	case ReasonAssetFrozen:
		return "asset_frozen"
	case ReasonAmountInvalid:
		return "amount_invalid"
	case ReasonInternal:
		return "internal"
	}
	return "unknown"
}

// Request is the uniform input shape for any of the three methods.
type Request struct {
	UserID          string
	TransferID      string
	Asset           string
	Amount          string // decimal string; preserved verbatim across leg boundaries
	PeerBiz         string
	Memo            string
	CompensateCause string // only used by CompensateTransferOut
}

// Result is what the saga driver consumes. Transport errors (unreachable
// peer, timeouts) surface through the separate error return; business
// rejections ride Status/Reason.
type Result struct {
	Status       Status
	Reason       RejectReason
	ReasonDetail string // optional free-form diagnostic (e.g. wrapped server msg)
}
