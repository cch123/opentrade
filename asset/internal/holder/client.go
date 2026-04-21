package holder

import "context"

// Client is the saga driver's contract for one biz_line. TransferOut /
// TransferIn / CompensateTransferOut follow the AssetHolder proto 1:1;
// implementations MUST be idempotent on Request.TransferID.
//
// A nil *Result with nil error is not allowed. Return a rejected Result
// (Status=Rejected, Reason=Internal) instead.
type Client interface {
	TransferOut(ctx context.Context, req Request) (Result, error)
	TransferIn(ctx context.Context, req Request) (Result, error)
	CompensateTransferOut(ctx context.Context, req Request) (Result, error)
}
