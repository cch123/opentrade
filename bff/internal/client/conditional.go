package client

import (
	"context"
	"io"

	"google.golang.org/grpc"

	condrpc "github.com/xargin/opentrade/api/gen/rpc/conditional"
	"github.com/xargin/opentrade/api/gen/rpc/connectutil"
)

// Conditional is the narrow surface BFF needs from the conditional service.
// Empty implementations (nil) in tests substitute a fake.
type Conditional interface {
	PlaceConditional(ctx context.Context, in *condrpc.PlaceConditionalRequest, opts ...grpc.CallOption) (*condrpc.PlaceConditionalResponse, error)
	CancelConditional(ctx context.Context, in *condrpc.CancelConditionalRequest, opts ...grpc.CallOption) (*condrpc.CancelConditionalResponse, error)
	QueryConditional(ctx context.Context, in *condrpc.QueryConditionalRequest, opts ...grpc.CallOption) (*condrpc.QueryConditionalResponse, error)
	ListConditionals(ctx context.Context, in *condrpc.ListConditionalsRequest, opts ...grpc.CallOption) (*condrpc.ListConditionalsResponse, error)
	PlaceOCO(ctx context.Context, in *condrpc.PlaceOCORequest, opts ...grpc.CallOption) (*condrpc.PlaceOCOResponse, error)
}

// DialConditional opens a plaintext RPC transport to the conditional
// service. mTLS / auth land with the broader credentials work later.
func DialConditional(_ context.Context, endpoint string) (io.Closer, Conditional, error) {
	httpClient, closer := connectutil.NewHTTPClient()
	return closer, condrpc.NewConditionalServiceConnectClient(httpClient, endpoint), nil
}
