package client

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	condrpc "github.com/xargin/opentrade/api/gen/rpc/conditional"
)

// Conditional is the narrow surface BFF needs from the conditional service.
// Empty implementations (nil) in tests substitute a fake.
type Conditional interface {
	PlaceConditional(ctx context.Context, in *condrpc.PlaceConditionalRequest, opts ...grpc.CallOption) (*condrpc.PlaceConditionalResponse, error)
	CancelConditional(ctx context.Context, in *condrpc.CancelConditionalRequest, opts ...grpc.CallOption) (*condrpc.CancelConditionalResponse, error)
	QueryConditional(ctx context.Context, in *condrpc.QueryConditionalRequest, opts ...grpc.CallOption) (*condrpc.QueryConditionalResponse, error)
	ListConditionals(ctx context.Context, in *condrpc.ListConditionalsRequest, opts ...grpc.CallOption) (*condrpc.ListConditionalsResponse, error)
}

// DialConditional opens a plaintext gRPC connection to the conditional
// service. mTLS / auth land with the broader credentials work later.
func DialConditional(_ context.Context, endpoint string) (*grpc.ClientConn, Conditional, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("grpc Dial %s: %w", endpoint, err)
	}
	return conn, condrpc.NewConditionalServiceClient(conn), nil
}
