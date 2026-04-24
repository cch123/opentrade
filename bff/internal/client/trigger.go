package client

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
)

// Trigger is the narrow surface BFF needs from the trigger service.
// Empty implementations (nil) in tests substitute a fake.
type Trigger interface {
	PlaceTrigger(ctx context.Context, in *condrpc.PlaceTriggerRequest, opts ...grpc.CallOption) (*condrpc.PlaceTriggerResponse, error)
	CancelTrigger(ctx context.Context, in *condrpc.CancelTriggerRequest, opts ...grpc.CallOption) (*condrpc.CancelTriggerResponse, error)
	QueryTrigger(ctx context.Context, in *condrpc.QueryTriggerRequest, opts ...grpc.CallOption) (*condrpc.QueryTriggerResponse, error)
	ListTriggers(ctx context.Context, in *condrpc.ListTriggersRequest, opts ...grpc.CallOption) (*condrpc.ListTriggersResponse, error)
	PlaceOCO(ctx context.Context, in *condrpc.PlaceOCORequest, opts ...grpc.CallOption) (*condrpc.PlaceOCOResponse, error)
}

// DialTrigger opens a plaintext gRPC connection to the trigger
// service. mTLS / auth land with the broader credentials work later.
func DialTrigger(_ context.Context, endpoint string) (*grpc.ClientConn, Trigger, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("grpc Dial %s: %w", endpoint, err)
	}
	return conn, condrpc.NewTriggerServiceClient(conn), nil
}
