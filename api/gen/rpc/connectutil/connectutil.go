package connectutil

import (
	"errors"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CloserFunc adapts a function into an io.Closer-style resource hook.
type CloserFunc func() error

// Close executes the wrapped cleanup function.
func (f CloserFunc) Close() error { return f() }

// NewHTTPClient returns a dedicated HTTP client and a closer that releases
// any idle connections held by its transport.
func NewHTTPClient() (*http.Client, CloserFunc) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	return &http.Client{Transport: transport}, CloserFunc(func() error {
		transport.CloseIdleConnections()
		return nil
	})
}

// NormalizeBaseURL accepts host:port or full URLs and returns a base URL
// suitable for connect-go procedure paths.
func NormalizeBaseURL(baseURL string) string {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return ""
	}
	if strings.Contains(baseURL, "://") {
		return baseURL
	}
	return "http://" + baseURL
}

// ProcedureURL joins a base URL with a fully-qualified RPC procedure path.
func ProcedureURL(baseURL, procedure string) string {
	return NormalizeBaseURL(baseURL) + procedure
}

// AsGRPCError maps connect-go transport errors back to gRPC status errors so
// existing callers can keep using status.Code/codes.* helpers unchanged.
func AsGRPCError(err error) error {
	if err == nil {
		return nil
	}
	var connectErr *connect.Error
	if !errors.As(err, &connectErr) {
		return err
	}
	return status.Error(toGRPCCode(connectErr.Code()), connectErr.Message())
}

// AsConnectError maps gRPC status errors into connect-go errors so server-side
// handlers preserve canonical RPC codes over the wire.
func AsConnectError(err error) error {
	if err == nil {
		return nil
	}
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		return err
	}
	return connect.NewError(toConnectCode(status.Code(err)), errors.New(status.Convert(err).Message()))
}

// ResponseMsg unwraps a connect-go unary response and translates transport
// errors into gRPC status errors for compatibility with existing callers.
func ResponseMsg[T any](resp *connect.Response[T], err error) (*T, error) {
	if err != nil {
		return nil, AsGRPCError(err)
	}
	return resp.Msg, nil
}

func toGRPCCode(code connect.Code) codes.Code {
	switch code {
	case connect.CodeCanceled:
		return codes.Canceled
	case connect.CodeUnknown:
		return codes.Unknown
	case connect.CodeInvalidArgument:
		return codes.InvalidArgument
	case connect.CodeDeadlineExceeded:
		return codes.DeadlineExceeded
	case connect.CodeNotFound:
		return codes.NotFound
	case connect.CodeAlreadyExists:
		return codes.AlreadyExists
	case connect.CodePermissionDenied:
		return codes.PermissionDenied
	case connect.CodeResourceExhausted:
		return codes.ResourceExhausted
	case connect.CodeFailedPrecondition:
		return codes.FailedPrecondition
	case connect.CodeAborted:
		return codes.Aborted
	case connect.CodeOutOfRange:
		return codes.OutOfRange
	case connect.CodeUnimplemented:
		return codes.Unimplemented
	case connect.CodeInternal:
		return codes.Internal
	case connect.CodeUnavailable:
		return codes.Unavailable
	case connect.CodeDataLoss:
		return codes.DataLoss
	case connect.CodeUnauthenticated:
		return codes.Unauthenticated
	default:
		return codes.Unknown
	}
}

func toConnectCode(code codes.Code) connect.Code {
	switch code {
	case codes.Canceled:
		return connect.CodeCanceled
	case codes.Unknown:
		return connect.CodeUnknown
	case codes.InvalidArgument:
		return connect.CodeInvalidArgument
	case codes.DeadlineExceeded:
		return connect.CodeDeadlineExceeded
	case codes.NotFound:
		return connect.CodeNotFound
	case codes.AlreadyExists:
		return connect.CodeAlreadyExists
	case codes.PermissionDenied:
		return connect.CodePermissionDenied
	case codes.ResourceExhausted:
		return connect.CodeResourceExhausted
	case codes.FailedPrecondition:
		return connect.CodeFailedPrecondition
	case codes.Aborted:
		return connect.CodeAborted
	case codes.OutOfRange:
		return connect.CodeOutOfRange
	case codes.Unimplemented:
		return connect.CodeUnimplemented
	case codes.Internal:
		return connect.CodeInternal
	case codes.Unavailable:
		return connect.CodeUnavailable
	case codes.DataLoss:
		return connect.CodeDataLoss
	case codes.Unauthenticated:
		return connect.CodeUnauthenticated
	default:
		return connect.CodeUnknown
	}
}
