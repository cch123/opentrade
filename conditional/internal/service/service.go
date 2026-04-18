// Package service adapts engine.Engine into request/response types that
// the gRPC layer can forward verbatim. It's deliberately thin — the real
// logic lives in engine.
package service

import (
	"errors"

	condrpc "github.com/xargin/opentrade/api/gen/rpc/conditional"
	"github.com/xargin/opentrade/conditional/internal/engine"
)

// Service wraps the engine with gRPC-facing helpers.
type Service struct {
	eng *engine.Engine
}

// New returns a Service. eng must be non-nil.
func New(eng *engine.Engine) *Service {
	if eng == nil {
		panic("conditional/service: nil engine")
	}
	return &Service{eng: eng}
}

// Engine returns the underlying engine — handy for tests that need to
// drive market-data directly.
func (s *Service) Engine() *engine.Engine { return s.eng }

// Place forwards to engine.Place + builds the gRPC response.
func (s *Service) Place(req *condrpc.PlaceConditionalRequest, nowMs int64) (*condrpc.PlaceConditionalResponse, error) {
	id, status, accepted, err := s.eng.Place(req)
	if err != nil {
		return nil, err
	}
	return &condrpc.PlaceConditionalResponse{
		Id:               id,
		Status:           status,
		Accepted:         accepted,
		ReceivedTsUnixMs: nowMs,
	}, nil
}

// Cancel forwards to engine.Cancel + builds the gRPC response.
func (s *Service) Cancel(req *condrpc.CancelConditionalRequest) (*condrpc.CancelConditionalResponse, error) {
	status, accepted, err := s.eng.Cancel(req.UserId, req.Id)
	if err != nil {
		return nil, err
	}
	return &condrpc.CancelConditionalResponse{
		Id:       req.Id,
		Accepted: accepted,
		Status:   status,
	}, nil
}

// Query forwards to engine.Get. Returns ErrNotFound for missing or
// foreign-owned records (NotOwner is collapsed into NotFound to avoid
// leaking the existence of another user's id).
func (s *Service) Query(req *condrpc.QueryConditionalRequest) (*condrpc.QueryConditionalResponse, error) {
	c, err := s.eng.Get(req.UserId, req.Id)
	if err != nil {
		if errors.Is(err, engine.ErrNotOwner) {
			return nil, engine.ErrNotFound
		}
		return nil, err
	}
	return &condrpc.QueryConditionalResponse{Conditional: engine.ToProto(c)}, nil
}

// List forwards to engine.List + wraps in protos.
func (s *Service) List(req *condrpc.ListConditionalsRequest) (*condrpc.ListConditionalsResponse, error) {
	items := s.eng.List(req.UserId, req.IncludeInactive)
	out := make([]*condrpc.Conditional, 0, len(items))
	for _, c := range items {
		out = append(out, engine.ToProto(c))
	}
	return &condrpc.ListConditionalsResponse{Conditionals: out}, nil
}
