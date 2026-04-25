// Package service adapts engine.Engine into request/response types that
// the gRPC layer can forward verbatim. It's deliberately thin — the real
// logic lives in engine.
package service

import (
	"context"
	"errors"

	condrpc "github.com/xargin/opentrade/api/gen/rpc/trigger"
	"github.com/xargin/opentrade/trigger/engine"
)

// Service wraps the engine with gRPC-facing helpers.
type Service struct {
	eng *engine.Engine
}

// New returns a Service. eng must be non-nil.
func New(eng *engine.Engine) *Service {
	if eng == nil {
		panic("trigger/service: nil engine")
	}
	return &Service{eng: eng}
}

// Engine returns the underlying engine — handy for tests that need to
// drive market-data directly.
func (s *Service) Engine() *engine.Engine { return s.eng }

// Place forwards to engine.Place + builds the gRPC response.
func (s *Service) Place(ctx context.Context, req *condrpc.PlaceTriggerRequest, nowMs int64) (*condrpc.PlaceTriggerResponse, error) {
	id, status, accepted, err := s.eng.Place(ctx, req)
	if err != nil {
		return nil, err
	}
	return &condrpc.PlaceTriggerResponse{
		Id:               id,
		Status:           status,
		Accepted:         accepted,
		ReceivedTsUnixMs: nowMs,
	}, nil
}

// Cancel forwards to engine.Cancel + builds the gRPC response.
func (s *Service) Cancel(ctx context.Context, req *condrpc.CancelTriggerRequest) (*condrpc.CancelTriggerResponse, error) {
	status, accepted, err := s.eng.Cancel(ctx, req.UserId, req.Id)
	if err != nil {
		return nil, err
	}
	return &condrpc.CancelTriggerResponse{
		Id:       req.Id,
		Accepted: accepted,
		Status:   status,
	}, nil
}

// Query forwards to engine.Get. Returns ErrNotFound for missing or
// foreign-owned records (NotOwner is collapsed into NotFound to avoid
// leaking the existence of another user's id).
func (s *Service) Query(req *condrpc.QueryTriggerRequest) (*condrpc.QueryTriggerResponse, error) {
	c, err := s.eng.Get(req.UserId, req.Id)
	if err != nil {
		if errors.Is(err, engine.ErrNotOwner) {
			return nil, engine.ErrNotFound
		}
		return nil, err
	}
	return &condrpc.QueryTriggerResponse{Trigger: engine.ToProto(c)}, nil
}

// List forwards to engine.List + wraps in protos.
func (s *Service) List(req *condrpc.ListTriggersRequest) (*condrpc.ListTriggersResponse, error) {
	items := s.eng.List(req.UserId, req.IncludeInactive)
	out := make([]*condrpc.Trigger, 0, len(items))
	for _, c := range items {
		out = append(out, engine.ToProto(c))
	}
	return &condrpc.ListTriggersResponse{Triggers: out}, nil
}

// PlaceOCO forwards to engine.PlaceOCO + builds the gRPC response.
func (s *Service) PlaceOCO(ctx context.Context, req *condrpc.PlaceOCORequest, nowMs int64) (*condrpc.PlaceOCOResponse, error) {
	gid, legs, accepted, err := s.eng.PlaceOCO(ctx, req.UserId, req.ClientOcoId, req.Legs)
	if err != nil {
		return nil, err
	}
	legResps := make([]*condrpc.PlaceTriggerResponse, len(legs))
	for i, lr := range legs {
		legResps[i] = &condrpc.PlaceTriggerResponse{
			Id:               lr.ID,
			Status:           lr.Status,
			Accepted:         accepted,
			ReceivedTsUnixMs: nowMs,
		}
	}
	return &condrpc.PlaceOCOResponse{
		OcoGroupId:       gid,
		Legs:             legResps,
		Accepted:         accepted,
		ReceivedTsUnixMs: nowMs,
	}, nil
}
