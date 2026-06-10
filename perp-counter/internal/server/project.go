package server

// project.go is the ADR-0075 §3 dry-run endpoint: admin-gateway submits the
// CANDIDATE risk tier table it wants to publish as IMMEDIATE; the shard
// reports how many of its accounts would see a higher maintenance
// requirement and how many would breach maintenance outright.

import (
	"context"
	"errors"
	"fmt"

	"connectrpc.com/connect"

	perprpc "github.com/xargin/opentrade/api/gen/rpc/perp"
	"github.com/xargin/opentrade/pkg/dec"
	"github.com/xargin/opentrade/pkg/perpcfg"
)

func (s *Server) ProjectRiskConfig(_ context.Context, req *connect.Request[perprpc.ProjectRiskConfigRequest]) (*connect.Response[perprpc.ProjectRiskConfigResponse], error) {
	if req.Msg.GetSymbol() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("symbol required"))
	}
	tiers, err := tiersFromParams(req.Msg.GetRiskTiers())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := perpcfg.ValidateRiskTiers(tiers); err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	candidate := (&perpcfg.PerpSymbolConfig{RiskTiers: tiers}).RiskModel()
	proj := s.eng.ProjectRiskTiers(req.Msg.GetSymbol(), candidate)
	return connect.NewResponse(&perprpc.ProjectRiskConfigResponse{
		AffectedAccounts:     proj.Affected,
		LiquidatableAccounts: proj.Liquidatable,
		PositionsScanned:     proj.Scanned,
	}), nil
}

func tiersFromParams(in []*perprpc.RiskTierParam) ([]perpcfg.RiskTier, error) {
	out := make([]perpcfg.RiskTier, 0, len(in))
	for i, t := range in {
		parse := func(name, v string) (dec.Decimal, error) {
			d, err := dec.Parse(v)
			if err != nil {
				return d, fmt.Errorf("risk_tiers[%d].%s: %w", i, name, err)
			}
			return d, nil
		}
		maxN, err := parse("max_notional", t.GetMaxNotional())
		if err != nil {
			return nil, err
		}
		mmr, err := parse("maintenance_margin_ratio", t.GetMaintenanceMarginRatio())
		if err != nil {
			return nil, err
		}
		lev, err := parse("max_leverage", t.GetMaxLeverage())
		if err != nil {
			return nil, err
		}
		fee, err := parse("liq_fee_rate", t.GetLiqFeeRate())
		if err != nil {
			return nil, err
		}
		out = append(out, perpcfg.RiskTier{
			RiskID: t.GetRiskId(), MaxNotional: maxN,
			MaintMarginRatio: mmr, MaxLeverage: lev, LiqFeeRate: fee,
		})
	}
	return out, nil
}
