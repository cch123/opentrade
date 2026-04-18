package engine

import (
	condrpc "github.com/xargin/opentrade/api/gen/rpc/conditional"
	"github.com/xargin/opentrade/pkg/dec"
)

// ToProto projects a Conditional into its wire form for gRPC responses.
// Zero-valued LimitPrice / Qty / QuoteQty are omitted (empty string) so
// consumers can distinguish "field not used" from "set to zero".
func ToProto(c *Conditional) *condrpc.Conditional {
	if c == nil {
		return nil
	}
	return &condrpc.Conditional{
		Id:                  c.ID,
		ClientConditionalId: c.ClientCondID,
		UserId:              c.UserID,
		Symbol:              c.Symbol,
		Side:                c.Side,
		Type:                c.Type,
		StopPrice:           c.StopPrice.String(),
		LimitPrice:          decOrEmpty(c.LimitPrice),
		Qty:                 decOrEmpty(c.Qty),
		QuoteQty:            decOrEmpty(c.QuoteQty),
		Tif:                 c.TIF,
		Status:              c.Status,
		CreatedAtUnixMs:     c.CreatedAtMs,
		TriggeredAtUnixMs:   c.TriggeredAtMs,
		PlacedOrderId:       c.PlacedOrderID,
		RejectReason:        c.RejectReason,
		ExpiresAtUnixMs:     c.ExpiresAtMs,
	}
}

func decOrEmpty(d dec.Decimal) string {
	if dec.IsZero(d) {
		return ""
	}
	return d.String()
}
