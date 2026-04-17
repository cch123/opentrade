// Package dec is a thin convenience wrapper around shopspring/decimal for
// monetary values (prices, quantities, balances).
//
// All monetary math MUST use dec.Decimal — never float32/float64 and never
// bare int64 without explicit scale (see ADR-0013).
package dec

import (
	"fmt"

	"github.com/shopspring/decimal"
)

// Decimal is an alias for shopspring/decimal.Decimal for brevity.
type Decimal = decimal.Decimal

// Zero is the zero value and is safe to use as default.
var Zero = decimal.Zero

// New constructs a Decimal from a string; panics on invalid input. Prefer
// Parse in user-facing code paths.
func New(s string) Decimal {
	d, err := decimal.NewFromString(s)
	if err != nil {
		panic(fmt.Sprintf("dec.New: %q is not a valid decimal: %v", s, err))
	}
	return d
}

// Parse constructs a Decimal from a string, returning an error for invalid input.
func Parse(s string) (Decimal, error) {
	if s == "" {
		return Zero, nil
	}
	return decimal.NewFromString(s)
}

// MustParse panics on invalid input. For use in tests / constants.
func MustParse(s string) Decimal { return New(s) }

// FromInt returns a Decimal equivalent to the given integer.
func FromInt(i int64) Decimal { return decimal.NewFromInt(i) }

// Equal returns true if a and b have the same numeric value (ignores
// representation).
func Equal(a, b Decimal) bool { return a.Cmp(b) == 0 }

// IsNegative reports whether d < 0.
func IsNegative(d Decimal) bool { return d.Sign() < 0 }

// IsPositive reports whether d > 0.
func IsPositive(d Decimal) bool { return d.Sign() > 0 }

// IsZero reports whether d == 0.
func IsZero(d Decimal) bool { return d.Sign() == 0 }
