package cursor

import "testing"

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []any{
		OrdersCursor{CreatedAt: 1712345678901, OrderID: 42},
		TradesCursor{Ts: 1712345678901, TradeID: "sym-0-17"},
		AccountLogsCursor{Ts: 1712345678901, ShardID: 3, SeqID: 99, Asset: "USDT"},
	}
	for _, c := range cases {
		s, err := Encode(c)
		if err != nil {
			t.Fatalf("encode %v: %v", c, err)
		}
		if s == "" {
			t.Fatalf("encoded empty for %v", c)
		}
		switch original := c.(type) {
		case OrdersCursor:
			var got OrdersCursor
			if err := Decode(s, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got != original {
				t.Fatalf("round-trip: got %+v want %+v", got, original)
			}
		case TradesCursor:
			var got TradesCursor
			if err := Decode(s, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got != original {
				t.Fatalf("round-trip: got %+v want %+v", got, original)
			}
		case AccountLogsCursor:
			var got AccountLogsCursor
			if err := Decode(s, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if got != original {
				t.Fatalf("round-trip: got %+v want %+v", got, original)
			}
		}
	}
}

func TestDecodeEmpty(t *testing.T) {
	var c OrdersCursor
	if err := Decode("", &c); err != nil {
		t.Fatalf("empty should not error: %v", err)
	}
	if (c != OrdersCursor{}) {
		t.Fatalf("empty should leave dst zero, got %+v", c)
	}
}

func TestDecodeInvalid(t *testing.T) {
	var c OrdersCursor
	// Non-base64 garbage.
	if err := Decode("!!!", &c); err != ErrInvalid {
		t.Fatalf("bad base64 err = %v, want ErrInvalid", err)
	}
	// Valid base64 but not JSON.
	bad := "aGVsbG8" // base64("hello") without padding
	if err := Decode(bad, &c); err != ErrInvalid {
		t.Fatalf("bad json err = %v, want ErrInvalid", err)
	}
}
