package dec

import "testing"

func TestParse(t *testing.T) {
	cases := []struct {
		in      string
		wantStr string
		err     bool
	}{
		{"", "0", false},
		{"1", "1", false},
		{"1.5", "1.5", false},
		{"0.00000001", "0.00000001", false},
		{"abc", "", true},
	}
	for _, tc := range cases {
		got, err := Parse(tc.in)
		if tc.err {
			if err == nil {
				t.Fatalf("Parse(%q) expected error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.in, err)
		}
		if got.String() != tc.wantStr {
			t.Fatalf("Parse(%q) = %s, want %s", tc.in, got.String(), tc.wantStr)
		}
	}
}

func TestHelpers(t *testing.T) {
	if !IsZero(Zero) {
		t.Fatal("Zero should be zero")
	}
	if IsPositive(Zero) || IsNegative(Zero) {
		t.Fatal("Zero is neither positive nor negative")
	}
	a := New("1.5")
	b := New("1.5")
	if !Equal(a, b) {
		t.Fatal("1.5 != 1.5")
	}
	if !IsPositive(a) {
		t.Fatal("1.5 is positive")
	}
	if !IsNegative(New("-1")) {
		t.Fatal("-1 is negative")
	}
}
