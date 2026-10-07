package money

import (
	"errors"
	"math"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		in    string
		minor int64
		err   error
	}{
		{"25.00", 2500, nil},
		{"0.00", 0, nil},
		{"0.05", 5, nil},
		{"1000.99", 100099, nil},
		{"92233720368547758.07", math.MaxInt64, nil},
		{"92233720368547758.08", 0, ErrOverflow},
		{"92233720368547759.00", 0, ErrOverflow},
		{"999999999999999999999.00", 0, ErrOverflow},
		{"", 0, ErrInvalidAmount},
		{"NaN", 0, ErrInvalidAmount},
		{"Infinity", 0, ErrInvalidAmount},
		{"1e3", 0, ErrInvalidAmount},
		{"1.0e1", 0, ErrInvalidAmount},
		{".50", 0, ErrInvalidAmount},
		{"1,00", 0, ErrInvalidAmount},
		{" 1.00", 0, ErrInvalidAmount},
		{"+1.00", 0, ErrInvalidAmount},
		{"01.00", 0, ErrInvalidAmount},
		{"-1.00", 0, ErrNegativeAmount},
		{"-0.00", 0, ErrNegativeAmount},
		{"25", 0, ErrScaleNotSupported},
		{"25.5", 0, ErrScaleNotSupported},
		{"25.", 0, ErrScaleNotSupported},
		{"25.001", 0, ErrScaleNotSupported},
		{"25.0a", 0, ErrInvalidAmount},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			m, err := Parse(tc.in, BRL)
			if !errors.Is(err, tc.err) {
				t.Fatalf("Parse(%q) err = %v, quer %v", tc.in, err, tc.err)
			}
			if err == nil && m.Minor() != tc.minor {
				t.Fatalf("Parse(%q) = %d, quer %d", tc.in, m.Minor(), tc.minor)
			}
		})
	}
}

func TestParseCurrencyInvalid(t *testing.T) {
	if _, err := Parse("1.00", Currency("XXX")); !errors.Is(err, ErrInvalidCurrency) {
		t.Fatalf("err = %v", err)
	}
	if _, err := Parse("1.00", Currency("")); !errors.Is(err, ErrInvalidCurrency) {
		t.Fatalf("err = %v", err)
	}
	if _, err := ParseCurrency("brl"); !errors.Is(err, ErrInvalidCurrency) {
		t.Fatalf("minúsculas devem ser rejeitadas: %v", err)
	}
	if c, err := ParseCurrency("BRL"); err != nil || c != BRL {
		t.Fatalf("BRL: %v %v", c, err)
	}
}

func mustMoney(t *testing.T, minor int64, c Currency) Money {
	t.Helper()
	m, err := FromMinor(minor, c)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestAmountFormatting(t *testing.T) {
	tests := map[int64]string{
		0:                 "0.00",
		5:                 "0.05",
		100:               "1.00",
		2500:              "25.00",
		-5:                "-0.05",
		-12345:            "-123.45",
		math.MaxInt64:     "92233720368547758.07",
		math.MinInt64:     "-92233720368547758.08",
		math.MinInt64 + 1: "-92233720368547758.07",
	}
	for minor, want := range tests {
		if got := mustMoney(t, minor, BRL).Amount(); got != want {
			t.Errorf("Amount(%d) = %q, quer %q", minor, got, want)
		}
	}
}

func TestParseRoundTrip(t *testing.T) {
	for _, s := range []string{"0.00", "0.01", "10.10", "123456789.99"} {
		m, err := Parse(s, BRL)
		if err != nil {
			t.Fatal(err)
		}
		if m.Amount() != s {
			t.Errorf("round trip %q -> %q", s, m.Amount())
		}
	}
}

func TestAddSub(t *testing.T) {
	a := mustMoney(t, 1000, BRL)
	b := mustMoney(t, 250, BRL)
	sum, err := a.Add(b)
	if err != nil || sum.Minor() != 1250 {
		t.Fatalf("add = %v, %v", sum, err)
	}
	diff, err := b.Sub(a)
	if err != nil || diff.Minor() != -750 {
		t.Fatalf("sub = %v, %v", diff, err)
	}
}

func TestOverflow(t *testing.T) {
	max := mustMoney(t, math.MaxInt64, BRL)
	min := mustMoney(t, math.MinInt64, BRL)
	one := mustMoney(t, 1, BRL)

	if _, err := max.Add(one); !errors.Is(err, ErrOverflow) {
		t.Errorf("max+1: %v", err)
	}
	if _, err := min.Sub(one); !errors.Is(err, ErrOverflow) {
		t.Errorf("min-1: %v", err)
	}
	if _, err := min.Add(mustMoney(t, -1, BRL)); !errors.Is(err, ErrOverflow) {
		t.Errorf("min+(-1): %v", err)
	}
	if _, err := min.Negate(); !errors.Is(err, ErrOverflow) {
		t.Errorf("-min: %v", err)
	}
	if _, err := one.Sub(min); !errors.Is(err, ErrOverflow) {
		t.Errorf("1-min: %v", err)
	}
	if got, err := max.Add(mustMoney(t, math.MinInt64, BRL)); err != nil || got.Minor() != -1 {
		t.Errorf("max+min = %v, %v", got, err)
	}
	if got, err := max.Negate(); err != nil || got.Minor() != -math.MaxInt64 {
		t.Errorf("-max = %v, %v", got, err)
	}
}

func TestCurrencyMismatch(t *testing.T) {
	brl := mustMoney(t, 100, BRL)
	usd := mustMoney(t, 100, "USD")
	if _, err := brl.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("add: %v", err)
	}
	if _, err := brl.Sub(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("sub: %v", err)
	}
	if _, err := brl.Cmp(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("cmp: %v", err)
	}
	if brl.Equal(usd) {
		t.Error("Equal deve considerar a moeda")
	}
}

func TestUninitialized(t *testing.T) {
	var z Money
	if z.IsValid() {
		t.Fatal("zero value não deve ser válido")
	}
	ok := mustMoney(t, 1, BRL)
	if _, err := z.Add(ok); !errors.Is(err, ErrUninitialized) {
		t.Errorf("add: %v", err)
	}
	if _, err := ok.Cmp(z); !errors.Is(err, ErrUninitialized) {
		t.Errorf("cmp: %v", err)
	}
	if _, err := z.Negate(); !errors.Is(err, ErrUninitialized) {
		t.Errorf("negate: %v", err)
	}
}

func TestCmpAndSigns(t *testing.T) {
	a, b := mustMoney(t, 100, BRL), mustMoney(t, 200, BRL)
	if c, _ := a.Cmp(b); c != -1 {
		t.Error("a<b")
	}
	if c, _ := b.Cmp(a); c != 1 {
		t.Error("b>a")
	}
	if c, _ := a.Cmp(a); c != 0 {
		t.Error("a==a")
	}
	z, _ := Zero(BRL)
	n := mustMoney(t, -1, BRL)
	if !z.IsZero() || z.IsPositive() || z.IsNegative() {
		t.Error("zero")
	}
	if !n.IsNegative() || !a.IsPositive() {
		t.Error("sinais")
	}
}
