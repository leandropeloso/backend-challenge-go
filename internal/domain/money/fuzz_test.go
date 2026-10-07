package money

import (
	"errors"
	"math"
	"testing"
)

// FuzzParse garante que nenhuma entrada derruba o parser e que tudo que ele
// aceita é canônico: reformatar o valor devolve exatamente o texto de entrada.
func FuzzParse(f *testing.F) {
	for _, s := range []string{
		"0.00", "25.00", "92233720368547758.07", "92233720368547758.08", "-1.00", "1e3", "NaN", "Infinity",
		"", ".", "1.", ".5", "1.5", "1.000", "007.00", "1,00", " 1.00", "1.00 ", "０.００", "1.0\x00", "+1.00",
		"99999999999999999999999999.99",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, in string) {
		m, err := Parse(in, BRL)
		if err != nil {
			if !errors.Is(err, ErrInvalidAmount) && !errors.Is(err, ErrNegativeAmount) &&
				!errors.Is(err, ErrScaleNotSupported) && !errors.Is(err, ErrOverflow) {
				t.Fatalf("erro fora do contrato para %q: %v", in, err)
			}
			return
		}
		if m.IsNegative() {
			t.Fatalf("%q produziu valor negativo", in)
		}
		if got := m.Amount(); got != in {
			t.Fatalf("não canônico: %q -> %q", in, got)
		}
	})
}

// Propriedades da aritmética em torno dos limites.
func TestArithmeticProperties(t *testing.T) {
	samples := []int64{0, 1, -1, 99, 100, 12345, math.MaxInt64, math.MaxInt64 - 1, math.MinInt64 + 1, math.MinInt64}
	for _, a := range samples {
		for _, b := range samples {
			ma, _ := FromMinor(a, BRL)
			mb, _ := FromMinor(b, BRL)
			sum, err := ma.Add(mb)
			want := a + b
			overflow := (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b)
			switch {
			case overflow && !errors.Is(err, ErrOverflow):
				t.Fatalf("%d+%d deveria estourar", a, b)
			case !overflow && (err != nil || sum.Minor() != want):
				t.Fatalf("%d+%d = %v, %v", a, b, sum, err)
			}
			// a - b == a + (-b), exceto quando -b estoura
			diff, derr := ma.Sub(mb)
			if neg, nerr := mb.Negate(); nerr == nil {
				viaAdd, aerr := ma.Add(neg)
				if (derr == nil) != (aerr == nil) || (derr == nil && !diff.Equal(viaAdd)) {
					t.Fatalf("%d-%d inconsistente com soma da negação", a, b)
				}
			} else if !errors.Is(derr, ErrOverflow) {
				t.Fatalf("%d-%d com -b estourando deveria estourar", a, b)
			}
		}
	}
}
