// Package money implementa o value object Money.
//
// O valor é um int64 em centavos (escala fixa de duas casas), o que cobre
// +-92.233.720.368.547.758,07. Todas as operações checam overflow e nenhuma
// etapa (parsing, cálculo, formatação) passa por ponto flutuante.
package money

import (
	"errors"
	"math"
	"strconv"
	"strings"
)

var (
	ErrUninitialized     = errors.New("money: uninitialized value")
	ErrCurrencyMismatch  = errors.New("money: currency mismatch")
	ErrOverflow          = errors.New("money: arithmetic overflow")
	ErrInvalidAmount     = errors.New("money: invalid amount")
	ErrNegativeAmount    = errors.New("money: negative amount")
	ErrScaleNotSupported = errors.New("money: amount must have exactly two decimal places")
)

// Money é imutável; o zero value (sem moeda) é inválido e rejeitado nas operações.
type Money struct {
	minor    int64
	currency Currency
}

// Parse lê um valor monetário externo. Aceita apenas o formato canônico
// "<inteiro>.<dd>" sem sinal, sem zeros à esquerda (exceto o próprio "0"),
// sem expoente e sem separadores. Formas equivalentes ("25", "25.5", "025.00")
// são rejeitadas em vez de normalizadas, o que mantém o hash de idempotência
// determinístico sem arredondamento silencioso.
func Parse(amount string, currency Currency) (Money, error) {
	if _, ok := iso4217[string(currency)]; !ok {
		return Money{}, ErrInvalidCurrency
	}
	if amount == "" {
		return Money{}, ErrInvalidAmount
	}
	if strings.HasPrefix(amount, "-") {
		return Money{}, ErrNegativeAmount
	}
	intPart, frac, found := strings.Cut(amount, ".")
	if intPart == "" || !allDigits(intPart) || (found && frac != "" && !allDigits(frac)) {
		return Money{}, ErrInvalidAmount
	}
	if !found || len(frac) != 2 {
		return Money{}, ErrScaleNotSupported
	}
	if len(intPart) > 1 && intPart[0] == '0' {
		return Money{}, ErrInvalidAmount
	}
	units, err := strconv.ParseInt(intPart, 10, 64)
	if err != nil {
		return Money{}, ErrOverflow
	}
	cents, _ := strconv.ParseInt(frac, 10, 64)
	if units > (math.MaxInt64-cents)/100 {
		return Money{}, ErrOverflow
	}
	return Money{minor: units*100 + cents, currency: currency}, nil
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) > 0
}

// FromMinor reconstrói um Money a partir de centavos (persistência, cálculos).
func FromMinor(minor int64, currency Currency) (Money, error) {
	if _, ok := iso4217[string(currency)]; !ok {
		return Money{}, ErrInvalidCurrency
	}
	return Money{minor: minor, currency: currency}, nil
}

func Zero(currency Currency) (Money, error) { return FromMinor(0, currency) }

func (m Money) Minor() int64       { return m.minor }
func (m Money) Currency() Currency { return m.currency }

func (m Money) IsValid() bool { return m.currency != "" }

func (m Money) IsZero() bool     { return m.minor == 0 }
func (m Money) IsNegative() bool { return m.minor < 0 }
func (m Money) IsPositive() bool { return m.minor > 0 }

func (m Money) compatible(o Money) error {
	if !m.IsValid() || !o.IsValid() {
		return ErrUninitialized
	}
	if m.currency != o.currency {
		return ErrCurrencyMismatch
	}
	return nil
}

func (m Money) Add(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) || (o.minor < 0 && m.minor < math.MinInt64-o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

func (m Money) Sub(o Money) (Money, error) {
	neg, err := o.Negate()
	if err != nil {
		return Money{}, err
	}
	return m.Add(neg)
}

func (m Money) Negate() (Money, error) {
	if !m.IsValid() {
		return Money{}, ErrUninitialized
	}
	if m.minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

// Cmp devolve -1, 0 ou 1.
func (m Money) Cmp(o Money) (int, error) {
	if err := m.compatible(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	}
	return 0, nil
}

func (m Money) Equal(o Money) bool {
	return m.currency == o.currency && m.minor == o.minor
}

// Amount formata o valor como decimal com duas casas, ex.: "-0.05", "25.00".
func (m Money) Amount() string {
	v := m.minor
	sign := ""
	var abs uint64
	if v < 0 {
		sign = "-"
		abs = uint64(-(v + 1)) + 1
	} else {
		abs = uint64(v)
	}
	units := abs / 100
	cents := abs % 100
	c := strconv.FormatUint(cents, 10)
	if cents < 10 {
		c = "0" + c
	}
	return sign + strconv.FormatUint(units, 10) + "." + c
}

func (m Money) String() string { return m.Amount() + " " + string(m.currency) }
