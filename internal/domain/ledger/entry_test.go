package ledger

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/leandropeloso/wager-service/internal/domain/money"
)

func brl(t *testing.T, minor int64) money.Money {
	t.Helper()
	m, err := money.FromMinor(minor, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func ids() (uuid.UUID, uuid.UUID, uuid.UUID) {
	return uuid.New(), uuid.New(), uuid.New()
}

func TestNewValidatesBalanceInvariant(t *testing.T) {
	id, w, tx := ids()
	now := time.Now()

	e, err := New(id, w, tx, Debit, brl(t, 2500), brl(t, 100000), brl(t, 97500), now)
	if err != nil {
		t.Fatalf("débito válido: %v", err)
	}
	if e.Direction() != Debit || e.BalanceAfter().Minor() != 97500 {
		t.Fatalf("entry inesperada: %+v", e)
	}
	if _, err := New(id, w, tx, Credit, brl(t, 2500), brl(t, 97500), brl(t, 100000), now); err != nil {
		t.Fatalf("crédito válido: %v", err)
	}

	cases := map[string]struct {
		dir                 Direction
		amount, before, aft int64
		want                error
	}{
		"débito com saldo somado":     {Debit, 100, 1000, 1100, ErrBalanceMismatch},
		"crédito com saldo subtraído": {Credit, 100, 1000, 900, ErrBalanceMismatch},
		"saldo posterior errado":      {Credit, 100, 1000, 1101, ErrBalanceMismatch},
		"valor zero":                  {Credit, 0, 1000, 1000, ErrInvalidEntry},
		"saldo negativo":              {Debit, 100, 50, -50, ErrInvalidEntry},
		"direção inválida":            {"SIDEWAYS", 100, 1000, 1100, ErrInvalidDirection},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := New(id, w, tx, c.dir, brl(t, c.amount), brl(t, c.before), brl(t, c.aft), now)
			if !errors.Is(err, c.want) {
				t.Fatalf("err = %v, quer %v", err, c.want)
			}
		})
	}
}

func TestNewRejectsMissingData(t *testing.T) {
	id, w, tx := ids()
	now := time.Now()
	if _, err := New(uuid.Nil, w, tx, Credit, brl(t, 1), brl(t, 0), brl(t, 1), now); !errors.Is(err, ErrInvalidEntry) {
		t.Errorf("id nulo: %v", err)
	}
	if _, err := New(id, w, tx, Credit, money.Money{}, brl(t, 0), brl(t, 1), now); !errors.Is(err, ErrInvalidEntry) {
		t.Errorf("money não inicializado: %v", err)
	}
	if _, err := New(id, w, tx, Credit, brl(t, 1), brl(t, 0), brl(t, 1), time.Time{}); !errors.Is(err, ErrInvalidEntry) {
		t.Errorf("sem timestamp: %v", err)
	}
	usd, _ := money.FromMinor(1, "USD")
	if _, err := New(id, w, tx, Credit, usd, brl(t, 0), brl(t, 1), now); !errors.Is(err, ErrBalanceMismatch) {
		t.Errorf("moedas distintas: %v", err)
	}
}
