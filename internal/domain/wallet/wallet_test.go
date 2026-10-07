package wallet

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/leandropeloso/wager-service/internal/domain/ledger"
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

func newWallet(t *testing.T, minor int64) *Wallet {
	t.Helper()
	w, err := Open(uuid.New(), uuid.New(), brl(t, minor), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestOpen(t *testing.T) {
	w := newWallet(t, 100000)
	if w.Version() != 1 {
		t.Errorf("versão inicial = %d", w.Version())
	}
	if w.Balance().Minor() != 100000 || w.Currency() != money.BRL {
		t.Errorf("saldo = %v", w.Balance())
	}
	if _, err := Open(uuid.New(), uuid.New(), brl(t, 0), time.Now()); err != nil {
		t.Errorf("saldo zero deve ser aceito: %v", err)
	}
	if _, err := Open(uuid.New(), uuid.New(), brl(t, -1), time.Now()); !errors.Is(err, ErrInvalidWallet) {
		t.Errorf("saldo negativo: %v", err)
	}
	if _, err := Open(uuid.Nil, uuid.New(), brl(t, 1), time.Now()); !errors.Is(err, ErrInvalidWallet) {
		t.Errorf("id nulo: %v", err)
	}
	if _, err := Open(uuid.New(), uuid.Nil, brl(t, 1), time.Now()); !errors.Is(err, ErrInvalidWallet) {
		t.Errorf("player nulo: %v", err)
	}
	if _, err := Open(uuid.New(), uuid.New(), money.Money{}, time.Now()); !errors.Is(err, ErrInvalidWallet) {
		t.Errorf("money zero value: %v", err)
	}
}

func TestDebitCredit(t *testing.T) {
	w := newWallet(t, 100000)
	mv, err := w.Debit(brl(t, 2500), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if mv.Direction != ledger.Debit || mv.BalanceBefore.Minor() != 100000 || mv.BalanceAfter.Minor() != 97500 || mv.Version != 2 {
		t.Fatalf("movimento = %+v", mv)
	}
	mv, err = w.Credit(brl(t, 500), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if mv.BalanceAfter.Minor() != 98000 || mv.Version != 3 || w.Version() != 3 {
		t.Fatalf("movimento = %+v, wallet v%d", mv, w.Version())
	}
}

func TestDebitRules(t *testing.T) {
	w := newWallet(t, 10000)
	if _, err := w.Debit(brl(t, 10001), time.Now()); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("saldo insuficiente: %v", err)
	}
	if w.Balance().Minor() != 10000 || w.Version() != 1 {
		t.Fatal("débito recusado não pode alterar o agregado")
	}
	if _, err := w.Debit(brl(t, 10000), time.Now()); err != nil {
		t.Fatalf("débito do saldo exato deve passar: %v", err)
	}
	if !w.Balance().IsZero() {
		t.Fatalf("saldo = %v", w.Balance())
	}
}

func TestMovementValidation(t *testing.T) {
	w := newWallet(t, 10000)
	usd, _ := money.FromMinor(100, "USD")
	if _, err := w.Credit(usd, time.Now()); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("moeda: %v", err)
	}
	if _, err := w.Debit(usd, time.Now()); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("moeda: %v", err)
	}
	if _, err := w.Credit(brl(t, 0), time.Now()); !errors.Is(err, ErrInvalidAmount) {
		t.Errorf("zero: %v", err)
	}
	if _, err := w.Debit(brl(t, -5), time.Now()); !errors.Is(err, ErrInvalidAmount) {
		t.Errorf("negativo: %v", err)
	}
	if _, err := w.Credit(money.Money{}, time.Now()); !errors.Is(err, money.ErrUninitialized) {
		t.Errorf("não inicializado: %v", err)
	}
	if w.Version() != 1 || w.Balance().Minor() != 10000 {
		t.Fatal("entradas inválidas não podem mudar o estado")
	}
}

func TestCreditOverflowLeavesWalletUntouched(t *testing.T) {
	w := newWallet(t, math.MaxInt64)
	if _, err := w.Credit(brl(t, 1), time.Now()); !errors.Is(err, money.ErrOverflow) {
		t.Fatalf("overflow: %v", err)
	}
	if w.Version() != 1 {
		t.Fatal("versão não deve mudar")
	}
}

func TestRehydrateDoesNotReapply(t *testing.T) {
	created := time.Now().Add(-time.Hour)
	w, err := Rehydrate(Snapshot{
		ID: uuid.New(), PlayerID: uuid.New(), Balance: brl(t, 4200),
		Version: 7, CreatedAt: created, UpdatedAt: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if w.Version() != 7 || w.Balance().Minor() != 4200 {
		t.Fatalf("estado alterado na reidratação: v%d %v", w.Version(), w.Balance())
	}
	if _, err := Rehydrate(Snapshot{ID: uuid.New(), PlayerID: uuid.New(), Balance: brl(t, 1), Version: 0, CreatedAt: created, UpdatedAt: created}); !errors.Is(err, ErrInvalidWallet) {
		t.Errorf("versão 0: %v", err)
	}
}
