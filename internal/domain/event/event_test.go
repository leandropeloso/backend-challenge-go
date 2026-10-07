package event

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/leandropeloso/wager-service/internal/domain/money"
	"github.com/leandropeloso/wager-service/internal/domain/wager"
	"github.com/leandropeloso/wager-service/internal/domain/wallet"
)

func brl(t *testing.T, minor int64) money.Money {
	t.Helper()
	m, err := money.FromMinor(minor, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func meta() Meta {
	return Meta{EventID: uuid.New(), CorrelationID: "corr-1", CausationID: "cause-1", Now: time.Now()}
}

func newTx(t *testing.T, kind wager.Kind, minor int64) *wager.Transaction {
	t.Helper()
	p := wager.ExternalParams{
		ID: uuid.New(), ProviderID: "p", ExternalID: "e", IdempotencyKey: "k",
		PlayerID: uuid.New(), WalletID: uuid.New(), RoundID: "r", GameID: "g",
		Kind: kind, Money: brl(t, minor), Now: time.Now(),
	}
	if kind.NeedsReference() {
		p.ReferenceExternalID = "ref"
	}
	tx, err := wager.NewExternal(p)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestProcessedEvent(t *testing.T) {
	tx := newTx(t, wager.Bet, 2500)
	if _, err := NewWagerTransactionProcessed(meta(), tx); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("PENDING não gera evento de processada: %v", err)
	}
	if err := tx.Process(brl(t, 97500), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	m := meta()
	ev, err := NewWagerTransactionProcessed(m, tx)
	if err != nil {
		t.Fatal(err)
	}
	if ev.Type() != TypeWagerTransactionProcessed || ev.Version() != 1 {
		t.Fatalf("tipo/versão: %s v%d", ev.Type(), ev.Version())
	}
	if ev.ID() != m.EventID || ev.AggregateID() != tx.ID() || ev.PartitionKey() != tx.WalletID() {
		t.Fatal("identidades do envelope")
	}
	d, ok := ev.Data().(WagerTransactionProcessed)
	if !ok || d.BalanceAfter.Minor() != 97500 || d.ProviderID != "p" {
		t.Fatalf("payload: %+v", ev.Data())
	}
	if ev.OccurredAt().Location() != time.UTC {
		t.Error("occurredAt deve ser UTC")
	}
}

func TestLossProducesProcessedEventWithoutBalanceChange(t *testing.T) {
	tx := newTx(t, wager.Loss, 0)
	if err := tx.Process(brl(t, 500), nil, time.Now()); err != nil {
		t.Fatal(err)
	}
	ev, err := NewWagerTransactionProcessed(meta(), tx)
	if err != nil {
		t.Fatal(err)
	}
	if d := ev.Data().(WagerTransactionProcessed); !d.Money.IsZero() || d.BalanceAfter.Minor() != 500 {
		t.Fatalf("payload: %+v", d)
	}
}

func TestRejectedEvent(t *testing.T) {
	tx := newTx(t, wager.Bet, 100)
	if _, err := NewWagerTransactionRejected(meta(), tx); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("PENDING: %v", err)
	}
	_ = tx.Reject(wager.InsufficientFunds, time.Now())
	ev, err := NewWagerTransactionRejected(meta(), tx)
	if err != nil {
		t.Fatal(err)
	}
	d := ev.Data().(WagerTransactionRejected)
	if d.FailureCode != wager.InsufficientFunds || d.Correctable {
		t.Fatalf("payload: %+v", d)
	}
}

func TestPendingReferenceEvent(t *testing.T) {
	tx := newTx(t, wager.Refund, 100)
	now := time.Now()
	if _, err := NewWagerTransactionPendingReference(meta(), tx); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("PENDING: %v", err)
	}
	if err := tx.AwaitReference(now, now.Add(time.Second), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	ev, err := NewWagerTransactionPendingReference(meta(), tx)
	if err != nil {
		t.Fatal(err)
	}
	d := ev.Data().(WagerTransactionPendingReference)
	if d.ReferenceExternalTransactionID != "ref" || d.ExpiresAt.IsZero() {
		t.Fatalf("payload: %+v", d)
	}
}

func TestBalanceChangedEvent(t *testing.T) {
	w, err := wallet.Open(uuid.New(), uuid.New(), brl(t, 1000), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	mv, err := w.Debit(brl(t, 300), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	txID := uuid.New()
	ev, err := NewWalletBalanceChanged(meta(), w.ID(), txID, mv)
	if err != nil {
		t.Fatal(err)
	}
	d := ev.Data().(WalletBalanceChanged)
	if d.BalanceBefore.Minor() != 1000 || d.BalanceAfter.Minor() != 700 || d.WalletVersion != 2 || d.TransactionID != txID {
		t.Fatalf("payload: %+v", d)
	}
	if ev.AggregateID() != w.ID() {
		t.Error("agregado do evento de saldo é a carteira")
	}
}

func TestMetaValidation(t *testing.T) {
	tx := newTx(t, wager.Bet, 100)
	_ = tx.Reject(wager.InsufficientFunds, time.Now())
	bad := []Meta{
		{CorrelationID: "c", Now: time.Now()},
		{EventID: uuid.New(), Now: time.Now()},
		{EventID: uuid.New(), CorrelationID: "c"},
	}
	for i, m := range bad {
		if _, err := NewWagerTransactionRejected(m, tx); !errors.Is(err, ErrInvalidEvent) {
			t.Errorf("meta %d: %v", i, err)
		}
	}
	// causationId é opcional
	m := Meta{EventID: uuid.New(), CorrelationID: "c", Now: time.Now()}
	if _, err := NewWagerTransactionRejected(m, tx); err != nil {
		t.Errorf("sem causationId: %v", err)
	}
}
