// Package ledger define o lançamento imutável do ledger da carteira.
package ledger

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/leandropeloso/wager-service/internal/domain/money"
)

var (
	ErrInvalidEntry     = errors.New("ledger: invalid entry")
	ErrInvalidDirection = errors.New("ledger: invalid direction")
	ErrBalanceMismatch  = errors.New("ledger: balanceAfter does not match balanceBefore and amount")
)

type Direction string

const (
	Debit  Direction = "DEBIT"
	Credit Direction = "CREDIT"
)

func (d Direction) Valid() bool { return d == Debit || d == Credit }

type Entry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	createdAt     time.Time
}

// New cria um lançamento validando balanceAfter = balanceBefore ± amount.
func New(id, walletID, transactionID uuid.UUID, dir Direction, amount, before, after money.Money, now time.Time) (*Entry, error) {
	e := &Entry{
		id: id, walletID: walletID, transactionID: transactionID, direction: dir,
		amount: amount, balanceBefore: before, balanceAfter: after, createdAt: now.UTC(),
	}
	if err := e.validate(); err != nil {
		return nil, err
	}
	return e, nil
}

// Rehydrate reconstrói um lançamento persistido, reaplicando apenas a validação estrutural.
func Rehydrate(id, walletID, transactionID uuid.UUID, dir Direction, amount, before, after money.Money, createdAt time.Time) (*Entry, error) {
	return New(id, walletID, transactionID, dir, amount, before, after, createdAt)
}

func (e *Entry) validate() error {
	if e.id == uuid.Nil || e.walletID == uuid.Nil || e.transactionID == uuid.Nil {
		return fmt.Errorf("%w: missing identifiers", ErrInvalidEntry)
	}
	if !e.direction.Valid() {
		return ErrInvalidDirection
	}
	if !e.amount.IsValid() || !e.balanceBefore.IsValid() || !e.balanceAfter.IsValid() {
		return fmt.Errorf("%w: %w", ErrInvalidEntry, money.ErrUninitialized)
	}
	if !e.amount.IsPositive() {
		return fmt.Errorf("%w: amount must be positive", ErrInvalidEntry)
	}
	if e.balanceBefore.IsNegative() || e.balanceAfter.IsNegative() {
		return fmt.Errorf("%w: negative balance", ErrInvalidEntry)
	}
	if e.createdAt.IsZero() {
		return fmt.Errorf("%w: missing timestamp", ErrInvalidEntry)
	}
	var expected money.Money
	var err error
	if e.direction == Credit {
		expected, err = e.balanceBefore.Add(e.amount)
	} else {
		expected, err = e.balanceBefore.Sub(e.amount)
	}
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBalanceMismatch, err)
	}
	if !expected.Equal(e.balanceAfter) {
		return ErrBalanceMismatch
	}
	return nil
}

func (e *Entry) ID() uuid.UUID              { return e.id }
func (e *Entry) WalletID() uuid.UUID        { return e.walletID }
func (e *Entry) TransactionID() uuid.UUID   { return e.transactionID }
func (e *Entry) Direction() Direction       { return e.direction }
func (e *Entry) Amount() money.Money        { return e.amount }
func (e *Entry) BalanceBefore() money.Money { return e.balanceBefore }
func (e *Entry) BalanceAfter() money.Money  { return e.balanceAfter }
func (e *Entry) CreatedAt() time.Time       { return e.createdAt }
