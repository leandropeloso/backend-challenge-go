// Package wallet contém o agregado Wallet, raiz do modelo financeiro.
package wallet

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/leandropeloso/wager-service/internal/domain/ledger"
	"github.com/leandropeloso/wager-service/internal/domain/money"
)

var (
	ErrInvalidWallet     = errors.New("wallet: invalid wallet")
	ErrInsufficientFunds = errors.New("wallet: insufficient funds")
	ErrInvalidAmount     = errors.New("wallet: movement amount must be positive")
	ErrCurrencyMismatch  = errors.New("wallet: movement currency differs from wallet currency")
	ErrVersionOverflow   = errors.New("wallet: version overflow")
)

type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
}

// Movement descreve uma mudança de saldo já aplicada ao agregado. A camada de
// aplicação a converte em lançamento de ledger e em evento, na mesma transação.
type Movement struct {
	Direction     ledger.Direction
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	Version       int64
}

// Open cria uma carteira nova com versão 1. O saldo inicial pode ser zero.
func Open(id, playerID uuid.UUID, initial money.Money, now time.Time) (*Wallet, error) {
	w := &Wallet{
		id: id, playerID: playerID, balance: initial, version: 1,
		createdAt: now.UTC(), updatedAt: now.UTC(),
	}
	if err := w.validate(); err != nil {
		return nil, err
	}
	return w, nil
}

// Snapshot é o estado persistido usado na reidratação.
type Snapshot struct {
	ID        uuid.UUID
	PlayerID  uuid.UUID
	Balance   money.Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Rehydrate reconstrói a carteira a partir do banco sem reaplicar movimentações.
func Rehydrate(s Snapshot) (*Wallet, error) {
	w := &Wallet{
		id: s.ID, playerID: s.PlayerID, balance: s.Balance, version: s.Version,
		createdAt: s.CreatedAt.UTC(), updatedAt: s.UpdatedAt.UTC(),
	}
	if err := w.validate(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Wallet) validate() error {
	switch {
	case w.id == uuid.Nil:
		return fmt.Errorf("%w: missing id", ErrInvalidWallet)
	case w.playerID == uuid.Nil:
		return fmt.Errorf("%w: missing player", ErrInvalidWallet)
	case !w.balance.IsValid():
		return fmt.Errorf("%w: %w", ErrInvalidWallet, money.ErrUninitialized)
	case w.balance.IsNegative():
		return fmt.Errorf("%w: negative balance", ErrInvalidWallet)
	case w.version < 1:
		return fmt.Errorf("%w: version must start at 1", ErrInvalidWallet)
	case w.createdAt.IsZero() || w.updatedAt.IsZero():
		return fmt.Errorf("%w: missing timestamps", ErrInvalidWallet)
	}
	return nil
}

func (w *Wallet) ID() uuid.UUID            { return w.id }
func (w *Wallet) PlayerID() uuid.UUID      { return w.playerID }
func (w *Wallet) Balance() money.Money     { return w.balance }
func (w *Wallet) Currency() money.Currency { return w.balance.Currency() }
func (w *Wallet) Version() int64           { return w.version }
func (w *Wallet) CreatedAt() time.Time     { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time     { return w.updatedAt }

func (w *Wallet) Credit(amount money.Money, now time.Time) (Movement, error) {
	if err := w.checkAmount(amount); err != nil {
		return Movement{}, err
	}
	after, err := w.balance.Add(amount)
	if err != nil {
		return Movement{}, err
	}
	return w.apply(ledger.Credit, amount, after, now)
}

func (w *Wallet) Debit(amount money.Money, now time.Time) (Movement, error) {
	if err := w.checkAmount(amount); err != nil {
		return Movement{}, err
	}
	after, err := w.balance.Sub(amount)
	if err != nil {
		return Movement{}, err
	}
	if after.IsNegative() {
		return Movement{}, ErrInsufficientFunds
	}
	return w.apply(ledger.Debit, amount, after, now)
}

func (w *Wallet) checkAmount(amount money.Money) error {
	if !amount.IsValid() {
		return money.ErrUninitialized
	}
	if amount.Currency() != w.balance.Currency() {
		return ErrCurrencyMismatch
	}
	if !amount.IsPositive() {
		return ErrInvalidAmount
	}
	return nil
}

func (w *Wallet) apply(dir ledger.Direction, amount, after money.Money, now time.Time) (Movement, error) {
	if w.version == int64(^uint64(0)>>1) {
		return Movement{}, ErrVersionOverflow
	}
	m := Movement{
		Direction: dir, Amount: amount,
		BalanceBefore: w.balance, BalanceAfter: after, Version: w.version + 1,
	}
	w.balance = after
	w.version = m.Version
	w.updatedAt = now.UTC()
	return m, nil
}
