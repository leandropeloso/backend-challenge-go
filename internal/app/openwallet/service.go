// Package openwallet implementa a abertura interna de carteiras.
package openwallet

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/domain/event"
	"github.com/leandropeloso/wager-service/internal/domain/journal"
	"github.com/leandropeloso/wager-service/internal/domain/ledger"
	"github.com/leandropeloso/wager-service/internal/domain/money"
	"github.com/leandropeloso/wager-service/internal/domain/wager"
	"github.com/leandropeloso/wager-service/internal/domain/wallet"
	"github.com/leandropeloso/wager-service/internal/telemetry"
)

var ErrAlreadyExists = errors.New("wallet already exists for this player and currency")

type InvalidInputError struct {
	Field  string
	Reason string
}

func (e *InvalidInputError) Error() string { return "invalid input: " + e.Field + ": " + e.Reason }

type Command struct {
	PlayerID      string
	Amount        string
	Currency      string
	CorrelationID string
}

type Service struct {
	uow   port.UnitOfWork
	clock port.Clock
	ids   port.IDGenerator
}

func NewService(uow port.UnitOfWork, clock port.Clock, ids port.IDGenerator) *Service {
	return &Service{uow: uow, clock: clock, ids: ids}
}

// Open cria a carteira. Com saldo positivo, grava no mesmo commit a transação
// OPENING, o lançamento de crédito e os eventos de outbox correspondentes.
func (s *Service) Open(ctx context.Context, cmd Command) (opened *wallet.Wallet, err error) {
	ctx, span := telemetry.Start(ctx, "wallet.open")
	defer func() {
		if opened != nil {
			span.SetAttributes(attribute.String("wallet.id", opened.ID().String()))
		}
		telemetry.Fail(span, err)
		span.End()
	}()
	playerID, err := parse(cmd.PlayerID)
	if err != nil {
		return nil, &InvalidInputError{Field: "playerId", Reason: "must be a UUID"}
	}
	cur, err := money.ParseCurrency(cmd.Currency)
	if err != nil {
		return nil, &InvalidInputError{Field: "initialBalance.currency", Reason: err.Error()}
	}
	initial, err := money.Parse(cmd.Amount, cur)
	if err != nil {
		return nil, &InvalidInputError{Field: "initialBalance.amount", Reason: err.Error()}
	}

	now := s.clock.Now()
	w, err := wallet.Open(s.ids.New(), playerID, initial, now)
	if err != nil {
		return nil, &InvalidInputError{Field: "wallet", Reason: err.Error()}
	}
	corr := cmd.CorrelationID
	if corr == "" {
		corr = w.ID().String()
	}

	err = s.uow.Do(ctx, func(ctx context.Context, r port.Repositories) error {
		if err := r.Wallets().Insert(ctx, w); err != nil {
			if errors.Is(err, port.ErrWalletExists) {
				return ErrAlreadyExists
			}
			return err
		}
		if !initial.IsPositive() {
			return nil
		}
		opening, err := wager.NewOpening(s.ids.New(), w.ID(), playerID, initial, now)
		if err != nil {
			return err
		}
		if err := r.Transactions().InsertInternal(ctx, opening); err != nil {
			return err
		}
		zero, err := money.Zero(cur)
		if err != nil {
			return err
		}
		entry, err := ledger.New(s.ids.New(), w.ID(), opening.ID(), ledger.Credit, initial, zero, initial, now)
		if err != nil {
			return err
		}
		if err := r.Ledger().Append(ctx, entry, w.Version()); err != nil {
			return err
		}
		je, err := journal.ForMovement(s.ids.New(), opening, ledger.Credit, initial, now)
		if err != nil {
			return err
		}
		if err := r.Journal().Append(ctx, je); err != nil {
			return err
		}
		meta := func() event.Meta {
			return event.Meta{EventID: s.ids.New(), CorrelationID: corr, CausationID: opening.ID().String(), Now: now}
		}
		processed, err := event.NewWagerTransactionProcessed(meta(), opening)
		if err != nil {
			return err
		}
		changed, err := event.NewWalletBalanceChanged(meta(), w.ID(), opening.ID(), wallet.Movement{
			Direction: ledger.Credit, Amount: initial, BalanceBefore: zero, BalanceAfter: initial, Version: w.Version(),
		})
		if err != nil {
			return err
		}
		for _, ev := range []event.Event{processed, changed} {
			if err := r.Outbox().Add(ctx, ev); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("open wallet: %w", err)
	}
	return w, nil
}

func parse(v string) (uuid.UUID, error) {
	if len(v) != 36 {
		return uuid.Nil, errors.New("invalid uuid")
	}
	id, err := uuid.Parse(v)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, errors.New("invalid uuid")
	}
	return id, nil
}
