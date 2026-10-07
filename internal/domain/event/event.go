// Package event define os eventos de integração emitidos pelo domínio.
//
// Os tipos aqui não conhecem formato de transporte; a serialização para o
// payload da outbox fica na camada de infraestrutura.
package event

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/leandropeloso/wager-service/internal/domain/ledger"
	"github.com/leandropeloso/wager-service/internal/domain/money"
	"github.com/leandropeloso/wager-service/internal/domain/wager"
	"github.com/leandropeloso/wager-service/internal/domain/wallet"
)

var ErrInvalidEvent = errors.New("event: invalid event")

type Type string

const (
	TypeWagerTransactionProcessed        Type = "WagerTransactionProcessed"
	TypeWagerTransactionRejected         Type = "WagerTransactionRejected"
	TypeWalletBalanceChanged             Type = "WalletBalanceChanged"
	TypeWagerTransactionPendingReference Type = "WagerTransactionPendingReference"
)

// Data é implementado apenas pelos payloads deste pacote.
type Data interface {
	eventType() Type
	version() int
}

// Event é o envelope. Tipo e versão derivam do payload, nunca do chamador.
type Event struct {
	id            uuid.UUID
	aggregateID   uuid.UUID
	partitionKey  uuid.UUID
	correlationID string
	causationID   string
	occurredAt    time.Time
	data          Data
}

func (e Event) ID() uuid.UUID          { return e.id }
func (e Event) Type() Type             { return e.data.eventType() }
func (e Event) Version() int           { return e.data.version() }
func (e Event) AggregateID() uuid.UUID { return e.aggregateID }

// PartitionKey agrupa eventos que precisam de ordenação relativa (a carteira).
func (e Event) PartitionKey() uuid.UUID { return e.partitionKey }
func (e Event) CorrelationID() string   { return e.correlationID }

// CausationID é opcional; vazio quando não há causa anterior.
func (e Event) CausationID() string   { return e.causationID }
func (e Event) OccurredAt() time.Time { return e.occurredAt }
func (e Event) Data() Data            { return e.data }

// Meta reúne o que o chamador controla na criação de um evento.
type Meta struct {
	EventID       uuid.UUID
	CorrelationID string
	CausationID   string
	Now           time.Time
}

func (m Meta) validate() error {
	if m.EventID == uuid.Nil {
		return fmt.Errorf("%w: missing event id", ErrInvalidEvent)
	}
	if m.CorrelationID == "" {
		return fmt.Errorf("%w: missing correlation id", ErrInvalidEvent)
	}
	if m.Now.IsZero() {
		return fmt.Errorf("%w: missing timestamp", ErrInvalidEvent)
	}
	return nil
}

// TransactionInfo é o trecho comum às transações. Para OPENING, os campos
// externos ficam vazios.
type TransactionInfo struct {
	TransactionID         uuid.UUID
	WalletID              uuid.UUID
	PlayerID              uuid.UUID
	Kind                  wager.Kind
	ProviderID            string
	ExternalTransactionID string
	RoundID               string
	GameID                string
	Money                 money.Money
}

func infoOf(t *wager.Transaction) TransactionInfo {
	return TransactionInfo{
		TransactionID: t.ID(), WalletID: t.WalletID(), PlayerID: t.PlayerID(), Kind: t.Kind(),
		ProviderID: t.ProviderID(), ExternalTransactionID: t.ExternalID(),
		RoundID: t.RoundID(), GameID: t.GameID(), Money: t.Money(),
	}
}

type WagerTransactionProcessed struct {
	TransactionInfo
	BalanceAfter money.Money
}

func (WagerTransactionProcessed) eventType() Type { return TypeWagerTransactionProcessed }
func (WagerTransactionProcessed) version() int    { return 1 }

type WagerTransactionRejected struct {
	TransactionInfo
	FailureCode wager.FailureCode
	Correctable bool
}

func (WagerTransactionRejected) eventType() Type { return TypeWagerTransactionRejected }
func (WagerTransactionRejected) version() int    { return 1 }

type WalletBalanceChanged struct {
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	Direction     ledger.Direction
	Money         money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	WalletVersion int64
}

func (WalletBalanceChanged) eventType() Type { return TypeWalletBalanceChanged }
func (WalletBalanceChanged) version() int    { return 1 }

type WagerTransactionPendingReference struct {
	TransactionInfo
	ReferenceExternalTransactionID string
	NextAttemptAt                  time.Time
	ExpiresAt                      time.Time
}

func (WagerTransactionPendingReference) eventType() Type { return TypeWagerTransactionPendingReference }
func (WagerTransactionPendingReference) version() int    { return 1 }

func build(m Meta, aggregate, partition uuid.UUID, d Data) (Event, error) {
	if err := m.validate(); err != nil {
		return Event{}, err
	}
	return Event{
		id: m.EventID, aggregateID: aggregate, partitionKey: partition,
		correlationID: m.CorrelationID, causationID: m.CausationID,
		occurredAt: m.Now.UTC(), data: d,
	}, nil
}

// NewWagerTransactionProcessed exige uma transação PROCESSED.
func NewWagerTransactionProcessed(m Meta, t *wager.Transaction) (Event, error) {
	if t.Status() != wager.Processed || t.ResultBalance() == nil {
		return Event{}, fmt.Errorf("%w: transaction is not processed", ErrInvalidEvent)
	}
	return build(m, t.ID(), t.WalletID(), WagerTransactionProcessed{
		TransactionInfo: infoOf(t), BalanceAfter: *t.ResultBalance(),
	})
}

// NewWagerTransactionRejected exige uma transação REJECTED.
func NewWagerTransactionRejected(m Meta, t *wager.Transaction) (Event, error) {
	if t.Status() != wager.Rejected {
		return Event{}, fmt.Errorf("%w: transaction is not rejected", ErrInvalidEvent)
	}
	return build(m, t.ID(), t.WalletID(), WagerTransactionRejected{
		TransactionInfo: infoOf(t), FailureCode: t.FailureCode(), Correctable: t.FailureCode().Correctable(),
	})
}

// NewWagerTransactionPendingReference exige uma transação PENDING_REFERENCE agendada.
func NewWagerTransactionPendingReference(m Meta, t *wager.Transaction) (Event, error) {
	if t.Status() != wager.PendingReference || t.NextAttemptAt() == nil || t.ExpiresAt() == nil {
		return Event{}, fmt.Errorf("%w: transaction is not waiting for a reference", ErrInvalidEvent)
	}
	return build(m, t.ID(), t.WalletID(), WagerTransactionPendingReference{
		TransactionInfo:                infoOf(t),
		ReferenceExternalTransactionID: t.ReferenceExternalID(),
		NextAttemptAt:                  t.NextAttemptAt().UTC(),
		ExpiresAt:                      t.ExpiresAt().UTC(),
	})
}

// NewWalletBalanceChanged descreve a mudança de saldo já aplicada à carteira.
func NewWalletBalanceChanged(m Meta, walletID, transactionID uuid.UUID, mv wallet.Movement) (Event, error) {
	if walletID == uuid.Nil || transactionID == uuid.Nil {
		return Event{}, fmt.Errorf("%w: missing identifiers", ErrInvalidEvent)
	}
	return build(m, walletID, walletID, WalletBalanceChanged{
		WalletID: walletID, TransactionID: transactionID, Direction: mv.Direction, Money: mv.Amount,
		BalanceBefore: mv.BalanceBefore, BalanceAfter: mv.BalanceAfter, WalletVersion: mv.Version,
	})
}
