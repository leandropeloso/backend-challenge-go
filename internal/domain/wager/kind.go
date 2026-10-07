// Package wager modela as operações financeiras dos provedores: tipo, estado,
// códigos de falha, idempotência (hash do conteúdo) e a máquina de estados da transação.
package wager

import "errors"

var (
	ErrInvalidKind     = errors.New("wager: invalid transaction kind")
	ErrKindNotExternal = errors.New("wager: kind is reserved for internal use")
)

type Kind string

const (
	Opening  Kind = "OPENING"
	Bet      Kind = "BET"
	Win      Kind = "WIN"
	Loss     Kind = "LOSS"
	Refund   Kind = "REFUND"
	Rollback Kind = "ROLLBACK"
)

// ParseExternalKind aceita somente os tipos que podem chegar por HTTP ou SQS.
func ParseExternalKind(s string) (Kind, error) {
	switch k := Kind(s); k {
	case Bet, Win, Loss, Refund, Rollback:
		return k, nil
	case Opening:
		return "", ErrKindNotExternal
	}
	return "", ErrInvalidKind
}

func (k Kind) IsReversal() bool { return k == Refund || k == Rollback }

// NeedsReference indica se o tipo exige referenceExternalTransactionId.
func (k Kind) NeedsReference() bool { return k.IsReversal() }

// AllowsReference indica se o tipo pode carregar uma referência.
func (k Kind) AllowsReference() bool { return k != Bet && k != Opening }

// MovesBalance é falso apenas para LOSS.
func (k Kind) MovesBalance() bool { return k != Loss }

// CanBeReversedBy diz se uma transação deste tipo pode ser alvo da reversão informada.
func (k Kind) CanBeReversedBy(r Kind) bool {
	switch r {
	case Refund:
		return k == Bet
	case Rollback:
		return k == Bet || k == Win || k == Refund
	}
	return false
}

type Status string

const (
	Pending          Status = "PENDING"
	PendingReference Status = "PENDING_REFERENCE"
	Processed        Status = "PROCESSED"
	Rejected         Status = "REJECTED"
	Failed           Status = "FAILED"
)

func (s Status) Terminal() bool { return s == Processed || s == Rejected || s == Failed }

func (s Status) Valid() bool {
	switch s {
	case Pending, PendingReference, Processed, Rejected, Failed:
		return true
	}
	return false
}

// FailureCode é estável e faz parte do contrato com os provedores.
type FailureCode string

const (
	// Resultados definitivos: reenviar a mesma operação não muda o desfecho.
	InsufficientFunds            FailureCode = "INSUFFICIENT_FUNDS"
	InsufficientFundsForReversal FailureCode = "INSUFFICIENT_FUNDS_FOR_REVERSAL"
	ReferenceNotFound            FailureCode = "REFERENCE_NOT_FOUND"
	ReferenceNotProcessed        FailureCode = "REFERENCE_NOT_PROCESSED"
	AlreadyReversed              FailureCode = "ALREADY_REVERSED"

	// Entradas corrigíveis: o provedor deve enviar uma nova operação corrigida.
	WalletNotFound          FailureCode = "WALLET_NOT_FOUND"
	PlayerMismatch          FailureCode = "PLAYER_MISMATCH"
	CurrencyMismatch        FailureCode = "CURRENCY_MISMATCH"
	ReferenceMismatch       FailureCode = "REFERENCE_MISMATCH"
	ReferenceKindNotAllowed FailureCode = "REFERENCE_KIND_NOT_ALLOWED"
	ReferenceAmountMismatch FailureCode = "REFERENCE_AMOUNT_MISMATCH"

	// Falha permanente de infraestrutura (estado FAILED).
	InternalError FailureCode = "INTERNAL_ERROR"
)

// Correctable indica rejeições causadas por dados da requisição que o
// provedor pode ajustar, em oposição a resultados definitivos de negócio.
func (c FailureCode) Correctable() bool {
	switch c {
	case WalletNotFound, PlayerMismatch, CurrencyMismatch, ReferenceMismatch,
		ReferenceKindNotAllowed, ReferenceAmountMismatch:
		return true
	}
	return false
}

func (c FailureCode) Valid() bool {
	switch c {
	case InsufficientFunds, InsufficientFundsForReversal, ReferenceNotFound, ReferenceNotProcessed,
		AlreadyReversed, WalletNotFound, PlayerMismatch, CurrencyMismatch, ReferenceMismatch,
		ReferenceKindNotAllowed, ReferenceAmountMismatch, InternalError:
		return true
	}
	return false
}
