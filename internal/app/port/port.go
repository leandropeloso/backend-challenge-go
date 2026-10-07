// Package port reúne as interfaces que a camada de aplicação exige da infraestrutura.
package port

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/leandropeloso/wager-service/internal/domain/event"
	"github.com/leandropeloso/wager-service/internal/domain/journal"
	"github.com/leandropeloso/wager-service/internal/domain/ledger"
	"github.com/leandropeloso/wager-service/internal/domain/money"
	"github.com/leandropeloso/wager-service/internal/domain/wager"
	"github.com/leandropeloso/wager-service/internal/domain/wallet"
)

var (
	ErrNotFound         = errors.New("not found")
	ErrWalletExists     = errors.New("a wallet already exists for this player and currency")
	ErrConcurrentUpdate = errors.New("wallet changed concurrently")
	// ErrRetryable marca falhas que desaparecem ao repetir a transação inteira
	// (deadlock, serialization failure).
	ErrRetryable = errors.New("retryable database conflict")
	// ErrTransient marca indisponibilidade temporária de dependências (banco, fila).
	ErrTransient = errors.New("transient dependency failure")
)

type Clock interface{ Now() time.Time }

type IDGenerator interface{ New() uuid.UUID }

// UnitOfWork executa fn dentro de uma única transação SQL. Todos os
// repositórios recebidos por fn compartilham essa transação; commit e rollback
// ficam a cargo de quem implementa.
type UnitOfWork interface {
	Do(ctx context.Context, fn func(ctx context.Context, r Repositories) error) error
}

type Repositories interface {
	Wallets() WalletRepository
	Transactions() TransactionRepository
	Ledger() LedgerRepository
	Journal() JournalRepository
	Inbox() InboxRepository
	Outbox() OutboxRepository
}

type WalletRepository interface {
	Insert(ctx context.Context, w *wallet.Wallet) error
	// GetForUpdate trava a linha da carteira até o fim da transação.
	GetForUpdate(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	// Save grava saldo e versão, exigindo que a versão no banco seja expectedVersion.
	Save(ctx context.Context, w *wallet.Wallet, expectedVersion int64) error
}

type TransactionRepository interface {
	// InsertIfAbsent grava a transação; devolve false se (provider, key) ou
	// (provider, externalId) já existirem.
	InsertIfAbsent(ctx context.Context, t *wager.Transaction) (bool, error)
	// InsertInternal grava uma transação OPENING.
	InsertInternal(ctx context.Context, t *wager.Transaction) error
	Update(ctx context.Context, t *wager.Transaction) error
	GetByID(ctx context.Context, id uuid.UUID) (*wager.Transaction, error)
	GetByProviderKey(ctx context.Context, providerID, idempotencyKey string) (*wager.Transaction, error)
	GetByProviderExternalID(ctx context.Context, providerID, externalID string) (*wager.Transaction, error)
	// HasProcessedReversal diz se a transação já recebeu REFUND/ROLLBACK processado.
	HasProcessedReversal(ctx context.Context, referenceID uuid.UUID) (bool, error)
	// ClaimDue trava (SKIP LOCKED) uma transação pendente cujo agendamento venceu.
	ClaimDue(ctx context.Context, now time.Time) (*wager.Transaction, error)
}

type LedgerRepository interface {
	Append(ctx context.Context, e *ledger.Entry, walletVersion int64) error
}

type JournalRepository interface {
	Append(ctx context.Context, e *journal.Entry) error
}

type InboxResult int

const (
	InboxNew InboxResult = iota
	InboxDuplicate
	InboxHashMismatch
)

type InboxRepository interface {
	Begin(ctx context.Context, consumer, messageID, hash string, now time.Time) (InboxResult, error)
	Complete(ctx context.Context, consumer, messageID string, now time.Time) error
}

type OutboxRepository interface {
	Add(ctx context.Context, e event.Event) error
}

// OutboxMessage é um evento reservado por um publisher.
type OutboxMessage struct {
	ID           uuid.UUID
	PartitionKey string
	EventType    string
	Payload      []byte
	Attempts     int
	OccurredAt   time.Time
	// TraceContext é o traceparent W3C do trace que gerou o evento (pode ser vazio).
	TraceContext string
}

// OutboxStore é a visão do publisher sobre a outbox, fora de transações de negócio.
type OutboxStore interface {
	// Claim reserva até limit eventos pendentes por lease; vários publishers
	// podem chamar em paralelo sem receber o mesmo evento.
	Claim(ctx context.Context, owner string, limit int, lease time.Duration) ([]OutboxMessage, error)
	MarkPublishedMany(ctx context.Context, ids []uuid.UUID, owner string) error
	Release(ctx context.Context, id uuid.UUID, owner string, cause string, retryAt time.Time) error
	Stats(ctx context.Context) (OutboxStats, error)
}

type OutboxStats struct {
	Pending   int64
	OldestAge time.Duration
}

// LedgerItem é a visão de leitura de um lançamento.
type LedgerItem struct {
	Entry         *ledger.Entry
	WalletVersion int64
}

// TrialLine é o total de débitos e créditos do diário em uma moeda. Os totais
// agregados podem exceder o intervalo de int64 (cada valor é válido, a soma não),
// por isso são decimais exatos calculados em NUMERIC, não Money.
type TrialLine struct {
	Currency money.Currency
	Debits   string // decimal com duas casas
	Credits  string
	Entries  int64
	Balanced bool
}

type Reconciliation struct {
	Stored     money.Money
	Calculated money.Money
	Entries    int64
}

// Reader serve as consultas, sem participar de transações de negócio.
type Reader interface {
	GetWallet(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error)
	// ListLedger devolve lançamentos com wallet_version > after, em ordem crescente.
	ListLedger(ctx context.Context, walletID uuid.UUID, after int64, limit int) ([]LedgerItem, error)
	GetTransaction(ctx context.Context, id uuid.UUID) (*wager.Transaction, error)
	GetTransactionByProvider(ctx context.Context, providerID, externalID string) (*wager.Transaction, error)
	// Reconcile lê saldo e ledger no mesmo snapshot consistente, sem alterar nada.
	Reconcile(ctx context.Context, walletID uuid.UUID) (Reconciliation, error)
	// TrialBalance totaliza o diário por moeda em um snapshot único.
	TrialBalance(ctx context.Context) ([]TrialLine, error)
}

// Metrics é implementada pela camada de observabilidade.
type Metrics interface {
	TransactionResult(kind wager.Kind, status wager.Status, code wager.FailureCode)
	Duplicate(source string)
	Retry(component string)
	DLQ(reason string)
	ConcurrencyConflict()
	ReconciliationDivergence()
	ProcessingDuration(source string, d time.Duration)
}
