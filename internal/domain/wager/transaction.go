package wager

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/leandropeloso/wager-service/internal/domain/money"
)

var (
	ErrInvalidTransaction = errors.New("wager: invalid transaction")
	ErrInvalidTransition  = errors.New("wager: invalid state transition")
	ErrTerminalState      = errors.New("wager: transaction already in a terminal state")
)

const maxFieldLen = 255

// Transaction é uma operação financeira registrada: externa (BET, WIN, ...) ou
// a abertura interna de carteira (OPENING).
type Transaction struct {
	id                  uuid.UUID
	providerID          string
	externalID          string
	idempotencyKey      string
	payloadHash         string
	walletID            uuid.UUID
	playerID            uuid.UUID
	roundID             string
	gameID              string
	kind                Kind
	amount              money.Money
	referenceExternalID string
	resolvedReferenceID *uuid.UUID
	status              Status
	failureCode         FailureCode
	resultBalance       *money.Money
	attempts            int
	nextAttemptAt       *time.Time
	expiresAt           *time.Time
	createdAt           time.Time
	updatedAt           time.Time
	completedAt         *time.Time
}

// ExternalParams reúne os dados de negócio de uma operação vinda de um provedor.
type ExternalParams struct {
	ID                  uuid.UUID
	ProviderID          string
	ExternalID          string
	IdempotencyKey      string
	PlayerID            uuid.UUID
	WalletID            uuid.UUID
	RoundID             string
	GameID              string
	Kind                Kind
	Money               money.Money
	ReferenceExternalID string
	Now                 time.Time
}

// NewExternal valida e cria uma operação externa em PENDING.
func NewExternal(p ExternalParams) (*Transaction, error) {
	if p.ID == uuid.Nil {
		return nil, invalid("missing id")
	}
	if p.Kind == Opening {
		return nil, ErrKindNotExternal
	}
	if _, err := ParseExternalKind(string(p.Kind)); err != nil {
		return nil, err
	}
	for name, v := range map[string]string{
		"providerId": p.ProviderID, "externalTransactionId": p.ExternalID,
		"idempotencyKey": p.IdempotencyKey, "roundId": p.RoundID, "gameId": p.GameID,
	} {
		if err := checkText(name, v); err != nil {
			return nil, err
		}
	}
	if p.PlayerID == uuid.Nil || p.WalletID == uuid.Nil {
		return nil, invalid("playerId and walletId are required")
	}
	if !p.Money.IsValid() {
		return nil, fmt.Errorf("%w: %w", ErrInvalidTransaction, money.ErrUninitialized)
	}
	if p.Kind == Loss {
		if !p.Money.IsZero() {
			return nil, invalid("LOSS requires a zero amount")
		}
	} else if !p.Money.IsPositive() {
		return nil, invalid(string(p.Kind) + " requires a positive amount")
	}
	ref := p.ReferenceExternalID
	switch {
	case p.Kind.NeedsReference() && ref == "":
		return nil, invalid("referenceExternalTransactionId is required for " + string(p.Kind))
	case ref != "" && !p.Kind.AllowsReference():
		return nil, invalid("referenceExternalTransactionId is not allowed for " + string(p.Kind))
	case ref != "":
		if err := checkText("referenceExternalTransactionId", ref); err != nil {
			return nil, err
		}
		if ref == p.ExternalID {
			return nil, invalid("a transaction cannot reference itself")
		}
	}
	now := p.Now.UTC()
	if now.IsZero() {
		return nil, invalid("missing timestamp")
	}
	t := &Transaction{
		id: p.ID, providerID: p.ProviderID, externalID: p.ExternalID, idempotencyKey: p.IdempotencyKey,
		walletID: p.WalletID, playerID: p.PlayerID, roundID: p.RoundID, gameID: p.GameID,
		kind: p.Kind, amount: p.Money, referenceExternalID: ref,
		status: Pending, createdAt: now, updatedAt: now,
	}
	t.payloadHash = ComputeHash(p)
	return t, nil
}

// NewOpening cria a transação interna de abertura de carteira, já PROCESSED.
// Só faz sentido para saldo inicial positivo.
func NewOpening(id, walletID, playerID uuid.UUID, initial money.Money, now time.Time) (*Transaction, error) {
	if id == uuid.Nil || walletID == uuid.Nil || playerID == uuid.Nil {
		return nil, invalid("OPENING requires id, walletId and playerId")
	}
	if !initial.IsValid() {
		return nil, fmt.Errorf("%w: %w", ErrInvalidTransaction, money.ErrUninitialized)
	}
	if !initial.IsPositive() {
		return nil, invalid("OPENING requires a positive amount")
	}
	if now.IsZero() {
		return nil, invalid("missing timestamp")
	}
	now = now.UTC()
	bal := initial
	return &Transaction{
		id: id, walletID: walletID, playerID: playerID, kind: Opening, amount: initial,
		status: Processed, resultBalance: &bal, createdAt: now, updatedAt: now, completedAt: &now,
	}, nil
}

func invalid(msg string) error { return fmt.Errorf("%w: %s", ErrInvalidTransaction, msg) }

func checkText(name, v string) error {
	if strings.TrimSpace(v) == "" {
		return invalid(name + " is required")
	}
	if v != strings.TrimSpace(v) {
		return invalid(name + " must not have leading or trailing spaces")
	}
	if utf8.RuneCountInString(v) > maxFieldLen || !utf8.ValidString(v) {
		return invalid(name + " is too long or not valid UTF-8")
	}
	for _, r := range v {
		if r < 0x20 || r == 0x7f {
			return invalid(name + " contains control characters")
		}
	}
	return nil
}

// ComputeHash devolve o SHA-256 (hex) do JSON canônico dos campos de negócio.
//
// Entram: providerId, externalTransactionId, playerId, walletId, roundId, gameId,
// kind, money.amount, money.currency e referenceExternalTransactionId (somente
// quando informado). Ficam de fora a chave de idempotência e qualquer
// metadado de transporte. Os UUIDs usam a forma canônica minúscula e o valor
// monetário já é normalizado por money.Parse; json.Marshal ordena as chaves
// dos mapas, então HTTP e SQS produzem o mesmo hash para o mesmo conteúdo.
func ComputeHash(p ExternalParams) string {
	doc := map[string]any{
		"providerId":            p.ProviderID,
		"externalTransactionId": p.ExternalID,
		"playerId":              p.PlayerID.String(),
		"walletId":              p.WalletID.String(),
		"roundId":               p.RoundID,
		"gameId":                p.GameID,
		"kind":                  string(p.Kind),
		"money": map[string]string{
			"amount":   p.Money.Amount(),
			"currency": string(p.Money.Currency()),
		},
	}
	if p.ReferenceExternalID != "" {
		doc["referenceExternalTransactionId"] = p.ReferenceExternalID
	}
	raw, _ := json.Marshal(doc) // map[string]any com valores simples nunca falha
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// Snapshot é o estado persistido usado para reidratar a transação.
type Snapshot struct {
	ID                  uuid.UUID
	ProviderID          string
	ExternalID          string
	IdempotencyKey      string
	PayloadHash         string
	WalletID            uuid.UUID
	PlayerID            uuid.UUID
	RoundID             string
	GameID              string
	Kind                Kind
	Money               money.Money
	ReferenceExternalID string
	ResolvedReferenceID *uuid.UUID
	Status              Status
	FailureCode         FailureCode
	ResultBalance       *money.Money
	Attempts            int
	NextAttemptAt       *time.Time
	ExpiresAt           *time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
	CompletedAt         *time.Time
}

// Rehydrate restaura a transação sem executar transições nem produzir eventos.
func Rehydrate(s Snapshot) (*Transaction, error) {
	t := &Transaction{
		id: s.ID, providerID: s.ProviderID, externalID: s.ExternalID, idempotencyKey: s.IdempotencyKey,
		payloadHash: s.PayloadHash, walletID: s.WalletID, playerID: s.PlayerID, roundID: s.RoundID,
		gameID: s.GameID, kind: s.Kind, amount: s.Money, referenceExternalID: s.ReferenceExternalID,
		resolvedReferenceID: s.ResolvedReferenceID, status: s.Status, failureCode: s.FailureCode,
		resultBalance: s.ResultBalance, attempts: s.Attempts, nextAttemptAt: s.NextAttemptAt,
		expiresAt: s.ExpiresAt, createdAt: s.CreatedAt.UTC(), updatedAt: s.UpdatedAt.UTC(),
		completedAt: s.CompletedAt,
	}
	if t.id == uuid.Nil || t.walletID == uuid.Nil || t.playerID == uuid.Nil || !t.amount.IsValid() {
		return nil, invalid("incomplete snapshot")
	}
	if !t.status.Valid() {
		return nil, invalid("unknown status " + string(t.status))
	}
	switch t.kind {
	case Opening:
		if t.providerID != "" || t.externalID != "" || t.idempotencyKey != "" || t.payloadHash != "" {
			return nil, invalid("OPENING must not carry external metadata")
		}
	case Bet, Win, Loss, Refund, Rollback:
		if t.providerID == "" || t.externalID == "" || t.idempotencyKey == "" || t.payloadHash == "" {
			return nil, invalid("external transaction without provider metadata")
		}
	default:
		return nil, ErrInvalidKind
	}
	if t.status == Rejected || t.status == Failed {
		if !t.failureCode.Valid() {
			return nil, invalid("terminal failure without a valid failure code")
		}
	}
	if t.status == Processed && t.resultBalance == nil {
		return nil, invalid("processed transaction without a result balance")
	}
	return t, nil
}

func (t *Transaction) ID() uuid.UUID                   { return t.id }
func (t *Transaction) ProviderID() string              { return t.providerID }
func (t *Transaction) ExternalID() string              { return t.externalID }
func (t *Transaction) IdempotencyKey() string          { return t.idempotencyKey }
func (t *Transaction) PayloadHash() string             { return t.payloadHash }
func (t *Transaction) WalletID() uuid.UUID             { return t.walletID }
func (t *Transaction) PlayerID() uuid.UUID             { return t.playerID }
func (t *Transaction) RoundID() string                 { return t.roundID }
func (t *Transaction) GameID() string                  { return t.gameID }
func (t *Transaction) Kind() Kind                      { return t.kind }
func (t *Transaction) Money() money.Money              { return t.amount }
func (t *Transaction) ReferenceExternalID() string     { return t.referenceExternalID }
func (t *Transaction) Status() Status                  { return t.status }
func (t *Transaction) FailureCode() FailureCode        { return t.failureCode }
func (t *Transaction) Attempts() int                   { return t.attempts }
func (t *Transaction) CreatedAt() time.Time            { return t.createdAt }
func (t *Transaction) UpdatedAt() time.Time            { return t.updatedAt }
func (t *Transaction) IsInternal() bool                { return t.kind == Opening }
func (t *Transaction) ResolvedReferenceID() *uuid.UUID { return cloneUUID(t.resolvedReferenceID) }
func (t *Transaction) ResultBalance() *money.Money     { return cloneMoney(t.resultBalance) }
func (t *Transaction) NextAttemptAt() *time.Time       { return cloneTime(t.nextAttemptAt) }
func (t *Transaction) ExpiresAt() *time.Time           { return cloneTime(t.expiresAt) }
func (t *Transaction) CompletedAt() *time.Time         { return cloneTime(t.completedAt) }

func cloneUUID(u *uuid.UUID) *uuid.UUID {
	if u == nil {
		return nil
	}
	c := *u
	return &c
}

func cloneMoney(m *money.Money) *money.Money {
	if m == nil {
		return nil
	}
	c := *m
	return &c
}

func cloneTime(v *time.Time) *time.Time {
	if v == nil {
		return nil
	}
	c := *v
	return &c
}

func (t *Transaction) guard() error {
	if t.status.Terminal() {
		return fmt.Errorf("%w: %s", ErrTerminalState, t.status)
	}
	return nil
}

// AwaitReference move PENDING para PENDING_REFERENCE e agenda a primeira nova tentativa.
func (t *Transaction) AwaitReference(now, nextAttemptAt, expiresAt time.Time) error {
	if err := t.guard(); err != nil {
		return err
	}
	if t.status != Pending {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.status, PendingReference)
	}
	if !t.kind.NeedsReference() && t.referenceExternalID == "" {
		return fmt.Errorf("%w: %s has no reference to wait for", ErrInvalidTransition, t.kind)
	}
	n, e := nextAttemptAt.UTC(), expiresAt.UTC()
	t.status = PendingReference
	t.nextAttemptAt = &n
	t.expiresAt = &e
	t.updatedAt = now.UTC()
	return nil
}

// ScheduleRetry registra uma nova tentativa agendada (PENDING ou PENDING_REFERENCE).
func (t *Transaction) ScheduleRetry(now, nextAttemptAt time.Time) error {
	if err := t.guard(); err != nil {
		return err
	}
	n := nextAttemptAt.UTC()
	t.attempts++
	t.nextAttemptAt = &n
	t.updatedAt = now.UTC()
	return nil
}

// ReferenceExpired informa se o prazo de espera pela referência já terminou.
func (t *Transaction) ReferenceExpired(now time.Time) bool {
	return t.status == PendingReference && t.expiresAt != nil && !now.Before(*t.expiresAt)
}

// Process conclui a operação com sucesso, guardando o saldo observado para replays.
func (t *Transaction) Process(balance money.Money, resolvedRef *uuid.UUID, now time.Time) error {
	if err := t.guard(); err != nil {
		return err
	}
	if !balance.IsValid() || balance.IsNegative() || balance.Currency() != t.amount.Currency() {
		return invalid("result balance must be a non-negative amount in the transaction currency")
	}
	if t.kind.IsReversal() && resolvedRef == nil {
		return invalid("reversal needs a resolved reference")
	}
	b := balance
	t.status = Processed
	t.resultBalance = &b
	t.resolvedReferenceID = cloneUUID(resolvedRef)
	t.clearSchedule()
	t.complete(now)
	return nil
}

// Reject encerra a operação por regra de negócio.
func (t *Transaction) Reject(code FailureCode, now time.Time) error {
	if err := t.guard(); err != nil {
		return err
	}
	if !code.Valid() || code == InternalError {
		return invalid("invalid rejection code " + string(code))
	}
	t.status = Rejected
	t.failureCode = code
	t.clearSchedule()
	t.complete(now)
	return nil
}

// Fail encerra a operação por falha permanente de infraestrutura.
func (t *Transaction) Fail(code FailureCode, now time.Time) error {
	if err := t.guard(); err != nil {
		return err
	}
	if !code.Valid() {
		return invalid("invalid failure code " + string(code))
	}
	t.status = Failed
	t.failureCode = code
	t.clearSchedule()
	t.complete(now)
	return nil
}

func (t *Transaction) clearSchedule() {
	t.nextAttemptAt = nil
	t.expiresAt = nil
}

func (t *Transaction) complete(now time.Time) {
	n := now.UTC()
	t.updatedAt = n
	t.completedAt = &n
}
