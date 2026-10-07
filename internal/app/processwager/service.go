// Package processwager implementa o caso de uso único de processamento de
// operações financeiras, compartilhado pela API HTTP e pelo consumidor SQS.
package processwager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

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

var (
	// ErrIdempotencyConflict: a chave de idempotência já foi usada com outro conteúdo.
	ErrIdempotencyConflict = errors.New("idempotency key reused with different content")
	// ErrExternalIDConflict: (providerId, externalTransactionId) já existe com outra chave.
	ErrExternalIDConflict = errors.New("external transaction already recorded under a different idempotency key")
	// ErrInboxConflict: o messageId voltou com conteúdo diferente.
	ErrInboxConflict = errors.New("message id redelivered with different content")
)

// InvalidInputError descreve uma entrada rejeitada antes de qualquer efeito.
type InvalidInputError struct {
	Field  string
	Reason string
	Err    error
}

func (e *InvalidInputError) Error() string {
	if e.Field == "" {
		return "invalid input: " + e.Reason
	}
	return fmt.Sprintf("invalid input: %s: %s", e.Field, e.Reason)
}

func (e *InvalidInputError) Unwrap() error { return e.Err }

type Config struct {
	PendingBaseBackoff time.Duration
	PendingMaxBackoff  time.Duration
	PendingTTL         time.Duration
	PendingMaxAttempts int
	MaxTxRetries       int
}

// Inbox identifica a mensagem de entrada, quando a origem é uma fila.
type Inbox struct {
	Consumer  string
	MessageID string
}

// Command carrega os dados crus da operação; a validação acontece aqui.
type Command struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           string
	Amount                         string
	Currency                       string
	ReferenceExternalTransactionID string
	CorrelationID                  string
	Source                         string
	Inbox                          *Inbox
}

type Outcome struct {
	TransactionID uuid.UUID
	Status        wager.Status
	Balance       *money.Money
	FailureCode   wager.FailureCode
	Correctable   bool
	Replay        bool
}

type Service struct {
	uow     port.UnitOfWork
	clock   port.Clock
	ids     port.IDGenerator
	metrics port.Metrics
	log     *slog.Logger
	cfg     Config
}

func NewService(uow port.UnitOfWork, clock port.Clock, ids port.IDGenerator, metrics port.Metrics, log *slog.Logger, cfg Config) *Service {
	if cfg.MaxTxRetries <= 0 {
		cfg.MaxTxRetries = 4
	}
	return &Service{uow: uow, clock: clock, ids: ids, metrics: metrics, log: log, cfg: cfg}
}

// Execute registra e processa a operação. É seguro chamar várias vezes, de
// várias instâncias, com o mesmo conteúdo: só a primeira aplica o efeito.
func (s *Service) Execute(ctx context.Context, cmd Command) (out Outcome, err error) {
	ctx, span := telemetry.Start(ctx, "wager.execute",
		attribute.String("wager.provider_id", cmd.ProviderID), attribute.String("wager.kind", cmd.Kind),
		attribute.String("wager.source", sourceOf(cmd)), attribute.String("wager.wallet_id", cmd.WalletID),
		attribute.String("wager.external_transaction_id", cmd.ExternalTransactionID))
	defer func() {
		if err == nil {
			span.SetAttributes(attribute.String("wager.status", string(out.Status)), attribute.Bool("wager.replay", out.Replay),
				attribute.String("wager.failure_code", string(out.FailureCode)))
		}
		telemetry.Fail(span, err)
		span.End()
	}()
	started := s.clock.Now()
	txID := s.ids.New()
	if _, err := s.build(cmd, txID, started); err != nil {
		return Outcome{}, err
	}

	err = s.retrying(ctx, func(ctx context.Context) error {
		return s.uow.Do(ctx, func(ctx context.Context, r port.Repositories) error {
			t, err := s.build(cmd, txID, s.clock.Now())
			if err != nil {
				return err
			}
			out, err = s.executeIn(ctx, r, cmd, t)
			return err
		})
	})
	if err != nil {
		return Outcome{}, err
	}
	if out.Replay {
		s.metrics.Duplicate(sourceOf(cmd))
	} else {
		s.recordResult(cmd, out)
	}
	s.metrics.ProcessingDuration(sourceOf(cmd), s.clock.Now().Sub(started))
	return out, nil
}

func sourceOf(cmd Command) string {
	if cmd.Source == "" {
		return "unknown"
	}
	return cmd.Source
}

func (s *Service) recordResult(cmd Command, out Outcome) {
	kind, _ := wager.ParseExternalKind(cmd.Kind)
	s.metrics.TransactionResult(kind, out.Status, out.FailureCode)
}

func (s *Service) retrying(ctx context.Context, fn func(context.Context) error) error {
	var err error
	for attempt := 0; attempt < s.cfg.MaxTxRetries; attempt++ {
		if err = fn(ctx); err == nil || !errors.Is(err, port.ErrRetryable) {
			return err
		}
		s.metrics.ConcurrencyConflict()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 10 * time.Millisecond):
		}
	}
	return err
}

func (s *Service) build(cmd Command, id uuid.UUID, now time.Time) (*wager.Transaction, error) {
	kind, err := wager.ParseExternalKind(cmd.Kind)
	if err != nil {
		return nil, &InvalidInputError{Field: "kind", Reason: err.Error(), Err: err}
	}
	cur, err := money.ParseCurrency(cmd.Currency)
	if err != nil {
		return nil, &InvalidInputError{Field: "money.currency", Reason: err.Error(), Err: err}
	}
	amount, err := money.Parse(cmd.Amount, cur)
	if err != nil {
		return nil, &InvalidInputError{Field: "money.amount", Reason: err.Error(), Err: err}
	}
	playerID, err := parseUUID(cmd.PlayerID)
	if err != nil {
		return nil, &InvalidInputError{Field: "playerId", Reason: "must be a UUID", Err: err}
	}
	walletID, err := parseUUID(cmd.WalletID)
	if err != nil {
		return nil, &InvalidInputError{Field: "walletId", Reason: "must be a UUID", Err: err}
	}
	t, err := wager.NewExternal(wager.ExternalParams{
		ID: id, ProviderID: cmd.ProviderID, ExternalID: cmd.ExternalTransactionID,
		IdempotencyKey: cmd.IdempotencyKey, PlayerID: playerID, WalletID: walletID,
		RoundID: cmd.RoundID, GameID: cmd.GameID, Kind: kind, Money: amount,
		ReferenceExternalID: cmd.ReferenceExternalTransactionID, Now: now,
	})
	if err != nil {
		return nil, &InvalidInputError{Reason: err.Error(), Err: err}
	}
	return t, nil
}

// parseUUID só aceita a forma canônica de 36 caracteres.
func parseUUID(v string) (uuid.UUID, error) {
	if len(v) != 36 {
		return uuid.Nil, errors.New("not a canonical uuid")
	}
	id, err := uuid.Parse(v)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, errors.New("not a valid uuid")
	}
	return id, nil
}

// evctx agrupa os identificadores de rastreio dos eventos produzidos.
type evctx struct {
	correlation string
	causation   string
}

func (s *Service) meta(now time.Time, ec evctx) event.Meta {
	return event.Meta{EventID: s.ids.New(), CorrelationID: ec.correlation, CausationID: ec.causation, Now: now}
}

func (s *Service) executeIn(ctx context.Context, r port.Repositories, cmd Command, t *wager.Transaction) (Outcome, error) {
	now := s.clock.Now()
	ec := evctx{correlation: cmd.CorrelationID, causation: t.IdempotencyKey()}
	if ec.correlation == "" {
		ec.correlation = t.ID().String()
	}
	if cmd.Inbox != nil {
		ec.causation = cmd.Inbox.MessageID
		res, err := r.Inbox().Begin(ctx, cmd.Inbox.Consumer, cmd.Inbox.MessageID, inboxHash(t), now)
		if err != nil {
			return Outcome{}, err
		}
		switch res {
		case port.InboxHashMismatch:
			return Outcome{}, ErrInboxConflict
		case port.InboxDuplicate:
			existing, err := r.Transactions().GetByProviderKey(ctx, t.ProviderID(), t.IdempotencyKey())
			if err != nil {
				if errors.Is(err, port.ErrNotFound) {
					return Outcome{}, ErrInboxConflict
				}
				return Outcome{}, err
			}
			return replayOf(existing), nil
		}
	}

	inserted, err := r.Transactions().InsertIfAbsent(ctx, t)
	if err != nil {
		return Outcome{}, err
	}
	var out Outcome
	if inserted {
		out, err = s.settle(ctx, r, t, now, ec)
	} else {
		out, err = s.existing(ctx, r, t)
	}
	if err != nil {
		return Outcome{}, err
	}
	if cmd.Inbox != nil {
		if err := r.Inbox().Complete(ctx, cmd.Inbox.Consumer, cmd.Inbox.MessageID, now); err != nil {
			return Outcome{}, err
		}
	}
	return out, nil
}

// existing trata um conflito de unicidade: ou é um replay legítimo ou um conflito.
func (s *Service) existing(ctx context.Context, r port.Repositories, t *wager.Transaction) (Outcome, error) {
	byKey, err := r.Transactions().GetByProviderKey(ctx, t.ProviderID(), t.IdempotencyKey())
	switch {
	case err == nil:
		if byKey.PayloadHash() != t.PayloadHash() {
			return Outcome{}, ErrIdempotencyConflict
		}
		return replayOf(byKey), nil
	case !errors.Is(err, port.ErrNotFound):
		return Outcome{}, err
	}
	_, err = r.Transactions().GetByProviderExternalID(ctx, t.ProviderID(), t.ExternalID())
	if err == nil {
		return Outcome{}, ErrExternalIDConflict
	}
	if errors.Is(err, port.ErrNotFound) {
		// O vencedor da disputa desfez a transação; o chamador pode repetir.
		return Outcome{}, fmt.Errorf("%w: unique conflict vanished", port.ErrRetryable)
	}
	return Outcome{}, err
}

func replayOf(t *wager.Transaction) Outcome {
	return Outcome{
		TransactionID: t.ID(), Status: t.Status(), Balance: t.ResultBalance(),
		FailureCode: t.FailureCode(), Correctable: t.FailureCode().Correctable(), Replay: true,
	}
}

func inboxHash(t *wager.Transaction) string {
	sum := sha256.Sum256([]byte(t.PayloadHash() + "|" + t.IdempotencyKey()))
	return hex.EncodeToString(sum[:])
}

// ResumeOne retoma uma transação pendente vencida (PENDING ou
// PENDING_REFERENCE). Devolve false quando não há nada vencido.
func (s *Service) ResumeOne(ctx context.Context) (bool, error) {
	var (
		found     bool
		done      *wager.Transaction
		out       Outcome
		claimedID uuid.UUID
	)
	err := s.retrying(ctx, func(ctx context.Context) error {
		found, done, claimedID = false, nil, uuid.Nil
		return s.uow.Do(ctx, func(ctx context.Context, r port.Repositories) error {
			now := s.clock.Now()
			t, err := r.Transactions().ClaimDue(ctx, now)
			if errors.Is(err, port.ErrNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			found, claimedID = true, t.ID()
			ctx, span := telemetry.Start(ctx, "wager.resume", attribute.String("wager.transaction_id", t.ID().String()),
				attribute.String("wager.kind", string(t.Kind())), attribute.Int("wager.attempts", t.Attempts()))
			defer span.End()
			out, err = s.settle(ctx, r, t, now, evctx{correlation: t.ID().String(), causation: t.IdempotencyKey()})
			if err != nil {
				return err
			}
			done = t
			return nil
		})
	})
	if err != nil {
		// Uma transação que falha de forma persistente não pode monopolizar o worker
		// (ela seria sempre a mais antiga): recebe backoff e, esgotadas as tentativas,
		// vira FAILED auditável. Falhas de infraestrutura (banco/contexto) não contam.
		if claimedID != uuid.Nil && !isInfrastructure(err) {
			if perr := s.parkPoison(ctx, claimedID, err); perr != nil {
				return false, fmt.Errorf("resume %s: %w (parking it also failed: %v)", claimedID, err, perr)
			}
			return true, nil
		}
		return false, err
	}
	if found && done != nil && done.Status().Terminal() {
		s.metrics.TransactionResult(done.Kind(), out.Status, out.FailureCode)
	}
	return found, nil
}

func isInfrastructure(err error) bool {
	return errors.Is(err, port.ErrTransient) || errors.Is(err, port.ErrRetryable) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// parkPoison registra uma falha persistente ao retomar a transação: agenda nova
// tentativa com backoff exponencial ou, no limite, a encerra como FAILED.
func (s *Service) parkPoison(ctx context.Context, id uuid.UUID, cause error) error {
	var failed bool
	err := s.retrying(ctx, func(ctx context.Context) error {
		return s.uow.Do(ctx, func(ctx context.Context, r port.Repositories) error {
			now := s.clock.Now()
			t, err := r.Transactions().GetByID(ctx, id)
			if err != nil {
				return err
			}
			if t.Status().Terminal() {
				return nil
			}
			failed = t.Attempts()+1 >= s.cfg.PendingMaxAttempts
			if failed {
				if err := t.Fail(wager.InternalError, now); err != nil {
					return err
				}
			} else if err := t.ScheduleRetry(now, now.Add(s.backoff(t.Attempts()+1))); err != nil {
				return err
			}
			return r.Transactions().Update(ctx, t)
		})
	})
	if err != nil {
		return err
	}
	level := slog.LevelWarn
	if failed {
		level = slog.LevelError
	}
	s.log.Log(ctx, level, "pending transaction keeps failing; parked", "transactionId", id, "permanentlyFailed", failed, "error", cause)
	s.metrics.Retry("pending-resume")
	return nil
}

// settle decide o destino da transação com a carteira travada e persiste o resultado.
func (s *Service) settle(ctx context.Context, r port.Repositories, t *wager.Transaction, now time.Time, ec evctx) (Outcome, error) {
	w, err := r.Wallets().GetForUpdate(ctx, t.WalletID())
	if errors.Is(err, port.ErrNotFound) {
		return s.reject(ctx, r, t, wager.WalletNotFound, now, ec)
	}
	if err != nil {
		return Outcome{}, err
	}
	if w.PlayerID() != t.PlayerID() {
		return s.reject(ctx, r, t, wager.PlayerMismatch, now, ec)
	}
	if w.Currency() != t.Money().Currency() {
		return s.reject(ctx, r, t, wager.CurrencyMismatch, now, ec)
	}

	var ref *wager.Transaction
	if t.ReferenceExternalID() != "" {
		d, err := s.checkReference(ctx, r, t, w)
		if err != nil {
			return Outcome{}, err
		}
		switch {
		case d.wait:
			return s.wait(ctx, r, t, d.exists, now, ec)
		case d.reject != "":
			return s.reject(ctx, r, t, d.reject, now, ec)
		}
		ref = d.ref
	}

	var (
		mv     wallet.Movement
		moved  bool
		reject wager.FailureCode
	)
	switch t.Kind() {
	case wager.Bet:
		mv, err = w.Debit(t.Money(), now)
		moved, reject = true, wager.InsufficientFunds
	case wager.Win, wager.Refund:
		mv, err = w.Credit(t.Money(), now)
		moved = true
	case wager.Loss:
		// sem movimentação, ledger nem mudança de versão
	case wager.Rollback:
		moved = true
		if ref.Kind() == wager.Bet {
			mv, err = w.Credit(t.Money(), now)
		} else {
			mv, err = w.Debit(t.Money(), now)
			reject = wager.InsufficientFundsForReversal
		}
	default:
		return Outcome{}, fmt.Errorf("unsupported kind %s", t.Kind())
	}
	if err != nil {
		switch {
		case errors.Is(err, wallet.ErrInsufficientFunds) && reject != "":
			return s.reject(ctx, r, t, reject, now, ec)
		case errors.Is(err, money.ErrOverflow), errors.Is(err, wallet.ErrVersionOverflow):
			return s.fail(ctx, r, t, now, err)
		}
		return Outcome{}, err
	}
	return s.finish(ctx, r, t, w, ref, moved, mv, now, ec)
}

func (s *Service) finish(ctx context.Context, r port.Repositories, t *wager.Transaction, w *wallet.Wallet,
	ref *wager.Transaction, moved bool, mv wallet.Movement, now time.Time, ec evctx) (Outcome, error) {

	var refID *uuid.UUID
	if ref != nil {
		id := ref.ID()
		refID = &id
	}
	if err := t.Process(w.Balance(), refID, now); err != nil {
		return Outcome{}, err
	}
	if moved {
		entry, err := ledger.New(s.ids.New(), w.ID(), t.ID(), mv.Direction, mv.Amount, mv.BalanceBefore, mv.BalanceAfter, now)
		if err != nil {
			return Outcome{}, err
		}
		if err := r.Ledger().Append(ctx, entry, mv.Version); err != nil {
			return Outcome{}, err
		}
		je, err := journal.ForMovement(s.ids.New(), t, mv.Direction, mv.Amount, now)
		if err != nil {
			return Outcome{}, err
		}
		if err := r.Journal().Append(ctx, je); err != nil {
			return Outcome{}, err
		}
		if err := r.Wallets().Save(ctx, w, mv.Version-1); err != nil {
			return Outcome{}, err
		}
	}
	if err := r.Transactions().Update(ctx, t); err != nil {
		return Outcome{}, err
	}
	processed, err := event.NewWagerTransactionProcessed(s.meta(now, ec), t)
	if err != nil {
		return Outcome{}, err
	}
	if err := r.Outbox().Add(ctx, processed); err != nil {
		return Outcome{}, err
	}
	if moved {
		changed, err := event.NewWalletBalanceChanged(s.meta(now, ec), w.ID(), t.ID(), mv)
		if err != nil {
			return Outcome{}, err
		}
		if err := r.Outbox().Add(ctx, changed); err != nil {
			return Outcome{}, err
		}
	}
	return outcomeOf(t), nil
}

func outcomeOf(t *wager.Transaction) Outcome {
	return Outcome{
		TransactionID: t.ID(), Status: t.Status(), Balance: t.ResultBalance(),
		FailureCode: t.FailureCode(), Correctable: t.FailureCode().Correctable(),
	}
}

func (s *Service) reject(ctx context.Context, r port.Repositories, t *wager.Transaction, code wager.FailureCode, now time.Time, ec evctx) (Outcome, error) {
	if err := t.Reject(code, now); err != nil {
		return Outcome{}, err
	}
	if err := r.Transactions().Update(ctx, t); err != nil {
		return Outcome{}, err
	}
	ev, err := event.NewWagerTransactionRejected(s.meta(now, ec), t)
	if err != nil {
		return Outcome{}, err
	}
	if err := r.Outbox().Add(ctx, ev); err != nil {
		return Outcome{}, err
	}
	return outcomeOf(t), nil
}

// fail registra uma falha permanente que nenhuma repetição resolve (ex.: overflow).
func (s *Service) fail(ctx context.Context, r port.Repositories, t *wager.Transaction, now time.Time, cause error) (Outcome, error) {
	s.log.Error("permanent processing failure", "transactionId", t.ID(), "walletId", t.WalletID(), "error", cause)
	if err := t.Fail(wager.InternalError, now); err != nil {
		return Outcome{}, err
	}
	if err := r.Transactions().Update(ctx, t); err != nil {
		return Outcome{}, err
	}
	return outcomeOf(t), nil
}

type refDecision struct {
	ref    *wager.Transaction
	wait   bool
	exists bool
	reject wager.FailureCode
}

// checkReference resolve a referência por (providerId, referenceExternalTransactionId).
// A carteira já está travada, então uma aposta da mesma carteira não muda durante a análise.
func (s *Service) checkReference(ctx context.Context, r port.Repositories, t *wager.Transaction, w *wallet.Wallet) (refDecision, error) {
	ref, err := r.Transactions().GetByProviderExternalID(ctx, t.ProviderID(), t.ReferenceExternalID())
	if errors.Is(err, port.ErrNotFound) {
		return refDecision{wait: true}, nil
	}
	if err != nil {
		return refDecision{}, err
	}
	switch ref.Status() {
	case wager.Pending, wager.PendingReference:
		return refDecision{wait: true, exists: true}, nil
	case wager.Rejected, wager.Failed:
		return refDecision{reject: wager.ReferenceNotProcessed}, nil
	}

	if ref.PlayerID() != t.PlayerID() || ref.WalletID() != t.WalletID() ||
		ref.Money().Currency() != t.Money().Currency() || ref.RoundID() != t.RoundID() {
		return refDecision{reject: wager.ReferenceMismatch}, nil
	}
	switch t.Kind() {
	case wager.Refund, wager.Rollback:
		if !ref.Kind().CanBeReversedBy(t.Kind()) {
			return refDecision{reject: wager.ReferenceKindNotAllowed}, nil
		}
		if !ref.Money().Equal(t.Money()) {
			return refDecision{reject: wager.ReferenceAmountMismatch}, nil
		}
		reversed, err := r.Transactions().HasProcessedReversal(ctx, ref.ID())
		if err != nil {
			return refDecision{}, err
		}
		if reversed {
			return refDecision{reject: wager.AlreadyReversed}, nil
		}
	default: // WIN e LOSS só podem apontar para uma aposta
		if ref.Kind() != wager.Bet {
			return refDecision{reject: wager.ReferenceKindNotAllowed}, nil
		}
	}
	return refDecision{ref: ref, exists: true}, nil
}

// wait coloca a transação em espera pela referência, ou a rejeita quando o prazo acaba.
func (s *Service) wait(ctx context.Context, r port.Repositories, t *wager.Transaction, exists bool, now time.Time, ec evctx) (Outcome, error) {
	if t.Status() == wager.Pending {
		next := now.Add(s.backoff(0))
		if err := t.AwaitReference(now, next, t.CreatedAt().Add(s.cfg.PendingTTL)); err != nil {
			return Outcome{}, err
		}
		if err := r.Transactions().Update(ctx, t); err != nil {
			return Outcome{}, err
		}
		ev, err := event.NewWagerTransactionPendingReference(s.meta(now, ec), t)
		if err != nil {
			return Outcome{}, err
		}
		if err := r.Outbox().Add(ctx, ev); err != nil {
			return Outcome{}, err
		}
		return outcomeOf(t), nil
	}

	if t.ReferenceExpired(now) || t.Attempts()+1 >= s.cfg.PendingMaxAttempts {
		code := wager.ReferenceNotFound
		if exists {
			code = wager.ReferenceNotProcessed
		}
		return s.reject(ctx, r, t, code, now, ec)
	}
	if err := t.ScheduleRetry(now, now.Add(s.backoff(t.Attempts()+1))); err != nil {
		return Outcome{}, err
	}
	if err := r.Transactions().Update(ctx, t); err != nil {
		return Outcome{}, err
	}
	s.metrics.Retry("pending-reference")
	return outcomeOf(t), nil
}

// backoff cresce exponencialmente a partir de PendingBaseBackoff até PendingMaxBackoff.
func (s *Service) backoff(attempt int) time.Duration {
	d := s.cfg.PendingBaseBackoff
	for i := 0; i < attempt && d < s.cfg.PendingMaxBackoff; i++ {
		d *= 2
	}
	if d > s.cfg.PendingMaxBackoff {
		d = s.cfg.PendingMaxBackoff
	}
	return d
}
