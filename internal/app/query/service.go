// Package query reúne os casos de uso de leitura e a reconciliação.
package query

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/google/uuid"

	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/domain/money"
	"github.com/leandropeloso/wager-service/internal/domain/wager"
	"github.com/leandropeloso/wager-service/internal/domain/wallet"
)

const (
	DefaultLimit = 50
	MaxLimit     = 200
)

var (
	ErrNotFound      = errors.New("not found")
	ErrInvalidCursor = errors.New("invalid cursor")
	ErrInvalidLimit  = errors.New("invalid limit")
)

type Service struct {
	reader  port.Reader
	metrics port.Metrics
	log     *slog.Logger
}

func NewService(reader port.Reader, metrics port.Metrics, log *slog.Logger) *Service {
	return &Service{reader: reader, metrics: metrics, log: log}
}

func (s *Service) Wallet(ctx context.Context, id uuid.UUID) (*wallet.Wallet, error) {
	w, err := s.reader.GetWallet(ctx, id)
	return w, mapNotFound(err)
}

func (s *Service) Transaction(ctx context.Context, id uuid.UUID) (*wager.Transaction, error) {
	t, err := s.reader.GetTransaction(ctx, id)
	return t, mapNotFound(err)
}

func (s *Service) TransactionByProvider(ctx context.Context, providerID, externalID string) (*wager.Transaction, error) {
	t, err := s.reader.GetTransactionByProvider(ctx, providerID, externalID)
	return t, mapNotFound(err)
}

type LedgerPage struct {
	Items      []port.LedgerItem
	NextCursor string
}

// Ledger devolve uma página ordenada por versão da carteira. O cursor é opaco
// (base64 de {"v": última versão}) e a ordenação é estável porque
// (wallet_id, wallet_version) é único.
func (s *Service) Ledger(ctx context.Context, walletID uuid.UUID, cursor string, limit int) (LedgerPage, error) {
	if limit == 0 {
		limit = DefaultLimit
	}
	if limit < 0 || limit > MaxLimit {
		return LedgerPage{}, ErrInvalidLimit
	}
	after, err := decodeCursor(cursor)
	if err != nil {
		return LedgerPage{}, err
	}
	if _, err := s.reader.GetWallet(ctx, walletID); err != nil {
		return LedgerPage{}, mapNotFound(err)
	}
	items, err := s.reader.ListLedger(ctx, walletID, after, limit+1)
	if err != nil {
		return LedgerPage{}, err
	}
	page := LedgerPage{Items: items}
	if len(items) > limit {
		page.Items = items[:limit]
		page.NextCursor = encodeCursor(items[limit-1].WalletVersion)
	}
	return page, nil
}

type cursorPayload struct {
	V int64 `json:"v"`
}

func encodeCursor(version int64) string {
	raw, _ := json.Marshal(cursorPayload{V: version})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeCursor(c string) (int64, error) {
	if c == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return 0, ErrInvalidCursor
	}
	var p cursorPayload
	if err := json.Unmarshal(raw, &p); err != nil || p.V < 0 {
		return 0, ErrInvalidCursor
	}
	return p.V, nil
}

// TrialBalance devolve o balancete do livro-diário: por moeda, total de débitos
// e créditos (iguais em um diário íntegro).
func (s *Service) TrialBalance(ctx context.Context) ([]port.TrialLine, error) {
	lines, err := s.reader.TrialBalance(ctx)
	if err != nil {
		return nil, err
	}
	for _, l := range lines {
		if !l.Balanced {
			s.metrics.ReconciliationDivergence()
			s.log.ErrorContext(ctx, "journal trial balance divergence",
				"currency", l.Currency, "debits", l.Debits, "credits", l.Credits)
		}
	}
	return lines, nil
}

type ReconciliationResult struct {
	WalletID   uuid.UUID
	Stored     money.Money
	Calculated money.Money
	Difference money.Money
	Consistent bool
	Entries    int64
}

// Reconcile reconstrói o saldo a partir do ledger e compara com o armazenado.
// Não altera nada; divergências viram log e métrica.
func (s *Service) Reconcile(ctx context.Context, walletID uuid.UUID) (ReconciliationResult, error) {
	rec, err := s.reader.Reconcile(ctx, walletID)
	if err != nil {
		return ReconciliationResult{}, mapNotFound(err)
	}
	diff, err := rec.Stored.Sub(rec.Calculated)
	if err != nil {
		return ReconciliationResult{}, err
	}
	res := ReconciliationResult{
		WalletID: walletID, Stored: rec.Stored, Calculated: rec.Calculated,
		Difference: diff, Consistent: diff.IsZero(), Entries: rec.Entries,
	}
	if !res.Consistent {
		s.metrics.ReconciliationDivergence()
		s.log.ErrorContext(ctx, "wallet reconciliation divergence",
			"walletId", walletID, "stored", rec.Stored.Amount(), "calculated", rec.Calculated.Amount(),
			"difference", diff.Amount())
	}
	return res, nil
}

func mapNotFound(err error) error {
	if errors.Is(err, port.ErrNotFound) {
		return ErrNotFound
	}
	return err
}
