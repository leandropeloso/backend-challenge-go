package httpapi

import (
	"time"

	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/domain/money"
	"github.com/leandropeloso/wager-service/internal/domain/wager"
	"github.com/leandropeloso/wager-service/internal/domain/wallet"
)

// Os valores monetários trafegam sempre como strings decimais.
type moneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func toMoney(m money.Money) moneyDTO {
	return moneyDTO{Amount: m.Amount(), Currency: string(m.Currency())}
}

func moneyPtr(m *money.Money) *moneyDTO {
	if m == nil {
		return nil
	}
	d := toMoney(*m)
	return &d
}

type openWalletRequest struct {
	PlayerID       string   `json:"playerId"`
	InitialBalance moneyDTO `json:"initialBalance"`
}

type walletResponse struct {
	ID        string   `json:"id"`
	PlayerID  string   `json:"playerId"`
	Balance   moneyDTO `json:"balance"`
	Version   int64    `json:"version"`
	CreatedAt string   `json:"createdAt"`
	UpdatedAt string   `json:"updatedAt"`
}

func toWallet(w *wallet.Wallet) walletResponse {
	return walletResponse{
		ID: w.ID().String(), PlayerID: w.PlayerID().String(), Balance: toMoney(w.Balance()), Version: w.Version(),
		CreatedAt: ts(w.CreatedAt()), UpdatedAt: ts(w.UpdatedAt()),
	}
}

type submitRequest struct {
	ProviderID                     string   `json:"providerId"`
	ExternalTransactionID          string   `json:"externalTransactionId"`
	PlayerID                       string   `json:"playerId"`
	WalletID                       string   `json:"walletId"`
	RoundID                        string   `json:"roundId"`
	GameID                         string   `json:"gameId"`
	Kind                           string   `json:"kind"`
	Money                          moneyDTO `json:"money"`
	ReferenceExternalTransactionID string   `json:"referenceExternalTransactionId"`
}

type submitResponse struct {
	TransactionID    string    `json:"transactionId"`
	Status           string    `json:"status"`
	Balance          *moneyDTO `json:"balance,omitempty"`
	FailureCode      string    `json:"failureCode,omitempty"`
	Correctable      *bool     `json:"correctable,omitempty"`
	IdempotentReplay bool      `json:"idempotentReplay"`
}

type transactionResponse struct {
	TransactionID                  string    `json:"transactionId"`
	Kind                           string    `json:"kind"`
	Status                         string    `json:"status"`
	ProviderID                     string    `json:"providerId,omitempty"`
	ExternalTransactionID          string    `json:"externalTransactionId,omitempty"`
	WalletID                       string    `json:"walletId"`
	PlayerID                       string    `json:"playerId"`
	RoundID                        string    `json:"roundId,omitempty"`
	GameID                         string    `json:"gameId,omitempty"`
	Money                          moneyDTO  `json:"money"`
	ReferenceExternalTransactionID string    `json:"referenceExternalTransactionId,omitempty"`
	Balance                        *moneyDTO `json:"balance,omitempty"`
	FailureCode                    string    `json:"failureCode,omitempty"`
	Correctable                    *bool     `json:"correctable,omitempty"`
	Attempts                       int       `json:"attempts"`
	NextAttemptAt                  string    `json:"nextAttemptAt,omitempty"`
	ExpiresAt                      string    `json:"expiresAt,omitempty"`
	CreatedAt                      string    `json:"createdAt"`
	UpdatedAt                      string    `json:"updatedAt"`
	CompletedAt                    string    `json:"completedAt,omitempty"`
}

func toTransaction(t *wager.Transaction) transactionResponse {
	r := transactionResponse{
		TransactionID: t.ID().String(), Kind: string(t.Kind()), Status: string(t.Status()),
		ProviderID: t.ProviderID(), ExternalTransactionID: t.ExternalID(), WalletID: t.WalletID().String(),
		PlayerID: t.PlayerID().String(), RoundID: t.RoundID(), GameID: t.GameID(), Money: toMoney(t.Money()),
		ReferenceExternalTransactionID: t.ReferenceExternalID(), Balance: moneyPtr(t.ResultBalance()),
		FailureCode: string(t.FailureCode()), Attempts: t.Attempts(),
		CreatedAt: ts(t.CreatedAt()), UpdatedAt: ts(t.UpdatedAt()),
	}
	if t.FailureCode() != "" && t.Status() == wager.Rejected {
		c := t.FailureCode().Correctable()
		r.Correctable = &c
	}
	if v := t.NextAttemptAt(); v != nil {
		r.NextAttemptAt = ts(*v)
	}
	if v := t.ExpiresAt(); v != nil {
		r.ExpiresAt = ts(*v)
	}
	if v := t.CompletedAt(); v != nil {
		r.CompletedAt = ts(*v)
	}
	return r
}

type ledgerItemResponse struct {
	ID            string   `json:"id"`
	WalletID      string   `json:"walletId"`
	TransactionID string   `json:"transactionId"`
	Direction     string   `json:"direction"`
	Money         moneyDTO `json:"money"`
	BalanceBefore moneyDTO `json:"balanceBefore"`
	BalanceAfter  moneyDTO `json:"balanceAfter"`
	WalletVersion int64    `json:"walletVersion"`
	CreatedAt     string   `json:"createdAt"`
}

type ledgerPageResponse struct {
	Items      []ledgerItemResponse `json:"items"`
	NextCursor *string              `json:"nextCursor"`
}

func toLedgerItem(i port.LedgerItem) ledgerItemResponse {
	e := i.Entry
	return ledgerItemResponse{
		ID: e.ID().String(), WalletID: e.WalletID().String(), TransactionID: e.TransactionID().String(),
		Direction: string(e.Direction()), Money: toMoney(e.Amount()), BalanceBefore: toMoney(e.BalanceBefore()),
		BalanceAfter: toMoney(e.BalanceAfter()), WalletVersion: i.WalletVersion, CreatedAt: ts(e.CreatedAt()),
	}
}

type reconciliationResponse struct {
	WalletID          string   `json:"walletId"`
	StoredBalance     moneyDTO `json:"storedBalance"`
	CalculatedBalance moneyDTO `json:"calculatedBalance"`
	Difference        moneyDTO `json:"difference"`
	Consistent        bool     `json:"consistent"`
	CheckedEntries    int64    `json:"checkedEntries"`
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

type trialLineResponse struct {
	Currency string `json:"currency"`
	Debits   string `json:"debits"`
	Credits  string `json:"credits"`
	Entries  int64  `json:"entries"`
	Balanced bool   `json:"balanced"`
}

type trialBalanceResponse struct {
	Balanced bool                `json:"balanced"`
	Lines    []trialLineResponse `json:"lines"`
}
