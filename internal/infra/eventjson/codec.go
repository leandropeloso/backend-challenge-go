// Package eventjson serializa eventos de domínio no contrato JSON publicado.
package eventjson

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/leandropeloso/wager-service/internal/domain/event"
	"github.com/leandropeloso/wager-service/internal/domain/money"
)

type moneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func toMoney(m money.Money) moneyDTO {
	return moneyDTO{Amount: m.Amount(), Currency: string(m.Currency())}
}

type envelope struct {
	EventID       string `json:"eventId"`
	EventType     string `json:"eventType"`
	AggregateID   string `json:"aggregateId"`
	CorrelationID string `json:"correlationId"`
	CausationID   string `json:"causationId,omitempty"`
	OccurredAt    string `json:"occurredAt"`
	Version       int    `json:"version"`
	Data          any    `json:"data"`
}

type transactionData struct {
	TransactionID         string   `json:"transactionId"`
	WalletID              string   `json:"walletId"`
	PlayerID              string   `json:"playerId"`
	Kind                  string   `json:"kind"`
	ProviderID            string   `json:"providerId,omitempty"`
	ExternalTransactionID string   `json:"externalTransactionId,omitempty"`
	RoundID               string   `json:"roundId,omitempty"`
	GameID                string   `json:"gameId,omitempty"`
	Money                 moneyDTO `json:"money"`
}

func toTransaction(i event.TransactionInfo) transactionData {
	return transactionData{
		TransactionID: i.TransactionID.String(), WalletID: i.WalletID.String(), PlayerID: i.PlayerID.String(),
		Kind: string(i.Kind), ProviderID: i.ProviderID, ExternalTransactionID: i.ExternalTransactionID,
		RoundID: i.RoundID, GameID: i.GameID, Money: toMoney(i.Money),
	}
}

type processedData struct {
	transactionData
	Balance moneyDTO `json:"balanceAfter"`
}

type rejectedData struct {
	transactionData
	FailureCode string `json:"failureCode"`
	Correctable bool   `json:"correctable"`
}

type pendingReferenceData struct {
	transactionData
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId"`
	NextAttemptAt                  string `json:"nextAttemptAt"`
	ExpiresAt                      string `json:"expiresAt"`
}

type balanceChangedData struct {
	WalletID      string   `json:"walletId"`
	TransactionID string   `json:"transactionId"`
	Direction     string   `json:"direction"`
	Money         moneyDTO `json:"money"`
	BalanceBefore moneyDTO `json:"balanceBefore"`
	BalanceAfter  moneyDTO `json:"balanceAfter"`
	WalletVersion int64    `json:"walletVersion"`
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// Marshal devolve o envelope completo, que é guardado na outbox e publicado sem alterações.
func Marshal(e event.Event) ([]byte, error) {
	var data any
	switch d := e.Data().(type) {
	case event.WagerTransactionProcessed:
		data = processedData{toTransaction(d.TransactionInfo), toMoney(d.BalanceAfter)}
	case event.WagerTransactionRejected:
		data = rejectedData{toTransaction(d.TransactionInfo), string(d.FailureCode), d.Correctable}
	case event.WagerTransactionPendingReference:
		data = pendingReferenceData{
			toTransaction(d.TransactionInfo), d.ReferenceExternalTransactionID, ts(d.NextAttemptAt), ts(d.ExpiresAt),
		}
	case event.WalletBalanceChanged:
		data = balanceChangedData{
			WalletID: d.WalletID.String(), TransactionID: d.TransactionID.String(), Direction: string(d.Direction),
			Money: toMoney(d.Money), BalanceBefore: toMoney(d.BalanceBefore), BalanceAfter: toMoney(d.BalanceAfter),
			WalletVersion: d.WalletVersion,
		}
	default:
		return nil, fmt.Errorf("eventjson: unsupported event data %T", d)
	}
	return json.Marshal(envelope{
		EventID: e.ID().String(), EventType: string(e.Type()), AggregateID: e.AggregateID().String(),
		CorrelationID: e.CorrelationID(), CausationID: e.CausationID(), OccurredAt: ts(e.OccurredAt()),
		Version: e.Version(), Data: data,
	})
}
