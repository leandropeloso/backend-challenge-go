package eventjson

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/leandropeloso/wager-service/internal/domain/event"
	"github.com/leandropeloso/wager-service/internal/domain/ledger"
	"github.com/leandropeloso/wager-service/internal/domain/money"
	"github.com/leandropeloso/wager-service/internal/domain/wager"
	"github.com/leandropeloso/wager-service/internal/domain/wallet"
)

func brl(t *testing.T, minor int64) money.Money {
	t.Helper()
	m, err := money.FromMinor(minor, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func decode(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("json inválido: %v\n%s", err, raw)
	}
	return out
}

func TestBalanceChangedContract(t *testing.T) {
	w, _ := wallet.Open(uuid.New(), uuid.New(), brl(t, 10000), time.Now())
	mv, err := w.Debit(brl(t, 2500), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	txID := uuid.New()
	ev, err := event.NewWalletBalanceChanged(event.Meta{
		EventID: uuid.New(), CorrelationID: "corr", CausationID: "cause", Now: time.Now(),
	}, w.ID(), txID, mv)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	doc := decode(t, raw)
	for _, k := range []string{"eventId", "eventType", "aggregateId", "correlationId", "causationId", "occurredAt", "version", "data"} {
		if _, ok := doc[k]; !ok {
			t.Errorf("envelope sem %s", k)
		}
	}
	if doc["eventType"] != "WalletBalanceChanged" || doc["version"].(float64) != 1 {
		t.Errorf("tipo/versão: %v %v", doc["eventType"], doc["version"])
	}
	if _, err := time.Parse(time.RFC3339Nano, doc["occurredAt"].(string)); err != nil || !strings.HasSuffix(doc["occurredAt"].(string), "Z") {
		t.Errorf("occurredAt deve ser RFC 3339 em UTC: %v", doc["occurredAt"])
	}
	data := doc["data"].(map[string]any)
	if data["direction"] != string(ledger.Debit) || data["walletVersion"].(float64) != 2 {
		t.Errorf("data: %v", data)
	}
	if data["money"].(map[string]any)["amount"] != "25.00" || data["balanceBefore"].(map[string]any)["amount"] != "100.00" ||
		data["balanceAfter"].(map[string]any)["amount"] != "75.00" {
		t.Errorf("valores monetários devem ser strings decimais: %v", data)
	}
	if data["walletId"] != w.ID().String() || data["transactionId"] != txID.String() {
		t.Errorf("identificadores: %v", data)
	}
}

func TestDefaultsAndMoneyNeverFloat(t *testing.T) {
	// nenhum número JSON deve representar dinheiro
	w, _ := wallet.Open(uuid.New(), uuid.New(), brl(t, 100), time.Now())
	mv, _ := w.Credit(brl(t, 1), time.Now())
	ev, _ := event.NewWalletBalanceChanged(event.Meta{EventID: uuid.New(), CorrelationID: "c", Now: time.Now()}, w.ID(), uuid.New(), mv)
	raw, _ := Marshal(ev)
	if strings.Contains(string(raw), `"amount":1`) || strings.Contains(string(raw), `"amount":0`) {
		t.Fatalf("amount não pode ser número: %s", raw)
	}
	if doc := decode(t, raw); doc["causationId"] != nil {
		t.Errorf("causationId vazio deve ser omitido: %v", doc["causationId"])
	}
}

func TestOpeningEventOmitsExternalMetadata(t *testing.T) {
	opening, err := wager.NewOpening(uuid.New(), uuid.New(), uuid.New(), brl(t, 100000), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ev, err := event.NewWagerTransactionProcessed(event.Meta{EventID: uuid.New(), CorrelationID: "c", Now: time.Now()}, opening)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := Marshal(ev)
	data := decode(t, raw)["data"].(map[string]any)
	for _, k := range []string{"providerId", "externalTransactionId", "roundId", "gameId"} {
		if _, ok := data[k]; ok {
			t.Errorf("evento de abertura não deve ter %s", k)
		}
	}
	if data["kind"] != "OPENING" || data["balanceAfter"].(map[string]any)["amount"] != "1000.00" {
		t.Errorf("data: %v", data)
	}
}
