package journal

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/leandropeloso/wager-service/internal/domain/ledger"
	"github.com/leandropeloso/wager-service/internal/domain/money"
	"github.com/leandropeloso/wager-service/internal/domain/wager"
)

func brl(t *testing.T, minor int64) money.Money {
	t.Helper()
	m, err := money.FromMinor(minor, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestNewRequiresBalancedPostings(t *testing.T) {
	id, tx, now := uuid.New(), uuid.New(), time.Now()
	ok := []Posting{
		{Account: "wallet:a", Direction: ledger.Credit, Amount: brl(t, 500)},
		{Account: "house:p", Direction: ledger.Debit, Amount: brl(t, 500)},
	}
	if _, err := New(id, tx, ok, now); err != nil {
		t.Fatalf("lançamento equilibrado: %v", err)
	}
	// três postings que equilibram
	if _, err := New(id, tx, []Posting{
		{Account: "wallet:a", Direction: ledger.Credit, Amount: brl(t, 500)},
		{Account: "house:p", Direction: ledger.Debit, Amount: brl(t, 300)},
		{Account: "fee:x", Direction: ledger.Debit, Amount: brl(t, 200)},
	}, now); err != nil {
		t.Fatalf("três postings: %v", err)
	}
	cases := map[string]struct {
		postings []Posting
		want     error
	}{
		"desequilibrado": {[]Posting{{Account: "a", Direction: ledger.Credit, Amount: brl(t, 500)}, {Account: "b", Direction: ledger.Debit, Amount: brl(t, 499)}}, ErrUnbalanced},
		"só um posting":  {[]Posting{{Account: "a", Direction: ledger.Credit, Amount: brl(t, 500)}}, ErrInvalidEntry},
		"valor zero":     {[]Posting{{Account: "a", Direction: ledger.Credit, Amount: brl(t, 0)}, {Account: "b", Direction: ledger.Debit, Amount: brl(t, 0)}}, ErrInvalidEntry},
		"sem conta":      {[]Posting{{Account: "", Direction: ledger.Credit, Amount: brl(t, 5)}, {Account: "b", Direction: ledger.Debit, Amount: brl(t, 5)}}, ErrInvalidEntry},
		"direção":        {[]Posting{{Account: "a", Direction: "X", Amount: brl(t, 5)}, {Account: "b", Direction: ledger.Debit, Amount: brl(t, 5)}}, ErrInvalidEntry},
	}
	for name, c := range cases {
		if _, err := New(id, tx, c.postings, now); !errors.Is(err, c.want) {
			t.Errorf("%s: %v", name, err)
		}
	}
	usd, _ := money.FromMinor(500, "USD")
	if _, err := New(id, tx, []Posting{
		{Account: "a", Direction: ledger.Credit, Amount: brl(t, 500)},
		{Account: "b", Direction: ledger.Debit, Amount: usd},
	}, now); err == nil {
		t.Error("moedas diferentes não podem equilibrar")
	}
	if _, err := New(id, tx, []Posting{
		{Account: "a", Direction: ledger.Credit, Amount: brl(t, math.MaxInt64)},
		{Account: "b", Direction: ledger.Credit, Amount: brl(t, 1)},
		{Account: "c", Direction: ledger.Debit, Amount: brl(t, 1)},
	}, now); !errors.Is(err, ErrInvalidEntry) {
		t.Errorf("overflow da soma: %v", err)
	}
}

func TestForMovementMirrorsTheWalletLedger(t *testing.T) {
	walletID, player := uuid.New(), uuid.New()
	bet, err := wager.NewExternal(wager.ExternalParams{
		ID: uuid.New(), ProviderID: "provider-a", ExternalID: "e", IdempotencyKey: "k", PlayerID: player,
		WalletID: walletID, RoundID: "r", GameID: "g", Kind: wager.Bet, Money: brl(t, 2500), Now: time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	// BET debita a carteira: a casa é creditada
	e, err := ForMovement(uuid.New(), bet, ledger.Debit, brl(t, 2500), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	p := e.Postings()
	if p[0].Account != WalletAccount(walletID) || p[0].Direction != ledger.Debit ||
		p[1].Account != HouseAccount("provider-a") || p[1].Direction != ledger.Credit {
		t.Fatalf("postings: %+v", p)
	}
	// WIN credita a carteira: a casa é debitada
	e, _ = ForMovement(uuid.New(), bet, ledger.Credit, brl(t, 100), time.Now())
	if q := e.Postings(); q[0].Direction != ledger.Credit || q[1].Direction != ledger.Debit {
		t.Fatalf("postings: %+v", q)
	}
	// abertura usa a conta de fundos
	opening, _ := wager.NewOpening(uuid.New(), walletID, player, brl(t, 10000), time.Now())
	e, _ = ForMovement(uuid.New(), opening, ledger.Credit, brl(t, 10000), time.Now())
	if q := e.Postings(); q[1].Account != OpeningFunding || q[1].Direction != ledger.Debit {
		t.Fatalf("abertura: %+v", q)
	}
	if !WalletAccount(walletID).IsWallet() || HouseAccount("p").IsWallet() {
		t.Error("IsWallet")
	}
}
