// Package journal modela o livro-diário de partidas dobradas: cada movimento
// financeiro vira um lançamento com pelo menos dois postings cujos débitos e
// créditos somam o mesmo valor. A carteira do jogador é uma conta de passivo
// (credit-normal): um crédito na carteira aumenta o saldo do jogador.
package journal

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/leandropeloso/wager-service/internal/domain/ledger"
	"github.com/leandropeloso/wager-service/internal/domain/money"
	"github.com/leandropeloso/wager-service/internal/domain/wager"
)

var (
	ErrInvalidEntry = errors.New("journal: invalid entry")
	ErrUnbalanced   = errors.New("journal: debits and credits do not balance")
)

// Account identifica uma conta contábil.
type Account string

const OpeningFunding Account = "funding:opening"

func WalletAccount(walletID uuid.UUID) Account { return Account("wallet:" + walletID.String()) }

// HouseAccount é a conta da operadora para o provedor (contrapartida das apostas e prêmios).
func HouseAccount(providerID string) Account { return Account("house:" + providerID) }

func (a Account) IsWallet() bool { return len(a) > 7 && a[:7] == "wallet:" }

type Posting struct {
	Account   Account
	Direction ledger.Direction
	Amount    money.Money
}

type Entry struct {
	id            uuid.UUID
	transactionID uuid.UUID
	postings      []Posting
	createdAt     time.Time
}

// New valida o lançamento: ao menos dois postings, valores positivos, uma só
// moeda, débitos = créditos.
func New(id, transactionID uuid.UUID, postings []Posting, now time.Time) (*Entry, error) {
	if id == uuid.Nil || transactionID == uuid.Nil || now.IsZero() {
		return nil, fmt.Errorf("%w: missing identifiers or timestamp", ErrInvalidEntry)
	}
	if len(postings) < 2 {
		return nil, fmt.Errorf("%w: at least two postings are required", ErrInvalidEntry)
	}
	var debits, credits money.Money
	for i, p := range postings {
		if p.Account == "" || !p.Direction.Valid() || !p.Amount.IsValid() || !p.Amount.IsPositive() {
			return nil, fmt.Errorf("%w: posting %d is incomplete or not positive", ErrInvalidEntry, i)
		}
		if i == 0 {
			debits, _ = money.Zero(p.Amount.Currency())
			credits = debits
		}
		var err error
		if p.Direction == ledger.Debit {
			debits, err = debits.Add(p.Amount)
		} else {
			credits, err = credits.Add(p.Amount)
		}
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidEntry, err)
		}
	}
	if !debits.Equal(credits) {
		return nil, ErrUnbalanced
	}
	return &Entry{id: id, transactionID: transactionID, postings: append([]Posting(nil), postings...), createdAt: now.UTC()}, nil
}

func (e *Entry) ID() uuid.UUID            { return e.id }
func (e *Entry) TransactionID() uuid.UUID { return e.transactionID }
func (e *Entry) CreatedAt() time.Time     { return e.createdAt }
func (e *Entry) Postings() []Posting      { return append([]Posting(nil), e.postings...) }

// ForMovement monta o lançamento de um movimento de carteira: a carteira recebe
// o posting na mesma direção do seu ledger e a contrapartida, na oposta.
func ForMovement(id uuid.UUID, t *wager.Transaction, walletDirection ledger.Direction, amount money.Money, now time.Time) (*Entry, error) {
	counter := OpeningFunding
	if !t.IsInternal() {
		counter = HouseAccount(t.ProviderID())
	}
	opposite := ledger.Credit
	if walletDirection == ledger.Credit {
		opposite = ledger.Debit
	}
	return New(id, t.ID(), []Posting{
		{Account: WalletAccount(t.WalletID()), Direction: walletDirection, Amount: amount},
		{Account: counter, Direction: opposite, Amount: amount},
	}, now)
}
