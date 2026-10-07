package wager

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/leandropeloso/wager-service/internal/domain/money"
)

func brl(t *testing.T, minor int64) money.Money {
	t.Helper()
	m, err := money.FromMinor(minor, money.BRL)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func params(t *testing.T, kind Kind, minor int64) ExternalParams {
	t.Helper()
	p := ExternalParams{
		ID:             uuid.New(),
		ProviderID:     "provider-a",
		ExternalID:     "tx-1",
		IdempotencyKey: "provider-a:tx-1",
		PlayerID:       uuid.New(),
		WalletID:       uuid.New(),
		RoundID:        "round-1",
		GameID:         "game-1",
		Kind:           kind,
		Money:          brl(t, minor),
		Now:            time.Now(),
	}
	if kind.NeedsReference() {
		p.ReferenceExternalID = "tx-0"
	}
	return p
}

func TestNewExternalMoneyPolicy(t *testing.T) {
	ok := []struct {
		kind  Kind
		minor int64
	}{{Bet, 100}, {Win, 100}, {Loss, 0}, {Refund, 100}, {Rollback, 100}}
	for _, c := range ok {
		if _, err := NewExternal(params(t, c.kind, c.minor)); err != nil {
			t.Errorf("%s com %d: %v", c.kind, c.minor, err)
		}
	}
	bad := []struct {
		kind  Kind
		minor int64
	}{{Bet, 0}, {Win, 0}, {Refund, 0}, {Rollback, 0}, {Loss, 1}, {Bet, -100}}
	for _, c := range bad {
		if _, err := NewExternal(params(t, c.kind, c.minor)); !errors.Is(err, ErrInvalidTransaction) {
			t.Errorf("%s com %d deveria ser inválido: %v", c.kind, c.minor, err)
		}
	}
}

func TestNewExternalRejectsOpeningAndUnknownKinds(t *testing.T) {
	if _, err := NewExternal(params(t, Opening, 100)); !errors.Is(err, ErrKindNotExternal) {
		t.Errorf("OPENING: %v", err)
	}
	if _, err := NewExternal(params(t, Kind("JACKPOT"), 100)); !errors.Is(err, ErrInvalidKind) {
		t.Errorf("tipo desconhecido: %v", err)
	}
	if _, err := ParseExternalKind("OPENING"); !errors.Is(err, ErrKindNotExternal) {
		t.Errorf("parse OPENING: %v", err)
	}
}

func TestNewExternalReferenceRules(t *testing.T) {
	p := params(t, Refund, 100)
	p.ReferenceExternalID = ""
	if _, err := NewExternal(p); !errors.Is(err, ErrInvalidTransaction) {
		t.Errorf("refund sem referência: %v", err)
	}
	p = params(t, Rollback, 100)
	p.ReferenceExternalID = p.ExternalID
	if _, err := NewExternal(p); !errors.Is(err, ErrInvalidTransaction) {
		t.Errorf("auto-referência: %v", err)
	}
	p = params(t, Bet, 100)
	p.ReferenceExternalID = "tx-0"
	if _, err := NewExternal(p); !errors.Is(err, ErrInvalidTransaction) {
		t.Errorf("bet com referência: %v", err)
	}
	p = params(t, Win, 100)
	p.ReferenceExternalID = "bet-1"
	if _, err := NewExternal(p); err != nil {
		t.Errorf("win com referência opcional: %v", err)
	}
}

func TestNewExternalRequiredFields(t *testing.T) {
	mutations := map[string]func(*ExternalParams){
		"provider":    func(p *ExternalParams) { p.ProviderID = "" },
		"external":    func(p *ExternalParams) { p.ExternalID = "  " },
		"key":         func(p *ExternalParams) { p.IdempotencyKey = "" },
		"round":       func(p *ExternalParams) { p.RoundID = "" },
		"game":        func(p *ExternalParams) { p.GameID = "" },
		"player":      func(p *ExternalParams) { p.PlayerID = uuid.Nil },
		"wallet":      func(p *ExternalParams) { p.WalletID = uuid.Nil },
		"id":          func(p *ExternalParams) { p.ID = uuid.Nil },
		"money":       func(p *ExternalParams) { p.Money = money.Money{} },
		"espaço":      func(p *ExternalParams) { p.GameID = " game" },
		"controle":    func(p *ExternalParams) { p.GameID = "a\nb" },
		"muito longo": func(p *ExternalParams) { p.RoundID = string(make([]byte, 300)) },
	}
	for name, mut := range mutations {
		p := params(t, Bet, 100)
		mut(&p)
		if _, err := NewExternal(p); !errors.Is(err, ErrInvalidTransaction) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestHashIsStableAndIgnoresKey(t *testing.T) {
	a := params(t, Bet, 2500)
	b := a
	b.ID = uuid.New()
	b.IdempotencyKey = "outra-chave"
	b.Now = a.Now.Add(time.Hour)
	if ComputeHash(a) != ComputeHash(b) {
		t.Fatal("hash não deve depender de id, chave nem timestamp")
	}
	txa, _ := NewExternal(a)
	if txa.PayloadHash() != ComputeHash(a) {
		t.Fatal("hash da transação difere do calculado")
	}

	changes := map[string]func(*ExternalParams){
		"valor":    func(p *ExternalParams) { p.Money = brl(t, 2501) },
		"moeda":    func(p *ExternalParams) { p.Money, _ = money.FromMinor(2500, "USD") },
		"tipo":     func(p *ExternalParams) { p.Kind = Win },
		"rodada":   func(p *ExternalParams) { p.RoundID = "round-2" },
		"jogo":     func(p *ExternalParams) { p.GameID = "game-2" },
		"jogador":  func(p *ExternalParams) { p.PlayerID = uuid.New() },
		"carteira": func(p *ExternalParams) { p.WalletID = uuid.New() },
		"externo":  func(p *ExternalParams) { p.ExternalID = "tx-2" },
		"provedor": func(p *ExternalParams) { p.ProviderID = "provider-b" },
		"ref":      func(p *ExternalParams) { p.ReferenceExternalID = "x" },
	}
	for name, mut := range changes {
		c := a
		mut(&c)
		if ComputeHash(c) == ComputeHash(a) {
			t.Errorf("mudança em %s não alterou o hash", name)
		}
	}
}

func TestNewOpening(t *testing.T) {
	now := time.Now()
	tx, err := NewOpening(uuid.New(), uuid.New(), uuid.New(), brl(t, 100000), now)
	if err != nil {
		t.Fatal(err)
	}
	if tx.Kind() != Opening || tx.Status() != Processed || !tx.IsInternal() {
		t.Fatalf("opening inesperada: %s %s", tx.Kind(), tx.Status())
	}
	if tx.ProviderID() != "" || tx.ExternalID() != "" || tx.IdempotencyKey() != "" || tx.PayloadHash() != "" || tx.RoundID() != "" || tx.GameID() != "" {
		t.Fatal("opening não deve carregar metadados externos")
	}
	if tx.ResultBalance() == nil || tx.ResultBalance().Minor() != 100000 {
		t.Fatal("saldo resultante da abertura")
	}
	if _, err := NewOpening(uuid.New(), uuid.New(), uuid.New(), brl(t, 0), now); !errors.Is(err, ErrInvalidTransaction) {
		t.Errorf("abertura zerada não gera OPENING: %v", err)
	}
	if err := tx.Reject(InsufficientFunds, now); !errors.Is(err, ErrTerminalState) {
		t.Errorf("opening é terminal: %v", err)
	}
}

func TestStateMachine(t *testing.T) {
	now := time.Now()
	later := now.Add(time.Minute)

	t.Run("pending para processed", func(t *testing.T) {
		tx, _ := NewExternal(params(t, Bet, 100))
		if tx.Status() != Pending {
			t.Fatal("deve iniciar em PENDING")
		}
		if err := tx.Process(brl(t, 900), nil, now); err != nil {
			t.Fatal(err)
		}
		if tx.Status() != Processed || tx.ResultBalance().Minor() != 900 || tx.CompletedAt() == nil {
			t.Fatalf("estado: %s", tx.Status())
		}
		for name, err := range map[string]error{
			"process": tx.Process(brl(t, 1), nil, now),
			"reject":  tx.Reject(InsufficientFunds, now),
			"fail":    tx.Fail(InternalError, now),
			"retry":   tx.ScheduleRetry(now, later),
			"await":   tx.AwaitReference(now, later, later),
		} {
			if !errors.Is(err, ErrTerminalState) {
				t.Errorf("%s após terminal: %v", name, err)
			}
		}
	})

	t.Run("espera de referência", func(t *testing.T) {
		tx, _ := NewExternal(params(t, Refund, 100))
		if err := tx.AwaitReference(now, later, now.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if tx.Status() != PendingReference || tx.NextAttemptAt() == nil || tx.ExpiresAt() == nil {
			t.Fatal("PENDING_REFERENCE sem agenda")
		}
		if err := tx.AwaitReference(now, later, later); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("await duplo: %v", err)
		}
		if err := tx.ScheduleRetry(later, later.Add(time.Minute)); err != nil || tx.Attempts() != 1 {
			t.Fatalf("retry: %v attempts=%d", err, tx.Attempts())
		}
		if tx.ReferenceExpired(now) || !tx.ReferenceExpired(now.Add(2*time.Hour)) {
			t.Error("expiração da referência")
		}
		ref := uuid.New()
		if err := tx.Process(brl(t, 1000), &ref, later); err != nil {
			t.Fatal(err)
		}
		if tx.NextAttemptAt() != nil || tx.ExpiresAt() != nil || tx.ResolvedReferenceID() == nil {
			t.Error("conclusão deve limpar agenda e guardar a referência")
		}
	})

	t.Run("reversão exige referência resolvida", func(t *testing.T) {
		tx, _ := NewExternal(params(t, Rollback, 100))
		if err := tx.Process(brl(t, 1), nil, now); !errors.Is(err, ErrInvalidTransaction) {
			t.Errorf("process sem ref: %v", err)
		}
	})

	t.Run("bet não espera referência", func(t *testing.T) {
		tx, _ := NewExternal(params(t, Bet, 100))
		if err := tx.AwaitReference(now, later, later); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("bet await: %v", err)
		}
	})

	t.Run("rejeição e falha", func(t *testing.T) {
		tx, _ := NewExternal(params(t, Bet, 100))
		if err := tx.Reject(InternalError, now); !errors.Is(err, ErrInvalidTransaction) {
			t.Errorf("INTERNAL_ERROR não é rejeição: %v", err)
		}
		if err := tx.Reject(FailureCode("XYZ"), now); !errors.Is(err, ErrInvalidTransaction) {
			t.Errorf("código desconhecido: %v", err)
		}
		if err := tx.Reject(InsufficientFunds, now); err != nil {
			t.Fatal(err)
		}
		if tx.Status() != Rejected || tx.FailureCode() != InsufficientFunds {
			t.Fatal("rejeição não registrada")
		}
		if err := tx.Fail(InternalError, now); !errors.Is(err, ErrTerminalState) {
			t.Errorf("fail após rejected: %v", err)
		}

		tx2, _ := NewExternal(params(t, Bet, 100))
		if err := tx2.Fail(InternalError, now); err != nil || tx2.Status() != Failed {
			t.Fatalf("fail: %v", err)
		}
	})

	t.Run("saldo resultante inválido", func(t *testing.T) {
		tx, _ := NewExternal(params(t, Bet, 100))
		usd, _ := money.FromMinor(1, "USD")
		if err := tx.Process(usd, nil, now); !errors.Is(err, ErrInvalidTransaction) {
			t.Errorf("moeda: %v", err)
		}
		if err := tx.Process(brl(t, -1), nil, now); !errors.Is(err, ErrInvalidTransaction) {
			t.Errorf("negativo: %v", err)
		}
		if tx.Status() != Pending {
			t.Fatal("falha de validação não pode mudar o estado")
		}
	})
}

func TestFailureCodeClassification(t *testing.T) {
	if InsufficientFunds == InsufficientFundsForReversal {
		t.Fatal("códigos de saldo devem ser distintos")
	}
	if !WalletNotFound.Correctable() || !ReferenceMismatch.Correctable() {
		t.Error("deveriam ser corrigíveis")
	}
	if InsufficientFunds.Correctable() || ReferenceNotFound.Correctable() || AlreadyReversed.Correctable() {
		t.Error("deveriam ser definitivos")
	}
}

func TestReversalTargets(t *testing.T) {
	if !Bet.CanBeReversedBy(Refund) || Win.CanBeReversedBy(Refund) {
		t.Error("REFUND só reverte BET")
	}
	for _, k := range []Kind{Bet, Win, Refund} {
		if !k.CanBeReversedBy(Rollback) {
			t.Errorf("ROLLBACK deve reverter %s", k)
		}
	}
	for _, k := range []Kind{Loss, Rollback, Opening} {
		if k.CanBeReversedBy(Rollback) {
			t.Errorf("ROLLBACK não deve reverter %s", k)
		}
	}
}

func TestRehydrateKeepsState(t *testing.T) {
	tx, _ := NewExternal(params(t, Bet, 100))
	_ = tx.Process(brl(t, 900), nil, time.Now())
	snap := Snapshot{
		ID: tx.ID(), ProviderID: tx.ProviderID(), ExternalID: tx.ExternalID(), IdempotencyKey: tx.IdempotencyKey(),
		PayloadHash: tx.PayloadHash(), WalletID: tx.WalletID(), PlayerID: tx.PlayerID(), RoundID: tx.RoundID(),
		GameID: tx.GameID(), Kind: tx.Kind(), Money: tx.Money(), Status: tx.Status(),
		ResultBalance: tx.ResultBalance(), CreatedAt: tx.CreatedAt(), UpdatedAt: tx.UpdatedAt(), CompletedAt: tx.CompletedAt(),
	}
	got, err := Rehydrate(snap)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status() != Processed || got.ResultBalance().Minor() != 900 {
		t.Fatal("estado não preservado")
	}
	snap.Status = Rejected
	snap.FailureCode = ""
	if _, err := Rehydrate(snap); !errors.Is(err, ErrInvalidTransaction) {
		t.Errorf("rejeitada sem código: %v", err)
	}
	snap.Status = Processed
	snap.ResultBalance = nil
	if _, err := Rehydrate(snap); !errors.Is(err, ErrInvalidTransaction) {
		t.Errorf("processada sem saldo: %v", err)
	}
	snap.Status = Status("???")
	if _, err := Rehydrate(snap); !errors.Is(err, ErrInvalidTransaction) {
		t.Errorf("status desconhecido: %v", err)
	}
}
