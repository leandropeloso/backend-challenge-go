package processwager

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/leandropeloso/wager-service/internal/domain/wager"
)

func TestBackoffGrowsExponentiallyAndIsCapped(t *testing.T) {
	s := &Service{cfg: Config{PendingBaseBackoff: 2 * time.Second, PendingMaxBackoff: 30 * time.Second}}
	want := []time.Duration{2, 4, 8, 16, 30, 30, 30}
	for i, w := range want {
		if got := s.backoff(i); got != w*time.Second {
			t.Errorf("backoff(%d) = %s, quer %s", i, got, w*time.Second)
		}
	}
	if got := s.backoff(10_000); got != 30*time.Second {
		t.Errorf("tentativas enormes devem respeitar o teto: %s", got)
	}
}

func TestParseUUIDIsStrict(t *testing.T) {
	ok := "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"
	if _, err := parseUUID(ok); err != nil {
		t.Fatalf("canônico: %v", err)
	}
	for _, bad := range []string{
		"", "nil", "00000000-0000-0000-0000-000000000000",
		"{0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1}",
		"urn:uuid:0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
		"0192f28f5dc07d58bdb2814ad6a0f4a1",
		" 0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
	} {
		if _, err := parseUUID(bad); err == nil {
			t.Errorf("%q deveria ser rejeitado", bad)
		}
	}
}

func cmd() Command {
	return Command{
		ProviderID: "provider-a", ExternalTransactionID: "t1", IdempotencyKey: "provider-a:t1",
		PlayerID: uuid.NewString(), WalletID: uuid.NewString(), RoundID: "r", GameID: "g",
		Kind: "BET", Amount: "25.00", Currency: "BRL",
	}
}

func TestBuildReportsTheOffendingField(t *testing.T) {
	s := &Service{}
	now := time.Now()
	cases := map[string]struct {
		mutate func(*Command)
		field  string
	}{
		"tipo":          {func(c *Command) { c.Kind = "OPENING" }, "kind"},
		"moeda":         {func(c *Command) { c.Currency = "brl" }, "money.currency"},
		"valor":         {func(c *Command) { c.Amount = "25.5" }, "money.amount"},
		"jogador":       {func(c *Command) { c.PlayerID = "x" }, "playerId"},
		"carteira":      {func(c *Command) { c.WalletID = "" }, "walletId"},
		"regra de tipo": {func(c *Command) { c.Kind = "REFUND" }, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := cmd()
			tc.mutate(&c)
			_, err := s.build(c, uuid.New(), now)
			var invalid *InvalidInputError
			if !errors.As(err, &invalid) {
				t.Fatalf("esperava InvalidInputError, veio %v", err)
			}
			if invalid.Field != tc.field {
				t.Fatalf("campo = %q, quer %q", invalid.Field, tc.field)
			}
		})
	}
	if _, err := s.build(cmd(), uuid.New(), now); err != nil {
		t.Fatalf("comando válido: %v", err)
	}
}

func TestSameContentGivesSameHashAcrossTransports(t *testing.T) {
	s := &Service{}
	now := time.Now()
	c := cmd()
	a, err := s.build(c, uuid.New(), now)
	if err != nil {
		t.Fatal(err)
	}
	// "SQS": mesmo conteúdo, outra origem, outro id interno, outra chave de entrega
	c2 := c
	c2.Source, c2.CorrelationID = "sqs", "msg-1"
	c2.Inbox = &Inbox{Consumer: "x", MessageID: "msg-1"}
	b, _ := s.build(c2, uuid.New(), now.Add(time.Hour))
	if a.PayloadHash() != b.PayloadHash() {
		t.Fatal("HTTP e SQS devem produzir o mesmo hash para o mesmo conteúdo")
	}
	// UUID em maiúsculas é a mesma operação
	c3 := c
	c3.PlayerID = upper(c.PlayerID)
	d, err := s.build(c3, uuid.New(), now)
	if err != nil {
		t.Fatal(err)
	}
	if d.PayloadHash() != a.PayloadHash() {
		t.Fatal("UUID em maiúsculas deve normalizar para a mesma forma canônica")
	}
	// a chave de idempotência entra no hash da inbox, mas não no hash do payload
	e := c
	e.IdempotencyKey = "outra"
	f, _ := s.build(e, uuid.New(), now)
	if f.PayloadHash() != a.PayloadHash() || inboxHash(f) == inboxHash(a) {
		t.Fatal("a chave não entra no hash do payload, mas diferencia o hash da inbox")
	}
	_ = wager.Bet
}

func upper(s string) string {
	b := []byte(s)
	for i, c := range b {
		if c >= 'a' && c <= 'f' {
			b[i] = c - 32
		}
	}
	return string(b)
}
