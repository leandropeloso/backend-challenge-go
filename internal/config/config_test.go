package config

import (
	"strings"
	"testing"
	"time"
)

func setBase(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost:5432/db")
	t.Setenv("AUTH_ISSUER", "http://localhost:8080/realms/wager")
	t.Setenv("AUTH_JWKS_URL", "http://keycloak:8080/realms/wager/protocol/openid-connect/certs")
}

func TestLoadDefaults(t *testing.T) {
	setBase(t)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.HTTPAddr != ":8080" || c.ShutdownTimeout != 30*time.Second || !c.RunConsumer || !c.RunOutbox {
		t.Fatalf("padrões inesperados: %+v", c)
	}
	if c.Consumer.ProcessTimeout >= c.Consumer.VisibilityTimeout {
		t.Fatal("o timeout de processamento deve ser menor que a visibilidade da mensagem")
	}
}

func TestLoadRequiresMandatoryVariables(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("AUTH_ISSUER", "")
	t.Setenv("AUTH_JWKS_URL", "")
	_, err := Load()
	if err == nil {
		t.Fatal("esperava erro")
	}
	for _, name := range []string{"DATABASE_URL", "AUTH_ISSUER", "AUTH_JWKS_URL"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("erro deveria citar %s: %v", name, err)
		}
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	cases := map[string]string{
		"CONSUMER_CONCURRENCY":     "zero",
		"OUTBOX_POLL_INTERVAL":     "-5s",
		"RUN_HTTP":                 "talvez",
		"SQS_WAGER_QUEUE":          "wager-transactions",
		"PENDING_MAX_ATTEMPTS":     "0",
		"CONSUMER_PROCESS_TIMEOUT": "45s",
		"AWS_ENDPOINT_URL":         "::::",
	}
	for key, value := range cases {
		t.Run(key, func(t *testing.T) {
			setBase(t)
			t.Setenv(key, value)
			if _, err := Load(); err == nil {
				t.Fatalf("%s=%q deveria ser rejeitado", key, value)
			}
		})
	}
}

func TestLoadRequiresCredentialsTogether(t *testing.T) {
	setBase(t)
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	if _, err := Load(); err == nil {
		t.Fatal("chave sem segredo deveria falhar")
	}
}
