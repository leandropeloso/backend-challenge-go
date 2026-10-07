package httpapi

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/leandropeloso/wager-service/internal/infra/auth"
)

// fakeValidator só interpreta tokens do formato "provider:<id>", "internal" ou "none".
// Os serviços ficam nulos de propósito: nenhuma requisição negada pode alcançá-los.
type fakeValidator struct{}

func (fakeValidator) Validate(raw string) (auth.Principal, error) {
	switch {
	case raw == "internal":
		return auth.NewPrincipal("svc", "", false, true), nil
	case strings.HasPrefix(raw, "provider:"):
		return auth.NewPrincipal("svc", strings.TrimPrefix(raw, "provider:"), true, false), nil
	}
	return auth.Principal{}, errors.New("bad token")
}

func newTestHandler() http.Handler {
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return New(nil, nil, nil, fakeValidator{}, nil, nil, log).Handler()
}

func do(h http.Handler, method, path, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestBusinessEndpointsRequireAuthentication(t *testing.T) {
	h := newTestHandler()
	routes := []struct{ method, path string }{
		{"POST", "/wallets"},
		{"GET", "/wallets/0192f291-27dd-7d3f-8071-5f8685deef37"},
		{"GET", "/wallets/0192f291-27dd-7d3f-8071-5f8685deef37/ledger"},
		{"POST", "/wallets/0192f291-27dd-7d3f-8071-5f8685deef37/reconciliation"},
		{"POST", "/wagering/transactions"},
		{"GET", "/wagering/transactions/0192f291-27dd-7d3f-8071-5f8685deef37"},
		{"GET", "/providers/provider-a/wagering/transactions/t1"},
	}
	for _, r := range routes {
		for name, token := range map[string]string{"sem token": "", "token inválido": "garbage"} {
			rec := do(h, r.method, r.path, token, "{}")
			if rec.Code != http.StatusUnauthorized {
				t.Errorf("%s %s (%s): status %d", r.method, r.path, name, rec.Code)
			}
		}
	}
	// esquema diferente de Bearer
	req := httptest.NewRequest("GET", "/wallets/0192f291-27dd-7d3f-8071-5f8685deef37", nil)
	req.Header.Set("Authorization", "Basic abc")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("Basic: status %d", rec.Code)
	}
}

func TestProviderCannotUseWalletEndpoints(t *testing.T) {
	h := newTestHandler()
	for _, r := range []struct{ method, path string }{
		{"POST", "/wallets"},
		{"GET", "/wallets/0192f291-27dd-7d3f-8071-5f8685deef37"},
		{"GET", "/wallets/0192f291-27dd-7d3f-8071-5f8685deef37/ledger"},
		{"POST", "/wallets/0192f291-27dd-7d3f-8071-5f8685deef37/reconciliation"},
	} {
		if rec := do(h, r.method, r.path, "provider:provider-a", "{}"); rec.Code != http.StatusForbidden {
			t.Errorf("%s %s: status %d", r.method, r.path, rec.Code)
		}
	}
}

func TestInternalCannotSubmitProviderOperations(t *testing.T) {
	h := newTestHandler()
	if rec := do(h, "POST", "/wagering/transactions", "internal", "{}"); rec.Code != http.StatusForbidden {
		t.Errorf("status %d", rec.Code)
	}
}

func TestProviderIDMustMatchToken(t *testing.T) {
	h := newTestHandler()
	req := httptest.NewRequest("POST", "/wagering/transactions", strings.NewReader(`{"providerId":"provider-b"}`))
	req.Header.Set("Authorization", "Bearer provider:provider-a")
	req.Header.Set("Idempotency-Key", "k")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "FORBIDDEN_PROVIDER") {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}

	rec = do(h, "GET", "/providers/provider-b/wagering/transactions/t1", "provider:provider-a", "")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("leitura cruzada: status %d", rec.Code)
	}
}

func TestIdempotencyKeyIsRequired(t *testing.T) {
	h := newTestHandler()
	rec := do(h, "POST", "/wagering/transactions", "provider:provider-a", `{"providerId":"provider-a"}`)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "MISSING_IDEMPOTENCY_KEY") {
		t.Fatalf("status %d body %s", rec.Code, rec.Body)
	}
}

func TestMalformedBodiesAreRejected(t *testing.T) {
	h := newTestHandler()
	bodies := map[string]string{
		"valor numérico": `{"providerId":"provider-a","money":{"amount":25.00,"currency":"BRL"}}`,
		"campo extra":    `{"providerId":"provider-a","surprise":1}`,
		"conteúdo extra": `{"providerId":"provider-a"} {"x":1}`,
		"json quebrado":  `{"providerId":`,
		"vazio":          ``,
	}
	for name, body := range bodies {
		req := httptest.NewRequest("POST", "/wagering/transactions", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer provider:provider-a")
		req.Header.Set("Idempotency-Key", "k")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: status %d body %s", name, rec.Code, rec.Body)
		}
	}
}

func TestHealthIsPublic(t *testing.T) {
	h := newTestHandler()
	if rec := do(h, "GET", "/health/live", "", ""); rec.Code != http.StatusOK {
		t.Errorf("live: %d", rec.Code)
	}
	if rec := do(h, "GET", "/health/ready", "", ""); rec.Code != http.StatusOK {
		t.Errorf("ready sem checks: %d", rec.Code)
	}
}
