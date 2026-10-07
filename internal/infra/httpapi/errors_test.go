package httpapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/leandropeloso/wager-service/internal/app/openwallet"
	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/app/processwager"
	"github.com/leandropeloso/wager-service/internal/app/query"
)

// O contrato de erros precisa permitir distinguir cada situação do §9 do enunciado.
func TestFailureMapping(t *testing.T) {
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{&processwager.InvalidInputError{Field: "kind", Reason: "x"}, 400, "INVALID_REQUEST"},
		{&openwallet.InvalidInputError{Field: "playerId", Reason: "x"}, 400, "INVALID_REQUEST"},
		{processwager.ErrIdempotencyConflict, 409, "IDEMPOTENCY_KEY_CONFLICT"},
		{processwager.ErrExternalIDConflict, 409, "EXTERNAL_TRANSACTION_CONFLICT"},
		{openwallet.ErrAlreadyExists, 409, "WALLET_ALREADY_EXISTS"},
		{query.ErrNotFound, 404, "NOT_FOUND"},
		{port.ErrNotFound, 404, "NOT_FOUND"},
		{query.ErrInvalidCursor, 400, "INVALID_REQUEST"},
		{query.ErrInvalidLimit, 400, "INVALID_REQUEST"},
		{fmt.Errorf("wrap: %w", port.ErrTransient), 503, "SERVICE_UNAVAILABLE"},
		{port.ErrRetryable, 503, "SERVICE_UNAVAILABLE"},
		{context.DeadlineExceeded, 503, "SERVICE_UNAVAILABLE"},
		{errors.New("boom"), 500, "INTERNAL_ERROR"},
	}
	a := newTestAPI()
	seen := map[string]int{}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		a.writeFailure(rec, httptest.NewRequest("GET", "/", nil), c.err)
		if rec.Code != c.status || !contains(rec.Body.String(), c.code) {
			t.Errorf("%v → %d %s", c.err, rec.Code, rec.Body)
		}
		if c.status == 503 && rec.Header().Get("Retry-After") == "" {
			t.Errorf("%v: 503 deve trazer Retry-After", c.err)
		}
		seen[c.code] = c.status
	}
	// indisponibilidade transitória, conflito e entrada inválida têm status distintos
	if seen["SERVICE_UNAVAILABLE"] == seen["IDEMPOTENCY_KEY_CONFLICT"] || seen["INVALID_REQUEST"] == seen["IDEMPOTENCY_KEY_CONFLICT"] {
		t.Fatal("as situações do contrato devem ser distinguíveis por status")
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func discardLogger() *slog.Logger { return slog.New(slog.NewJSONHandler(io.Discard, nil)) }

func newTestAPI() *API {
	return New(nil, nil, nil, fakeValidator{}, nil, nil, discardLogger())
}

func TestUnknownRoutesAndMethods(t *testing.T) {
	h := newTestHandler()
	if rec := do(h, "GET", "/nope", "", ""); rec.Code != http.StatusNotFound {
		t.Errorf("rota inexistente: %d", rec.Code)
	}
	if rec := do(h, "DELETE", "/wallets/0192f291-27dd-7d3f-8071-5f8685deef37", "internal", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("método não permitido: %d", rec.Code)
	}
}

func TestOversizedBodyIsRejected(t *testing.T) {
	h := newTestHandler()
	big := `{"providerId":"provider-a","gameId":"` + strings.Repeat("x", 70<<10) + `"}`
	req := httptest.NewRequest("POST", "/wagering/transactions", strings.NewReader(big))
	req.Header.Set("Authorization", "Bearer provider:provider-a")
	req.Header.Set("Idempotency-Key", "k")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("corpo gigante: %d", rec.Code)
	}
}
