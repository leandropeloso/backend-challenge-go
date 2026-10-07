package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/leandropeloso/wager-service/internal/app/openwallet"
	"github.com/leandropeloso/wager-service/internal/app/port"
	"github.com/leandropeloso/wager-service/internal/app/processwager"
	"github.com/leandropeloso/wager-service/internal/app/query"
)

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: message}})
}

func writeFieldError(w http.ResponseWriter, status int, code, field, message string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: message, Field: field}})
}

// writeFailure traduz erros da aplicação para o contrato HTTP:
//
//	400 INVALID_REQUEST         entrada inválida
//	404 NOT_FOUND               recurso inexistente (ou de outro provedor)
//	409 *_CONFLICT              chave de idempotência / transação externa / carteira em conflito
//	503 SERVICE_UNAVAILABLE     dependência temporariamente indisponível (Retry-After)
//	500 INTERNAL_ERROR          qualquer outra falha
func (a *API) writeFailure(w http.ResponseWriter, r *http.Request, err error) {
	var invalid *processwager.InvalidInputError
	var invalidWallet *openwallet.InvalidInputError
	switch {
	case errors.As(err, &invalid):
		writeFieldError(w, http.StatusBadRequest, "INVALID_REQUEST", invalid.Field, invalid.Reason)
	case errors.As(err, &invalidWallet):
		writeFieldError(w, http.StatusBadRequest, "INVALID_REQUEST", invalidWallet.Field, invalidWallet.Reason)
	case errors.Is(err, processwager.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "IDEMPOTENCY_KEY_CONFLICT", "the idempotency key was already used with different content")
	case errors.Is(err, processwager.ErrExternalIDConflict):
		writeError(w, http.StatusConflict, "EXTERNAL_TRANSACTION_CONFLICT", "the external transaction is already recorded under a different idempotency key")
	case errors.Is(err, openwallet.ErrAlreadyExists):
		writeError(w, http.StatusConflict, "WALLET_ALREADY_EXISTS", "a wallet already exists for this player and currency")
	case errors.Is(err, query.ErrNotFound), errors.Is(err, port.ErrNotFound):
		writeError(w, http.StatusNotFound, "NOT_FOUND", "resource not found")
	case errors.Is(err, query.ErrInvalidCursor):
		writeFieldError(w, http.StatusBadRequest, "INVALID_REQUEST", "cursor", "invalid cursor")
	case errors.Is(err, query.ErrInvalidLimit):
		writeFieldError(w, http.StatusBadRequest, "INVALID_REQUEST", "limit", "limit must be between 1 and 200")
	case errors.Is(err, port.ErrTransient), errors.Is(err, port.ErrRetryable),
		errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		a.logger(r.Context()).Warn("transient failure", "error", err)
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "temporarily unavailable, retry with the same Idempotency-Key")
	default:
		a.logger(r.Context()).Error("request failed", "error", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "unexpected error")
	}
}
