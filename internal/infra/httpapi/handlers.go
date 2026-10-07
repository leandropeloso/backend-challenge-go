package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/leandropeloso/wager-service/internal/app/openwallet"
	"github.com/leandropeloso/wager-service/internal/app/processwager"
	"github.com/leandropeloso/wager-service/internal/domain/wager"
	"github.com/leandropeloso/wager-service/internal/infra/auth"
	"github.com/leandropeloso/wager-service/internal/infra/observability"
)

func (a *API) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	status, body := http.StatusOK, map[string]string{}
	for _, c := range a.checks {
		if err := c.Check(ctx); err != nil {
			status = http.StatusServiceUnavailable
			body[c.Name] = "unavailable"
			a.log.Warn("readiness check failed", "check", c.Name, "error", err)
			continue
		}
		body[c.Name] = "ok"
	}
	overall := "ok"
	if status != http.StatusOK {
		overall = "unavailable"
	}
	writeJSON(w, status, map[string]any{"status": overall, "checks": body})
}

// decode lê um corpo JSON estrito: sem campos desconhecidos e sem conteúdo extra.
// Valores monetários são strings; um número JSON no lugar delas é rejeitado.
func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "INVALID_REQUEST", "request body too large")
			return false
		}
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "malformed JSON body: "+jsonProblem(err))
		return false
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "unexpected content after the JSON body")
		return false
	}
	return true
}

func jsonProblem(err error) string {
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return "field " + typeErr.Field + " has the wrong type"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "empty or truncated body"
	}
	return "invalid syntax or unknown field"
}

func pathUUID(w http.ResponseWriter, r *http.Request, name string) (uuid.UUID, bool) {
	raw := r.PathValue(name)
	id, err := uuid.Parse(raw)
	if err != nil || len(raw) != 36 {
		writeFieldError(w, http.StatusBadRequest, "INVALID_REQUEST", name, "must be a UUID")
		return uuid.Nil, false
	}
	return id, true
}

func principal(r *http.Request) auth.Principal {
	p, _ := auth.PrincipalFrom(r.Context())
	return p
}

func (a *API) openWallet(w http.ResponseWriter, r *http.Request) {
	var req openWalletRequest
	if !decode(w, r, &req) {
		return
	}
	created, err := a.wallets.Open(r.Context(), openwallet.Command{
		PlayerID: req.PlayerID, Amount: req.InitialBalance.Amount, Currency: req.InitialBalance.Currency,
		CorrelationID: w.Header().Get(correlationHeader),
	})
	if err != nil {
		a.writeFailure(w, r, err)
		return
	}
	a.logger(r.Context()).Info("wallet opened", "walletId", created.ID())
	w.Header().Set("Location", "/wallets/"+created.ID().String())
	writeJSON(w, http.StatusCreated, toWallet(created))
}

func (a *API) getWallet(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "walletId")
	if !ok {
		return
	}
	found, err := a.queries.Wallet(r.Context(), id)
	if err != nil {
		a.writeFailure(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toWallet(found))
}

func (a *API) getLedger(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "walletId")
	if !ok {
		return
	}
	limit := 0
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			writeFieldError(w, http.StatusBadRequest, "INVALID_REQUEST", "limit", "limit must be an integer")
			return
		}
		limit = n
		if limit == 0 {
			limit = -1 // 0 explícito é inválido; só a ausência usa o padrão
		}
	}
	page, err := a.queries.Ledger(r.Context(), id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		a.writeFailure(w, r, err)
		return
	}
	resp := ledgerPageResponse{Items: make([]ledgerItemResponse, 0, len(page.Items))}
	for _, it := range page.Items {
		resp.Items = append(resp.Items, toLedgerItem(it))
	}
	if page.NextCursor != "" {
		resp.NextCursor = &page.NextCursor
	}
	writeJSON(w, http.StatusOK, resp)
}

func (a *API) reconcile(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "walletId")
	if !ok {
		return
	}
	res, err := a.queries.Reconcile(r.Context(), id)
	if err != nil {
		a.writeFailure(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, reconciliationResponse{
		WalletID: res.WalletID.String(), StoredBalance: toMoney(res.Stored), CalculatedBalance: toMoney(res.Calculated),
		Difference: toMoney(res.Difference), Consistent: res.Consistent, CheckedEntries: res.Entries,
	})
}

// submitTransaction recebe uma operação de um provedor.
//
//	200 PROCESSED            operação aplicada (ou replay de uma aplicada)
//	202 PENDING_REFERENCE    aceita, aguardando a transação referenciada
//	422 REJECTED             recusada por regra de negócio (failureCode estável)
//	400 / 409 / 503 / 500    ver writeFailure
func (a *API) submitTransaction(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get(idempotencyHeader)
	if key == "" {
		writeFieldError(w, http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY", idempotencyHeader, "the Idempotency-Key header is required")
		return
	}
	var req submitRequest
	if !decode(w, r, &req) {
		return
	}
	p := principal(r)
	if req.ProviderID != p.ProviderID {
		writeFieldError(w, http.StatusForbidden, "FORBIDDEN_PROVIDER", "providerId", "providerId does not match the authenticated provider")
		return
	}

	out, err := a.wagers.Execute(r.Context(), processwager.Command{
		ProviderID: req.ProviderID, ExternalTransactionID: req.ExternalTransactionID, IdempotencyKey: key,
		PlayerID: req.PlayerID, WalletID: req.WalletID, RoundID: req.RoundID, GameID: req.GameID,
		Kind: req.Kind, Amount: req.Money.Amount, Currency: req.Money.Currency,
		ReferenceExternalTransactionID: req.ReferenceExternalTransactionID,
		CorrelationID:                  w.Header().Get(correlationHeader), Source: "http",
	})
	if err != nil {
		a.writeFailure(w, r, err)
		return
	}
	a.logger(r.Context()).Info("wager transaction handled", "transactionId", out.TransactionID,
		"walletId", req.WalletID, "status", out.Status, "replay", out.Replay)

	resp := submitResponse{
		TransactionID: out.TransactionID.String(), Status: string(out.Status),
		Balance: moneyPtr(out.Balance), IdempotentReplay: out.Replay,
	}
	switch out.Status {
	case wager.Processed:
		writeJSON(w, http.StatusOK, resp)
	case wager.Pending, wager.PendingReference:
		w.Header().Set("Location", "/wagering/transactions/"+out.TransactionID.String())
		resp.Balance = nil
		writeJSON(w, http.StatusAccepted, resp)
	case wager.Rejected:
		resp.Balance = nil
		resp.FailureCode = string(out.FailureCode)
		resp.Correctable = &out.Correctable
		writeJSON(w, http.StatusUnprocessableEntity, resp)
	default:
		resp.Balance = nil
		resp.FailureCode = string(out.FailureCode)
		writeJSON(w, http.StatusInternalServerError, resp)
	}
}

func (a *API) getTransaction(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "transactionId")
	if !ok {
		return
	}
	t, err := a.queries.Transaction(r.Context(), id)
	if err != nil {
		a.writeFailure(w, r, err)
		return
	}
	p := principal(r)
	// Provedores só enxergam as próprias transações; o resto responde como inexistente.
	if !p.IsInternal() && (t.IsInternal() || t.ProviderID() != p.ProviderID) {
		observability.FromContext(r.Context(), a.log).Warn("cross-provider transaction access denied", "transactionId", id)
		writeError(w, http.StatusNotFound, "NOT_FOUND", "resource not found")
		return
	}
	writeJSON(w, http.StatusOK, toTransaction(t))
}

func (a *API) getProviderTransaction(w http.ResponseWriter, r *http.Request) {
	providerID := r.PathValue("providerId")
	p := principal(r)
	if !p.IsInternal() && providerID != p.ProviderID {
		writeError(w, http.StatusForbidden, "FORBIDDEN_PROVIDER", "the authenticated provider cannot read another provider's transactions")
		return
	}
	t, err := a.queries.TransactionByProvider(r.Context(), providerID, r.PathValue("externalTransactionId"))
	if err != nil {
		a.writeFailure(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toTransaction(t))
}

// trialBalance totaliza o livro-diário de partidas dobradas por moeda.
func (a *API) trialBalance(w http.ResponseWriter, r *http.Request) {
	lines, err := a.queries.TrialBalance(r.Context())
	if err != nil {
		a.writeFailure(w, r, err)
		return
	}
	resp := trialBalanceResponse{Balanced: true, Lines: make([]trialLineResponse, 0, len(lines))}
	for _, l := range lines {
		resp.Balanced = resp.Balanced && l.Balanced
		resp.Lines = append(resp.Lines, trialLineResponse{
			Currency: string(l.Currency), Debits: l.Debits, Credits: l.Credits, Entries: l.Entries, Balanced: l.Balanced,
		})
	}
	writeJSON(w, http.StatusOK, resp)
}
