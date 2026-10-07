// Package httpapi expõe a API HTTP: roteamento, autenticação, autorização e contratos JSON.
package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/leandropeloso/wager-service/internal/app/openwallet"
	"github.com/leandropeloso/wager-service/internal/app/processwager"
	"github.com/leandropeloso/wager-service/internal/app/query"
	"github.com/leandropeloso/wager-service/internal/infra/auth"
	"github.com/leandropeloso/wager-service/internal/infra/observability"
	"github.com/leandropeloso/wager-service/internal/telemetry"
)

const (
	maxBodyBytes      = 64 << 10
	correlationHeader = "X-Correlation-Id"
	idempotencyHeader = "Idempotency-Key"
)

// TokenValidator valida o bearer token e devolve a identidade.
type TokenValidator interface {
	Validate(raw string) (auth.Principal, error)
}

// ReadyCheck é uma verificação de dependência para /health/ready.
type ReadyCheck struct {
	Name  string
	Check func(ctx context.Context) error
}

type API struct {
	wagers  *processwager.Service
	wallets *openwallet.Service
	queries *query.Service
	auth    TokenValidator
	checks  []ReadyCheck
	metrics http.Handler
	log     *slog.Logger
}

func New(wagers *processwager.Service, wallets *openwallet.Service, queries *query.Service,
	validator TokenValidator, checks []ReadyCheck, metrics http.Handler, log *slog.Logger) *API {
	return &API{wagers: wagers, wallets: wallets, queries: queries, auth: validator, checks: checks, metrics: metrics, log: log}
}

// Handler monta as rotas. Saúde e métricas são públicas; o restante exige token.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", a.live)
	mux.HandleFunc("GET /health/ready", a.ready)
	if a.metrics != nil {
		mux.Handle("GET /metrics", a.metrics)
	}

	internal := a.require(roleInternal)
	provider := a.require(roleProvider)
	either := a.require(roleInternal, roleProvider)

	mux.Handle("POST /wallets", internal(a.openWallet))
	mux.Handle("GET /wallets/{walletId}", internal(a.getWallet))
	mux.Handle("GET /wallets/{walletId}/ledger", internal(a.getLedger))
	mux.Handle("POST /wallets/{walletId}/reconciliation", internal(a.reconcile))
	mux.Handle("GET /accounting/trial-balance", internal(a.trialBalance))

	mux.Handle("POST /wagering/transactions", provider(a.submitTransaction))
	mux.Handle("GET /wagering/transactions/{transactionId}", either(a.getTransaction))
	mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", either(a.getProviderTransaction))

	// otelhttp na borda: extrai o traceparent da requisição e abre o span raiz.
	// Saúde e métricas não geram spans.
	return otelhttp.NewHandler(a.recoverer(a.requestContext(mux)), "http",
		otelhttp.WithFilter(func(r *http.Request) bool {
			return !strings.HasPrefix(r.URL.Path, "/health") && r.URL.Path != "/metrics"
		}))
}

type role int

const (
	roleInternal role = iota
	roleProvider
)

// require autentica o bearer token e confere se o papel é permitido.
func (a *API) require(allowed ...role) func(http.HandlerFunc) http.Handler {
	return func(next http.HandlerFunc) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			header := r.Header.Get("Authorization")
			scheme, token, found := strings.Cut(header, " ")
			if !found || !strings.EqualFold(scheme, "Bearer") || strings.TrimSpace(token) == "" {
				w.Header().Set("WWW-Authenticate", `Bearer realm="wager"`)
				writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "missing or malformed bearer token")
				return
			}
			p, err := a.auth.Validate(strings.TrimSpace(token))
			if err != nil {
				w.Header().Set("WWW-Authenticate", `Bearer realm="wager", error="invalid_token"`)
				writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "invalid or expired token")
				return
			}
			ok := false
			for _, ro := range allowed {
				if (ro == roleInternal && p.IsInternal()) || (ro == roleProvider && p.IsProvider()) {
					ok = true
				}
			}
			if !ok {
				writeError(w, http.StatusForbidden, "FORBIDDEN", "the caller is not allowed to use this endpoint")
				return
			}
			ctx := auth.WithPrincipal(r.Context(), p)
			if p.IsProvider() {
				ctx = observability.WithLogger(ctx, a.logger(ctx).With("providerId", p.ProviderID))
			}
			next(w, r.WithContext(ctx))
		})
	}
}

func (a *API) logger(ctx context.Context) *slog.Logger { return observability.FromContext(ctx, a.log) }

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (s *statusWriter) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// requestContext atribui o correlationId e registra uma linha de log por requisição.
func (a *API) requestContext(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		corr := r.Header.Get(correlationHeader)
		if corr == "" || len(corr) > 128 {
			corr = uuid.NewString()
		}
		w.Header().Set(correlationHeader, corr)
		log := a.log.With("correlationId", corr)
		if id := telemetry.TraceID(r.Context()); id != "" {
			log = log.With("traceId", id) // logs e traces se correlacionam
		}
		ctx := observability.WithLogger(r.Context(), log)
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		// O mux grava o padrão da rota na requisição que recebe, então é preciso
		// guardar essa cópia para lê-lo depois.
		routed := r.WithContext(ctx)
		next.ServeHTTP(sw, routed)
		if strings.HasPrefix(r.URL.Path, "/health") || r.URL.Path == "/metrics" {
			return
		}
		if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
			// O padrão do mux já inclui o método ("POST /wagering/transactions").
			_, route, _ := strings.Cut(routed.Pattern, " ")
			span.SetName(routed.Pattern)
			span.SetAttributes(attribute.String("http.route", route), attribute.Int("http.status_code", sw.status))
			if sw.status >= 500 {
				span.SetStatus(codes.Error, http.StatusText(sw.status))
			}
		}
		observability.FromContext(ctx, log).Info("http request",
			"method", r.Method, "route", routed.Pattern, "status", sw.status, "durationMs", time.Since(start).Milliseconds())
	})
}

func (a *API) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				if errors.Is(asError(rec), http.ErrAbortHandler) {
					panic(rec)
				}
				a.log.Error("panic in http handler", "panic", rec, "stack", string(debug.Stack()))
				writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "unexpected error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func asError(v any) error {
	if err, ok := v.(error); ok {
		return err
	}
	return nil
}
