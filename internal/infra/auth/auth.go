// Package auth valida tokens JWT emitidos por um IdP OIDC (Keycloak) e extrai
// a identidade autenticada.
package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"

	"github.com/leandropeloso/wager-service/internal/config"
)

var ErrInvalidToken = errors.New("invalid token")

// Principal é a identidade já validada.
type Principal struct {
	Subject    string
	ProviderID string
	provider   bool
	internal   bool
}

// NewPrincipal monta uma identidade já validada (usado por validadores
// alternativos e por testes dos handlers).
func NewPrincipal(subject, providerID string, provider, internal bool) Principal {
	return Principal{Subject: subject, ProviderID: providerID, provider: provider, internal: internal}
}

func (p Principal) IsProvider() bool { return p.provider && p.ProviderID != "" }
func (p Principal) IsInternal() bool { return p.internal }

type ctxKey struct{}

func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}

type Validator struct {
	cfg    config.Auth
	parser *jwt.Parser
	keys   atomic.Pointer[keyfunc.Keyfunc]

	runCtx context.Context
	stop   context.CancelFunc
}

func NewValidator(cfg config.Auth) *Validator {
	v := &Validator{
		cfg: cfg,
		parser: jwt.NewParser(
			jwt.WithValidMethods([]string{"RS256", "ES256"}),
			jwt.WithIssuer(cfg.Issuer),
			jwt.WithAudience(cfg.Audience),
			jwt.WithExpirationRequired(),
			jwt.WithLeeway(2*time.Second),
		),
	}
	v.runCtx, v.stop = context.WithCancel(context.Background())
	return v
}

// Start baixa o JWKS do IdP, tentando de novo enquanto ele sobe, e o mantém
// atualizado em segundo plano até Stop. Falha se o IdP não responder dentro de ctx.
func (v *Validator) Start(ctx context.Context) error {
	// keyfunc só registra o erro quando o primeiro download falha; por isso o
	// JWKS é sondado explicitamente antes de liberar a validação.
	for {
		err := v.probe(ctx)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("load JWKS from %s: %w (last error: %v)", v.cfg.JWKSURL, ctx.Err(), err)
		case <-time.After(time.Second):
		}
	}
	keys, err := keyfunc.NewDefaultCtx(v.runCtx, []string{v.cfg.JWKSURL})
	if err != nil {
		return fmt.Errorf("create key set: %w", err)
	}
	v.keys.Store(&keys)
	return nil
}

func (v *Validator) probe(ctx context.Context) error {
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, v.cfg.JWKSURL, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	var doc struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&doc); err != nil {
		return fmt.Errorf("invalid JWKS document: %w", err)
	}
	if len(doc.Keys) == 0 {
		return errors.New("JWKS has no keys")
	}
	return nil
}

func (v *Validator) Stop() { v.stop() }

// Ready informa se as chaves do IdP já foram carregadas.
func (v *Validator) Ready() bool { return v.keys.Load() != nil }

// Validate confere assinatura, emissor, audiência e expiração, e monta o Principal.
func (v *Validator) Validate(raw string) (Principal, error) {
	keys := v.keys.Load()
	if keys == nil {
		return Principal{}, fmt.Errorf("%w: signing keys not loaded", ErrInvalidToken)
	}
	claims := jwt.MapClaims{}
	if _, err := v.parser.ParseWithClaims(raw, claims, (*keys).Keyfunc); err != nil {
		return Principal{}, fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	sub, _ := claims["sub"].(string)
	p := Principal{Subject: sub}
	for _, role := range realmRoles(claims) {
		switch role {
		case v.cfg.ProviderRole:
			p.provider = true
		case v.cfg.InternalRole:
			p.internal = true
		}
	}
	if id, ok := claims[v.cfg.ProviderIDClaim].(string); ok {
		p.ProviderID = id
	}
	if !p.IsProvider() && !p.IsInternal() {
		return Principal{}, fmt.Errorf("%w: token carries no usable role", ErrInvalidToken)
	}
	return p, nil
}

func realmRoles(claims jwt.MapClaims) []string {
	access, _ := claims["realm_access"].(map[string]any)
	list, _ := access["roles"].([]any)
	roles := make([]string, 0, len(list))
	for _, r := range list {
		if s, ok := r.(string); ok {
			roles = append(roles, s)
		}
	}
	return roles
}
