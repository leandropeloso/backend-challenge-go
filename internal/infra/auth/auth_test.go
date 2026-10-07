package auth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/leandropeloso/wager-service/internal/config"
)

const issuer = "http://idp.test/realms/wager"

type idp struct {
	key    *rsa.PrivateKey
	server *httptest.Server
}

// newIDP publica um JWKS real e assina tokens com a chave correspondente.
func newIDP(t *testing.T) *idp {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	jwks := map[string]any{"keys": []map[string]string{{
		"kty": "RSA", "kid": "k1", "alg": "RS256", "use": "sig",
		"n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
		"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
	}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jwks)
	}))
	t.Cleanup(srv.Close)
	return &idp{key: key, server: srv}
}

func (i *idp) sign(t *testing.T, key *rsa.PrivateKey, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = "k1"
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func baseClaims() jwt.MapClaims {
	return jwt.MapClaims{
		"iss": issuer, "aud": "wager-api", "sub": "svc-1",
		"exp": time.Now().Add(time.Hour).Unix(), "iat": time.Now().Unix(),
		"providerId":   "provider-a",
		"realm_access": map[string]any{"roles": []string{"provider"}},
	}
}

func newValidator(t *testing.T, i *idp) *Validator {
	t.Helper()
	v := NewValidator(config.Auth{
		Issuer: issuer, JWKSURL: i.server.URL, Audience: "wager-api",
		ProviderRole: "provider", InternalRole: "internal", ProviderIDClaim: "providerId",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := v.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(v.Stop)
	return v
}

func TestValidateAcceptsProviderToken(t *testing.T) {
	i := newIDP(t)
	v := newValidator(t, i)
	p, err := v.Validate(i.sign(t, i.key, baseClaims()))
	if err != nil {
		t.Fatal(err)
	}
	if !p.IsProvider() || p.IsInternal() || p.ProviderID != "provider-a" || p.Subject != "svc-1" {
		t.Fatalf("principal: %+v", p)
	}
}

func TestValidateInternalToken(t *testing.T) {
	i := newIDP(t)
	v := newValidator(t, i)
	c := baseClaims()
	delete(c, "providerId")
	c["realm_access"] = map[string]any{"roles": []string{"internal"}}
	p, err := v.Validate(i.sign(t, i.key, c))
	if err != nil || !p.IsInternal() || p.IsProvider() {
		t.Fatalf("principal: %+v err=%v", p, err)
	}
}

func TestValidateRejections(t *testing.T) {
	i := newIDP(t)
	v := newValidator(t, i)
	other, _ := rsa.GenerateKey(rand.Reader, 2048)

	mutate := func(f func(jwt.MapClaims)) jwt.MapClaims {
		c := baseClaims()
		f(c)
		return c
	}
	cases := map[string]string{
		"assinatura de outra chave": i.sign(t, other, baseClaims()),
		"expirado":                  i.sign(t, i.key, mutate(func(c jwt.MapClaims) { c["exp"] = time.Now().Add(-time.Minute).Unix() })),
		"sem exp":                   i.sign(t, i.key, mutate(func(c jwt.MapClaims) { delete(c, "exp") })),
		"emissor errado":            i.sign(t, i.key, mutate(func(c jwt.MapClaims) { c["iss"] = "http://evil/realms/x" })),
		"audiência errada":          i.sign(t, i.key, mutate(func(c jwt.MapClaims) { c["aud"] = "outra-api" })),
		"sem papel útil":            i.sign(t, i.key, mutate(func(c jwt.MapClaims) { c["realm_access"] = map[string]any{"roles": []string{"offline_access"}} })),
		"provider sem providerId":   i.sign(t, i.key, mutate(func(c jwt.MapClaims) { delete(c, "providerId") })),
		"lixo":                      "not-a-jwt",
		"vazio":                     "",
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := v.Validate(raw); !errors.Is(err, ErrInvalidToken) {
				t.Fatalf("esperava ErrInvalidToken, veio %v", err)
			}
		})
	}
}

func TestValidateRejectsUnsignedAndHMACTokens(t *testing.T) {
	i := newIDP(t)
	v := newValidator(t, i)

	none := jwt.NewWithClaims(jwt.SigningMethodNone, baseClaims())
	raw, err := none.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.Validate(raw); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("alg=none: %v", err)
	}

	hs := jwt.NewWithClaims(jwt.SigningMethodHS256, baseClaims())
	hs.Header["kid"] = "k1"
	raw, _ = hs.SignedString(i.key.N.Bytes())
	if _, err := v.Validate(raw); !errors.Is(err, ErrInvalidToken) {
		t.Errorf("HS256 com chave pública: %v", err)
	}
}

func TestValidateBeforeKeysLoaded(t *testing.T) {
	v := NewValidator(config.Auth{Issuer: issuer, JWKSURL: "http://127.0.0.1:1", Audience: "wager-api", ProviderRole: "provider", InternalRole: "internal", ProviderIDClaim: "providerId"})
	defer v.Stop()
	if v.Ready() {
		t.Fatal("não deveria estar pronto")
	}
	if _, err := v.Validate("x.y.z"); !errors.Is(err, ErrInvalidToken) {
		t.Fatalf("err = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := v.Start(ctx); err == nil {
		t.Fatal("Start deveria falhar com IdP indisponível")
	}
}
