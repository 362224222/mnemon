package access

import (
	"net/http"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// The binding authenticator resolves each request per the principal's auth mode, so token-auth and
// header-auth principals coexist on ONE server: a token principal authenticates via its bearer and
// cannot be impersonated via the header; a header principal authenticates via the trusted header.
func TestBindingAuthenticatorPerPrincipal(t *testing.T) {
	// alice has a token (token-auth); bob has no credential (header-auth).
	auth := NewBindingAuthenticator(LoadedBindings{Tokens: map[string]contract.ActorID{"tok-A": "alice@x"}})

	req := func(set func(*http.Request)) *http.Request {
		r, _ := http.NewRequest(http.MethodPost, "http://x/ingest", nil)
		set(r)
		return r
	}

	if p, err := auth.Authenticate(req(func(r *http.Request) { r.Header.Set("Authorization", "Bearer tok-A") })); err != nil || p != "alice@x" {
		t.Fatalf("bearer must resolve the token principal; got %q err %v", p, err)
	}
	if p, err := auth.Authenticate(req(func(r *http.Request) { r.Header.Set(principalHeader, "bob@x") })); err != nil || p != "bob@x" {
		t.Fatalf("a header-auth principal must authenticate via the header; got %q err %v", p, err)
	}
	if _, err := auth.Authenticate(req(func(r *http.Request) { r.Header.Set(principalHeader, "alice@x") })); err == nil {
		t.Fatal("a token-auth principal must NOT be impersonable via the trusted header")
	}
	if _, err := auth.Authenticate(req(func(r *http.Request) { r.Header.Set("Authorization", "Bearer nope") })); err == nil {
		t.Fatal("an unrecognized bearer token must be rejected")
	}
}
