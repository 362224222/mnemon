package runtime

import (
	"net/http/httptest"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/rule"
)

func TestChannelBindingValidate(t *testing.T) {
	good := access.HostAgentBinding("agent", "http://localhost:8787", []contract.ResourceRef{{Kind: "memory", ID: "m1"}})
	if err := good.Validate(); err != nil {
		t.Fatalf("a host-agent binding must validate: %v", err)
	}
	if !good.Allows(access.VerbObserve) || !good.Allows(access.VerbPull) {
		t.Fatalf("host-agent binding must allow observe + pull")
	}
	// ControlAgent is the SAME channel, different binding (zero new surface).
	ctrl := access.ControlAgentBinding("operator", "http://localhost:8787", nil)
	if ctrl.ActorKind != contract.KindControlAgent {
		t.Fatalf("control binding kind = %q", ctrl.ActorKind)
	}
	if ctrl.IdempotencyNamespace == good.IdempotencyNamespace {
		t.Fatalf("distinct principals must get distinct idempotency namespaces")
	}
	replica := access.ReplicaAgentBinding("replica", "http://localhost:8787", nil)
	if err := replica.Validate(); err != nil {
		t.Fatalf("replica-agent binding must validate: %v", err)
	}
	if !replica.Allows(access.VerbSyncPush) || replica.Allows(access.VerbObserve) {
		t.Fatalf("replica-agent must be sync-only, got %+v", replica.AllowedVerbs)
	}

	bad := []access.ChannelBinding{
		{ActorKind: contract.KindHostAgent, AllowedVerbs: []access.Verb{access.VerbObserve}}, // no principal
		{Principal: "x", ActorKind: "root", AllowedVerbs: []access.Verb{access.VerbObserve}}, // unknown kind
		{Principal: "x", ActorKind: contract.KindHostAgent},                                  // no verbs
	}
	for i, b := range bad {
		if err := b.Validate(); err == nil {
			t.Fatalf("malformed binding %d must be rejected", i)
		}
	}
}

func TestChannelBindingAllowsObservedType(t *testing.T) {
	any := access.HostAgentBinding("agent", "", nil) // empty AllowedObservedTypes => any
	if !any.AllowsObservedType("memory.observed") {
		t.Fatalf("empty allow-list must permit any observed type")
	}
	scoped := access.ChannelBinding{Principal: "agent", ActorKind: contract.KindHostAgent, AllowedVerbs: []access.Verb{access.VerbObserve}, AllowedObservedTypes: []string{"memory.observed"}}
	if !scoped.AllowsObservedType("memory.observed") || scoped.AllowsObservedType("goal.observed") {
		t.Fatalf("scoped allow-list must permit only its listed types")
	}
}

// TestTokenAuthenticatorSeam proves the access.Authenticator seam: the same channel served with a
// non-header authenticator resolves the principal from a bearer token (and rejects an unknown
// one), without the trusted X-Mnemon-Principal header.
func TestTokenAuthenticatorSeam(t *testing.T) {
	_, _, cs := newServerWith(t, rule.NewRuleSet())
	auth := access.TokenAuthenticator{Tokens: map[string]contract.ActorID{"tok-agent": "agent"}}
	srv := httptest.NewServer(access.NewHTTPHandlerWithAuth(cs, auth))
	defer srv.Close()

	// A request with a valid token resolves to principal "agent".
	c := access.NewClientWithToken(srv.URL, "tok-agent")
	if _, _, err := c.Ingest("agent", contract.ObservationEnvelope{ExternalID: "e1", Event: contract.Event{Type: "memory.observed", CorrelationID: "c1"}}); err != nil {
		t.Fatalf("valid token must authenticate: %v", err)
	}
	// An unknown token is rejected (401).
	bad := access.NewClientWithToken(srv.URL, "nope")
	if _, _, err := bad.Ingest("agent", contract.ObservationEnvelope{ExternalID: "e2", Event: contract.Event{Type: "memory.observed", CorrelationID: "c2"}}); err == nil {
		t.Fatalf("unknown token must be rejected")
	}
}
