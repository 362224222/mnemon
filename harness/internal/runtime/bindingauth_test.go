package runtime

import (
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
)

func obs(t string) contract.ObservationEnvelope {
	return contract.ObservationEnvelope{ExternalID: "x-" + t, Event: contract.Event{Type: t, CorrelationID: "c-" + t}}
}

// TestChannelBindingAuthorizer pins P2.1: the runtime's channel API enforces the binding manifest —
// principal must have a binding; the verb must be granted; the observed type must be allowed; pull
// refs must be within binding scope; and the internal-only suffix guard still fires INSIDE
// ControlServer.Ingest (the authorizer wraps it, it does not replace it).
func TestChannelBindingAuthorizer(t *testing.T) {
	ref := contract.ResourceRef{Kind: "memory", ID: "m1"}
	other := contract.ResourceRef{Kind: "memory", ID: "other"}

	rt, err := OpenRuntime(filepath.Join(t.TempDir(), "s.db"), RuntimeConfig{
		Subs: map[contract.ActorID]contract.Subscription{
			"codex":    {Actor: "codex", Refs: []contract.ResourceRef{ref}},
			"operator": {Actor: "operator", Refs: []contract.ResourceRef{ref}},
			"reader":   {Actor: "reader", Refs: []contract.ResourceRef{ref}},
		},
		Bindings: []access.ChannelBinding{
			{Principal: "codex", ActorKind: contract.KindHostAgent, AllowedVerbs: []access.Verb{access.VerbObserve, access.VerbPull},
				AllowedObservedTypes: []string{"session.observed"}, SubscriptionScope: []contract.ResourceRef{ref}},
			{Principal: "operator", ActorKind: contract.KindControlAgent, AllowedVerbs: []access.Verb{access.VerbObserve, access.VerbPull},
				SubscriptionScope: []contract.ResourceRef{ref}}, // empty AllowedObservedTypes => any
			{Principal: "reader", ActorKind: contract.KindHostAgent, AllowedVerbs: []access.Verb{access.VerbPull},
				SubscriptionScope: []contract.ResourceRef{ref}},
		},
	})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	api := rt.API()

	// unknown principal: no binding => rejected.
	if _, _, err := api.Ingest("ghost", obs("session.observed")); err == nil {
		t.Fatal("a principal without a binding must be rejected")
	}
	// verb not granted: reader may pull, not observe.
	if _, _, err := api.Ingest("reader", obs("session.observed")); err == nil {
		t.Fatal("a principal not granted observe must be rejected")
	}
	// observed type outside the binding allow-list => rejected.
	if _, _, err := api.Ingest("codex", obs("memory.observed")); err == nil {
		t.Fatal("an observed type outside the binding allow-list must be rejected")
	}
	// allowed observed type => accepted.
	if _, _, err := api.Ingest("codex", obs("session.observed")); err != nil {
		t.Fatalf("an allowed observed type must ingest: %v", err)
	}
	// in-scope pull => OK; out-of-scope ref => rejected by the binding authorizer.
	if _, err := api.PullPresentationView("codex", contract.Subscription{Actor: "codex", Refs: []contract.ResourceRef{ref}}); err != nil {
		t.Fatalf("in-scope pull must succeed: %v", err)
	}
	if _, err := api.PullPresentationView("codex", contract.Subscription{Actor: "codex", Refs: []contract.ResourceRef{other}}); err == nil {
		t.Fatal("a pull ref outside the binding scope must be rejected")
	}
	// internal-only suffix still fails INSIDE ControlServer.Ingest even when the binding allows any
	// observed type (operator's allow-list is empty/any).
	if _, _, err := api.Ingest("operator", obs("memory.write.proposed")); err == nil {
		t.Fatal("a forged *.proposed must still be rejected inside ControlServer.Ingest")
	}
}
