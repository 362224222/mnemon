package runtime

import (
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/channel"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/kernel"
	"github.com/mnemon-dev/mnemon/harness/internal/rule"
)

// TestP2ChannelEndToEnd is the P2 gate's positive path: a runtime booted with ONE in-memory binding
// serves observe (session.observed) -> auto-tick -> pull -> status, all over real HTTP, all on the
// SAME store and principal.
func TestP2ChannelEndToEnd(t *testing.T) {
	ref := contract.ResourceRef{Kind: "memory", ID: "m1"}
	storePath := filepath.Join(t.TempDir(), "governed.db")
	// A rule that creates m1 on a session.observed, so the single observe produces canonical state.
	createRule := rule.NewNativeRule("creator", "codex", "memory.write.proposed", []string{"session.observed"},
		func(in rule.RuleInput) (contract.RuleDecision, error) {
			for _, rv := range in.View.Resources {
				if rv.Ref == ref && rv.Version > 0 {
					return contract.RuleDecision{Verdict: contract.VerdictAllow}, nil
				}
			}
			return contract.RuleDecision{Verdict: contract.VerdictPropose, Proposal: &contract.ProposedEvent{
				Type:    "memory.write.proposed",
				Payload: map[string]any{"writes": []contract.ResourceWrite{{Ref: ref, Kind: contract.OpCreate, Fields: map[string]any{"content": "from session"}}}},
			}}, nil
		})
	binding := channel.ChannelBinding{
		Principal: "codex", ActorKind: contract.KindHostAgent,
		AllowedVerbs: []channel.Verb{channel.VerbObserve, channel.VerbPull, channel.VerbStatus}, AllowedObservedTypes: []string{"session.observed"},
		SubscriptionScope: []contract.ResourceRef{ref}, IdempotencyNamespace: "host:codex",
	}
	rt, err := OpenRuntime(storePath, RuntimeConfig{
		Rules:     rule.NewRuleSet(createRule),
		Authority: kernel.AuthorityRules{Allow: map[contract.ActorID][]contract.ResourceKind{"codex": {"memory"}}},
		Subs:      map[contract.ActorID]contract.Subscription{"codex": {Actor: "codex", Refs: []contract.ResourceRef{ref}}},
		Bindings:  []channel.ChannelBinding{binding},
		NewID:     seqGen(), Now: fixedNow(),
		SchemaGuard: kernel.SchemaGuardWith(map[contract.ResourceKind][]string{"memory": {"content"}, "skill": {"name"}}),
	})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	srv := httptest.NewServer(NewRuntimeHandler(rt, channel.HeaderAuthenticator{}))
	defer srv.Close()
	c := channel.NewClient(srv.URL, "codex")

	rec, err := c.IngestObserve("codex", contract.ObservationEnvelope{ExternalID: "s1", Event: contract.Event{Type: "session.observed", CorrelationID: "c1"}})
	if err != nil || !rec.Ticked {
		t.Fatalf("observe must ingest + auto-tick; rec=%+v err=%v", rec, err)
	}
	proj, err := c.PullEventView("codex", contract.Subscription{Actor: "codex"})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if rvVersionSrv(proj.Resources, ref) == 0 {
		t.Fatalf("pull must reflect the governed write; got %+v", proj.Resources)
	}
	st, err := c.Status("codex")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Digest != proj.Digest || st.StoreRef != storePath || st.ActorKind != contract.KindHostAgent {
		t.Fatalf("status must agree with the same store/principal; st=%+v projDigest=%s", st, proj.Digest)
	}
}

// TestP2ChannelNegatives is the P2 gate's negative path: unknown principal, disallowed observed type,
// cross-scope pull, and a forged *.proposed are each rejected over the channel.
func TestP2ChannelNegatives(t *testing.T) {
	ref := contract.ResourceRef{Kind: "memory", ID: "m1"}
	other := contract.ResourceRef{Kind: "memory", ID: "secret"}
	rt, err := OpenRuntime(filepath.Join(t.TempDir(), "s.db"), RuntimeConfig{
		Subs: map[contract.ActorID]contract.Subscription{"codex": {Actor: "codex", Refs: []contract.ResourceRef{ref}}},
		Bindings: []channel.ChannelBinding{{
			Principal: "codex", ActorKind: contract.KindHostAgent,
			AllowedVerbs: []channel.Verb{channel.VerbObserve, channel.VerbPull, channel.VerbStatus}, AllowedObservedTypes: []string{"session.observed"},
			SubscriptionScope: []contract.ResourceRef{ref}, IdempotencyNamespace: "host:codex",
		}},
		SchemaGuard: kernel.SchemaGuardWith(map[contract.ResourceKind][]string{"memory": {"content"}, "skill": {"name"}}),
	})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	srv := httptest.NewServer(NewRuntimeHandler(rt, channel.HeaderAuthenticator{}))
	defer srv.Close()

	// unknown principal.
	if _, _, err := channel.NewClient(srv.URL, "ghost").Ingest("ghost", obs("session.observed")); err == nil {
		t.Fatal("unknown principal must be rejected")
	}
	codex := channel.NewClient(srv.URL, "codex")
	// disallowed observed type.
	if _, _, err := codex.Ingest("codex", obs("memory.observed")); err == nil {
		t.Fatal("disallowed observed type must be rejected")
	}
	// cross-scope pull (ref outside the binding scope).
	if _, err := codex.PullEventView("codex", contract.Subscription{Actor: "codex", Refs: []contract.ResourceRef{other}}); err == nil {
		t.Fatal("out-of-scope pull must be rejected")
	}
	// forged *.proposed.
	if _, _, err := codex.Ingest("codex", contract.ObservationEnvelope{ExternalID: "f", Event: contract.Event{Type: "memory.write.proposed"}}); err == nil {
		t.Fatal("forged *.proposed must be rejected")
	}
}

func rvVersionSrv(rvs []contract.ResourceVersion, ref contract.ResourceRef) contract.Version {
	for _, rv := range rvs {
		if rv.Ref == ref {
			return rv.Version
		}
	}
	return 0
}
