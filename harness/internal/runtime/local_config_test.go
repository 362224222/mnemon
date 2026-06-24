package runtime

import (
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/state"
)

// localRuntimeConfigT mirrors app.LocalRuntimeConfigFromBindings for the runtime-level integration
// tests, which exercise the capability rules end-to-end through the runtime (and assert on runtime
// internals). The production derivation lives in app; this keeps the test in package runtime without
// importing app (which would cycle).
func localRuntimeConfigT(bindings []access.ChannelBinding) RuntimeConfig {
	catalog := policy.StandardRegistry()
	var rules []admission.Rule
	allow := map[contract.ActorID][]contract.ResourceKind{}
	for _, b := range bindings {
		for _, ref := range b.SubscriptionScope {
			cap, ok := catalog[string(ref.Kind)]
			if !ok {
				continue
			}
			if b.Allows(access.VerbObserve) && b.AllowsObservedType(cap.ObservedType) {
				rules = append(rules, cap.Rule(b.Principal, ref, policy.Limits{}))
			}
		}
		if b.ActorKind != contract.KindHostAgent {
			continue
		}
		seen := map[contract.ResourceKind]bool{}
		for _, ref := range b.SubscriptionScope {
			if _, ok := catalog[string(ref.Kind)]; ok {
				seen[ref.Kind] = true
			}
		}
		for kind := range seen {
			allow[b.Principal] = append(allow[b.Principal], kind)
		}
	}
	return RuntimeConfig{
		Bindings:      bindings,
		Subs:          access.SubsFromBindings(bindings),
		Rules:         admission.NewRuleSet(rules...),
		Authority:     state.AuthorityRules{Allow: allow},
		SchemaGuard:   state.SchemaGuardWith(requiredHeadersT(catalog)),
		SyncableKinds: policy.ImportableKinds(catalog),
	}
}

func requiredHeadersT(catalog policy.Registry) map[contract.ResourceKind][]string {
	out := map[contract.ResourceKind][]string{}
	for _, cap := range catalog {
		out[cap.ResourceKind] = cap.RequiredHeader
	}
	return out
}

func scopeRefT(b access.ChannelBinding, kind contract.ResourceKind) (contract.ResourceRef, bool) {
	for _, ref := range b.SubscriptionScope {
		if ref.Kind == kind {
			return ref, true
		}
	}
	return contract.ResourceRef{}, false
}
