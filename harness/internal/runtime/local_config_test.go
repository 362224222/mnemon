package runtime

import (
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/kernel"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
)

// localRuntimeConfigT mirrors app.LocalRuntimeConfigFromBindings for the runtime-level integration
// tests, which exercise the capability rules end-to-end through the runtime (and assert on runtime
// internals). The production derivation lives in app; this keeps the test in package runtime without
// importing app (which would cycle).
func localRuntimeConfigT(bindings []access.ChannelBinding) RuntimeConfig {
	var rules []admission.Rule
	allow := map[contract.ActorID][]contract.ResourceKind{}
	for _, b := range bindings {
		if b.Allows(access.VerbObserve) && b.AllowsObservedType(policy.MemoryWriteCandidateObserved) {
			if ref, ok := scopeRefT(b, "memory"); ok {
				rules = append(rules, policy.EmbeddedCatalog()["memory"].Rule(b.Principal, ref, policy.Limits{}))
			}
		}
		if b.Allows(access.VerbObserve) && b.AllowsObservedType(policy.SkillWriteCandidateObserved) {
			if ref, ok := scopeRefT(b, "skill"); ok {
				rules = append(rules, policy.EmbeddedCatalog()["skill"].Rule(b.Principal, ref, policy.Limits{}))
			}
		}
		if b.ActorKind != contract.KindHostAgent {
			continue
		}
		seen := map[contract.ResourceKind]bool{}
		for _, ref := range b.SubscriptionScope {
			if ref.Kind == "memory" || ref.Kind == "skill" {
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
		Authority:     kernel.AuthorityRules{Allow: allow},
		SchemaGuard:   kernel.SchemaGuardWith(map[contract.ResourceKind][]string{"memory": {"content"}, "skill": {"name"}}),
		SyncableKinds: policy.ImportableKinds(policy.EmbeddedCatalog()),
	}
}

func scopeRefT(b access.ChannelBinding, kind contract.ResourceKind) (contract.ResourceRef, bool) {
	for _, ref := range b.SubscriptionScope {
		if ref.Kind == kind {
			return ref, true
		}
	}
	return contract.ResourceRef{}, false
}
