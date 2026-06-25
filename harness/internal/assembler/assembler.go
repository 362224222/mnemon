// Package assembler compiles selected event packages plus channel bindings into a runtime config.
// It only SELECTS already-compiled packages from the provided registry (resolved via
// native:<id> rule_ref); an unknown package id fails closed. Config can never define new behavior.
package assembler

import (
	"fmt"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/config"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/admission"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/state"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

// Assemble derives the Local Mnemon runtime config from the enabled event packages in cfg and the
// installed channel bindings. For each enabled package it resolves the descriptor by rule_ref
// from catalog (fail-closed on an unknown id), then builds one actor-bound rule per binding that may
// observe the package's type, granting that principal kernel write authority for the resource kind.
//
// catalog selects the package universe; nil means policy.StandardRegistry(). The boot path passes
// the merged policy.ResolveRegistry result when external packages are present.
//
// Divergence from the locked Assemble(cfg, loops) signature (code wins): the runtime config needs the
// channel bindings (principals/scope), which the loop manifests do not carry; bindings are the second
// argument. This is the production boot path: app.OpenLocalRuntime derives the config.File from the
// setup-written loops list and assembles here.
func Assemble(cfg config.File, bindings []access.ChannelBinding, catalog policy.Registry) (runtime.RuntimeConfig, error) {
	if catalog == nil {
		catalog = policy.StandardRegistry()
	}
	var rules []admission.Rule
	allow := map[contract.ActorID][]contract.ResourceKind{}
	// The live kernel's schema guard is the governance core (state.DefaultSchemaGuard) PLUS each
	// enabled package's declared required header — so a declared user kind has ONE source, the
	// compiled event package. DefaultSchemaGuard returns a fresh map per call; add-only registration
	// keeps a compiled kind's hand-written required while the transitional default still carries it.
	guard := state.DefaultSchemaGuard()
	for name, cc := range cfg.EventPackages {
		if !cc.Enabled {
			continue
		}
		const nativePrefix = "native:"
		if !strings.HasPrefix(cc.RuleRef, nativePrefix) {
			return runtime.RuntimeConfig{}, fmt.Errorf("event package %q: rule_ref %q must be %q-prefixed (fail-closed)", name, cc.RuleRef, nativePrefix)
		}
		id := strings.TrimPrefix(cc.RuleRef, nativePrefix)
		cap, ok := catalog[id]
		if !ok {
			return runtime.RuntimeConfig{}, fmt.Errorf("event package %q: unknown rule_ref %q (fail-closed)", name, cc.RuleRef)
		}
		if _, known := guard.Required[cap.ResourceKind]; !known {
			guard.Required[cap.ResourceKind] = cap.RequiredHeader
		}
		defRef, err := parseRef(cc.ResourceRef)
		if err != nil {
			return runtime.RuntimeConfig{}, fmt.Errorf("event package %q: %w", name, err)
		}
		for _, b := range bindings {
			// host-agents are the ordinary submitters; control-agents are operators, who submit too —
			// they are the principal a high-risk candidate must be re-submitted as (P3e). Both get an
			// admission rule + kernel write authority; replica-agents (sync) never submit host candidates.
			if b.ActorKind != contract.KindHostAgent && b.ActorKind != contract.KindControlAgent {
				continue
			}
			if !b.Allows(access.VerbObserve) || !b.AllowsObservedType(cap.ObservedType) {
				continue
			}
			ref, ok := refForBinding(b, cap.ResourceKind, defRef)
			if !ok {
				continue // unscoped for this kind: no rule, no authority (it could never pull what it writes)
			}
			rules = append(rules, cap.Rule(b.Principal, ref, policy.Limits{MaxPayloadBytes: cc.MaxPayloadBytes}))
			// Risk gate alongside the admission rule (P3): the gate's deny outranks the admission propose
			// (admission.Evaluate is deny-priority). mid → evidence required; high → the operator-only gate,
			// built ONLY for non-operator (host-agent) principals so an operator (control-agent) is exempt.
			switch cap.Risk {
			case "mid":
				rules = append(rules, policy.RiskEvidenceGate(cap, b.Principal))
			case "high":
				if b.ActorKind != contract.KindControlAgent {
					rules = append(rules, policy.RiskOperatorGate(cap, b.Principal))
				}
			}
			allow[b.Principal] = appendKind(allow[b.Principal], cap.ResourceKind)
		}
	}
	return runtime.RuntimeConfig{
		Bindings:    bindings,
		Subs:        access.SubsFromBindings(bindings),
		Rules:       admission.NewRuleSet(rules...),
		Authority:   state.AuthorityRules{Allow: allow},
		SchemaGuard: guard,
	}, nil
}

// refForBinding picks the binding's admission target for one event package kind: the config-pinned
// default if the binding's scope contains it, else the binding's first ref of that kind, else none
// (an unscoped binding gets no rule — it could never pull what it writes).
func refForBinding(b access.ChannelBinding, kind contract.ResourceKind, def contract.ResourceRef) (contract.ResourceRef, bool) {
	for _, ref := range b.SubscriptionScope {
		if ref == def {
			return ref, true
		}
	}
	for _, ref := range b.SubscriptionScope {
		if ref.Kind == kind {
			return ref, true
		}
	}
	return contract.ResourceRef{}, false
}

func parseRef(s string) (contract.ResourceRef, error) {
	parts := strings.SplitN(s, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return contract.ResourceRef{}, fmt.Errorf("resource_ref %q must be \"<kind>/<id>\"", s)
	}
	return contract.ResourceRef{Kind: contract.ResourceKind(parts[0]), ID: contract.ResourceID(parts[1])}, nil
}

func appendKind(kinds []contract.ResourceKind, kind contract.ResourceKind) []contract.ResourceKind {
	for _, k := range kinds {
		if k == kind {
			return kinds
		}
	}
	return append(kinds, kind)
}
