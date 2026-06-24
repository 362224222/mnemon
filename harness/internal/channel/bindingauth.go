package channel

import (
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/eventview"
)

// BindingSet indexes the channel bindings by principal. It is the in-memory authorizer source for
// the walking skeleton (P2.1): the smallest runtime control that turns the ChannelBinding manifest
// into enforcement, without yet committing a binding-file schema (P3). The actor kind rides the
// binding, never a separate code path (D6) — the authorizer branches on verbs/types/scope, not on
// the role.
type BindingSet struct {
	byPrincipal map[contract.ActorID]ChannelBinding
}

// NewBindingSet validates each binding and indexes them by principal. Duplicate principals and
// colliding idempotency namespaces are rejected (the namespace isolates a principal's ExternalIDs).
func NewBindingSet(bindings ...ChannelBinding) (*BindingSet, error) {
	byPrincipal := make(map[contract.ActorID]ChannelBinding, len(bindings))
	namespaces := make(map[string]contract.ActorID, len(bindings))
	for _, b := range bindings {
		if err := b.Validate(); err != nil {
			return nil, err
		}
		if _, dup := byPrincipal[b.Principal]; dup {
			return nil, fmt.Errorf("duplicate channel binding for principal %q", b.Principal)
		}
		if ns := b.IdempotencyNamespace; ns != "" {
			if owner, clash := namespaces[ns]; clash {
				return nil, fmt.Errorf("idempotency namespace %q is bound to both %q and %q", ns, owner, b.Principal)
			}
			namespaces[ns] = b.Principal
		}
		byPrincipal[b.Principal] = b
	}
	return &BindingSet{byPrincipal: byPrincipal}, nil
}

// Binding returns the principal's binding (and whether one exists).
func (s *BindingSet) Binding(principal contract.ActorID) (ChannelBinding, bool) {
	b, ok := s.byPrincipal[principal]
	return b, ok
}

// authorizedAPI wraps a ServerAPI with BindingSet enforcement. It checks the binding-level grant
// (principal/verb/observed-type/scope) and then DELEGATES to the inner API, which still enforces the
// engine-level invariants (S9 principal==subscription, S9/R11 internal-only suffix reject). The
// authorizer is additive: it never replaces the inner trust boundary.
type authorizedAPI struct {
	inner    ServerAPI
	bindings *BindingSet
}

// NewAuthorizedAPI returns inner gated by bindings. With a nil/empty BindingSet, callers should use
// inner directly (an unbound, trusted in-process owner such as the embedded coreengine path).
func NewAuthorizedAPI(inner ServerAPI, bindings *BindingSet) ServerAPI {
	return &authorizedAPI{inner: inner, bindings: bindings}
}

func (a *authorizedAPI) Ingest(principal contract.ActorID, env contract.ObservationEnvelope) (int64, bool, error) {
	b, ok := a.bindings.Binding(principal)
	if !ok {
		return 0, false, fmt.Errorf("no channel binding for principal %q", principal)
	}
	if !b.Allows(VerbObserve) {
		return 0, false, fmt.Errorf("principal %q is not bound to observe", principal)
	}
	if !b.AllowsObservedType(env.Event.Type) {
		return 0, false, fmt.Errorf("principal %q may not observe event type %q", principal, env.Event.Type)
	}
	// The authorizer is the only layer holding the binding, so it clamps any ResourceRefs the
	// observation names to the binding scope (same fail-closed semantics as ClampRefs: an empty
	// scope denies every explicit ref). The ONE documented exception: an observation that names
	// no refs is unconstrained here — the rule pre-gate still derives the in-scope target.
	if len(env.Event.ResourceRefs) > 0 {
		allowed := b.scopeSet()
		for _, r := range env.Event.ResourceRefs {
			if !allowed[r] {
				return 0, false, fmt.Errorf("principal %q observation ref %s/%s is outside its binding scope", principal, r.Kind, r.ID)
			}
		}
	}
	return a.inner.Ingest(principal, env)
}

func (a *authorizedAPI) PullEventView(principal contract.ActorID, sub contract.Subscription) (eventview.EventView, error) {
	b, ok := a.bindings.Binding(principal)
	if !ok {
		return eventview.EventView{}, fmt.Errorf("no channel binding for principal %q", principal)
	}
	if !b.Allows(VerbPull) {
		return eventview.EventView{}, fmt.Errorf("principal %q is not bound to pull", principal)
	}
	// Clamp to the binding's SubscriptionScope (ClampRefs — the one scope clamp): an empty request
	// defaults to the whole scope — the auditable narrowing ceiling, not the broader engine
	// cfg.Subs the inner would otherwise fall back to. The inner still intersects with the
	// server-side subs, so the effective scope is subs ∩ binding.
	refs, err := b.ClampRefs(sub.Refs)
	if err != nil {
		return eventview.EventView{}, err
	}
	sub.Refs = refs
	return a.inner.PullEventView(principal, sub)
}

var _ ServerAPI = (*authorizedAPI)(nil)
