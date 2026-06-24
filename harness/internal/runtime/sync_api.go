package runtime

import (
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/channel"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub"
)

// The runtime's sync verbs are the CO-HOSTED hub form: the same mnemonhub adjudication mnemon-hub
// hosts standalone, authorized here by adapting the channel bindings to replica grants (the
// dual-form rule, sync-abi-v1 §2). Zero hub logic lives in the runtime anymore — only the adapter.

func (r *Runtime) SyncPush(principal contract.ActorID, req contract.SyncPushRequest) (contract.SyncPushResponse, error) {
	return r.syncHub().Push(principal, req)
}

func (r *Runtime) SyncPull(principal contract.ActorID, req contract.SyncPullRequest) (contract.SyncPullResponse, error) {
	return r.syncHub().Pull(principal, req)
}

func (r *Runtime) SyncStatus(principal contract.ActorID) (contract.SyncStatusResponse, error) {
	return r.syncHub().Status(principal)
}

// syncHub builds the hub view over the runtime's open store. It is stateless (adjudication and
// counters are durable in the store), so a per-call construction is correct and cheap.
func (r *Runtime) syncHub() *mnemonhub.Server {
	return mnemonhub.New(r.store, bindingGrants{bindings: r.bindings}, r.cs.now)
}

// bindingGrants adapts the runtime's channel bindings to the mnemonhub Grants seam with the EXACT
// pre-extraction requireSyncBinding semantics (zero behavior change): a grant exists iff the
// principal has a binding, the binding's kind is replica-agent, and it allows the verb. The grant
// scope is the binding's SubscriptionScope.
type bindingGrants struct {
	bindings *channel.BindingSet
}

func (g bindingGrants) Grant(principal contract.ActorID, verb string) (contract.ReplicaGrant, bool) {
	if g.bindings == nil {
		return contract.ReplicaGrant{}, false
	}
	b, ok := g.bindings.Binding(principal)
	if !ok || b.ActorKind != contract.KindReplicaAgent || !b.Allows(channel.Verb(verb)) {
		return contract.ReplicaGrant{}, false
	}
	// Fail closed on an empty sync scope (parity with mnemon-hub's replicas.json gate,
	// replicas.go:80). An empty grant scope would otherwise reach RemoteSyncedEventsAfter, whose
	// "no scope filter = serve all" SQL bypasses scope authorization — an empty-scope replica
	// binding must grant NOTHING, never the whole hub log. ClampRefs already denies explicit refs
	// under an empty scope; this closes the EMPTY-requested-defaults-to-empty-scope hole at the
	// grant boundary before SQL.
	if len(b.SubscriptionScope) == 0 {
		return contract.ReplicaGrant{}, false
	}
	return contract.ReplicaGrant{Principal: principal, Scopes: b.SubscriptionScope}, true
}

// clampSyncScopes delegates to the binding's ONE scope clamp (channel.ChannelBinding.ClampRefs,
// itself a delegate of contract.ClampRefs — the implementation the extracted hub shares). The live
// sync pull path now clamps inside mnemonhub.Pull against the adapted grant scope (= this binding
// scope), so this helper remains as the runtime-level pin of the binding-clamp semantics.
// TIGHTENING vs the prior hand-rolled copy: an empty-scope replica binding used to pass explicit
// requested refs through unchecked — and this was the only enforcement on the sync path before
// SQL. ClampRefs denies explicit refs under an empty scope (fail closed).
func clampSyncScopes(binding channel.ChannelBinding, requested []contract.ResourceRef) ([]contract.ResourceRef, error) {
	scopes, err := binding.ClampRefs(requested)
	if err != nil {
		return nil, fmt.Errorf("sync scope: %w", err)
	}
	return scopes, nil
}
