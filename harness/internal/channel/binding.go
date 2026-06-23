package channel

import (
	"fmt"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// Transport names the wire a binding uses.
type Transport string

const (
	TransportLocal Transport = "local" // in-process / local socket, trusted header
	TransportHTTP  Transport = "http"  // loopback / network http
	TransportMTLS  Transport = "mtls"  // mutual-TLS authenticated
)

// Verb is a channel operation. The Agent Integration channel exposes observe (Ingest), pull
// (PullProjection), render (read-only cue/content), and status. Replica sync gets separate verbs so
// a sync credential does not inherit Agent Integration access.
type Verb string

const (
	VerbObserve Verb = "observe"
	VerbPull    Verb = "pull"
	VerbRender  Verb = "render"
	VerbStatus  Verb = "status"
	// The sync verb STRINGS are ABI surface owned by contract (sync-abi-v1 §1); these aliases keep
	// the channel's Verb space complete without channel becoming the wire-name authority.
	VerbSyncPush   Verb = contract.SyncVerbPush
	VerbSyncPull   Verb = contract.SyncVerbPull
	VerbSyncStatus Verb = contract.SyncVerbStatus
)

// ChannelBinding is the manifest that scopes ONE principal's access to the channel (D6). The
// channel is the same for every binding; the binding — never a privileged code path — is what
// differs a HostAgent from a ControlAgent. The server still enforces the scope at runtime (S9);
// the binding makes the grant explicit and auditable.
type ChannelBinding struct {
	Principal            contract.ActorID       // the authenticated identity
	ActorKind            contract.ActorKind     // role classification (not a privilege path)
	Transport            Transport              // wire
	Endpoint             string                 // base URL / socket path
	AllowedVerbs         []Verb                 // observe / pull / status
	AllowedObservedTypes []string               // observed event types this principal may Ingest ("" or "*" = any)
	SubscriptionScope    []contract.ResourceRef // the refs this principal may pull
	IdempotencyNamespace string                 // prefix isolating this principal's ExternalIDs (cross-principal dedup isolation)
	Budget               contract.BudgetTier    // context-budget tier for this endpoint's render/pull context (P4); empty = hot (full)
}

// Validate checks the binding is well-formed: a principal, a known kind, at least one verb.
func (b ChannelBinding) Validate() error {
	if strings.TrimSpace(string(b.Principal)) == "" {
		return fmt.Errorf("channel binding requires a principal")
	}
	if b.ActorKind != contract.KindHostAgent && b.ActorKind != contract.KindControlAgent && b.ActorKind != contract.KindReplicaAgent {
		return fmt.Errorf("channel binding actor_kind %q is not host-agent, control-agent, or replica-agent", b.ActorKind)
	}
	if len(b.AllowedVerbs) == 0 {
		return fmt.Errorf("channel binding %q grants no verbs", b.Principal)
	}
	if _, err := contract.ResolveBudgetTier(b.Budget); err != nil {
		return fmt.Errorf("channel binding %q: %w", b.Principal, err)
	}
	return nil
}

// Allows reports whether the binding grants verb.
func (b ChannelBinding) Allows(v Verb) bool {
	for _, av := range b.AllowedVerbs {
		if av == v {
			return true
		}
	}
	return false
}

// AllowsObservedType reports whether the binding permits Ingesting an observation of eventType.
// An empty AllowedObservedTypes (or a "*" entry) means any observed type.
func (b ChannelBinding) AllowsObservedType(eventType string) bool {
	if len(b.AllowedObservedTypes) == 0 {
		return true
	}
	for _, t := range b.AllowedObservedTypes {
		if t == "*" || t == eventType {
			return true
		}
	}
	return false
}

// HostAgentBinding and ControlAgentBinding are the two canonical bindings over the SAME channel —
// the role differs ONLY by the binding (zero new surface for the control agent, D6).
func HostAgentBinding(principal contract.ActorID, endpoint string, scope []contract.ResourceRef) ChannelBinding {
	return ChannelBinding{
		Principal: principal, ActorKind: contract.KindHostAgent, Transport: TransportHTTP, Endpoint: endpoint,
		AllowedVerbs: []Verb{VerbObserve, VerbPull, VerbRender, VerbStatus}, SubscriptionScope: scope,
		IdempotencyNamespace: "host:" + string(principal),
	}
}

func ControlAgentBinding(principal contract.ActorID, endpoint string, scope []contract.ResourceRef) ChannelBinding {
	return ChannelBinding{
		Principal: principal, ActorKind: contract.KindControlAgent, Transport: TransportHTTP, Endpoint: endpoint,
		AllowedVerbs: []Verb{VerbObserve, VerbPull, VerbRender, VerbStatus}, SubscriptionScope: scope,
		IdempotencyNamespace: "control:" + string(principal),
	}
}

func ReplicaAgentBinding(principal contract.ActorID, endpoint string, scope []contract.ResourceRef) ChannelBinding {
	return ChannelBinding{
		Principal: principal, ActorKind: contract.KindReplicaAgent, Transport: TransportHTTP, Endpoint: endpoint,
		AllowedVerbs: []Verb{VerbSyncPush, VerbSyncPull, VerbSyncStatus}, SubscriptionScope: scope,
		IdempotencyNamespace: "replica:" + string(principal),
	}
}

// scopeSet indexes the binding's SubscriptionScope for membership checks.
func (b ChannelBinding) scopeSet() map[contract.ResourceRef]bool {
	allowed := make(map[contract.ResourceRef]bool, len(b.SubscriptionScope))
	for _, ref := range b.SubscriptionScope {
		allowed[ref] = true
	}
	return allowed
}

// ClampRefs clamps a requested ref set to the binding's SubscriptionScope — the team-scale
// authorization ceiling, implemented ONCE for pull / sync / status (hand-rolled copies had already
// diverged on empty-scope handling). The one implementation is contract.ClampRefs — shared with the
// standalone hub (syncserver), which cannot import channel — and this method DELEGATES to it: empty
// requested defaults to the full scope; any explicit ref outside the scope is an error; an EMPTY
// scope denies every explicit ref (fail closed). The ingest path keeps its documented exception (an
// observation naming no refs is unconstrained) at its own call site.
func (b ChannelBinding) ClampRefs(requested []contract.ResourceRef) ([]contract.ResourceRef, error) {
	return contract.ClampRefs(b.Principal, b.SubscriptionScope, requested)
}
