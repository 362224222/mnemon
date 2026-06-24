package contract

import (
	"fmt"

	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

// Access-facing DTOs shared across the mnemond access/runtime/app layers. They live in contract
// (zero deps) so the access port and the runtime that satisfies it can both name them without a back-edge.

// ActorKind classifies a mnemond access principal by role. It is NOT a privilege path: access is
// the same for every principal; the role differs by binding, never by a privileged code path
// (D6). HostAgent pushes host observations; ControlAgent is an operator/control client;
// ReplicaAgent is the background Remote Workspace sync actor.
type ActorKind string

const (
	KindHostAgent    ActorKind = "host-agent"
	KindControlAgent ActorKind = "control-agent"
	KindReplicaAgent ActorKind = "replica-agent"
)

// SyncImportActor is the well-known principal under which pulled synced events re-enter Event Intake.
// The runtime uses it to skip re-recording synced events for sync-imported decisions; the app sync glue
// drives the import runtime under it.
const SyncImportActor = ActorID("sync@local")

// The three Remote Workspace sync wire verbs (sync-abi-v1 §1). They live in contract because the ABI
// names them: the mnemond access binding layer and the standalone hub (mnemonhub/mnemon-hub) must agree on the
// strings without either importing the other. Deliberately untyped so access can alias them into its
// Verb space unchanged.
const (
	SyncVerbPush   = "sync.push"
	SyncVerbPull   = "sync.pull"
	SyncVerbStatus = "sync.status"
)

// ReplicaGrant is the replica-credential scope record both hub forms share (sync-abi-v1 §2 dual-form
// rule): a co-hosted hub derives it from a replica-agent channel binding entry; mnemon-hub derives it
// from a replicas.json entry. Same fields, same semantics. Token is the optional bearer credential
// (resolved from credential_ref); it is empty when the transport authenticates another way (e.g. the
// co-hosted hub's binding authenticator already resolved the principal).
type ReplicaGrant struct {
	Principal ActorID
	Token     string
	Scopes    []ResourceRef
}

// ClampRefs clamps a requested ref set to a principal's granted scope — the team-scale authorization
// ceiling, implemented ONCE for pull / sync / status (hand-rolled copies had already diverged on
// empty-scope handling). access.ChannelBinding.ClampRefs and the mnemonhub hub both delegate here.
// Empty requested defaults to the full scope; any explicit ref outside the scope is an error; an
// EMPTY scope denies every explicit ref (fail closed). The ingest path keeps its documented exception
// (an observation naming no refs is unconstrained) at its own call site.
func ClampRefs(principal ActorID, scope, requested []ResourceRef) ([]ResourceRef, error) {
	if len(requested) == 0 {
		return append([]ResourceRef(nil), scope...), nil
	}
	allowed := make(map[ResourceRef]bool, len(scope))
	for _, ref := range scope {
		allowed[ref] = true
	}
	for _, ref := range requested {
		if !allowed[ref] {
			return nil, fmt.Errorf("ref %s/%s is outside principal %q binding scope", ref.Kind, ref.ID, principal)
		}
	}
	return append([]ResourceRef(nil), requested...), nil
}

// ChannelStatus is the principal's channel status surface (digest + scope counts + sync state).
type ChannelStatus struct {
	Principal     ActorID   `json:"principal"`
	Digest        string    `json:"digest"`
	Resources     int       `json:"resources"`
	ActorKind     ActorKind `json:"actor_kind,omitempty"`
	StoreRef      string    `json:"store_ref"`
	Mode          string    `json:"mode"`
	SyncPending   int       `json:"sync_pending"`
	SyncSynced    int       `json:"sync_synced"`
	SyncConflicts int       `json:"sync_conflicts"`
}

// Sync{Push,Pull,Status} request/response DTOs for the Remote Workspace sync verbs.

type SyncPushRequest struct {
	ReplicaID string                     `json:"replica_id"`
	BatchID   string                     `json:"batch_id"`
	Events    []eventmodel.EventEnvelope `json:"events"`
}

type SyncPushResponse struct {
	Accepted   []EventExchangeResult `json:"accepted"`
	Rejected   []EventExchangeResult `json:"rejected"`
	Conflicts  []EventExchangeResult `json:"conflicts"`
	NextCursor string                `json:"next_cursor,omitempty"`
}

type SyncPullRequest struct {
	ReplicaID    string        `json:"replica_id"`
	RemoteCursor string        `json:"remote_cursor"`
	Scopes       []ResourceRef `json:"scopes"`
}

type SyncPullResponse struct {
	Events      []eventmodel.EventEnvelope `json:"events"`
	Diagnostics []EventExchangeResult      `json:"diagnostics"`
	NextCursor  string                     `json:"next_cursor"`
}

// SyncStatusResponse carries the hub-side event exchange evidence: total synced envelopes accepted
// into the hub log, total synced envelopes returned across pulls, and the last next_cursor served
// to each replica principal.
type SyncStatusResponse struct {
	Principal         ActorID           `json:"principal"`
	RemoteWorkspace   string            `json:"remote_workspace"`
	HubEventsReceived int64             `json:"hub_events_received"`
	HubEventsServed   int64             `json:"hub_events_served"`
	HubReplicaCursors map[string]string `json:"hub_replica_cursors,omitempty"`
}

type EventExchangeResult struct {
	OriginMnemond string                  `json:"origin_mnemond"`
	EventID       string                  `json:"event_id"`
	Subject       eventmodel.EventSubject `json:"subject"`
	Status        string                  `json:"status"`
	Diagnostic    string                  `json:"diagnostic,omitempty"`
}
