package runtime

import (
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

// Live-store sync passthroughs (v1.1 #2): the in-process sync worker operates the ALREADY-OPEN
// runtime store through these — never a second OpenStore/OpenRuntime by path, which would
// self-collide on the single-writer flock. Together they satisfy exchange.LiveStore, so the
// worker reuses the exact helpers the offline CLI verbs use, just over the live handle.

// ReplicaID is this store's durable replica identity (minted on first read).
func (r *Runtime) ReplicaID() (string, error) { return r.store.ReplicaID() }

// PendingSyncedEvents reads the outbound sync ledger's pending synced envelopes.
func (r *Runtime) PendingSyncedEvents() ([]eventmodel.EventEnvelope, error) {
	return r.store.PendingSyncedEvents()
}

// MarkSyncedEventStatus mirrors a hub verdict into the local sync ledger (pusher-side attribution).
func (r *Runtime) MarkSyncedEventStatus(originMnemond, eventID string, subject eventmodel.EventSubject, status, remotePeerID, at, diagnostic string) error {
	return r.store.MarkSyncedEventStatus(originMnemond, eventID, subject, status, remotePeerID, at, diagnostic)
}

// GetCursor / SetCursor expose the durable cursor surface for the SYNC cursor names only
// ("sync_pull:<remote>"). The dispatch/sink cursors belong to the ControlServer — driving them from
// outside would corrupt the governed loop; mnemonhub exchange helpers are the intended callers.
func (r *Runtime) GetCursor(name string) int64            { return r.store.GetCursor(name) }
func (r *Runtime) SetCursor(name string, seq int64) error { return r.store.SetCursor(name, seq) }

// IngestTrusted is the in-process OWNER's intake: the co-hosted sync worker imports pulled events
// under contract.SyncImportActor through it. It bypasses ONLY the channel-binding authorizer —
// sync@local is a platform actor, never a wire principal, so it holds no binding — while the full
// intake trust boundary still applies unchanged (observed-type grammar, internal-only suffix
// reject, exactly-once dedupe, rule pre-gate, kernel authority). Never exposed on the wire.
func (r *Runtime) IngestTrusted(principal contract.ActorID, env contract.ObservationEnvelope) (int64, bool, error) {
	return r.cs.Ingest(principal, env)
}
