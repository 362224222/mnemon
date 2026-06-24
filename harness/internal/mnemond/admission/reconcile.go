package admission

import (
	"encoding/json"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/state"
)

// isProposal reports whether an event is a proposed operation the reconciler should try to apply.
// The event log carries BOTH observations and proposals; only proposals (by the "*.proposed" type
// convention) become KernelOps. Observations are consumed without a decision. A "*.proposed" event that
// carries no decodable writes is a MALFORMED proposal and is still Rejected by the kernel (not skipped).
func isProposal(ev contract.Event) bool { return strings.HasSuffix(ev.Type, ".proposed") }

type Reconciler struct {
	store  *state.Store
	kernel *state.Kernel
	cursor int64
}

// NewReconciler seeds its cursor from the durable decision log (Store.MaxDecidedSeq), so a process
// restart resumes after the last consumed event instead of re-reading the log from 0 and re-deciding
// already-accepted events (which would pollute pull feedback). The decision log is the cursor.
//
// The liveness-escalation counter (Invariant #10) is NOT kept in memory either — it is derived per event
// from the durable log (Store.DeferralCount), so escalation survives restart exactly as the cursor does.
func NewReconciler(s *state.Store, k *state.Kernel) *Reconciler {
	return &Reconciler{store: s, kernel: k, cursor: s.MaxDecidedSeq()}
}

// opFromEvent builds the KernelOp from a TRUSTED event. Actor and read-set come from the event envelope
// which the admission path stamped from trusted sources (registry binding + the dispatched scoped view),
// NEVER from callback-controlled payload (trust-boundary fix, Invariants #13/#15).
//
// Deviation from the plan's literal `ev.Payload["writes"].([]contract.ResourceWrite)`: that type-assert
// PANICS after the AppendEvent->PendingEvents JSON round-trip (Payload["writes"] decodes to []any, not the
// typed slice). We re-marshal+unmarshal the payload's "writes" into typed ResourceWrite — round-trip-safe
// and behaviorally identical for the typed fixtures.
func opFromEvent(ev contract.Event) contract.KernelOp {
	var writes []contract.ResourceWrite
	if raw, ok := ev.Payload["writes"]; ok {
		b, _ := json.Marshal(raw)
		if err := json.Unmarshal(b, &writes); err != nil {
			writes = nil // malformed payload -> no writes -> kernel rejects it (never a phantom Accepted no-op, #3)
		}
	}
	return contract.KernelOp{OpID: ev.ID, Actor: ev.Actor, Writes: writes, ReadSet: ev.BasedOn, IngestSeq: ev.IngestSeq, CorrelationID: ev.CorrelationID}
}

func (r *Reconciler) RunOnce(modes contract.Modes) []contract.Decision {
	var out []contract.Decision
	evs, err := r.store.PendingEvents(r.cursor)
	if err != nil {
		// A corrupt ingest log is FAIL-STOP: do not advance the cursor or manufacture decisions from a
		// partial/garbage read. Note this is a hard stop — one corrupt row blocks reconciliation until it
		// is repaired (no skip/quarantine). RunOnce cannot surface the error without a signature change; the
		// durable signal is the error returned by Store.PendingEvents itself. When core is wired into a
		// runtime, that caller should call PendingEvents (or a RunOnce that returns an error) to detect this.
		return out
	}
	for _, ev := range evs { // strictly IngestSeq order (Invariant #9)
		if !isProposal(ev) {
			r.cursor = ev.IngestSeq // observation: consumed, but not a write attempt — no decision (R2#2)
			continue
		}
		call := modes
		// Escalate BEFORE Apply (so the persisted decision is terminal, #10). The deferral count is read
		// from the durable log, not in-memory, so a restart cannot silently reset the escalation clock.
		// Escalation requires a DECLARED CorrelationID: it is the only stable key that groups a proposal's
		// retries. An event with no CorrelationID opts out of retry-grouping — it never escalates and never
		// contributes to another group's count (the "" bucket is written but never read). This avoids both
		// the empty-bucket collision and the per-event-ID "never groups" failure of a naive event-ID fallback.
		if call.Conflict == contract.ConflictRebase && ev.CorrelationID != "" && r.store.DeferralCount(ev.CorrelationID) >= 2 {
			call.Conflict = contract.ConflictDeferToHuman
		}
		d := r.kernel.Apply(opFromEvent(ev), call) // kernel is the serializer, not us (Invariant #2)
		out = append(out, d)
		r.cursor = ev.IngestSeq
	}
	return out
}
