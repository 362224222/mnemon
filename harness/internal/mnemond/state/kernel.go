package state

import (
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

type Kernel struct {
	store  *Store
	schema SchemaGuard
	rules  AuthorityRules
}

func NewKernel(s *Store, g SchemaGuard, r AuthorityRules) *Kernel {
	return &Kernel{store: s, schema: g, rules: r}
}
func (k *Kernel) Store() *Store { return k.store }

// Apply is the ONLY canonical writer (Invariant #2). check+write are one atomic txn (Invariant #3);
// multi-resource is all-or-nothing (Invariant #5). It persists exactly one terminal decision (Invariant #7):
// the accept is written INSIDE the writes txn (crash-safe); non-accepts are written in their own txn.
func (k *Kernel) Apply(op contract.KernelOp, m contract.Modes) contract.Decision {
	d := contract.Decision{DecisionID: "dec_" + uuid.NewString(), OpID: op.OpID, Actor: op.Actor, IngestSeq: op.IngestSeq, CorrelationID: op.CorrelationID}
	var newVers []contract.ResourceVersion
	var newResources []contract.ResourceSnapshot
	var conflicts []contract.Conflict

	// A write-op must write at least one resource, and every write must name a supported op kind. A
	// malformed/undecodable proposal (no writes, or a zero-value write whose Kind is "") must NOT be
	// rubber-stamped Accepted as a phantom no-op, nor rejected with an incidental authz reason
	// (review finding #3). Reject it terminally up-front with a clear reason — rebase can't fix it.
	if len(op.Writes) == 0 {
		d.Status, d.NextAction, d.Reason = contract.Rejected, "", "empty op: no writes"
		_ = k.store.AppendDecision(d)
		return d
	}
	// Every write must name a supported op kind, and the writes must target DISTINCT resources. Aliasing one
	// ref twice in a single op would apply sequentially with last-write-wins and report two NewVersions for one
	// resource — degenerating multi-RESOURCE all-or-nothing (Invariant #5) into a self-cancelling op (e.g. a
	// budget reserve+reset that launders the spend ceiling, S6). Reject both terminally up-front: rebase can't
	// fix a malformed op.
	seen := make(map[contract.ResourceRef]bool, len(op.Writes))
	for _, w := range op.Writes {
		if w.Kind != contract.OpCreate && w.Kind != contract.OpUpdate {
			d.Status, d.NextAction, d.Reason = contract.Rejected, "", "malformed op: unsupported op kind \""+string(w.Kind)+"\""
			_ = k.store.AppendDecision(d)
			return d
		}
		if seen[w.Ref] {
			d.Status, d.NextAction, d.Reason = contract.Rejected, "", "malformed op: duplicate write to "+string(w.Ref.Kind)+"/"+string(w.Ref.ID)+" (multi-write must target distinct resources, #5)"
			_ = k.store.AppendDecision(d)
			return d
		}
		seen[w.Ref] = true
	}

	err := k.store.WithTx(func(tx *Tx) error {
		if m.Isolation == contract.IsolationEventViewReadSet { // read-set validation (Invariant #6)
			for _, rv := range op.ReadSet {
				cur, e := tx.ReadVersion(rv.Ref)
				if e != nil {
					return e
				}
				if cur != rv.Version {
					conflicts = append(conflicts, contract.Conflict{Ref: rv.Ref, ExpectedVersion: rv.Version, ActualVersion: cur, Kind: contract.ReadStale})
				}
			}
			if len(conflicts) > 0 {
				return &conflictError{conflicts}
			}
		}
		for _, w := range op.Writes {
			if e := k.schema.Validate(w.Ref.Kind, w.Fields); e != nil {
				return e
			} // -> errSchema
			if e := k.rules.Enforce(op.Actor, w.Ref.Kind); e != nil {
				return e
			} // -> errAuthz (no version!)
			switch w.Kind {
			case contract.OpCreate:
				if e := tx.CreateResource(w.Ref, w.Fields); e != nil {
					cur, _ := tx.ReadVersion(w.Ref)
					conflicts = append(conflicts, contract.Conflict{Ref: w.Ref, ExpectedVersion: 0, ActualVersion: cur, Kind: contract.WriteWrite})
					return &conflictError{conflicts}
				}
			case contract.OpUpdate:
				ok, e := tx.CASUpdate(w.Ref, w.BasedOn, w.Fields)
				if e != nil {
					return e
				}
				if !ok {
					cur, _ := tx.ReadVersion(w.Ref)
					conflicts = append(conflicts, contract.Conflict{Ref: w.Ref, ExpectedVersion: w.BasedOn, ActualVersion: cur, Kind: contract.WriteWrite})
					return &conflictError{conflicts}
				}
			default:
				return errors.New("unsupported op kind " + string(w.Kind)) // no phantom-accept (go-correctness fix)
			}
			cur, _ := tx.ReadVersion(w.Ref) // derive resulting version from the store, not arithmetic (Invariant #4)
			newVers = append(newVers, contract.ResourceVersion{Ref: w.Ref, Version: cur})
			newResources = append(newResources, contract.ResourceSnapshot{Ref: w.Ref, Version: cur, Fields: w.Fields})
		}
		// ACCEPTED: persist the decision in the SAME txn (crash-safe atomicity, Invariant #7)
		d.Status = contract.Accepted
		d.AppliedAt = time.Now().UTC().Format(time.RFC3339)
		d.NewVersions = newVers
		d.NewResources = newResources
		if err := tx.AppendDecisionTx(d); err != nil {
			return err
		}
		return tx.RecordAcceptedEventEnvelopesTx(d)
	})
	if err == nil {
		return d
	}

	// NON-ACCEPT: map error class -> terminal status/next-action, then persist in its own txn.
	var ce *conflictError
	switch {
	case errors.As(err, &ce): // CAS / read-stale conflict -> conflict-mode mapping
		d.Conflicts = ce.conflicts
		switch m.Conflict {
		case contract.ConflictReject:
			d.Status, d.NextAction, d.Reason = contract.Rejected, "", "conflict (reject mode)"
		case contract.ConflictDeferToHuman:
			d.Status, d.NextAction, d.Reason = contract.Deferred, "human_review", "conflict (defer_to_human)"
		case contract.ConflictAutoMergeDisjoint: // FAIL-CLOSED until implemented (trust-boundary fix)
			d.Status, d.NextAction, d.Reason = contract.Deferred, "human_review", "mode auto_merge_disjoint not implemented"
		default: // rebase
			d.Status, d.NextAction, d.Reason = contract.Deferred, "rebase", "conflict (rebase)"
		}
	case errors.Is(err, errSchema), errors.Is(err, errAuthz): // policy rejection -> rebase can't fix
		d.Status, d.NextAction, d.Reason = contract.Rejected, "", err.Error()
	default: // raw IO error -> Rejected, not a bogus "rebase me"
		d.Status, d.NextAction, d.Reason = contract.Rejected, "", err.Error()
	}
	_ = k.store.AppendDecision(d)
	return d
}
