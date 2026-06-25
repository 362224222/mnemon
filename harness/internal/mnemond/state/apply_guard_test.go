package state

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// #3: an op with zero writes must NOT be committed as an Accepted no-op (it mutated nothing, so
// "accepted" is a phantom success that pollutes the decision log). It is Rejected, terminal.
func TestEmptyWritesOpIsRejected(t *testing.T) {
	k := newMaterializer(t)
	d := k.Apply(contract.StateOp{OpID: "empty", Actor: "user"}, p0Modes()) // no Writes
	if d.Status == contract.Accepted {
		t.Fatal("empty op must NOT be an Accepted no-op")
	}
	if d.Status != contract.Rejected || d.NextAction != "" {
		t.Fatalf("empty op must be Rejected/'' (terminal), got %s/%q", d.Status, d.NextAction)
	}
	if k.Store().DecisionCount() != 1 {
		t.Fatalf("exactly one decision persisted, got %d", k.Store().DecisionCount())
	}
}

// #3 (hardening): a non-empty write with an invalid OpKind (e.g. a zero-value write from a {"writes":[{}]}
// payload) must be Rejected with a reason that NAMES the malformed op kind — caught up-front, not
// incidentally via an authz error on an empty resource kind.
func TestMalformedWriteKindIsRejectedWithClearReason(t *testing.T) {
	k := newMaterializer(t)
	d := k.Apply(contract.StateOp{OpID: "z", Actor: "user", Writes: []contract.ResourceWrite{{}}}, p0Modes())
	if d.Status != contract.Rejected || d.NextAction != "" {
		t.Fatalf("malformed write must be Rejected/'' (terminal), got %s/%q", d.Status, d.NextAction)
	}
	if !strings.Contains(d.Reason, "op kind") {
		t.Fatalf("reason should name the malformed op kind, got %q", d.Reason)
	}
}
