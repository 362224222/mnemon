package admission

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// One identical Arm-A fixture under three conflict modes -> three distinct, each-internally-deterministic
// loser decisions. The mapping lives in Materializer.Apply (P0.4); this asserts it is wired through reconcile
// and that auto_merge_disjoint is FAIL-CLOSED (never a silent accept/merge).
func TestConflictModeChangesDecisionDeterministically(t *testing.T) {
	rej := runArmA(t, contract.Modes{Conflict: contract.ConflictReject, Isolation: contract.IsolationWriteCAS, Authz: contract.AuthzStrict})
	if rej.loser.Status != contract.Rejected || rej.loser.NextAction != "" {
		t.Fatalf("reject mode: loser must be Rejected/'', got %s/%q", rej.loser.Status, rej.loser.NextAction)
	}
	hum := runArmA(t, contract.Modes{Conflict: contract.ConflictDeferToHuman, Isolation: contract.IsolationWriteCAS, Authz: contract.AuthzStrict})
	if hum.loser.NextAction != "human_review" {
		t.Fatalf("defer_to_human: loser NextAction must be human_review, got %q", hum.loser.NextAction)
	}
	// auto_merge_disjoint is FAIL-CLOSED, never a silent accept/merge
	am := runArmA(t, contract.Modes{Conflict: contract.ConflictAutoMergeDisjoint, Isolation: contract.IsolationWriteCAS, Authz: contract.AuthzStrict})
	if am.loser.Status != contract.Deferred || am.loser.NextAction != "human_review" {
		t.Fatalf("auto_merge_disjoint must fail-closed to human_review, got %s/%q", am.loser.Status, am.loser.NextAction)
	}
	// each run is internally deterministic under a fixed mode
	if runArmA(t, rej.modes).loser.Status != contract.Rejected {
		t.Fatal("non-deterministic under fixed mode")
	}
}
