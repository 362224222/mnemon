package admission

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// ---- Arm C — determinism: identical fresh-store fixture, run twice, element-wise identical decisions ----
func TestArmC_Determinism(t *testing.T) {
	run := func() []contract.Decision {
		s, k := newRecon(t)
		X := contract.ResourceRef{Kind: "memory", ID: "X"}
		seedCreate(t, k, X, map[string]any{"content": "v0"})
		appendProposal(t, s, updateProposal("e1", "a1", "c1", X, 1, map[string]any{"content": "a1"}, nil))
		appendProposal(t, s, updateProposal("e2", "a2", "c2", X, 1, map[string]any{"content": "a2"}, nil))
		return NewReconciler(s, k).RunOnce(casModes())
	}
	d1 := run()
	d2 := run()
	if len(d1) != len(d2) {
		t.Fatalf("length mismatch %d vs %d", len(d1), len(d2))
	}
	for i := range d1 {
		// DecisionID/AppliedAt are intentionally NOT compared (uuid/timestamp); Status/OpID/Conflicts are the deterministic core.
		if d1[i].Status != d2[i].Status || d1[i].OpID != d2[i].OpID {
			t.Fatalf("non-deterministic at %d: %+v vs %+v", i, d1[i], d2[i])
		}
		if len(d1[i].Conflicts) != len(d2[i].Conflicts) {
			t.Fatalf("conflict count differs at %d", i)
		}
		for j := range d1[i].Conflicts {
			if d1[i].Conflicts[j] != d2[i].Conflicts[j] {
				t.Fatalf("conflict differs at %d/%d: %+v vs %+v", i, j, d1[i].Conflicts[j], d2[i].Conflicts[j])
			}
		}
	}
}
