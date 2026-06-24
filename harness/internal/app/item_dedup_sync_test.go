package app

import (
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// P3f: a coordination kind (assignment) syncs via the GENERIC item-dedup strategy — the import
// preserves EVERY item field (scope/ttl/assignee), which entry-dedup (content-only) would lose. This
// is the §577 generic append-merge that makes the AgentTeam nouns syncable.
func TestItemDedupImportPreservesAllFields(t *testing.T) {
	ref := contract.ResourceRef{Kind: "assignment", ID: "project"}
	rt, err := OpenSyncImportRuntime(filepath.Join(t.TempDir(), "id.db"), []contract.ResourceRef{ref}, nil)
	if err != nil {
		t.Fatalf("open import runtime: %v", err)
	}
	defer rt.Close()

	material := contract.SyncedEventMaterial{
		OriginReplicaID: "remote-a",
		LocalDecisionID: "dec-1",
		LocalIngestSeq:  5,
		Actor:           "codex@remote",
		ResourceRef:     ref,
		ResourceVersion: 1,
		Fields: map[string]any{
			"items": []any{map[string]any{
				"id": "remote/remote-a/dec-1", "scope": "fix the projector", "ttl": "2h",
				"assignee": "codex@impl", "expected_work": "fix the projector",
				"expected_feedback": "summary and blockers", "evidence": "PR-42", "actor": "codex@remote", "ingest_seq": float64(5),
			}},
			"content":    "# Assignments\n- fix the projector",
			"updated_by": "codex@remote",
		},
	}
	if _, _, err := rt.API().Ingest(contract.SyncImportActor, contract.ObservationEnvelope{
		ExternalID: "imp1",
		Event: contract.Event{
			Type:    "assignment.remote_synced_event.observed",
			Payload: map[string]any{"material": material},
		},
	}); err != nil {
		t.Fatalf("ingest remote assignment material: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick: %v", err)
	}

	_, fields, err := rt.Resource(ref)
	if err != nil {
		t.Fatalf("read assignment: %v", err)
	}
	items, ok := fields["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("import must write one assignment item, got %+v", fields)
	}
	item, _ := items[0].(map[string]any)
	for k, want := range map[string]string{
		"scope": "fix the projector", "ttl": "2h", "assignee": "codex@impl",
		"expected_work": "fix the projector", "expected_feedback": "summary and blockers", "evidence": "PR-42",
	} {
		if got, _ := item[k].(string); got != want {
			t.Fatalf("item-dedup must preserve %q: got %q, want %q (item: %+v)", k, got, want, item)
		}
	}
}
