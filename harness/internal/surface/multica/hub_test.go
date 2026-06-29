package multica

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestHubMetadataDetectsAssignmentMailbox(t *testing.T) {
	meta := ParseMulticaHubMetadata(map[string]any{
		"metadata": []any{
			map[string]any{"key": MulticaMetadataHubBackend, "value": MulticaHubBackend},
			map[string]any{"key": MulticaMetadataKind, "value": MulticaHubKindAssignmentMailbox},
			map[string]any{"key": MulticaMetadataAssignmentID, "value": "assignment-1"},
			map[string]any{"key": MulticaMetadataRootIssueID, "value": "root-1"},
			map[string]any{"key": MulticaMetadataPrincipal, "value": "worker@team"},
		},
	})
	if !meta.IsAssignmentMailbox() {
		t.Fatalf("metadata was not detected as assignment mailbox: %+v", meta)
	}
	if meta.RootIssueID != "root-1" || meta.Principal != "worker@team" {
		t.Fatalf("metadata mismatch: %+v", meta)
	}
	back := meta.Map()
	if back[MulticaMetadataHubBackend] != MulticaHubBackend {
		t.Fatalf("metadata map missing backend: %+v", back)
	}
}

func TestAssignmentFingerprintStable(t *testing.T) {
	left := MulticaAssignmentFingerprint(MulticaAssignmentFingerprintInput{
		AssignmentID:     " assignment-1 ",
		Assignee:         "worker@team",
		Scope:            "docs",
		ExpectedWork:     "write the API notes",
		ExpectedFeedback: "summary",
		ContextRefs:      []string{"ctx-b", "ctx-a", "ctx-a"},
		EvidenceRefs:     []string{" ev-1 "},
		CorrelationID:    "session-1",
	})
	right := MulticaAssignmentFingerprint(MulticaAssignmentFingerprintInput{
		AssignmentID:     "assignment-1",
		Assignee:         " worker@team ",
		Scope:            "docs",
		ExpectedWork:     "write the API notes",
		ExpectedFeedback: "summary",
		ContextRefs:      []string{"ctx-a", "ctx-b"},
		EvidenceRefs:     []string{"ev-1"},
		CorrelationID:    "session-1",
	})
	if left != right {
		t.Fatalf("fingerprint should be stable across whitespace/order/dedup:\nleft=%s\nright=%s", left, right)
	}
	if !strings.HasPrefix(left, "sha256:") {
		t.Fatalf("fingerprint should carry algorithm prefix: %q", left)
	}
}

func TestFileHubLedgerDedupesRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hub-ledger.jsonl")
	ledger := NewFileMulticaHubLedger(path)
	source := MulticaHubLedgerSource{
		SessionID:             "session-1",
		AssignmentID:          "assignment-1",
		AssignmentFingerprint: "sha256:abc",
		Principal:             "worker@team",
		ProjectionKind:        "assignment",
	}
	record := MulticaHubLedgerRecord{
		Kind:   MulticaHubKindAssignmentMailbox,
		Source: source,
		Target: MulticaHubLedgerTarget{
			RootIssueID:  "root-1",
			ChildIssueID: "child-1",
			Status:       "created",
		},
	}
	if err := ledger.Record(record); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Record(record); err != nil {
		t.Fatal(err)
	}
	records, err := NewFileMulticaHubLedger(path).Records()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("ledger should keep one record for the same source key, got %d: %+v", len(records), records)
	}
	found, ok, err := NewFileMulticaHubLedger(path).Find(MulticaHubKindAssignmentMailbox, source)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || found.Target.ChildIssueID != "child-1" {
		t.Fatalf("ledger find mismatch: ok=%v record=%+v", ok, found)
	}
}
