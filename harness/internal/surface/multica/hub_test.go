package multica

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestRootSessionHubMetadataDefaults(t *testing.T) {
	meta := RootSessionHubMetadata(MulticaHubMetadata{Principal: "planner@team"}, "root-1")
	if meta.HubBackend != MulticaHubBackend ||
		meta.Kind != MulticaHubKindSession ||
		meta.RootIssueID != "root-1" ||
		meta.SessionID != MulticaSessionID("root-1") ||
		meta.CorrelationID != "multica:issue:root-1" ||
		meta.Principal != "planner@team" {
		t.Fatalf("root session metadata mismatch: %+v", meta)
	}
}

func TestAssignmentMailboxHubMetadataDefaults(t *testing.T) {
	meta := AssignmentMailboxHubMetadata(MulticaHubMetadata{
		SourceIssueID: "root-1",
		AssignmentID:  "asg-1",
		Principal:     "worker@team",
	}, "child-1")
	if meta.HubBackend != MulticaHubBackend ||
		meta.Kind != MulticaHubKindAssignmentMailbox ||
		meta.RootIssueID != "root-1" ||
		meta.SessionID != MulticaSessionID("root-1") ||
		meta.CorrelationID != "multica:issue:child-1" ||
		meta.AssignmentID != "asg-1" ||
		meta.Principal != "worker@team" {
		t.Fatalf("assignment mailbox metadata mismatch: %+v", meta)
	}
}

func TestAssignmentMailboxMarkerPrefersEventThenAssignmentThenIssue(t *testing.T) {
	if got := AssignmentMailboxMarker(MulticaHubMetadata{EventID: "event-1", AssignmentID: "asg-1"}, "child-1"); got != "event-1" {
		t.Fatalf("event marker = %q", got)
	}
	if got := AssignmentMailboxMarker(MulticaHubMetadata{AssignmentID: "asg-1"}, "child-1"); got != "multica-assignment-asg-1" {
		t.Fatalf("assignment marker = %q", got)
	}
	if got := AssignmentMailboxMarker(MulticaHubMetadata{}, "child-1"); got != "multica-issue-child-1" {
		t.Fatalf("issue marker = %q", got)
	}
}

func TestRootSessionMetadataMap(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	meta := RootSessionMetadataMap(RootSessionMetadataMaterial{
		HubMetadata:     MulticaHubMetadata{Principal: "planner@team"},
		EventID:         "multica-task-task-1",
		EventType:       "teamwork_signal.write_candidate.observed",
		EventPhase:      "observed",
		SourceIssueID:   "root-1",
		ProjectionOwner: "planner@team",
		ProjectedAt:     now,
	})
	for key, want := range map[string]string{
		MulticaMetadataHubBackend:      MulticaHubBackend,
		MulticaMetadataKind:            MulticaHubKindSession,
		MulticaMetadataRootIssueID:     "root-1",
		MulticaMetadataSessionID:       MulticaSessionID("root-1"),
		MulticaMetadataCorrelationID:   "multica:issue:root-1",
		MulticaMetadataEventID:         "multica-task-task-1",
		MulticaMetadataEventType:       "teamwork_signal.write_candidate.observed",
		MulticaMetadataEventPhase:      "observed",
		MulticaMetadataPrincipal:       "planner@team",
		MulticaMetadataSourceIssueID:   "root-1",
		MulticaMetadataProjectionOwner: "planner@team",
		MulticaMetadataProjectedAt:     now.Format(time.RFC3339),
	} {
		if got := meta[key]; got != want {
			t.Fatalf("metadata[%s] = %q, want %q: %+v", key, got, want, meta)
		}
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
