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

func TestAssignmentMailboxProjectionForRuntimeItem(t *testing.T) {
	now := time.Unix(200, 0).UTC()
	projection := AssignmentMailboxProjectionForRuntimeItem(AssignmentMailboxProjectionMaterial{
		Item: RuntimeAssignmentItem{
			ID:               " assignment-1 ",
			EventID:          "event-1",
			Assignee:         "worker@team",
			Scope:            "release/readiness",
			ExpectedWork:     "Check release notes.",
			ExpectedFeedback: "result or blocker",
			SignalRef:        "sig-1",
			ContextRefs:      []string{"ctx-b", "ctx-a", "ctx-a"},
			EvidenceRefs:     []string{" ev-1 "},
		},
		SessionID:       "multica:session:root-1",
		CorrelationID:   "multica:issue:root-1",
		RootIssueID:     "root-1",
		SourceIssueID:   "root-1",
		ProjectionOwner: "planner@team",
		MulticaAgentID:  "agent-worker",
		ProjectedAt:     now,
	})
	if projection.Fingerprint == "" || !strings.HasPrefix(projection.Fingerprint, "sha256:") {
		t.Fatalf("fingerprint missing algorithm prefix: %+v", projection)
	}
	if projection.Source.SessionID != "multica:session:root-1" ||
		projection.Source.CorrelationID != "multica:issue:root-1" ||
		projection.Source.EventID != "event-1" ||
		projection.Source.AssignmentID != " assignment-1 " ||
		projection.Source.AssignmentFingerprint != projection.Fingerprint ||
		projection.Source.Principal != "worker@team" ||
		projection.Source.ProjectionKind != "assignment" {
		t.Fatalf("source mismatch: %+v", projection.Source)
	}
	meta := projection.Metadata
	if meta.SchemaVersion != "1" ||
		meta.HubBackend != MulticaHubBackend ||
		meta.Kind != MulticaHubKindAssignmentMailbox ||
		meta.SessionID != "multica:session:root-1" ||
		meta.CorrelationID != "multica:issue:root-1" ||
		meta.EventID != "event-1" ||
		meta.EventType != "assignment.accepted" ||
		meta.EventPhase != "accepted" ||
		meta.AssignmentID != " assignment-1 " ||
		meta.AssignmentFingerprint != projection.Fingerprint ||
		meta.Principal != "worker@team" ||
		meta.SourceIssueID != "root-1" ||
		meta.RootIssueID != "root-1" ||
		meta.ProjectionOwner != "planner@team" ||
		meta.MulticaAgentID != "agent-worker" ||
		meta.ProjectedAt != now.Format(time.RFC3339) {
		t.Fatalf("metadata mismatch: %+v", meta)
	}
}

func TestAssignmentMailboxMetadataGroupsDispatchBeforeSupplemental(t *testing.T) {
	full := map[string]string{
		MulticaMetadataSchemaVersion:         " 1 ",
		MulticaMetadataHubBackend:            MulticaHubBackend,
		MulticaMetadataKind:                  MulticaHubKindAssignmentMailbox,
		MulticaMetadataSessionID:             "session-1",
		MulticaMetadataCorrelationID:         "correlation-1",
		MulticaMetadataEventID:               "event-1",
		MulticaMetadataEventType:             "assignment.accepted",
		MulticaMetadataEventPhase:            "accepted",
		MulticaMetadataAssignmentID:          "asg-1",
		MulticaMetadataAssignmentFingerprint: "sha256:abc",
		MulticaMetadataPrincipal:             "worker@team",
		MulticaMetadataSourceIssueID:         "root-1",
		MulticaMetadataRootIssueID:           "root-1",
		MulticaMetadataProjectionOwner:       " planner@team ",
		MulticaMetadataProjectedAt:           "2026-06-29T09:00:00Z",
		MulticaMetadataEnvelopeDigest:        "",
	}
	dispatch := AssignmentMailboxDispatchMetadata(full)
	for _, key := range []string{
		MulticaMetadataSchemaVersion,
		MulticaMetadataHubBackend,
		MulticaMetadataKind,
		MulticaMetadataSessionID,
		MulticaMetadataCorrelationID,
		MulticaMetadataEventID,
		MulticaMetadataEventType,
		MulticaMetadataEventPhase,
		MulticaMetadataAssignmentID,
		MulticaMetadataAssignmentFingerprint,
		MulticaMetadataPrincipal,
		MulticaMetadataSourceIssueID,
		MulticaMetadataRootIssueID,
	} {
		if strings.TrimSpace(full[key]) == "" {
			continue
		}
		if got := dispatch[key]; got != strings.TrimSpace(full[key]) {
			t.Fatalf("dispatch metadata[%s] = %q, want %q: %+v", key, got, strings.TrimSpace(full[key]), dispatch)
		}
	}
	if _, ok := dispatch[MulticaMetadataProjectionOwner]; ok {
		t.Fatalf("projection owner should be supplemental, not dispatch metadata: %+v", dispatch)
	}

	supplemental := AssignmentMailboxSupplementalMetadata(full, dispatch)
	if supplemental[MulticaMetadataProjectionOwner] != "planner@team" {
		t.Fatalf("supplemental metadata should trim projection owner: %+v", supplemental)
	}
	if _, ok := supplemental[MulticaMetadataAssignmentID]; ok {
		t.Fatalf("supplemental metadata must not duplicate dispatch keys: %+v", supplemental)
	}
	if _, ok := supplemental[MulticaMetadataEnvelopeDigest]; ok {
		t.Fatalf("empty supplemental metadata must be omitted: %+v", supplemental)
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
