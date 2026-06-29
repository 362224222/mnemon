package multica

import (
	"encoding/json"
	"reflect"
	"testing"

	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

func TestRuntimeAssignmentViewItemReadsStructuredPayloadSections(t *testing.T) {
	item := RuntimeAssignmentViewItem(map[string]any{
		"id":         "event-1",
		"ingest_seq": json.Number("42"),
		"actor":      "planner@team",
		eventmodel.PayloadRuleKey: map[string]any{
			"assignment_id": "asg-1",
			"assignee":      "worker@team",
			"scope":         "release/readiness",
			"ttl":           "30m",
			"signal_ref":    "sig-1",
			"session_id":    "multica:session:root-1",
			"root_issue_id": "root-1",
		},
		eventmodel.PayloadNarrativeKey: map[string]any{
			"expected_work":     "Check the release note flow.",
			"expected_feedback": "result or blocker",
			"rationale":         "Validate routing.",
		},
		eventmodel.PayloadRefsKey: map[string]any{
			"context_refs":  []any{" multica:issue:root-1 ", "multica:issue:root-1"},
			"evidence_refs": "evidence-1",
		},
	})
	if item.ID != "asg-1" || item.EventID != "event-1" || item.IngestSeq != 42 {
		t.Fatalf("assignment identity mismatch: %+v", item)
	}
	if item.Assignee != "worker@team" || item.Scope != "release/readiness" || item.TTL != "30m" || item.SignalRef != "sig-1" {
		t.Fatalf("assignment rule fields mismatch: %+v", item)
	}
	if item.ExpectedWork == "" || item.ExpectedFeedback == "" || item.Rationale == "" {
		t.Fatalf("assignment narrative fields missing: %+v", item)
	}
	if !reflect.DeepEqual(item.ContextRefs, []string{"multica:issue:root-1"}) {
		t.Fatalf("context refs = %#v", item.ContextRefs)
	}
	if !reflect.DeepEqual(item.EvidenceRefs, []string{"evidence-1"}) {
		t.Fatalf("evidence refs = %#v", item.EvidenceRefs)
	}
}

func TestRuntimeProgressViewItemReadsRefsAndNarrative(t *testing.T) {
	item := RuntimeProgressViewItem(map[string]any{
		"event_id":   "pg-1",
		"ingest_seq": "51",
		eventmodel.PayloadRuleKey: map[string]any{
			"assignment_ref": "asg-1",
			"feedback_kind":  "result",
			"scope":          "release/readiness",
			"session_id":     "multica:session:root-1",
			"root_issue_id":  "root-1",
		},
		eventmodel.PayloadNarrativeKey: map[string]any{
			"summary": "Done",
			"result":  "Validated",
		},
		eventmodel.PayloadRefsKey: map[string]any{
			"context_refs":  []string{"multica:issue:root-1"},
			"artifact_refs": []any{"artifact-1", "artifact-1"},
			"evidence_refs": []any{"evidence-1"},
		},
	})
	if item.ID != "pg-1" || item.EventID != "pg-1" || item.IngestSeq != 51 {
		t.Fatalf("progress identity mismatch: %+v", item)
	}
	if item.AssignmentRef != "asg-1" || item.FeedbackKind != "result" || item.Summary != "Done" || item.Result != "Validated" {
		t.Fatalf("progress fields mismatch: %+v", item)
	}
	if !reflect.DeepEqual(item.ArtifactRefs, []string{"artifact-1"}) {
		t.Fatalf("artifact refs = %#v", item.ArtifactRefs)
	}
}

func TestRuntimeViewItemsMatchScopeAndRootIngest(t *testing.T) {
	scope := RuntimeScopeMaterial{SessionID: "multica:session:root-1", RootIssueID: "root-1"}
	if !RuntimeAssignmentMatchesScope(RuntimeAssignmentItem{
		SessionID:   "multica:session:root-1",
		RootIssueID: "root-1",
		ContextRefs: []string{"multica:issue:root-1"},
	}, scope) {
		t.Fatal("assignment should match current scope")
	}
	if RuntimeProgressMatchesScope(RuntimeProgressItem{
		ContextRefs: []string{"multica:issue:other-root"},
	}, scope) {
		t.Fatal("progress from another scoped issue should not match")
	}
	if !RuntimeItemAfterRootIngest(11, 10) || RuntimeItemAfterRootIngest(9, 10) {
		t.Fatal("root ingest sequence filter mismatch")
	}
	if !RuntimeItemAfterRootIngest(0, 10) || !RuntimeItemAfterRootIngest(9, 0) {
		t.Fatal("unknown sequence should remain eligible")
	}
}
