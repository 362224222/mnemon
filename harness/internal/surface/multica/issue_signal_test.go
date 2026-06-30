package multica

import "testing"

func TestBuildIssueTeamworkSignalSeparatesRuleNarrativeRefs(t *testing.T) {
	draft, err := BuildIssueTeamworkSignal(IssueSignalMaterial{
		ID:          "iss-123",
		Identifier:  "MUL-123",
		Title:       "Validate bridge",
		Description: "Check that Multica issue context can start Mnemon teamwork.",
	}, IssueSignalOptions{
		Scope:        "multica/poc",
		TTL:          "45m",
		WhyTeamwork:  "The task needs more than one local agent.",
		WorkspaceID:  "workspace-1",
		TaskID:       "task-1",
		AgentID:      "agent-1",
		Principal:    "planner@team",
		EvidenceRefs: []string{"multica:issue/iss-123"},
		ExternalID:   "multica-task-task-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if draft.EventType != "teamwork_signal.write_candidate.observed" {
		t.Fatalf("event type = %q", draft.EventType)
	}
	rule := draft.Payload["rule"].(map[string]any)
	if rule["external_source"] != MulticaExternalSource || rule["external_issue_id"] != "iss-123" || rule["scope"] != "multica/poc" {
		t.Fatalf("rule mapping mismatch: %+v", rule)
	}
	if rule["external_workspace_id"] != "workspace-1" || rule["external_task_id"] != "task-1" || rule["external_agent_id"] != "agent-1" || rule["principal"] != "planner@team" {
		t.Fatalf("runtime rule mapping mismatch: %+v", rule)
	}
	if draft.ExternalID != "multica-task-task-1" {
		t.Fatalf("external id = %q", draft.ExternalID)
	}
	narrative := draft.Payload["narrative"].(map[string]any)
	if narrative["statement"] != "Check that Multica issue context can start Mnemon teamwork." {
		t.Fatalf("narrative statement mismatch: %+v", narrative)
	}
	if _, ok := narrative["external_issue_id"]; ok {
		t.Fatalf("narrative must not carry rule ids: %+v", narrative)
	}
	if _, ok := narrative["external_task_id"]; ok {
		t.Fatalf("narrative must not carry runtime ids: %+v", narrative)
	}
	refs := draft.Payload["refs"].(map[string]any)
	if got := refs["evidence_refs"].([]string); len(got) != 1 || got[0] != "multica:issue/iss-123" {
		t.Fatalf("evidence refs mismatch: %+v", refs)
	}
}

func TestBuildIssueTeamworkSignalDefaults(t *testing.T) {
	draft, err := BuildIssueTeamworkSignal(IssueSignalMaterial{ID: "iss-1"}, IssueSignalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	rule := draft.Payload["rule"].(map[string]any)
	if rule["scope"] != "multica/teamwork" || rule["ttl"] != "30m" || rule["correlation_id"] != "multica:issue:iss-1" {
		t.Fatalf("default rule mismatch: %+v", rule)
	}
	narrative := draft.Payload["narrative"].(map[string]any)
	if narrative["title"] != "iss-1" || narrative["statement"] != "iss-1" || narrative["why_teamwork"] == "" {
		t.Fatalf("default narrative mismatch: %+v", narrative)
	}
	refs := draft.Payload["refs"].(map[string]any)
	if got := refs["evidence_refs"].([]string); len(got) != 1 || got[0] != "multica:issue:iss-1" {
		t.Fatalf("default evidence refs mismatch: %+v", refs)
	}
	if draft.ExternalID != "multica-issue-iss-1" {
		t.Fatalf("default external id = %q", draft.ExternalID)
	}
}
