package multica

import "testing"

func TestRuntimeExplicitScopeMatchesChecksKnownSessionAndRoot(t *testing.T) {
	material := RuntimeScopeMaterial{
		SessionID:   "multica:session:root-1",
		RootIssueID: "root-1",
	}
	if !RuntimeExplicitScopeMatches("multica:session:root-1", "root-1", material) {
		t.Fatal("matching explicit scope rejected")
	}
	if RuntimeExplicitScopeMatches("multica:session:other", "root-1", material) {
		t.Fatal("mismatched session accepted")
	}
	if RuntimeExplicitScopeMatches("multica:session:root-1", "other-root", material) {
		t.Fatal("mismatched root accepted")
	}
	if !RuntimeExplicitScopeMatches("", "", material) {
		t.Fatal("unscoped item should remain eligible")
	}
}

func TestRuntimeRefsMatchScopeUsesMulticaStructuredRefs(t *testing.T) {
	material := RuntimeScopeMaterial{
		SessionID:     "multica:session:root-1",
		RootIssueID:   "root-1",
		CorrelationID: "multica:issue:root-1",
		TaskID:        "task-1",
	}
	for _, refs := range [][]string{
		{"multica:issue:root-1"},
		{"multica:issue/root-1"},
		{"mention://issue/root-1"},
		{"multica:session:root-1"},
		{"multica:session/root-1"},
		{"multica:task:task-1"},
		{"multica:task/task-1"},
		{"unrelated", "mention://issue/child-1"},
	} {
		if !RuntimeRefsMatchScope(refs, material, "child-1") {
			t.Fatalf("refs should match current scope: %v", refs)
		}
	}
}

func TestRuntimeRefsMatchScopeRejectsOtherMulticaScope(t *testing.T) {
	material := RuntimeScopeMaterial{SessionID: "multica:session:root-1", RootIssueID: "root-1"}
	if RuntimeRefsMatchScope([]string{"multica:issue:other-root"}, material) {
		t.Fatal("other issue scope accepted")
	}
	if RuntimeRefsMatchScope([]string{"multica:task:other-task"}, material) {
		t.Fatal("other task scope accepted")
	}
	if !RuntimeRefsMatchScope([]string{"local:event:1", "docs/example"}, material) {
		t.Fatal("unscoped refs should not filter item out")
	}
}
