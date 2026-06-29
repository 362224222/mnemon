package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/driver"
)

func TestMulticaRuntimeProdSimAcceptanceObservesRunMessages(t *testing.T) {
	tmp := t.TempDir()
	registryPath := filepath.Join(tmp, "registry.json")
	if err := driver.SaveMulticaRegistry(registryPath, driver.MulticaRegistry{
		SchemaVersion:    1,
		WorkspaceID:      "ws-1",
		RuntimeProfileID: "profile-1",
		RuntimeID:        "runtime-1",
		Participants: []driver.MulticaParticipantRecord{{
			Principal: "planner@team",
			AgentName: "mnemon-planner",
			AgentID:   "agent-1",
			Role:      "planner",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	argsPath := filepath.Join(tmp, "args.txt")
	stdinPath := filepath.Join(tmp, "stdin.txt")
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" >> "$MULTICA_ARGS_PATH"
cat >> "$MULTICA_STDIN_PATH"
case "$*" in
  *"issue create"*) printf '{"id":"iss-9","identifier":"TEA-9","title":"Runtime prod sim","description":"Teamwork acceptance","status":"todo"}\n' ;;
  *"issue runs iss-9"*) printf '[{"id":"task-9","issue_id":"iss-9","agent_id":"agent-1","status":"completed","completed_at":"2026-06-28T09:00:00Z","workspace_id":"ws-1"}]\n' ;;
  *"issue run-messages task-9"*) printf '[{"task_id":"task-9","issue_id":"iss-9","seq":1,"type":"assistant","content":"Mnemon Multica runtime handled issue TEA-9. Mnemon ingest: recorded. Managed wake: completed.","created_at":"2026-06-28T09:00:01Z"}]\n' ;;
  *) printf '{}\n' ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MULTICA_ARGS_PATH", argsPath)
	t.Setenv("MULTICA_STDIN_PATH", stdinPath)

	report, err := runMulticaRuntimeProdSimAcceptance(context.Background(), multicaRuntimeProdSimOptions{
		RunRoot:            filepath.Join(tmp, ".testdata", "multica-runtime"),
		MulticaBin:         bin,
		WorkspaceID:        "ws-1",
		RegistryPath:       registryPath,
		AssigneePrincipal:  "planner@team",
		IssueTitle:         "Runtime prod sim",
		IssueDescription:   "Teamwork acceptance",
		Wait:               time.Millisecond,
		Poll:               time.Millisecond,
		RequireIngest:      true,
		RequireManagedWake: true,
	})
	if err != nil {
		t.Fatalf("acceptance: %v report=%+v", err, report)
	}
	if report.Status != "ok" || report.Issue.ID != "iss-9" || len(report.RunMessages) != 1 {
		t.Fatalf("report mismatch: %+v", report)
	}
	data, err := os.ReadFile(report.ReportPath)
	if err != nil {
		t.Fatal(err)
	}
	var written multicaRuntimeProdSimReport
	if err := json.Unmarshal(data, &written); err != nil {
		t.Fatalf("report JSON: %v\n%s", err, data)
	}
	if written.Status != "ok" || !multicaProdSimAssertionsPassed(written) {
		t.Fatalf("written report mismatch: %+v", written)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "issue create --title Runtime prod sim") || !strings.Contains(string(args), "--assignee-id agent-1") {
		t.Fatalf("issue was not created through assigned Multica path:\n%s", args)
	}
	stdin, err := os.ReadFile(stdinPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stdin), "Teamwork acceptance") {
		t.Fatalf("issue description was not passed through stdin:\n%s", stdin)
	}
}

func TestMulticaRuntimeProdSimAcceptanceRequiresHubFlow(t *testing.T) {
	tmp := t.TempDir()
	registryPath := filepath.Join(tmp, "registry.json")
	var participants []driver.MulticaParticipantRecord
	for _, role := range []string{"planner", "researcher", "implementer", "reviewer", "integrator"} {
		participants = append(participants, driver.MulticaParticipantRecord{
			Principal: role + "@team",
			AgentName: "mnemon-" + role,
			AgentID:   "agent-" + role,
			Role:      role,
		})
	}
	if err := driver.SaveMulticaRegistry(registryPath, driver.MulticaRegistry{
		SchemaVersion:    1,
		WorkspaceID:      "ws-1",
		RuntimeProfileID: "profile-1",
		RuntimeID:        "runtime-1",
		Participants:     participants,
	}); err != nil {
		t.Fatal(err)
	}
	argsPath := filepath.Join(tmp, "args.txt")
	stdinPath := filepath.Join(tmp, "stdin.txt")
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" >> "$MULTICA_ARGS_PATH"
cat >> "$MULTICA_STDIN_PATH"
case "$*" in
  *"issue create"*) printf '{"id":"root-9","identifier":"TEA-9","title":"Runtime hub flow","description":"Teamwork acceptance","status":"todo"}\n' ;;
  *"issue get root-9"*) printf '{"id":"root-9","identifier":"TEA-9","title":"Runtime hub flow","description":"Teamwork acceptance","status":"done"}\n' ;;
  *"issue get child-2"*) printf '%s\n' '{"id":"child-2","identifier":"TEA-10","title":"TEA-9: routing check","description":"## Assignment\n\nCheck routing.\n\n## Context\n\n- Root issue: [TEA-9](mention://issue/root-9) - Runtime hub flow\n- Assignee: researcher@team (mnemon-researcher)\n- Scope: routing check\n\n## Feedback\n\n- Expected feedback: result or blocker\n- Progress path: Mnemon runtime progress, result, or blocker feedback","status":"done"}' ;;
  *"issue get child-3"*) printf '%s\n' '{"id":"child-3","identifier":"TEA-11","title":"TEA-9: runtime display","description":"## Assignment\n\nCheck runtime display.\n\n## Context\n\n- Root issue: [TEA-9](mention://issue/root-9) - Runtime hub flow\n- Assignee: implementer@team (mnemon-implementer)\n- Scope: runtime display\n\n## Feedback\n\n- Expected feedback: result or blocker\n- Progress path: Mnemon runtime progress, result, or blocker feedback","status":"done"}' ;;
  *"issue metadata list root-9"*) printf '[{"key":"mnemon.hub_backend","value":"multica"},{"key":"mnemon.kind","value":"session_mailbox"},{"key":"mnemon.session_id","value":"multica:session:root-9"}]\n' ;;
  *"issue children root-9"*) printf '{"children":[{"id":"child-2","identifier":"TEA-10","title":"Assignment 2","status":"done","metadata":{"mnemon.hub_backend":"multica","mnemon.kind":"assignment_mailbox","mnemon.root_issue_id":"root-9","mnemon.session_id":"multica:session:root-9","mnemon.assignment_id":"asg-2","mnemon.principal":"researcher@team"}},{"id":"child-3","identifier":"TEA-11","title":"Assignment 3","status":"done","metadata":{"mnemon.hub_backend":"multica","mnemon.kind":"assignment_mailbox","mnemon.root_issue_id":"root-9","mnemon.session_id":"multica:session:root-9","mnemon.assignment_id":"asg-3","mnemon.principal":"implementer@team"}}]}\n' ;;
  *"issue comment list root-9"*) printf '[{"id":"comment-root","issue_id":"root-9","content":"Mnemon update: issue admitted\\n\\nmnemon:event=multica-task-root"}]\n' ;;
  *"issue comment list child-2"*) printf '[{"id":"comment-child-2","issue_id":"child-2","content":"Mnemon update: assignment feedback\\n\\nSummary: checked routing.\\n\\nmnemon:event=pg-2"}]\n' ;;
  *"issue comment list child-3"*) printf '[{"id":"comment-child-3","issue_id":"child-3","content":"Mnemon update: assignment feedback\\n\\nSummary: checked runtime display.\\n\\nmnemon:event=pg-3"}]\n' ;;
  *"issue runs root-9"*) printf '[{"id":"task-root","issue_id":"root-9","agent_id":"agent-planner","status":"completed","completed_at":"2026-06-28T09:00:00Z","workspace_id":"ws-1"}]\n' ;;
  *"issue run-messages task-root"*) printf '[{"task_id":"task-root","issue_id":"root-9","seq":1,"type":"text","content":"Mnemon Multica runtime handled issue TEA-9. Mnemon ingest: recorded. Managed wake: completed. Multica updates: 2 assignment mailboxes created.","created_at":"2026-06-28T09:00:01Z"},{"task_id":"task-root","issue_id":"root-9","seq":2,"type":"tool_use","content":"mnemond ingest observe --principal planner@team","created_at":"2026-06-28T09:00:02Z"},{"task_id":"task-root","issue_id":"root-9","seq":3,"type":"tool_result","content":"recorded","created_at":"2026-06-28T09:00:03Z"}]\n' ;;
  *"issue runs child-2"*) printf '[{"id":"task-child-2","issue_id":"child-2","agent_id":"agent-researcher","status":"completed","completed_at":"2026-06-28T09:01:00Z","workspace_id":"ws-1"}]\n' ;;
  *"issue run-messages task-child-2"*) printf '[{"task_id":"task-child-2","issue_id":"child-2","seq":1,"type":"text","content":"Mnemon Multica runtime handled issue TEA-10. Mnemon assignment mailbox: correlated. Managed wake: completed.","created_at":"2026-06-28T09:01:01Z"},{"task_id":"task-child-2","issue_id":"child-2","seq":2,"type":"tool_use","content":"mnemond managed wake --principal researcher@team [mnemon:wake]","created_at":"2026-06-28T09:01:02Z"},{"task_id":"task-child-2","issue_id":"child-2","seq":3,"type":"tool_result","content":"Managed wake completed.","created_at":"2026-06-28T09:01:03Z"}]\n' ;;
  *"issue runs child-3"*) printf '[{"id":"task-child-3","issue_id":"child-3","agent_id":"agent-implementer","status":"completed","completed_at":"2026-06-28T09:02:00Z","workspace_id":"ws-1"}]\n' ;;
  *"issue run-messages task-child-3"*) printf '[{"task_id":"task-child-3","issue_id":"child-3","seq":1,"type":"text","content":"Mnemon Multica runtime handled issue TEA-11. Mnemon assignment mailbox: correlated. Managed wake: completed.","created_at":"2026-06-28T09:02:01Z"},{"task_id":"task-child-3","issue_id":"child-3","seq":2,"type":"tool_use","content":"mnemond managed wake --principal implementer@team [mnemon:wake]","created_at":"2026-06-28T09:02:02Z"},{"task_id":"task-child-3","issue_id":"child-3","seq":3,"type":"tool_result","content":"Managed wake completed.","created_at":"2026-06-28T09:02:03Z"}]\n' ;;
  *) printf '{}\n' ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MULTICA_ARGS_PATH", argsPath)
	t.Setenv("MULTICA_STDIN_PATH", stdinPath)

	report, err := runMulticaRuntimeProdSimAcceptance(context.Background(), multicaRuntimeProdSimOptions{
		RunRoot:            filepath.Join(tmp, ".testdata", "multica-hub-runtime"),
		MulticaBin:         bin,
		WorkspaceID:        "ws-1",
		RegistryPath:       registryPath,
		AssigneePrincipal:  "planner@team",
		IssueTitle:         "Runtime hub flow",
		IssueDescription:   "Teamwork acceptance",
		Wait:               time.Millisecond,
		Poll:               time.Millisecond,
		RequireIngest:      true,
		RequireManagedWake: true,
		RequireHubFlow:     true,
		MinParticipants:    5,
		MinActiveAgents:    3,
	})
	if err != nil {
		t.Fatalf("acceptance: %v report=%+v", err, report)
	}
	if report.Status != "ok" || len(report.Participants) != 5 || len(report.ChildIssues) != 2 || len(report.ActiveAgents) != 3 || report.FinalRoot.Status != "done" {
		t.Fatalf("hub report mismatch: %+v", report)
	}
	if report.RootMetadata[driver.MulticaMetadataKind] != driver.MulticaHubKindSession {
		t.Fatalf("root metadata mismatch: %+v", report.RootMetadata)
	}
	if !multicaProdSimAssertionsPassed(report) {
		t.Fatalf("hub assertions failed: %+v", report.Assertions)
	}
	var visibleTextAssertion bool
	for _, assertion := range report.Assertions {
		if assertion.Name == "assignment child issue visible text is structured" {
			visibleTextAssertion = assertion.Passed
			break
		}
	}
	if !visibleTextAssertion {
		t.Fatalf("missing or failed visible text assertion: %+v", report.Assertions)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"issue metadata list root-9 --output json",
		"issue children root-9 --output json",
		"issue runs child-2 --output json",
		"issue runs child-3 --output json",
	} {
		if !strings.Contains(string(args), want) {
			t.Fatalf("args missing %q:\n%s", want, args)
		}
	}
}

func TestMulticaRuntimeProdSimHubFlowAllowsDeferredRunMessages(t *testing.T) {
	tmp := t.TempDir()
	registryPath := filepath.Join(tmp, "registry.json")
	var participants []driver.MulticaParticipantRecord
	for _, role := range []string{"planner", "researcher", "implementer", "reviewer", "integrator"} {
		participants = append(participants, driver.MulticaParticipantRecord{
			Principal: role + "@team",
			AgentName: "mnemon-" + role,
			AgentID:   "agent-" + role,
			Role:      role,
		})
	}
	if err := driver.SaveMulticaRegistry(registryPath, driver.MulticaRegistry{
		SchemaVersion:    1,
		WorkspaceID:      "ws-1",
		RuntimeProfileID: "profile-1",
		RuntimeID:        "runtime-1",
		Participants:     participants,
	}); err != nil {
		t.Fatal(err)
	}
	argsPath := filepath.Join(tmp, "args.txt")
	stdinPath := filepath.Join(tmp, "stdin.txt")
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" >> "$MULTICA_ARGS_PATH"
cat >> "$MULTICA_STDIN_PATH"
case "$*" in
  *"issue create"*) printf '{"id":"root-deferred","identifier":"TEA-20","title":"Deferred run messages","description":"Teamwork acceptance","status":"todo"}\n' ;;
  *"issue get root-deferred"*) printf '{"id":"root-deferred","identifier":"TEA-20","title":"Deferred run messages","description":"Teamwork acceptance","status":"done"}\n' ;;
  *"issue get child-a"*) printf '%s\n' '{"id":"child-a","identifier":"TEA-21","title":"TEA-20: routing","description":"## Assignment\n\nCheck routing.\n\n## Context\n\n- Root issue: [TEA-20](mention://issue/root-deferred) - Deferred run messages\n- Assignee: researcher@team (mnemon-researcher)\n- Scope: routing\n\n## Feedback\n\n- Expected feedback: result","status":"done"}' ;;
  *"issue get child-b"*) printf '%s\n' '{"id":"child-b","identifier":"TEA-22","title":"TEA-20: status","description":"## Assignment\n\nCheck status.\n\n## Context\n\n- Root issue: [TEA-20](mention://issue/root-deferred) - Deferred run messages\n- Assignee: implementer@team (mnemon-implementer)\n- Scope: status\n\n## Feedback\n\n- Expected feedback: result","status":"done"}' ;;
  *"issue metadata list root-deferred"*) printf '[{"key":"mnemon.hub_backend","value":"multica"},{"key":"mnemon.kind","value":"session_mailbox"},{"key":"mnemon.session_id","value":"multica:session:root-deferred"}]\n' ;;
  *"issue children root-deferred"*) printf '{"children":[{"id":"child-a","identifier":"TEA-21","title":"TEA-20: routing","status":"done","metadata":{"mnemon.hub_backend":"multica","mnemon.kind":"assignment_mailbox","mnemon.root_issue_id":"root-deferred","mnemon.session_id":"multica:session:root-deferred","mnemon.assignment_id":"asg-a","mnemon.principal":"researcher@team"}},{"id":"child-b","identifier":"TEA-22","title":"TEA-20: status","status":"done","metadata":{"mnemon.hub_backend":"multica","mnemon.kind":"assignment_mailbox","mnemon.root_issue_id":"root-deferred","mnemon.session_id":"multica:session:root-deferred","mnemon.assignment_id":"asg-b","mnemon.principal":"implementer@team"}}]}\n' ;;
  *"issue comment list root-deferred"*) printf '[]\n' ;;
  *"issue comment list child-a"*) printf '[{"id":"comment-a","issue_id":"child-a","content":"Mnemon update: assignment feedback\\n\\nmnemon:event=pg-a"}]\n' ;;
  *"issue comment list child-b"*) printf '[{"id":"comment-b","issue_id":"child-b","content":"Mnemon update: assignment feedback\\n\\nmnemon:event=pg-b"}]\n' ;;
  *"issue runs root-deferred"*) printf '[{"id":"task-root","issue_id":"root-deferred","agent_id":"agent-planner","status":"running","workspace_id":"ws-1"}]\n' ;;
  *"issue runs child-a"*) printf '[{"id":"task-a","issue_id":"child-a","agent_id":"agent-researcher","status":"running","workspace_id":"ws-1"}]\n' ;;
  *"issue runs child-b"*) printf '[{"id":"task-b","issue_id":"child-b","agent_id":"agent-implementer","status":"running","workspace_id":"ws-1"}]\n' ;;
  *) printf '{}\n' ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MULTICA_ARGS_PATH", argsPath)
	t.Setenv("MULTICA_STDIN_PATH", stdinPath)

	report, err := runMulticaRuntimeProdSimAcceptance(context.Background(), multicaRuntimeProdSimOptions{
		RunRoot:            filepath.Join(tmp, ".testdata", "multica-hub-deferred"),
		MulticaBin:         bin,
		WorkspaceID:        "ws-1",
		RegistryPath:       registryPath,
		AssigneePrincipal:  "planner@team",
		IssueTitle:         "Deferred run messages",
		IssueDescription:   "Teamwork acceptance",
		Wait:               time.Millisecond,
		Poll:               time.Millisecond,
		RequireIngest:      true,
		RequireManagedWake: true,
		RequireHubFlow:     true,
		MinParticipants:    5,
		MinActiveAgents:    3,
	})
	if err != nil {
		t.Fatalf("acceptance: %v report=%+v", err, report)
	}
	if report.Status != "ok" || len(report.RunMessages) != 0 || len(report.ActiveAgents) != 3 || report.FinalRoot.Status != "done" {
		t.Fatalf("deferred hub report mismatch: %+v", report)
	}
	if !multicaProdSimAssertionsPassed(report) {
		t.Fatalf("deferred hub assertions failed: %+v", report.Assertions)
	}
}

func TestMulticaRuntimeProdSimAcceptanceReportsPrerequisitesTogether(t *testing.T) {
	tmp := t.TempDir()
	report, err := runMulticaRuntimeProdSimAcceptance(context.Background(), multicaRuntimeProdSimOptions{
		RunRoot:      filepath.Join(tmp, ".testdata", "multica-readiness"),
		MulticaBin:   filepath.Join(tmp, "missing-multica"),
		RegistryPath: filepath.Join(tmp, "missing-registry.json"),
		Wait:         time.Millisecond,
		Poll:         time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected prerequisite error")
	}
	if report.Status != "failed" || report.ReportPath == "" {
		t.Fatalf("report mismatch: %+v", report)
	}
	if !strings.Contains(err.Error(), "prerequisites failed") ||
		!strings.Contains(err.Error(), "Multica CLI not executable") ||
		!strings.Contains(err.Error(), "Multica registry not found") {
		t.Fatalf("error did not report both prerequisites: %v", err)
	}
	assertionByName := map[string]multicaRuntimeProdSimAssertion{}
	for _, assertion := range report.Assertions {
		assertionByName[assertion.Name] = assertion
	}
	for _, name := range []string{"Multica CLI available", "Multica registry available"} {
		assertion, ok := assertionByName[name]
		if !ok {
			t.Fatalf("missing assertion %q in %+v", name, report.Assertions)
		}
		if assertion.Passed {
			t.Fatalf("assertion %q unexpectedly passed: %+v", name, assertion)
		}
	}
	data, err := os.ReadFile(report.ReportPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "Multica CLI available") || !strings.Contains(string(data), "Multica registry available") {
		t.Fatalf("written report missing prerequisite assertions:\n%s", data)
	}
}
