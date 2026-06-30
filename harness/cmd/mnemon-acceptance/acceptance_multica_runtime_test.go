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

func TestSelectMulticaAcceptanceAssigneeUsesRegistryHelpers(t *testing.T) {
	reg := driver.MulticaRegistry{Participants: []driver.MulticaParticipantRecord{
		{Principal: "planner@team", AgentName: "mnemon-planner"},
		{Principal: " reviewer@team ", AgentName: "mnemon-reviewer", AgentID: "agent-reviewer"},
		{Principal: "implementer@team", AgentName: "mnemon-implementer", AgentID: "agent-implementer"},
	}}
	assignee, err := selectMulticaAcceptanceAssignee(reg, "reviewer@team")
	if err != nil {
		t.Fatal(err)
	}
	if assignee.AgentID != "agent-reviewer" {
		t.Fatalf("principal assignee = %+v", assignee)
	}
	assignee, err = selectMulticaAcceptanceAssignee(reg, "")
	if err != nil {
		t.Fatal(err)
	}
	if assignee.AgentID != "agent-reviewer" {
		t.Fatalf("fallback assignee = %+v", assignee)
	}
	if _, err := selectMulticaAcceptanceAssignee(reg, "planner@team"); err == nil || !strings.Contains(err.Error(), "no Multica agent id") {
		t.Fatalf("missing agent id error = %v", err)
	}
}

func TestMulticaRuntimeProdSimAcceptanceObservesRunMessages(t *testing.T) {
	tmp := t.TempDir()
	registryPath := writeMulticaRuntimeTestRegistry(t, tmp, []string{"planner"})
	argsPath := filepath.Join(tmp, "args.txt")
	stdinPath := filepath.Join(tmp, "stdin.txt")
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" >> "$MULTICA_ARGS_PATH"
cat >> "$MULTICA_STDIN_PATH"
case "$*" in
  *"issue create"*) printf '{"id":"iss-9","identifier":"TEA-9","title":"Runtime prod sim","description":"Teamwork acceptance","status":"todo"}\n' ;;
  *"issue runs iss-9"*) printf '[{"id":"task-9","issue_id":"iss-9","agent_id":"agent-planner","status":"completed","completed_at":"2026-06-28T09:00:00Z","workspace_id":"ws-1"}]\n' ;;
  *"issue run-messages task-9"*) printf '[{"task_id":"task-9","issue_id":"iss-9","seq":1,"type":"assistant","content":"Mnemon Multica runtime handled issue TEA-9. Multica surface input: observed.","created_at":"2026-06-28T09:00:01Z"}]\n' ;;
  *) printf '{}\n' ;;
esac
`
	writeFakeMultica(t, bin, argsPath, stdinPath, script)

	report, err := runMulticaRuntimeProdSimAcceptance(context.Background(), multicaRuntimeProdSimOptions{
		RunRoot:           filepath.Join(tmp, ".testdata", "multica-runtime"),
		MulticaBin:        bin,
		WorkspaceID:       "ws-1",
		RegistryPath:      registryPath,
		AssigneePrincipal: "planner@team",
		IssueTitle:        "Runtime prod sim",
		IssueDescription:  "Teamwork acceptance",
		Wait:              time.Millisecond,
		Poll:              time.Millisecond,
		RequireIngest:     true,
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
	if !strings.Contains(string(args), "issue create --title Runtime prod sim") || !strings.Contains(string(args), "--assignee-id agent-planner") {
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

func TestMulticaRuntimeProdSimAcceptanceRequiresSurfaceFlow(t *testing.T) {
	tmp := t.TempDir()
	registryPath := writeMulticaRuntimeTestRegistry(t, tmp, []string{"planner", "researcher", "implementer", "reviewer", "integrator"})
	argsPath := filepath.Join(tmp, "args.txt")
	stdinPath := filepath.Join(tmp, "stdin.txt")
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" >> "$MULTICA_ARGS_PATH"
cat >> "$MULTICA_STDIN_PATH"
case "$*" in
  *"agent env get agent-"*) agent="${4:-}"; printf '{"agent_id":"%s","custom_env":{"MNEMON_MULTICA_PROVIDER_RUNTIME":"codex","MNEMON_MULTICA_PROVIDER_COMMAND":"codex","MNEMON_CONTROL_ADDR":"http://127.0.0.1:8787"}}\n' "$agent" ;;
  *"issue create"*) printf '{"id":"root-9","identifier":"TEA-9","title":"Runtime surface flow","description":"Teamwork acceptance","status":"todo"}\n' ;;
  *"issue runs root-9"*) printf '[{"id":"task-root","issue_id":"root-9","agent_id":"agent-planner","status":"completed","completed_at":"2026-06-28T09:00:00Z","workspace_id":"ws-1"}]\n' ;;
  *"issue run-messages task-root"*) printf '[{"task_id":"task-root","issue_id":"root-9","seq":1,"type":"text","content":"Mnemon Multica runtime handled issue TEA-9. Multica surface input: observed.","created_at":"2026-06-28T09:00:01Z"},{"task_id":"task-root","issue_id":"root-9","seq":2,"type":"tool_use","content":"mnemon observe --principal planner@team","created_at":"2026-06-28T09:00:02Z"}]\n' ;;
  *"issue metadata list root-9"*) printf '[{"key":"mnemon.surface_role","value":"display"},{"key":"mnemon.event_ref","value":"event-root-9"},{"key":"mnemon.no_auto_dispatch","value":"true"}]\n' ;;
  *"issue get root-9"*) printf '{"id":"root-9","identifier":"TEA-9","title":"Runtime surface flow","description":"Teamwork acceptance","status":"in_progress"}\n' ;;
  *"issue comment list root-9"*) printf '[{"id":"comment-root","issue_id":"root-9","content":"Mnemon 更新: 进展\\n\\n事件引用: event-root-9"}]\n' ;;
  *) printf '{}\n' ;;
esac
`
	writeFakeMultica(t, bin, argsPath, stdinPath, script)

	report, err := runMulticaRuntimeProdSimAcceptance(context.Background(), multicaRuntimeProdSimOptions{
		RunRoot:            filepath.Join(tmp, ".testdata", "multica-surface-runtime"),
		MulticaBin:         bin,
		WorkspaceID:        "ws-1",
		RegistryPath:       registryPath,
		AssigneePrincipal:  "planner@team",
		IssueTitle:         "Runtime surface flow",
		IssueDescription:   "Teamwork acceptance",
		Wait:               time.Millisecond,
		Poll:               time.Millisecond,
		RequireIngest:      true,
		RequireSurfaceFlow: true,
		MinParticipants:    5,
		MinActiveAgents:    3,
	})
	if err != nil {
		t.Fatalf("acceptance: %v report=%+v", err, report)
	}
	if report.Status != "ok" || len(report.Participants) != 5 || report.FinalRoot.ID != "root-9" || len(report.RootComments) != 1 {
		t.Fatalf("surface report mismatch: %+v", report)
	}
	if report.RootMetadata[driver.MulticaMetadataSurfaceRole] != driver.MulticaSurfaceRoleDisplay {
		t.Fatalf("root metadata mismatch: %+v", report.RootMetadata)
	}
	for _, assertion := range report.Assertions {
		if assertion.Name == "root issue does not use legacy hub metadata" && !assertion.Passed {
			t.Fatalf("legacy metadata assertion failed: %+v", assertion)
		}
	}
}

func TestMulticaRuntimeProdSimAcceptanceRejectsLegacySurfaceFlowPrereq(t *testing.T) {
	tmp := t.TempDir()
	registryPath := writeMulticaRuntimeTestRegistry(t, tmp, []string{"planner", "researcher", "implementer"})
	argsPath := filepath.Join(tmp, "args.txt")
	stdinPath := filepath.Join(tmp, "stdin.txt")
	bin := filepath.Join(tmp, "multica")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" >> "$MULTICA_ARGS_PATH"
case "$*" in
  *"agent env get agent-"*) agent="${4:-}"; printf '{"agent_id":"%s","custom_env":{"MNEMON_MANAGED_RUNTIME":"codex-appserver","MNEMON_CONTROL_ADDR":"http://127.0.0.1:8787"}}\n' "$agent" ;;
  *"issue create"*) printf '{"id":"should-not-create"}\n' ;;
  *) printf '{}\n' ;;
esac
`
	writeFakeMultica(t, bin, argsPath, stdinPath, script)

	report, err := runMulticaRuntimeProdSimAcceptance(context.Background(), multicaRuntimeProdSimOptions{
		RunRoot:            filepath.Join(tmp, ".testdata", "multica-surface-runtime"),
		MulticaBin:         bin,
		WorkspaceID:        "ws-1",
		RegistryPath:       registryPath,
		AssigneePrincipal:  "planner@team",
		Wait:               time.Millisecond,
		Poll:               time.Millisecond,
		RequireSurfaceFlow: true,
		MinParticipants:    3,
		MinActiveAgents:    2,
	})
	if err == nil {
		t.Fatalf("surface-flow prerequisite should fail: %+v", report)
	}
	var readiness *multicaRuntimeProdSimAssertion
	for i := range report.Assertions {
		if report.Assertions[i].Name == "surface-flow agents expose provider wrapper" {
			readiness = &report.Assertions[i]
			break
		}
	}
	if readiness == nil || readiness.Passed || !strings.Contains(readiness.Detail, "missing (not surface-flow capable)") {
		t.Fatalf("readiness assertion mismatch: %+v", report.Assertions)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(args), "issue create") {
		t.Fatalf("issue create must not run when provider wrapper env is missing:\n%s", args)
	}
}

func TestMulticaMetadataContainsLegacyHubKeys(t *testing.T) {
	if !multicaMetadataContainsLegacyHubKeys(map[string]string{"mnemon.kind": "assignment_mailbox"}) {
		t.Fatal("legacy mnemon.kind must be rejected")
	}
	if multicaMetadataContainsLegacyHubKeys(map[string]string{driver.MulticaMetadataSurfaceRole: driver.MulticaSurfaceRoleDisplay}) {
		t.Fatal("R3 surface metadata should be accepted")
	}
}

func writeMulticaRuntimeTestRegistry(t *testing.T, tmp string, roles []string) string {
	t.Helper()
	registryPath := filepath.Join(tmp, "registry.json")
	var participants []driver.MulticaParticipantRecord
	for _, role := range roles {
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
	return registryPath
}

func writeFakeMultica(t *testing.T, bin, argsPath, stdinPath, script string) {
	t.Helper()
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MULTICA_ARGS_PATH", argsPath)
	t.Setenv("MULTICA_STDIN_PATH", stdinPath)
}
