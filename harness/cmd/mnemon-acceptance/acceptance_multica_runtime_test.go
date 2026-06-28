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
  *"issue run-messages task-9"*) printf '[{"task_id":"task-9","issue_id":"iss-9","seq":1,"type":"assistant","content":"Mnemon Multica runtime handled issue TEA-9. Mnemon ingest: recorded seq=17. Managed wake: completed turn=noop-turn.","created_at":"2026-06-28T09:00:01Z"}]\n' ;;
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
		t.Fatalf("acceptance: %v", err)
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
