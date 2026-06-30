package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/driver"
)

func TestMulticaTaskCaseProtocolReActIsMultiRound(t *testing.T) {
	started := time.Date(2026, 6, 30, 9, 10, 11, 0, time.UTC)
	material, err := multicaAcceptanceTaskCase(multicaAcceptanceTaskCaseProtocolReAct, started)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"Protocol ReAct collaboration drill",
		"Round 1 - Observe",
		"Round 2 - Act",
		"Round 3 - Reflect",
		"follow-up assignment",
	} {
		if !strings.Contains(material.Title+"\n"+material.Description, want) {
			t.Fatalf("task case missing %q:\n%s\n%s", want, material.Title, material.Description)
		}
	}
	if material.Expectations.MinActiveAgents != 4 ||
		material.Expectations.MinChildSurfaces != 3 ||
		material.Expectations.MinFeedbackComments != 3 ||
		len(material.Expectations.TeamworkRounds) != 3 {
		t.Fatalf("unexpected expectations: %+v", material.Expectations)
	}
}

func TestMulticaTaskCaseParallelPocMaterializesIsolatedOverlapPlan(t *testing.T) {
	started := time.Date(2026, 6, 30, 9, 10, 11, 0, time.UTC)
	material, err := multicaAcceptanceTaskCase(multicaAcceptanceTaskCaseParallelPoc, started)
	if err != nil {
		t.Fatal(err)
	}
	runRoot := t.TempDir()
	plan, err := materializeMulticaAcceptanceExecutionPlan(runRoot, material)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Workstreams) != 3 || len(plan.Roles) != 5 || len(plan.SharedContexts) < 4 {
		t.Fatalf("unexpected plan shape: %+v", plan)
	}
	for _, dir := range append([]string{plan.CaseRoot, plan.SharedContextDir, plan.EvidenceDir}, multicaPlanDirs(plan)...) {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("missing plan dir %s: %v", dir, err)
		}
		if !info.IsDir() {
			t.Fatalf("plan path is not a dir: %s", dir)
		}
		rel, err := filepath.Rel(runRoot, dir)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
			t.Fatalf("plan dir escaped run root: root=%s dir=%s rel=%s", runRoot, dir, rel)
		}
	}
	reused := false
	for _, shared := range plan.SharedContexts {
		if len(shared.UsedBy) > 1 {
			reused = true
			break
		}
	}
	if !reused {
		t.Fatalf("expected at least one shared context reused by multiple PoCs: %+v", plan.SharedContexts)
	}
	rendered := renderMulticaAcceptanceExecutionPlan(plan)
	for _, want := range []string{
		"Parallel PoCs",
		"Context Reuse Checks",
		"poc-runtime-routing",
		"poc-operator-runbook",
		"poc-release-risk",
		"evidence-ledger",
	} {
		if !strings.Contains(rendered, want) {
			t.Fatalf("rendered plan missing %q:\n%s", want, rendered)
		}
	}
}

func TestMulticaRuntimeProdSimTaskCaseWritesExecutionPlanToIssue(t *testing.T) {
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
  *"issue create"*) printf '{"id":"iss-poc","identifier":"TEA-50","title":"Parallel PoC overlap drill","description":"Teamwork acceptance","status":"todo"}\n' ;;
  *"issue runs iss-poc"*) printf '[{"id":"task-poc","issue_id":"iss-poc","agent_id":"agent-1","status":"completed","completed_at":"2026-06-30T09:00:00Z","workspace_id":"ws-1"}]\n' ;;
  *"issue run-messages task-poc"*) printf '[{"task_id":"task-poc","issue_id":"iss-poc","seq":1,"type":"assistant","content":"Mnemon Multica runtime handled issue TEA-50. Multica surface input: observed.","created_at":"2026-06-30T09:00:01Z"}]\n' ;;
  *) printf '{}\n' ;;
esac
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MULTICA_ARGS_PATH", argsPath)
	t.Setenv("MULTICA_STDIN_PATH", stdinPath)

	report, err := runMulticaRuntimeProdSimAcceptance(context.Background(), multicaRuntimeProdSimOptions{
		RunRoot:           filepath.Join(tmp, ".testdata", "multica-parallel-poc"),
		MulticaBin:        bin,
		WorkspaceID:       "ws-1",
		RegistryPath:      registryPath,
		AssigneePrincipal: "planner@team",
		TaskCase:          multicaAcceptanceTaskCaseParallelPoc,
		Wait:              time.Millisecond,
		Poll:              time.Millisecond,
		RequireIngest:     true,
	})
	if err != nil {
		t.Fatalf("acceptance: %v report=%+v", err, report)
	}
	if report.TaskCase != multicaAcceptanceTaskCaseParallelPoc ||
		report.TaskExpectations.MinActiveAgents != 5 ||
		report.TaskExpectations.InitialChildSurfaces != 3 ||
		report.TaskExpectations.MinChildSurfaces != 4 ||
		len(report.ExecutionPlan.Workstreams) != 3 {
		t.Fatalf("task case report mismatch: %+v", report)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "issue create --title Parallel PoC overlap drill") {
		t.Fatalf("issue title did not come from task case:\n%s", args)
	}
	stdin, err := os.ReadFile(stdinPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"## Execution Plan",
		"## Parallel PoCs",
		"## Context Reuse Checks",
		"不要把 Multica 当 canonical state store",
		"activation carrier",
		"activation-carrier commands",
		"poc-runtime-routing",
		"poc-operator-runbook",
		"poc-release-risk",
	} {
		if !strings.Contains(string(stdin), want) {
			t.Fatalf("issue stdin missing %q:\n%s", want, stdin)
		}
	}
}

func TestMulticaRuntimeProdSimRejectsUnknownTaskCase(t *testing.T) {
	runRoot := filepath.Join(t.TempDir(), ".testdata", "unknown-task-case")
	report, err := runMulticaRuntimeProdSimAcceptance(context.Background(), multicaRuntimeProdSimOptions{
		RunRoot:  runRoot,
		TaskCase: "missing-task-case",
		Wait:     time.Millisecond,
		Poll:     time.Millisecond,
	})
	if err == nil {
		t.Fatal("expected unknown task case error")
	}
	if !strings.Contains(err.Error(), "unknown Multica task case") {
		t.Fatalf("unexpected error: %v", err)
	}
	if report.Status != "failed" || report.ReportPath == "" || !strings.HasPrefix(report.ReportPath, runRoot) {
		t.Fatalf("report mismatch: %+v", report)
	}
	if _, err := os.Stat(report.ReportPath); err != nil {
		t.Fatalf("report was not written: %v", err)
	}
}

func multicaPlanDirs(plan multicaAcceptanceExecutionPlan) []string {
	var out []string
	for _, stream := range plan.Workstreams {
		out = append(out, stream.Directory)
	}
	for _, role := range plan.Roles {
		out = append(out, role.Directory)
	}
	for _, shared := range plan.SharedContexts {
		out = append(out, shared.Directory)
	}
	return out
}
