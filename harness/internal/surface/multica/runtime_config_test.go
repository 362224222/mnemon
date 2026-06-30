package multica

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRuntimeEnvValueUsesLastValue(t *testing.T) {
	env := []string{
		"MNEMON_MANAGED_RUNTIME=codex-appserver",
		"MNEMON_MANAGED_RUNTIME=off",
	}
	if got := RuntimeEnvValue(env, "MNEMON_MANAGED_RUNTIME"); got != "off" {
		t.Fatalf("RuntimeEnvValue = %q, want off", got)
	}
	if got := RuntimeEnvDefault(env, "MISSING", "fallback"); got != "fallback" {
		t.Fatalf("RuntimeEnvDefault = %q, want fallback", got)
	}
}

func TestRuntimeContextFromActivationNormalizesDaemonAndRuntimeMetadata(t *testing.T) {
	ctx := RuntimeContextFromActivation([]string{
		"MULTICA_TASK_ID=task-1",
		"MULTICA_ISSUE_ID=iss-env",
		"MULTICA_AGENT_ID=agent-1",
		"MULTICA_AGENT_NAME=mnemon-planner",
		"MULTICA_WORKSPACE_ID=ws-daemon",
		"MNEMON_MULTICA_WORKSPACE_ID=ws-mnemon",
		"MULTICA_SERVER_URL=https://api.multica.ai",
		"MNEMON_MULTICA_SERVER_URL=https://desktop-api.multica.ai",
		"MNEMON_HUB_BACKEND=multica",
		"MNEMON_CONTROL_ADDR=http://127.0.0.1:8787",
		"MNEMON_CONTROL_PRINCIPAL=planner@team",
	}, "/repo", RuntimeInput{
		Text:                "Ignore copied @TEA-1.",
		IssueIdentity:       "iss-input",
		IssueIdentitySource: RuntimeIssueSourceInput,
	})
	if ctx.IssueIdentity != "iss-env" || ctx.IssueIdentitySource != RuntimeIssueSourceEnv {
		t.Fatalf("issue identity/source = %q/%q", ctx.IssueIdentity, ctx.IssueIdentitySource)
	}
	if ctx.TaskID != "task-1" || ctx.AgentID != "agent-1" || ctx.AgentName != "mnemon-planner" {
		t.Fatalf("task/agent metadata mismatch: %+v", ctx)
	}
	if ctx.WorkspaceID != "ws-mnemon" || ctx.ServerURL != "https://desktop-api.multica.ai" {
		t.Fatalf("workspace/server metadata mismatch: %+v", ctx)
	}
	if ctx.HubBackend != "multica" || ctx.ControlAddr != "http://127.0.0.1:8787" || ctx.ControlPrincipal != "planner@team" {
		t.Fatalf("Mnemon control metadata mismatch: %+v", ctx)
	}
}

func TestRuntimeContextFromActivationFallsBackToStructuredInput(t *testing.T) {
	ctx := RuntimeContextFromActivation(nil, "", RuntimeInput{
		Text:                "Please review the linked issue.",
		IssueIdentity:       "iss-selected",
		IssueIdentitySource: RuntimeIssueSourceInput,
	})
	if ctx.IssueIdentity != "iss-selected" || ctx.IssueIdentitySource != RuntimeIssueSourceInput {
		t.Fatalf("structured input issue mismatch: %+v", ctx)
	}
}

func TestRuntimeContextFromActivationFallsBackToVisibleIssueTag(t *testing.T) {
	ctx := RuntimeContextFromActivation(nil, "", RuntimeInput{Text: "Please handle @TEA-50 next."})
	if ctx.IssueIdentity != "TEA-50" || ctx.IssueIdentitySource != RuntimeIssueSourceInputText {
		t.Fatalf("visible tag issue mismatch: %+v", ctx)
	}
}

func TestMulticaRuntimeCommandNamePinned(t *testing.T) {
	if MulticaRuntimeCommandName != "mnemon-multica-runtime" {
		t.Fatalf("MulticaRuntimeCommandName = %q", MulticaRuntimeCommandName)
	}
	if MulticaRuntimeProfileName != "mnemon-runtime" {
		t.Fatalf("MulticaRuntimeProfileName = %q", MulticaRuntimeProfileName)
	}
}

func TestRuntimeTimeoutUsesMulticaHTTPFallback(t *testing.T) {
	if got := RuntimeTimeout([]string{"MULTICA_HTTP_TIMEOUT=2m"}); got != 2*time.Minute {
		t.Fatalf("RuntimeTimeout fallback = %s, want 2m", got)
	}
	if got := RuntimeTimeout([]string{"MULTICA_HTTP_TIMEOUT=2m", "MNEMON_MULTICA_RUNTIME_TIMEOUT=15s"}); got != 15*time.Second {
		t.Fatalf("RuntimeTimeout override = %s, want 15s", got)
	}
	if got := RuntimeTimeout([]string{"MNEMON_MULTICA_RUNTIME_TIMEOUT=bad"}); got != 30*time.Second {
		t.Fatalf("RuntimeTimeout invalid = %s, want default 30s", got)
	}
}

func TestRuntimeAdapterSwitches(t *testing.T) {
	if !RuntimeProjectionCommentsEnabled(nil) || !RuntimeHubWriteEnabled(nil) {
		t.Fatal("runtime switches should default on")
	}
	if RuntimeProjectionCommentsEnabled([]string{"MNEMON_MULTICA_PROJECT_COMMENTS=off"}) {
		t.Fatal("projection comments should honor off")
	}
	if RuntimeHubWriteEnabled([]string{"MNEMON_MULTICA_HUB_WRITE=disabled"}) {
		t.Fatal("hub write should honor disabled")
	}
}

func TestRuntimeManagedLedgerPath(t *testing.T) {
	tmp := t.TempDir()
	workspace := filepath.Join(tmp, "managed-workspace")
	want := filepath.Join(workspace, ".mnemon", "harness", "local", "managed-agent", "wake-ledger.jsonl")
	if got := RuntimeManagedLedgerPath(nil, workspace); got != want {
		t.Fatalf("RuntimeManagedLedgerPath = %q, want %q", got, want)
	}
	explicit := filepath.Join(tmp, "explicit.jsonl")
	if got := RuntimeManagedLedgerPath([]string{"MNEMON_MANAGED_LEDGER=" + explicit}, workspace); got != explicit {
		t.Fatalf("explicit RuntimeManagedLedgerPath = %q, want %q", got, explicit)
	}
}

func TestRuntimeMulticaHubLedgerPath(t *testing.T) {
	tmp := t.TempDir()
	workspace := filepath.Join(tmp, "managed-workspace")
	cwd := filepath.Join(tmp, "task-workdir")
	got := RuntimeMulticaHubLedgerPath([]string{"MNEMON_MANAGED_WORKSPACE=" + workspace}, cwd)
	want := filepath.Join(workspace, MulticaDefaultHubLedgerRelPath)
	if got != want {
		t.Fatalf("hub ledger path = %q, want %q", got, want)
	}
	explicit := filepath.Join(tmp, "explicit.jsonl")
	got = RuntimeMulticaHubLedgerPath([]string{
		"MNEMON_MANAGED_WORKSPACE=" + workspace,
		"MNEMON_MULTICA_HUB_LEDGER=" + explicit,
	}, cwd)
	if got != explicit {
		t.Fatalf("explicit hub ledger path = %q, want %q", got, explicit)
	}
}

func TestRuntimeMulticaRegistryPaths(t *testing.T) {
	tmp := t.TempDir()
	explicit := filepath.Join(tmp, "explicit-registry.json")
	workspace := filepath.Join(tmp, "managed-workspace")
	cwd := filepath.Join(tmp, "task-workdir")
	got := RuntimeMulticaRegistryPaths([]string{
		"MNEMON_MULTICA_REGISTRY=" + explicit,
		"MNEMON_MANAGED_WORKSPACE=" + workspace,
	}, cwd)
	want := []string{
		explicit,
		MulticaRegistryPath(workspace, ""),
		MulticaRegistryPath(cwd, ""),
	}
	if len(got) != len(want) {
		t.Fatalf("registry paths len = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("registry path %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestRuntimeMulticaRegistryLoadsManagedWorkspace(t *testing.T) {
	tmp := t.TempDir()
	workspace := filepath.Join(tmp, "managed-workspace")
	path := MulticaRegistryPath(workspace, "")
	if err := SaveMulticaRegistry(path, MulticaRegistry{
		WorkspaceID: "ws-1",
		Participants: []MulticaParticipantRecord{{
			Principal: "planner@team",
			AgentName: "mnemon-planner",
			AgentID:   "agent-planner",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	reg, ok, err := RuntimeMulticaRegistry([]string{"MNEMON_MANAGED_WORKSPACE=" + workspace}, tmp)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || reg.WorkspaceID != "ws-1" {
		t.Fatalf("registry = ok:%v %+v", ok, reg)
	}
}

func TestRuntimeManagedWakeScopeIDPrefersAssignmentThenRoot(t *testing.T) {
	if got := RuntimeManagedWakeScopeID(RuntimeManagedWakeMaterial{
		AssignmentID: "asg-1",
		RootIssueID:  "root-1",
		IssueID:      "child-1",
	}); got != "asg-1" {
		t.Fatalf("assignment mailbox scope = %q, want asg-1", got)
	}
	if got := RuntimeManagedWakeScopeID(RuntimeManagedWakeMaterial{
		RootIssueID: "root-1",
		IssueID:     "root-1",
	}); got != "root-1" {
		t.Fatalf("root session scope = %q, want root-1", got)
	}
}

func TestRuntimeManagedTurnEnvInjectsRenderScope(t *testing.T) {
	env := RuntimeManagedTurnEnv([]string{"EXISTING=1"}, RuntimeManagedWakeMaterial{
		SessionID:    "multica:session:root-1",
		RootIssueID:  "root-1",
		AssignmentID: "asg-1",
	})
	if got := RuntimeEnvValue(env, "MNEMON_RENDER_HOST"); got != "multica" {
		t.Fatalf("render host = %q", got)
	}
	if got := RuntimeEnvValue(env, "MNEMON_RENDER_SESSION_ID"); got != "multica:session:root-1" {
		t.Fatalf("render session = %q", got)
	}
	if got := RuntimeEnvValue(env, "MNEMON_RENDER_INPUT_ID"); got != "asg-1" {
		t.Fatalf("render input = %q", got)
	}

	preserved := RuntimeManagedTurnEnv([]string{"MNEMON_RENDER_HOST=custom"}, RuntimeManagedWakeMaterial{SessionID: "session-1", RootIssueID: "root-1"})
	if got := RuntimeEnvValue(preserved, "MNEMON_RENDER_HOST"); got != "custom" {
		t.Fatalf("managed env should preserve explicit host, got %q", got)
	}
}
