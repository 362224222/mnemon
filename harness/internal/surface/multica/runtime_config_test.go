package multica

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRuntimeEnvValueUsesLastValue(t *testing.T) {
	env := []string{
		"MNEMON_MULTICA_PROVIDER_RUNTIME=codex",
		"MNEMON_MULTICA_PROVIDER_RUNTIME=off",
	}
	if got := RuntimeEnvValue(env, "MNEMON_MULTICA_PROVIDER_RUNTIME"); got != "off" {
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
	if ctx.ControlAddr != "http://127.0.0.1:8787" || ctx.ControlPrincipal != "planner@team" {
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

func TestRuntimeMulticaRegistryPaths(t *testing.T) {
	tmp := t.TempDir()
	explicit := filepath.Join(tmp, "explicit-registry.json")
	workspace := filepath.Join(tmp, "provider-workspace")
	cwd := filepath.Join(tmp, "task-workdir")
	got := RuntimeMulticaRegistryPaths([]string{
		"MNEMON_MULTICA_REGISTRY=" + explicit,
		"MNEMON_MULTICA_PROVIDER_WORKSPACE=" + workspace,
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

func TestRuntimeMulticaRegistryLoadsProviderWorkspace(t *testing.T) {
	tmp := t.TempDir()
	workspace := filepath.Join(tmp, "provider-workspace")
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
	reg, ok, err := RuntimeMulticaRegistry([]string{"MNEMON_MULTICA_PROVIDER_WORKSPACE=" + workspace}, tmp)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || reg.WorkspaceID != "ws-1" {
		t.Fatalf("registry = ok:%v %+v", ok, reg)
	}
}
