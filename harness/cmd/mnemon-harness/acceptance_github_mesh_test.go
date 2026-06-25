package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

func TestR1GitHubMeshBranchesDefaultShape(t *testing.T) {
	got := r1GitHubMeshBranches("mnemon/agent-", 5)
	want := []string{"mnemon/agent-a", "mnemon/agent-b", "mnemon/agent-c", "mnemon/agent-d", "mnemon/agent-e"}
	if len(got) != len(want) {
		t.Fatalf("branches = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("branches = %v, want %v", got, want)
		}
	}
}

func TestWriteR1GitHubMeshRemotesCreatesPublishAndSubscribePlan(t *testing.T) {
	root := t.TempDir()
	tokenFile := filepath.Join(root, "github.token")
	if err := os.WriteFile(tokenFile, []byte("secret-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	workspace := filepath.Join(root, "workspaces", "codex-03")
	if err := os.MkdirAll(workspace, 0o755); err != nil {
		t.Fatal(err)
	}
	branches := r1GitHubMeshBranches("mnemon/agent-", 5)
	if err := writeR1GitHubMeshRemotes(workspace, "mnemon-dev/mnemon-teamwork-example", tokenFile, branches, 2); err != nil {
		t.Fatalf("write github mesh remotes: %v", err)
	}

	remotesPath := filepath.Join(workspace, ".mnemon", "harness", "sync", "remotes.json")
	plan, err := exchange.LoadRemotePlan(remotesPath, "default")
	if err != nil {
		t.Fatalf("load remote plan: %v", err)
	}
	if len(plan.PushTargets) != 1 || plan.PushTargets[0].ID != "self" || plan.PushTargets[0].Branch != "mnemon/agent-c" {
		t.Fatalf("push targets = %+v, want self on mnemon/agent-c", plan.PushTargets)
	}
	if len(plan.PullSources) != 4 {
		t.Fatalf("pull sources = %+v, want four peer streams", plan.PullSources)
	}
	for _, remote := range append(plan.PushTargets, plan.PullSources...) {
		if remote.Backend != exchange.RemoteBackendGitHub ||
			remote.Repo != "mnemon-dev/mnemon-teamwork-example" ||
			remote.CredentialRef != tokenFile ||
			remote.Endpoint != "" {
			t.Fatalf("remote not a github publication stream: %+v", remote)
		}
	}
}

func TestBuildR1GitHubMeshSyncReportProvesIsolationAndNoHub(t *testing.T) {
	root := t.TempDir()
	tokenFile := filepath.Join(root, "github.token")
	if err := os.WriteFile(tokenFile, []byte("secret-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	branches := r1GitHubMeshBranches("mnemon/agent-", 3)
	agents := make([]r1CodexSyncAgent, 0, 3)
	for i := range branches {
		workspace := filepath.Join(root, "workspaces", branches[i][len("mnemon/"):])
		if err := os.MkdirAll(workspace, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := writeR1GitHubMeshRemotes(workspace, "mnemon-dev/mnemon-teamwork-example", tokenFile, branches, i); err != nil {
			t.Fatal(err)
		}
		agents = append(agents, r1CodexSyncAgent{r1CodexAgent: r1CodexAgent{
			principal: "codex-0" + string(rune('1'+i)) + "@project",
			workspace: workspace,
		}})
	}

	report := buildR1GitHubMeshSyncReport("mnemon-dev/mnemon-teamwork-example", agents)
	if report.Backend != exchange.RemoteBackendGitHub || report.Repo != "mnemon-dev/mnemon-teamwork-example" || report.HubURL != "" {
		t.Fatalf("report backend/repo/hub wrong: %+v", report)
	}
	if len(report.PublicationBranches) != 3 || len(report.BranchByAgent) != 3 {
		t.Fatalf("report branches wrong: %+v", report)
	}
	if !distinctStrings(report.RuntimeWorkspaces) || !distinctStrings(report.LocalStorePaths) {
		t.Fatalf("report must prove isolated workspaces/stores: workspaces=%v stores=%v", report.RuntimeWorkspaces, report.LocalStorePaths)
	}
	for i, storePath := range report.LocalStorePaths {
		want := filepath.Join(agents[i].workspace, runtime.DefaultStorePath)
		if storePath != want {
			t.Fatalf("store path[%d] = %q, want %q", i, storePath, want)
		}
	}
}

func TestR1GitHubMeshScenarioContract(t *testing.T) {
	names := r1GitHubMeshScenarioNames(nil)
	want := []string{"onboarding-synthesis", "sync-risk-review", "live-readiness-operator-safety"}
	if len(names) != len(want) {
		t.Fatalf("default scenarios = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("default scenarios = %v, want %v", names, want)
		}
	}
	run := r1GitHubMeshRun{agents: make([]r1CodexSyncAgent, 5)}
	entries, err := run.scenarioEntries("live-readiness-operator-safety")
	if err != nil {
		t.Fatalf("multi-poc scenario entries: %v", err)
	}
	if len(entries) != 2 || entries[0].index == entries[1].index {
		t.Fatalf("live-readiness scenario must use two distinct PoC agents, got %+v", entries)
	}
	for _, entry := range entries {
		for _, forbidden := range []string{"Agent A must", "Agent B must", "must create assignments for"} {
			if strings.Contains(entry.prompt, forbidden) {
				t.Fatalf("natural prompt contains forced choreography %q: %s", forbidden, entry.prompt)
			}
		}
	}
}

func TestR1GitHubMeshScenarioStatusRequiresSelectedOK(t *testing.T) {
	scenarios := []r1TaskSimScenarioReport{
		{Name: "onboarding-synthesis", Status: "ok"},
		{Name: "sync-risk-review", Status: "failed"},
		{Name: "live-readiness-operator-safety", Status: "ok"},
	}
	if allR1GitHubMeshScenariosOK(scenarios, nil) {
		t.Fatal("default scenario set must require every selected scenario to be ok")
	}
	scenarios[1].Status = "ok"
	if !allR1GitHubMeshScenariosOK(scenarios, nil) {
		t.Fatal("default scenario set should pass when every default scenario is ok")
	}
	if !allR1GitHubMeshScenariosOK(scenarios[:1], []string{"onboarding-synthesis"}) {
		t.Fatal("explicit scenario selection should require only the selected scenario")
	}
}

func TestR1GitHubMeshPromptRoundsCountsScenarioPrompts(t *testing.T) {
	contract := &r1RunnerContractReport{}
	recordR1ClusterPrompt(contract, "codex-01@project", "natural_user_message:onboarding-synthesis", "prompt")
	recordR1ClusterPrompt(contract, "codex-02@project", "worker_wake:onboarding-synthesis", "wake")
	recordR1ClusterPrompt(contract, "codex-01@project", "integration:onboarding-synthesis", "integrate")
	recordR1ClusterPrompt(contract, "codex-01@project", "integration:other", "other")
	if got := r1GitHubMeshPromptRounds(contract, "onboarding-synthesis"); got != 3 {
		t.Fatalf("prompt rounds = %d, want 3", got)
	}
}
