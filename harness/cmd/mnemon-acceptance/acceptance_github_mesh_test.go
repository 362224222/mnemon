package main

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/codexapp"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

func TestR1GitHubMeshBranchesDefaultShape(t *testing.T) {
	got := r1GitHubMeshBranches("mnemon/mnemond-", 5)
	want := []string{"mnemon/mnemond-a", "mnemon/mnemond-b", "mnemon/mnemond-c", "mnemon/mnemond-d", "mnemon/mnemond-e"}
	if len(got) != len(want) {
		t.Fatalf("branches = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("branches = %v, want %v", got, want)
		}
	}
}

func TestR1GitHubMeshBranchPrefixDefaultsToRunScopedBranches(t *testing.T) {
	started := time.Date(2026, 6, 25, 18, 57, 20, 0, time.UTC)
	prefix := r1GitHubMeshBranchPrefix("", started)
	if prefix != "mnemon/mnemond-20260625T185720Z-" {
		t.Fatalf("prefix = %q, want run-scoped mnemond prefix", prefix)
	}
	got := r1GitHubMeshBranches(prefix, 2)
	want := []string{
		"mnemon/mnemond-20260625T185720Z-a",
		"mnemon/mnemond-20260625T185720Z-b",
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("branches = %v, want %v", got, want)
		}
	}
	if explicit := r1GitHubMeshBranchPrefix("mnemon/mnemond-team-", started); explicit != "mnemon/mnemond-team-" {
		t.Fatalf("explicit prefix = %q, want unchanged", explicit)
	}
}

func TestFetchR1GitHubMeshRateLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Fatalf("authorization header = %q", got)
		}
		_, _ = w.Write([]byte(`{"resources":{"core":{"limit":5000,"remaining":4321,"reset":1782725122,"used":679}}}`))
	}))
	defer server.Close()
	oldURL := r1GitHubMeshRateLimitAPIURL
	oldClient := r1GitHubMeshHTTPClient
	r1GitHubMeshRateLimitAPIURL = server.URL
	r1GitHubMeshHTTPClient = server.Client()
	t.Cleanup(func() {
		r1GitHubMeshRateLimitAPIURL = oldURL
		r1GitHubMeshHTTPClient = oldClient
	})

	limit, err := fetchR1GitHubMeshRateLimit(context.Background(), "secret-token")
	if err != nil {
		t.Fatal(err)
	}
	if limit.Limit != 5000 || limit.Remaining != 4321 || limit.Used != 679 || limit.ResetAt.Unix() != 1782725122 {
		t.Fatalf("rate limit mismatch: %+v", limit)
	}
}

func TestPreflightR1GitHubMeshRateLimitBlocksLowRemaining(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"resources":{"core":{"limit":5000,"remaining":42,"reset":1782725122,"used":4958}}}`))
	}))
	defer server.Close()
	oldURL := r1GitHubMeshRateLimitAPIURL
	oldClient := r1GitHubMeshHTTPClient
	r1GitHubMeshRateLimitAPIURL = server.URL
	r1GitHubMeshHTTPClient = server.Client()
	t.Cleanup(func() {
		r1GitHubMeshRateLimitAPIURL = oldURL
		r1GitHubMeshHTTPClient = oldClient
	})
	tokenFile := filepath.Join(t.TempDir(), "github.token")
	if err := os.WriteFile(tokenFile, []byte("secret-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	limit, err := preflightR1GitHubMeshRateLimit(context.Background(), tokenFile, 1500)
	if err == nil || !strings.Contains(err.Error(), "below required 1500") {
		t.Fatalf("expected low-rate-limit error, got limit=%+v err=%v", limit, err)
	}
	if limit.Remaining != 42 {
		t.Fatalf("rate limit should be returned for diagnostics: %+v", limit)
	}
}

func TestValidateR1GitHubMeshSyncIntervalProtectsAgentTurns(t *testing.T) {
	err := validateR1GitHubMeshSyncInterval(r1GitHubMeshAcceptanceOptions{
		r1CodexAcceptanceOptions: r1CodexAcceptanceOptions{AgentTurns: true},
		SyncInterval:             10 * time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), ">= 30s") {
		t.Fatalf("expected short sync interval guard, got %v", err)
	}
	if err := validateR1GitHubMeshSyncInterval(r1GitHubMeshAcceptanceOptions{
		r1CodexAcceptanceOptions: r1CodexAcceptanceOptions{AgentTurns: true},
		SyncInterval:             30 * time.Second,
	}); err != nil {
		t.Fatalf("30s sync interval should be allowed: %v", err)
	}
	if err := validateR1GitHubMeshSyncInterval(r1GitHubMeshAcceptanceOptions{SyncInterval: 10 * time.Second}); err != nil {
		t.Fatalf("short non-agent-turn sync interval should be allowed: %v", err)
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
	branches := r1GitHubMeshBranches("mnemon/mnemond-", 5)
	if err := writeR1GitHubMeshRemotes(workspace, "mnemon-dev/mnemon-teamwork-example", tokenFile, branches, 2); err != nil {
		t.Fatalf("write github mesh remotes: %v", err)
	}

	remotesPath := filepath.Join(workspace, ".mnemon", "harness", "sync", "remotes.json")
	plan, err := exchange.LoadRemotePlan(remotesPath, "default")
	if err != nil {
		t.Fatalf("load remote plan: %v", err)
	}
	if len(plan.PushTargets) != 1 || plan.PushTargets[0].ID != "self" || plan.PushTargets[0].Branch != "mnemon/mnemond-c" {
		t.Fatalf("push targets = %+v, want self on mnemon/mnemond-c", plan.PushTargets)
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

func TestSetupR1CodexGitHubMeshAgentsCanDelayLocalMnemondStart(t *testing.T) {
	root := t.TempDir()
	tokenFile := filepath.Join(root, "github.token")
	if err := os.WriteFile(tokenFile, []byte("secret-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	agents, err := setupR1CodexGitHubMeshAgents(context.Background(), root, root, "mnemon-dev/mnemon-teamwork-example", tokenFile, "mnemon/mnemond-", 5, "", 30*time.Second, 0)
	if err != nil {
		t.Fatalf("setup delayed github mesh agents: %v", err)
	}
	if len(agents) != 5 {
		t.Fatalf("agents = %d, want 5", len(agents))
	}
	if got := r1GitHubMeshLocalOnlineIndexes(agents); len(got) != 0 {
		t.Fatalf("local online indexes = %v, want none before delayed start", got)
	}
	for i, agent := range agents {
		if agent.localCancel != nil || agent.localErr != nil {
			t.Fatalf("agent %d local mnemond should not be started", i)
		}
		remotesPath := filepath.Join(agent.workspace, ".mnemon", "harness", "sync", "remotes.json")
		plan, err := exchange.LoadRemotePlan(remotesPath, "default")
		if err != nil {
			t.Fatalf("load delayed remote plan %d: %v", i, err)
		}
		if len(plan.PushTargets) != 1 || len(plan.PullSources) != 4 {
			t.Fatalf("remote plan %d = %+v, want one publish and four subscribe streams", i, plan)
		}
	}
}

func TestR1GitHubMeshInitialOnlineLeavesTwoDelayedAgents(t *testing.T) {
	if got := r1GitHubMeshInitialOnline(5); got != 3 {
		t.Fatalf("initial online for 5 = %d, want 3", got)
	}
	if got := r1GitHubMeshInitialOnline(7); got != 5 {
		t.Fatalf("initial online for 7 = %d, want 5", got)
	}
	agents := []r1CodexSyncAgent{
		{localCancel: func() {}},
		{},
		{localCancel: func() {}},
	}
	got := r1GitHubMeshLocalOnlineIndexes(agents)
	if len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Fatalf("local online indexes = %v, want [0 2]", got)
	}
}

func TestBuildR1GitHubMeshSyncReportProvesIsolationAndNoHub(t *testing.T) {
	root := t.TempDir()
	tokenFile := filepath.Join(root, "github.token")
	if err := os.WriteFile(tokenFile, []byte("secret-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	branches := r1GitHubMeshBranches("mnemon/mnemond-", 3)
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
	if report.TransportModel != "repo-mediated-publication" || report.RosterSource != "configured-remotes-json" || report.NetworkDiscovery != "none" {
		t.Fatalf("report must pin github bootstrap semantics without p2p discovery: %+v", report)
	}
	if len(report.PublicationBranches) != 3 || len(report.BranchByAgent) != 3 {
		t.Fatalf("report branches wrong: %+v", report)
	}
	if len(report.RemotePlanPaths) != 3 || !distinctStrings(report.RemotePlanPaths) {
		t.Fatalf("report must expose one remotes.json per workspace, got %+v", report.RemotePlanPaths)
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

func TestR1GitHubMeshStrictTopologyRequiresNoHub(t *testing.T) {
	top := &r1AcceptanceTopologyReport{
		Mode:               "per-hostagent-mnemond",
		Agents:             5,
		MnemondInstances:   5,
		MnemonhubInstances: 0,
		SharedMnemond:      false,
		AgentMnemondMap: map[string]string{
			"a": "/tmp/a.db",
			"b": "/tmp/b.db",
			"c": "/tmp/c.db",
			"d": "/tmp/d.db",
			"e": "/tmp/e.db",
		},
	}
	if !r1GitHubMeshStrictTopology(top) {
		t.Fatalf("github mesh topology should pass without a central hub: %+v", top)
	}
	top.MnemonhubInstances = 1
	if r1GitHubMeshStrictTopology(top) {
		t.Fatal("github mesh topology must reject central mnemon-hub instances")
	}
}

func TestPopulateR1GitHubMeshSyncEvidence(t *testing.T) {
	report := &r1CodexAcceptanceReport{Sync: &r1CodexSyncReport{
		Agents: []r1CodexAgentReport{
			{Principal: "codex-01@project"},
			{Principal: "codex-02@project"},
		},
		BranchByAgent: map[string]string{
			"codex-01@project": "mnemon/mnemond-run-a",
			"codex-02@project": "mnemon/mnemond-run-b",
		},
	}}
	obs := acceptanceObserveReport{Stores: []acceptanceStoreInspect{
		{
			Name:               "codex-01",
			Role:               "mnemond",
			Counts:             map[string]int{"imported_accepted": 4},
			SyncEventsByStatus: map[string]int{"synced": 3},
			ObservedByType:     map[string]int{"sync.diagnostic": 1},
			EnvelopeByType:     map[string]int{"agent_profile.accepted": 5},
		},
		{
			Name:               "codex-02",
			Role:               "mnemond",
			Counts:             map[string]int{"imported_accepted": 2},
			SyncEventsByStatus: map[string]int{"synced": 1},
			ObservedByType:     map[string]int{"sync.remote_diagnostic.observed": 2},
			EnvelopeByType:     map[string]int{"agent_profile.accepted": 3},
		},
	}}

	populateR1GitHubMeshSyncEvidence(report, obs)

	if got := report.Sync.PublishedByBranch["mnemon/mnemond-run-a"]; got != 3 {
		t.Fatalf("published branch a = %d, want 3", got)
	}
	if got := report.Sync.ImportedByMnemond["codex-02@project"]; got != 2 {
		t.Fatalf("imported codex-02 = %d, want 2", got)
	}
	if got := report.Sync.DiagnosticsByMnemond["codex-02@project"]; got != 2 {
		t.Fatalf("diagnostics codex-02 = %d, want 2", got)
	}
	if got := report.Sync.ProfileByMnemond["codex-01@project"]; got != 5 {
		t.Fatalf("profiles codex-01 = %d, want 5", got)
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
	if !strings.Contains(r1GitHubMeshWorkerWakePrompt, "agent_profile") {
		t.Fatal("github mesh worker wake prompt should let agents naturally refresh their profile")
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

func TestR1GitHubMeshNaturalSuiteEvidenceHelpers(t *testing.T) {
	scenarios := []r1TaskSimScenarioReport{
		{Name: "onboarding-synthesis", Status: "ok", Evidence: map[string]any{"replanning_rounds": 3}},
		{Name: "sync-risk-review", Status: "ok", Evidence: map[string]any{"cross_task_reuse_or_completion": true}},
		{Name: "live-readiness-operator-safety", Status: "ok", Evidence: map[string]any{"multi_poc": true}},
	}
	prior := r1GitHubMeshOKScenarioNames(scenarios[:1])
	if !r1GitHubMeshCrossTaskReuseCandidate("sync-risk-review", prior) {
		t.Fatal("sync-risk-review should be a cross-task reuse candidate after onboarding-synthesis")
	}
	if r1GitHubMeshCrossTaskReuseCandidate("sync-risk-review", nil) {
		t.Fatal("sync-risk-review without prior onboarding should not claim cross-task reuse")
	}
	if !r1GitHubMeshHasOKScenarioEvidenceBool(scenarios, "live-readiness-operator-safety", "multi_poc") {
		t.Fatal("multi-poc evidence should be detected on the successful live-readiness scenario")
	}
	if !r1GitHubMeshHasOKScenarioEvidenceBool(scenarios, "sync-risk-review", "cross_task_reuse_or_completion") {
		t.Fatal("cross-task evidence should be detected on the successful sync-risk scenario")
	}
	if !r1GitHubMeshHasAnyOKScenarioEvidenceIntAtLeast(scenarios, "replanning_rounds", 2) {
		t.Fatal("replanning evidence should accept int counts")
	}
	scenarios[0].Evidence["replanning_rounds"] = float64(2)
	if !r1GitHubMeshHasAnyOKScenarioEvidenceIntAtLeast(scenarios, "replanning_rounds", 2) {
		t.Fatal("replanning evidence should accept json-decoded float64 counts")
	}
	scenarios[2].Status = "failed"
	if r1GitHubMeshHasOKScenarioEvidenceBool(scenarios, "live-readiness-operator-safety", "multi_poc") {
		t.Fatal("failed scenarios must not satisfy evidence assertions")
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
	if got := r1GitHubMeshPromptKindCount(contract, "worker_wake:onboarding-synthesis"); got != 1 {
		t.Fatalf("worker wake prompt count = %d, want 1", got)
	}
}

func TestR1GitHubMeshEntrySeedTimeoutIsBounded(t *testing.T) {
	if got := r1GitHubMeshEntrySeedTimeout(8 * time.Minute); got != 8*time.Minute {
		t.Fatalf("seed timeout = %s, want full turn timeout", got)
	}
	if got := r1GitHubMeshEntrySeedTimeout(2 * time.Minute); got != 2*time.Minute {
		t.Fatalf("short seed timeout = %s, want full short turn timeout", got)
	}
	if got := r1GitHubMeshEntrySeedTimeout(0); got != 5*time.Minute {
		t.Fatalf("default seed timeout = %s, want 5m", got)
	}
}

func TestR1GitHubMeshEntrySeedReadyUsesAssignment(t *testing.T) {
	if !r1GitHubMeshEntrySeedReady(map[string]int{"assignment": 1}) {
		t.Fatal("assignment should be enough to seed worker handoff")
	}
	if r1GitHubMeshEntrySeedReady(map[string]int{"teamwork_signal": 1}) {
		t.Fatal("signal without assignment should not seed worker handoff")
	}
}

func TestR1GitHubMeshProfileConvergenceTimeoutScalesWithSyncInterval(t *testing.T) {
	if got := r1GitHubMeshProfileConvergenceTimeout(30 * time.Second); got != 150*time.Second {
		t.Fatalf("30s sync convergence timeout = %s, want 150s", got)
	}
	if got := r1GitHubMeshProfileConvergenceTimeout(90 * time.Second); got != 6*time.Minute {
		t.Fatalf("90s sync convergence timeout = %s, want capped 6m", got)
	}
	if got := r1GitHubMeshProfileConvergenceTimeout(0); got != 120*time.Second {
		t.Fatalf("default convergence timeout = %s, want 120s", got)
	}
}

func TestR1GitHubMeshIntegrationAgentSkipsBusyEntrypoint(t *testing.T) {
	agents := []r1CodexSyncAgent{
		{r1CodexAgent: r1CodexAgent{principal: "codex-01@project", server: codexapp.New("codex", t.TempDir()), threadID: "thread-1"}, localCancel: func() {}},
		{r1CodexAgent: r1CodexAgent{principal: "codex-02@project", server: codexapp.New("codex", t.TempDir()), threadID: "thread-2"}, localCancel: func() {}},
		{r1CodexAgent: r1CodexAgent{principal: "codex-03@project", server: codexapp.New("codex", t.TempDir()), threadID: "thread-3"}, localCancel: func() {}},
	}
	entries := []r1GitHubMeshScenarioEntry{{index: 0}}
	if got := r1GitHubMeshIntegrationAgentIndex(agents, entries, nil); got != 0 {
		t.Fatalf("integration agent = %d, want entrypoint when idle", got)
	}
	if got := r1GitHubMeshIntegrationAgentIndex(agents, entries, map[int]bool{0: true}); got != 1 {
		t.Fatalf("integration agent = %d, want first idle teammate", got)
	}
	if got := r1GitHubMeshIntegrationAgentIndex(agents, entries, map[int]bool{0: true, 1: true, 2: true}); got != -1 {
		t.Fatalf("integration agent = %d, want none when all ready agents are busy", got)
	}
}

func TestR1GitHubMeshKindTotalCountsGovernedEvents(t *testing.T) {
	counts := map[string]map[string]int{
		"codex-01@project": {"assignment": 2, "progress_digest": 1},
		"codex-02@project": {"assignment": 1, "teamwork_signal": 1},
	}
	if got := r1GitHubMeshKindTotal(counts, "assignment"); got != 3 {
		t.Fatalf("assignment total = %d, want 3", got)
	}
	if got := r1GitHubMeshKindTotal(counts, "progress_digest"); got != 1 {
		t.Fatalf("progress total = %d, want 1", got)
	}
	if got := r1GitHubMeshKindTotal(counts, "project_intent"); got != 0 {
		t.Fatalf("missing kind total = %d, want 0", got)
	}
}

func TestR1GitHubMeshTeamEvidenceCountsReady(t *testing.T) {
	counts := map[string]map[string]int{
		"codex-01@project": {"assignment": 2},
		"codex-02@project": {"progress_digest": 1},
		"codex-03@project": {"progress_digest": 1},
	}
	if !r1GitHubMeshTeamEvidenceCountsReady(counts) {
		t.Fatalf("team evidence should be ready for two non-profile participants: %+v", counts)
	}
	counts = map[string]map[string]int{
		"codex-01@project": {"assignment": 2},
		"codex-02@project": {"agent_profile": 1},
	}
	if r1GitHubMeshTeamEvidenceCountsReady(counts) {
		t.Fatalf("team evidence should require progress beyond assignment publication: %+v", counts)
	}
}

func TestR1GitHubMeshAuthoredEventCountsUseLocalSyncEvents(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "workspaces", "codex-01")
	storePath := filepath.Join(workspace, runtime.DefaultStorePath)
	if err := os.MkdirAll(filepath.Dir(storePath), 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", storePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE sync_events(actor TEXT, resource_kind TEXT, status TEXT)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO sync_events(actor, resource_kind, status) VALUES
		('codex-01@project', 'assignment', 'synced'),
		('codex-01@project', 'assignment', 'pending'),
		('codex-02@project', 'progress_digest', 'synced'),
		('', 'progress_digest', 'synced')`); err != nil {
		t.Fatal(err)
	}

	counts, warnings := r1GitHubMeshAuthoredEventCounts([]r1CodexSyncAgent{{
		r1CodexAgent: r1CodexAgent{principal: "codex-01@project", workspace: workspace},
	}})
	if len(warnings) != 0 {
		t.Fatalf("warnings = %v", warnings)
	}
	if got := counts["codex-01@project"]["assignment"]; got != 2 {
		t.Fatalf("codex-01 assignment count = %d, want 2", got)
	}
	if got := counts["codex-02@project"]["progress_digest"]; got != 1 {
		t.Fatalf("codex-02 progress count = %d, want 1", got)
	}
	if _, ok := counts[""]; ok {
		t.Fatal("blank actors must not be counted")
	}
}
