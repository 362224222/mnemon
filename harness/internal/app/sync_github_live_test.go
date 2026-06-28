package app

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
	githubbackend "github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange/backend/github"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

func TestGitHubLivePublishPullImport(t *testing.T) {
	cfg := liveGitHubTestConfig(t)
	store, err := githubbackend.NewPublicationStore(githubbackend.PublicationStoreConfig{
		Repo:  cfg.repo,
		Token: cfg.token,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("github publication store: %v", err)
	}
	remoteA := liveGitHubRemote(t, store, cfg.repo, cfg.branchA)
	remoteB := liveGitHubRemote(t, store, cfg.repo, cfg.branchB)

	sourcePrincipal := contract.ActorID("codex-github-live-a@project")
	targetPrincipal := contract.ActorID("codex-github-live-b@project")
	source := openMeshServingRuntime(t, filepath.Join(t.TempDir(), "source"), string(sourcePrincipal))
	target := openMeshServingRuntime(t, filepath.Join(t.TempDir(), "target"), string(targetPrincipal))

	runID := "github-live-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	assignmentID := runID + "-assignment"
	assignmentScope := "github-live/publish-pull-import/" + runID
	observeLiveAssignment(t, source, sourcePrincipal, assignmentID, assignmentScope, targetPrincipal)

	if err := syncWorkerPush(source, remoteA, "github-live-publish-a"); err != nil {
		t.Fatalf("source publish branch %s: %v", cfg.branchA, err)
	}
	if pending, err := source.PendingSyncedEvents(); err != nil || len(pending) != 0 {
		t.Fatalf("source publish must drain pending events, pending=%+v err=%v", pending, err)
	}

	if err := syncWorkerPull(target, remoteA, "github-live-subscribe-a", nil); err != nil {
		t.Fatalf("target pull branch %s: %v", cfg.branchA, err)
	}
	assertAssignmentScopeCount(t, target, assignmentScope, 1)
	if err := syncWorkerPull(target, remoteA, "github-live-subscribe-a", nil); err != nil {
		t.Fatalf("target repeat pull branch %s: %v", cfg.branchA, err)
	}
	assertAssignmentScopeCount(t, target, assignmentScope, 1)

	progressSummary := runID + " completed assignment " + assignmentID
	observeLiveProgress(t, target, targetPrincipal, runID+"-progress", progressSummary)
	if err := syncWorkerPush(target, remoteB, "github-live-publish-b"); err != nil {
		t.Fatalf("target publish branch %s: %v", cfg.branchB, err)
	}
	if pending, err := target.PendingSyncedEvents(); err != nil || len(pending) != 0 {
		t.Fatalf("target publish must drain pending events, pending=%+v err=%v", pending, err)
	}

	if err := syncWorkerPull(source, remoteB, "github-live-subscribe-b", nil); err != nil {
		t.Fatalf("source pull branch %s: %v", cfg.branchB, err)
	}
	assertProgressSummaryCount(t, source, progressSummary, 1)
	if err := syncWorkerPull(source, remoteB, "github-live-subscribe-b", nil); err != nil {
		t.Fatalf("source repeat pull branch %s: %v", cfg.branchB, err)
	}
	assertProgressSummaryCount(t, source, progressSummary, 1)
}

type liveGitHubConfig struct {
	repo    string
	branchA string
	branchB string
	token   string
}

func liveGitHubTestConfig(t *testing.T) liveGitHubConfig {
	t.Helper()
	if os.Getenv("MNEMON_GITHUB_LIVE") != "1" {
		t.Skip("set MNEMON_GITHUB_LIVE=1 to run the real GitHub publish/pull/import test")
	}
	token := strings.TrimSpace(os.Getenv("GITHUB_TOKEN"))
	if tokenFile := strings.TrimSpace(os.Getenv("MNEMON_GITHUB_TOKEN_FILE")); tokenFile != "" {
		raw, err := os.ReadFile(tokenFile)
		if err != nil {
			t.Fatalf("read MNEMON_GITHUB_TOKEN_FILE: %v", err)
		}
		token = strings.TrimSpace(string(raw))
	}
	if token == "" {
		t.Skip("GITHUB_TOKEN or MNEMON_GITHUB_TOKEN_FILE is required for live GitHub publish/pull/import test")
	}
	repo := strings.TrimSpace(os.Getenv("MNEMON_GITHUB_REPO"))
	if repo == "" {
		repo = "mnemon-dev/mnemon-teamwork-example"
	}
	branchA := strings.TrimSpace(os.Getenv("MNEMON_GITHUB_BRANCH_A"))
	if branchA == "" {
		branchA = "mnemon/agent-a"
	}
	branchB := strings.TrimSpace(os.Getenv("MNEMON_GITHUB_BRANCH_B"))
	if branchB == "" {
		branchB = "mnemon/agent-b"
	}
	return liveGitHubConfig{repo: repo, branchA: branchA, branchB: branchB, token: token}
}

func liveGitHubRemote(t *testing.T, store exchange.PublicationStore, repo, branch string) exchange.RemoteWorkspace {
	t.Helper()
	remote, err := githubbackend.New(githubbackend.Config{
		Store:  store,
		Repo:   repo,
		Branch: branch,
		Scopes: []contract.ResourceRef{
			{Kind: "assignment", ID: "project"},
			{Kind: "progress_digest", ID: "project"},
		},
	})
	if err != nil {
		t.Fatalf("github remote %s: %v", branch, err)
	}
	return remote
}

func observeLiveAssignment(t *testing.T, rt *runtime.Runtime, principal contract.ActorID, assignmentID, scope string, assignee contract.ActorID) {
	t.Helper()
	if _, _, err := rt.API().Ingest(principal, contract.ObservationEnvelope{
		ExternalID: assignmentID,
		Event: contract.Event{Type: "assignment.write_candidate.observed", Payload: r2AssignmentPayload(
			map[string]any{"assignment_id": assignmentID, "scope": scope, "ttl": "30m", "assignee": string(assignee)},
			map[string]any{"expected_work": "complete live GitHub Remote Workspace publish/pull/import validation", "expected_feedback": "progress_digest with result evidence"},
			map[string]any{"evidence_refs": []any{"gated live GitHub publication backend test"}},
		)},
	}); err != nil {
		t.Fatalf("observe live assignment: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick live assignment: %v", err)
	}
}

func observeLiveProgress(t *testing.T, rt *runtime.Runtime, principal contract.ActorID, externalID, summary string) {
	t.Helper()
	if _, _, err := rt.API().Ingest(principal, contract.ObservationEnvelope{
		ExternalID: externalID,
		Event:      contract.Event{Type: "progress_digest.write_candidate.observed", Payload: r2Progress(summary)},
	}); err != nil {
		t.Fatalf("observe live progress: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick live progress: %v", err)
	}
}

func assertAssignmentScopeCount(t *testing.T, rt *runtime.Runtime, scope string, want int) {
	t.Helper()
	_, fields, err := rt.Resource(contract.ResourceRef{Kind: "assignment", ID: "project"})
	if err != nil {
		t.Fatalf("read assignment: %v", err)
	}
	content, _ := fields["content"].(string)
	if got := strings.Count(content, scope); got != want {
		t.Fatalf("assignment scope %q count = %d, want %d\n%s", scope, got, want, content)
	}
}

func assertProgressSummaryCount(t *testing.T, rt *runtime.Runtime, summary string, want int) {
	t.Helper()
	_, fields, err := rt.Resource(contract.ResourceRef{Kind: "progress_digest", ID: "project"})
	if err != nil {
		t.Fatalf("read progress_digest: %v", err)
	}
	content, _ := fields["content"].(string)
	if got := strings.Count(content, summary); got != want {
		t.Fatalf("progress summary %q count = %d, want %d\n%s", summary, got, want, content)
	}
}
