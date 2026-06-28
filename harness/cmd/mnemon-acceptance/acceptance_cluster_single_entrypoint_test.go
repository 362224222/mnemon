package main

import (
	"strings"
	"testing"
)

func TestChooseR1ClusterEntrypoint(t *testing.T) {
	agents := []r1CodexSyncAgent{
		{r1CodexAgent: r1CodexAgent{principal: "codex-01@project"}},
		{r1CodexAgent: r1CodexAgent{principal: "codex-02@project"}},
		{r1CodexAgent: r1CodexAgent{principal: "codex-03@project"}},
	}
	idx, err := chooseR1ClusterEntrypoint(agents, "codex-02@project", 1)
	if err != nil {
		t.Fatalf("explicit entrypoint: %v", err)
	}
	if idx != 1 {
		t.Fatalf("explicit entrypoint index = %d, want 1", idx)
	}
	first, err := chooseR1ClusterEntrypoint(agents, "", 42)
	if err != nil {
		t.Fatalf("seeded entrypoint first: %v", err)
	}
	second, err := chooseR1ClusterEntrypoint(agents, "", 42)
	if err != nil {
		t.Fatalf("seeded entrypoint second: %v", err)
	}
	if first != second {
		t.Fatalf("seeded entrypoint must be deterministic: %d vs %d", first, second)
	}
	if _, err := chooseR1ClusterEntrypoint(agents, "missing@project", 1); err == nil {
		t.Fatal("missing explicit entrypoint must fail")
	}
}

func TestR1ClusterRunnerContractPrompts(t *testing.T) {
	contract := &r1RunnerContractReport{}
	recordR1ClusterPrompt(contract, "codex-01@project", "business_task", "do cluster work")
	recordR1ClusterPrompt(contract, "codex-02@project", "worker_wake", r1ClusterWorkerWakePrompt)
	contract.BusinessTaskPrompts = 1
	contract.WorkerWakePrompts = 1
	if !r1ClusterBusinessBeforeWake(contract) {
		t.Fatal("business prompt must be recorded before worker wakes")
	}
	if !r1ClusterWorkerPromptsGeneric(contract) {
		t.Fatal("worker wake prompt must match the generic contract")
	}
	recordR1ClusterPrompt(contract, "codex-03@project", "worker_wake", "inspect assignment a1")
	if r1ClusterWorkerPromptsGeneric(contract) {
		t.Fatal("business-shaped worker prompt must violate the generic wake contract")
	}
}

func TestR1ClusterWokeAllNonEntrypoints(t *testing.T) {
	agents := []r1CodexAgentReport{
		{Principal: "codex-01@project"},
		{Principal: "codex-02@project"},
		{Principal: "codex-03@project"},
	}
	contract := &r1RunnerContractReport{}
	recordR1ClusterPrompt(contract, "codex-02@project", "worker_wake", r1ClusterWorkerWakePrompt)
	if r1ClusterWokeAllNonEntrypoints(contract, agents, "codex-01@project") {
		t.Fatal("partial worker wake coverage must not pass")
	}
	recordR1ClusterPrompt(contract, "codex-03@project", "worker_wake", r1ClusterWorkerWakePrompt)
	if !r1ClusterWokeAllNonEntrypoints(contract, agents, "codex-01@project") {
		t.Fatal("all non-entrypoint agents should be covered by generic wakes")
	}
}

func TestR1ClusterActorEventCountsAndProgressReady(t *testing.T) {
	obs := acceptanceObserveReport{CrossEvents: []acceptanceCrossEvent{
		{Actor: "codex-01@project", EventSubject: "agent_profile/project@1", Status: "accepted"},
		{Actor: "codex-02@project", EventSubject: "agent_profile/project@2", Status: "accepted"},
		{Actor: "codex-03@project", EventSubject: "agent_profile/project@3", Status: "accepted"},
		{Actor: "codex-01@project", EventSubject: "project_intent/project@1", Status: "accepted"},
		{Actor: "codex-01@project", EventSubject: "teamwork_signal/project@1", Status: "accepted"},
		{Actor: "codex-01@project", EventSubject: "assignment/project@1", Status: "accepted"},
		{Actor: "codex-01@project", EventSubject: "assignment/project@2", Status: "accepted"},
		{Actor: "codex-02@project", EventSubject: "progress_digest/project@1", Status: "accepted"},
		{Actor: "codex-03@project", EventSubject: "progress_digest/project@2", Status: "accepted"},
	}}
	counts := r1ClusterActorEventCounts(obs)
	if !r1ClusterProgressReady(counts, "codex-01@project") {
		t.Fatalf("cluster should be progress-ready: %+v", counts)
	}
	participants := r1ClusterParticipants(counts, "codex-01@project")
	if got := len(participants); got != 3 {
		t.Fatalf("participants = %d, want 3: %+v", got, participants)
	}
	workers := r1ClusterWorkerProgressActors(counts, "codex-01@project")
	if len(workers) != 2 || workers[0] != "codex-02@project" || workers[1] != "codex-03@project" {
		t.Fatalf("worker progress actors wrong: %+v", workers)
	}
}

func TestR1ClusterFindingNoDefectClassification(t *testing.T) {
	finding := r1ClusterFindingFromAnswer("No concrete defect found; no code change is justified.", map[string]int{"progress_digest": 2})
	if finding.Kind != "no-defect" {
		t.Fatalf("finding kind = %q, want no-defect", finding.Kind)
	}
	if !finding.Resolved {
		t.Fatal("no-code-change finding should be treated as resolved")
	}
}

func TestR1ClusterFindingAppliedFixResolved(t *testing.T) {
	finding := r1ClusterFindingFromAnswer("Found an issue and applied the reviewed minimal fix. Validation passed.", map[string]int{"progress_digest": 4})
	if finding.Kind != "issue" {
		t.Fatalf("finding kind = %q, want issue", finding.Kind)
	}
	if !finding.Resolved {
		t.Fatal("applied fix should be treated as resolved")
	}
}

func TestR1ClusterAcceptedEventCount(t *testing.T) {
	obs := acceptanceObserveReport{CrossEvents: []acceptanceCrossEvent{
		{Status: "accepted"},
		{Status: "rejected"},
		{Status: "accepted"},
	}}
	if got := r1ClusterAcceptedEventCount(obs); got != 2 {
		t.Fatalf("accepted event count = %d, want 2", got)
	}
}

func TestR1ClusterAcceptanceEnvPinsGitCeiling(t *testing.T) {
	runRoot := t.TempDir()
	env := acceptanceEnv("/tmp/mnemon-bin", "/tmp/codex-home", runRoot)
	if got := testEnvValue(env, "GIT_CEILING_DIRECTORIES"); got != runRoot {
		t.Fatalf("GIT_CEILING_DIRECTORIES = %q, want %q", got, runRoot)
	}
}

func testEnvValue(env []string, key string) string {
	prefix := key + "="
	for _, item := range env {
		if strings.HasPrefix(item, prefix) {
			return strings.TrimPrefix(item, prefix)
		}
	}
	return ""
}
