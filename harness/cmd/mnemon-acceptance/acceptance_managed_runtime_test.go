package main

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/driver"
)

func TestManagedRuntimeAcceptanceCommandRegistered(t *testing.T) {
	commands := map[string]bool{}
	for _, cmd := range rootCmd.Commands() {
		commands[cmd.Name()] = true
	}
	if !commands["managed-runtime"] {
		t.Fatalf("mnemon-acceptance should expose managed-runtime command: %v", commands)
	}
}

func TestManagedRuntimeAcceptanceRequiresSentinelWake(t *testing.T) {
	report, err := runManagedRuntimeAcceptance(context.Background(), managedRuntimeAcceptanceOptions{
		RunRoot:  t.TempDir(),
		Agents:   3,
		Exchange: "mnemonhub",
		Wake: func(_ context.Context, _ managedRuntimeAcceptanceOptions, _ string) (string, error) {
			return driver.ManagedWakeQuery, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != "ok" || report.Layer != "managed_runtime_acceptance" || report.RunnerRole != "seed_and_observe" {
		t.Fatalf("report contract mismatch: %+v", report)
	}
	if len(report.Agents) != 3 {
		t.Fatalf("agents = %d, want 3", len(report.Agents))
	}
	for _, agent := range report.Agents {
		if agent.RawQuery != driver.ManagedWakeQuery {
			t.Fatalf("raw query = %q, want %q", agent.RawQuery, driver.ManagedWakeQuery)
		}
	}
	if managedRuntimeDirectWorkerPromptCount(report) != 0 {
		t.Fatalf("managed acceptance must not record worker business prompts: %+v", report.PromptAudit)
	}
}

func TestManagedRuntimeAcceptanceRejectsNonSentinelWake(t *testing.T) {
	report, err := runManagedRuntimeAcceptance(context.Background(), managedRuntimeAcceptanceOptions{
		RunRoot:  t.TempDir(),
		Agents:   1,
		Exchange: "mnemonhub",
		Wake: func(_ context.Context, _ managedRuntimeAcceptanceOptions, _ string) (string, error) {
			return "inspect assignment asg1", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Status == "ok" {
		t.Fatalf("non-sentinel wake should fail managed acceptance: %+v", report)
	}
	if !strings.Contains(report.Agents[0].RawQuery, "assignment") {
		t.Fatalf("test setup did not preserve non-sentinel query: %+v", report.Agents)
	}
}

func TestManagedRuntimeGitHubRequiresTokenFile(t *testing.T) {
	report, err := runManagedRuntimeAcceptance(context.Background(), managedRuntimeAcceptanceOptions{
		RunRoot:  t.TempDir(),
		Agents:   1,
		Exchange: "github",
		Wake: func(_ context.Context, _ managedRuntimeAcceptanceOptions, _ string) (string, error) {
			return driver.ManagedWakeQuery, nil
		},
	})
	if err == nil {
		t.Fatal("github managed acceptance without token file should return an explicit blocker")
	}
	if report.Status != "blocked" || !strings.Contains(err.Error(), "--github-token-file") {
		t.Fatalf("github blocker mismatch: status=%s err=%v report=%+v", report.Status, err, report)
	}
}

func TestManagedRuntimeAcceptanceDoesNotInjectDeveloperInstructions(t *testing.T) {
	raw, err := os.ReadFile("acceptance_managed_runtime.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "developerInstructions") {
		t.Fatal("managed-runtime acceptance must rely on standard GUIDE/hook/render flow, not acceptance-specific developer instructions")
	}
}
