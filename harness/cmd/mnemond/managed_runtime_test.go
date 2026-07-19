package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestManagedWakeEnvironmentIsClosedAndDeterministic(t *testing.T) {
	input := []string{
		"PATH=/managed/bin", "HOME=/home/agent", "CODEX_HOME=/home/agent/.codex",
		"XDG_CACHE_HOME=/home/agent/.cache", "LC_ALL=C.UTF-8", "LANG=en_US.UTF-8",
		"PATH=/untrusted/duplicate", "OPENAI_API_KEY=private", "MNEMON_EVENT_BODY=private",
		"MNEMON_HARNESS_RUN_ATTACHMENT=/private/run.attach",
		"MNEMON_HARNESS_INTERNAL_MNEMOND_ENSURE_FD=99", "MALFORMED",
	}
	want := []string{
		"PATH=/managed/bin", "HOME=/home/agent", "CODEX_HOME=/home/agent/.codex",
		"XDG_CACHE_HOME=/home/agent/.cache", "LC_ALL=C.UTF-8", "LANG=en_US.UTF-8",
	}
	got := managedWakeEnvironment(input)
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("managedWakeEnvironment() = %q, want %q", got, want)
	}
	joined := strings.Join(got, "\n")
	for _, forbidden := range []string{"OPENAI_API_KEY", "MNEMON_EVENT_BODY",
		"MNEMON_HARNESS_RUN_ATTACHMENT", "MNEMON_HARNESS_INTERNAL_MNEMOND_ENSURE_FD"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("managed wake environment retained %s", forbidden)
		}
	}
}

func TestManagedInstallationVerifierRejectsCancelledContext(t *testing.T) {
	verify, err := managedInstallationVerifier(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := verify.Verify(ctx, model.Profile{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Verify(cancelled) error = %v", err)
	}
}
