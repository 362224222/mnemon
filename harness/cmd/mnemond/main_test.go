package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
)

// Boot smoke: without setup artifacts the daemon refuses with the SAME product remediation
// `mnemon-harness local run` gives (shared app.ResolveLocalBoot — alias, not fork).
func TestRunWithoutSetupReportsNotSetUp(t *testing.T) {
	err := run(context.Background(), []string{"--root", t.TempDir()}, io.Discard, io.Discard)
	if err == nil {
		t.Fatal("daemon boot without setup must fail")
	}
	for _, want := range []string{
		"Local Mnemon is not set up.",
		"mnemon-harness setup --host codex",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("missing remediation %q in error:\n%v", want, err)
		}
	}
}

// T1 floor: an explicit non-loopback --addr is refused without --allow-nonloopback — the same
// loopback-only gate as `local run` (app.ValidateListenAddr), checked after a real setup so the
// boot chain itself resolves.
func TestRunRefusesNonLoopbackAddr(t *testing.T) {
	root := t.TempDir()
	if _, err := app.New(root).Setup(context.Background(), io.Discard, io.Discard, app.SetupOptions{
		Host: "codex",
	}); err != nil {
		t.Fatalf("setup: %v", err)
	}
	err := run(context.Background(), []string{"--root", root, "--addr", "0.0.0.0:0"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("non-loopback --addr must be refused (T1), got: %v", err)
	}
}

func TestHelpDescribesLocalEventNode(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"help"}} {
		var errw bytes.Buffer
		err := run(context.Background(), args, io.Discard, &errw)
		if err != nil {
			t.Fatalf("%v help should exit successfully, got %v", args, err)
		}
		got := errw.String()
		for _, want := range []string{"Local Mnemon event node", "local event API", "admission", "state", "presentation", "drive candidates", "Commands:", "serve", "status", "agent run"} {
			if !strings.Contains(got, want) {
				t.Fatalf("%v mnemond help missing %q:\n%s", args, want, got)
			}
		}
	}
}

func TestAgentHelpListsLocalDriveSourceCommand(t *testing.T) {
	var errw bytes.Buffer
	err := run(context.Background(), []string{"agent", "--help"}, io.Discard, &errw)
	if err != nil {
		t.Fatalf("agent help should exit successfully, got %v", err)
	}
	got := errw.String()
	for _, want := range []string{"local managed-agent drive sources", "mnemond agent run", "[mnemon:wake]", "managed runtime"} {
		if !strings.Contains(got, want) {
			t.Fatalf("agent help missing %q:\n%s", want, got)
		}
	}
}

func TestAgentRunHelpFramesLocalDriveSource(t *testing.T) {
	var errw bytes.Buffer
	err := run(context.Background(), []string{"agent", "run", "--help"}, io.Discard, &errw)
	if err != nil {
		t.Fatalf("agent run help should exit successfully, got %v", err)
	}
	got := errw.String()
	for _, want := range []string{"local drive source", "[mnemon:wake]", "managed runtime"} {
		if !strings.Contains(got, want) {
			t.Fatalf("agent run help missing %q:\n%s", want, got)
		}
	}
}
