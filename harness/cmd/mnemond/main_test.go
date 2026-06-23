package main

import (
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
		Host:  "codex",
		Loops: []string{"memory"},
	}); err != nil {
		t.Fatalf("setup: %v", err)
	}
	err := run(context.Background(), []string{"--root", root, "--addr", "0.0.0.0:0"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("non-loopback --addr must be refused (T1), got: %v", err)
	}
}
