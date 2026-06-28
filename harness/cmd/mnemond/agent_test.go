package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/driver"
)

func TestAgentRunDryRunPrintsSentinelOnly(t *testing.T) {
	var out, errw bytes.Buffer
	err := run(context.Background(), []string{"agent", "run", "--principal", "codex-a@project", "--dry-run"}, &out, &errw)
	if err != nil {
		t.Fatalf("agent run dry-run: %v\nstderr=%s", err, errw.String())
	}
	if got := strings.TrimSpace(out.String()); got != driver.ManagedWakeQuery {
		t.Fatalf("dry-run output = %q, want %q", got, driver.ManagedWakeQuery)
	}
}

func TestAgentRunRequiresPrincipal(t *testing.T) {
	var out, errw bytes.Buffer
	err := run(context.Background(), []string{"agent", "run", "--dry-run"}, &out, &errw)
	if err == nil || !strings.Contains(err.Error(), "--principal") {
		t.Fatalf("missing principal should fail clearly, got err=%v out=%q stderr=%q", err, out.String(), errw.String())
	}
}

func TestAgentRunNoopRecordsSentinelQuery(t *testing.T) {
	var out, errw bytes.Buffer
	err := run(context.Background(), []string{"agent", "run", "--principal", "codex-a@project", "--runtime", "noop"}, &out, &errw)
	if err != nil {
		t.Fatalf("agent run noop: %v\nstderr=%s", err, errw.String())
	}
	if !strings.Contains(out.String(), `"query": "`+driver.ManagedWakeQuery+`"`) {
		t.Fatalf("noop run should report sentinel query only:\n%s", out.String())
	}
}
