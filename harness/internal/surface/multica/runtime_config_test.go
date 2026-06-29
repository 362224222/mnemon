package multica

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRuntimeEnvValueUsesLastValue(t *testing.T) {
	env := []string{
		"MNEMON_MANAGED_RUNTIME=codex-appserver",
		"MNEMON_MANAGED_RUNTIME=off",
	}
	if got := RuntimeEnvValue(env, "MNEMON_MANAGED_RUNTIME"); got != "off" {
		t.Fatalf("RuntimeEnvValue = %q, want off", got)
	}
	if got := RuntimeEnvDefault(env, "MISSING", "fallback"); got != "fallback" {
		t.Fatalf("RuntimeEnvDefault = %q, want fallback", got)
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

func TestRuntimeAdapterSwitches(t *testing.T) {
	if !RuntimeProjectionCommentsEnabled(nil) || !RuntimeHubWriteEnabled(nil) {
		t.Fatal("runtime switches should default on")
	}
	if RuntimeProjectionCommentsEnabled([]string{"MNEMON_MULTICA_PROJECT_COMMENTS=off"}) {
		t.Fatal("projection comments should honor off")
	}
	if RuntimeHubWriteEnabled([]string{"MNEMON_MULTICA_HUB_WRITE=disabled"}) {
		t.Fatal("hub write should honor disabled")
	}
}

func TestRuntimeManagedLedgerPath(t *testing.T) {
	tmp := t.TempDir()
	workspace := filepath.Join(tmp, "managed-workspace")
	want := filepath.Join(workspace, ".mnemon", "harness", "local", "managed-agent", "wake-ledger.jsonl")
	if got := RuntimeManagedLedgerPath(nil, workspace); got != want {
		t.Fatalf("RuntimeManagedLedgerPath = %q, want %q", got, want)
	}
	explicit := filepath.Join(tmp, "explicit.jsonl")
	if got := RuntimeManagedLedgerPath([]string{"MNEMON_MANAGED_LEDGER=" + explicit}, workspace); got != explicit {
		t.Fatalf("explicit RuntimeManagedLedgerPath = %q, want %q", got, explicit)
	}
}
