package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
)

func TestSetupHookGateExecutesActualProjectionWithoutInheritedAttachment(t *testing.T) {
	for _, output := range []string{"", "printf '%s\\n' '" + WakeCue + "'"} {
		t.Run(output, func(t *testing.T) {
			workspace, hook := setupHookFixture(t, assets.HostCodex, "exit 0\n")
			t.Setenv(localapi.RunAttachmentEnv, filepath.Join(workspace, "inherited.attach"))
			script := "#!/bin/sh\nset -eu\ntest \"${PWD}\" = \"" + workspace + "\"\n" +
				"test -z \"${" + localapi.RunAttachmentEnv + "-}\"\n" + output + "\n"
			if err := os.WriteFile(hook, []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			gate, err := newSetupHookGate(workspace, assets.HostCodex)
			if err != nil {
				t.Fatal(err)
			}
			if err := gate.VerifyReady(context.Background(), localapi.HealthResponse{}); err != nil {
				t.Fatalf("VerifyReady() error = %v", err)
			}
		})
	}
}

func TestSetupHookGateFailsClosedForOutputFailureTimeoutAndPathDrift(t *testing.T) {
	tests := []struct {
		name   string
		script string
		short  bool
	}{
		{name: "unexpected output", script: "printf 'Event body\\n'"},
		{name: "stderr", script: "printf 'diagnostic\\n' >&2"},
		{name: "overflow", script: "head -c 1024 /dev/zero"},
		{name: "exit", script: "exit 7"},
		{name: "timeout", script: "while :; do :; done", short: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workspace, _ := setupHookFixture(t, assets.HostClaudeCode, test.script+"\n")
			gate, err := newSetupHookGate(workspace, assets.HostClaudeCode)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if test.short {
				bounded, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
				defer cancel()
				ctx = bounded
			}
			if err := gate.VerifyReady(ctx, localapi.HealthResponse{}); !errors.Is(err, errSetupHookGate) {
				t.Fatalf("VerifyReady() error = %v", err)
			}
		})
	}

	workspace, hook := setupHookFixture(t, assets.HostCodex, "exit 0\n")
	gate, err := newSetupHookGate(workspace, assets.HostCodex)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(hook); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(workspace, "outside"), hook); err != nil {
		t.Fatal(err)
	}
	if err := gate.VerifyReady(context.Background(), localapi.HealthResponse{}); !errors.Is(err, errSetupHookGate) {
		t.Fatalf("drift VerifyReady() error = %v", err)
	}
}

func TestSetupHookGateRetriesTransientSelfCheckFailures(t *testing.T) {
	tests := []struct {
		name   string
		stderr string
	}{
		{name: "asset revision mismatch",
			stderr: "asset_revision_mismatch: transient projection observation"},
		{name: "runtime unavailable",
			stderr: "mnemond_unavailable: managed Runtime worker is not ready"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workspace, hook := setupHookFixture(t, assets.HostCodex, "exit 0\n")
			marker := filepath.Join(workspace, "retry.marker")
			t.Setenv("MNEMON_TEST_SETUP_GATE_RETRY_MARKER", marker)
			script := `#!/bin/sh
set -eu
marker=${MNEMON_TEST_SETUP_GATE_RETRY_MARKER:?}
if [ ! -f "$marker" ]; then
	printf '%s\n' first >"$marker"
	printf '%s\n' 'mnemon-harness hook check failed; managed Agent execution is blocked' >&2
	printf '%s\n' '` + test.stderr + `' >&2
	exit 2
fi
printf '%s\n' second >>"$marker"
exit 0
`
			if err := os.WriteFile(hook, []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			gate, err := newSetupHookGate(workspace, assets.HostCodex)
			if err != nil {
				t.Fatal(err)
			}
			if err := gate.VerifyReady(context.Background(), localapi.HealthResponse{}); err != nil {
				t.Fatalf("VerifyReady() error = %v", err)
			}
			raw, err := os.ReadFile(marker)
			if err != nil {
				t.Fatal(err)
			}
			if string(raw) != "first\nsecond\n" {
				t.Fatalf("retry marker = %q", raw)
			}
		})
	}
}

func TestSetupHookGateRejectsUnknownHostAndUnsafeHook(t *testing.T) {
	workspace := setupGateWorkspace(t)
	if gate, err := newSetupHookGate(workspace, "other"); gate != nil || !errors.Is(err, errSetupHookGate) {
		t.Fatalf("unknown Host gate = (%#v, %v)", gate, err)
	}
	workspace, hook := setupHookFixture(t, assets.HostCodex, "exit 0\n")
	if err := os.Chmod(hook, 0o777); err != nil {
		t.Fatal(err)
	}
	if gate, err := newSetupHookGate(workspace, assets.HostCodex); gate != nil || !errors.Is(err, errSetupHookGate) {
		t.Fatalf("unsafe Hook gate = (%#v, %v)", gate, err)
	}
}

func setupHookFixture(t *testing.T, host assets.Host, body string) (string, string) {
	t.Helper()
	workspace := setupGateWorkspace(t)
	hostRoot := map[assets.Host]string{assets.HostCodex: ".codex", assets.HostClaudeCode: ".claude"}[host]
	hook := filepath.Join(workspace, hostRoot, "hooks", "mnemon-harness", "hook.sh")
	if err := os.MkdirAll(filepath.Dir(hook), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(hook, []byte("#!/bin/sh\nset -eu\n"+body), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(hook, 0o755); err != nil {
		t.Fatal(err)
	}
	return workspace, hook
}

func setupGateWorkspace(t *testing.T) string {
	t.Helper()
	workspace, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return workspace
}
