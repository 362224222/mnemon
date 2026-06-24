package app

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/hostagent"
)

func TestSetupWiresChannelAndStaticShim(t *testing.T) {
	root := t.TempDir()
	h := New(root)
	var out, errw bytes.Buffer
	opts := SetupOptions{
		Host: "codex", Loops: []string{"memory"}, ControlURL: "http://127.0.0.1:8787",
		Principal: "codex@project", UseToken: true,
	}
	if _, err := h.Setup(context.Background(), &out, &errw, opts); err != nil {
		t.Fatalf("setup: %v\nstderr=%s", err, errw.String())
	}
	assertPublicSetupOutput(t, out.String())

	primeHook := string(mustRead(t, filepath.Join(root, ".codex", "hooks", "mnemon-r1", "prime.sh")))
	if !strings.Contains(primeHook, "control render") || strings.Contains(primeHook, "MEMORY.md") || strings.Contains(primeHook, "GUIDE.md") {
		t.Fatalf("standard hook must be render-only:\n%s", primeHook)
	}
	hooksJSON := string(mustRead(t, filepath.Join(root, ".codex", "hooks.json")))
	if !strings.Contains(hooksJSON, "mnemon-r1") {
		t.Fatalf("hooks.json must register standard shim:\n%s", hooksJSON)
	}
	if _, err := os.Stat(filepath.Join(root, ".codex", "skills", "memory-get", "SKILL.md")); !os.IsNotExist(err) {
		t.Fatalf("setup must not project legacy per-loop skills; err=%v", err)
	}

	bindingFile := filepath.Join(root, ".mnemon", "harness", "channel", "bindings.json")
	status, err := h.SetupStatus("", "codex@project")
	if err != nil {
		t.Fatalf("setup status: %v", err)
	}
	assertPublicStatusLines(t, status)
	bf := string(mustRead(t, bindingFile))
	if !strings.Contains(bf, "codex@project") || !strings.Contains(bf, "127.0.0.1:8787") {
		t.Fatalf("bindings.json must record the principal + endpoint:\n%s", bf)
	}
	tokenFile := filepath.Join(root, ".mnemon", "harness", "channel", "credentials", "codex-project.token")
	if fi, err := os.Stat(tokenFile); err != nil || fi.Size() == 0 {
		t.Fatalf("token file must exist + be non-empty: %v", err)
	}
	env := string(mustRead(t, filepath.Join(root, ".mnemon", "harness", "channel", "env.sh")))
	for _, want := range []string{"MNEMON_HARNESS_BIN", "MNEMON_CONTROL_ADDR", "MNEMON_CONTROL_PRINCIPAL", "MNEMON_CONTROL_TOKEN_FILE", "MNEMON_MEMORY_LOOP_DIR"} {
		if !strings.Contains(env, want) {
			t.Fatalf("channel env must export %s; got:\n%s", want, env)
		}
	}

	if _, err := h.Setup(context.Background(), &out, &errw, opts); err != nil {
		t.Fatalf("reinstall: %v", err)
	}
	if n := strings.Count(string(mustRead(t, bindingFile)), `"codex@project"`); n != 1 {
		t.Fatalf("reinstall must not duplicate the binding; got %d codex entries", n)
	}

	userOpts := SetupOptions{Host: "codex", Loops: []string{"memory"}, ControlURL: "http://127.0.0.1:8787", Principal: "human@project"}
	if _, err := h.Setup(context.Background(), &out, &errw, userOpts); err != nil {
		t.Fatalf("user setup: %v", err)
	}
	if err := h.SetupUninstall(context.Background(), &out, &errw, opts); err != nil {
		t.Fatalf("uninstall codex: %v", err)
	}
	after := string(mustRead(t, bindingFile))
	if strings.Contains(after, "codex@project") {
		t.Fatalf("uninstall must remove the managed binding; still present:\n%s", after)
	}
	if !strings.Contains(after, "human@project") {
		t.Fatalf("uninstall must preserve the user-added binding; gone:\n%s", after)
	}
	if _, err := os.Stat(tokenFile); !os.IsNotExist(err) {
		t.Fatalf("uninstall must remove the managed token file; err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".codex", "hooks", "mnemon-r1")); err != nil {
		t.Fatalf("standard shim must remain while a sibling binding exists: %v", err)
	}
	if err := h.SetupUninstall(context.Background(), &out, &errw, userOpts); err != nil {
		t.Fatalf("uninstall human: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".codex", "hooks", "mnemon-r1")); !os.IsNotExist(err) {
		t.Fatalf("last binding uninstall must remove standard shim; err=%v", err)
	}
}

func TestSetupInstallsStaticShimWithoutLoop(t *testing.T) {
	root := t.TempDir()
	var out, errw bytes.Buffer
	res, err := New(root).Setup(context.Background(), &out, &errw, SetupOptions{
		Host: "codex", ControlURL: "http://127.0.0.1:8787", Principal: "codex@project", UseToken: true,
	})
	if err != nil {
		t.Fatalf("setup static shim: %v\nstderr=%s", err, errw.String())
	}
	assertPublicSetupOutput(t, out.String())
	if !strings.Contains(string(mustRead(t, filepath.Join(root, ".codex", "hooks", "mnemon-r1", "prime.sh"))), "control render") {
		t.Fatal("setup without --loop must still install the static render hook")
	}
	configJSON := string(mustRead(t, res.ConfigFile))
	if strings.Contains(configJSON, `"hosts"`) || strings.Contains(configJSON, `"mirror_mode"`) {
		t.Fatalf("static setup config must not record projection state:\n%s", configJSON)
	}
}

func TestSetupDryRunWritesNothing(t *testing.T) {
	root := t.TempDir()
	var out, errw bytes.Buffer
	_, err := New(root).Setup(context.Background(), &out, &errw, SetupOptions{
		Host: "codex", Loops: []string{"memory"}, ControlURL: "http://127.0.0.1:8787",
		Principal: "codex@project", UseToken: true, DryRun: true,
	})
	if err != nil {
		t.Fatalf("dry-run setup: %v\nstderr=%s", err, errw.String())
	}
	if !strings.Contains(out.String(), "dry-run") {
		t.Fatalf("dry-run must announce changes; got:\n%s", out.String())
	}
	assertPublicSetupOutput(t, out.String())
	for _, path := range []string{
		filepath.Join(root, ".mnemon", "harness", "channel", "bindings.json"),
		filepath.Join(root, ".codex", "hooks", "mnemon-r1", "prime.sh"),
	} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("dry-run must not write %s; err=%v", path, err)
		}
	}
}

func TestSetupRejectsUnsupportedProductLoop(t *testing.T) {
	root := t.TempDir()
	var out, errw bytes.Buffer
	_, err := New(root).Setup(context.Background(), &out, &errw, SetupOptions{
		Host: "codex", Loops: []string{"eval"}, ControlURL: "http://127.0.0.1:8787",
		Principal: "codex@project",
	})
	if err == nil || !strings.Contains(err.Error(), `unsupported product loop "eval"`) {
		t.Fatalf("expected unsupported product loop error, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, ".mnemon", "harness", "channel", "bindings.json")); !os.IsNotExist(err) {
		t.Fatalf("unsupported loop setup must not write channel bindings; err=%v", err)
	}
	if out.Len() != 0 || errw.Len() != 0 {
		t.Fatalf("unsupported loop setup should fail before output; stdout=%q stderr=%q", out.String(), errw.String())
	}
}

func TestAgentIntegrationHooksDoNotReferenceRemoteWorkspace(t *testing.T) {
	for _, host := range []string{"codex", "claude-code"} {
		for _, timing := range []string{"prime", "remind", "nudge", "compact"} {
			content, err := hostagent.RenderStandardThinHook(host, timing)
			if err != nil {
				t.Fatalf("render %s/%s: %v", host, timing, err)
			}
			assertContentHasNoRemoteWorkspace(t, host+"/"+timing, content)
		}
	}
}

func assertContentHasNoRemoteWorkspace(t *testing.T, label, content string) {
	t.Helper()
	blocked := []string{"remote workspace", "remote token", "remote credential", "mnemon_remote", "remote_workspace", "https://"}
	lower := strings.ToLower(content)
	for _, term := range blocked {
		if strings.Contains(lower, term) {
			t.Fatalf("generated hook %s leaked %q", label, term)
		}
	}
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}

func assertPublicSetupOutput(t *testing.T, output string) {
	t.Helper()
	for _, want := range []string{"Agent Integration:", "Local Mnemon:", "Remote Workspace:"} {
		if !strings.Contains(output, want) {
			t.Fatalf("setup output must include %q:\n%s", want, output)
		}
	}
	for _, blocked := range []string{"channel", "binding", "runtime", "kernel", "cursor", "outbox", "projection"} {
		if strings.Contains(strings.ToLower(output), blocked) {
			t.Fatalf("setup output leaked internal term %q:\n%s", blocked, output)
		}
	}
}

func assertPublicStatusLines(t *testing.T, lines []string) {
	t.Helper()
	joined := strings.Join(lines, "\n")
	for _, want := range []string{"Agent Integration:", "Local Mnemon:", "Remote Workspace:"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("setup status must include %q:\n%s", want, joined)
		}
	}
	for _, blocked := range []string{"channel", "binding", "runtime", "kernel", "cursor", "outbox", "projection"} {
		if strings.Contains(strings.ToLower(joined), blocked) {
			t.Fatalf("setup status leaked internal term %q:\n%s", blocked, joined)
		}
	}
}
