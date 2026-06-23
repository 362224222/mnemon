package app

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/hostsurface"
)

func writeMemoryFixture(t *testing.T, root string) {
	t.Helper()
	loopDir := filepath.Join(root, "harness", "loops", "memory")
	hostDir := filepath.Join(root, "harness", "hosts", "codex")
	bindingDir := filepath.Join(root, "harness", "bindings")
	for _, dir := range []string{
		filepath.Join(loopDir, "skills", "memory-get"),
		filepath.Join(hostDir, "memory", "hooks"),
		bindingDir,
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	write := func(p, c string) {
		if err := os.WriteFile(p, []byte(c), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []string{
		filepath.Join(loopDir, "GUIDE.md"), filepath.Join(loopDir, "env.sh"), filepath.Join(loopDir, "MEMORY.md"),
		filepath.Join(loopDir, "skills", "memory-get", "SKILL.md"),
	} {
		write(p, "fixture\n")
	}
	for _, name := range []string{"prime.sh", "remind.sh", "nudge.sh", "compact.sh"} {
		write(filepath.Join(hostDir, "memory", "hooks", name), "#!/usr/bin/env bash\necho fixture\n")
	}
	write(filepath.Join(loopDir, "loop.json"), `{
  "schema_version": 2, "name": "memory",
  "surfaces": {"projection": [], "observation": []},
  "assets": {"guide": "GUIDE.md", "env": "env.sh", "runtime_files": ["MEMORY.md"],
    "skills": ["skills/memory-get/SKILL.md"], "subagents": []}}`)
	write(filepath.Join(hostDir, "host.json"), `{
  "schema_version": 2, "name": "codex",
  "surfaces": {"projection": [".codex/skills", ".codex/hooks", ".codex/hooks.json", ".codex/mnemon-memory"], "observation": []},
  "lifecycle_mapping": {}, "supports": {"skills": true, "hooks": true}}`)
	write(filepath.Join(bindingDir, "codex.memory.json"), `{
  "schema_version": 1, "name": "codex.memory", "host": "codex", "loop": "memory",
  "projection_path": ".codex", "runtime_surface": ".codex/mnemon-memory",
  "lifecycle_mapping": {"prime": "SessionStart", "remind": "UserPromptSubmit", "nudge": "Stop", "compact": "PreCompact"},
  "reconcile": ["read", "write", "no-op"]}`)
}

// TestSetupProjectsLoopAndWiresChannel verifies that setup projects loop assets
// and wires the channel artifacts. It also checks reinstall idempotency, status,
// and that uninstall removes the managed binding while preserving a user-added one.
func TestSetupProjectsLoopAndWiresChannel(t *testing.T) {
	root := t.TempDir()
	writeMemoryFixture(t, root)
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

	// projector ran: managed hooks + skill projected.
	hooksJSON := filepath.Join(root, ".codex", "hooks.json")
	if b, err := os.ReadFile(hooksJSON); err != nil || !strings.Contains(string(b), "mnemon") {
		t.Fatalf(".codex/hooks.json must contain managed hooks; err=%v content=%q", err, string(b))
	}
	if _, err := os.Stat(filepath.Join(root, ".codex", "skills", "memory-get", "SKILL.md")); err != nil {
		t.Fatalf("projected SKILL.md missing: %v", err)
	}
	assertProjectedAssetsHaveNoRemoteWorkspace(t, filepath.Join(root, ".codex"))

	// channel artifacts: binding entry, token file, runtime env.
	bindingFile := filepath.Join(root, ".mnemon", "harness", "channel", "bindings.json")
	loaded, err := New(root).SetupStatus("", "codex@project") // exercises LoadBindingFile path
	if err != nil {
		t.Fatalf("setup status: %v", err)
	}
	assertPublicStatusLines(t, loaded)
	bf, err := os.ReadFile(bindingFile)
	if err != nil || !strings.Contains(string(bf), "codex@project") || !strings.Contains(string(bf), "127.0.0.1:8787") {
		t.Fatalf("bindings.json must record the principal + endpoint; err=%v content=%s", err, string(bf))
	}
	tokenFile := filepath.Join(root, ".mnemon", "harness", "channel", "credentials", "codex-project.token")
	if fi, err := os.Stat(tokenFile); err != nil || fi.Size() == 0 {
		t.Fatalf("token file must exist + be non-empty: %v", err)
	}
	envSh := filepath.Join(root, ".mnemon", "harness", "channel", "env.sh")
	env, err := os.ReadFile(envSh)
	if err != nil {
		t.Fatalf("read channel env: %v", err)
	}
	for _, want := range []string{"MNEMON_HARNESS_BIN", "MNEMON_CONTROL_ADDR", "MNEMON_CONTROL_PRINCIPAL", "MNEMON_CONTROL_TOKEN_FILE", "MNEMON_MEMORY_LOOP_DIR"} {
		if !strings.Contains(string(env), want) {
			t.Fatalf("channel env must export %s; got:\n%s", want, string(env))
		}
	}

	// reinstall is idempotent: still exactly one codex binding entry.
	if _, err := h.Setup(context.Background(), &out, &errw, opts); err != nil {
		t.Fatalf("reinstall: %v", err)
	}
	if n := strings.Count(string(mustRead(t, bindingFile)), `"codex@project"`); n != 1 {
		t.Fatalf("reinstall must not duplicate the binding; got %d codex entries", n)
	}

	// a user-added sibling binding must survive uninstall.
	userOpts := SetupOptions{Host: "codex", Loops: []string{"memory"}, ControlURL: "http://127.0.0.1:8787", Principal: "human@project"}
	if _, err := h.Setup(context.Background(), &out, &errw, userOpts); err != nil {
		t.Fatalf("user setup: %v", err)
	}
	if err := h.SetupUninstall(context.Background(), &out, &errw, opts); err != nil {
		t.Fatalf("uninstall: %v", err)
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
}

func TestSetupInstallsRealCodexMemoryLocalAssets(t *testing.T) {
	projectRoot := t.TempDir()
	h := New(repoRoot(t))
	var out, errw bytes.Buffer
	opts := SetupOptions{
		Host: "codex", Loops: []string{"memory"}, ControlURL: "http://127.0.0.1:8787",
		Principal: "codex@project", UseToken: true, ProjectRoot: projectRoot,
	}
	res, err := h.Setup(context.Background(), &out, &errw, opts)
	if err != nil {
		t.Fatalf("setup real codex memory: %v\nstderr=%s", err, errw.String())
	}
	assertPublicSetupOutput(t, out.String())
	if res.ConfigFile == "" {
		t.Fatal("setup must report the Local Mnemon config file")
	}

	memoryGet := string(mustRead(t, filepath.Join(projectRoot, ".codex", "skills", "memory-get", "SKILL.md")))
	if !strings.Contains(memoryGet, "mnemon-harness control pull --json") {
		t.Fatalf("memory-get must pull scoped Local Mnemon content:\n%s", memoryGet)
	}
	memorySet := string(mustRead(t, filepath.Join(projectRoot, ".codex", "skills", "memory-set", "SKILL.md")))
	if !strings.Contains(memorySet, "memory.write_candidate.observed") || !strings.Contains(memorySet, "mnemon-harness control observe") {
		t.Fatalf("memory-set must observe local memory candidates:\n%s", memorySet)
	}
	primeHook := string(mustRead(t, filepath.Join(projectRoot, ".codex", "hooks", "mnemon-memory", "prime.sh")))
	for _, want := range []string{".mnemon/harness/local/env.sh", "control render", `--intent "teamwork.cue"`} {
		if !strings.Contains(primeHook, want) {
			t.Fatalf("prime hook must use the R1 render shim and Local Mnemon env; missing %q:\n%s", want, primeHook)
		}
	}
	for _, blocked := range []string{"--mirror", "GUIDE.md", "MEMORY.md", "control observe", "control pull"} {
		if strings.Contains(primeHook, blocked) {
			t.Fatalf("prime hook must not contain legacy dynamic projection content %q:\n%s", blocked, primeHook)
		}
	}
	mirror := string(mustRead(t, filepath.Join(projectRoot, ".codex", "mnemon-memory", "MEMORY.md")))
	if !strings.Contains(mirror, "Non-authoritative mirror") {
		t.Fatalf("projected MEMORY.md must be marked as a mirror:\n%s", mirror)
	}

	env := string(mustRead(t, filepath.Join(projectRoot, ".mnemon", "harness", "local", "env.sh")))
	for _, want := range []string{"MNEMON_HARNESS_BIN", "MNEMON_CONTROL_ADDR", "MNEMON_CONTROL_PRINCIPAL", "MNEMON_CONTROL_TOKEN_FILE", "MNEMON_MEMORY_LOOP_DIR"} {
		if !strings.Contains(env, want) {
			t.Fatalf("Local Mnemon env missing %s:\n%s", want, env)
		}
	}
	if strings.Contains(strings.ToLower(env), "remote") || strings.Contains(env, "https://") {
		t.Fatalf("Local Mnemon env must not contain remote sync details:\n%s", env)
	}
	bindingJSON := string(mustRead(t, filepath.Join(projectRoot, ".mnemon", "harness", "channel", "bindings.json")))
	if !strings.Contains(bindingJSON, ".mnemon/harness/channel/credentials/codex-project.token") {
		t.Fatalf("binding credential_ref must use the setup credentials path:\n%s", bindingJSON)
	}
	configJSON := string(mustRead(t, res.ConfigFile))
	for _, want := range []string{"local", "bindings.json", "governed.db"} {
		if !strings.Contains(configJSON, want) {
			t.Fatalf("Local Mnemon config missing %q:\n%s", want, configJSON)
		}
	}

	storePath := filepath.Join(projectRoot, ".mnemon", "harness", "control", "governed.db")
	if err := os.MkdirAll(filepath.Dir(storePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(storePath, []byte("store"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := h.SetupUninstall(context.Background(), &out, &errw, opts); err != nil {
		t.Fatalf("uninstall real codex memory: %v", err)
	}
	for _, removed := range []string{
		filepath.Join(projectRoot, ".codex", "skills", "memory-get"),
		filepath.Join(projectRoot, ".codex", "skills", "memory-set"),
		filepath.Join(projectRoot, ".codex", "hooks", "mnemon-memory"),
	} {
		if _, err := os.Stat(removed); !os.IsNotExist(err) {
			t.Fatalf("uninstall must remove projected asset %s; err=%v", removed, err)
		}
	}
	if _, err := os.Stat(storePath); err != nil {
		t.Fatalf("uninstall must preserve the canonical local store: %v", err)
	}
}

func TestSetupCanProjectThinRenderShimHooks(t *testing.T) {
	projectRoot := t.TempDir()
	h := New(repoRoot(t))
	var out, errw bytes.Buffer
	_, err := h.Setup(context.Background(), &out, &errw, SetupOptions{
		Host: "codex", Loops: []string{"memory"}, ControlURL: "http://127.0.0.1:8787",
		Principal: "codex@project", UseToken: true, ProjectRoot: projectRoot,
	})
	if err != nil {
		t.Fatalf("setup thin render shim: %v\nstderr=%s", err, errw.String())
	}
	primeHook := string(mustRead(t, filepath.Join(projectRoot, ".codex", "hooks", "mnemon-memory", "prime.sh")))
	for _, want := range []string{"control render", `--intent "teamwork.cue"`, "continue only with local context"} {
		if !strings.Contains(primeHook, want) {
			t.Fatalf("thin hook missing %q:\n%s", want, primeHook)
		}
	}
	for _, blocked := range []string{"--mirror", "GUIDE.md", "MEMORY.md", "control observe", "control pull"} {
		if strings.Contains(primeHook, blocked) {
			t.Fatalf("thin hook should not contain legacy dynamic projection content %q:\n%s", blocked, primeHook)
		}
	}
}

// TestSetupDryRunWritesNothing is the P4 gate dry-run check: --dry-run prints changes without
// writing channel artifacts.
func TestSetupDryRunWritesNothing(t *testing.T) {
	root := t.TempDir()
	writeMemoryFixture(t, root)
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
	if _, err := os.Stat(filepath.Join(root, ".mnemon", "harness", "channel", "bindings.json")); !os.IsNotExist(err) {
		t.Fatalf("dry-run must not write the binding file; err=%v", err)
	}
}

func TestSetupRejectsUnsupportedProductLoop(t *testing.T) {
	root := t.TempDir()
	writeMemoryFixture(t, root)
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
		t.Fatalf("unsupported loop setup should fail before projection output; stdout=%q stderr=%q", out.String(), errw.String())
	}
}

func TestAgentIntegrationAssetsDoNotReferenceRemoteWorkspace(t *testing.T) {
	root := repoRoot(t)
	for _, rel := range []string{
		"harness/internal/assets/loops/memory/skills",
		"harness/internal/assets/loops/skill/skills",
		"harness/internal/assets/loops/skill/hooks/fragments",
	} {
		assertProjectedAssetsHaveNoRemoteWorkspace(t, filepath.Join(root, rel))
	}
	// Hooks are GENERATED now (stage 3); the content policy applies to the generator output.
	for _, host := range []string{"codex", "claude-code"} {
		for _, loop := range []string{"memory", "skill"} {
			for _, timing := range []string{"prime", "remind", "nudge", "compact"} {
				content, err := hostsurface.RenderHook(assets.FS, loop, host, timing)
				if err != nil {
					t.Fatalf("render %s/%s/%s: %v", host, loop, timing, err)
				}
				assertContentHasNoRemoteWorkspace(t, host+"/"+loop+"/"+timing, content)
			}
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

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test file path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
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

func assertProjectedAssetsHaveNoRemoteWorkspace(t *testing.T, root string) {
	t.Helper()
	blocked := []string{"remote workspace", "remote token", "remote credential", "mnemon_remote", "remote_workspace", "https://"}
	if err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		lower := strings.ToLower(string(data))
		for _, term := range blocked {
			if strings.Contains(lower, term) {
				t.Fatalf("projected Agent Integration asset %s leaked %q", path, term)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("scan projected assets: %v", err)
	}
}
