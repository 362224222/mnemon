package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
)

func TestSetupProductFlagsSelectLoops(t *testing.T) {
	oldLoops := setupLoops
	t.Cleanup(func() {
		setupLoops = oldLoops
	})

	// Every integration is a loop now (PD7: no --memory/--skills); selectedSetupLoops only dedupes
	// the repeated --loop flag, preserving first-seen order.
	setupLoops = []string{"memory", "skill", "memory"}

	got := selectedSetupLoops()
	want := []string{"memory", "skill"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("selectedSetupLoops() = %#v, want %#v", got, want)
	}
}

func TestSetupCommandUsesProductDefaults(t *testing.T) {
	restoreSetupFlags(t)
	projectRoot := t.TempDir()
	setupRoot = cmdRepoRoot(t)
	setupProjectRoot = projectRoot
	setupHost = "codex"
	setupLoops = []string{"memory", "skill"}
	setupPrincipal = ""
	setupControlURL = ""
	setupUseToken = false

	var out, errw bytes.Buffer
	setupCmd.SetOut(&out)
	setupCmd.SetErr(&errw)
	t.Cleanup(func() {
		setupCmd.SetOut(os.Stdout)
		setupCmd.SetErr(os.Stderr)
	})
	if err := setupCmd.RunE(setupCmd, nil); err != nil {
		t.Fatalf("setup command with product defaults: %v\nstderr=%s", err, errw.String())
	}
	got := out.String()
	for _, want := range []string{"Agent Integration:", "Local Mnemon:", "Remote Workspace:"} {
		if !strings.Contains(got, want) {
			t.Fatalf("setup output missing %q:\n%s", want, got)
		}
	}

	bindingJSON := string(mustReadCmd(t, filepath.Join(projectRoot, access.DefaultBindingFile)))
	for _, want := range []string{
		`"principal": "codex@project"`,
		`"endpoint": "http://127.0.0.1:8787"`,
		`"memory.write_candidate.observed"`,
		`"skill.write_candidate.observed"`,
		`.mnemon/harness/channel/credentials/codex-project.token`,
	} {
		if !strings.Contains(bindingJSON, want) {
			t.Fatalf("setup defaults missing %q from bindings:\n%s", want, bindingJSON)
		}
	}
	// Single canonical pin: setup must no longer dual-emit the legacy underscore alias.
	if strings.Contains(bindingJSON, "write_candidate_observed") {
		t.Fatalf("setup bindings must not carry the legacy underscore observed-type alias:\n%s", bindingJSON)
	}
	if _, err := os.Stat(filepath.Join(projectRoot, ".mnemon", "harness", "channel", "credentials", "codex-project.token")); err != nil {
		t.Fatalf("setup must generate the default local token: %v", err)
	}
	configJSON := string(mustReadCmd(t, filepath.Join(projectRoot, ".mnemon", "harness", "local", "config.json")))
	for _, want := range []string{`"endpoint": "http://127.0.0.1:8787"`, `"principal": "codex@project"`, "bindings.json", "governed.db"} {
		if !strings.Contains(configJSON, want) {
			t.Fatalf("Local Mnemon config missing %q:\n%s", want, configJSON)
		}
	}
}

func restoreSetupFlags(t *testing.T) {
	t.Helper()
	oldRoot := setupRoot
	oldProjectRoot := setupProjectRoot
	oldHost := setupHost
	oldLoops := setupLoops
	oldPrincipal := setupPrincipal
	oldControlURL := setupControlURL
	oldActorKind := setupActorKind
	oldUseToken := setupUseToken
	oldDryRun := setupDryRun
	oldThinRenderShim := setupThinRenderShim
	t.Cleanup(func() {
		setupRoot = oldRoot
		setupProjectRoot = oldProjectRoot
		setupHost = oldHost
		setupLoops = oldLoops
		setupPrincipal = oldPrincipal
		setupControlURL = oldControlURL
		setupActorKind = oldActorKind
		setupUseToken = oldUseToken
		setupDryRun = oldDryRun
		setupThinRenderShim = oldThinRenderShim
	})
	setupRoot = "."
	setupProjectRoot = ""
	setupHost = ""
	setupLoops = nil
	setupPrincipal = ""
	setupControlURL = ""
	setupActorKind = "host-agent"
	setupUseToken = false
	setupDryRun = false
	setupThinRenderShim = true
}

func cmdRepoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve command test path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func setupProductIntegration(t *testing.T, projectRoot string) {
	t.Helper()
	restoreSetupFlags(t)
	setupRoot = cmdRepoRoot(t)
	setupProjectRoot = projectRoot
	setupHost = "codex"
	setupLoops = []string{"memory", "skill"}
	setupPrincipal = ""
	setupControlURL = ""
	setupUseToken = false
	var out, errw bytes.Buffer
	setupCmd.SetOut(&out)
	setupCmd.SetErr(&errw)
	t.Cleanup(func() {
		setupCmd.SetOut(os.Stdout)
		setupCmd.SetErr(os.Stderr)
	})
	ctx := context.Background()
	setupCmd.SetContext(ctx)
	if err := setupCmd.RunE(setupCmd, nil); err != nil {
		t.Fatalf("setup product integration: %v\nstdout=%s\nstderr=%s", err, out.String(), errw.String())
	}
}
