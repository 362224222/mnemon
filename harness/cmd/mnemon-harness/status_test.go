package main

import (
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

func TestProductStatusBeforeAndAfterSetup(t *testing.T) {
	projectRoot := t.TempDir()
	restoreStatusFlags(t)
	statusRoot = cmdRepoRoot(t)
	statusProjectRoot = projectRoot

	cmd, output := testCommand()
	if err := runProductStatus(cmd, nil); err != nil {
		t.Fatalf("status before setup: %v", err)
	}
	before := output.String()
	for _, want := range []string{
		"Agent Integration: not installed",
		"Local Mnemon: not configured",
		"Remote Workspace: not connected",
		"Sync: 0 pending, 0 synced, 0 conflicts",
	} {
		if !strings.Contains(before, want) {
			t.Fatalf("status before setup missing %q:\n%s", want, before)
		}
	}

	setupProductIntegration(t, projectRoot)
	output.Reset()
	if err := runProductStatus(cmd, nil); err != nil {
		t.Fatalf("status after setup: %v", err)
	}
	after := output.String()
	for _, want := range []string{
		"Agent Integration: installed",
		"Local Mnemon: ready",
		"Remote Workspace: not connected",
		"Sync: 0 pending, 0 synced, 0 conflicts",
	} {
		if !strings.Contains(after, want) {
			t.Fatalf("status after setup missing %q:\n%s", want, after)
		}
	}
	for _, blocked := range []string{"binding", "channel", "projection", "kernel", "runtime", "cursor", "token"} {
		if strings.Contains(strings.ToLower(after), blocked) {
			t.Fatalf("status leaked internal term %q:\n%s", blocked, after)
		}
	}
}

func TestProductStatusUsesReachableLocalMnemon(t *testing.T) {
	projectRoot := t.TempDir()
	setupProductIntegration(t, projectRoot)
	restoreLocalFlags(t)
	localRoot = projectRoot
	boot, err := app.ResolveLocalBoot(projectRoot, localStorePath, localBindingsPath)
	if err != nil {
		t.Fatalf("resolve local boot: %v", err)
	}
	rt, err := app.OpenLocalRuntime(boot.StorePath, boot.Loaded, boot.Config.Loops, nil)
	if err != nil {
		t.Fatalf("open local runtime: %v", err)
	}
	defer rt.Close()
	if _, _, err := rt.API().Ingest("codex@project", contract.ObservationEnvelope{
		ExternalID: "status-pending",
		Event: contract.Event{Type: "progress_digest.write_candidate.observed", Payload: map[string]any{
			"summary": "Status should read pending sync from the live Local Mnemon service.",
		}},
	}); err != nil {
		t.Fatalf("seed progress candidate: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick local runtime: %v", err)
	}

	srv := httptest.NewServer(runtime.NewRuntimeHandler(rt, access.TokenAuthenticator{Tokens: boot.Loaded.Tokens}))
	defer srv.Close()
	cfg := boot.Config
	cfg.Endpoint = srv.URL
	writeLocalConfigForTest(t, projectRoot, cfg)

	restoreStatusFlags(t)
	statusRoot = cmdRepoRoot(t)
	statusProjectRoot = projectRoot
	cmd, output := testCommand()
	if err := runProductStatus(cmd, nil); err != nil {
		t.Fatalf("status while local reachable: %v", err)
	}
	got := output.String()
	for _, want := range []string{
		"Agent Integration: installed",
		"Local Mnemon: ready",
		"Sync: 1 pending, 0 synced, 0 conflicts",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("reachable status missing %q:\n%s", want, got)
		}
	}
}

func TestProductStatusReportsConnectedRemoteWorkspace(t *testing.T) {
	projectRoot := t.TempDir()
	setupProductIntegration(t, projectRoot)
	restoreSyncFlags(t)
	syncRoot = projectRoot
	syncRemoteURL = "https://remote.example.test"
	syncRemoteToken = "secret-status-token"
	connectCmd, _ := testCommand()
	if err := runSyncConnect(connectCmd, []string{"team"}); err != nil {
		t.Fatalf("sync connect for status: %v", err)
	}

	restoreStatusFlags(t)
	statusRoot = cmdRepoRoot(t)
	statusProjectRoot = projectRoot
	cmd, output := testCommand()
	if err := runProductStatus(cmd, nil); err != nil {
		t.Fatalf("status with remote connected: %v", err)
	}
	got := output.String()
	if !strings.Contains(got, "Remote Workspace: connected team") {
		t.Fatalf("status must show connected remote:\n%s", got)
	}
	if strings.Contains(got, "secret-status-token") {
		t.Fatalf("status must not expose remote token:\n%s", got)
	}
}

func restoreStatusFlags(t *testing.T) {
	t.Helper()
	oldRoot := statusRoot
	oldProjectRoot := statusProjectRoot
	oldPrincipal := statusPrincipal
	t.Cleanup(func() {
		statusRoot = oldRoot
		statusProjectRoot = oldProjectRoot
		statusPrincipal = oldPrincipal
	})
	statusRoot = "."
	statusProjectRoot = ""
	statusPrincipal = ""
}

func writeLocalConfigForTest(t *testing.T, projectRoot string, cfg app.LocalConfig) {
	t.Helper()
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(projectRoot, ".mnemon", "harness", "local", "config.json")
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		t.Fatalf("write Local Mnemon config: %v", err)
	}
}
