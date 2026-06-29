package main

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/productconfig"
)

func TestConfigValidateReadsProductConfig(t *testing.T) {
	root := t.TempDir()
	cfg := productconfig.Default()
	cfg.Participants = []productconfig.Participant{{
		Principal: "planner@team",
		HostRuntime: productconfig.HostRuntime{
			Kind: productconfig.RuntimeKindCodex,
			Mode: productconfig.RuntimeModeManaged,
		},
	}}
	if err := productconfig.Save(productconfig.DefaultPath(root, ""), cfg); err != nil {
		t.Fatal(err)
	}
	oldRoot, oldPath := configRoot, configPath
	configRoot, configPath = root, ""
	t.Cleanup(func() { configRoot, configPath = oldRoot, oldPath })

	cmd, out := testCommand()
	if err := runConfigValidate(cmd, nil); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, "Harness config: valid") || !strings.Contains(got, "Participants: 1") {
		t.Fatalf("unexpected config validate output:\n%s", got)
	}
}

func TestDaemonStatusDoesNotMutateMissingConfig(t *testing.T) {
	oldRoot := daemonRoot
	daemonRoot = t.TempDir()
	t.Cleanup(func() { daemonRoot = oldRoot })

	cmd, out := testCommand()
	if err := runDaemonStatus(cmd, nil); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{"Harness config: not configured", "Harness daemon: not running"} {
		if !strings.Contains(got, want) {
			t.Fatalf("daemon status missing %q:\n%s", want, got)
		}
	}
}

func TestAgentAddAndListWriteProductConfig(t *testing.T) {
	root := t.TempDir()
	oldRoot, oldPath := agentRoot, agentConfigPath
	oldPrincipal, oldDisplayName, oldRole := agentPrincipal, agentDisplayName, agentRole
	oldKind, oldMode := agentRuntimeKind, agentRuntimeMode
	agentRoot = root
	agentConfigPath = ""
	agentPrincipal = "planner@team"
	agentDisplayName = "Planner"
	agentRole = "planner"
	agentRuntimeKind = productconfig.RuntimeKindCodex
	agentRuntimeMode = productconfig.RuntimeModeManagedOrHost
	t.Cleanup(func() {
		agentRoot, agentConfigPath = oldRoot, oldPath
		agentPrincipal, agentDisplayName, agentRole = oldPrincipal, oldDisplayName, oldRole
		agentRuntimeKind, agentRuntimeMode = oldKind, oldMode
	})

	cmd, out := testCommand()
	if err := runAgentAdd(cmd, nil); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, "Agent: added planner@team") {
		t.Fatalf("unexpected add output:\n%s", got)
	}
	cfg := loadProductConfigForTest(t, root)
	if len(cfg.Participants) != 1 || cfg.Participants[0].Principal != "planner@team" {
		t.Fatalf("participant not written: %+v", cfg.Participants)
	}
	if got := cfg.Participants[0].HostRuntime.Kind; got != productconfig.RuntimeKindCodex {
		t.Fatalf("unexpected runtime kind: %q", got)
	}
	if len(cfg.Daemon.DriveSources) != 1 || cfg.Daemon.DriveSources[0] != productconfig.DriveManagedLocal {
		t.Fatalf("managed drive source not configured: %+v", cfg.Daemon.DriveSources)
	}

	cmd, out = testCommand()
	if err := runAgentList(cmd, nil); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, "Agent: planner@team (Planner) - planner") {
		t.Fatalf("unexpected list output:\n%s", got)
	}
}

func TestConnectCommandsWriteProductConfig(t *testing.T) {
	root := t.TempDir()
	oldRoot, oldPath := connectRoot, connectConfigPath
	oldWorkspace, oldRuntime := connectMulticaWS, connectMulticaRuntime
	oldRepo, oldBranch := connectGitHubRepo, connectGitHubBranch
	oldEndpoint := connectMnemonhubURL
	connectRoot = root
	connectConfigPath = ""
	connectMulticaWS = "teamwork-grivn"
	connectMulticaRuntime = "mnemon-multica-runtime"
	connectGitHubRepo = "mnemon-dev/mnemon-teamwork-example"
	connectGitHubBranch = "mnemond-planner"
	connectMnemonhubURL = "https://hub.example.invalid"
	t.Cleanup(func() {
		connectRoot, connectConfigPath = oldRoot, oldPath
		connectMulticaWS, connectMulticaRuntime = oldWorkspace, oldRuntime
		connectGitHubRepo, connectGitHubBranch = oldRepo, oldBranch
		connectMnemonhubURL = oldEndpoint
	})

	cmd, _ := testCommand()
	if err := runConnectMultica(cmd, nil); err != nil {
		t.Fatal(err)
	}
	if err := runConnectGitHub(cmd, nil); err != nil {
		t.Fatal(err)
	}
	if err := runConnectMnemonhub(cmd, nil); err != nil {
		t.Fatal(err)
	}

	cfg := loadProductConfigForTest(t, root)
	if !cfg.Connections.Multica.Enabled || cfg.Connections.Multica.Workspace != "teamwork-grivn" {
		t.Fatalf("multica connection not written: %+v", cfg.Connections.Multica)
	}
	if got := cfg.Connections.Multica.RuntimeBinary; got != "mnemon-multica-runtime" {
		t.Fatalf("unexpected runtime binary: %q", got)
	}
	if !cfg.Connections.GitHub.Enabled || cfg.Connections.GitHub.Repo != "mnemon-dev/mnemon-teamwork-example" {
		t.Fatalf("github connection not written: %+v", cfg.Connections.GitHub)
	}
	if !cfg.Connections.Mnemonhub.Enabled || cfg.Connections.Mnemonhub.Endpoint != "https://hub.example.invalid" {
		t.Fatalf("mnemonhub connection not written: %+v", cfg.Connections.Mnemonhub)
	}
	for _, want := range []string{productconfig.ConnectionMultica, productconfig.ConnectionGitHub, productconfig.ConnectionMnemonhub} {
		if !containsString(cfg.Daemon.InteractionWatchers, want) {
			t.Fatalf("interaction watcher %q missing: %+v", want, cfg.Daemon.InteractionWatchers)
		}
		if !containsString(cfg.Daemon.ProjectionSurfaces, want) {
			t.Fatalf("projection surface %q missing: %+v", want, cfg.Daemon.ProjectionSurfaces)
		}
	}
	if got := cfg.Sessions.PrimaryActivationCarrier; got != productconfig.ConnectionMultica {
		t.Fatalf("unexpected primary activation carrier: %q", got)
	}
}

func loadProductConfigForTest(t *testing.T, root string) productconfig.Config {
	t.Helper()
	cfg, err := productconfig.Load(filepath.Join(root, productconfig.DefaultRelPath))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
