package productconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfigRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "config.json")
	cfg := Default()
	cfg.Participants = []Participant{{
		Principal: "planner@team",
		HostRuntime: HostRuntime{
			Kind: RuntimeKindCodex,
			Mode: RuntimeModeManagedOrHost,
		},
	}}
	cfg.Connections.Multica = MulticaConnection{
		Enabled:       true,
		Workspace:     "teamwork-grivn",
		RuntimeBinary: "mnemon-multica-runtime",
	}
	cfg.Daemon.InteractionWatchers = []string{ConnectionMultica}
	cfg.Daemon.ProjectionSurfaces = []string{ConnectionMultica}
	cfg.Daemon.DriveSources = []string{DriveManagedLocal}
	cfg.Sessions.PrimaryActivationCarrier = ConnectionMultica

	if err := Save(path, cfg); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != SchemaVersion || got.Connections.Multica.RuntimeBinary != "mnemon-multica-runtime" || len(got.Participants) != 1 {
		t.Fatalf("config mismatch: %+v", got)
	}
}

func TestConfigValidateRejectsCrossLayerLeaks(t *testing.T) {
	cfg := Default()
	cfg.SchemaVersion = SchemaVersion
	cfg.Participants = []Participant{{
		Principal: "planner@team",
		HostRuntime: HostRuntime{
			Kind: RuntimeKindCodex,
			Mode: RuntimeModeManaged,
		},
	}}
	cfg.Daemon.ProjectionSurfaces = []string{ConnectionMultica}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "not enabled") {
		t.Fatalf("expected disabled projection surface error, got %v", err)
	}
}

func TestConfigValidateRejectsDuplicateParticipants(t *testing.T) {
	cfg := Default()
	cfg.Participants = []Participant{
		{Principal: "planner@team", HostRuntime: HostRuntime{Kind: RuntimeKindCodex, Mode: RuntimeModeManaged}},
		{Principal: "planner@team", HostRuntime: HostRuntime{Kind: RuntimeKindCodex, Mode: RuntimeModeManaged}},
	}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "duplicate participant") {
		t.Fatalf("expected duplicate participant error, got %v", err)
	}
}

func TestFromLegacyBridgesLocalAndRemoteConfigs(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".mnemon", "harness", "local"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, ".mnemon", "harness", "sync"), 0o755); err != nil {
		t.Fatal(err)
	}
	local := `{
  "schema_version": 1,
  "mode": "local",
  "endpoint": "http://127.0.0.1:8787",
  "principal": "planner@team",
  "loops": ["assignment"],
  "binding_file": ".mnemon/harness/channel/bindings.json",
  "store_path": ".mnemon/harness/local/governed.db"
}`
	remotes := `{
  "schema_version": 1,
  "current": "mesh",
  "remotes": [
    {"id": "mesh", "backend": "github", "repo": "mnemon-dev/mnemon-teamwork-example", "branch": "mnemond-planner", "credential_ref": ".mnemon/harness/sync/credentials/self.token"},
    {"id": "hub", "backend": "http", "endpoint": "https://hub.example", "credential_ref": ".mnemon/harness/sync/credentials/hub.token"}
  ]
}`
	if err := os.WriteFile(filepath.Join(root, ".mnemon", "harness", "local", "config.json"), []byte(local), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".mnemon", "harness", "sync", "remotes.json"), []byte(remotes), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg, found, err := FromLegacy(root)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("legacy configs should be found")
	}
	if len(cfg.Participants) != 1 || cfg.Participants[0].Principal != "planner@team" {
		t.Fatalf("participant bridge mismatch: %+v", cfg.Participants)
	}
	if !cfg.Connections.GitHub.Enabled || cfg.Connections.GitHub.Repo != "mnemon-dev/mnemon-teamwork-example" {
		t.Fatalf("github bridge mismatch: %+v", cfg.Connections.GitHub)
	}
	if !cfg.Connections.Mnemonhub.Enabled || cfg.Connections.Mnemonhub.Endpoint != "https://hub.example" {
		t.Fatalf("mnemonhub bridge mismatch: %+v", cfg.Connections.Mnemonhub)
	}
	if len(cfg.Daemon.DriveSources) != 1 || cfg.Daemon.DriveSources[0] != DriveManagedLocal {
		t.Fatalf("drive sources mismatch: %+v", cfg.Daemon.DriveSources)
	}
}
