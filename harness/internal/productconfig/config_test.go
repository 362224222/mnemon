package productconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	multicasurface "github.com/mnemon-dev/mnemon/harness/internal/surface/multica"
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
	cfg.Daemon.DisplaySurfaces = []string{ConnectionMultica}
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
	cfg.Daemon.DisplaySurfaces = []string{ConnectionMultica}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "not enabled") {
		t.Fatalf("expected disabled display surface error, got %v", err)
	}

	cfg = Default()
	cfg.Connections.Mnemonhub = MnemonhubConnection{Enabled: true, Endpoint: "https://hub.example"}
	cfg.Daemon.DisplaySurfaces = []string{ConnectionMnemonhub}
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "remote exchange backend") {
		t.Fatalf("expected mnemonhub display surface error, got %v", err)
	}

	cfg = Default()
	cfg.Connections.Mnemonhub = MnemonhubConnection{Enabled: true, Endpoint: "https://hub.example"}
	cfg.Sessions.PrimaryActivationCarrier = ConnectionMnemonhub
	err = cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "primary activation carrier") || !strings.Contains(err.Error(), "remote exchange backend") {
		t.Fatalf("expected mnemonhub primary activation carrier error, got %v", err)
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

func TestConfigValidateRejectsDuplicateDaemonRoles(t *testing.T) {
	base := Default()
	base.Connections.Multica = MulticaConnection{Enabled: true, Workspace: "ws-multica"}
	cases := []struct {
		name string
		edit func(*Config)
		want string
	}{
		{
			name: "watcher",
			edit: func(cfg *Config) {
				cfg.Daemon.InteractionWatchers = []string{ConnectionMultica, " " + ConnectionMultica + " "}
			},
			want: "duplicate interaction watcher",
		},
		{
			name: "empty watcher",
			edit: func(cfg *Config) {
				cfg.Daemon.InteractionWatchers = []string{" "}
			},
			want: "interaction watcher cannot be empty",
		},
		{
			name: "surface",
			edit: func(cfg *Config) {
				cfg.Daemon.DisplaySurfaces = []string{ConnectionMultica, " " + ConnectionMultica}
			},
			want: "duplicate display surface",
		},
		{
			name: "empty surface",
			edit: func(cfg *Config) {
				cfg.Daemon.DisplaySurfaces = []string{""}
			},
			want: "display surface cannot be empty",
		},
		{
			name: "drive",
			edit: func(cfg *Config) {
				cfg.Daemon.DriveSources = []string{DriveManagedLocal, " " + DriveManagedLocal}
			},
			want: "duplicate drive source",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.edit(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected %q error, got %v", tc.want, err)
			}
		})
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
	if !cfg.Connections.Mnemonhub.Enabled || cfg.Connections.Mnemonhub.Endpoint != "https://hub.example" {
		t.Fatalf("mnemonhub bridge mismatch: %+v", cfg.Connections.Mnemonhub)
	}
	if !stringSliceContains(cfg.Daemon.InteractionWatchers, ConnectionMnemonhub) {
		t.Fatalf("mnemonhub watcher missing: %+v", cfg.Daemon.InteractionWatchers)
	}
	if stringSliceContains(cfg.Daemon.DisplaySurfaces, ConnectionMnemonhub) {
		t.Fatalf("mnemonhub must not be bridged as a display surface: %+v", cfg.Daemon.DisplaySurfaces)
	}
	if stringSliceContains(cfg.Daemon.InteractionWatchers, "github") || stringSliceContains(cfg.Daemon.DisplaySurfaces, "github") {
		t.Fatalf("legacy github remotes must not bridge into product config: %+v", cfg.Daemon)
	}
	if len(cfg.Daemon.DriveSources) != 1 || cfg.Daemon.DriveSources[0] != DriveManagedLocal {
		t.Fatalf("drive sources mismatch: %+v", cfg.Daemon.DriveSources)
	}
}

func TestFromLegacyBridgesMulticaRegistry(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, ".mnemon", "harness", "local"), 0o755); err != nil {
		t.Fatal(err)
	}
	local := `{
  "schema_version": 1,
  "principal": "planner@team",
  "loops": ["assignment"]
}`
	if err := os.WriteFile(filepath.Join(root, ".mnemon", "harness", "local", "config.json"), []byte(local), 0o600); err != nil {
		t.Fatal(err)
	}
	reg := multicasurface.MulticaRegistry{
		SchemaVersion:    1,
		WorkspaceID:      "ws-multica",
		RuntimeProfileID: "profile-1",
		RuntimeID:        "runtime-1",
		Participants: []multicasurface.MulticaParticipantRecord{
			{
				Principal: "planner@team",
				AgentName: "mnemon-planner",
				AgentID:   "agent-planner",
				Role:      "planner",
			},
			{
				Principal: "reviewer@team",
				AgentName: "mnemon-reviewer",
				AgentID:   "agent-reviewer",
				Role:      "reviewer",
			},
		},
	}
	if err := multicasurface.SaveMulticaRegistry(multicasurface.MulticaRegistryPath(root, ""), reg); err != nil {
		t.Fatal(err)
	}

	cfg, found, err := FromLegacy(root)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("legacy multica registry should be found")
	}
	if !cfg.Connections.Multica.Enabled || cfg.Connections.Multica.Workspace != "ws-multica" || cfg.Connections.Multica.RuntimeBinary != "mnemon-multica-runtime" {
		t.Fatalf("multica bridge mismatch: %+v", cfg.Connections.Multica)
	}
	if cfg.Sessions.PrimaryActivationCarrier != ConnectionMultica {
		t.Fatalf("primary carrier = %q", cfg.Sessions.PrimaryActivationCarrier)
	}
	if !stringSliceContains(cfg.Daemon.InteractionWatchers, ConnectionMultica) || !stringSliceContains(cfg.Daemon.DisplaySurfaces, ConnectionMultica) {
		t.Fatalf("daemon multica roles missing: %+v", cfg.Daemon)
	}
	planner := participantByPrincipal(cfg.Participants, "planner@team")
	if planner.HostRuntime.Kind != RuntimeKindConfigured || planner.HostRuntime.Mode != RuntimeModeAttached {
		t.Fatalf("planner host runtime should come from legacy local config: %+v", planner)
	}
	if planner.DisplayName != "mnemon-planner" || planner.Role != "planner" {
		t.Fatalf("planner multica display fields not merged: %+v", planner)
	}
	reviewer := participantByPrincipal(cfg.Participants, "reviewer@team")
	if reviewer.HostRuntime.Kind != RuntimeKindCodex || reviewer.HostRuntime.Mode != RuntimeModeManagedOrHost || reviewer.DisplayName != "mnemon-reviewer" {
		t.Fatalf("reviewer participant mismatch: %+v", reviewer)
	}
	if !stringSliceContains(cfg.Daemon.DriveSources, DriveManagedLocal) {
		t.Fatalf("drive source missing: %+v", cfg.Daemon.DriveSources)
	}
}

func participantByPrincipal(participants []Participant, principal string) Participant {
	for _, participant := range participants {
		if participant.Principal == principal {
			return participant
		}
	}
	return Participant{}
}

func stringSliceContains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
