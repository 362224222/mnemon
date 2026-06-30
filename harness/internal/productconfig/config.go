package productconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	participantrole "github.com/mnemon-dev/mnemon/harness/internal/participant"
	multicasurface "github.com/mnemon-dev/mnemon/harness/internal/surface/multica"
)

const (
	SchemaVersion  = 1
	DefaultRelPath = ".mnemon/harness/config.json"

	RuntimeKindCodex         = "codex"
	RuntimeKindConfigured    = "configured-host-agent"
	RuntimeModeAttached      = "attached"
	RuntimeModeManaged       = "managed"
	RuntimeModeManagedOrHost = "managed-or-attached"

	ConnectionMultica   = "multica"
	ConnectionGitHub    = "github"
	ConnectionMnemonhub = "mnemonhub"

	DefaultMulticaRuntimeBinary = multicasurface.MulticaRuntimeCommandName

	DriveManagedLocal = "managed-local"

	DuplicateActivationSuppress = "suppress"
	DuplicateActivationAllow    = "allow"
)

type Config struct {
	SchemaVersion int             `json:"schema_version"`
	Team          Team            `json:"team,omitempty"`
	Participants  []Participant   `json:"participants,omitempty"`
	Connections   Connections     `json:"connections,omitempty"`
	Daemon        Daemon          `json:"daemon,omitempty"`
	Sessions      SessionDefaults `json:"sessions,omitempty"`
}

type Team struct {
	Name    string `json:"name,omitempty"`
	Profile string `json:"profile,omitempty"`
}

type Participant struct {
	Principal   string      `json:"principal"`
	DisplayName string      `json:"display_name,omitempty"`
	Role        string      `json:"role,omitempty"`
	HostRuntime HostRuntime `json:"host_runtime"`
}

type HostRuntime struct {
	Kind string `json:"kind"`
	Mode string `json:"mode"`
}

type Connections struct {
	Multica   MulticaConnection   `json:"multica,omitempty"`
	GitHub    GitHubConnection    `json:"github,omitempty"`
	Mnemonhub MnemonhubConnection `json:"mnemonhub,omitempty"`
}

type MulticaConnection struct {
	Enabled       bool   `json:"enabled,omitempty"`
	Workspace     string `json:"workspace,omitempty"`
	RuntimeBinary string `json:"runtime_binary,omitempty"`
}

type GitHubConnection struct {
	Enabled bool   `json:"enabled,omitempty"`
	Repo    string `json:"repo,omitempty"`
	Branch  string `json:"branch,omitempty"`
}

type MnemonhubConnection struct {
	Enabled  bool   `json:"enabled,omitempty"`
	Endpoint string `json:"endpoint,omitempty"`
}

type Daemon struct {
	InteractionWatchers []string `json:"interaction_watchers,omitempty"`
	DriveSources        []string `json:"drive_sources,omitempty"`
	DisplaySurfaces     []string `json:"display_surfaces,omitempty"`
}

type SessionDefaults struct {
	PrimaryActivationCarrier  string `json:"primary_activation_carrier,omitempty"`
	DuplicateActivationPolicy string `json:"duplicate_activation_policy,omitempty"`
}

func DefaultPath(root, explicit string) string {
	if strings.TrimSpace(explicit) != "" {
		return filepath.Clean(explicit)
	}
	root = strings.TrimSpace(root)
	if root == "" {
		root = "."
	}
	return filepath.Join(root, DefaultRelPath)
}

func Default() Config {
	return Config{
		SchemaVersion: SchemaVersion,
		Team: Team{
			Name:    "default",
			Profile: "teamwork",
		},
		Sessions: SessionDefaults{
			DuplicateActivationPolicy: DuplicateActivationSuppress,
		},
	}
}

func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("parse product config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func Save(path string, cfg Config) error {
	if cfg.SchemaVersion == 0 {
		cfg.SchemaVersion = SchemaVersion
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o600)
}

func (cfg Config) Validate() error {
	if cfg.SchemaVersion != SchemaVersion {
		return fmt.Errorf("product config schema_version %d unsupported (want %d)", cfg.SchemaVersion, SchemaVersion)
	}
	if err := participantrole.ValidateUniquePrincipals(cfg.Participants, "participant", func(participant Participant) string {
		return participant.Principal
	}); err != nil {
		return err
	}
	for _, participant := range cfg.Participants {
		principal := participantrole.NormalizePrincipal(participant.Principal)
		if strings.TrimSpace(participant.HostRuntime.Kind) == "" {
			return fmt.Errorf("participant %q host_runtime.kind is required", principal)
		}
		if strings.TrimSpace(participant.HostRuntime.Mode) == "" {
			return fmt.Errorf("participant %q host_runtime.mode is required", principal)
		}
	}
	if cfg.Connections.Multica.Enabled {
		if strings.TrimSpace(cfg.Connections.Multica.Workspace) == "" {
			return fmt.Errorf("multica connection workspace is required when enabled")
		}
	}
	if cfg.Connections.GitHub.Enabled {
		if strings.TrimSpace(cfg.Connections.GitHub.Repo) == "" {
			return fmt.Errorf("github connection repo is required when enabled")
		}
	}
	if cfg.Connections.Mnemonhub.Enabled {
		if strings.TrimSpace(cfg.Connections.Mnemonhub.Endpoint) == "" {
			return fmt.Errorf("mnemonhub connection endpoint is required when enabled")
		}
	}
	if err := validatePrimaryActivationCarrier(cfg.Sessions.PrimaryActivationCarrier, cfg); err != nil {
		return err
	}
	switch strings.TrimSpace(cfg.Sessions.DuplicateActivationPolicy) {
	case "", DuplicateActivationSuppress, DuplicateActivationAllow:
	default:
		return fmt.Errorf("duplicate_activation_policy must be %q or %q", DuplicateActivationSuppress, DuplicateActivationAllow)
	}
	if err := validateDaemonCarriers("interaction watcher", cfg.Daemon.InteractionWatchers, cfg); err != nil {
		return err
	}
	if err := validateDisplaySurfaces(cfg.Daemon.DisplaySurfaces, cfg); err != nil {
		return err
	}
	if err := validateDaemonDriveSources(cfg.Daemon.DriveSources); err != nil {
		return err
	}
	return nil
}

func validateDaemonCarriers(label string, values []string, cfg Config) error {
	seen := map[string]bool{}
	for _, value := range values {
		carrier := strings.TrimSpace(value)
		if carrier == "" {
			return fmt.Errorf("%s cannot be empty", label)
		}
		if err := validateCarrier(label, carrier, cfg); err != nil {
			return err
		}
		if seen[carrier] {
			return fmt.Errorf("duplicate %s %q", label, carrier)
		}
		seen[carrier] = true
	}
	return nil
}

func validatePrimaryActivationCarrier(carrier string, cfg Config) error {
	carrier = strings.TrimSpace(carrier)
	if carrier == "" {
		return nil
	}
	if carrier == ConnectionMnemonhub {
		return fmt.Errorf("primary activation carrier %q is unsupported; mnemonhub is a remote exchange backend", carrier)
	}
	return validateCarrier("primary activation carrier", carrier, cfg)
}

func validateDisplaySurfaces(values []string, cfg Config) error {
	seen := map[string]bool{}
	for _, value := range values {
		surface := strings.TrimSpace(value)
		if surface == "" {
			return fmt.Errorf("display surface cannot be empty")
		}
		if surface == ConnectionMnemonhub {
			return fmt.Errorf("display surface %q is unsupported; mnemonhub is a remote exchange backend", surface)
		}
		if err := validateCarrier("display surface", surface, cfg); err != nil {
			return err
		}
		if seen[surface] {
			return fmt.Errorf("duplicate display surface %q", surface)
		}
		seen[surface] = true
	}
	return nil
}

func validateDaemonDriveSources(values []string) error {
	seen := map[string]bool{}
	for _, source := range values {
		source = strings.TrimSpace(source)
		if source == "" {
			return fmt.Errorf("drive source cannot be empty")
		}
		if source != DriveManagedLocal {
			return fmt.Errorf("unsupported drive source %q", source)
		}
		if seen[source] {
			return fmt.Errorf("duplicate drive source %q", source)
		}
		seen[source] = true
	}
	return nil
}

func validateCarrier(label, carrier string, cfg Config) error {
	carrier = strings.TrimSpace(carrier)
	if carrier == "" {
		return nil
	}
	switch carrier {
	case ConnectionMultica:
		if !cfg.Connections.Multica.Enabled {
			return fmt.Errorf("%s %q is not enabled", label, carrier)
		}
	case ConnectionGitHub:
		if !cfg.Connections.GitHub.Enabled {
			return fmt.Errorf("%s %q is not enabled", label, carrier)
		}
	case ConnectionMnemonhub:
		if !cfg.Connections.Mnemonhub.Enabled {
			return fmt.Errorf("%s %q is not enabled", label, carrier)
		}
	default:
		return fmt.Errorf("%s %q is unsupported", label, carrier)
	}
	return nil
}

type legacyLocalConfig struct {
	SchemaVersion int      `json:"schema_version"`
	Principal     string   `json:"principal"`
	Loops         []string `json:"loops"`
}

type legacyRemotesDoc struct {
	SchemaVersion int                 `json:"schema_version"`
	Current       string              `json:"current,omitempty"`
	Remotes       []legacyRemoteEntry `json:"remotes"`
}

type legacyRemoteEntry struct {
	Backend  string `json:"backend,omitempty"`
	ID       string `json:"id"`
	Endpoint string `json:"endpoint,omitempty"`
	Repo     string `json:"repo,omitempty"`
	Branch   string `json:"branch,omitempty"`
}

func FromLegacy(root string) (Config, bool, error) {
	cfg := Default()
	found := false
	local, ok, err := loadLegacyLocal(root)
	if err != nil {
		return Config{}, false, err
	}
	if ok {
		found = true
		if principal := participantrole.NormalizePrincipal(local.Principal); principal != "" {
			cfg.Participants = append(cfg.Participants, Participant{
				Principal: principal,
				HostRuntime: HostRuntime{
					Kind: RuntimeKindConfigured,
					Mode: RuntimeModeAttached,
				},
			})
		}
	}
	remotes, ok, err := loadLegacyRemotes(root)
	if err != nil {
		return Config{}, false, err
	}
	if ok {
		found = true
		for _, remote := range remotes.Remotes {
			backend := strings.TrimSpace(remote.Backend)
			if backend == "" {
				backend = ConnectionMnemonhub
			}
			switch backend {
			case "http", ConnectionMnemonhub:
				cfg.Connections.Mnemonhub.Enabled = true
				if strings.TrimSpace(cfg.Connections.Mnemonhub.Endpoint) == "" {
					cfg.Connections.Mnemonhub.Endpoint = strings.TrimSpace(remote.Endpoint)
				}
			case ConnectionGitHub:
				cfg.Connections.GitHub.Enabled = true
				if strings.TrimSpace(cfg.Connections.GitHub.Repo) == "" {
					cfg.Connections.GitHub.Repo = strings.TrimSpace(remote.Repo)
					cfg.Connections.GitHub.Branch = strings.TrimSpace(remote.Branch)
				}
			}
		}
	}
	multicaRegistry, ok, err := loadLegacyMulticaRegistry(root)
	if err != nil {
		return Config{}, false, err
	}
	if ok {
		found = true
		if err := bridgeMulticaRegistry(&cfg, multicaRegistry); err != nil {
			return Config{}, false, err
		}
	}
	if cfg.Connections.Mnemonhub.Enabled {
		cfg.Daemon.InteractionWatchers = appendCarrier(cfg.Daemon.InteractionWatchers, ConnectionMnemonhub)
	}
	if cfg.Connections.GitHub.Enabled {
		cfg.Daemon.InteractionWatchers = appendCarrier(cfg.Daemon.InteractionWatchers, ConnectionGitHub)
		cfg.Daemon.DisplaySurfaces = appendCarrier(cfg.Daemon.DisplaySurfaces, ConnectionGitHub)
	}
	if len(cfg.Participants) > 0 {
		cfg.Daemon.DriveSources = appendCarrier(cfg.Daemon.DriveSources, DriveManagedLocal)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, false, err
	}
	return cfg, found, nil
}

func bridgeMulticaRegistry(cfg *Config, reg multicasurface.MulticaRegistry) error {
	cfg.Connections.Multica.Enabled = true
	if strings.TrimSpace(cfg.Connections.Multica.Workspace) == "" {
		cfg.Connections.Multica.Workspace = strings.TrimSpace(reg.WorkspaceID)
	}
	if strings.TrimSpace(cfg.Connections.Multica.RuntimeBinary) == "" {
		cfg.Connections.Multica.RuntimeBinary = DefaultMulticaRuntimeBinary
	}
	cfg.Daemon.InteractionWatchers = appendCarrier(cfg.Daemon.InteractionWatchers, ConnectionMultica)
	cfg.Daemon.DisplaySurfaces = appendCarrier(cfg.Daemon.DisplaySurfaces, ConnectionMultica)
	if strings.TrimSpace(cfg.Sessions.PrimaryActivationCarrier) == "" {
		cfg.Sessions.PrimaryActivationCarrier = ConnectionMultica
	}
	for _, record := range reg.Participants {
		participant, err := participantFromMulticaRegistryRecord(record)
		if err != nil {
			return err
		}
		cfg.Participants = mergeParticipant(cfg.Participants, participant)
	}
	if len(cfg.Participants) > 0 {
		cfg.Daemon.DriveSources = appendCarrier(cfg.Daemon.DriveSources, DriveManagedLocal)
	}
	return nil
}

func participantFromMulticaRegistryRecord(record multicasurface.MulticaParticipantRecord) (Participant, error) {
	principal, err := participantrole.RequirePrincipal("multica registry participant", record.Principal)
	if err != nil {
		return Participant{}, err
	}
	displayName := strings.TrimSpace(record.AgentName)
	if displayName == "" {
		displayName = strings.TrimSpace(record.AgentID)
	}
	return Participant{
		Principal:   principal,
		DisplayName: displayName,
		Role:        strings.TrimSpace(record.Role),
		HostRuntime: HostRuntime{
			Kind: RuntimeKindCodex,
			Mode: RuntimeModeManagedOrHost,
		},
	}, nil
}

func mergeParticipant(participants []Participant, next Participant) []Participant {
	return participantrole.UpsertByPrincipal(participants, next, func(participant Participant) string {
		return participant.Principal
	}, func(existing, next Participant) Participant {
		if strings.TrimSpace(existing.DisplayName) == "" {
			existing.DisplayName = next.DisplayName
		}
		if strings.TrimSpace(existing.Role) == "" {
			existing.Role = next.Role
		}
		if strings.TrimSpace(existing.HostRuntime.Kind) == "" {
			existing.HostRuntime.Kind = next.HostRuntime.Kind
		}
		if strings.TrimSpace(existing.HostRuntime.Mode) == "" {
			existing.HostRuntime.Mode = next.HostRuntime.Mode
		}
		return existing
	})
}

func loadLegacyLocal(root string) (legacyLocalConfig, bool, error) {
	path := filepath.Join(root, ".mnemon", "harness", "local", "config.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return legacyLocalConfig{}, false, nil
	}
	if err != nil {
		return legacyLocalConfig{}, false, err
	}
	var cfg legacyLocalConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return legacyLocalConfig{}, false, fmt.Errorf("parse legacy local config: %w", err)
	}
	if cfg.SchemaVersion != 1 {
		return legacyLocalConfig{}, false, fmt.Errorf("legacy local config schema_version %d unsupported (want 1)", cfg.SchemaVersion)
	}
	return cfg, true, nil
}

func loadLegacyRemotes(root string) (legacyRemotesDoc, bool, error) {
	path := filepath.Join(root, ".mnemon", "harness", "sync", "remotes.json")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return legacyRemotesDoc{}, false, nil
	}
	if err != nil {
		return legacyRemotesDoc{}, false, err
	}
	var doc legacyRemotesDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return legacyRemotesDoc{}, false, fmt.Errorf("parse legacy remote config: %w", err)
	}
	if doc.SchemaVersion != 1 {
		return legacyRemotesDoc{}, false, fmt.Errorf("legacy remote config schema_version %d unsupported (want 1)", doc.SchemaVersion)
	}
	return doc, true, nil
}

func loadLegacyMulticaRegistry(root string) (multicasurface.MulticaRegistry, bool, error) {
	return multicasurface.LoadMulticaRegistry(multicasurface.MulticaRegistryPath(root, ""))
}

func appendCarrier(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}
