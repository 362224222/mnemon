// localboot.go is the LOCAL boot face shared by the TWO local-trust-domain mains:
// `mnemon-harness local run` and the mnemond local governance daemon (P1 D13). Both resolve the
// same setup-written config, the same store path, the same endpoint-derived listen address, and
// the same T1 loopback floor — sunk here (pure move from cmd/mnemon-harness/local.go, behavior
// unchanged) so the daemon is a true alias of `local run`, never a drifting fork.
package app

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/channel"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

// LocalNotSetupMessage is the product remediation for a boot without setup artifacts.
const LocalNotSetupMessage = "Local Mnemon is not set up.\nRun: mnemon-harness setup --host codex"

// ErrLocalNotSetup is returned when no Local Mnemon config exists under the project root.
var ErrLocalNotSetup = errors.New(LocalNotSetupMessage)

// LocalBoot is the resolved boot state both local mains serve from.
type LocalBoot struct {
	Configured bool
	StorePath  string
	Loaded     channel.LoadedBindings
	Config     LocalConfig
}

// LocalConfig mirrors the setup-written .mnemon/harness/local/config.json document.
type LocalConfig struct {
	SchemaVersion int      `json:"schema_version"`
	Mode          string   `json:"mode"`
	Endpoint      string   `json:"endpoint"`
	Principal     string   `json:"principal"`
	Loops         []string `json:"loops"`
	BindingFile   string   `json:"binding_file"`
	StorePath     string   `json:"store_path"`
}

// ResolveLocalBoot resolves the boot state from the cleaned project root plus the two operator
// overrides: storePath (the --store flag; "" = config/default discovery) and bindingsPath (the
// hidden --bindings flag; "" = setup-config-driven discovery).
func ResolveLocalBoot(root, storePath, bindingsPath string) (LocalBoot, error) {
	if bindingsPath != "" {
		loaded, err := channel.LoadBindingFile(root, ResolveProjectPath(root, bindingsPath))
		if err != nil {
			return LocalBoot{}, err
		}
		return LocalBoot{Configured: true, StorePath: ResolveLocalStorePath(root, storePath), Loaded: loaded}, nil
	}
	cfg, err := ReadLocalConfig(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return LocalBoot{}, ErrLocalNotSetup
		}
		return LocalBoot{}, err
	}
	bindingPath := cfg.BindingFile
	if bindingPath == "" {
		bindingPath = channel.DefaultBindingFile
	}
	loaded, err := channel.LoadBindingFile(root, ResolveProjectPath(root, bindingPath))
	if err != nil {
		return LocalBoot{}, err
	}
	resolvedStore := ResolveLocalStorePath(root, storePath)
	if storePath == "" {
		if cfg.StorePath != "" {
			resolvedStore = ResolveProjectPath(root, cfg.StorePath)
		} else {
			resolvedStore = filepath.Join(root, runtime.DefaultStorePath)
		}
	}
	return LocalBoot{Configured: true, StorePath: resolvedStore, Loaded: loaded, Config: cfg}, nil
}

// ReadLocalConfig reads + validates the setup-written Local Mnemon config under root.
func ReadLocalConfig(root string) (LocalConfig, error) {
	path := filepath.Join(root, ".mnemon", "harness", "local", "config.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return LocalConfig{}, err
	}
	var cfg LocalConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return LocalConfig{}, fmt.Errorf("parse Local Mnemon config: %w", err)
	}
	if cfg.SchemaVersion != 1 {
		return LocalConfig{}, fmt.Errorf("Local Mnemon config schema_version %d unsupported (want 1)", cfg.SchemaVersion)
	}
	return cfg, nil
}

// ResolveLocalStorePath resolves the effective store path for a --store override ("" = the
// project-default store under root).
func ResolveLocalStorePath(root, storePath string) string {
	if storePath != "" {
		return ResolveProjectPath(root, storePath)
	}
	return filepath.Join(root, runtime.DefaultStorePath)
}

// ResolveProjectPath resolves path against the project root (absolute paths pass through cleaned).
func ResolveProjectPath(root, path string) string {
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(root, path)
}

// ValidateListenAddr fail-closes a non-loopback listen address unless explicitly allowed:
// the local control plane is a same-machine governance boundary (T1) — binding 0.0.0.0 or a
// LAN address silently exposes the channel beyond it.
func ValidateListenAddr(addr string, allowNonLoopback bool) error {
	if allowNonLoopback {
		return nil
	}
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	if host == "localhost" {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return fmt.Errorf("refusing non-loopback listen address %q (T1 loopback-only); pass --allow-nonloopback to override explicitly", addr)
}

// ListenAddrFromEndpoint derives the listen address from the setup-written channel endpoint
// (e.g. "http://127.0.0.1:9001" -> "127.0.0.1:9001"), so a bare `local run` listens where
// setup pointed the hooks/bindings. An empty/unparsable endpoint falls back to fallback.
func ListenAddrFromEndpoint(endpoint, fallback string) string {
	if endpoint == "" {
		return fallback
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return fallback
	}
	return u.Host
}

// RemoteWorkspaceStatus renders the one-line Remote Workspace banner both local mains (and the
// status command) print: "not connected" or "connected <remote id>" from remotes.json.
func RemoteWorkspaceStatus(projectRoot string) string {
	remote, ok := currentRemoteWorkspace(projectRoot)
	if !ok {
		return "not connected"
	}
	return "connected " + remote
}

func currentRemoteWorkspace(projectRoot string) (string, bool) {
	raw, err := os.ReadFile(filepath.Join(projectRoot, ".mnemon", "harness", "sync", "remotes.json"))
	if err != nil {
		return "", false
	}
	var doc exchange.RemotesDoc
	if err := json.Unmarshal(raw, &doc); err != nil || doc.SchemaVersion != 1 {
		return "", false
	}
	if strings.TrimSpace(doc.Current) != "" {
		return strings.TrimSpace(doc.Current), true
	}
	if len(doc.Remotes) == 1 && strings.TrimSpace(doc.Remotes[0].ID) != "" {
		return strings.TrimSpace(doc.Remotes[0].ID), true
	}
	return "", false
}
