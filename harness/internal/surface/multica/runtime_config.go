package multica

import (
	"path/filepath"
	"strings"
	"time"
)

const (
	MulticaRuntimeCommandName = "mnemon-multica-runtime"
	MulticaRuntimeProfileName = "mnemon-runtime"
)

type RuntimeManagedWakeMaterial struct {
	IssueID      string
	RootIssueID  string
	AssignmentID string
	SessionID    string
}

func RuntimeEnvValue(env []string, key string) string {
	prefix := key + "="
	for i := len(env) - 1; i >= 0; i-- {
		item := env[i]
		if strings.HasPrefix(item, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(item, prefix))
		}
	}
	return ""
}

func RuntimeEnvDefault(env []string, key, fallback string) string {
	if value := RuntimeEnvValue(env, key); value != "" {
		return value
	}
	return fallback
}

func RuntimeTimeout(env []string) time.Duration {
	return runtimeDuration(env, []string{"MNEMON_MULTICA_RUNTIME_TIMEOUT", "MULTICA_HTTP_TIMEOUT"}, 30*time.Second)
}

func RuntimeManagedTurnTimeout(env []string) time.Duration {
	return runtimeDuration(env, []string{"MNEMON_MANAGED_TURN_TIMEOUT"}, 5*time.Minute)
}

func RuntimeHubProjectionInterval(env []string) time.Duration {
	return runtimeDuration(env, []string{"MNEMON_MULTICA_HUB_PROJECT_INTERVAL"}, 5*time.Second)
}

func RuntimeManagedLedgerPath(env []string, workspace string) string {
	if explicit := RuntimeEnvValue(env, "MNEMON_MANAGED_LEDGER"); explicit != "" {
		return explicit
	}
	root := strings.TrimSpace(workspace)
	if root == "" {
		root = "."
	}
	return filepath.Join(root, ".mnemon", "harness", "local", "managed-agent", "wake-ledger.jsonl")
}

func RuntimeMulticaHubLedgerPath(env []string, cwd string) string {
	if explicit := RuntimeEnvValue(env, "MNEMON_MULTICA_HUB_LEDGER"); explicit != "" {
		return MulticaHubLedgerPath("", explicit)
	}
	if workspace := RuntimeEnvValue(env, "MNEMON_MANAGED_WORKSPACE"); workspace != "" {
		return MulticaHubLedgerPath(workspace, "")
	}
	return MulticaHubLedgerPath(cwd, "")
}

func RuntimeMulticaRegistryPaths(env []string, cwd string) []string {
	paths := []string{}
	add := func(path string) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		for _, existing := range paths {
			if existing == path {
				return
			}
		}
		paths = append(paths, path)
	}
	if explicit := RuntimeEnvValue(env, "MNEMON_MULTICA_REGISTRY"); explicit != "" {
		add(explicit)
	}
	if workspace := RuntimeEnvValue(env, "MNEMON_MANAGED_WORKSPACE"); workspace != "" {
		add(MulticaRegistryPath(workspace, ""))
	}
	if strings.TrimSpace(cwd) != "" {
		add(MulticaRegistryPath(cwd, ""))
	}
	return paths
}

func RuntimeMulticaRegistry(env []string, cwd string) (MulticaRegistry, bool, error) {
	for _, path := range RuntimeMulticaRegistryPaths(env, cwd) {
		reg, ok, err := LoadMulticaRegistry(path)
		if err != nil || ok {
			return reg, ok, err
		}
	}
	return MulticaRegistry{}, false, nil
}

func RuntimeManagedWakeScopeID(material RuntimeManagedWakeMaterial) string {
	return firstNonEmptyRuntimeString(material.AssignmentID, material.RootIssueID, material.IssueID)
}

func RuntimeManagedTurnEnv(env []string, material RuntimeManagedWakeMaterial) []string {
	out := append([]string(nil), env...)
	add := func(key, value string) {
		value = strings.TrimSpace(value)
		if value == "" || RuntimeEnvValue(out, key) != "" {
			return
		}
		out = append(out, key+"="+value)
	}
	add("MNEMON_RENDER_HOST", "multica")
	add("MNEMON_RENDER_SESSION_ID", material.SessionID)
	add("MNEMON_RENDER_INPUT_ID", RuntimeManagedWakeScopeID(material))
	return out
}

func RuntimeProjectionCommentsEnabled(env []string) bool {
	value := strings.ToLower(RuntimeEnvDefault(env, "MNEMON_MULTICA_PROJECT_COMMENTS", "true"))
	switch value {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

func RuntimeHubWriteEnabled(env []string) bool {
	value := strings.TrimSpace(RuntimeEnvValue(env, "MNEMON_MULTICA_HUB_WRITE"))
	if value == "" {
		return true
	}
	switch strings.ToLower(value) {
	case "0", "false", "off", "disabled", "no":
		return false
	default:
		return true
	}
}

func firstNonEmptyRuntimeString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func runtimeDuration(env []string, keys []string, fallback time.Duration) time.Duration {
	raw := ""
	for _, key := range keys {
		raw = RuntimeEnvValue(env, key)
		if raw != "" {
			break
		}
	}
	if raw == "" {
		return fallback
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}
