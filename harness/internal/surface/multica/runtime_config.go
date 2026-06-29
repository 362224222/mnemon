package multica

import (
	"path/filepath"
	"strings"
	"time"
)

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
