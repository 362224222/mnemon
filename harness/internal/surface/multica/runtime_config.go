package multica

import (
	"strings"
	"time"
)

const (
	MulticaRuntimeCommandName = "mnemon-multica-runtime"
	MulticaRuntimeProfileName = "mnemon-runtime"
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
	if workspace := RuntimeEnvValue(env, "MNEMON_MULTICA_PROVIDER_WORKSPACE"); workspace != "" {
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
