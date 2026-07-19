package node

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

// ManagedRuntimeInstallation is the Node-owned view of one installed Agent
// Runtime. The provider resolves the executable at composition time and
// revalidates the projected Hook, Skill and Guide assets before every Run.
type ManagedRuntimeInstallation interface {
	InstallationVerifier
	RuntimeExecutable(context.Context, model.Profile) (string, error)
}

// NewManagedWakeAdapterFactory binds one canonical workspace to its installed
// Codex Runtime. The returned factory accepts only the exact durable Node and
// Profile authority supplied by OpenDaemon.
func NewManagedWakeAdapterFactory(workspace string,
	install ManagedRuntimeInstallation,
) (WakeAdapterFactory, error) {
	validatedWorkspace, err := validateDaemonWorkspace(workspace)
	if err != nil {
		return nil, fmt.Errorf("compose managed wake adapter: %w", err)
	}
	if isNilNodeInterface(install) {
		return nil, errors.New("compose managed wake adapter: installation is unavailable")
	}
	nodeState := filepath.Join(validatedWorkspace, ".mnemon", "harness", "node")
	return WakeAdapterFactoryFunc(func(ctx context.Context,
		options WakeAdapterFactoryOptions,
	) (agent.WakeWorkerAdapter, error) {
		if ctx == nil {
			return nil, errors.New("compose managed wake adapter: context is unavailable")
		}
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("compose managed wake adapter: %w", err)
		}
		if !managedWakeAuthorityMatches(validatedWorkspace, nodeState, options) {
			return nil, errors.New("compose managed wake adapter: durable authority differs")
		}
		executable, err := install.RuntimeExecutable(ctx, options.Profile)
		if err != nil {
			return nil, fmt.Errorf("resolve managed Runtime executable: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("compose managed wake adapter: %w", err)
		}
		adapter, err := agent.NewCodexWakeAdapter(agent.CodexWakeAdapterOptions{
			Executable:  executable,
			Workspace:   validatedWorkspace,
			Environment: managedWakeEnvironment(os.Environ()),
			VerifyProjection: func(runCtx context.Context) error {
				return install.Verify(runCtx, options.Profile)
			},
		})
		if err != nil {
			return nil, fmt.Errorf("compose managed Codex wake adapter: %w", err)
		}
		return adapter, nil
	}), nil
}

func managedWakeAuthorityMatches(workspace, nodeState string,
	options WakeAdapterFactoryOptions,
) bool {
	profile := options.Profile
	return options.Workspace == workspace && options.NodeState == nodeState &&
		profile.ID() == model.TeamworkProfileID() && profile.Enabled() &&
		profile.WorkspaceRoot() == workspace && profile.Host() == model.HostCodex &&
		profile.Runtime() == model.RuntimeCodexAppServer
}

// managedWakeEnvironment is the closed inheritance boundary for managed
// Runtime children. Provider credentials, Event data, launch permits and stale
// Run capabilities are intentionally excluded.
func managedWakeEnvironment(environment []string) []string {
	seen := make(map[string]struct{}, 16)
	result := make([]string, 0, 16)
	for _, entry := range environment {
		name, _, ok := strings.Cut(entry, "=")
		if !ok || !managedWakeEnvironmentName(name) {
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			continue
		}
		seen[name] = struct{}{}
		result = append(result, entry)
	}
	return result
}

func managedWakeEnvironmentName(name string) bool {
	if strings.HasPrefix(name, "LC_") {
		return true
	}
	switch name {
	case "CODEX_HOME", "HOME", "LANG", "LOGNAME", "PATH", "TEMP", "TMP", "TMPDIR", "USER",
		"XDG_CACHE_HOME", "XDG_CONFIG_HOME", "XDG_DATA_HOME":
		return true
	default:
		return false
	}
}
