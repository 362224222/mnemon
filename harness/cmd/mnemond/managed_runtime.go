package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func managedWakeAdapterFactory(workspace string,
	install node.InstallationVerifier,
) node.WakeAdapterFactory {
	return node.WakeAdapterFactoryFunc(func(ctx context.Context,
		options node.WakeAdapterFactoryOptions,
	) (agent.WakeWorkerAdapter, error) {
		if ctx == nil || install == nil || workspace == "" ||
			options.Workspace != workspace ||
			options.NodeState != filepath.Join(workspace, ".mnemon", "harness", "node") ||
			options.Profile.WorkspaceRoot() != workspace ||
			options.Profile.Host() != model.HostCodex ||
			options.Profile.Runtime() != model.RuntimeCodexAppServer {
			return nil, errors.New("managed Codex wake adapter authority is unavailable")
		}
		observation, err := integration.InspectHost(ctx, assets.HostCodex)
		if err != nil || observation.Host != assets.HostCodex || observation.Executable == "" {
			return nil, errors.New("managed Codex wake adapter preflight failed")
		}
		adapter, err := agent.NewCodexWakeAdapter(agent.CodexWakeAdapterOptions{
			Executable: observation.Executable, Workspace: workspace,
			Environment: managedWakeEnvironment(os.Environ()),
			VerifyProjection: func(runCtx context.Context) error {
				return install.Verify(runCtx, options.Profile)
			},
		})
		if err != nil {
			return nil, fmt.Errorf("compose managed Codex wake adapter: %w", err)
		}
		return adapter, nil
	})
}

// managedWakeEnvironment is a second closed boundary below the detached
// mnemond environment. Runtime children inherit only Host location, locale,
// home and temporary-directory configuration; ambient provider keys, Event
// data, launch permits and stale Run capabilities are excluded.
func managedWakeEnvironment(environment []string) []string {
	allowed := map[string]bool{
		"CODEX_HOME": true, "HOME": true, "LANG": true, "LOGNAME": true,
		"PATH": true, "TEMP": true, "TMP": true, "TMPDIR": true, "USER": true,
		"XDG_CACHE_HOME": true, "XDG_CONFIG_HOME": true, "XDG_DATA_HOME": true,
	}
	seen := make(map[string]struct{}, len(allowed)+4)
	result := make([]string, 0, len(allowed)+4)
	for _, entry := range environment {
		name, _, ok := strings.Cut(entry, "=")
		if !ok || !(allowed[name] || strings.HasPrefix(name, "LC_")) {
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

func activateManagedNode(ctx context.Context, options node.ActivateOptions) (node.ActivateResult, error) {
	verify, err := managedInstallationVerifier(options.Workspace)
	if err != nil {
		return node.ActivateResult{}, err
	}
	options.Install = verify
	return node.Activate(ctx, options)
}

func managedInstallationVerifier(workspace string) (node.InstallationVerifier, error) {
	bundle, err := assets.Load()
	if err != nil {
		return nil, fmt.Errorf("load canonical managed assets: %w", err)
	}
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	return node.InstallationVerifierFunc(func(ctx context.Context, profile model.Profile) error {
		if ctx == nil {
			return errors.New("managed installation verification context is unavailable")
		}
		host := assets.Host(profile.Host())
		if !host.Valid() || profile.ActiveAssetRevision() != bundle.Manifest().AssetRevision {
			return errors.New("active Profile does not select this canonical asset bundle")
		}
		if err := integration.VerifyNodeBundle(nodeState, bundle); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := integration.VerifyHostProjection(workspace, nodeState, host, bundle); err != nil {
			return err
		}
		return ctx.Err()
	}), nil
}
