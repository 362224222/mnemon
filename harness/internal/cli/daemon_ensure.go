package cli

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

type agentDaemonEnsureDependencies struct {
	loadBundle        func() (assets.Bundle, error)
	currentExecutable func() (string, error)
	newPreflight      func(node.DaemonPreflightOptions) (node.DaemonEnsurePreflight, error)
	newLauncher       func(node.DaemonProcessOptions) (node.DaemonLauncher, error)
	ensure            func(context.Context, node.DaemonEnsureOptions) (node.DaemonEnsureResult, error)
}

func productionAgentDaemonEnsureDependencies() agentDaemonEnsureDependencies {
	return agentDaemonEnsureDependencies{
		loadBundle:        assets.Load,
		currentExecutable: os.Executable,
		newPreflight: func(options node.DaemonPreflightOptions) (node.DaemonEnsurePreflight, error) {
			return node.NewDaemonPreflight(options)
		},
		newLauncher: func(options node.DaemonProcessOptions) (node.DaemonLauncher, error) {
			return node.NewDaemonProcessLauncher(options)
		},
		ensure: node.EnsureDaemon,
	}
}

// ensureAgentDaemon is shared by every hidden Agent command, including the
// projected Hook. A healthy daemon takes the EnsureDaemon fast path without
// resolving or validating a launch executable; companion discovery is lazy
// and occurs only after authenticated unavailability and strict preflight.
func ensureAgentDaemon(ctx context.Context, workspace, nodeState string,
	client daemonHealthClient,
) *localapi.APIError {
	return ensureAgentDaemonWith(ctx, workspace, nodeState, client,
		productionAgentDaemonEnsureDependencies())
}

// EnsureAgentDaemon is the narrow process-lifecycle seam used by the neutral
// R7 Agency terminal. It deliberately exposes no R5 Profile, Work, Channel,
// or Teamwork state; those remain behind the legacy control client.
func EnsureAgentDaemon(ctx context.Context, workspace, nodeState string) *localapi.APIError {
	client, err := localapi.NewClient(nodeState)
	if err != nil {
		if errors.Is(err, localapi.ErrUnsafeClientState) {
			return localapi.NewAPIError(localapi.CodeAssetRevisionMismatch,
				"managed Node authority or installed assets are invalid")
		}
		return localapi.NewAPIError(localapi.CodeMnemondUnavailable,
			"mnemond local control is unavailable")
	}
	return ensureAgentDaemon(ctx, workspace, nodeState, client)
}

func ensureAgentDaemonWith(ctx context.Context, workspace, nodeState string,
	client daemonHealthClient, dependencies agentDaemonEnsureDependencies,
) *localapi.APIError {
	if ctx == nil || client == nil || dependencies.loadBundle == nil ||
		dependencies.currentExecutable == nil || dependencies.newPreflight == nil ||
		dependencies.newLauncher == nil || dependencies.ensure == nil {
		return localapi.NewAPIError(localapi.CodeInternal,
			"managed daemon ensure dependencies are unavailable")
	}
	bundle, err := dependencies.loadBundle()
	if err != nil {
		return localapi.NewAPIError(localapi.CodeInternal,
			"canonical managed assets are unavailable")
	}
	install, err := integration.NewManagedInstallationFromBundle(workspace, bundle)
	if err != nil {
		return localapi.NewAPIError(localapi.CodeInternal,
			"canonical managed assets are unavailable")
	}
	revision := install.Revision()
	preflight, err := dependencies.newPreflight(node.DaemonPreflightOptions{
		Workspace: workspace, NodeState: nodeState, AssetRevision: revision, Install: install,
		Credentials: localapi.NodeRuntime{},
	})
	if err != nil {
		return daemonEnsureError(err)
	}
	launcher := &lazyAgentDaemonLauncher{workspace: workspace, nodeState: nodeState,
		currentExecutable: dependencies.currentExecutable, newLauncher: dependencies.newLauncher}
	_, err = dependencies.ensure(ctx, node.DaemonEnsureOptions{NodeState: nodeState,
		AssetRevision: revision, Probe: client, Preflight: preflight, Launcher: launcher})
	if err != nil {
		return daemonEnsureError(err)
	}
	return nil
}

type lazyAgentDaemonLauncher struct {
	workspace         string
	nodeState         string
	currentExecutable func() (string, error)
	newLauncher       func(node.DaemonProcessOptions) (node.DaemonLauncher, error)
}

func (launcher *lazyAgentDaemonLauncher) Launch(ctx context.Context,
	permit node.DaemonLaunchPermit,
) (node.DaemonLaunch, error) {
	if launcher == nil || ctx == nil || launcher.currentExecutable == nil || launcher.newLauncher == nil {
		return nil, errors.New("managed daemon launcher is unavailable")
	}
	executable, err := launcher.currentExecutable()
	if err != nil {
		return nil, err
	}
	companion, err := resolveMnemondCompanion(executable)
	if err != nil {
		return nil, err
	}
	production, err := launcher.newLauncher(node.DaemonProcessOptions{Executable: companion,
		Workspace: launcher.workspace, NodeState: launcher.nodeState})
	if err != nil {
		return nil, err
	}
	return production.Launch(ctx, permit)
}

func resolveMnemondCompanion(currentExecutable string) (string, error) {
	if currentExecutable == "" || !filepath.IsAbs(currentExecutable) ||
		filepath.Clean(currentExecutable) != currentExecutable {
		return "", errors.New("mnemon-harness executable path is not absolute and canonical")
	}
	physicalCurrent, err := filepath.EvalSymlinks(currentExecutable)
	if err != nil {
		return "", errors.New("mnemon-harness executable path is unavailable")
	}
	companion := filepath.Join(filepath.Dir(physicalCurrent), "mnemond")
	physicalCompanion, err := filepath.EvalSymlinks(companion)
	if err != nil || !filepath.IsAbs(physicalCompanion) ||
		filepath.Clean(physicalCompanion) != physicalCompanion || physicalCompanion != companion {
		return "", errors.New("mnemond companion is unavailable beside mnemon-harness")
	}
	return physicalCompanion, nil
}

func daemonEnsureError(err error) *localapi.APIError {
	var apiErr *localapi.APIError
	if errors.As(err, &apiErr) && (apiErr.Code == localapi.CodeAuthenticationFailed ||
		apiErr.Code == localapi.CodeAssetRevisionMismatch) {
		return localapi.NewAPIError(apiErr.Code, apiErr.Message)
	}
	if errors.Is(err, node.ErrDaemonPreflight) || errors.Is(err, node.ErrDaemonAuthority) ||
		errors.Is(err, node.ErrDaemonHealthAuthority) ||
		errors.Is(err, localapi.ErrUnsafeClientState) {
		return localapi.NewAPIError(localapi.CodeAssetRevisionMismatch,
			"managed Node authority or installed assets are invalid")
	}
	return localapi.NewAPIError(localapi.CodeMnemondUnavailable,
		"mnemond could not be made ready")
}
