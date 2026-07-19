package cli

import (
	"context"
	"errors"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func (app *setupApp) ensureOptions(workspace, nodeState, revision string,
	bundle assets.Bundle, host assets.Host, client setupAuthorityClient,
) (node.DaemonEnsureOptions, *localapi.APIError) {
	install := node.InstallationVerifierFunc(func(verifyCtx context.Context,
		profile model.Profile,
	) error {
		if profile.Host() != model.HostKind(host) ||
			profile.ActiveAssetRevision() != revision {
			return errors.New("active Profile differs from setup authority")
		}
		if err := app.deps.verifyProjection(workspace, nodeState, host, bundle); err != nil {
			return err
		}
		return verifyCtx.Err()
	})
	preflight, err := app.deps.newPreflight(node.DaemonPreflightOptions{
		Workspace: workspace, NodeState: nodeState, AssetRevision: revision, Install: install,
	})
	if err != nil {
		return node.DaemonEnsureOptions{}, setupAssetsError()
	}
	gate, err := app.deps.newHookGate(workspace, host)
	if err != nil {
		return node.DaemonEnsureOptions{}, setupAssetsError()
	}
	launcher := &lazyAgentDaemonLauncher{workspace: workspace, nodeState: nodeState,
		currentExecutable: app.deps.currentExecutable, newLauncher: app.deps.newLauncher}
	return node.DaemonEnsureOptions{NodeState: nodeState, AssetRevision: revision,
		Probe: client, Preflight: preflight, Launcher: launcher, ReadyGate: gate}, nil
}
