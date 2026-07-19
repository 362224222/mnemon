package cli

import (
	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func (app *setupApp) ensureOptions(workspace, nodeState, revision string,
	preflight node.DaemonEnsurePreflight, host assets.Host, client setupAuthorityClient,
) (node.DaemonEnsureOptions, *localapi.APIError) {
	if preflight == nil {
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
