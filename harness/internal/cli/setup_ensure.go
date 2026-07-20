package cli

import (
	"context"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi/nodecontrol"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

type setupAuthorityClient interface {
	nodecontrol.HealthClient
	nodecontrol.MutationShutdownClient
	ReadAuthority(context.Context) (localapi.AuthorityResponse, *localapi.APIError)
}

func newSetupDaemonPreflight(options node.DaemonPreflightOptions) (node.DaemonEnsurePreflight, error) {
	options.Credentials = nodecontrol.ProfileCredentials{}
	return node.NewDaemonPreflight(options)
}

func quiesceSetupAuthority(ctx context.Context, lease setupDaemonLifecycle, client setupAuthorityClient,
	companion setupCompanion, wire localapi.AuthorityResponse) *localapi.APIError {
	expected, err := nodecontrol.Authority(wire)
	if err != nil {
		return setupAuthError("managed authority is invalid")
	}
	quiesced, err := lease.Quiesce(ctx, nodecontrol.AdaptLifecycleClient(client), companion, expected)
	if err != nil {
		return setupLifecycleError(err)
	}
	if quiesced != expected {
		return setupAuthError("managed authority changed while stopping mnemond")
	}
	return nil
}

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
		Probe: nodecontrol.AdaptHealthClient(client), Preflight: preflight,
		Launcher: launcher, ReadyGate: gate}, nil
}
