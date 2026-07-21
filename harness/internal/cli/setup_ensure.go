package cli

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

const (
	setupDaemonSettleLimit = time.Second
	setupDaemonSettlePoll  = 20 * time.Millisecond
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

func ensureSetupDaemon(ctx context.Context,
	options node.DaemonEnsureOptions,
) (node.DaemonEnsureResult, error) {
	return ensureSetupDaemonWith(ctx, options, node.EnsureDaemon, setupDaemonSettleLimit,
		setupDaemonSettlePoll)
}

func ensureSetupDaemonWith(ctx context.Context, options node.DaemonEnsureOptions,
	ensure func(context.Context, node.DaemonEnsureOptions) (node.DaemonEnsureResult, error),
	settleLimit, settlePoll time.Duration,
) (node.DaemonEnsureResult, error) {
	if ctx == nil {
		return node.DaemonEnsureResult{}, fmt.Errorf("%w: setup settle context is unavailable",
			node.ErrDaemonEnsure)
	}
	if ensure == nil || settleLimit <= 0 || settlePoll <= 0 || settlePoll > settleLimit {
		return node.DaemonEnsureResult{}, fmt.Errorf("%w: setup settle bounds are invalid",
			node.ErrDaemonEnsure)
	}
	deadline := time.Now().Add(settleLimit)
	for {
		result, err := ensure(ctx, options)
		if err == nil || !errors.Is(err, node.ErrDaemonHealthAuthority) ||
			time.Now().After(deadline) {
			return result, err
		}
		timer := time.NewTimer(settlePoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return node.DaemonEnsureResult{}, fmt.Errorf("%w: setup settle: %w",
				node.ErrDaemonEnsure, ctx.Err())
		case <-timer.C:
		}
	}
}
