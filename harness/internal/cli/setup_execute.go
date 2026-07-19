package cli

import (
	"context"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

func (app *setupApp) execute(ctx context.Context,
	request setupRequest,
) (setupReceipt, *localapi.APIError) {
	workspace, err := resolveSetupWorkspace(request.projectRoot, app.deps.workingDirectory)
	if err != nil {
		return setupReceipt{}, setupError(localapi.CodeInvalidArgument,
			"project root must be an existing physical directory")
	}
	bundle, err := app.deps.loadBundle()
	if err != nil {
		return setupReceipt{}, setupAssetsError()
	}
	revision := bundle.Manifest().AssetRevision
	if _, err := model.ParseDigest(revision); err != nil {
		return setupReceipt{}, setupAssetsError()
	}
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	installation, err := integration.NewManagedInstallationFromBundle(workspace, bundle)
	if err != nil {
		return setupReceipt{}, setupAssetsError()
	}
	preflight, err := app.deps.newPreflight(node.DaemonPreflightOptions{
		Workspace: workspace, NodeState: nodeState, AssetRevision: revision, Install: installation,
	})
	if err != nil || preflight == nil {
		return setupReceipt{}, setupAssetsError()
	}
	companion, err := app.deps.newCompanion(ctx, workspace, app.version)
	if err != nil {
		return setupReceipt{}, setupUnavailableError("mnemond companion is unavailable")
	}
	preparedNodeState, err := app.deps.prepareNode(workspace)
	if err != nil || preparedNodeState != nodeState {
		return setupReceipt{}, setupAuthError("managed Node state is unsafe")
	}

	lock, err := app.deps.acquireLock(ctx, nodeState)
	if err != nil || lock == nil {
		return setupReceipt{}, setupUnavailableError("managed setup lock is unavailable")
	}
	receipt, apiErr := app.executeLocked(ctx, request, workspace, nodeState, revision,
		bundle, preflight, companion)
	if closeErr := lock.Close(); closeErr != nil {
		return setupReceipt{}, setupAuthError("managed setup lock changed during setup")
	}
	return receipt, apiErr
}
