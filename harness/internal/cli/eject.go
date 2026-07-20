package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

type ejectCompanion interface {
	node.DaemonOfflineConfirmer
	Inspect(context.Context) (localapi.AuthorityResponse, error)
	Deactivate(context.Context, model.HostKind, string, time.Time) (
		companionLifecycleReceipt, error,
	)
}

type ejectAuthorityClient interface {
	node.DaemonLifecycleClient
	ReadAuthority(context.Context) (localapi.AuthorityResponse, *localapi.APIError)
}

type ejectDependencies struct {
	workingDirectory func() (string, error)
	loadBundle       func() (assets.Bundle, error)
	newCompanion     func(context.Context, string, string) (ejectCompanion, error)
	acquireLock      func(context.Context, string) (io.Closer, error)
	newClient        func(string) (ejectAuthorityClient, error)
	installBundle    func(string, assets.Bundle) error
	ejectProjection  func(string, string, assets.Host,
		assets.Bundle) (integration.EjectHostProjectionReceipt, error)
	verifyAbsent     func(string, string, assets.Host, assets.Bundle) error
	acquireLifecycle func(context.Context,
		node.DaemonLifecycleOptions) (setupDaemonLifecycle, error)
}

type ejectApp struct {
	stdout  io.Writer
	stderr  io.Writer
	version string
	deps    ejectDependencies
}

type ejectRequest struct {
	host        string
	projectRoot string
}

type ejectReceipt struct {
	AssetRevision       string `json:"asset_revision"`
	Host                string `json:"host"`
	PeerID              string `json:"peer_id"`
	RegistrationRemoved bool   `json:"registration_removed"`
	RemovedFiles        int    `json:"removed_files"`
	Replayed            bool   `json:"replayed"`
	SchemaVersion       int    `json:"schema_version"`
	Status              string `json:"status"`
}

func productionEjectDependencies() ejectDependencies {
	return ejectDependencies{
		workingDirectory: os.Getwd,
		loadBundle:       assets.Load,
		newCompanion: func(ctx context.Context, workspace, version string) (
			ejectCompanion, error,
		) {
			return newCompanionRunner(ctx, workspace, version)
		},
		acquireLock: func(ctx context.Context, nodeState string) (io.Closer, error) {
			return acquireSetupLock(ctx, nodeState)
		},
		newClient: func(nodeState string) (ejectAuthorityClient, error) {
			return localapi.NewClient(nodeState)
		},
		installBundle: func(nodeState string, bundle assets.Bundle) error {
			_, err := integration.InstallNodeBundle(nodeState, bundle)
			return err
		},
		ejectProjection: integration.EjectHostProjection,
		verifyAbsent:    integration.VerifyHostProjectionAbsent,
		acquireLifecycle: func(ctx context.Context,
			options node.DaemonLifecycleOptions,
		) (setupDaemonLifecycle, error) {
			return node.AcquireDaemonLifecycle(ctx, options)
		},
	}
}

// RunEject is the public lifecycle removal boundary. It withdraws managed
// Agent authority before touching Host assets, retains the durable Node and
// reports only a bounded non-secret receipt.
func RunEject(ctx context.Context, args []string, stdout, stderr io.Writer,
	version string,
) int {
	app := &ejectApp{stdout: stdout, stderr: stderr, version: version,
		deps: productionEjectDependencies()}
	return app.run(ctx, args)
}

func (app *ejectApp) run(ctx context.Context, args []string) int {
	if app == nil || ctx == nil || app.stdout == nil || app.stderr == nil ||
		!validCompanionVersion(app.version) || !validEjectDependencies(app.deps) {
		return 1
	}
	request, apiErr := parseEjectRequest(args)
	if apiErr != nil {
		return app.writeError(apiErr)
	}
	receipt, apiErr := app.execute(ctx, request)
	if apiErr != nil {
		return app.writeError(apiErr)
	}
	raw, err := model.CanonicalMarshal(receipt)
	if err != nil {
		return 1
	}
	if _, err := app.stdout.Write(append(raw, '\n')); err != nil {
		return 1
	}
	return 0
}

func (app *ejectApp) execute(ctx context.Context,
	request ejectRequest,
) (ejectReceipt, *localapi.APIError) {
	workspace, err := resolveSetupWorkspace(request.projectRoot, app.deps.workingDirectory)
	if err != nil {
		return ejectReceipt{}, setupError(localapi.CodeInvalidArgument,
			"project root must be an existing physical directory")
	}
	bundle, err := app.deps.loadBundle()
	if err != nil {
		return ejectReceipt{}, setupAssetsError()
	}
	revision := bundle.Manifest().AssetRevision
	if _, err := model.ParseDigest(revision); err != nil {
		return ejectReceipt{}, setupAssetsError()
	}
	companion, err := app.deps.newCompanion(ctx, workspace, app.version)
	if err != nil {
		return ejectReceipt{}, setupUnavailableError("mnemond companion is unavailable")
	}
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	lock, err := app.deps.acquireLock(ctx, nodeState)
	if err != nil || lock == nil {
		return ejectReceipt{}, setupAuthError("managed Node is not initialized or is unsafe")
	}
	receipt, apiErr := app.executeLocked(ctx, request, workspace, nodeState, bundle,
		companion)
	if closeErr := lock.Close(); closeErr != nil {
		return ejectReceipt{}, setupAuthError("managed setup lock changed during eject")
	}
	return receipt, apiErr
}

func (app *ejectApp) executeLocked(ctx context.Context, request ejectRequest,
	workspace, nodeState string, bundle assets.Bundle, companion ejectCompanion,
) (ejectReceipt, *localapi.APIError) {
	authority, client, apiErr := app.observeAuthority(ctx, nodeState, companion)
	if apiErr != nil {
		return ejectReceipt{}, apiErr
	}
	authorityUpdatedAt, err := parseSetupAuthorityTime(authority.UpdatedAt)
	if err != nil {
		return ejectReceipt{}, setupAuthError("managed Node authority generation is invalid")
	}
	host := assets.Host(authority.Host)
	if !host.Valid() {
		return ejectReceipt{}, setupAuthError("managed Node authority is invalid")
	}
	if request.host != "auto" && assets.Host(request.host) != host {
		return ejectReceipt{}, setupError(localapi.CodeProfileHostMismatch,
			"managed Profile is bound to another Host")
	}
	if err := app.deps.installBundle(nodeState, bundle); err != nil {
		return ejectReceipt{}, setupAssetsError()
	}
	lease, err := app.deps.acquireLifecycle(ctx, node.DaemonLifecycleOptions{
		Workspace: workspace, NodeState: nodeState,
	})
	if err != nil || lease == nil {
		return ejectReceipt{}, setupUnavailableError("managed daemon lifecycle is unavailable")
	}
	receipt, apiErr := app.executeLeased(ctx, workspace, nodeState, bundle, host,
		authority, authorityUpdatedAt, client, companion, lease)
	if closeErr := lease.Close(); closeErr != nil {
		return ejectReceipt{}, setupAuthError("managed daemon lifecycle changed during eject")
	}
	return receipt, apiErr
}

func (app *ejectApp) executeLeased(ctx context.Context, workspace, nodeState string,
	bundle assets.Bundle, host assets.Host, authority localapi.AuthorityResponse,
	authorityUpdatedAt time.Time, client ejectAuthorityClient, companion ejectCompanion,
	lease setupDaemonLifecycle,
) (ejectReceipt, *localapi.APIError) {
	quiesced, err := lease.Quiesce(ctx, client, companion, authority)
	if err != nil {
		return ejectReceipt{}, ejectLifecycleError(err)
	}
	if quiesced != authority {
		return ejectReceipt{}, setupAuthError("managed authority changed while stopping mnemond")
	}
	if apiErr := deactivateEjectAuthority(ctx, companion, host, authority, authorityUpdatedAt); apiErr != nil {
		return ejectReceipt{}, apiErr
	}
	projection, err := app.deps.ejectProjection(workspace, nodeState, host, bundle)
	if err != nil {
		if errors.Is(err, integration.ErrProjectionConflict) ||
			errors.Is(err, integration.ErrUnsafeProjection) {
			return ejectReceipt{}, setupError(localapi.CodeAssetRevisionMismatch,
				"managed Agent is disabled, but changed Host assets were preserved; run doctor before switching Host")
		}
		return ejectReceipt{}, setupError(localapi.CodeInternal,
			"managed Host projection could not be ejected safely")
	}
	wantProjectionRevision := authority.AssetRevision
	if projection.Replayed {
		wantProjectionRevision = bundle.Manifest().AssetRevision
	}
	if projection.Host != host || projection.Revision != wantProjectionRevision ||
		projection.RemovedFiles < 0 || projection.RemovedFiles > 3 ||
		projection.Replayed && (projection.RemovedFiles != 0 || projection.RegistrationRemoved) {
		return ejectReceipt{}, setupError(localapi.CodeInternal,
			"ejected Host projection differed from managed authority")
	}
	if err := app.deps.verifyAbsent(workspace, nodeState, host, bundle); err != nil {
		return ejectReceipt{}, setupError(localapi.CodeAssetRevisionMismatch,
			"managed Agent is disabled, but Host projection absence could not be proved; run doctor before switching Host")
	}
	return ejectReceipt{AssetRevision: authority.AssetRevision, Host: string(host),
		PeerID: authority.PeerID, RegistrationRemoved: projection.RegistrationRemoved,
		RemovedFiles:  projection.RemovedFiles,
		Replayed:      !authority.Enabled && projection.Replayed,
		SchemaVersion: localapi.SchemaVersion, Status: "ejected"}, nil
}

func deactivateEjectAuthority(ctx context.Context, companion ejectCompanion, host assets.Host,
	authority localapi.AuthorityResponse, authorityUpdatedAt time.Time,
) *localapi.APIError {
	if !authority.Enabled {
		return nil
	}
	deactivated, err := companion.Deactivate(ctx, model.HostKind(host),
		authority.AssetRevision, authorityUpdatedAt)
	if err != nil {
		return ejectLifecycleError(err)
	}
	deactivatedAt, err := parseSetupAuthorityTime(deactivated.UpdatedAt)
	if !deactivated.Changed || err != nil || !deactivatedAt.After(authorityUpdatedAt) {
		return setupError(localapi.CodeInternal,
			"managed eject returned an invalid deactivation generation")
	}
	return nil
}

func ejectLifecycleError(err error) *localapi.APIError {
	var apiErr *localapi.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.Code {
		case localapi.CodeOperationPending:
			return setupError(localapi.CodeOperationPending,
				"managed Profile has active Agent work; retry eject after it becomes idle")
		case localapi.CodeOperationMismatch:
			return setupError(localapi.CodeOperationMismatch,
				"managed Profile authority changed; rerun eject")
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, node.ErrDaemonLifecycle) || errors.Is(err, node.ErrOfflineAuthorityActive) {
		return setupUnavailableError("managed daemon could not be stopped safely")
	}
	return setupError(localapi.CodeInternal, "managed eject could not be completed safely")
}

func (app *ejectApp) observeAuthority(ctx context.Context, nodeState string,
	companion ejectCompanion,
) (localapi.AuthorityResponse, ejectAuthorityClient, *localapi.APIError) {
	client, err := app.deps.newClient(nodeState)
	if err != nil {
		return localapi.AuthorityResponse{}, nil,
			setupAuthError("managed Profile credential is unavailable")
	}
	authority, apiErr := client.ReadAuthority(ctx)
	if apiErr == nil {
		return authority, client, nil
	}
	if apiErr.Code != localapi.CodeMnemondUnavailable {
		return localapi.AuthorityResponse{}, nil, normalizeSetupAPIError(apiErr)
	}
	authority, err = companion.Inspect(ctx)
	if err != nil {
		return localapi.AuthorityResponse{}, nil,
			setupAuthError("managed Node authority could not be inspected")
	}
	return authority, client, nil
}

func parseEjectRequest(args []string) (ejectRequest, *localapi.APIError) {
	request := ejectRequest{host: "auto"}
	seen := make(map[string]bool)
	for index := 0; index < len(args); index++ {
		flag := args[index]
		if flag != "--host" && flag != "--project-root" {
			return ejectRequest{}, setupError(localapi.CodeInvalidArgument,
				"eject accepts only --host and --project-root")
		}
		if seen[flag] || index+1 >= len(args) || args[index+1] == "" ||
			strings.HasPrefix(args[index+1], "--") {
			return ejectRequest{}, setupError(localapi.CodeInvalidArgument,
				flag+" requires exactly one value")
		}
		seen[flag] = true
		index++
		switch flag {
		case "--host":
			request.host = args[index]
		case "--project-root":
			request.projectRoot = args[index]
		}
	}
	if request.host != "auto" && request.host != string(assets.HostCodex) &&
		request.host != string(assets.HostClaudeCode) {
		return ejectRequest{}, setupError(localapi.CodeInvalidArgument,
			"--host must be auto, codex, or claude-code")
	}
	return request, nil
}

func validEjectDependencies(dependencies ejectDependencies) bool {
	return dependencies.workingDirectory != nil && dependencies.loadBundle != nil &&
		dependencies.newCompanion != nil && dependencies.acquireLock != nil &&
		dependencies.newClient != nil && dependencies.installBundle != nil &&
		dependencies.ejectProjection != nil && dependencies.verifyAbsent != nil &&
		dependencies.acquireLifecycle != nil
}

func (app *ejectApp) writeError(apiErr *localapi.APIError) int {
	if apiErr == nil {
		apiErr = setupError(localapi.CodeInternal, "internal eject error")
	}
	if _, err := fmt.Fprintf(app.stderr, "%s: %s\n", apiErr.Code, apiErr.Message); err != nil {
		return 1
	}
	return apiErr.ExitStatus()
}
