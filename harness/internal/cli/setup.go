package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/integration"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/node"
)

// RunSetup is the complete public setup boundary. The command package passes
// only argv, process streams and its build version; all authority and setup
// state remains owned by the harness packages and the mnemond companion.
func RunSetup(ctx context.Context, args []string, stdout, stderr io.Writer, version string) int {
	app := &setupApp{stdout: stdout, stderr: stderr, version: version,
		deps: productionSetupDependencies()}
	return app.run(ctx, args)
}

func (app *setupApp) run(ctx context.Context, args []string) int {
	if app == nil || ctx == nil || app.stdout == nil || app.stderr == nil ||
		!validCompanionVersion(app.version) || !validSetupDependencies(app.deps) {
		return 1
	}
	request, apiErr := parseSetupRequest(args)
	if apiErr != nil {
		return app.writeSetupError(apiErr)
	}
	receipt, apiErr := app.execute(ctx, request)
	if apiErr != nil {
		return app.writeSetupError(apiErr)
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

func (app *setupApp) executeLocked(ctx context.Context, request setupRequest,
	workspace, nodeState, revision string, bundle assets.Bundle, preflight node.DaemonEnsurePreflight, companion setupCompanion,
) (setupReceipt, *localapi.APIError) {
	observed := app.observeAuthority(ctx, nodeState, companion)
	if observed.terminal != nil {
		return setupReceipt{}, observed.terminal
	}
	if !observed.found {
		allowed, err := app.deps.canInitialize(nodeState)
		if err != nil || !allowed {
			if observed.fallback != nil {
				return setupReceipt{}, observed.fallback
			}
			return setupReceipt{}, setupAuthError("managed Node authority is unavailable")
		}
		selected, err := app.deps.detectHost(ctx, request.host)
		if err != nil || !selected.Host.Valid() {
			return setupReceipt{}, setupUnavailableError("no requested Host adapter passed preflight")
		}
		if !app.deps.activationSupported(selected) {
			return setupReceipt{}, setupUnsupportedActivationError()
		}
		if _, err := companion.Initialize(ctx, model.HostKind(selected.Host), revision); err != nil {
			return setupReceipt{}, setupAuthError("managed Node initialization failed")
		}
		authority, err := companion.Inspect(ctx)
		if err != nil {
			return setupReceipt{}, setupAuthError("managed Node authority could not be inspected")
		}
		observed = setupAuthorityObservation{authority: authority, found: true}
	}

	authority := observed.authority
	authorityUpdatedAt, err := parseSetupAuthorityTime(authority.UpdatedAt)
	if err != nil {
		return setupReceipt{}, setupAuthError("managed Node authority generation is invalid")
	}
	durableHost := assets.Host(authority.Host)
	if !durableHost.Valid() {
		return setupReceipt{}, setupAuthError("managed Node authority is invalid")
	}
	targetHost := durableHost
	if request.host != "auto" {
		targetHost = assets.Host(request.host)
	}
	if targetHost != durableHost && authority.Enabled {
		return setupReceipt{}, setupError(localapi.CodeProfileHostMismatch,
			"managed Profile is bound to another Host; eject is required before switching")
	}
	hostObservation, err := app.deps.inspectHost(ctx, targetHost)
	if err != nil {
		return setupReceipt{}, setupUnavailableError("selected Host adapter failed preflight")
	}
	if !app.deps.activationSupported(hostObservation) {
		return setupReceipt{}, setupUnsupportedActivationError()
	}
	if err := app.deps.installBundle(nodeState, bundle); err != nil {
		return setupReceipt{}, setupAssetsError()
	}
	if !authority.Enabled {
		otherHost, ok := otherSetupHost(targetHost)
		if !ok {
			return setupReceipt{}, setupAuthError("managed Host selection is invalid")
		}
		if err := app.deps.verifyAbsent(workspace, nodeState, otherHost, bundle); err != nil {
			if targetHost != durableHost {
				return setupReceipt{}, setupError(localapi.CodeProfileHostMismatch,
					"previous Host projection is not fully ejected; complete eject before switching")
			}
			return setupReceipt{}, setupError(localapi.CodeProfileHostMismatch,
				"another managed Host projection remains; select that Host explicitly to resume switching")
		}
	}
	if authority.Enabled && authority.AssetRevision != revision {
		return app.upgradeActive(ctx, workspace, nodeState, revision, bundle, preflight, targetHost,
			hostObservation, authority, authorityUpdatedAt, observed.client, companion)
	}
	if err := app.deps.installProjection(workspace, nodeState, targetHost, bundle); err != nil {
		return setupReceipt{}, setupAssetsError()
	}
	if err := app.deps.verifyProjection(workspace, nodeState, targetHost, bundle); err != nil {
		return setupReceipt{}, setupAssetsError()
	}
	if err := app.deps.verifyActivation(ctx, workspace, nodeState, hostObservation,
		bundle); err != nil {
		return setupReceipt{}, setupHostActivationError(err)
	}

	client := observed.client
	if client == nil {
		var err error
		client, err = app.deps.newClient(nodeState)
		if err != nil {
			return setupReceipt{}, setupAuthError("managed Profile credential is unavailable")
		}
	}
	ensureOptions, apiErr := app.ensureOptions(workspace, nodeState, revision, preflight,
		targetHost, client)
	if apiErr != nil {
		return setupReceipt{}, apiErr
	}

	activationChanged := false
	activationUpdatedAt := authorityUpdatedAt
	if !authority.Enabled {
		activated, err := companion.Activate(ctx, model.HostKind(targetHost), revision,
			authorityUpdatedAt)
		if err != nil {
			return setupReceipt{}, setupAssetsError()
		}
		if !activated.Changed {
			return setupReceipt{}, setupError(localapi.CodeInternal,
				"managed activation did not publish the requested authority")
		}
		activationUpdatedAt, err = parseSetupAuthorityTime(activated.UpdatedAt)
		if err != nil || !activationUpdatedAt.After(authorityUpdatedAt) {
			return setupReceipt{}, setupError(localapi.CodeInternal,
				"managed activation returned an invalid authority generation")
		}
		activationChanged = true
	}
	ensured, err := app.deps.ensure(ctx, ensureOptions)
	if err != nil {
		return setupReceipt{}, app.rollbackActivation(ctx, companion, targetHost, revision,
			activationUpdatedAt, activationChanged, ensured.FailureOutcome, setupEnsureError(err))
	}
	return setupReceipt{AssetRevision: revision, Host: string(targetHost), PeerID: authority.PeerID,
		Replayed: authority.Enabled, SchemaVersion: localapi.SchemaVersion, Started: ensured.Started,
		Status: "ready"}, nil
}

func otherSetupHost(host assets.Host) (assets.Host, bool) {
	switch host {
	case assets.HostCodex:
		return assets.HostClaudeCode, true
	case assets.HostClaudeCode:
		return assets.HostCodex, true
	default:
		return "", false
	}
}

func (app *setupApp) upgradeActive(ctx context.Context, workspace, nodeState,
	revision string, bundle assets.Bundle, daemonPreflight node.DaemonEnsurePreflight, host assets.Host,
	hostObservation integration.HostObservation,
	authority localapi.AuthorityResponse, authorityUpdatedAt time.Time,
	client setupAuthorityClient, companion setupCompanion,
) (setupReceipt, *localapi.APIError) {
	previousRevision := authority.AssetRevision
	preflight, err := app.deps.preflightUpgrade(workspace, nodeState, host,
		previousRevision, bundle)
	if err != nil || preflight.Host != host ||
		preflight.PreviousRevision != previousRevision || preflight.Revision != revision {
		return setupReceipt{}, setupAssetsError()
	}
	if client == nil {
		client, err = app.deps.newClient(nodeState)
		if err != nil {
			return setupReceipt{}, setupAuthError("managed Profile credential is unavailable")
		}
	}
	lease, err := app.deps.acquireLifecycle(ctx, node.DaemonLifecycleOptions{
		Workspace: workspace, NodeState: nodeState,
	})
	if err != nil || lease == nil {
		return setupReceipt{}, setupUnavailableError("managed daemon lifecycle is unavailable")
	}
	receipt, apiErr := app.upgradeActiveLeased(ctx, workspace, nodeState, revision,
		bundle, daemonPreflight, host, hostObservation, authority, authorityUpdatedAt, client, companion, lease)
	if closeErr := lease.Close(); closeErr != nil {
		return setupReceipt{}, setupAuthError("managed daemon lifecycle changed during setup")
	}
	return receipt, apiErr
}

func (app *setupApp) upgradeActiveLeased(ctx context.Context, workspace, nodeState,
	revision string, bundle assets.Bundle, daemonPreflight node.DaemonEnsurePreflight, host assets.Host,
	hostObservation integration.HostObservation,
	authority localapi.AuthorityResponse, authorityUpdatedAt time.Time,
	client setupAuthorityClient, companion setupCompanion, lease setupDaemonLifecycle,
) (setupReceipt, *localapi.APIError) {
	quiesced, err := lease.Quiesce(ctx, client, companion, authority)
	if err != nil {
		return setupReceipt{}, setupLifecycleError(err)
	}
	if quiesced != authority {
		return setupReceipt{}, setupAuthError("managed authority changed while stopping mnemond")
	}
	deactivated, err := companion.Deactivate(ctx, model.HostKind(host),
		authority.AssetRevision, authorityUpdatedAt)
	if err != nil {
		return setupReceipt{}, setupLifecycleError(err)
	}
	deactivatedAt, generationErr := parseSetupAuthorityTime(deactivated.UpdatedAt)
	if !deactivated.Changed || generationErr != nil ||
		!deactivatedAt.After(authorityUpdatedAt) {
		return setupReceipt{}, setupError(localapi.CodeInternal,
			"managed revision upgrade returned an invalid deactivation generation")
	}

	// From this point onward the projection journal is a forward-only recovery
	// authority. Setup must never restore the old shared Host registration or
	// reactivate the old revision after a partial failure.
	if err := app.deps.installProjection(workspace, nodeState, host, bundle); err != nil {
		return setupReceipt{}, setupAssetsError()
	}
	if err := app.deps.verifyProjection(workspace, nodeState, host, bundle); err != nil {
		return setupReceipt{}, setupAssetsError()
	}
	if err := app.deps.verifyActivation(ctx, workspace, nodeState, hostObservation,
		bundle); err != nil {
		return setupReceipt{}, setupHostActivationError(err)
	}
	ensureOptions, apiErr := app.ensureOptions(workspace, nodeState, revision, daemonPreflight,
		host, client)
	if apiErr != nil {
		return setupReceipt{}, apiErr
	}
	activated, err := companion.Activate(ctx, model.HostKind(host), revision, deactivatedAt)
	if err != nil {
		return setupReceipt{}, setupLifecycleError(err)
	}
	activatedAt, generationErr := parseSetupAuthorityTime(activated.UpdatedAt)
	if !activated.Changed || generationErr != nil || !activatedAt.After(deactivatedAt) {
		return setupReceipt{}, setupError(localapi.CodeInternal,
			"managed revision upgrade returned an invalid activation generation")
	}
	ensured, err := lease.Ensure(ctx, ensureOptions)
	if err != nil {
		return setupReceipt{}, setupEnsureError(err)
	}
	return setupReceipt{AssetRevision: revision, Host: string(host), PeerID: authority.PeerID,
		Replayed: false, SchemaVersion: localapi.SchemaVersion, Started: ensured.Started,
		Status: "ready"}, nil
}

func (app *setupApp) observeAuthority(ctx context.Context, nodeState string,
	companion setupCompanion,
) setupAuthorityObservation {
	client, err := app.deps.newClient(nodeState)
	if err != nil {
		return setupAuthorityObservation{fallback: setupAuthError(
			"managed Profile credential is unavailable")}
	}
	authority, apiErr := client.ReadAuthority(ctx)
	if apiErr == nil {
		return setupAuthorityObservation{authority: authority, client: client, found: true}
	}
	if apiErr.Code != localapi.CodeMnemondUnavailable {
		return setupAuthorityObservation{terminal: normalizeSetupAPIError(apiErr)}
	}
	authority, err = companion.Inspect(ctx)
	if err != nil {
		return setupAuthorityObservation{client: client,
			fallback: setupAuthError("managed Node authority could not be inspected")}
	}
	return setupAuthorityObservation{authority: authority, client: client, found: true}
}

func (app *setupApp) rollbackActivation(ctx context.Context, companion setupCompanion,
	host assets.Host, revision string, activationUpdatedAt time.Time, activationChanged bool,
	failureOutcome node.DaemonEnsureFailureOutcome,
	setupFailure *localapi.APIError,
) *localapi.APIError {
	if !activationChanged || !failureOutcome.AllowsCompensation() {
		return setupFailure
	}
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), companionCommandTimeout)
	defer cancel()
	deactivated, err := companion.Deactivate(rollbackCtx, model.HostKind(host), revision,
		activationUpdatedAt)
	deactivatedAt, generationErr := parseSetupAuthorityTime(deactivated.UpdatedAt)
	if err != nil || !deactivated.Changed || generationErr != nil ||
		!deactivatedAt.After(activationUpdatedAt) {
		return setupError(localapi.CodeInternal,
			"managed setup rollback could not be completed safely")
	}
	return setupFailure
}

func parseSetupAuthorityTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, err
	}
	canonical := parsed.Round(0).UTC()
	if canonical.IsZero() || canonical.UnixNano() <= 0 ||
		!time.Unix(0, canonical.UnixNano()).UTC().Equal(canonical) ||
		canonical.Format(time.RFC3339Nano) != value {
		return time.Time{}, errors.New("authority time is not canonical")
	}
	return canonical, nil
}

func validSetupDependencies(dependencies setupDependencies) bool {
	return dependencies.workingDirectory != nil && dependencies.loadBundle != nil &&
		dependencies.newCompanion != nil && dependencies.detectHost != nil &&
		dependencies.inspectHost != nil && dependencies.activationSupported != nil &&
		dependencies.prepareNode != nil &&
		dependencies.canInitialize != nil && dependencies.acquireLock != nil &&
		dependencies.newClient != nil && dependencies.installBundle != nil &&
		dependencies.installProjection != nil && dependencies.verifyProjection != nil &&
		dependencies.verifyActivation != nil && dependencies.verifyAbsent != nil &&
		dependencies.preflightUpgrade != nil && dependencies.acquireLifecycle != nil &&
		dependencies.newPreflight != nil && dependencies.currentExecutable != nil &&
		dependencies.newLauncher != nil && dependencies.newHookGate != nil &&
		dependencies.ensure != nil
}

func normalizeSetupAPIError(apiErr *localapi.APIError) *localapi.APIError {
	if apiErr == nil {
		return setupError(localapi.CodeInternal, "internal setup error")
	}
	switch apiErr.Code {
	case localapi.CodeAuthenticationFailed:
		return setupAuthError("managed Profile authentication failed")
	case localapi.CodeAssetRevisionMismatch:
		return setupAssetsError()
	case localapi.CodeProfileHostMismatch:
		return setupError(localapi.CodeProfileHostMismatch,
			"managed Profile is bound to another Host; eject is required before switching")
	case localapi.CodeMnemondUnavailable:
		return setupUnavailableError("mnemond local control is unavailable")
	default:
		return setupError(localapi.CodeInternal, "managed authority observation failed")
	}
}

func setupEnsureError(err error) *localapi.APIError {
	var apiErr *localapi.APIError
	if errors.As(err, &apiErr) {
		return normalizeSetupAPIError(apiErr)
	}
	if errors.Is(err, errSetupHookGate) || errors.Is(err, node.ErrDaemonPreflight) ||
		errors.Is(err, node.ErrDaemonAuthority) || errors.Is(err, node.ErrDaemonHealthAuthority) ||
		errors.Is(err, localapi.ErrUnsafeClientState) {
		return setupAssetsError()
	}
	return setupUnavailableError("mnemond could not be made ready")
}

func setupLifecycleError(err error) *localapi.APIError {
	var apiErr *localapi.APIError
	if errors.As(err, &apiErr) && apiErr.Code == localapi.CodeOperationPending {
		return setupError(localapi.CodeOperationPending,
			"managed Profile has active Agent work; retry setup after it becomes idle")
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, node.ErrDaemonLifecycle) || errors.Is(err, node.ErrOfflineAuthorityActive) {
		return setupUnavailableError("managed daemon could not be stopped safely")
	}
	return setupError(localapi.CodeInternal, "managed revision upgrade could not be completed safely")
}

func setupError(code localapi.ErrorCode, message string) *localapi.APIError {
	return localapi.NewAPIError(code, message)
}

func setupAuthError(message string) *localapi.APIError {
	return setupError(localapi.CodeAuthenticationFailed, message)
}

func setupAssetsError() *localapi.APIError {
	return setupError(localapi.CodeAssetRevisionMismatch,
		"canonical managed assets or projection are invalid")
}

func setupHostActivationError(err error) *localapi.APIError {
	if errors.Is(err, integration.ErrHostActivationRequired) {
		return setupError(localapi.CodeHostActivationRequired,
			"Codex has not loaded the exact trusted Mnemon Hook and Skill; trust this project and the current Hook with /hooks, then rerun setup")
	}
	if errors.Is(err, integration.ErrProjectionConflict) ||
		errors.Is(err, integration.ErrUnsafeProjection) {
		return setupAssetsError()
	}
	return setupUnavailableError("selected Host activation could not be observed")
}

func setupUnsupportedActivationError() *localapi.APIError {
	return setupError(localapi.CodeHostActivationRequired,
		"selected Host has no verifiable managed Hook activation surface; use codex")
}

func setupUnavailableError(message string) *localapi.APIError {
	return setupError(localapi.CodeMnemondUnavailable, message)
}

func (app *setupApp) writeSetupError(apiErr *localapi.APIError) int {
	if apiErr == nil {
		apiErr = setupError(localapi.CodeInternal, "internal setup error")
	}
	if _, err := fmt.Fprintf(app.stderr, "%s: %s\n", apiErr.Code, apiErr.Message); err != nil {
		return 1
	}
	return apiErr.ExitStatus()
}
