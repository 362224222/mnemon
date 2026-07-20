package node

import (
	"context"
	"errors"
	"path/filepath"
	"sync"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

type ControllerOptions struct {
	NodeState    string
	Workspace    string
	Store        *store.Store
	ArtifactCAS  *artifact.CAS
	Profile      model.Profile
	Signer       event.PublicationSigner
	Clock        Clock
	Install      InstallationVerifier
	actionPolicy agent.ActionPolicy
	// WakeAdapter enables the managed Runtime worker. It is optional only for
	// low-level controller and daemon composition; OpenManagedDaemon requires
	// the production composition layer to supply one.
	WakeAdapter agent.WakeWorkerAdapter
	Attachments agent.WakeAttachmentFilesystem
	Control     ControlTransportFactory
	// BeforeAccept is reserved for production mnemond composition. It releases
	// the inherited ensure.lock duplicate after the control socket exists and
	// immediately before the first local-control admission.
	BeforeAccept func() error
	// wakeWorker is a same-package test seam for lifecycle and readiness
	// verification. Production composition always uses WakeAdapter.
	wakeWorker     managedWakeWorker
	meshTransport  managedMeshTransport
	actionHandlers agent.ActionHandlers
}

type managedWakeWorker interface {
	Run(context.Context) error
	Snapshot() agent.WakeWorkerSnapshot
}

// Controller is the single local composition root for mnemond-managed Agent
// traffic. It owns no second domain state: every route reaches the one Store,
// while CAS and readonly views stay beneath the same Node state directory.
type Controller struct {
	nodeState         string
	assetRevision     string
	profile           model.Profile
	store             *store.Store
	artifactCAS       *artifact.CAS
	admission         *controllerAdmissionGate
	activation        controllerManagedActivationGate
	controlFactory    ControlTransportFactory
	controlService    ManagedControlService
	wakeWorker        managedWakeWorker
	meshTransport     managedMeshTransport
	shutdownRequested chan struct{}
	shutdownOnce      sync.Once
	serveMu           sync.Mutex
	served            bool
	beforeAccept      func() error
	beforeAcceptOnce  sync.Once
	beforeAcceptErr   error
}

func NewController(ctx context.Context, options ControllerOptions) (*Controller, error) {
	options, err := prepareControllerOptions(ctx, options)
	if err != nil {
		return nil, err
	}
	return newController(options)
}

func validateControllerOptions(ctx context.Context, options ControllerOptions) (ControllerOptions, error) {
	if options.Clock == nil {
		options.Clock = wallClock{}
	}
	if !validControllerCoreOptions(ctx, options) {
		return ControllerOptions{}, errors.New(
			"mnemond controller requires canonical Node, workspace, Profile, Store and ports")
	}
	hasWakeWorker := !isNilNodeInterface(options.wakeWorker)
	hasWakeAdapter := !isNilNodeInterface(options.WakeAdapter)
	if !validControllerWakeOptions(options, hasWakeWorker, hasWakeAdapter) {
		return ControllerOptions{}, errors.New(
			"mnemond controller requires one complete managed wake composition")
	}
	if !hasWakeWorker {
		options.wakeWorker = nil
	}
	if !hasWakeAdapter {
		options.WakeAdapter = nil
		options.Attachments = nil
	}
	return options, nil
}

func validControllerCoreOptions(ctx context.Context, options ControllerOptions) bool {
	return ctx != nil && ctx.Err() == nil && options.Store != nil &&
		!isNilNodeInterface(options.Clock) && !isNilNodeInterface(options.Signer) &&
		!isNilNodeInterface(options.Install) && !isNilNodeInterface(options.Control) &&
		options.Profile.ID() == model.TeamworkProfileID() && options.Profile.Enabled() &&
		options.NodeState != "" && options.Workspace != "" &&
		filepath.IsAbs(options.NodeState) && filepath.Clean(options.NodeState) == options.NodeState &&
		filepath.IsAbs(options.Workspace) && filepath.Clean(options.Workspace) == options.Workspace &&
		options.Profile.WorkspaceRoot() == options.Workspace
}

func validControllerWakeOptions(options ControllerOptions, hasWorker, hasAdapter bool) bool {
	hasAttachments := !isNilNodeInterface(options.Attachments)
	return (options.wakeWorker == nil || hasWorker) &&
		(options.WakeAdapter == nil || hasAdapter) &&
		(options.Attachments == nil || hasAttachments) &&
		!(hasWorker && hasAdapter) && hasAdapter == hasAttachments &&
		!(hasWorker && hasAttachments)
}

// PrepareMutationShutdown seals every Store-facing Agent admission path,
// drains calls that entered before the seal, and then asks Store to prove the
// authenticated Profile generation is exact and idle. Failure always reopens
// admission; success returns a release callback for later server validation
// failures, while the accepted shutdown deliberately retains the seal.
func (controller *Controller) PrepareMutationShutdown(ctx context.Context,
	profile model.Profile,
) (Authority, func(), *ControlError) {
	if controller == nil || controller.admission == nil || controller.store == nil || ctx == nil {
		return Authority{}, nil, agent.NewControlError(agent.CodeInternal,
			"mutation shutdown controller is unavailable")
	}
	generation, err := controller.admission.seal(ctx)
	if err != nil {
		return Authority{}, nil, agent.NewControlError(
			agent.CodeMnemondUnavailable, "managed admission could not be sealed")
	}
	release := func() {
		controller.admission.reopen(generation)
	}
	authority, err := controller.store.PreflightProfileDeactivation(ctx, profile)
	if err != nil {
		release()
		switch {
		case errors.Is(err, store.ErrProfileDeactivationBusy):
			return Authority{}, nil, agent.NewControlError(
				agent.CodeOperationPending, "managed Agent authority is still active")
		case errors.Is(err, store.ErrProfileDeactivationConflict):
			return Authority{}, nil, agent.NewControlError(
				agent.CodeOperationMismatch, "managed Profile authority changed")
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return Authority{}, nil, agent.NewControlError(
				agent.CodeMnemondUnavailable, "mutation shutdown was cancelled")
		default:
			return Authority{}, nil, agent.NewControlError(
				agent.CodeInternal, "managed Profile idleness could not be proved")
		}
	}
	value, valueErr := authorityValue(authority)
	if valueErr != nil {
		release()
		return Authority{}, nil, agent.NewControlError(agent.CodeInternal,
			"managed Profile authority is invalid")
	}
	return value, release, nil
}

func (controller *Controller) ObserveControlHealth(ctx context.Context,
	profile model.Profile,
) (DaemonHealth, *ControlError) {
	if controller == nil {
		return DaemonHealth{}, agent.NewControlError(agent.CodeInternal,
			"health observation is unavailable")
	}
	ready := controller.activation.Check(ctx, profile) == nil
	return DaemonHealth{AssetRevision: controller.assetRevision, Ready: ready}, nil
}

func (controller *Controller) ObserveControlStatus(ctx context.Context,
	profile model.Profile,
) (ControlStatus, *ControlError) {
	if controller == nil || controller.store == nil || ctx == nil || ctx.Err() != nil ||
		profile.ID() != model.TeamworkProfileID() {
		return ControlStatus{}, agent.NewControlError(agent.CodeInternal,
			"operational status observation was cancelled")
	}
	current, err := controller.store.ReadLocalAuthority(ctx)
	if err != nil {
		return ControlStatus{}, agent.NewControlError(agent.CodeInternal,
			"durable operational status is unavailable")
	}
	activationIssue := ""
	if !sameControllerProfile(current.Profile, controller.profile) {
		activationIssue = "durable_authority_mismatch"
	} else if current.Node.ActiveAssetRevision() != controller.assetRevision ||
		controller.activation.install.install.Verify(ctx, controller.profile) != nil {
		activationIssue = "asset_revision_mismatch"
	}
	worker := agent.WakeWorkerSnapshot{Healthy: true}
	if controller.wakeWorker != nil {
		worker = controller.wakeWorker.Snapshot()
	}
	return ControlStatus{AssetRevision: controller.assetRevision,
		ActivationReady: activationIssue == "", ActivationIssue: activationIssue,
		Runtime: RuntimeStatus{Running: worker.Running, Ready: worker.Ready,
			Healthy: worker.Healthy, Recovering: worker.Recovering, Issue: worker.LastError}}, nil
}

func (controller *Controller) ObserveControlAuthority(ctx context.Context,
	profile model.Profile,
) (Authority, *ControlError) {
	if controller == nil || controller.store == nil || ctx == nil || ctx.Err() != nil ||
		profile.ID() != model.TeamworkProfileID() {
		return Authority{}, agent.NewControlError(agent.CodeInternal,
			"durable authority observation was cancelled")
	}
	current, err := controller.store.ReadLocalAuthority(ctx)
	if err != nil {
		return Authority{}, agent.NewControlError(agent.CodeInternal,
			"durable authority is unavailable")
	}
	value, valueErr := authorityValue(current)
	if valueErr != nil {
		return Authority{}, agent.NewControlError(agent.CodeInternal,
			"durable authority is invalid")
	}
	return value, nil
}

func (controller *Controller) requestShutdown() {
	if controller == nil || controller.shutdownRequested == nil {
		return
	}
	controller.shutdownOnce.Do(func() { close(controller.shutdownRequested) })
}

type controllerActivationGate struct {
	expected model.Profile
	install  InstallationVerifier
}

// controllerManagedActivationGate keeps the four Agent routes closed until
// both the canonical installation and the single managed Runtime worker are
// ready. A nil worker preserves the explicit low-level composition seam.
type controllerManagedActivationGate struct {
	install controllerActivationGate
	worker  managedWakeWorker
}

func (gate controllerManagedActivationGate) Check(ctx context.Context,
	profile model.Profile,
) *agent.ControlError {
	if apiErr := gate.install.Check(ctx, profile); apiErr != nil {
		return apiErr
	}
	if isNilNodeInterface(gate.worker) {
		return nil
	}
	snapshot := gate.worker.Snapshot()
	if !snapshot.Running || !snapshot.Ready || !snapshot.Healthy || snapshot.Recovering {
		return agent.NewControlError(agent.CodeMnemondUnavailable,
			"managed Runtime worker is not ready")
	}
	return nil
}

func (gate controllerActivationGate) Check(ctx context.Context, profile model.Profile) *agent.ControlError {
	if ctx == nil || ctx.Err() != nil {
		return agent.NewControlError(agent.CodeInternal, "managed activation check was cancelled")
	}
	if isNilNodeInterface(gate.install) || !sameControllerProfile(profile, gate.expected) ||
		gate.install.Verify(ctx, profile) != nil {
		return agent.NewControlError(agent.CodeAssetRevisionMismatch,
			"managed Node assets or Host projection differ from the active Profile")
	}
	return nil
}

func sameControllerProfile(got, want model.Profile) bool {
	return got.ID() == want.ID() && got.Principal() == want.Principal() &&
		got.WorkspaceRoot() == want.WorkspaceRoot() && got.Host() == want.Host() &&
		got.Runtime() == want.Runtime() && got.CredentialHash() == want.CredentialHash() &&
		got.ActiveAssetRevision() == want.ActiveAssetRevision() && got.UpdatedAt().Equal(want.UpdatedAt()) &&
		got.HandlingBudget().String() == want.HandlingBudget().String() && got.Enabled() && want.Enabled()
}

func (controller *Controller) Serve(ctx context.Context) error {
	if controller == nil || isNilNodeInterface(controller.controlFactory) ||
		controller.controlService == nil || ctx == nil {
		return errors.New("mnemond controller is unavailable")
	}
	controller.serveMu.Lock()
	if controller.served {
		controller.serveMu.Unlock()
		return errors.New("mnemond controller can serve only once")
	}
	controller.served = true
	controller.serveMu.Unlock()

	transport, err := controller.controlFactory.Prepare(ctx, ControlTransportOptions{
		NodeState: controller.nodeState, AssetRevision: controller.assetRevision,
		MaxConnections: controllerControlConnectionLimit,
	}, ControlBindings{Authenticator: controller.store, Agent: controller.controlService,
		Observer: controller, Mutation: controller, Shutdown: controller.requestShutdown})
	if err != nil || isNilNodeInterface(transport) {
		if err == nil {
			err = errors.New("local control transport factory returned no transport")
		}
		var closeErr error
		if !isNilNodeInterface(transport) {
			closeErr = transport.Close()
		}
		return errors.Join(err, closeErr)
	}
	components := controller.runtimeComponents(transport)
	supervisor, err := newNodeSupervisor(components)
	if err != nil {
		return errors.Join(err, transport.Close())
	}
	serveErr := supervisor.Run(ctx, controller.shutdownRequested)
	return errors.Join(serveErr, transport.Close())
}

func (controller *Controller) releaseBeforeAccept() error {
	if controller == nil {
		return nil
	}
	controller.beforeAcceptOnce.Do(func() {
		if controller.beforeAccept != nil {
			controller.beforeAcceptErr = controller.beforeAccept()
		}
		controller.beforeAccept = nil
	})
	return controller.beforeAcceptErr
}
