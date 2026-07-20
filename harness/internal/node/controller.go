package node

import (
	"context"
	"errors"
	"fmt"
	"os"
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
	admission         *controllerAdmissionGate
	activation        controllerManagedActivationGate
	controlFactory    ControlTransportFactory
	controlService    ManagedControlService
	wakeWorker        managedWakeWorker
	shutdownRequested chan struct{}
	shutdownOnce      sync.Once
	serveMu           sync.Mutex
	served            bool
	beforeAccept      func() error
	beforeAcceptOnce  sync.Once
	beforeAcceptErr   error
}

func NewController(ctx context.Context, options ControllerOptions) (*Controller, error) {
	var err error
	options, err = validateControllerOptions(ctx, options)
	if err != nil {
		return nil, err
	}
	stateInfo, err := os.Lstat(options.NodeState)
	if err != nil || !stateInfo.IsDir() || stateInfo.Mode()&os.ModeSymlink != 0 ||
		stateInfo.Mode().Perm() != 0o700 || options.Store.Path() != filepath.Join(options.NodeState, "node.db") {
		return nil, errors.New("mnemond controller Store is outside its owner-only Node state")
	}
	assetRevision := options.Profile.ActiveAssetRevision()
	if err := bindControllerActionPolicy(ctx, options.Profile, options.Install, assetRevision, &options.actionPolicy, &options.actionHandlers); err != nil {
		return nil, err
	}
	cas, err := artifact.NewCAS(filepath.Join(options.NodeState, "objects", "sha256"))
	if err != nil {
		return nil, err
	}
	capturer, err := artifact.NewCapturer(options.Workspace, cas, options.Clock.Now)
	if err != nil {
		return nil, err
	}
	capture, err := agent.NewArtifactCaptureCoordinator(capturer, cas, cas, options.Store, options.Clock)
	if err != nil {
		return nil, err
	}
	materializer, err := artifact.NewViewMaterializer(options.NodeState, cas)
	if err != nil {
		return nil, err
	}
	currentViews, err := agent.NewCurrentViewCoordinator(materializer)
	if err != nil {
		return nil, err
	}
	viewValidator, err := agent.NewReadonlyArtifactViewValidator(options.NodeState)
	if err != nil {
		return nil, err
	}
	artifactResolver, err := agent.NewArtifactResolver(capture, viewValidator)
	if err != nil {
		return nil, err
	}
	executor, err := agent.NewTeamworkActionExecutor(options.Store, agent.TeamworkActionExecutorOptions{
		Profile: options.Profile, Actions: options.actionHandlers, Signer: options.Signer, Artifacts: artifactResolver, Clock: options.Clock,
	})
	if err != nil {
		return nil, err
	}
	installGate := controllerActivationGate{expected: options.Profile, install: options.Install}
	admission := newControllerAdmissionGate()
	wakeWorker := options.wakeWorker
	if wakeWorker == nil && options.WakeAdapter != nil {
		wakeWorker, err = newManagedWakeWorker(options.Store, options.Profile,
			options.Clock, options.Install, options.WakeAdapter, options.Attachments, admission)
		if err != nil {
			return nil, fmt.Errorf("mnemond controller managed wake worker: %w", err)
		}
	}
	gate := controllerManagedActivationGate{install: installGate, worker: wakeWorker}
	service, err := agent.NewService(options.Store, agent.ServiceOptions{Actions: options.actionHandlers,
		Clock: options.Clock, Executor: executor, CurrentViews: currentViews, ActivationGate: gate})
	if err != nil {
		return nil, err
	}
	controller := &Controller{nodeState: options.NodeState, assetRevision: assetRevision, store: options.Store,
		profile: options.Profile, admission: admission, activation: gate,
		controlFactory: options.Control, wakeWorker: wakeWorker,
		shutdownRequested: make(chan struct{}), beforeAccept: options.BeforeAccept}
	controller.controlService = controllerAdmissionService{gate: admission, next: service}
	return controller, nil
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
		return errors.Join(err, closeErr, controller.releaseBeforeAccept())
	}
	if err := controller.releaseBeforeAccept(); err != nil {
		return errors.Join(err, transport.Close())
	}
	components := []componentSpec{{Name: "local-control",
		Readiness: transport.Readiness, Run: transport.Run, Shutdown: transport.Shutdown,
		Restart:   componentRestartNever,
		Resources: componentResourceBudget{MaxConcurrent: controllerControlConnectionLimit}}}
	if controller.wakeWorker != nil {
		workerStarted := make(chan struct{})
		components = append(components, componentSpec{Name: "managed-wake",
			Dependencies: []string{"local-control"},
			Readiness: func(readyCtx context.Context) error {
				// Dynamic Runtime health remains authoritative in WakeWorker.Snapshot.
				// This signal only proves that lifecycle ownership has transferred.
				select {
				case <-workerStarted:
					return nil
				case <-readyCtx.Done():
					return readyCtx.Err()
				}
			},
			Run: func(workerCtx context.Context) error {
				close(workerStarted)
				_ = controller.wakeWorker.Run(workerCtx)
				// WakeWorker publishes domain failure through its existing Snapshot.
				// Keep the lifecycle component present so local control remains reachable and
				// managed actions stay fail-closed until explicit Node shutdown.
				if workerCtx.Err() == nil {
					<-workerCtx.Done()
				}
				return nil
			},
			Shutdown: func(context.Context) error { return nil }, Restart: componentRestartNever,
			Resources: componentResourceBudget{MaxConcurrent: 1}})
	}
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
