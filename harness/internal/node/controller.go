package node

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/artifact"
	"github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
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
	// BeforeAccept is reserved for production mnemond composition. It releases
	// the inherited ensure.lock duplicate after the control socket exists and
	// immediately before the first HTTP accept.
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
	store             *store.Store
	server            *localapi.Server
	admission         *controllerAdmissionGate
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
	if options.Clock == nil {
		options.Clock = wallClock{}
	}
	if options.Store == nil || options.Signer == nil || options.Install == nil ||
		options.Profile.ID() != model.TeamworkProfileID() ||
		!options.Profile.Enabled() || options.NodeState == "" || options.Workspace == "" ||
		!filepath.IsAbs(options.NodeState) || filepath.Clean(options.NodeState) != options.NodeState ||
		!filepath.IsAbs(options.Workspace) || filepath.Clean(options.Workspace) != options.Workspace ||
		options.Profile.WorkspaceRoot() != options.Workspace {
		return nil, errors.New("mnemond controller requires canonical Node, workspace, Profile, Store and signer")
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
	if wakeWorker != nil && options.WakeAdapter != nil {
		return nil, errors.New("mnemond controller accepts only one managed wake worker")
	}
	if wakeWorker == nil && options.WakeAdapter != nil {
		wakeWorker, err = newManagedWakeWorker(options.Store, options.NodeState, options.Profile,
			options.Clock, options.Install, options.WakeAdapter, admission)
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
	health := localapi.HealthProviderFunc(func(healthCtx context.Context,
		metadata localapi.RequestMetadata,
	) (localapi.HealthSnapshot, *localapi.APIError) {
		ready := gate.Check(healthCtx, metadata.Profile) == nil
		return localapi.HealthSnapshot{AssetRevision: assetRevision, WorkersReady: ready}, nil
	})
	status := localapi.StatusProviderFunc(func(statusCtx context.Context,
		metadata localapi.RequestMetadata,
	) (localapi.StatusSnapshot, *localapi.APIError) {
		if statusCtx == nil || statusCtx.Err() != nil ||
			metadata.Profile.ID() != model.TeamworkProfileID() {
			return localapi.StatusSnapshot{}, localapi.NewAPIError(localapi.CodeInternal,
				"operational status observation was cancelled")
		}
		current, readErr := options.Store.ReadLocalAuthority(statusCtx)
		if readErr != nil {
			return localapi.StatusSnapshot{}, localapi.NewAPIError(localapi.CodeInternal,
				"durable operational status is unavailable")
		}
		activationIssue := ""
		if !sameControllerProfile(current.Profile, options.Profile) {
			activationIssue = "durable_authority_mismatch"
		} else if current.Node.ActiveAssetRevision() != assetRevision ||
			options.Install.Verify(statusCtx, options.Profile) != nil {
			activationIssue = "asset_revision_mismatch"
		}
		worker := agent.WakeWorkerSnapshot{Healthy: true}
		if wakeWorker != nil {
			worker = wakeWorker.Snapshot()
		}
		return localapi.StatusSnapshot{AssetRevision: assetRevision,
			ActivationReady: activationIssue == "", ActivationIssue: activationIssue,
			Runtime: localapi.RuntimeStatusSnapshot{Running: worker.Running, Ready: worker.Ready,
				Healthy: worker.Healthy, Recovering: worker.Recovering, Issue: worker.LastError}}, nil
	})
	authority := localapi.AuthorityProviderFunc(func(authorityCtx context.Context,
		metadata localapi.RequestMetadata,
	) (localapi.AuthoritySnapshot, *localapi.APIError) {
		if authorityCtx == nil || authorityCtx.Err() != nil ||
			metadata.Profile.ID() != model.TeamworkProfileID() {
			return localapi.AuthoritySnapshot{}, localapi.NewAPIError(localapi.CodeInternal,
				"durable authority observation was cancelled")
		}
		current, readErr := options.Store.ReadLocalAuthority(authorityCtx)
		if readErr != nil {
			return localapi.AuthoritySnapshot{}, localapi.NewAPIError(localapi.CodeInternal,
				"durable authority is unavailable")
		}
		return authoritySnapshot(current), nil
	})
	controller := &Controller{nodeState: options.NodeState, assetRevision: assetRevision, store: options.Store,
		admission: admission, wakeWorker: wakeWorker,
		shutdownRequested: make(chan struct{}), beforeAccept: options.BeforeAccept}
	managedService := controllerAdmissionService{gate: admission, next: newLocalAPIServiceAdapter(service)}
	server, err := localapi.NewServerWithStatusLifecycle(options.Store, managedService, health, status,
		authority, localapi.LifecycleFunc(controller.requestShutdown), controller)
	if err != nil {
		return nil, err
	}
	controller.server = server
	return controller, nil
}

// PrepareMutationShutdown seals every Store-facing Agent admission path,
// drains calls that entered before the seal, and then asks Store to prove the
// authenticated Profile generation is exact and idle. Failure always reopens
// admission; success returns a release callback for later server validation
// failures, while the accepted shutdown deliberately retains the seal.
func (controller *Controller) PrepareMutationShutdown(ctx context.Context,
	metadata localapi.RequestMetadata,
) (localapi.AuthoritySnapshot, localapi.AdmissionReleaseFunc, *localapi.APIError) {
	if controller == nil || controller.admission == nil || controller.store == nil || ctx == nil {
		return localapi.AuthoritySnapshot{}, nil, localapi.NewAPIError(localapi.CodeInternal,
			"mutation shutdown controller is unavailable")
	}
	generation, err := controller.admission.seal(ctx)
	if err != nil {
		return localapi.AuthoritySnapshot{}, nil, localapi.NewAPIError(
			localapi.CodeMnemondUnavailable, "managed admission could not be sealed")
	}
	release := localapi.AdmissionReleaseFunc(func() {
		controller.admission.reopen(generation)
	})
	authority, err := controller.store.PreflightProfileDeactivation(ctx, metadata.Profile)
	if err != nil {
		release()
		switch {
		case errors.Is(err, store.ErrProfileDeactivationBusy):
			return localapi.AuthoritySnapshot{}, nil, localapi.NewAPIError(
				localapi.CodeOperationPending, "managed Agent authority is still active")
		case errors.Is(err, store.ErrProfileDeactivationConflict):
			return localapi.AuthoritySnapshot{}, nil, localapi.NewAPIError(
				localapi.CodeOperationMismatch, "managed Profile authority changed")
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return localapi.AuthoritySnapshot{}, nil, localapi.NewAPIError(
				localapi.CodeMnemondUnavailable, "mutation shutdown was cancelled")
		default:
			return localapi.AuthoritySnapshot{}, nil, localapi.NewAPIError(
				localapi.CodeInternal, "managed Profile idleness could not be proved")
		}
	}
	return authoritySnapshot(authority), release, nil
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
	if gate.worker == nil {
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
	if gate.install == nil || !sameControllerProfile(profile, gate.expected) ||
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
	if controller == nil || controller.server == nil || ctx == nil {
		return errors.New("mnemond controller is unavailable")
	}
	controller.serveMu.Lock()
	if controller.served {
		controller.serveMu.Unlock()
		return errors.New("mnemond controller can serve only once")
	}
	controller.served = true
	controller.serveMu.Unlock()

	socketPath := filepath.Join(controller.nodeState, "control.sock")
	if _, err := localapi.RemoveStaleOwnerUnix(ctx, socketPath); err != nil {
		return errors.Join(err, controller.releaseBeforeAccept())
	}
	listener, err := localapi.ListenOwnerUnix(socketPath)
	if err != nil {
		return errors.Join(err, controller.releaseBeforeAccept())
	}
	admitted, err := newConnectionAdmissionListener(listener, controllerHTTPConnectionLimit)
	if err != nil {
		return errors.Join(err, listener.Close(), controller.releaseBeforeAccept())
	}
	if err := controller.releaseBeforeAccept(); err != nil {
		return errors.Join(err, admitted.Close())
	}
	requests := newControllerRequestTracker(controller.server.Handler())
	server := &http.Server{Handler: requests, ReadHeaderTimeout: 5 * time.Second}
	components := []componentSpec{{Name: "control-http",
		Readiness: func(readyCtx context.Context) error {
			// A successful authenticated round-trip proves owner socket, HTTP,
			// credential, schema, and exact asset revision readiness.
			return proveControllerHTTP(readyCtx, controller.nodeState, controller.assetRevision)
		},
		Run: func(runCtx context.Context) error {
			return normalizeControllerServeError(runCtx, server.Serve(admitted))
		},
		Shutdown:  func(context.Context) error { return closeControllerHTTP(server, requests) },
		Restart:   componentRestartNever,
		Resources: componentResourceBudget{MaxConcurrent: controllerHTTPConnectionLimit}}}
	if controller.wakeWorker != nil {
		workerStarted := make(chan struct{})
		components = append(components, componentSpec{Name: "managed-wake",
			Dependencies: []string{"control-http"},
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
				// Keep the lifecycle component present so HTTP remains reachable and
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
		return errors.Join(err, admitted.Close())
	}
	serveErr := supervisor.Run(ctx, controller.shutdownRequested)
	return errors.Join(serveErr, admitted.Close())
}

type controllerRequestTracker struct {
	next      http.Handler
	mu        sync.Mutex
	accepting bool
	active    uint64
	drained   chan struct{}
}

func newControllerRequestTracker(next http.Handler) *controllerRequestTracker {
	return &controllerRequestTracker{next: next, accepting: true, drained: make(chan struct{})}
}

func (tracker *controllerRequestTracker) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	tracker.mu.Lock()
	if !tracker.accepting {
		tracker.mu.Unlock()
		http.Error(writer, "mnemond is stopping", http.StatusServiceUnavailable)
		return
	}
	tracker.active++
	tracker.mu.Unlock()

	defer func() {
		tracker.mu.Lock()
		tracker.active--
		if !tracker.accepting && tracker.active == 0 {
			close(tracker.drained)
		}
		tracker.mu.Unlock()
	}()
	tracker.next.ServeHTTP(writer, request)
}

func (tracker *controllerRequestTracker) seal() <-chan struct{} {
	tracker.mu.Lock()
	if tracker.accepting {
		tracker.accepting = false
		if tracker.active == 0 {
			close(tracker.drained)
		}
	}
	drained := tracker.drained
	tracker.mu.Unlock()
	return drained
}

func proveControllerHTTP(ctx context.Context, nodeState, assetRevision string) error {
	if ctx == nil || ctx.Err() != nil {
		return errors.New("mnemond controller startup was cancelled")
	}
	client, err := localapi.NewClient(nodeState)
	if err != nil {
		return errors.New("mnemond controller startup authority is unavailable")
	}
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	health, apiErr := client.ProbeHealth(probeCtx)
	if apiErr != nil || health.SchemaVersion != localapi.SchemaVersion ||
		health.AssetRevision != assetRevision ||
		(health.Status != "ready" && health.Status != "not_ready") {
		return errors.New("mnemond controller HTTP startup proof failed")
	}
	return nil
}

func closeControllerHTTP(server *http.Server, requests *controllerRequestTracker) error {
	if server == nil || requests == nil {
		return errors.New("mnemond controller HTTP lifecycle is unavailable")
	}
	drained := requests.seal()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	shutdownErr := server.Shutdown(shutdownCtx)
	cancel()
	if shutdownErr != nil {
		closeErr := server.Close()
		if errors.Is(closeErr, http.ErrServerClosed) {
			closeErr = nil
		}
		<-drained
		return errors.Join(shutdownErr, closeErr)
	}
	<-drained
	return nil
}

func normalizeControllerServeError(ctx context.Context, err error) error {
	if errors.Is(err, http.ErrServerClosed) || ctx != nil && ctx.Err() != nil && errors.Is(err, os.ErrClosed) {
		return nil
	}
	return err
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
