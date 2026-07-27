package node

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
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
	Channels     ChannelService
	Clock        Clock
	Install      InstallationVerifier
	Control      ControlRuntime
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
	wakeWorker        managedWakeWorker
	networkRuntime    managedNetworkRuntime
	artifactTransfers artifactTransferObserver
	actionHandlers    agent.ActionHandlers
	shutdown          *gracefulShutdown
}

type managedWakeWorker interface {
	Run(context.Context) error
	Snapshot() agent.WakeWorkerSnapshot
}

type managedNetworkRuntime interface {
	Run(context.Context) error
	Readiness(context.Context) error
	MaxConcurrent() uint32
}

// Controller is the single local composition root for mnemond-managed Agent
// traffic. It owns no second domain state: every route reaches the one Store,
// while CAS and readonly views stay beneath the same Node state directory.
type Controller struct {
	nodeState         string
	assetRevision     string
	store             *store.Store
	server            ControlServer
	control           ControlRuntime
	admission         *controllerAdmissionGate
	wakeWorker        managedWakeWorker
	networkRuntime    managedNetworkRuntime
	shutdown          *gracefulShutdown
	shutdownRequested chan struct{}
	shutdownOnce      sync.Once
	serveMu           sync.Mutex
	served            bool
	beforeAccept      func() error
	beforeAcceptOnce  sync.Once
	beforeAcceptErr   error
}

func NewController(ctx context.Context, options ControllerOptions) (*Controller, error) {
	control, err := requireControlRuntime(options.Control)
	if err != nil {
		return nil, err
	}
	options.Control = control
	assetRevision, err := validateControllerOptions(&options)
	if err != nil {
		return nil, err
	}
	admission := newControllerAdmissionGate()
	composition, err := composeController(ctx, &options, admission)
	if err != nil {
		return nil, err
	}
	shutdown := options.shutdown
	if shutdown == nil {
		shutdown = newGracefulShutdown(defaultGracefulShutdownBudget)
	}
	controller := &Controller{nodeState: options.NodeState, assetRevision: assetRevision, store: options.Store,
		control: options.Control, admission: admission, wakeWorker: composition.wakeWorker,
		networkRuntime:    options.networkRuntime,
		shutdownRequested: make(chan struct{}), beforeAccept: options.BeforeAccept,
		shutdown: shutdown}
	managedAgent := controllerAdmissionService{gate: admission, next: newLocalAPIServiceAdapter(composition.service)}
	var managedService Service = managedAgent
	if options.Channels != nil {
		managedService = controllerChannelService{Service: managedAgent, gate: admission,
			channels: options.Channels}
	}
	server, err := options.Control.NewControlServer(options.Store, managedService, composition.observer,
		composition.observer, composition.observer, LifecycleFunc(controller.requestShutdown), controller)
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
	metadata RequestMetadata,
) (AuthoritySnapshot, AdmissionReleaseFunc, *APIError) {
	if controller == nil || controller.admission == nil || controller.store == nil || ctx == nil {
		return AuthoritySnapshot{}, nil, NewAPIError(CodeInternal,
			"mutation shutdown controller is unavailable")
	}
	generation, err := controller.admission.seal(ctx)
	if err != nil {
		return AuthoritySnapshot{}, nil, NewAPIError(
			CodeMnemondUnavailable, "managed admission could not be sealed")
	}
	release := AdmissionReleaseFunc(func() {
		controller.admission.reopen(generation)
	})
	authority, err := controller.store.PreflightProfileDeactivation(ctx, metadata.Profile)
	if err != nil {
		release()
		switch {
		case errors.Is(err, store.ErrProfileDeactivationBusy):
			return AuthoritySnapshot{}, nil, NewAPIError(
				CodeOperationPending, "managed Agent authority is still active")
		case errors.Is(err, store.ErrProfileDeactivationConflict):
			return AuthoritySnapshot{}, nil, NewAPIError(
				CodeOperationMismatch, "managed Profile authority changed")
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			return AuthoritySnapshot{}, nil, NewAPIError(
				CodeMnemondUnavailable, "mutation shutdown was cancelled")
		default:
			return AuthoritySnapshot{}, nil, NewAPIError(
				CodeInternal, "managed Profile idleness could not be proved")
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
	if _, err := controller.control.RemoveStaleOwnerUnix(ctx, socketPath); err != nil {
		return errors.Join(err, controller.releaseBeforeAccept())
	}
	listener, err := controller.control.ListenOwnerUnix(socketPath)
	if err != nil {
		return errors.Join(err, controller.releaseBeforeAccept())
	}
	admitted, err := newConnectionAdmissionListener(listener, controllerHTTPConnectionLimit)
	if err != nil {
		return errors.Join(err, listener.Close(), controller.releaseBeforeAccept())
	}
	requests := newControllerRequestTracker(controller.server.Handler())
	server := &http.Server{Handler: requests, ReadHeaderTimeout: 5 * time.Second}
	control := componentSpec{Name: "control-http",
		Readiness: func(readyCtx context.Context) error {
			// A successful authenticated round-trip proves owner socket, HTTP,
			// credential, schema, and exact asset revision readiness.
			return proveControllerHTTP(readyCtx, controller.control, controller.nodeState,
				controller.assetRevision)
		},
		Run: func(runCtx context.Context) error {
			// Keep the inherited ensure.lock authority until every dependency is
			// ready, then consume it at the final boundary before HTTP Accept.
			if err := controller.releaseBeforeAccept(); err != nil {
				return err
			}
			return normalizeControllerServeError(runCtx, server.Serve(admitted))
		},
		Shutdown: func(shutdownCtx context.Context) error {
			return closeControllerHTTP(shutdownCtx, server, requests)
		},
		Restart:   componentRestartNever,
		Resources: componentResourceBudget{MaxConcurrent: controllerHTTPConnectionLimit}}
	components := make([]componentSpec, 0, 3)
	if controller.networkRuntime != nil {
		maximum := controller.networkRuntime.MaxConcurrent()
		if maximum == 0 {
			return errors.Join(errors.New("mnemond network runtime has no resource budget"),
				admitted.Close())
		}
		components = append(components, componentSpec{Name: "managed-network",
			Readiness: controller.networkRuntime.Readiness,
			Run:       controller.networkRuntime.Run,
			Shutdown:  func(context.Context) error { return nil },
			Restart:   componentRestartNever,
			Resources: componentResourceBudget{MaxConcurrent: maximum}})
		control.Dependencies = []string{"managed-network"}
	}
	components = append(components, control)
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
		return errors.Join(err, admitted.Close(), controller.releaseBeforeAccept())
	}
	serveErr := supervisor.Run(ctx, controller.shutdownRequested, controller.shutdown)
	return errors.Join(serveErr, admitted.Close(), controller.releaseBeforeAccept())
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

func proveControllerHTTP(ctx context.Context, control ControlClientFactory,
	nodeState, assetRevision string,
) error {
	if ctx == nil || ctx.Err() != nil {
		return errors.New("mnemond controller startup was cancelled")
	}
	if control == nil {
		return errors.New("mnemond controller startup authority is unavailable")
	}
	client, err := control.NewControlClient(nodeState)
	if err != nil {
		return errors.New("mnemond controller startup authority is unavailable")
	}
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	health, apiErr := client.ProbeHealth(probeCtx)
	if apiErr != nil || health.SchemaVersion != SchemaVersion ||
		health.AssetRevision != assetRevision ||
		(health.Status != "ready" && health.Status != "not_ready") {
		return errors.New("mnemond controller HTTP startup proof failed")
	}
	return nil
}

func closeControllerHTTP(ctx context.Context, server *http.Server,
	requests *controllerRequestTracker,
) error {
	if ctx == nil || server == nil || requests == nil {
		return errors.New("mnemond controller HTTP lifecycle is unavailable")
	}
	drained := requests.seal()
	shutdownErr := server.Shutdown(ctx)
	var closeErr error
	if shutdownErr != nil {
		closeErr = server.Close()
		if errors.Is(closeErr, http.ErrServerClosed) {
			closeErr = nil
		}
	}
	drainErr := waitForGracefulShutdown(ctx, drained, "drain controller HTTP requests")
	return errors.Join(shutdownErr, closeErr, drainErr)
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
