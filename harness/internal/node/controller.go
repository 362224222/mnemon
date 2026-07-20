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
	assetRevision, err := validateControllerOptions(&options)
	if err != nil {
		return nil, err
	}
	admission := newControllerAdmissionGate()
	composition, err := composeController(ctx, &options, admission)
	if err != nil {
		return nil, err
	}
	controller := &Controller{nodeState: options.NodeState, assetRevision: assetRevision, store: options.Store,
		admission: admission, wakeWorker: composition.wakeWorker,
		shutdownRequested: make(chan struct{}), beforeAccept: options.BeforeAccept}
	managedService := controllerAdmissionService{gate: admission, next: newLocalAPIServiceAdapter(composition.service)}
	server, err := localapi.NewServerWithStatusLifecycle(options.Store, managedService, composition.observer,
		composition.observer, composition.observer, localapi.LifecycleFunc(controller.requestShutdown), controller)
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
