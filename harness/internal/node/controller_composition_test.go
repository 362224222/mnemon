package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/teamwork"
)

func TestBindControllerActionPolicyVerifiesAndFreezesTheInstallationRevision(t *testing.T) {
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	verified := 0
	install := testInstallationWithActions(InstallationVerifierFunc(func(context.Context, model.Profile) error {
		verified++
		return nil
	}), bundle)
	policy := agent.ActionPolicy{}
	handlers := agent.ActionHandlers{}
	if err := bindControllerActionPolicy(context.Background(), model.Profile{}, install,
		bundle.Revision(), &policy, &handlers); err != nil {
		t.Fatal(err)
	}
	if verified != 1 || policy.AssetRevision().String() != bundle.Revision() ||
		len(policy.Actions()) != teamwork.TeamworkActionCount ||
		handlers.AssetRevision().String() != bundle.Revision() {
		t.Fatalf("bound policy = (%d, %s, %d)", verified,
			policy.AssetRevision(), len(policy.Actions()))
	}
	if err := bindControllerActionPolicy(context.Background(), model.Profile{}, install,
		model.Sum([]byte("other controller revision")).String(), &policy, &handlers); err == nil {
		t.Fatal("bindControllerActionPolicy accepted a different active revision")
	}
	if err := bindControllerActionPolicy(context.Background(), model.Profile{}, install,
		bundle.Revision(), nil, &handlers); err == nil {
		t.Fatal("bindControllerActionPolicy accepted a missing policy destination")
	}
	if err := bindControllerActionPolicy(context.Background(), model.Profile{}, install,
		bundle.Revision(), &policy, nil); err == nil {
		t.Fatal("bindControllerActionPolicy accepted a missing handler destination")
	}
}

func TestControllerRuntimeComponentsFenceAdmissionOnMeshReadiness(t *testing.T) {
	t.Run("ready", func(t *testing.T) {
		mesh := newControllerCompositionMesh()
		local := newControllerCompositionControl(nil)
		order := &controllerCompositionOrder{}
		mesh.order, local.order = order, order
		var releases atomic.Int32
		controller := &Controller{meshTransport: mesh, beforeAccept: func() error {
			releases.Add(1)
			close(local.permitReleased)
			return nil
		}}
		supervisor, err := newNodeSupervisor(controller.runtimeComponents(local))
		if err != nil {
			t.Fatal(err)
		}
		if got := controllerCompositionNames(supervisor.components); got !=
			"mesh-transport,local-control" {
			t.Fatalf("component order = %q", got)
		}
		stop := make(chan struct{})
		done := make(chan error, 1)
		go func() { done <- supervisor.Run(context.Background(), stop) }()
		waitControllerCompositionSignal(t, mesh.runStarted)
		if releases.Load() != 0 || channelClosed(local.runStarted) {
			t.Fatalf("mesh-not-ready admission = releases %d, local started %t",
				releases.Load(), channelClosed(local.runStarted))
		}
		mesh.ready <- nil
		waitControllerCompositionSignal(t, local.runStarted)
		if releases.Load() != 1 || !local.runSawPermit.Load() {
			t.Fatalf("ready admission = releases %d, run saw permit %t",
				releases.Load(), local.runSawPermit.Load())
		}
		close(stop)
		if err := waitControllerCompositionError(t, done); err != nil {
			t.Fatal(err)
		}
		if got := order.String(); got != "local-control,mesh-transport" {
			t.Fatalf("shutdown order = %q", got)
		}
	})

	t.Run("readiness failure", func(t *testing.T) {
		mesh := newControllerCompositionMesh()
		local := newControllerCompositionControl(nil)
		var releases atomic.Int32
		controller := &Controller{meshTransport: mesh, beforeAccept: func() error {
			releases.Add(1)
			return nil
		}}
		supervisor, err := newNodeSupervisor(controller.runtimeComponents(local))
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- supervisor.Run(context.Background(), nil) }()
		waitControllerCompositionSignal(t, mesh.runStarted)
		readyErr := errors.New("injected mesh readiness failure")
		mesh.ready <- readyErr
		err = waitControllerCompositionError(t, done)
		if !errors.Is(err, readyErr) || releases.Load() != 0 ||
			channelClosed(local.runStarted) || mesh.closeCalls.Load() != 1 {
			t.Fatalf("readiness result = %v, releases=%d local=%t mesh closes=%d",
				err, releases.Load(), channelClosed(local.runStarted), mesh.closeCalls.Load())
		}
	})
}

func TestControllerRuntimeComponentsRetainMeshAfterUnsafeControlDrain(t *testing.T) {
	mesh := newControllerCompositionMesh()
	local := newControllerCompositionControl(ErrControlTransportUndrained)
	controller := &Controller{meshTransport: mesh, beforeAccept: func() error {
		close(local.permitReleased)
		return nil
	}}
	supervisor, err := newNodeSupervisor(controller.runtimeComponents(local))
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(context.Background(), stop) }()
	mesh.ready <- nil
	waitControllerCompositionSignal(t, local.runStarted)
	close(stop)
	err = waitControllerCompositionError(t, done)
	if !errors.Is(err, ErrControlTransportUndrained) || mesh.closeCalls.Load() != 0 ||
		local.shutdownCalls.Load() != 1 || channelClosed(mesh.runStopped) {
		t.Fatalf("unsafe shutdown = %v, mesh closes=%d local shutdowns=%d stopped=%t", err,
			mesh.closeCalls.Load(), local.shutdownCalls.Load(), channelClosed(mesh.runStopped))
	}
	if err := mesh.Close(); err != nil {
		t.Fatal(err)
	}
	waitControllerCompositionSignal(t, mesh.runStopped)
}

type controllerCompositionMesh struct {
	ready      chan error
	runStarted chan struct{}
	runStopped chan struct{}
	stop       chan struct{}
	started    sync.Once
	stopped    sync.Once
	stopOnce   sync.Once
	closeCalls atomic.Int32
	order      *controllerCompositionOrder
}

func newControllerCompositionMesh() *controllerCompositionMesh {
	return &controllerCompositionMesh{ready: make(chan error, 1), runStarted: make(chan struct{}),
		runStopped: make(chan struct{}), stop: make(chan struct{})}
}

func (mesh *controllerCompositionMesh) Run(ctx context.Context) error {
	mesh.started.Do(func() { close(mesh.runStarted) })
	select {
	case <-ctx.Done():
	case <-mesh.stop:
	}
	mesh.stopped.Do(func() { close(mesh.runStopped) })
	return nil
}

func (mesh *controllerCompositionMesh) Readiness(ctx context.Context) error {
	select {
	case err := <-mesh.ready:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (mesh *controllerCompositionMesh) Close() error {
	mesh.closeCalls.Add(1)
	mesh.stopOnce.Do(func() { close(mesh.stop) })
	if mesh.order != nil {
		mesh.order.Add("mesh-transport")
	}
	return nil
}

type controllerCompositionControl struct {
	permitReleased chan struct{}
	runStarted     chan struct{}
	runSawPermit   atomic.Bool
	shutdownErr    error
	shutdownCalls  atomic.Int32
	order          *controllerCompositionOrder
}

func newControllerCompositionControl(shutdownErr error) *controllerCompositionControl {
	return &controllerCompositionControl{permitReleased: make(chan struct{}),
		runStarted: make(chan struct{}), shutdownErr: shutdownErr}
}

func (control *controllerCompositionControl) Run(ctx context.Context) error {
	control.runSawPermit.Store(channelClosed(control.permitReleased))
	close(control.runStarted)
	<-ctx.Done()
	return nil
}

func (control *controllerCompositionControl) Readiness(ctx context.Context) error {
	select {
	case <-control.runStarted:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (control *controllerCompositionControl) Shutdown(context.Context) error {
	control.shutdownCalls.Add(1)
	if control.order != nil {
		control.order.Add("local-control")
	}
	return control.shutdownErr
}

func (*controllerCompositionControl) Close() error { return nil }

type controllerCompositionOrder struct {
	mu     sync.Mutex
	values []string
}

func (order *controllerCompositionOrder) Add(value string) {
	order.mu.Lock()
	defer order.mu.Unlock()
	order.values = append(order.values, value)
}

func (order *controllerCompositionOrder) String() string {
	order.mu.Lock()
	defer order.mu.Unlock()
	result := ""
	for index, value := range order.values {
		if index != 0 {
			result += ","
		}
		result += value
	}
	return result
}

func controllerCompositionNames(specs []componentSpec) string {
	result := ""
	for index, spec := range specs {
		if index != 0 {
			result += ","
		}
		result += spec.Name
	}
	return result
}

func waitControllerCompositionSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Controller component")
	}
}

func waitControllerCompositionError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Controller Supervisor")
		return nil
	}
}

func channelClosed(signal <-chan struct{}) bool {
	select {
	case <-signal:
		return true
	default:
		return false
	}
}
