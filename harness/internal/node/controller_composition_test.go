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

func TestValidateControllerManagedRuntimePairFailsClosed(t *testing.T) {
	var typedNilMesh *controllerCompositionMesh
	var typedNilRuntime *controllerCompositionChannelRuntime
	for _, test := range []struct {
		name    string
		mesh    managedMeshTransport
		runtime managedChannelRuntime
		wantErr bool
	}{
		{name: "unmanaged"},
		{name: "managed", mesh: newControllerCompositionMesh(),
			runtime: newControllerCompositionChannelRuntime()},
		{name: "mesh only", mesh: newControllerCompositionMesh(), wantErr: true},
		{name: "runtime only", runtime: newControllerCompositionChannelRuntime(), wantErr: true},
		{name: "typed nil mesh", mesh: typedNilMesh,
			runtime: newControllerCompositionChannelRuntime(), wantErr: true},
		{name: "typed nil runtime", mesh: newControllerCompositionMesh(),
			runtime: typedNilRuntime, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateControllerManagedRuntimePair(test.mesh, test.runtime)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateControllerManagedRuntimePair() = %v, want error %t",
					err, test.wantErr)
			}
		})
	}
}

func TestControllerRuntimeComponentsFenceAdmissionOnManagedStartup(t *testing.T) {
	mesh := newControllerCompositionMesh()
	channelRuntime := newControllerCompositionChannelRuntime()
	local := newControllerCompositionControl(nil)
	order := &controllerCompositionOrder{}
	mesh.order, channelRuntime.order, local.order = order, order, order
	var releases atomic.Int32
	controller := &Controller{meshTransport: mesh, channelRuntime: channelRuntime,
		beforeAccept: func() error {
			releases.Add(1)
			close(local.permitReleased)
			return nil
		}}
	supervisor, err := newNodeSupervisor(controller.runtimeComponents(local))
	if err != nil {
		t.Fatal(err)
	}
	if got := controllerCompositionNames(supervisor.components); got !=
		"mesh-transport,channel-runtime,local-control" {
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
	waitControllerCompositionSignal(t, channelRuntime.runStarted)
	if releases.Load() != 0 || channelClosed(local.runStarted) {
		t.Fatalf("topics-not-ready admission = releases %d, local started %t",
			releases.Load(), channelClosed(local.runStarted))
	}
	channelRuntime.ready <- nil
	waitControllerCompositionSignal(t, local.runStarted)
	if releases.Load() != 1 || !local.runSawPermit.Load() {
		t.Fatalf("ready admission = releases %d, run saw permit %t",
			releases.Load(), local.runSawPermit.Load())
	}
	close(stop)
	if err := waitControllerCompositionError(t, done); err != nil {
		t.Fatal(err)
	}
	if got := order.String(); got != "local-control,channel-runtime,mesh-transport" {
		t.Fatalf("shutdown order = %q", got)
	}
}

func TestControllerRuntimeComponentsRejectMeshReadinessFailure(t *testing.T) {
	mesh := newControllerCompositionMesh()
	channelRuntime := newControllerCompositionChannelRuntime()
	local := newControllerCompositionControl(nil)
	var releases atomic.Int32
	controller := &Controller{meshTransport: mesh, channelRuntime: channelRuntime,
		beforeAccept: func() error { releases.Add(1); return nil }}
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
		channelClosed(channelRuntime.runStarted) || channelClosed(local.runStarted) ||
		mesh.closeCalls.Load() != 1 {
		t.Fatalf("readiness result = %v, releases=%d runtime=%t local=%t mesh closes=%d",
			err, releases.Load(), channelClosed(channelRuntime.runStarted),
			channelClosed(local.runStarted), mesh.closeCalls.Load())
	}
}

func TestControllerRuntimeComponentsRejectChannelRuntimeReadinessFailure(t *testing.T) {
	mesh := newControllerCompositionMesh()
	channelRuntime := newControllerCompositionChannelRuntime()
	local := newControllerCompositionControl(nil)
	var releases atomic.Int32
	controller := &Controller{meshTransport: mesh, channelRuntime: channelRuntime,
		beforeAccept: func() error { releases.Add(1); return nil }}
	supervisor, err := newNodeSupervisor(controller.runtimeComponents(local))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(context.Background(), nil) }()
	mesh.ready <- nil
	waitControllerCompositionSignal(t, channelRuntime.runStarted)
	readyErr := errors.New("injected Channel runtime readiness failure")
	channelRuntime.ready <- readyErr
	err = waitControllerCompositionError(t, done)
	if !errors.Is(err, readyErr) || releases.Load() != 0 ||
		channelClosed(local.runStarted) || mesh.closeCalls.Load() != 1 ||
		!channelClosed(channelRuntime.runStopped) {
		t.Fatalf("Channel readiness result = %v, releases=%d local=%t mesh closes=%d stopped=%t",
			err, releases.Load(), channelClosed(local.runStarted), mesh.closeCalls.Load(),
			channelClosed(channelRuntime.runStopped))
	}
}

func TestControllerRuntimeComponentsPropagateChannelRuntimeFailure(t *testing.T) {
	mesh := newControllerCompositionMesh()
	channelRuntime := newControllerCompositionChannelRuntime()
	local := newControllerCompositionControl(nil)
	controller := &Controller{meshTransport: mesh, channelRuntime: channelRuntime,
		beforeAccept: func() error { close(local.permitReleased); return nil }}
	supervisor, err := newNodeSupervisor(controller.runtimeComponents(local))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(context.Background(), nil) }()
	mesh.ready <- nil
	channelRuntime.ready <- nil
	waitControllerCompositionSignal(t, local.runStarted)
	terminalErr := errors.New("injected Channel runtime terminal failure")
	channelRuntime.terminal <- terminalErr
	err = waitControllerCompositionError(t, done)
	if !errors.Is(err, terminalErr) || local.shutdownCalls.Load() != 1 ||
		mesh.closeCalls.Load() != 1 {
		t.Fatalf("terminal result = %v, local shutdowns=%d mesh closes=%d", err,
			local.shutdownCalls.Load(), mesh.closeCalls.Load())
	}
}

func TestControllerRuntimeComponentsRetainMeshAfterUnsafeControlDrain(t *testing.T) {
	mesh := newControllerCompositionMesh()
	channelRuntime := newControllerCompositionChannelRuntime()
	local := newControllerCompositionControl(ErrControlTransportUndrained)
	controller := &Controller{meshTransport: mesh, channelRuntime: channelRuntime,
		beforeAccept: func() error {
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
	channelRuntime.ready <- nil
	waitControllerCompositionSignal(t, local.runStarted)
	close(stop)
	err = waitControllerCompositionError(t, done)
	if !errors.Is(err, ErrControlTransportUndrained) || mesh.closeCalls.Load() != 0 ||
		local.shutdownCalls.Load() != 1 || channelClosed(mesh.runStopped) ||
		!channelClosed(channelRuntime.runStopped) {
		t.Fatalf("unsafe shutdown = %v, mesh closes=%d local shutdowns=%d mesh stopped=%t runtime stopped=%t",
			err, mesh.closeCalls.Load(), local.shutdownCalls.Load(),
			channelClosed(mesh.runStopped), channelClosed(channelRuntime.runStopped))
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

type controllerCompositionChannelRuntime struct {
	ready      chan error
	terminal   chan error
	runStarted chan struct{}
	runStopped chan struct{}
	started    sync.Once
	stopped    sync.Once
	order      *controllerCompositionOrder
}

func newControllerCompositionChannelRuntime() *controllerCompositionChannelRuntime {
	return &controllerCompositionChannelRuntime{ready: make(chan error, 1),
		terminal: make(chan error, 1), runStarted: make(chan struct{}),
		runStopped: make(chan struct{})}
}

func (runtime *controllerCompositionChannelRuntime) Run(ctx context.Context) error {
	runtime.started.Do(func() { close(runtime.runStarted) })
	var err error
	select {
	case err = <-runtime.terminal:
	case <-ctx.Done():
	}
	if runtime.order != nil {
		runtime.order.Add("channel-runtime")
	}
	runtime.stopped.Do(func() { close(runtime.runStopped) })
	return err
}

func (runtime *controllerCompositionChannelRuntime) Readiness(ctx context.Context) error {
	select {
	case err := <-runtime.ready:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
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
