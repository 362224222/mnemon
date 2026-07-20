package node

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
)

func TestControllerAdmissionGateDrainsEnteredWorkAndRejectsAfterSeal(t *testing.T) {
	gate := newControllerAdmissionGate()
	release, err := gate.Enter(context.Background())
	if err != nil || release == nil {
		t.Fatalf("Enter() = (present=%t, %v)", release != nil, err)
	}
	type sealResult struct {
		generation uint64
		err        error
	}
	sealed := make(chan sealResult, 1)
	go func() {
		generation, err := gate.seal(context.Background())
		sealed <- sealResult{generation: generation, err: err}
	}()
	waitAdmissionSealed(t, gate)
	if entered, err := gate.Enter(context.Background()); entered != nil ||
		!errors.Is(err, ErrManagedAdmission) {
		t.Fatalf("Enter() after seal = (present=%t, %v)", entered != nil, err)
	}
	select {
	case result := <-sealed:
		t.Fatalf("seal returned before entered work drained: %#v", result)
	default:
	}
	release()
	result := <-sealed
	if result.err != nil || result.generation == 0 {
		t.Fatalf("seal() = (%d, %v)", result.generation, result.err)
	}
	if entered, err := gate.Enter(context.Background()); entered != nil ||
		!errors.Is(err, ErrManagedAdmission) {
		t.Fatalf("retained seal Enter() = (present=%t, %v)", entered != nil, err)
	}
	gate.reopen(result.generation)
	entered, err := gate.Enter(context.Background())
	if err != nil || entered == nil {
		t.Fatalf("Enter() after reopen = (present=%t, %v)", entered != nil, err)
	}
	entered()
}

func TestControllerAdmissionGateCancellationReopensWithoutStealingAnotherSeal(t *testing.T) {
	gate := newControllerAdmissionGate()
	release, err := gate.Enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := gate.seal(ctx)
		result <- err
	}()
	waitAdmissionSealed(t, gate)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) ||
		!errors.Is(err, ErrManagedAdmission) {
		t.Fatalf("cancelled seal error = %v", err)
	}
	entered, err := gate.Enter(context.Background())
	if err != nil || entered == nil {
		t.Fatalf("Enter() after cancelled seal = (present=%t, %v)", entered != nil, err)
	}
	entered()
	release()

	generation, err := gate.seal(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.seal(context.Background()); !errors.Is(err, ErrManagedAdmission) {
		t.Fatalf("concurrent seal error = %v", err)
	}
	gate.reopen(generation + 1)
	if entered, err := gate.Enter(context.Background()); entered != nil ||
		!errors.Is(err, ErrManagedAdmission) {
		t.Fatalf("wrong-generation reopen changed seal = (present=%t, %v)", entered != nil, err)
	}
	gate.reopen(generation)
}

func TestControllerAdmissionGateWorkerWaitsForExactGenerationReopen(t *testing.T) {
	gate := newControllerAdmissionGate()
	generation, err := gate.seal(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	workerEntered := make(chan func(), 1)
	workerFailed := make(chan error, 1)
	go func() {
		release, err := gate.EnterWorker(context.Background())
		if err != nil {
			workerFailed <- err
			return
		}
		workerEntered <- release
	}()
	assertWorkerAdmissionWaiting(t, workerEntered, workerFailed)

	gate.reopen(generation + 1)
	assertWorkerAdmissionWaiting(t, workerEntered, workerFailed)
	if entered, err := gate.Enter(context.Background()); entered != nil ||
		!errors.Is(err, ErrManagedAdmission) {
		t.Fatalf("route Enter() after wrong-generation reopen = (present=%t, %v)",
			entered != nil, err)
	}

	gate.reopen(generation)
	var release func()
	select {
	case release = <-workerEntered:
	case err := <-workerFailed:
		t.Fatalf("EnterWorker() after exact reopen error = %v", err)
	case <-time.After(time.Second):
		t.Fatal("worker did not enter after exact generation reopened")
	}

	type sealResult struct {
		generation uint64
		err        error
	}
	sealed := make(chan sealResult, 1)
	go func() {
		nextGeneration, err := gate.seal(context.Background())
		sealed <- sealResult{generation: nextGeneration, err: err}
	}()
	waitAdmissionSealed(t, gate)
	select {
	case result := <-sealed:
		t.Fatalf("seal returned before worker admission drained: %#v", result)
	default:
	}
	release()
	result := <-sealed
	if result.err != nil || result.generation == generation {
		t.Fatalf("next seal() = (%d, %v)", result.generation, result.err)
	}
	gate.reopen(result.generation)
}

func TestControllerAdmissionGateWorkerCancellationLeavesRetainedSeal(t *testing.T) {
	gate := newControllerAdmissionGate()
	generation, err := gate.seal(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		release, err := gate.EnterWorker(ctx)
		if release != nil {
			release()
			t.Errorf("cancelled EnterWorker() returned a release function")
		}
		result <- err
	}()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrManagedAdmission) {
			t.Fatalf("cancelled EnterWorker() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled worker remained blocked by retained seal")
	}
	if entered, err := gate.Enter(context.Background()); entered != nil ||
		!errors.Is(err, ErrManagedAdmission) {
		t.Fatalf("cancelled worker changed retained seal = (present=%t, %v)",
			entered != nil, err)
	}
	gate.reopen(generation)
}

func TestControllerAdmissionGateWorkerDoesNotMissConcurrentReopen(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		gate := newControllerAdmissionGate()
		generation, err := gate.seal(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		result := make(chan error, 1)
		go func() {
			release, err := gate.EnterWorker(context.Background())
			if release != nil {
				release()
			}
			result <- err
		}()
		gate.reopen(generation)
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("iteration %d EnterWorker() error = %v", iteration, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("iteration %d worker missed concurrent reopen", iteration)
		}
	}
}

func TestControllerAdmissionServiceWrapsEveryManagedRoute(t *testing.T) {
	gate := newControllerAdmissionGate()
	next := &recordingAdmissionService{}
	service := controllerAdmissionService{gate: gate, next: next}
	ctx := context.Background()
	metadata := localapi.RequestMetadata{}
	if _, apiErr := service.HookCheck(ctx, metadata, localapi.HookCheckRequest{}); apiErr != nil {
		t.Fatal(apiErr)
	}
	if _, apiErr := service.AgentCurrent(ctx, metadata, localapi.AgentCurrentRequest{}); apiErr != nil {
		t.Fatal(apiErr)
	}
	if _, apiErr := service.TeamworkAction(ctx, metadata, localapi.TeamworkActionRequest{}); apiErr != nil {
		t.Fatal(apiErr)
	}
	if _, apiErr := service.AgentResolve(ctx, metadata, localapi.AgentResolveRequest{}); apiErr != nil {
		t.Fatal(apiErr)
	}
	if next.calls.Load() != 4 {
		t.Fatalf("open admission calls = %d", next.calls.Load())
	}
	generation, err := gate.seal(ctx)
	if err != nil {
		t.Fatal(err)
	}
	checks := []func() *localapi.APIError{
		func() *localapi.APIError {
			_, apiErr := service.HookCheck(ctx, metadata, localapi.HookCheckRequest{})
			return apiErr
		},
		func() *localapi.APIError {
			_, apiErr := service.AgentCurrent(ctx, metadata, localapi.AgentCurrentRequest{})
			return apiErr
		},
		func() *localapi.APIError {
			_, apiErr := service.TeamworkAction(ctx, metadata, localapi.TeamworkActionRequest{})
			return apiErr
		},
		func() *localapi.APIError {
			_, apiErr := service.AgentResolve(ctx, metadata, localapi.AgentResolveRequest{})
			return apiErr
		},
	}
	for index, check := range checks {
		if apiErr := check(); apiErr == nil || apiErr.Code != localapi.CodeMnemondUnavailable {
			t.Fatalf("sealed route %d error = %#v", index, apiErr)
		}
	}
	if next.calls.Load() != 4 {
		t.Fatalf("sealed admission reached service: calls=%d", next.calls.Load())
	}
	gate.reopen(generation)
}

func TestControllerAdmissionServiceKeepsHandlerEnteredUntilStoreWorkReturns(t *testing.T) {
	gate := newControllerAdmissionGate()
	next := &blockingAdmissionService{entered: make(chan struct{}), proceed: make(chan struct{})}
	service := controllerAdmissionService{gate: gate, next: next}
	handlerDone := make(chan *localapi.APIError, 1)
	go func() {
		_, apiErr := service.HookCheck(context.Background(), localapi.RequestMetadata{},
			localapi.HookCheckRequest{})
		handlerDone <- apiErr
	}()
	select {
	case <-next.entered:
	case <-time.After(time.Second):
		t.Fatal("managed handler did not enter its Store-facing service")
	}
	type sealResult struct {
		generation uint64
		err        error
	}
	sealed := make(chan sealResult, 1)
	go func() {
		generation, err := gate.seal(context.Background())
		sealed <- sealResult{generation: generation, err: err}
	}()
	waitAdmissionSealed(t, gate)
	select {
	case result := <-sealed:
		t.Fatalf("admission seal passed an in-flight handler: %#v", result)
	default:
	}
	close(next.proceed)
	if apiErr := <-handlerDone; apiErr != nil {
		t.Fatalf("managed handler error = %#v", apiErr)
	}
	result := <-sealed
	if result.err != nil || result.generation == 0 {
		t.Fatalf("seal after handler drain = (%d, %v)", result.generation, result.err)
	}
	gate.reopen(result.generation)
}

func waitAdmissionSealed(t *testing.T, gate *controllerAdmissionGate) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		gate.mu.Lock()
		sealed := gate.sealed
		gate.mu.Unlock()
		if sealed {
			return
		}
		select {
		case <-deadline:
			t.Fatal("admission gate was not sealed")
		default:
			runtime.Gosched()
		}
	}
}

func assertWorkerAdmissionWaiting(t *testing.T, entered <-chan func(), failed <-chan error) {
	t.Helper()
	select {
	case release := <-entered:
		if release != nil {
			release()
		}
		t.Fatal("worker entered while admission remained sealed")
	case err := <-failed:
		t.Fatalf("worker admission failed while waiting: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
}

type recordingAdmissionService struct{ calls atomic.Int32 }

type blockingAdmissionService struct {
	entered chan struct{}
	proceed chan struct{}
}

func (service *blockingAdmissionService) HookCheck(context.Context, localapi.RequestMetadata,
	localapi.HookCheckRequest,
) (localapi.HookCheckResponse, *localapi.APIError) {
	close(service.entered)
	<-service.proceed
	return localapi.HookCheckResponse{}, nil
}

func (*blockingAdmissionService) AgentCurrent(context.Context, localapi.RequestMetadata,
	localapi.AgentCurrentRequest,
) (localapi.AgentCurrentResponse, *localapi.APIError) {
	return localapi.AgentCurrentResponse{}, localapi.NewAPIError(localapi.CodeInternal,
		"unexpected AgentCurrent")
}

func (*blockingAdmissionService) TeamworkAction(context.Context, localapi.RequestMetadata,
	localapi.TeamworkActionRequest,
) (localapi.OperationResponse, *localapi.APIError) {
	return localapi.OperationResponse{}, localapi.NewAPIError(localapi.CodeInternal,
		"unexpected TeamworkAction")
}

func (*blockingAdmissionService) AgentResolve(context.Context, localapi.RequestMetadata,
	localapi.AgentResolveRequest,
) (localapi.OperationResponse, *localapi.APIError) {
	return localapi.OperationResponse{}, localapi.NewAPIError(localapi.CodeInternal,
		"unexpected AgentResolve")
}

func (service *recordingAdmissionService) HookCheck(context.Context, localapi.RequestMetadata,
	localapi.HookCheckRequest,
) (localapi.HookCheckResponse, *localapi.APIError) {
	service.calls.Add(1)
	return localapi.HookCheckResponse{}, nil
}

func (service *recordingAdmissionService) AgentCurrent(context.Context, localapi.RequestMetadata,
	localapi.AgentCurrentRequest,
) (localapi.AgentCurrentResponse, *localapi.APIError) {
	service.calls.Add(1)
	return localapi.AgentCurrentResponse{}, nil
}

func (service *recordingAdmissionService) TeamworkAction(context.Context, localapi.RequestMetadata,
	localapi.TeamworkActionRequest,
) (localapi.OperationResponse, *localapi.APIError) {
	service.calls.Add(1)
	return localapi.OperationResponse{}, nil
}

func (service *recordingAdmissionService) AgentResolve(context.Context, localapi.RequestMetadata,
	localapi.AgentResolveRequest,
) (localapi.OperationResponse, *localapi.APIError) {
	service.calls.Add(1)
	return localapi.OperationResponse{}, nil
}

var _ ManagedAdmission = (*controllerAdmissionGate)(nil)
