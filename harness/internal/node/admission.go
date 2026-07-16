package node

import (
	"context"
	"errors"
	"sync"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
)

var ErrManagedAdmission = errors.New("managed Agent admission is sealed")

// ManagedAdmission is the narrow fence surface shared by controller routes
// and future managed workers. A successful Enter must be paired with exactly
// one release call around the complete Store-facing operation.
type ManagedAdmission interface {
	Enter(context.Context) (release func(), err error)
}

type controllerAdmissionGate struct {
	mu         sync.Mutex
	active     uint64
	drained    chan struct{}
	generation uint64
	sealed     bool
}

func newControllerAdmissionGate() *controllerAdmissionGate {
	drained := make(chan struct{})
	close(drained)
	return &controllerAdmissionGate{drained: drained}
}

func (gate *controllerAdmissionGate) Enter(ctx context.Context) (func(), error) {
	if gate == nil || ctx == nil {
		return nil, fmtAdmissionError(errors.New("gate or context is unavailable"))
	}
	if err := ctx.Err(); err != nil {
		return nil, fmtAdmissionError(err)
	}
	gate.mu.Lock()
	if gate.sealed {
		gate.mu.Unlock()
		return nil, ErrManagedAdmission
	}
	if gate.active == 0 {
		gate.drained = make(chan struct{})
	}
	gate.active++
	gate.mu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(gate.leave)
	}
	if err := ctx.Err(); err != nil {
		release()
		return nil, fmtAdmissionError(err)
	}
	return release, nil
}

func (gate *controllerAdmissionGate) leave() {
	gate.mu.Lock()
	defer gate.mu.Unlock()
	if gate.active == 0 {
		return
	}
	gate.active--
	if gate.active == 0 {
		close(gate.drained)
	}
}

func (gate *controllerAdmissionGate) seal(ctx context.Context) (uint64, error) {
	if gate == nil || ctx == nil {
		return 0, fmtAdmissionError(errors.New("gate or context is unavailable"))
	}
	if err := ctx.Err(); err != nil {
		return 0, fmtAdmissionError(err)
	}
	gate.mu.Lock()
	if gate.sealed {
		gate.mu.Unlock()
		return 0, ErrManagedAdmission
	}
	gate.sealed = true
	gate.generation++
	generation := gate.generation
	drained := gate.drained
	gate.mu.Unlock()

	select {
	case <-drained:
		return generation, nil
	case <-ctx.Done():
		gate.reopen(generation)
		return 0, fmtAdmissionError(ctx.Err())
	}
}

func (gate *controllerAdmissionGate) reopen(generation uint64) {
	if gate == nil || generation == 0 {
		return
	}
	gate.mu.Lock()
	if gate.sealed && gate.generation == generation {
		gate.sealed = false
	}
	gate.mu.Unlock()
}

func fmtAdmissionError(err error) error {
	if err == nil {
		return ErrManagedAdmission
	}
	return errors.Join(ErrManagedAdmission, err)
}

type controllerAdmissionService struct {
	gate ManagedAdmission
	next localapi.Service
}

func (service controllerAdmissionService) HookCheck(ctx context.Context,
	metadata localapi.RequestMetadata, request localapi.HookCheckRequest,
) (localapi.HookCheckResponse, *localapi.APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return localapi.HookCheckResponse{}, apiErr
	}
	defer release()
	return service.next.HookCheck(ctx, metadata, request)
}

func (service controllerAdmissionService) AgentCurrent(ctx context.Context,
	metadata localapi.RequestMetadata, request localapi.AgentCurrentRequest,
) (localapi.AgentCurrentResponse, *localapi.APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return localapi.AgentCurrentResponse{}, apiErr
	}
	defer release()
	return service.next.AgentCurrent(ctx, metadata, request)
}

func (service controllerAdmissionService) TeamworkAction(ctx context.Context,
	metadata localapi.RequestMetadata, request localapi.TeamworkActionRequest,
) (localapi.OperationResponse, *localapi.APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return localapi.OperationResponse{}, apiErr
	}
	defer release()
	return service.next.TeamworkAction(ctx, metadata, request)
}

func (service controllerAdmissionService) AgentResolve(ctx context.Context,
	metadata localapi.RequestMetadata, request localapi.AgentResolveRequest,
) (localapi.OperationResponse, *localapi.APIError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return localapi.OperationResponse{}, apiErr
	}
	defer release()
	return service.next.AgentResolve(ctx, metadata, request)
}

func enterControllerAdmission(ctx context.Context,
	gate ManagedAdmission,
) (func(), *localapi.APIError) {
	if gate == nil {
		return nil, localapi.NewAPIError(localapi.CodeInternal,
			"managed admission gate is unavailable")
	}
	release, err := gate.Enter(ctx)
	if err != nil || release == nil {
		return nil, localapi.NewAPIError(localapi.CodeMnemondUnavailable,
			"managed admission is stopping")
	}
	return release, nil
}

var _ localapi.Service = controllerAdmissionService{}
