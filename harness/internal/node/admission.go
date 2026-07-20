package node

import (
	"context"
	"errors"
	"sync"

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
)

var ErrManagedAdmission = errors.New("managed Agent admission is sealed")

// ManagedAdmission is the narrow fence surface shared by controller routes
// and managed workers. Route admission rejects a sealed gate immediately,
// while worker admission waits for the observed seal generation to reopen.
// Every successful entry must be paired with exactly one release call around
// the complete Store-facing operation.
type ManagedAdmission interface {
	Enter(context.Context) (release func(), err error)
	EnterWorker(context.Context) (release func(), err error)
}

type controllerAdmissionGate struct {
	mu         sync.Mutex
	active     uint64
	drained    chan struct{}
	generation uint64
	reopened   chan struct{}
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
	release := gate.enterLocked()
	gate.mu.Unlock()

	if err := ctx.Err(); err != nil {
		release()
		return nil, fmtAdmissionError(err)
	}
	return release, nil
}

// EnterWorker waits while mutation admission is sealed. Waiting is tied to
// the exact seal generation observed under gate.mu, so an unrelated reopen
// cannot admit work and an exact reopen cannot be missed between observation
// and waiting. A new seal established before the worker acquires admission is
// observed in the next loop iteration.
func (gate *controllerAdmissionGate) EnterWorker(ctx context.Context) (func(), error) {
	if gate == nil || ctx == nil {
		return nil, fmtAdmissionError(errors.New("gate or context is unavailable"))
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmtAdmissionError(err)
		}

		gate.mu.Lock()
		if !gate.sealed {
			release := gate.enterLocked()
			gate.mu.Unlock()
			if err := ctx.Err(); err != nil {
				release()
				return nil, fmtAdmissionError(err)
			}
			return release, nil
		}
		reopened := gate.reopened
		gate.mu.Unlock()

		select {
		case <-reopened:
			continue
		case <-ctx.Done():
			return nil, fmtAdmissionError(ctx.Err())
		}
	}
}

// enterLocked records one admitted operation. gate.mu must be held and the
// caller must have established that the gate is open.
func (gate *controllerAdmissionGate) enterLocked() func() {
	if gate.active == 0 {
		gate.drained = make(chan struct{})
	}
	gate.active++

	var once sync.Once
	return func() {
		once.Do(gate.leave)
	}
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
	gate.reopened = make(chan struct{})
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
		reopened := gate.reopened
		gate.reopened = nil
		close(reopened)
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
	next ManagedControlService
}

func (service controllerAdmissionService) HookCheck(ctx context.Context,
	metadata ControlMetadata,
) (HookCheckResponse, *ControlError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return HookCheckResponse{}, apiErr
	}
	defer release()
	return service.next.HookCheck(ctx, metadata)
}

func (service controllerAdmissionService) AgentCurrent(ctx context.Context,
	metadata ControlMetadata,
) (AgentCurrentResponse, *ControlError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return AgentCurrentResponse{}, apiErr
	}
	defer release()
	return service.next.AgentCurrent(ctx, metadata)
}

func (service controllerAdmissionService) TeamworkAction(ctx context.Context,
	metadata ControlMetadata, request TeamworkActionRequest,
) (OperationResponse, *ControlError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return OperationResponse{}, apiErr
	}
	defer release()
	return service.next.TeamworkAction(ctx, metadata, request)
}

func (service controllerAdmissionService) AgentResolve(ctx context.Context,
	metadata ControlMetadata, request AgentResolveRequest,
) (OperationResponse, *ControlError) {
	release, apiErr := enterControllerAdmission(ctx, service.gate)
	if apiErr != nil {
		return OperationResponse{}, apiErr
	}
	defer release()
	return service.next.AgentResolve(ctx, metadata, request)
}

func enterControllerAdmission(ctx context.Context,
	gate ManagedAdmission,
) (func(), *ControlError) {
	if gate == nil {
		return nil, agent.NewControlError(agent.CodeInternal,
			"managed admission gate is unavailable")
	}
	release, err := gate.Enter(ctx)
	if err != nil || release == nil {
		return nil, agent.NewControlError(agent.CodeMnemondUnavailable,
			"managed admission is stopping")
	}
	return release, nil
}

var _ ManagedControlService = controllerAdmissionService{}
