package node

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

type componentRestartPolicy uint8

const (
	componentRestartNever componentRestartPolicy = iota + 1
)

// componentResourceBudget states the in-memory concurrency admitted by one
// component. The component remains responsible for enforcing the same bound at
// its concrete admission surface; the Supervisor validates that no component
// leaves its concurrency unbounded.
type componentResourceBudget struct {
	MaxConcurrent uint32
}

// componentSpec is the immutable lifecycle contract consumed by the Node
// Supervisor. Readiness must be bounded by ctx and may not retain callbacks or
// perform work while a Supervisor lock is held. Shutdown must be idempotent and
// is called after the component context is cancelled.
type componentSpec struct {
	Name         string
	Dependencies []string
	Readiness    func(context.Context) error
	Run          func(context.Context) error
	Shutdown     func(context.Context) error
	Restart      componentRestartPolicy
	Resources    componentResourceBudget
}

// nodeSupervisor owns one goroutine per component, deterministic dependency
// startup, cancellation, reverse-order shutdown, and a wait path for every
// component. Domain retries remain inside the component implementation.
type nodeSupervisor struct {
	components []componentSpec
}

type supervisedComponent struct {
	spec   componentSpec
	cancel context.CancelFunc
	done   chan struct{}
}

// supervisorFailure marks a Supervisor-owned terminal cause. Parent and stop
// cancellation remain clean lifecycle requests, while component and readiness
// failures are returned to the caller. context cancellation atomically retains
// the first cause across all four sources.
type supervisorFailure struct {
	cause error
}

func (failure *supervisorFailure) Error() string {
	return failure.cause.Error()
}

func (failure *supervisorFailure) Unwrap() error {
	return failure.cause
}

func newNodeSupervisor(specs []componentSpec) (*nodeSupervisor, error) {
	if len(specs) == 0 {
		return nil, errors.New("node Supervisor requires at least one component")
	}
	byName := make(map[string]componentSpec, len(specs))
	for _, candidate := range specs {
		spec, err := validateComponentSpec(candidate)
		if err != nil {
			return nil, err
		}
		if _, duplicate := byName[spec.Name]; duplicate {
			return nil, fmt.Errorf("node Supervisor component %q is duplicated", spec.Name)
		}
		byName[spec.Name] = spec
	}
	for _, spec := range byName {
		for _, dependency := range spec.Dependencies {
			if _, exists := byName[dependency]; !exists {
				return nil, fmt.Errorf("node Supervisor component %q depends on unknown %q",
					spec.Name, dependency)
			}
		}
	}
	ordered, err := orderComponentSpecs(byName)
	if err != nil {
		return nil, err
	}
	return &nodeSupervisor{components: ordered}, nil
}

func validateComponentSpec(candidate componentSpec) (componentSpec, error) {
	if candidate.Name == "" || candidate.Readiness == nil || candidate.Run == nil ||
		candidate.Shutdown == nil {
		return componentSpec{}, errors.New(
			"node Supervisor component requires name, readiness, run, and shutdown")
	}
	if candidate.Restart != componentRestartNever {
		return componentSpec{}, fmt.Errorf("node Supervisor component %q has unsupported restart policy",
			candidate.Name)
	}
	if candidate.Resources.MaxConcurrent == 0 {
		return componentSpec{}, fmt.Errorf("node Supervisor component %q has unbounded concurrency",
			candidate.Name)
	}
	dependencies := append([]string(nil), candidate.Dependencies...)
	sort.Strings(dependencies)
	for index, dependency := range dependencies {
		if dependency == candidate.Name {
			return componentSpec{}, fmt.Errorf("node Supervisor component %q depends on itself",
				candidate.Name)
		}
		if index != 0 && dependency == dependencies[index-1] {
			return componentSpec{}, fmt.Errorf("node Supervisor component %q repeats dependency %q",
				candidate.Name, dependency)
		}
	}
	candidate.Dependencies = dependencies
	return candidate, nil
}

func orderComponentSpecs(byName map[string]componentSpec) ([]componentSpec, error) {
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	state := make(map[string]uint8, len(names))
	ordered := make([]componentSpec, 0, len(names))
	var visit func(string) error
	visit = func(name string) error {
		switch state[name] {
		case 1:
			return fmt.Errorf("node Supervisor component dependency cycle reaches %q", name)
		case 2:
			return nil
		}
		state[name] = 1
		spec := byName[name]
		for _, dependency := range spec.Dependencies {
			if err := visit(dependency); err != nil {
				return err
			}
		}
		state[name] = 2
		ordered = append(ordered, spec)
		return nil
	}
	for _, name := range names {
		if err := visit(name); err != nil {
			return nil, err
		}
	}
	return ordered, nil
}

// Run serves until caller cancellation, an explicit stop request, or the first
// component exit. Domain components that intentionally remain degraded adapt
// their terminal domain result into a lifecycle that waits for cancellation.
func (supervisor *nodeSupervisor) Run(ctx context.Context, stop <-chan struct{}) error {
	if supervisor == nil || len(supervisor.components) == 0 || ctx == nil {
		return errors.New("node Supervisor is unavailable")
	}
	runCtx, cancelAll := context.WithCancelCause(ctx)
	watchDone := watchSupervisorStop(runCtx, stop, cancelAll)
	started := make([]*supervisedComponent, 0, len(supervisor.components))

	for _, spec := range supervisor.components {
		if runCtx.Err() != nil {
			break
		}
		// Supervisor, rather than parent-context propagation, owns component
		// cancellation. This makes cancellation itself follow reverse dependency
		// order instead of merely calling Shutdown callbacks in reverse.
		componentCtx, cancel := context.WithCancel(context.WithoutCancel(runCtx))
		runtime := &supervisedComponent{spec: spec, cancel: cancel, done: make(chan struct{})}
		started = append(started, runtime)
		go runSupervisedComponent(componentCtx, runtime, cancelAll)
		if err := spec.Readiness(runCtx); err != nil {
			cancelAll(&supervisorFailure{cause: fmt.Errorf(
				"node Supervisor component %q readiness: %w", spec.Name, err)})
			break
		}
	}

	firstErr := waitForSupervisorStop(runCtx)
	cancelAll(nil)
	shutdownErr := shutdownSupervisedComponents(ctx, started)
	<-watchDone
	return errors.Join(firstErr, shutdownErr)
}

func watchSupervisorStop(ctx context.Context, stop <-chan struct{},
	cancel context.CancelCauseFunc,
) <-chan struct{} {
	done := make(chan struct{})
	if stop == nil {
		close(done)
		return done
	}
	go func() {
		defer close(done)
		select {
		case <-stop:
			cancel(nil)
		case <-ctx.Done():
		}
	}()
	return done
}

func runSupervisedComponent(ctx context.Context, runtime *supervisedComponent,
	cancel context.CancelCauseFunc,
) {
	err := runtime.spec.Run(ctx)
	cancel(&supervisorFailure{cause: supervisedComponentExitError(runtime.spec.Name, err)})
	close(runtime.done)
}

func waitForSupervisorStop(ctx context.Context) error {
	<-ctx.Done()
	var failure *supervisorFailure
	if errors.As(context.Cause(ctx), &failure) {
		return failure.cause
	}
	return nil
}

func supervisedComponentExitError(name string, err error) error {
	if err == nil {
		err = errors.New("component stopped before shutdown")
	}
	return fmt.Errorf("node Supervisor component %q: %w", name, err)
}

func shutdownSupervisedComponents(caller context.Context, started []*supervisedComponent) error {
	shutdownCtx := context.Background()
	if caller != nil {
		shutdownCtx = context.WithoutCancel(caller)
	}
	var result error
	for index := len(started) - 1; index >= 0; index-- {
		runtime := started[index]
		runtime.cancel()
		shutdownErr := runtime.spec.Shutdown(shutdownCtx)
		<-runtime.done
		result = errors.Join(result, shutdownErr)
	}
	return result
}
