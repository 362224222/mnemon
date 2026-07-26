package node

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNodeSupervisorStartsDependenciesAndWaitsForReverseShutdown(t *testing.T) {
	t.Parallel()
	var logMu sync.Mutex
	var log []string
	record := func(entry string) {
		logMu.Lock()
		log = append(log, entry)
		logMu.Unlock()
	}
	component := func(name string, dependencies ...string) componentSpec {
		started := make(chan struct{})
		cancelled := make(chan struct{})
		release := make(chan struct{})
		return componentSpec{Name: name, Dependencies: dependencies,
			Readiness: func(ctx context.Context) error {
				select {
				case <-started:
					record("ready:" + name)
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
			Run: func(ctx context.Context) error {
				record("run:" + name)
				close(started)
				<-ctx.Done()
				record("cancel:" + name)
				close(cancelled)
				<-release
				record("exit:" + name)
				return nil
			},
			Shutdown: func(context.Context) error {
				<-cancelled
				record("shutdown:" + name)
				close(release)
				return nil
			}, Restart: componentRestartNever,
			Resources: componentResourceBudget{MaxConcurrent: 1}}
	}
	supervisor, err := newNodeSupervisor([]componentSpec{
		component("worker", "http"), component("store"), component("http", "store"),
	})
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- supervisor.Run(context.Background(), stop, newGracefulShutdown(time.Second))
	}()
	wantStarted := []string{"run:store", "ready:store", "run:http", "ready:http",
		"run:worker", "ready:worker"}
	waitSupervisorLog(t, &logMu, &log, len(wantStarted))
	close(stop)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Supervisor did not wait for reverse shutdown")
	}
	want := append(wantStarted, "cancel:worker", "shutdown:worker", "exit:worker", "cancel:http",
		"shutdown:http", "exit:http", "cancel:store", "shutdown:store", "exit:store")
	logMu.Lock()
	got := append([]string(nil), log...)
	logMu.Unlock()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lifecycle order = %v, want %v", got, want)
	}
}

func TestNodeSupervisorPropagatesFirstFatalErrorAndCancelsDependents(t *testing.T) {
	t.Parallel()
	fail := make(chan struct{})
	fatalStarted := make(chan struct{})
	workerStarted := make(chan struct{})
	workerCancelled := make(chan struct{})
	workerRelease := make(chan struct{})
	fatal := componentSpec{Name: "http", Readiness: channelReadiness(fatalStarted),
		Run: func(context.Context) error {
			close(fatalStarted)
			<-fail
			return errors.New("accept failed")
		}, Shutdown: func(context.Context) error { return nil }, Restart: componentRestartNever,
		Resources: componentResourceBudget{MaxConcurrent: 1}}
	worker := componentSpec{Name: "worker", Dependencies: []string{"http"},
		Readiness: channelReadiness(workerStarted), Run: func(ctx context.Context) error {
			close(workerStarted)
			<-ctx.Done()
			close(workerCancelled)
			<-workerRelease
			return nil
		}, Shutdown: func(context.Context) error {
			close(workerRelease)
			return nil
		}, Restart: componentRestartNever,
		Resources: componentResourceBudget{MaxConcurrent: 1}}
	supervisor, err := newNodeSupervisor([]componentSpec{worker, fatal})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- supervisor.Run(context.Background(), nil, newGracefulShutdown(time.Second))
	}()
	select {
	case <-workerStarted:
	case <-time.After(time.Second):
		t.Fatal("dependent worker did not start after HTTP readiness")
	}
	close(fail)
	select {
	case <-workerCancelled:
	case <-time.After(time.Second):
		t.Fatal("fatal component error did not cancel dependent")
	}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), `component "http"`) ||
			!strings.Contains(err.Error(), "accept failed") {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Supervisor did not propagate fatal component error")
	}
}

func TestNodeSupervisorPropagatesComponentExitDuringReadiness(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		runErr  error
		wantErr string
	}{
		{name: "fatal exit", runErr: errors.New("listen failed"), wantErr: "listen failed"},
		{name: "clean premature exit", wantErr: "component stopped before shutdown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			readinessEntered := make(chan struct{})
			exit := make(chan struct{})
			shutdownCalled := make(chan struct{})
			dependentStarted := make(chan struct{})
			primary := componentSpec{Name: "primary",
				Readiness: func(ctx context.Context) error {
					close(readinessEntered)
					<-ctx.Done()
					return ctx.Err()
				},
				Run: func(context.Context) error {
					<-exit
					return test.runErr
				},
				Shutdown: func(context.Context) error {
					close(shutdownCalled)
					return nil
				}, Restart: componentRestartNever,
				Resources: componentResourceBudget{MaxConcurrent: 1}}
			dependent := componentSpec{Name: "dependent", Dependencies: []string{"primary"},
				Readiness: func(context.Context) error { return nil },
				Run: func(ctx context.Context) error {
					close(dependentStarted)
					<-ctx.Done()
					return nil
				}, Shutdown: func(context.Context) error { return nil },
				Restart:   componentRestartNever,
				Resources: componentResourceBudget{MaxConcurrent: 1}}
			supervisor, err := newNodeSupervisor([]componentSpec{dependent, primary})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				done <- supervisor.Run(context.Background(), nil, newGracefulShutdown(time.Second))
			}()
			awaitSupervisorSignal(t, readinessEntered, "primary readiness")
			close(exit)
			select {
			case err := <-done:
				if err == nil || !strings.Contains(err.Error(), `component "primary"`) ||
					!strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("Run() error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Supervisor did not propagate an exit during readiness")
			}
			awaitSupervisorSignal(t, shutdownCalled, "primary shutdown")
			select {
			case <-dependentStarted:
				t.Fatal("dependent started before primary readiness")
			default:
			}
		})
	}
}

func TestNodeSupervisorRollsBackReadinessFailure(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	shutdownCalled := make(chan struct{})
	dependentStarted := make(chan struct{})
	primary := componentSpec{Name: "primary",
		Readiness: func(ctx context.Context) error {
			select {
			case <-started:
				return errors.New("probe failed")
			case <-ctx.Done():
				return ctx.Err()
			}
		}, Run: func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return nil
		}, Shutdown: func(context.Context) error {
			close(shutdownCalled)
			return nil
		}, Restart: componentRestartNever,
		Resources: componentResourceBudget{MaxConcurrent: 1}}
	dependent := componentSpec{Name: "dependent", Dependencies: []string{"primary"},
		Readiness: func(context.Context) error { return nil },
		Run: func(ctx context.Context) error {
			close(dependentStarted)
			<-ctx.Done()
			return nil
		}, Shutdown: func(context.Context) error { return nil }, Restart: componentRestartNever,
		Resources: componentResourceBudget{MaxConcurrent: 1}}
	supervisor, err := newNodeSupervisor([]componentSpec{dependent, primary})
	if err != nil {
		t.Fatal(err)
	}
	err = supervisor.Run(context.Background(), nil, newGracefulShutdown(time.Second))
	if err == nil || !strings.Contains(err.Error(), `component "primary" readiness: probe failed`) {
		t.Fatalf("Run() error = %v", err)
	}
	awaitSupervisorSignal(t, shutdownCalled, "primary shutdown")
	select {
	case <-dependentStarted:
		t.Fatal("dependent started after primary readiness failed")
	default:
	}
}

func TestNodeSupervisorAggregatesShutdownErrorsAndAcceptsCallerCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{}, 2)
	shutdownCalled := make(chan string, 2)
	primaryShutdownErr := errors.New("primary shutdown failed")
	dependentShutdownErr := errors.New("dependent shutdown failed")
	component := func(name string, shutdownErr error, dependencies ...string) componentSpec {
		return componentSpec{Name: name, Dependencies: dependencies,
			Readiness: func(context.Context) error {
				started <- struct{}{}
				return nil
			}, Run: func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			}, Shutdown: func(context.Context) error {
				shutdownCalled <- name
				return shutdownErr
			}, Restart: componentRestartNever,
			Resources: componentResourceBudget{MaxConcurrent: 1}}
	}
	supervisor, err := newNodeSupervisor([]componentSpec{
		component("primary", primaryShutdownErr),
		component("dependent", dependentShutdownErr, "primary"),
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx, nil, newGracefulShutdown(time.Second)) }()
	awaitSupervisorSignal(t, started, "primary readiness")
	awaitSupervisorSignal(t, started, "dependent readiness")
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, primaryShutdownErr) || !errors.Is(err, dependentShutdownErr) {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Supervisor did not finish after caller cancellation")
	}
	if first, second := <-shutdownCalled, <-shutdownCalled; first != "dependent" || second != "primary" {
		t.Fatalf("shutdown order = %q, %q", first, second)
	}
}

func TestNodeSupervisorTreatsExternalStopsAsClean(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		stop func(context.CancelFunc, chan struct{})
	}{
		{name: "caller cancellation", stop: func(cancel context.CancelFunc, _ chan struct{}) {
			cancel()
		}},
		{name: "stop request", stop: func(_ context.CancelFunc, stop chan struct{}) {
			close(stop)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			stop := make(chan struct{})
			started := make(chan struct{})
			shutdownCalled := make(chan struct{})
			spec := componentSpec{Name: "primary", Readiness: channelReadiness(started),
				Run: func(ctx context.Context) error {
					close(started)
					<-ctx.Done()
					return nil
				}, Shutdown: func(context.Context) error {
					close(shutdownCalled)
					return nil
				}, Restart: componentRestartNever,
				Resources: componentResourceBudget{MaxConcurrent: 1}}
			supervisor, err := newNodeSupervisor([]componentSpec{spec})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				done <- supervisor.Run(ctx, stop, newGracefulShutdown(time.Second))
			}()
			awaitSupervisorSignal(t, started, "component start")
			test.stop(cancel, stop)
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("Run() error = %v, want nil", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Supervisor did not accept external stop")
			}
			awaitSupervisorSignal(t, shutdownCalled, "component shutdown")
		})
	}
}

func TestNodeSupervisorBoundsBlockedShutdownAndAttemptsReverseOrder(t *testing.T) {
	t.Parallel()
	started := make(chan struct{}, 2)
	attempted := make(chan string, 2)
	component := func(name string, block bool, dependencies ...string) componentSpec {
		return componentSpec{Name: name, Dependencies: dependencies,
			Readiness: func(context.Context) error {
				started <- struct{}{}
				return nil
			},
			Run: func(ctx context.Context) error {
				<-ctx.Done()
				return nil
			},
			Shutdown: func(ctx context.Context) error {
				attempted <- name
				if block {
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			},
			Restart:   componentRestartNever,
			Resources: componentResourceBudget{MaxConcurrent: 1}}
	}
	supervisor, err := newNodeSupervisor([]componentSpec{
		component("primary", false),
		component("dependent", true, "primary"),
	})
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- supervisor.Run(context.Background(), stop,
			newGracefulShutdown(50*time.Millisecond))
	}()
	awaitSupervisorSignal(t, started, "primary readiness")
	awaitSupervisorSignal(t, started, "dependent readiness")
	close(stop)

	select {
	case err := <-done:
		if !errors.Is(err, ErrGracefulShutdownDeadline) ||
			!errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Run() error = %v, want graceful shutdown deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Supervisor exceeded the process shutdown budget")
	}
	if first, second := <-attempted, <-attempted; first != "dependent" || second != "primary" {
		t.Fatalf("shutdown attempts = %q, %q, want dependent then primary", first, second)
	}
}

func TestNodeSupervisorBoundsBlockedRunJoin(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	shutdownCalled := make(chan struct{})
	release := make(chan struct{})
	exited := make(chan struct{})
	spec := componentSpec{Name: "blocked-run",
		Readiness: channelReadiness(started),
		Run: func(context.Context) error {
			close(started)
			<-release
			close(exited)
			return nil
		},
		Shutdown: func(context.Context) error {
			close(shutdownCalled)
			return nil
		},
		Restart:   componentRestartNever,
		Resources: componentResourceBudget{MaxConcurrent: 1}}
	supervisor, err := newNodeSupervisor([]componentSpec{spec})
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- supervisor.Run(context.Background(), stop,
			newGracefulShutdown(50*time.Millisecond))
	}()
	awaitSupervisorSignal(t, started, "blocked component start")
	close(stop)
	awaitSupervisorSignal(t, shutdownCalled, "blocked component shutdown")
	select {
	case err := <-done:
		if !errors.Is(err, ErrGracefulShutdownDeadline) ||
			!strings.Contains(err.Error(), `join component "blocked-run"`) {
			t.Fatalf("Run() error = %v, want blocked Run diagnostic", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Supervisor did not bound a blocked Run join")
	}
	close(release)
	awaitSupervisorSignal(t, exited, "blocked component release")
}

func TestWaitForSupervisorStopPreservesFirstTerminationCause(t *testing.T) {
	t.Parallel()
	componentErr := errors.New("component failed")
	tests := []struct {
		name      string
		terminate func(context.CancelCauseFunc)
		wantErr   error
	}{
		{name: "external stop wins", terminate: func(cancel context.CancelCauseFunc) {
			cancel(nil)
			cancel(&supervisorFailure{cause: componentErr})
		}},
		{name: "component failure wins", terminate: func(cancel context.CancelCauseFunc) {
			cancel(&supervisorFailure{cause: componentErr})
			cancel(nil)
		}, wantErr: componentErr},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancelCause(context.Background())
			test.terminate(cancel)
			err := waitForSupervisorStop(ctx)
			if !errors.Is(err, test.wantErr) || (test.wantErr == nil && err != nil) {
				t.Fatalf("waitForSupervisorStop() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestNodeSupervisorRejectsInvalidComponentGraphsAndBudgets(t *testing.T) {
	t.Parallel()
	valid := func(name string, dependencies ...string) componentSpec {
		return componentSpec{Name: name, Dependencies: dependencies,
			Readiness: func(context.Context) error { return nil },
			Run:       func(ctx context.Context) error { <-ctx.Done(); return nil },
			Shutdown:  func(context.Context) error { return nil }, Restart: componentRestartNever,
			Resources: componentResourceBudget{MaxConcurrent: 1}}
	}
	tests := []struct {
		name  string
		specs []componentSpec
	}{
		{name: "empty"},
		{name: "duplicate", specs: []componentSpec{valid("a"), valid("a")}},
		{name: "unknown dependency", specs: []componentSpec{valid("a", "missing")}},
		{name: "self dependency", specs: []componentSpec{valid("a", "a")}},
		{name: "repeated dependency", specs: []componentSpec{valid("a"), valid("b", "a", "a")}},
		{name: "cycle", specs: []componentSpec{valid("a", "b"), valid("b", "a")}},
		{name: "unbounded", specs: []componentSpec{func() componentSpec {
			spec := valid("a")
			spec.Resources.MaxConcurrent = 0
			return spec
		}()}},
		{name: "restart", specs: []componentSpec{func() componentSpec {
			spec := valid("a")
			spec.Restart = 0
			return spec
		}()}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := newNodeSupervisor(test.specs); err == nil {
				t.Fatal("newNodeSupervisor() accepted an invalid contract")
			}
		})
	}
}

func channelReadiness(started <-chan struct{}) func(context.Context) error {
	return func(ctx context.Context) error {
		select {
		case <-started:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func waitSupervisorLog(t *testing.T, mu *sync.Mutex, log *[]string, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		current := len(*log)
		mu.Unlock()
		if current >= count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("Supervisor log did not reach %d entries", count)
}

func awaitSupervisorSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("Supervisor did not reach %s", name)
	}
}
