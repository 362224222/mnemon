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
	go func() { done <- supervisor.Run(context.Background(), stop) }()
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
	go func() { done <- supervisor.Run(context.Background(), nil) }()
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
