package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type fakeDaemon struct {
	serveErr     error
	closeErr     error
	served       bool
	closed       bool
	closeBounded bool
}

func (runtime *fakeDaemon) Serve(ctx context.Context) error {
	runtime.served = ctx != nil
	return runtime.serveErr
}

func (runtime *fakeDaemon) Close(ctx context.Context) error {
	runtime.closed = true
	deadline, ok := ctx.Deadline()
	runtime.closeBounded = ok && time.Until(deadline) > 0 && time.Until(deadline) <= gracefulShutdownBudget
	return runtime.closeErr
}

func TestServePassesCanonicalStateAndJoinsBoundedClose(t *testing.T) {
	state := canonicalDirectory(t)
	link := filepath.Join(canonicalDirectory(t), "state-link")
	if err := os.Symlink(state, link); err != nil {
		t.Fatal(err)
	}
	serveFailure := errors.New("serve failure")
	closeFailure := errors.New("close failure")
	runtime := &fakeDaemon{serveErr: serveFailure, closeErr: closeFailure}
	var gotState string
	open := func(ctx context.Context, stateDirectory string) (daemonRuntime, error) {
		if ctx == nil {
			t.Fatal("open received nil context")
		}
		gotState = stateDirectory
		return runtime, nil
	}
	err := runServe(context.Background(), []string{"--state-dir", link}, open)
	if !errors.Is(err, serveFailure) || !errors.Is(err, closeFailure) ||
		gotState != state || !runtime.served || !runtime.closed || !runtime.closeBounded {
		t.Fatalf("serve = state %q runtime %#v err %v", gotState, runtime, err)
	}
}

func TestServeRejectsMalformedOrCancelledInputBeforeOpen(t *testing.T) {
	state := canonicalDirectory(t)
	opened := 0
	open := func(context.Context, string) (daemonRuntime, error) {
		opened++
		return &fakeDaemon{}, nil
	}
	invalid := [][]string{
		{},
		{"--state-dir", state, "--state-dir", state},
		{"--principal", "principal:test"},
		{"--project-root", state},
	}
	for _, args := range invalid {
		if err := runServe(context.Background(), args, open); err == nil {
			t.Fatalf("%v succeeded", args)
		}
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runServe(cancelled, []string{"--state-dir", state}, open); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled serve = %v", err)
	}
	if opened != 0 {
		t.Fatalf("rejected input opened %d daemons", opened)
	}
}

func canonicalDirectory(t *testing.T) string {
	t.Helper()
	temporary, err := os.MkdirTemp("/tmp", "mnemond-command-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(temporary) })
	directory, err := filepath.EvalSymlinks(temporary)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(directory)
}
