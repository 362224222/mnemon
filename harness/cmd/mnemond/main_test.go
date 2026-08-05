package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/daemon"
)

func TestRunHasOnlyTheR7DaemonSurface(t *testing.T) {
	t.Parallel()
	command := func(args ...string) (string, string, error) {
		var stdout, stderr bytes.Buffer
		err := runWithDaemon(context.Background(), args, &stdout, &stderr,
			func(context.Context, string) (daemonRuntime, error) {
				t.Fatal("non-serve command opened daemon")
				return nil, nil
			})
		return stdout.String(), stderr.String(), err
	}
	help, stderr, err := command()
	if err != nil || stderr != "" || help != helpText {
		t.Fatalf("empty invocation = (%q, %q, %v)", help, stderr, err)
	}
	for _, argument := range []string{"-h", "--help", "help"} {
		stdout, stderr, err := command(argument)
		if err != nil || stdout != help || stderr != "" {
			t.Fatalf("help %q = (%q, %q, %v)", argument, stdout, stderr, err)
		}
	}
	for _, argument := range []string{"--version", "version"} {
		stdout, stderr, err := command(argument)
		if err != nil || stdout != "mnemond version dev\n" || stderr != "" {
			t.Fatalf("version %q = (%q, %q, %v)", argument, stdout, stderr, err)
		}
	}
	for _, retired := range []string{"initialize", "activate", "deactivate", "inspect", "confirm-offline"} {
		stdout, stderr, err := command(retired)
		if err == nil || stdout != "" || stderr != "" || !strings.Contains(err.Error(), retired) {
			t.Fatalf("retired %q = (%q, %q, %v)", retired, stdout, stderr, err)
		}
	}
	lower := strings.ToLower(help)
	for _, forbidden := range []string{"r5", "work", "channel", "teamwork", "activate", "managed"} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("help contains retired or case-specific vocabulary %q", forbidden)
		}
	}
}

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
	err := runWithDaemon(context.Background(), []string{"serve", "--state-dir", link},
		io.Discard, io.Discard, open)
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
		{"serve"},
		{"serve", "--state-dir", state, "--state-dir", state},
		{"serve", "--principal", "principal:test"},
		{"serve", "--project-root", state},
	}
	for _, args := range invalid {
		if err := runWithDaemon(context.Background(), args, io.Discard, io.Discard, open); err == nil {
			t.Fatalf("%v succeeded", args)
		}
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runWithDaemon(cancelled, []string{"serve", "--state-dir", state},
		io.Discard, io.Discard, open); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled serve = %v", err)
	}
	if opened != 0 {
		t.Fatalf("rejected input opened %d daemons", opened)
	}
}

func TestProductionServeReachesLocalReadinessAndClosesOnCancellation(t *testing.T) {
	state := provisionR7State(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"serve", "--state-dir", state}, io.Discard, io.Discard)
	}()
	waitForReady(t, state, done)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled production serve = %v", err)
		}
	case <-time.After(2 * gracefulShutdownBudget):
		t.Fatal("production serve did not close within its shutdown budget")
	}
	if _, err := os.Lstat(filepath.Join(state, "control.sock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("control socket survived shutdown: %v", err)
	}
	reopened, err := daemon.OpenProvisioned(context.Background(), state)
	if err != nil {
		t.Fatalf("writer was not released: %v", err)
	}
	closeContext, closeCancel := context.WithTimeout(context.Background(), gracefulShutdownBudget)
	defer closeCancel()
	if err := reopened.Close(closeContext); err != nil {
		t.Fatalf("close reopened daemon: %v", err)
	}
}

func provisionR7State(t *testing.T) string {
	t.Helper()
	root := canonicalDirectory(t)
	result, err := daemon.Provision(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	return result.StateDirectory()
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

func waitForReady(t *testing.T, state string, serveDone <-chan error) {
	t.Helper()
	socket := filepath.Join(state, "control.sock")
	client := &http.Client{Timeout: time.Second, Transport: &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	}}
	defer client.CloseIdleConnections()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-serveDone:
			t.Fatalf("mnemond stopped before readiness: %v", err)
		default:
		}
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
			"http://mnemond/v1/agency/status", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("mnemond did not reach local readiness")
}
