package integration

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

func TestHostActivationWatcherStopAndCancellationAreJoined(t *testing.T) {
	t.Run("explicit stop", func(t *testing.T) {
		watcher := startHostActivationWatcher(context.Background(), 0)
		watcher.stopAndWait()
		assertHostActivationWatcherDone(t, watcher)
		watcher.stopAndWait()
	})

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		watcher := startHostActivationWatcher(ctx, 0)
		cancel()
		select {
		case <-watcher.done:
		case <-time.After(time.Second):
			t.Fatal("Host activation watcher did not finish after cancellation")
		}
		watcher.stopAndWait()
	})
}

func assertHostActivationWatcherDone(t *testing.T, watcher *hostActivationWatcher) {
	t.Helper()
	select {
	case <-watcher.done:
	default:
		t.Fatal("Host activation watcher stop returned before its goroutine finished")
	}
}

func TestVerifyHostActivationCancellationKillsTheWholeProcessGroup(t *testing.T) {
	workspace, nodeState, bundle := newProjectionWorkspace(t)
	if _, err := InstallHostProjection(workspace, nodeState, assets.HostCodex, bundle); err != nil {
		t.Fatal(err)
	}
	executable, capture := writeActivationHost(t, workspace,
		validCodexHooksResponse(t, workspace, bundle), "grandchild-timeout")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- VerifyHostActivation(ctx, workspace, nodeState,
			HostObservation{Host: assets.HostCodex, Executable: executable,
				Version: "codex activation-test"}, bundle)
	}()
	raw := waitForActivationChild(t, capture+".child", result)
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, ErrHostUnavailable) {
			t.Fatalf("VerifyHostActivation() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Host activation did not join its cancellation watcher")
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 0 {
		t.Fatalf("grandchild PID = %q, %v", raw, err)
	}
	assertActivationProcessExited(t, pid)
}

func waitForActivationChild(t *testing.T, path string, result <-chan error) []byte {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		raw, err := os.ReadFile(path)
		if err == nil && len(raw) > 0 && raw[len(raw)-1] == '\n' {
			return raw
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		select {
		case err := <-result:
			t.Fatalf("Host activation returned before starting its grandchild: %v", err)
		case <-deadline.C:
			t.Fatal("Host activation did not start its grandchild")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func assertActivationProcessExited(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		if err != nil {
			t.Fatalf("inspect grandchild PID %d: %v", pid, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("Host activation grandchild %d survived cancellation", pid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
