package node

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDaemonLifecycleQuiesceRejectsOfflineGenerationDrift(t *testing.T) {
	t.Parallel()
	fixture := newDaemonFixture(t, true)
	current := daemonFixtureAuthorityResponse(t, fixture)
	drifted := current
	generation, err := time.Parse(time.RFC3339Nano, current.UpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	drifted.UpdatedAt = generation.Add(time.Nanosecond).UTC().Format(time.RFC3339Nano)
	withoutGeneration := drifted
	withoutGeneration.UpdatedAt = current.UpdatedAt
	if withoutGeneration != current || drifted.UpdatedAt == current.UpdatedAt {
		t.Fatal("generation-drift fixture changed non-generation authority")
	}
	lease := acquireTestDaemonLifecycle(t, fixture)
	defer lease.Close()
	var shutdowns atomic.Int32
	client := lifecycleClientStub{shutdown: func(context.Context,
		AuthorityResponse,
	) (ShutdownResponse, *APIError) {
		shutdowns.Add(1)
		return ShutdownResponse{}, NewAPIError(
			CodeInternal, "unexpected shutdown")
	}}
	_, err = lease.Quiesce(context.Background(), client,
		daemonFixtureOfflineConfirmer(fixture), drifted)
	if !errors.Is(err, ErrDaemonLifecycle) || !errors.Is(err, ErrOfflineAuthority) ||
		shutdowns.Load() != 0 {
		t.Fatalf("generation-drift Quiesce() = %v", err)
	}
	snapshot, inspectErr := InspectAuthority(context.Background(), fixture.workspace)
	observed, responseErr := NewAuthorityResponse(snapshot)
	if inspectErr != nil || responseErr != nil || observed != current {
		t.Fatalf("generation drift changed durable authority = (%#v, %v, %v)",
			observed, inspectErr, responseErr)
	}
}

func TestDaemonLifecycleQuiesceRejectsUnsafeOfflineSocket(t *testing.T) {
	t.Parallel()
	fixture := newDaemonFixture(t, true)
	path := filepath.Join(fixture.nodeState, controlSocketName)
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	lease := acquireTestDaemonLifecycle(t, fixture)
	defer lease.Close()
	_, err := lease.Quiesce(context.Background(), daemonFixtureLifecycleClient(t, fixture),
		daemonFixtureOfflineConfirmer(fixture), daemonFixtureAuthorityResponse(t, fixture))
	if !errors.Is(err, ErrDaemonLifecycle) {
		t.Fatalf("unsafe-socket Quiesce() = %v", err)
	}
	if info, statErr := os.Lstat(path); statErr != nil || !info.Mode().IsRegular() {
		t.Fatalf("unsafe socket path was changed: (%v, %v)", info, statErr)
	}
}

func TestDaemonLifecycleQuiesceRejectsOnlineSocketReplacement(t *testing.T) {
	t.Parallel()
	fixture := newDaemonFixture(t, true)
	expected := daemonFixtureAuthorityResponse(t, fixture)
	path := filepath.Join(fixture.nodeState, controlSocketName)
	original, err := ListenOwnerUnix(path)
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	var replacement net.Listener
	var confirmations atomic.Int32
	lease := acquireTestDaemonLifecycle(t, fixture)
	defer lease.Close()
	client := lifecycleClientStub{
		shutdown: func(_ context.Context,
			observed AuthorityResponse,
		) (ShutdownResponse, *APIError) {
			if observed != expected {
				t.Errorf("shutdown expected authority = %#v", observed)
			}
			if err := original.Close(); err != nil {
				t.Errorf("close original socket: %v", err)
			}
			replacement, err = ListenOwnerUnix(path)
			if err != nil {
				t.Errorf("create replacement socket: %v", err)
			}
			return daemonFixtureShutdownResponse(t, expected), nil
		},
	}
	confirmer := DaemonOfflineConfirmerFunc(func(context.Context,
		AuthorityResponse,
	) (AuthorityResponse, error) {
		confirmations.Add(1)
		return expected, nil
	})
	_, err = lease.Quiesce(context.Background(), client, confirmer, expected)
	if replacement != nil {
		_ = replacement.Close()
	}
	if !errors.Is(err, ErrDaemonLifecycle) || errors.Is(err, context.DeadlineExceeded) ||
		confirmations.Load() != 0 {
		t.Fatalf("replacement-socket Quiesce() = %v, confirmations=%d",
			err, confirmations.Load())
	}
}

func TestDaemonLifecycleQuiesceRecoversStaleOwnerSocket(t *testing.T) {
	t.Parallel()
	fixture := newDaemonFixture(t, true)
	path := filepath.Join(fixture.nodeState, controlSocketName)
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	lease := acquireTestDaemonLifecycle(t, fixture)
	defer lease.Close()
	expected := daemonFixtureAuthorityResponse(t, fixture)
	authority, err := lease.Quiesce(context.Background(), daemonFixtureLifecycleClient(t, fixture),
		daemonFixtureOfflineConfirmer(fixture), expected)
	if err != nil || authority != expected {
		t.Fatalf("stale-socket Quiesce() = (%#v, %v)", authority, err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale socket remains: %v", err)
	}
}

func TestDaemonLifecycleQuiesceBoundsWriterTimeout(t *testing.T) {
	t.Parallel()
	fixture := newDaemonFixture(t, true)
	expected := daemonFixtureAuthorityResponse(t, fixture)
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Install: fixture.install})
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	lease := acquireTestDaemonLifecycle(t, fixture)
	defer lease.Close()
	_, err = lease.quiesce(context.Background(), daemonFixtureLifecycleClient(t, fixture),
		daemonFixtureOfflineConfirmer(fixture), expected,
		daemonLifecycleTiming{deadline: 60 * time.Millisecond, poll: 5 * time.Millisecond})
	if !errors.Is(err, ErrDaemonLifecycle) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("writer-timeout Quiesce() = %v", err)
	}
}

func TestDaemonLifecycleQuiesceCancelledDuringWriterWait(t *testing.T) {
	t.Parallel()
	fixture := newDaemonFixture(t, true)
	expected := daemonFixtureAuthorityResponse(t, fixture)
	daemon, err := OpenDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Install: fixture.install})
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	lease := acquireTestDaemonLifecycle(t, fixture)
	defer lease.Close()
	writerWaitStarted := make(chan struct{})
	var writerWaitOnce sync.Once
	confirmer := DaemonOfflineConfirmerFunc(func(ctx context.Context,
		expected AuthorityResponse,
	) (AuthorityResponse, error) {
		response, err := daemonFixtureOfflineConfirmer(fixture).ConfirmOffline(ctx, expected)
		if errors.Is(err, ErrOfflineAuthorityActive) {
			writerWaitOnce.Do(func() { close(writerWaitStarted) })
		}
		return response, err
	})
	client := daemonFixtureLifecycleClient(t, fixture)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := lease.quiesce(ctx, client, confirmer, expected,
			daemonLifecycleTiming{deadline: time.Second, poll: 5 * time.Millisecond})
		result <- err
	}()
	select {
	case <-writerWaitStarted:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("Quiesce never entered its writer-active wait")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, ErrDaemonLifecycle) || !errors.Is(err, context.Canceled) {
			t.Fatalf("mid-wait cancelled Quiesce() = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("mid-wait cancellation did not stop Quiesce")
	}
}

func TestDaemonLifecycleQuiesceRejectsCancelledContext(t *testing.T) {
	t.Parallel()
	fixture := newDaemonFixture(t, true)
	lease := acquireTestDaemonLifecycle(t, fixture)
	defer lease.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := lease.Quiesce(ctx, daemonFixtureLifecycleClient(t, fixture),
		daemonFixtureOfflineConfirmer(fixture), daemonFixtureAuthorityResponse(t, fixture))
	if !errors.Is(err, ErrDaemonLifecycle) || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Quiesce() = %v", err)
	}
}
