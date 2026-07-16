package node

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"golang.org/x/sys/unix"
)

func TestOpenManagedDaemonRequiresExactInheritedEnsureLockBeforeStoreOpen(t *testing.T) {
	t.Run("missing permit", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		writer, err := store.OpenExisting(context.Background(),
			filepath.Join(fixture.nodeState, "node.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Close()
		t.Setenv(daemonLaunchPermitEnvironment, "")
		daemon, err := OpenManagedDaemon(context.Background(), DaemonOptions{
			Workspace: fixture.workspace, Install: fixture.install,
		})
		if daemon != nil || !errors.Is(err, ErrDaemonLaunchPermit) ||
			errors.Is(err, store.ErrWriterActive) {
			t.Fatalf("OpenManagedDaemon() = (%v, %v)", daemon, err)
		}
	})

	t.Run("wrong inode", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		writer, err := store.OpenExisting(context.Background(),
			filepath.Join(fixture.nodeState, "node.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Close()
		wrong, err := os.OpenFile(filepath.Join(fixture.nodeState, "not-ensure.lock"),
			os.O_CREATE|os.O_RDWR|os.O_EXCL, ensureLockMode)
		if err != nil {
			t.Fatal(err)
		}
		fd, err := unix.Dup(int(wrong.Fd()))
		if err != nil {
			t.Fatal(err)
		}
		if err := wrong.Close(); err != nil {
			t.Fatal(err)
		}
		t.Setenv(daemonLaunchPermitEnvironment, strconv.Itoa(fd))
		daemon, err := OpenManagedDaemon(context.Background(), DaemonOptions{
			Workspace: fixture.workspace, Install: fixture.install,
		})
		if daemon != nil || !errors.Is(err, ErrDaemonLaunchPermit) ||
			errors.Is(err, store.ErrWriterActive) {
			t.Fatalf("OpenManagedDaemon() = (%v, %v)", daemon, err)
		}
		assertClosedDescriptor(t, fd)
	})

	t.Run("unlocked descriptor cannot bypass holder", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		held := acquirePermitTestEnsureLock(t, fixture.nodeState)
		defer held.close()
		fd, err := unix.Open(filepath.Join(fixture.nodeState, ensureLockName),
			unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if err != nil {
			t.Fatal(err)
		}
		t.Setenv(daemonLaunchPermitEnvironment, strconv.Itoa(fd))
		daemon, err := OpenManagedDaemon(context.Background(), DaemonOptions{
			Workspace: fixture.workspace, Install: fixture.install,
		})
		if daemon != nil || !errors.Is(err, ErrDaemonLaunchPermit) {
			t.Fatalf("OpenManagedDaemon() = (%v, %v)", daemon, err)
		}
		assertClosedDescriptor(t, fd)
		if err := validateHeldEnsureLock(held, fixture.nodeState); err != nil {
			t.Fatalf("failed opener disturbed lifecycle holder: %v", err)
		}
	})
}

func TestOpenManagedDaemonRetainsInheritedPermitUntilSocketReadyWithoutSelfDeadlock(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	parent := acquirePermitTestEnsureLock(t, fixture.nodeState)
	defer parent.close()
	childFD, err := unix.Dup(int(parent.file.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(daemonLaunchPermitEnvironment, strconv.Itoa(childFD))
	daemon, err := OpenManagedDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Clock: controllerTestClock{fixture.profile.UpdatedAt()}, Install: fixture.install})
	if err != nil {
		t.Fatal(err)
	}
	defer daemon.Close()
	if _, inherited := os.LookupEnv(daemonLaunchPermitEnvironment); inherited {
		t.Fatal("managed daemon retained its consumed launch-permit environment")
	}
	assertOpenDescriptor(t, childFD)
	assertEnsureLockContended(t, fixture.nodeState)

	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- daemon.Serve(serveCtx) }()
	waitControllerSocket(t, filepath.Join(fixture.nodeState, controlSocketName), served)
	client, err := localapi.NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	health, apiErr := client.ProbeHealth(context.Background())
	if apiErr != nil || health.Status != "ready" {
		t.Fatalf("ProbeHealth() = (%#v, %v)", health, apiErr)
	}
	assertClosedDescriptor(t, childFD)
	if err := validateHeldEnsureLock(parent, fixture.nodeState); err != nil {
		t.Fatalf("child release dropped parent launch fence: %v", err)
	}
	assertEnsureLockContended(t, fixture.nodeState)

	if err := parent.close(); err != nil {
		t.Fatal(err)
	}
	reacquired := acquirePermitTestEnsureLock(t, fixture.nodeState)
	if err := reacquired.close(); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("managed daemon did not stop")
	}
}

func acquirePermitTestEnsureLock(t *testing.T, nodeState string) *ensureLock {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	lock, err := acquireEnsureLock(ctx, nodeState, daemonEnsurePoll)
	if err != nil {
		t.Fatal(err)
	}
	return lock
}

func assertEnsureLockContended(t *testing.T, nodeState string) {
	t.Helper()
	fd, err := unix.Open(filepath.Join(nodeState, ensureLockName),
		unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
	if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
		if err == nil {
			_ = unix.Flock(fd, unix.LOCK_UN)
			t.Fatal("independent descriptor acquired held ensure lock")
		}
		t.Fatalf("independent ensure lock probe = %v", err)
	}
}

func assertOpenDescriptor(t *testing.T, fd int) {
	t.Helper()
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil {
		t.Fatalf("descriptor %d is not open: %v", fd, err)
	}
}

func assertClosedDescriptor(t *testing.T, fd int) {
	t.Helper()
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
		t.Fatalf("descriptor %d remained open: %v", fd, err)
	}
}
