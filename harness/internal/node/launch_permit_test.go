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

	"github.com/mnemon-dev/mnemon/harness/internal/agent"
	"github.com/mnemon-dev/mnemon/harness/internal/localapi"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
	"github.com/mnemon-dev/mnemon/harness/internal/store"
	"golang.org/x/sys/unix"
)

func TestOpenManagedDaemonRequiresExactInheritedEnsureLockBeforeStoreOpen(t *testing.T) {
	t.Run("action authority before permit and Store", func(t *testing.T) {
		fixture := newDaemonFixture(t, true)
		writer, err := store.OpenExisting(context.Background(),
			filepath.Join(fixture.nodeState, "node.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Close()
		called := false
		install := InstallationVerifierFunc(func(context.Context, model.Profile) error {
			called = true
			return nil
		})
		t.Setenv(daemonLaunchPermitEnvironment, "")
		daemon, err := OpenManagedDaemon(context.Background(), DaemonOptions{
			Workspace: fixture.workspace, Install: install, Credentials: testProfileCredentials{},
		})
		if daemon != nil || !errors.Is(err, ErrDaemonAuthority) ||
			errors.Is(err, ErrDaemonLaunchPermit) || errors.Is(err, store.ErrWriterActive) {
			t.Fatalf("OpenManagedDaemon() = (%v, %v)", daemon, err)
		}
		if called {
			t.Fatal("action authority failure reached installation verification")
		}
	})

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
			Workspace: fixture.workspace, Install: fixture.install, Credentials: testProfileCredentials{},
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
			Workspace: fixture.workspace, Install: fixture.install, Credentials: testProfileCredentials{},
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
			Workspace: fixture.workspace, Install: fixture.install, Credentials: testProfileCredentials{},
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
	publishDaemonTestMeshPending(t, fixture)
	parent := acquirePermitTestEnsureLock(t, fixture.nodeState)
	defer parent.close()
	childFD, err := unix.Dup(int(parent.file.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(daemonLaunchPermitEnvironment, strconv.Itoa(childFD))
	daemon, err := OpenManagedDaemon(context.Background(), DaemonOptions{Workspace: fixture.workspace,
		Clock: controllerTestClock{fixture.profile.UpdatedAt()}, Install: fixture.install,
		Credentials: testProfileCredentials{}, Control: newTestControlTransportFactory(),
		Attachments:        &testWakeAttachmentFilesystem{},
		WakeAdapterFactory: permitTestWakeFactory()})
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
	waitControllerHealth(t, client, "ready")
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

func TestManagedDaemonControlPrepareFailureReleasesPermitBeforeReturn(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	publishDaemonTestMeshPending(t, fixture)
	parent := acquirePermitTestEnsureLock(t, fixture.nodeState)
	defer parent.close()
	childFD, err := unix.Dup(int(parent.file.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(daemonLaunchPermitEnvironment, strconv.Itoa(childFD))
	errPrepare := errors.New("test managed control prepare failure")
	prepares := 0
	daemon, err := OpenManagedDaemon(context.Background(), DaemonOptions{
		Workspace: fixture.workspace, Install: fixture.install,
		Credentials: testProfileCredentials{}, WakeAdapterFactory: permitTestWakeFactory(),
		Attachments: &testWakeAttachmentFilesystem{},
		Control: ControlTransportFactoryFunc(func(context.Context, ControlTransportOptions,
			ControlBindings,
		) (PreparedControlTransport, error) {
			prepares++
			return nil, errPrepare
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertOpenDescriptor(t, childFD)
	if err := daemon.Serve(context.Background()); !errors.Is(err, errPrepare) || prepares != 1 {
		t.Fatalf("Serve() = %v, prepares=%d", err, prepares)
	}
	assertClosedDescriptor(t, childFD)
	if err := validateHeldEnsureLock(parent, fixture.nodeState); err != nil {
		t.Fatalf("control preparation failure disturbed parent permit: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(fixture.nodeState, controlSocketName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("control preparation failure retained socket: %v", err)
	}
	if err := daemon.Close(); err != nil {
		t.Fatal(err)
	}
	assertDaemonStoreReopenable(t, fixture.nodeState)
}

type permitTestWakeAdapter struct{}

func (permitTestWakeAdapter) Run(context.Context,
	agent.CodexWakeRequest,
) (agent.CodexWakeResult, error) {
	return agent.CodexWakeResult{}, errors.New("permit test wake adapter was unexpectedly invoked")
}

func permitTestWakeFactory() WakeAdapterFactory {
	return WakeAdapterFactoryFunc(func(context.Context,
		WakeAdapterFactoryOptions,
	) (agent.WakeWorkerAdapter, error) {
		return permitTestWakeAdapter{}, nil
	})
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
