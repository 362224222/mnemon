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
			Workspace: fixture.workspace, Install: install,
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
		Clock: controllerTestClock{fixture.profile.UpdatedAt()}, Install: fixture.install,
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
	client, err := NewClient(fixture.nodeState)
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

func TestControllerRetainsLaunchPermitUntilNetworkReadiness(t *testing.T) {
	fixture := newDaemonFixture(t, true)
	controller, network, serveDone, released, closeStore := startNetworkReadinessController(t,
		fixture)
	defer closeStore()
	waitControllerNetworkStarted(t, network, serveDone)
	assertLaunchPermitWaitsForNetworkReadiness(t, network, serveDone, released)
	client, err := NewClient(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	waitControllerHealth(t, client, "ready")
	controller.requestShutdown()
	assertControllerStoppedNetwork(t, network, serveDone)
}

func startNetworkReadinessController(t *testing.T, fixture daemonFixture) (
	*Controller, *controllerTestNetworkRuntime, chan error, chan struct{}, func(),
) {
	t.Helper()
	authority, err := openExistingDaemonAuthority(context.Background(), fixture.workspace,
		fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	network := newControllerTestNetworkRuntime()
	released := make(chan struct{})
	controller, err := NewController(context.Background(), ControllerOptions{
		NodeState: fixture.nodeState, Workspace: fixture.workspace, Store: authority.store,
		Profile: authority.authority.Profile, Signer: authority.identity.PublicationSigner(),
		Clock: controllerTestClock{fixture.profile.UpdatedAt()}, Install: fixture.install,
		networkRuntime: network, BeforeAccept: func() error {
			close(released)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- controller.Serve(context.Background()) }()
	return controller, network, serveDone, released, func() { _ = authority.store.Close() }
}

func waitControllerNetworkStarted(t *testing.T, network *controllerTestNetworkRuntime,
	serveDone <-chan error,
) {
	t.Helper()
	select {
	case <-network.started:
	case err := <-serveDone:
		t.Fatalf("Controller exited before network startup: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Controller did not start its network runtime")
	}
}

func assertLaunchPermitWaitsForNetworkReadiness(t *testing.T,
	network *controllerTestNetworkRuntime, serveDone <-chan error, released <-chan struct{},
) {
	t.Helper()
	select {
	case <-released:
		t.Fatal("launch permit released before network readiness")
	default:
	}
	close(network.allowReady)
	select {
	case <-released:
	case err := <-serveDone:
		t.Fatalf("Controller exited before consuming launch permit: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Controller did not consume launch permit after network readiness")
	}
}

func assertControllerStoppedNetwork(t *testing.T, network *controllerTestNetworkRuntime,
	serveDone <-chan error,
) {
	t.Helper()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Controller did not stop its network runtime")
	}
	select {
	case <-network.stopped:
	default:
		t.Fatal("Controller returned before network runtime stopped")
	}
}
