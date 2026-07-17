package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const (
	daemonProcessHelperEnvironment = "MNEMON_PROCESS_HELPER"
	daemonProcessHelperMarker      = "MNEMON_PROCESS_HELPER_MARKER"
)

func TestMain(main *testing.M) {
	if os.Getenv(daemonProcessHelperEnvironment) == "1" {
		os.Exit(runDaemonProcessHelper(os.Args[1:]))
	}
	os.Exit(main.Run())
}

func TestDaemonProcessLauncherStartsExactDetachedChildAndTerminatesIt(t *testing.T) {
	fixture := newDaemonProcessFixture(t)
	launcher := fixture.launcher(t)
	handle, err := fixture.launch(t, launcher, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	marker := waitDaemonProcessMarker(t, fixture.marker)
	if marker != strings.Join([]string{"serve", "--project-root", fixture.workspace}, "\n")+"\n" {
		t.Fatalf("child arguments = %q", marker)
	}

	record, pidInfo, encoded := readDaemonProcessTestPID(t, fixture.nodeState)
	if record.PID <= 0 || record.Executable != fixture.executable ||
		record.Workspace != fixture.workspace || record.NodeState != fixture.nodeState {
		t.Fatalf("PID authority = %#v", record)
	}
	if !daemonProcessTestAlive(record.PID) {
		t.Fatal("launched child is not alive")
	}
	if session, err := unix.Getsid(record.PID); err != nil || session != record.PID {
		t.Fatalf("child session = %d, %v; want %d", session, err, record.PID)
	}
	assertDaemonProcessFile(t, filepath.Join(fixture.nodeState, daemonPIDName))
	assertDaemonProcessFile(t, filepath.Join(fixture.nodeState, daemonLogName))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := handle.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	if daemonProcessTestAlive(record.PID) {
		t.Fatal("Terminate left the child alive")
	}
	if _, err := os.Lstat(filepath.Join(fixture.nodeState, daemonPIDName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Terminate left PID state: %v", err)
	}
	if err := handle.Terminate(ctx); err != nil {
		t.Fatalf("repeated Terminate() = %v", err)
	}
	logBytes, err := os.ReadFile(filepath.Join(fixture.nodeState, daemonLogName))
	if err != nil || !bytes.Contains(logBytes, []byte("mnemond process helper ready\n")) {
		t.Fatalf("daemon log = %q, %v", logBytes, err)
	}
	if pidInfo == nil || len(encoded) == 0 {
		t.Fatal("PID publication did not expose a stable inode and canonical bytes")
	}
}

func TestDaemonProcessEnvironmentIsClosedForManagedRuntimeBootstrap(t *testing.T) {
	input := []string{
		"PATH=/managed/bin", "HOME=/home/agent", "CODEX_HOME=/home/agent/.codex",
		"XDG_CACHE_HOME=/home/agent/.cache", "LC_ALL=C.UTF-8", "LANG=en_US.UTF-8",
		"OPENAI_API_KEY=secret", "MNEMON_HARNESS_RUN_ATTACHMENT=/stale.attach",
		"MNEMON_EVENT_BODY=private", daemonLaunchPermitEnvironment + "=99", "MALFORMED",
	}
	want := []string{
		"PATH=/managed/bin", "HOME=/home/agent", "CODEX_HOME=/home/agent/.codex",
		"XDG_CACHE_HOME=/home/agent/.cache", "LC_ALL=C.UTF-8", "LANG=en_US.UTF-8",
	}
	if got := daemonProcessEnvironment(input); !slices.Equal(got, want) {
		t.Fatalf("daemonProcessEnvironment() = %q, want %q", got, want)
	}
}

func TestDaemonProcessLauncherRejectsMissingOrWrongLaunchPermitBeforeStarting(t *testing.T) {
	fixture := newDaemonProcessFixture(t)
	launcher := fixture.launcher(t)
	if handle, err := launcher.Launch(context.Background(), DaemonLaunchPermit{}); handle != nil ||
		!errors.Is(err, ErrDaemonProcess) {
		t.Fatalf("missing permit Launch() = (%#v, %v)", handle, err)
	}
	other := newDaemonProcessFixture(t)
	otherLock := acquirePermitTestEnsureLock(t, other.nodeState)
	defer otherLock.close()
	if handle, err := launcher.Launch(context.Background(),
		DaemonLaunchPermit{lock: otherLock}); handle != nil || !errors.Is(err, ErrDaemonProcess) {
		t.Fatalf("wrong Node permit Launch() = (%#v, %v)", handle, err)
	}
	for _, path := range []string{fixture.marker,
		filepath.Join(fixture.nodeState, daemonPIDName),
		filepath.Join(fixture.nodeState, daemonLogName)} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("invalid permit created %s: %v", path, err)
		}
	}
}

func TestDaemonProcessLauncherReleaseDetachesAndPreservesDiagnostics(t *testing.T) {
	fixture := newDaemonProcessFixture(t)
	launcher := fixture.launcher(t)
	launchContext, cancelLaunch := context.WithCancel(context.Background())
	handle, err := fixture.launch(t, launcher, launchContext)
	if err != nil {
		t.Fatal(err)
	}
	_ = waitDaemonProcessMarker(t, fixture.marker)
	record, before, encoded := readDaemonProcessTestPID(t, fixture.nodeState)
	if err := handle.Release(); err != nil {
		t.Fatal(err)
	}
	cancelLaunch()
	time.Sleep(50 * time.Millisecond)
	if !daemonProcessTestAlive(record.PID) {
		t.Fatal("launch context cancellation killed a released child")
	}
	afterRecord, after, afterBytes := readDaemonProcessTestPID(t, fixture.nodeState)
	if afterRecord != record || !os.SameFile(before, after) || !bytes.Equal(encoded, afterBytes) {
		t.Fatal("Release changed PID diagnostics")
	}
	if err := handle.Release(); err != nil {
		t.Fatalf("repeated Release() = %v", err)
	}
	if err := handle.Terminate(context.Background()); !errors.Is(err, ErrDaemonProcess) {
		t.Fatalf("Terminate after Release error = %v", err)
	}
	reapReleasedDaemonProcess(t, record.PID)
}

func TestDaemonProcessLauncherReplacesOnlyItsCanonicalStalePID(t *testing.T) {
	fixture := newDaemonProcessFixture(t)
	first := fixture.launcher(t)
	firstHandle, err := fixture.launch(t, first, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = waitDaemonProcessMarker(t, fixture.marker)
	firstRecord, firstInfo, firstBytes := readDaemonProcessTestPID(t, fixture.nodeState)
	if err := firstHandle.Release(); err != nil {
		t.Fatal(err)
	}
	reapReleasedDaemonProcess(t, firstRecord.PID)
	if daemonProcessTestAlive(firstRecord.PID) {
		t.Fatal("released helper did not stop")
	}

	fixture.marker = filepath.Join(fixture.workspace, "second.marker")
	t.Setenv(daemonProcessHelperMarker, fixture.marker)
	second := fixture.launcher(t)
	secondHandle, err := fixture.launch(t, second, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = waitDaemonProcessMarker(t, fixture.marker)
	secondRecord, secondInfo, secondBytes := readDaemonProcessTestPID(t, fixture.nodeState)
	if secondRecord.PID == firstRecord.PID || secondRecord.Instance == firstRecord.Instance ||
		os.SameFile(firstInfo, secondInfo) || bytes.Equal(firstBytes, secondBytes) {
		t.Fatalf("stale PID was not replaced: first=%#v second=%#v", firstRecord, secondRecord)
	}
	terminateDaemonProcessTestHandle(t, secondHandle)

	unknown := append([]byte(nil), firstBytes...)
	unknown = bytes.Replace(unknown, []byte(fixture.executable), []byte(filepath.Join(fixture.workspace, "other-mnemond")), 1)
	if err := os.WriteFile(filepath.Join(fixture.nodeState, daemonPIDName), unknown, daemonProcessFileMode); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.launch(t, fixture.launcher(t), context.Background()); !errors.Is(err, ErrDaemonProcess) {
		t.Fatalf("unmanaged stale PID error = %v", err)
	}
}

func TestDaemonProcessTerminatePreservesPIDReplacementEvenWithIdenticalBytes(t *testing.T) {
	fixture := newDaemonProcessFixture(t)
	handle, err := fixture.launch(t, fixture.launcher(t), context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = waitDaemonProcessMarker(t, fixture.marker)
	_, originalInfo, encoded := readDaemonProcessTestPID(t, fixture.nodeState)
	replacement := filepath.Join(fixture.nodeState, ".replacement.pid")
	if err := os.WriteFile(replacement, encoded, daemonProcessFileMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(replacement, daemonProcessFileMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacement, filepath.Join(fixture.nodeState, daemonPIDName)); err != nil {
		t.Fatal(err)
	}
	_, replacementInfo, replacementBytes := readDaemonProcessTestPID(t, fixture.nodeState)
	if os.SameFile(originalInfo, replacementInfo) {
		t.Fatal("test did not replace the PID inode")
	}
	terminateDaemonProcessTestHandle(t, handle)
	current, err := os.ReadFile(filepath.Join(fixture.nodeState, daemonPIDName))
	if err != nil || !bytes.Equal(current, replacementBytes) {
		t.Fatalf("Terminate changed replacement PID = %q, %v", current, err)
	}
}

func TestDaemonProcessLauncherFailsClosedForUnsafeExecutableAndProcessFiles(t *testing.T) {
	t.Run("relative executable", func(t *testing.T) {
		fixture := newDaemonProcessFixture(t)
		_, err := NewDaemonProcessLauncher(DaemonProcessOptions{
			Executable: "mnemond", Workspace: fixture.workspace, NodeState: fixture.nodeState,
		})
		if !errors.Is(err, ErrDaemonProcess) {
			t.Fatalf("NewDaemonProcessLauncher() error = %v", err)
		}
	})

	t.Run("symlink executable", func(t *testing.T) {
		fixture := newDaemonProcessFixture(t)
		linked := filepath.Join(fixture.workspace, "mnemond-link")
		if err := os.Symlink(fixture.executable, linked); err != nil {
			t.Fatal(err)
		}
		_, err := NewDaemonProcessLauncher(DaemonProcessOptions{
			Executable: linked, Workspace: fixture.workspace, NodeState: fixture.nodeState,
		})
		if !errors.Is(err, ErrDaemonProcess) {
			t.Fatalf("NewDaemonProcessLauncher() error = %v", err)
		}
	})

	for _, mode := range []os.FileMode{0o600, 0o777} {
		t.Run(fmt.Sprintf("unsafe executable mode %04o", mode), func(t *testing.T) {
			fixture := newDaemonProcessFixture(t)
			unsafeExecutable := filepath.Join(fixture.workspace, "unsafe-mnemond")
			if err := os.WriteFile(unsafeExecutable, []byte("#!/bin/sh\nexit 0\n"), mode); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(unsafeExecutable, mode); err != nil {
				t.Fatal(err)
			}
			_, err := NewDaemonProcessLauncher(DaemonProcessOptions{
				Executable: unsafeExecutable, Workspace: fixture.workspace, NodeState: fixture.nodeState,
			})
			if !errors.Is(err, ErrDaemonProcess) {
				t.Fatalf("NewDaemonProcessLauncher() error = %v", err)
			}
		})
	}

	tests := []struct {
		name    string
		prepare func(*testing.T, daemonProcessFixture)
	}{
		{name: "permissive log", prepare: func(t *testing.T, fixture daemonProcessFixture) {
			if err := os.WriteFile(filepath.Join(fixture.nodeState, daemonLogName), nil, 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(filepath.Join(fixture.nodeState, daemonLogName), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "permissive PID", prepare: func(t *testing.T, fixture daemonProcessFixture) {
			if err := os.WriteFile(filepath.Join(fixture.nodeState, daemonPIDName),
				[]byte("unsafe\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(filepath.Join(fixture.nodeState, daemonPIDName), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink log", prepare: func(t *testing.T, fixture daemonProcessFixture) {
			if err := os.Symlink(filepath.Join(fixture.workspace, "outside.log"),
				filepath.Join(fixture.nodeState, daemonLogName)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "symlink PID", prepare: func(t *testing.T, fixture daemonProcessFixture) {
			if err := os.Symlink(filepath.Join(fixture.workspace, "outside.pid"),
				filepath.Join(fixture.nodeState, daemonPIDName)); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "noncanonical PID", prepare: func(t *testing.T, fixture daemonProcessFixture) {
			if err := os.WriteFile(filepath.Join(fixture.nodeState, daemonPIDName),
				[]byte("123\n"), daemonProcessFileMode); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDaemonProcessFixture(t)
			test.prepare(t, fixture)
			if _, err := fixture.launch(t, fixture.launcher(t), context.Background()); !errors.Is(err, ErrDaemonProcess) {
				t.Fatalf("Launch() error = %v", err)
			}
			if _, err := os.Lstat(fixture.marker); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unsafe state started a child: %v", err)
			}
		})
	}
}

func TestDaemonProcessLauncherAppendsLogAndConvergesSafeStaging(t *testing.T) {
	fixture := newDaemonProcessFixture(t)
	logPath := filepath.Join(fixture.nodeState, daemonLogName)
	if err := os.WriteFile(logPath, []byte("existing diagnostics\n"), daemonProcessFileMode); err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(fixture.nodeState,
		daemonPIDStagePrefix+strings.Repeat("a", daemonPIDInstanceBytes*2)+daemonPIDStageSuffix)
	if err := os.WriteFile(stage, []byte("crashed stage"), daemonProcessFileMode); err != nil {
		t.Fatal(err)
	}
	handle, err := fixture.launch(t, fixture.launcher(t), context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = waitDaemonProcessMarker(t, fixture.marker)
	terminateDaemonProcessTestHandle(t, handle)
	if _, err := os.Lstat(stage); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staged PID was not converged: %v", err)
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil || !bytes.HasPrefix(logBytes, []byte("existing diagnostics\n")) ||
		!bytes.Contains(logBytes, []byte("mnemond process helper ready\n")) {
		t.Fatalf("append log = %q, %v", logBytes, err)
	}
}

func TestDaemonProcessLauncherKillsChildWhenPIDPublicationFailsOrContextCancels(t *testing.T) {
	t.Run("publication failure", func(t *testing.T) {
		fixture := newDaemonProcessFixture(t)
		launcher := fixture.launcher(t)
		launcher.testBeforePIDPublication = func() {
			_ = waitDaemonProcessMarker(t, fixture.marker)
			if err := os.WriteFile(filepath.Join(fixture.nodeState, daemonPIDName),
				[]byte("foreign state\n"), daemonProcessFileMode); err != nil {
				t.Fatal(err)
			}
		}
		if handle, err := fixture.launch(t, launcher, context.Background()); handle != nil ||
			!errors.Is(err, ErrDaemonProcess) {
			t.Fatalf("Launch() = (%#v, %v)", handle, err)
		}
		pid := readDaemonProcessHelperPID(t, fixture.marker)
		if daemonProcessTestAlive(pid) {
			t.Fatal("PID publication failure left its child alive")
		}
	})

	t.Run("context cancellation before publication", func(t *testing.T) {
		fixture := newDaemonProcessFixture(t)
		launcher := fixture.launcher(t)
		ctx, cancel := context.WithCancel(context.Background())
		launcher.testBeforePIDPublication = func() {
			_ = waitDaemonProcessMarker(t, fixture.marker)
			cancel()
		}
		if handle, err := fixture.launch(t, launcher, ctx); handle != nil ||
			!errors.Is(err, context.Canceled) {
			t.Fatalf("Launch() = (%#v, %v)", handle, err)
		}
		pid := readDaemonProcessHelperPID(t, fixture.marker)
		if daemonProcessTestAlive(pid) {
			t.Fatal("cancellation before PID publication left its child alive")
		}
		if _, err := os.Lstat(filepath.Join(fixture.nodeState, daemonPIDName)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("canceled launch published PID: %v", err)
		}
	})
}

func TestDaemonProcessLauncherNodeLockContentionHonorsDeadlineWithoutStartingChild(t *testing.T) {
	fixture := newDaemonProcessFixture(t)
	launcher := fixture.launcher(t)
	held, err := openIdentityNodeState(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	defer held.close()
	if err := held.lock(); err != nil {
		t.Fatal(err)
	}
	defer held.unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	handle, err := fixture.launch(t, launcher, ctx)
	if handle != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Launch() = (%#v, %v)", handle, err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("lock deadline took %s", elapsed)
	}
	for _, path := range []string{
		fixture.marker,
		filepath.Join(fixture.nodeState, daemonPIDName),
		filepath.Join(fixture.nodeState, daemonLogName),
	} {
		if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("bounded lock failure created %s: %v", path, statErr)
		}
	}
}

func TestDaemonProcessTerminateHonorsDeadlineWhilePIDCleanupLockIsHeld(t *testing.T) {
	fixture := newDaemonProcessFixture(t)
	handle, err := fixture.launch(t, fixture.launcher(t), context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_ = waitDaemonProcessMarker(t, fixture.marker)
	record, _, _ := readDaemonProcessTestPID(t, fixture.nodeState)

	held, err := openIdentityNodeState(fixture.nodeState)
	if err != nil {
		t.Fatal(err)
	}
	defer held.close()
	if err := held.lock(); err != nil {
		t.Fatal(err)
	}
	defer held.unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	err = handle.Terminate(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Terminate() error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("bounded PID cleanup took %s", elapsed)
	}
	if daemonProcessTestAlive(record.PID) {
		t.Fatal("bounded Terminate left the exact child alive")
	}
	if _, err := os.Lstat(filepath.Join(fixture.nodeState, daemonPIDName)); err != nil {
		t.Fatalf("contended cleanup did not preserve stale PID diagnostics: %v", err)
	}
}

type daemonProcessFixture struct {
	workspace  string
	nodeState  string
	executable string
	marker     string
}

func newDaemonProcessFixture(t *testing.T) daemonProcessFixture {
	t.Helper()
	workspace := t.TempDir()
	nodeState := filepath.Join(workspace, ".mnemon", "harness", "node")
	if err := os.MkdirAll(nodeState, identityDirectoryMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(nodeState, identityDirectoryMode); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(workspace, "helper.marker")
	t.Setenv(daemonProcessHelperEnvironment, "1")
	t.Setenv(daemonProcessHelperMarker, marker)
	return daemonProcessFixture{
		workspace: workspace, nodeState: nodeState, executable: executable, marker: marker,
	}
}

func (fixture daemonProcessFixture) launcher(t *testing.T) *DaemonProcessLauncher {
	t.Helper()
	launcher, err := NewDaemonProcessLauncher(DaemonProcessOptions{
		Executable: fixture.executable,
		Workspace:  fixture.workspace,
		NodeState:  fixture.nodeState,
	})
	if err != nil {
		t.Fatal(err)
	}
	launcher.testEnvironment = []string{
		daemonProcessHelperEnvironment + "=1",
		daemonProcessHelperMarker + "=" + fixture.marker,
	}
	return launcher
}

func (fixture daemonProcessFixture) launch(t *testing.T, launcher *DaemonProcessLauncher,
	ctx context.Context,
) (DaemonLaunch, error) {
	t.Helper()
	lock, err := acquireEnsureLock(ctx, fixture.nodeState, daemonEnsurePoll)
	if err != nil {
		return nil, err
	}
	handle, launchErr := launcher.Launch(ctx, DaemonLaunchPermit{lock: lock})
	closeErr := lock.close()
	return handle, errors.Join(launchErr, closeErr)
}

func runDaemonProcessHelper(args []string) int {
	if len(args) != 3 || args[0] != "serve" || args[1] != "--project-root" {
		return 69
	}
	permit, err := openInheritedDaemonLaunchPermit(
		filepath.Join(args[2], ".mnemon", "harness", "node"))
	if err != nil {
		return 69
	}
	if err := permit.close(); err != nil {
		return 69
	}
	marker := os.Getenv(daemonProcessHelperMarker)
	if marker == "" {
		return 70
	}
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, syscall.SIGTERM, os.Interrupt)
	defer signal.Stop(interrupts)
	encoded := strings.Join(args, "\n") + "\n" + fmt.Sprintf("pid=%d\n", os.Getpid())
	if err := os.WriteFile(marker, []byte(encoded), 0o600); err != nil {
		return 71
	}
	fmt.Println("mnemond process helper ready")
	<-interrupts
	return 0
}

func waitDaemonProcessMarker(t *testing.T, path string) string {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		encoded, err := os.ReadFile(path)
		if err == nil {
			lines := strings.Split(string(encoded), "\n")
			if len(lines) >= 2 && strings.HasPrefix(lines[len(lines)-2], "pid=") {
				return strings.Join(lines[:len(lines)-2], "\n") + "\n"
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for helper marker")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func readDaemonProcessHelperPID(t *testing.T, path string) int {
	t.Helper()
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(encoded)), "\n")
	var pid int
	if len(lines) == 0 {
		t.Fatalf("helper marker = %q", encoded)
	}
	if _, err := fmt.Sscanf(lines[len(lines)-1], "pid=%d", &pid); err != nil || pid <= 0 {
		t.Fatalf("helper PID marker = %q, %v", lines[len(lines)-1], err)
	}
	return pid
}

func readDaemonProcessTestPID(t *testing.T, nodeState string) (daemonPIDRecord, os.FileInfo, []byte) {
	t.Helper()
	path := filepath.Join(nodeState, daemonPIDName)
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	record, err := decodeDaemonPIDRecord(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return record, info, encoded
}

func assertDaemonProcessFile(t *testing.T, path string) {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != daemonProcessFileMode ||
		info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("process file %s mode = %v", path, info.Mode())
	}
}

func daemonProcessTestAlive(pid int) bool {
	alive, err := daemonProcessAlive(pid)
	return err == nil && alive
}

func terminateDaemonProcessTestHandle(t *testing.T, handle DaemonLaunch) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := handle.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
}

func reapReleasedDaemonProcess(t *testing.T, pid int) {
	t.Helper()
	process, err := os.FindProcess(pid)
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() {
		_, err := process.Wait()
		waited <- err
	}()
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		_ = process.Kill()
		t.Fatal("timed out reaping released helper")
	}
}
