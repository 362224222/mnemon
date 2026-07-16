package node

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	daemonLogName          = "mnemond.log"
	daemonPIDName          = "mnemond.pid"
	daemonProcessFileMode  = os.FileMode(0o600)
	daemonPIDSchemaVersion = 1
	daemonPIDMaximumBytes  = int64(4096)
	daemonPIDMaximum       = 1<<31 - 1
	daemonPIDStageAttempts = 32
	daemonPIDInstanceBytes = 16
	daemonPIDStagePrefix   = ".mnemond-pid-"
	daemonPIDStageSuffix   = ".tmp"
	daemonProcessLockPoll  = 10 * time.Millisecond
)

var ErrDaemonProcess = errors.New("launch mnemond process")

// DaemonProcessOptions binds a production launcher to one physical mnemond
// executable and one workspace-local Node. The executable is never resolved
// through PATH and the fixed Node state location prevents redirecting owner
// process files outside the managed workspace.
type DaemonProcessOptions struct {
	Executable string
	Workspace  string
	NodeState  string
}

// DaemonProcessLauncher starts the one detached mnemond child used by the
// bounded ensure path. Readiness and launch serialization remain owned by
// EnsureDaemon; this type owns only process, log and PID publication.
type DaemonProcessLauncher struct {
	executable string
	workspace  string
	nodeState  string

	// The callback is deliberately private and nil in production. Tests use it
	// to make the otherwise tiny post-Start/pre-publication failure window
	// deterministic without a package-global failpoint.
	testBeforePIDPublication func()
}

// NewDaemonProcessLauncher constructs a strict launcher. Disk identities are
// revalidated by every Launch so a safe constructor cannot be used to bless a
// later executable, workspace or Node-state replacement.
func NewDaemonProcessLauncher(options DaemonProcessOptions) (*DaemonProcessLauncher, error) {
	if options.Executable == "" || !filepath.IsAbs(options.Executable) ||
		filepath.Clean(options.Executable) != options.Executable {
		return nil, daemonProcessError("configure", errors.New("executable path must be absolute and canonical"))
	}
	if options.Workspace == "" || !filepath.IsAbs(options.Workspace) ||
		filepath.Clean(options.Workspace) != options.Workspace {
		return nil, daemonProcessError("configure", errors.New("workspace path must be absolute and canonical"))
	}
	wantedNodeState := filepath.Join(options.Workspace, ".mnemon", "harness", "node")
	if options.NodeState != wantedNodeState {
		return nil, daemonProcessError("configure", errors.New("Node state is outside the managed workspace location"))
	}
	launcher := &DaemonProcessLauncher{
		executable: options.Executable,
		workspace:  options.Workspace,
		nodeState:  options.NodeState,
	}
	executable, err := launcher.openExecutable()
	if err != nil {
		return nil, err
	}
	if err := executable.Close(); err != nil {
		return nil, daemonProcessError("configure", err)
	}
	if _, err := validateDaemonWorkspace(options.Workspace); err != nil {
		return nil, daemonProcessError("configure", err)
	}
	state, err := openIdentityNodeState(options.NodeState)
	if err != nil {
		return nil, daemonProcessError("configure", err)
	}
	state.close()
	return launcher, nil
}

// Launch starts exactly "mnemond serve --project-root <workspace>" with a new
// session, /dev/null stdin and its output appended to the owner-only Node log.
// Context cancellation is observed through PID publication, but no
// CommandContext is used: cancellation after Release must not kill a healthy
// detached daemon.
func (launcher *DaemonProcessLauncher) Launch(ctx context.Context) (DaemonLaunch, error) {
	if launcher == nil || ctx == nil {
		return nil, daemonProcessError("start", errors.New("launcher or context is unavailable"))
	}
	if err := ctx.Err(); err != nil {
		return nil, daemonProcessError("start", err)
	}
	executable, err := launcher.openExecutable()
	if err != nil {
		return nil, err
	}
	defer executable.Close()
	if _, err := validateDaemonWorkspace(launcher.workspace); err != nil {
		return nil, daemonProcessError("validate workspace", err)
	}

	state, err := openIdentityNodeState(launcher.nodeState)
	if err != nil {
		return nil, daemonProcessError("open Node state", err)
	}
	defer state.close()
	if err := lockDaemonProcessNodeState(ctx, state); err != nil {
		return nil, daemonProcessError("lock Node state", err)
	}
	defer state.unlock()
	if err := cleanupDaemonPIDStages(state); err != nil {
		return nil, err
	}
	stale, err := inspectDaemonPID(state, launcher)
	if err != nil {
		return nil, err
	}
	logFile, err := openDaemonLog(state)
	if err != nil {
		return nil, err
	}
	defer logFile.Close()
	nullFile, err := os.Open(os.DevNull)
	if err != nil {
		return nil, daemonProcessError("open stdin", err)
	}
	defer nullFile.Close()
	if err := ctx.Err(); err != nil {
		return nil, daemonProcessError("start", err)
	}

	command := exec.Command(launcher.executable, "serve", "--project-root", launcher.workspace)
	command.Dir = launcher.workspace
	command.Stdin = nullFile
	command.Stdout = logFile
	command.Stderr = logFile
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := command.Start(); err != nil {
		return nil, daemonProcessError("start child", err)
	}
	launch := &daemonProcessLaunch{
		command:   command,
		nodeState: launcher.nodeState,
		state:     daemonProcessOwned,
	}
	abort := func(cause error) (DaemonLaunch, error) {
		abortErr := launch.abortLockedState(state)
		return nil, errors.Join(cause, abortErr)
	}

	if err := validateOpenedExecutable(launcher.executable, executable); err != nil {
		return abort(daemonProcessError("revalidate executable", err))
	}
	if err := validateOpenedDaemonProcessFile(state, daemonLogName, logFile); err != nil {
		return abort(daemonProcessError("revalidate log", err))
	}
	if launcher.testBeforePIDPublication != nil {
		launcher.testBeforePIDPublication()
	}
	if err := ctx.Err(); err != nil {
		return abort(daemonProcessError("publish PID", err))
	}
	record, encoded, err := newDaemonPIDRecord(launcher, command.Process.Pid)
	if err != nil {
		return abort(err)
	}
	published, err := publishDaemonPID(ctx, state, stale, encoded)
	if published != nil {
		launch.pid = published
		launch.record = record
	}
	if err != nil {
		return abort(err)
	}
	return launch, nil
}

func (launcher *DaemonProcessLauncher) openExecutable() (*os.File, error) {
	resolved, err := filepath.EvalSymlinks(launcher.executable)
	if err != nil || resolved != launcher.executable {
		if err == nil {
			err = errors.New("executable path contains a symlink")
		}
		return nil, daemonProcessError("inspect executable", err)
	}
	before, err := os.Lstat(launcher.executable)
	if err != nil {
		return nil, daemonProcessError("inspect executable", err)
	}
	if err := validateExecutableInfo(before); err != nil {
		return nil, daemonProcessError("inspect executable", err)
	}
	fd, err := unix.Open(launcher.executable, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, daemonProcessError("open executable", err)
	}
	file := os.NewFile(uintptr(fd), launcher.executable)
	if file == nil {
		_ = unix.Close(fd)
		return nil, daemonProcessError("open executable", errors.New("open returned no file"))
	}
	if err := validateOpenedExecutable(launcher.executable, file); err != nil {
		_ = file.Close()
		return nil, daemonProcessError("open executable", err)
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		_ = file.Close()
		return nil, daemonProcessError("open executable", errors.New("executable identity changed"))
	}
	return file, nil
}

func validateOpenedExecutable(path string, file *os.File) error {
	if file == nil {
		return errors.New("executable file is unavailable")
	}
	opened, err := file.Stat()
	if err != nil {
		return err
	}
	if err := validateExecutableInfo(opened); err != nil {
		return err
	}
	live, err := os.Lstat(path)
	if err != nil || !os.SameFile(opened, live) {
		return errors.New("executable identity changed")
	}
	return validateExecutableInfo(live)
}

func validateExecutableInfo(info os.FileInfo) error {
	if info == nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return errors.New("executable must be a regular file")
	}
	if info.Mode().Perm()&0o111 == 0 || info.Mode().Perm()&0o022 != 0 {
		return errors.New("executable must be executable and not group or world writable")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("executable owner is unavailable")
	}
	owner := uint32(stat.Uid)
	if owner != uint32(os.Geteuid()) && owner != 0 {
		return errors.New("executable is not owned by the current user or root")
	}
	return nil
}

type daemonPIDRecord struct {
	SchemaVersion int    `json:"schema_version"`
	Instance      string `json:"instance"`
	PID           int    `json:"pid"`
	Executable    string `json:"executable"`
	Workspace     string `json:"workspace"`
	NodeState     string `json:"node_state"`
}

type daemonPIDSnapshot struct {
	info    os.FileInfo
	encoded []byte
}

func newDaemonPIDRecord(launcher *DaemonProcessLauncher, pid int) (daemonPIDRecord, []byte, error) {
	var random [daemonPIDInstanceBytes]byte
	if _, err := io.ReadFull(rand.Reader, random[:]); err != nil {
		return daemonPIDRecord{}, nil, daemonProcessError("allocate PID instance", err)
	}
	record := daemonPIDRecord{
		SchemaVersion: daemonPIDSchemaVersion,
		Instance:      hex.EncodeToString(random[:]),
		PID:           pid,
		Executable:    launcher.executable,
		Workspace:     launcher.workspace,
		NodeState:     launcher.nodeState,
	}
	encoded, err := encodeDaemonPIDRecord(record)
	if err != nil {
		return daemonPIDRecord{}, nil, daemonProcessError("encode PID", err)
	}
	return record, encoded, nil
}

func encodeDaemonPIDRecord(record daemonPIDRecord) ([]byte, error) {
	if err := validateDaemonPIDRecord(record); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	encoded = append(encoded, '\n')
	if int64(len(encoded)) > daemonPIDMaximumBytes {
		return nil, errors.New("PID record exceeds its closed bound")
	}
	return encoded, nil
}

func decodeDaemonPIDRecord(encoded []byte) (daemonPIDRecord, error) {
	if len(encoded) == 0 || int64(len(encoded)) > daemonPIDMaximumBytes {
		return daemonPIDRecord{}, errors.New("PID record size is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var record daemonPIDRecord
	if err := decoder.Decode(&record); err != nil {
		return daemonPIDRecord{}, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("PID record has trailing JSON")
		}
		return daemonPIDRecord{}, err
	}
	canonical, err := encodeDaemonPIDRecord(record)
	if err != nil {
		return daemonPIDRecord{}, err
	}
	if !bytes.Equal(encoded, canonical) {
		return daemonPIDRecord{}, errors.New("PID record is not canonical")
	}
	return record, nil
}

func validateDaemonPIDRecord(record daemonPIDRecord) error {
	if record.SchemaVersion != daemonPIDSchemaVersion || record.PID <= 0 ||
		record.PID > daemonPIDMaximum {
		return errors.New("PID record authority is invalid")
	}
	instance, err := hex.DecodeString(record.Instance)
	if err != nil || len(instance) != daemonPIDInstanceBytes || hex.EncodeToString(instance) != record.Instance {
		return errors.New("PID record instance is invalid")
	}
	for _, path := range []string{record.Executable, record.Workspace, record.NodeState} {
		if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return errors.New("PID record path is invalid")
		}
	}
	return nil
}

func inspectDaemonPID(state *identityNodeState,
	launcher *DaemonProcessLauncher,
) (*daemonPIDSnapshot, error) {
	snapshot, err := readDaemonPIDSnapshot(state)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, daemonProcessError("inspect PID", err)
	}
	record, err := decodeDaemonPIDRecord(snapshot.encoded)
	if err != nil {
		return nil, daemonProcessError("inspect PID", err)
	}
	if record.Executable != launcher.executable || record.Workspace != launcher.workspace ||
		record.NodeState != launcher.nodeState {
		return nil, daemonProcessError("inspect PID", errors.New("existing PID is not managed by this launcher"))
	}
	alive, err := daemonProcessAlive(record.PID)
	if err != nil {
		return nil, daemonProcessError("inspect PID", err)
	}
	if alive {
		return nil, daemonProcessError("inspect PID", errors.New("managed mnemond PID is still alive"))
	}
	return snapshot, nil
}

func readDaemonPIDSnapshot(state *identityNodeState) (*daemonPIDSnapshot, error) {
	if err := state.validateLive(); err != nil {
		return nil, err
	}
	before, err := state.root.Lstat(daemonPIDName)
	if err != nil {
		return nil, err
	}
	if err := validateDaemonProcessFile(before, state.ownerUID); err != nil {
		return nil, err
	}
	file, err := state.root.OpenFile(daemonPIDName, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return nil, errors.New("PID file identity changed")
	}
	if err := validateDaemonProcessFile(opened, state.ownerUID); err != nil {
		return nil, err
	}
	if opened.Size() <= 0 || opened.Size() > daemonPIDMaximumBytes {
		return nil, errors.New("PID file size is invalid")
	}
	encoded, err := io.ReadAll(io.LimitReader(file, daemonPIDMaximumBytes+1))
	if err != nil || int64(len(encoded)) != opened.Size() {
		if err == nil {
			err = errors.New("PID file size changed")
		}
		return nil, err
	}
	after, err := state.root.Lstat(daemonPIDName)
	if err != nil || !os.SameFile(before, after) {
		return nil, errors.New("PID file identity changed")
	}
	if err := validateDaemonProcessFile(after, state.ownerUID); err != nil {
		return nil, err
	}
	return &daemonPIDSnapshot{info: opened, encoded: encoded}, nil
}

func openDaemonLog(state *identityNodeState) (*os.File, error) {
	flags := unix.O_WRONLY | unix.O_APPEND | unix.O_CLOEXEC | unix.O_NOFOLLOW
	fd, err := unix.Openat(int(state.dir.Fd()), daemonLogName,
		flags|unix.O_CREAT|unix.O_EXCL, uint32(daemonProcessFileMode))
	created := err == nil
	if errors.Is(err, syscall.EEXIST) {
		fd, err = unix.Openat(int(state.dir.Fd()), daemonLogName, flags, 0)
	}
	if err != nil {
		return nil, daemonProcessError("open log", err)
	}
	file := os.NewFile(uintptr(fd), filepath.Join(state.path, daemonLogName))
	if file == nil {
		_ = unix.Close(fd)
		return nil, daemonProcessError("open log", errors.New("open returned no file"))
	}
	closeFailure := true
	defer func() {
		if closeFailure {
			_ = file.Close()
		}
	}()
	if created {
		if err := file.Chmod(daemonProcessFileMode); err != nil {
			return nil, daemonProcessError("secure log", err)
		}
		if err := state.dir.Sync(); err != nil {
			return nil, daemonProcessError("persist log", err)
		}
	}
	if err := validateOpenedDaemonProcessFile(state, daemonLogName, file); err != nil {
		return nil, daemonProcessError("inspect log", err)
	}
	closeFailure = false
	return file, nil
}

func validateOpenedDaemonProcessFile(state *identityNodeState, name string, file *os.File) error {
	if state == nil || file == nil {
		return errors.New("process file is unavailable")
	}
	if err := state.validateLive(); err != nil {
		return err
	}
	opened, err := file.Stat()
	if err != nil {
		return err
	}
	if err := validateDaemonProcessFile(opened, state.ownerUID); err != nil {
		return err
	}
	live, err := state.root.Lstat(name)
	if err != nil || !os.SameFile(opened, live) {
		return errors.New("process file identity changed")
	}
	return validateDaemonProcessFile(live, state.ownerUID)
}

func validateDaemonProcessFile(info os.FileInfo, ownerUID uint32) error {
	owner, err := validateIdentityOwnerPath(info, daemonProcessFileMode, false)
	if err != nil || owner != ownerUID {
		if err == nil {
			err = errors.New("process file owner differs from Node state owner")
		}
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return errors.New("process file must have exactly one filesystem link")
	}
	return nil
}

// lockDaemonProcessNodeState is the context-aware equivalent of the identity
// module's blocking lock. Bounded ensure and bounded cleanup must never lose
// their hard deadline while waiting for either an in-process identity writer
// or a writer in another process.
func lockDaemonProcessNodeState(ctx context.Context, state *identityNodeState) error {
	if ctx == nil || state == nil || state.dir == nil {
		return errors.New("Node state lock is unavailable")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if identityProcessMu.TryLock() {
			err := unix.Flock(int(state.dir.Fd()), unix.LOCK_EX|unix.LOCK_NB)
			if err == nil {
				if err := ctx.Err(); err != nil {
					_ = unix.Flock(int(state.dir.Fd()), unix.LOCK_UN)
					identityProcessMu.Unlock()
					return err
				}
				if err := state.validateLive(); err != nil {
					_ = unix.Flock(int(state.dir.Fd()), unix.LOCK_UN)
					identityProcessMu.Unlock()
					return err
				}
				state.locked = true
				return nil
			}
			identityProcessMu.Unlock()
			if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
				return err
			}
		}
		timer := time.NewTimer(daemonProcessLockPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func cleanupDaemonPIDStages(state *identityNodeState) error {
	directory, err := state.root.Open(".")
	if err != nil {
		return daemonProcessError("scan staged PID files", err)
	}
	entries, err := directory.ReadDir(-1)
	closeErr := directory.Close()
	if err != nil || closeErr != nil {
		return daemonProcessError("scan staged PID files", errors.Join(err, closeErr))
	}
	removed := false
	for _, entry := range entries {
		if !isDaemonPIDStageName(entry.Name()) {
			continue
		}
		info, err := state.root.Lstat(entry.Name())
		if err != nil {
			return daemonProcessError("inspect staged PID file", err)
		}
		if err := validateDaemonProcessFile(info, state.ownerUID); err != nil {
			return daemonProcessError("inspect staged PID file", err)
		}
		if err := state.root.Remove(entry.Name()); err != nil {
			return daemonProcessError("remove staged PID file", err)
		}
		removed = true
	}
	if removed {
		if err := state.dir.Sync(); err != nil {
			return daemonProcessError("persist staged PID cleanup", err)
		}
	}
	return nil
}

func isDaemonPIDStageName(name string) bool {
	if len(name) != len(daemonPIDStagePrefix)+daemonPIDInstanceBytes*2+len(daemonPIDStageSuffix) ||
		name[:len(daemonPIDStagePrefix)] != daemonPIDStagePrefix ||
		name[len(name)-len(daemonPIDStageSuffix):] != daemonPIDStageSuffix {
		return false
	}
	encoded := name[len(daemonPIDStagePrefix) : len(name)-len(daemonPIDStageSuffix)]
	decoded, err := hex.DecodeString(encoded)
	return err == nil && len(decoded) == daemonPIDInstanceBytes && hex.EncodeToString(decoded) == encoded
}

func publishDaemonPID(ctx context.Context, state *identityNodeState, stale *daemonPIDSnapshot,
	encoded []byte,
) (*daemonPIDSnapshot, error) {
	stageName, stageFile, err := createDaemonPIDStage(state)
	if err != nil {
		return nil, err
	}
	stagePresent := true
	defer func() {
		_ = stageFile.Close()
		if stagePresent {
			_ = state.root.Remove(stageName)
		}
	}()
	if _, err := stageFile.Write(encoded); err != nil {
		return nil, daemonProcessError("write staged PID", err)
	}
	if err := stageFile.Sync(); err != nil {
		return nil, daemonProcessError("sync staged PID", err)
	}
	stageInfo, err := stageFile.Stat()
	if err != nil {
		return nil, daemonProcessError("inspect staged PID", err)
	}
	if err := validateDaemonProcessFile(stageInfo, state.ownerUID); err != nil {
		return nil, daemonProcessError("inspect staged PID", err)
	}
	stagedPathInfo, err := state.root.Lstat(stageName)
	if err != nil || !os.SameFile(stageInfo, stagedPathInfo) {
		return nil, daemonProcessError("inspect staged PID", errors.New("staged PID identity changed"))
	}
	if err := validateDaemonProcessFile(stagedPathInfo, state.ownerUID); err != nil {
		return nil, daemonProcessError("inspect staged PID", err)
	}
	if err := state.validateLive(); err != nil {
		return nil, daemonProcessError("publish PID", err)
	}

	directoryFD := int(state.dir.Fd())
	if stale == nil {
		if err := ctx.Err(); err != nil {
			return nil, daemonProcessError("publish PID", err)
		}
		if err := unix.Linkat(directoryFD, stageName, directoryFD, daemonPIDName, 0); err != nil {
			return nil, daemonProcessError("publish PID", err)
		}
		if err := state.root.Remove(stageName); err != nil {
			return &daemonPIDSnapshot{info: stageInfo, encoded: append([]byte(nil), encoded...)},
				daemonProcessError("remove staged PID", err)
		}
		stagePresent = false
	} else {
		matches, err := daemonPIDMatches(state, stale)
		if err != nil {
			return nil, daemonProcessError("replace stale PID", err)
		}
		if !matches {
			return nil, daemonProcessError("replace stale PID", errors.New("stale PID identity changed"))
		}
		if err := ctx.Err(); err != nil {
			return nil, daemonProcessError("replace stale PID", err)
		}
		if err := unix.Renameat(directoryFD, stageName, directoryFD, daemonPIDName); err != nil {
			return nil, daemonProcessError("replace stale PID", err)
		}
		stagePresent = false
	}

	confirmed, err := readDaemonPIDSnapshot(state)
	if err != nil || !os.SameFile(stageInfo, confirmed.info) ||
		!bytes.Equal(encoded, confirmed.encoded) {
		return &daemonPIDSnapshot{info: stageInfo, encoded: append([]byte(nil), encoded...)},
			daemonProcessError("verify published PID", errors.New("PID identity changed"))
	}
	if err := state.dir.Sync(); err != nil {
		return confirmed, daemonProcessError("persist published PID", err)
	}
	return confirmed, nil
}

func createDaemonPIDStage(state *identityNodeState) (string, *os.File, error) {
	for range daemonPIDStageAttempts {
		var random [daemonPIDInstanceBytes]byte
		if _, err := io.ReadFull(rand.Reader, random[:]); err != nil {
			return "", nil, daemonProcessError("allocate staged PID", err)
		}
		name := daemonPIDStagePrefix + hex.EncodeToString(random[:]) + daemonPIDStageSuffix
		file, err := state.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL,
			daemonProcessFileMode)
		if err == nil {
			if chmodErr := file.Chmod(daemonProcessFileMode); chmodErr != nil {
				_ = file.Close()
				_ = state.root.Remove(name)
				return "", nil, daemonProcessError("secure staged PID", chmodErr)
			}
			return name, file, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return "", nil, daemonProcessError("create staged PID", err)
		}
	}
	return "", nil, daemonProcessError("create staged PID", errors.New("temporary name space exhausted"))
}

func daemonPIDMatches(state *identityNodeState, expected *daemonPIDSnapshot) (bool, error) {
	if expected == nil {
		return false, nil
	}
	current, err := readDaemonPIDSnapshot(state)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return os.SameFile(expected.info, current.info) && bytes.Equal(expected.encoded, current.encoded), nil
}

func daemonProcessAlive(pid int) (bool, error) {
	err := unix.Kill(pid, 0)
	switch {
	case err == nil, errors.Is(err, syscall.EPERM):
		return true, nil
	case errors.Is(err, syscall.ESRCH):
		return false, nil
	default:
		return false, err
	}
}

type daemonProcessState uint8

const (
	daemonProcessOwned daemonProcessState = iota + 1
	daemonProcessReleased
	daemonProcessTerminated
)

type daemonProcessLaunch struct {
	mu        sync.Mutex
	command   *exec.Cmd
	nodeState string
	pid       *daemonPIDSnapshot
	record    daemonPIDRecord
	state     daemonProcessState
}

func (launch *daemonProcessLaunch) Release() error {
	if launch == nil {
		return nil
	}
	launch.mu.Lock()
	defer launch.mu.Unlock()
	switch launch.state {
	case daemonProcessReleased, daemonProcessTerminated:
		return nil
	case daemonProcessOwned:
	default:
		return daemonProcessError("release child", errors.New("child ownership is invalid"))
	}
	if launch.command == nil || launch.command.Process == nil {
		return daemonProcessError("release child", errors.New("child process is unavailable"))
	}
	// A released daemon deliberately outlives the short-lived
	// mnemon-harness caller. Its PID file remains a diagnostic/stale-start
	// guard; the released handle is not retained as a later kill authority.
	if err := launch.command.Process.Release(); err != nil {
		return daemonProcessError("release child", err)
	}
	launch.command = nil
	launch.state = daemonProcessReleased
	return nil
}

func (launch *daemonProcessLaunch) Terminate(ctx context.Context) error {
	if launch == nil {
		return nil
	}
	if ctx == nil {
		return daemonProcessError("terminate child", errors.New("context is unavailable"))
	}
	launch.mu.Lock()
	defer launch.mu.Unlock()
	switch launch.state {
	case daemonProcessTerminated:
		return nil
	case daemonProcessReleased:
		return daemonProcessError("terminate child", errors.New("child ownership was released"))
	case daemonProcessOwned:
	default:
		return daemonProcessError("terminate child", errors.New("child ownership is invalid"))
	}
	processErr := terminateDaemonCommand(ctx, launch.command)
	cleanupErr := cleanupPublishedDaemonPID(ctx, launch.nodeState, launch.pid)
	launch.command = nil
	launch.state = daemonProcessTerminated
	return errors.Join(processErr, cleanupErr)
}

func (launch *daemonProcessLaunch) abortLockedState(state *identityNodeState) error {
	if launch == nil {
		return nil
	}
	bounded, cancel := context.WithTimeout(context.Background(), daemonCleanupDeadline)
	defer cancel()
	processErr := terminateDaemonCommand(bounded, launch.command)
	var cleanupErr error
	if launch.pid != nil {
		cleanupErr = removeDaemonPIDIfOwned(state, launch.pid)
	}
	launch.command = nil
	launch.state = daemonProcessTerminated
	return errors.Join(processErr, cleanupErr)
}

func terminateDaemonCommand(ctx context.Context, command *exec.Cmd) error {
	if command == nil || command.Process == nil {
		return daemonProcessError("terminate child", errors.New("child process is unavailable"))
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		_ = command.Process.Kill()
	}
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()
	select {
	case err := <-waited:
		if err != nil {
			var exitError *exec.ExitError
			if !errors.As(err, &exitError) {
				return daemonProcessError("wait for child", err)
			}
		}
		return nil
	case <-ctx.Done():
		_ = command.Process.Kill()
		err := <-waited
		var exitError *exec.ExitError
		if err != nil && !errors.As(err, &exitError) {
			return errors.Join(daemonProcessError("wait for child", err),
				daemonProcessError("terminate child", ctx.Err()))
		}
		return daemonProcessError("terminate child", ctx.Err())
	}
}

func cleanupPublishedDaemonPID(ctx context.Context, nodeState string,
	expected *daemonPIDSnapshot,
) error {
	if expected == nil {
		return nil
	}
	state, err := openIdentityNodeState(nodeState)
	if err != nil {
		return daemonProcessError("clean PID", err)
	}
	defer state.close()
	if err := lockDaemonProcessNodeState(ctx, state); err != nil {
		return daemonProcessError("clean PID", err)
	}
	defer state.unlock()
	return removeDaemonPIDIfOwned(state, expected)
}

func removeDaemonPIDIfOwned(state *identityNodeState, expected *daemonPIDSnapshot) error {
	matches, err := daemonPIDMatches(state, expected)
	if err != nil {
		// A replacement that is absent, unsafe or unreadable is not launch-owned
		// state. Preserve it and do not turn successful child termination into a
		// deletion oracle for another writer.
		return nil
	}
	if !matches {
		return nil
	}
	if err := state.root.Remove(daemonPIDName); err != nil {
		return daemonProcessError("remove PID", err)
	}
	if err := state.dir.Sync(); err != nil {
		return daemonProcessError("persist PID removal", err)
	}
	return nil
}

func daemonProcessError(operation string, err error) error {
	return fmt.Errorf("%w: %s: %w", ErrDaemonProcess, operation, err)
}
