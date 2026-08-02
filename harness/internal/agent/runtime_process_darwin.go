//go:build darwin

package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

const (
	darwinRuntimeProcessStopped  = int8(4)
	darwinRuntimeProcessZombie   = int8(5)
	darwinRuntimeProcessStopWait = 2 * time.Second
	darwinRuntimeProcessExitWait = 2 * time.Second
	darwinRuntimeProcessPoll     = 5 * time.Millisecond
	darwinRuntimeProcessEntryMax = 131072
)

var errDarwinRuntimeProcessNotExist = errors.New("Darwin process does not exist")

type darwinRuntimeProcessSnapshot struct {
	pid        int
	pgid       int
	sid        int
	uid        uint32
	startToken string
	state      int8
}

type darwinRuntimeProcessSystem interface {
	BootID() (string, error)
	EffectiveUID() uint32
	Snapshot(int) (darwinRuntimeProcessSnapshot, error)
	GroupExists(int) (bool, error)
	GroupHasLiveMembers(context.Context, int) (bool, error)
	GroupHasUnstoppedMembers(context.Context, int) (bool, error)
	SignalGroup(int, syscall.Signal) error
}

type systemDarwinRuntimeProcess struct{}

func captureSystemRuntimeProcess(pid int) (runtimeProcessIDs, error) {
	return captureDarwinRuntimeProcess(pid, systemDarwinRuntimeProcess{})
}

func recoverSystemRuntimeProcess(ctx context.Context,
	ids runtimeProcessIDs,
) (runtimeProcessPlatformProof, error) {
	return recoverDarwinRuntimeProcess(ctx, ids, systemDarwinRuntimeProcess{})
}

func observeOwnedSystemRuntimeProcess(ctx context.Context,
	ids runtimeProcessIDs,
) (runtimeProcessPlatformProof, error) {
	return observeOwnedDarwinRuntimeProcess(ctx, ids, systemDarwinRuntimeProcess{})
}

func terminateOwnedSystemRuntimeProcess(ctx context.Context,
	ids runtimeProcessIDs,
) (runtimeProcessPlatformProof, error) {
	return terminateOwnedDarwinRuntimeProcess(ctx, ids, systemDarwinRuntimeProcess{})
}

// checkSystemRuntimeProcessSupport probes the observation primitives used by
// restart recovery. The daemon itself need not be a session leader; Setsid is
// required only for each managed Runtime child.
func checkSystemRuntimeProcessPrimitives() error {
	return checkDarwinRuntimeProcessSupport(os.Getpid(), systemDarwinRuntimeProcess{})
}

func checkDarwinRuntimeProcessSupport(pid int, system darwinRuntimeProcessSystem) error {
	if system == nil || pid <= 0 {
		return errors.New("Darwin Runtime process support probe input is invalid")
	}
	bootID, err := system.BootID()
	if err != nil || !canonicalRuntimeProcessUUID(bootID) {
		if err == nil {
			err = errors.New("Darwin boot session UUID is invalid")
		}
		return fmt.Errorf("read Darwin boot session: %w", err)
	}
	snapshot, err := system.Snapshot(pid)
	if err != nil {
		return fmt.Errorf("read Darwin self process identity: %w", err)
	}
	if snapshot.pid != pid || snapshot.pgid <= 0 || snapshot.sid <= 0 ||
		snapshot.uid != system.EffectiveUID() || snapshot.state == darwinRuntimeProcessZombie ||
		runtimeProcessTokenBootID(runtimeProcessIDs{StartToken: snapshot.startToken}) != bootID {
		return errors.New("Darwin self process identity is inconsistent")
	}
	if err := validateRuntimeProcessStartToken("darwin", snapshot.startToken); err != nil {
		return fmt.Errorf("validate Darwin self process identity: %w", err)
	}
	groupExists, err := system.GroupExists(snapshot.pgid)
	if err != nil || !groupExists {
		if err == nil {
			err = errors.New("Darwin self process group is absent")
		}
		return fmt.Errorf("inspect Darwin self process group: %w", err)
	}
	live, err := system.GroupHasLiveMembers(context.Background(), snapshot.pgid)
	if err != nil || !live {
		if err == nil {
			err = errors.New("Darwin self process group has no live member")
		}
		return fmt.Errorf("scan Darwin self process group: %w", err)
	}
	return nil
}

func captureDarwinRuntimeProcess(pid int,
	system darwinRuntimeProcessSystem,
) (runtimeProcessIDs, error) {
	if system == nil || pid <= 1 {
		return runtimeProcessIDs{}, errors.New("Darwin process capture input is invalid")
	}
	snapshot, err := system.Snapshot(pid)
	if err != nil {
		return runtimeProcessIDs{}, err
	}
	if snapshot.state == darwinRuntimeProcessZombie {
		return runtimeProcessIDs{}, errors.New("initialized Darwin process is already a zombie")
	}
	if snapshot.pid != pid || snapshot.pgid != pid || snapshot.sid != pid ||
		snapshot.uid != system.EffectiveUID() {
		return runtimeProcessIDs{}, errors.New("Darwin process is not the exact isolated child")
	}
	return runtimeProcessIDs{SchemaVersion: runtimeProcessSchemaVersion, OS: "darwin",
		PID: pid, PGID: snapshot.pgid, SID: snapshot.sid, UID: snapshot.uid,
		StartToken: snapshot.startToken}, nil
}

func recoverDarwinRuntimeProcess(ctx context.Context, ids runtimeProcessIDs,
	system darwinRuntimeProcessSystem,
) (runtimeProcessPlatformProof, error) {
	if system == nil {
		return runtimeProcessPlatformProof{}, errors.New("Darwin process inspector is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return runtimeProcessPlatformProof{}, err
	}
	bootID, err := system.BootID()
	if err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("read Darwin boot session: %w", err)
	}
	if bootID != runtimeProcessTokenBootID(ids) {
		return runtimeProcessPlatformProof{state: runtimeProcessGone,
			method: "boot_session_changed"}, nil
	}
	if ids.UID != system.EffectiveUID() {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: durable UID differs from daemon UID",
			ErrRuntimeProcessUncertain)
	}
	snapshot, err := system.Snapshot(ids.PID)
	if errors.Is(err, errDarwinRuntimeProcessNotExist) {
		return darwinRuntimeProcessAbsentProof(ids, system)
	}
	if err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: inspect Darwin leader: %v",
			ErrRuntimeProcessUncertain, err)
	}
	if err := ctx.Err(); err != nil {
		return runtimeProcessPlatformProof{}, err
	}
	if !sameDarwinRuntimeProcess(ids, snapshot) {
		exists, groupErr := system.GroupExists(ids.PGID)
		if groupErr != nil {
			return runtimeProcessPlatformProof{}, fmt.Errorf("%w: inspect reused Darwin group: %v",
				ErrRuntimeProcessUncertain, groupErr)
		}
		if exists {
			return runtimeProcessPlatformProof{}, fmt.Errorf("%w: PID was reused while the durable group exists",
				ErrRuntimeProcessUncertain)
		}
		return runtimeProcessPlatformProof{state: runtimeProcessReused,
			method: "pid_reused_group_absent"}, nil
	}
	if snapshot.state == darwinRuntimeProcessZombie {
		// kern.proc.all is one kernel process-table snapshot rather than a
		// cursor over per-process files. If it contains no live group member,
		// the exact zombie cannot create another one. Restart recovery remains
		// observation-only and never signals the numeric PGID.
		if err := waitDarwinRuntimeProcessGroupExit(ctx, ids.PGID, system); err != nil {
			return runtimeProcessPlatformProof{}, fmt.Errorf("%w: stabilize zombie Darwin group: %v",
				ErrRuntimeProcessUncertain, err)
		}
		return runtimeProcessPlatformProof{state: runtimeProcessExactExited,
			method: "exact_process_exited"}, nil
	}
	// x/sys exposes only numeric kill(2) on Darwin. There is no restart-safe
	// handle equivalent to Linux pidfd, so a start-time check followed by kill
	// would retain a PID-reuse race. The strict R5 path waits for stdin EOF to
	// end this process and refuses to signal it after daemon ownership is lost.
	return runtimeProcessPlatformProof{}, fmt.Errorf("%w: exact Darwin Runtime must exit before restart recovery",
		ErrRuntimeProcessLive)
}

// observeOwnedDarwinRuntimeProcess is the adapter's non-blocking exit probe.
// In particular, an exact zombie is still the retained, unreaped direct child:
// it must flow through owned termination so that any descendants are handled
// before the adapter calls Wait.
func observeOwnedDarwinRuntimeProcess(ctx context.Context, ids runtimeProcessIDs,
	system darwinRuntimeProcessSystem,
) (runtimeProcessPlatformProof, error) {
	if system == nil || ctx == nil {
		return runtimeProcessPlatformProof{}, errors.New("Darwin owned process inspector is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return runtimeProcessPlatformProof{}, err
	}
	if err := validateRuntimeProcessIDs(ids, "darwin"); err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: invalid Darwin owned process IDs: %v",
			ErrRuntimeProcessUncertain, err)
	}
	bootID, err := system.BootID()
	if err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: read Darwin boot session: %v",
			ErrRuntimeProcessUncertain, err)
	}
	if bootID != runtimeProcessTokenBootID(ids) {
		return runtimeProcessPlatformProof{state: runtimeProcessGone,
			method: "boot_session_changed"}, nil
	}
	if ids.UID != system.EffectiveUID() {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: durable UID differs from daemon UID",
			ErrRuntimeProcessUncertain)
	}
	snapshot, err := system.Snapshot(ids.PID)
	switch {
	case errors.Is(err, errDarwinRuntimeProcessNotExist):
		return darwinRuntimeProcessAbsentProof(ids, system)
	case err != nil:
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: inspect Darwin owned leader: %v",
			ErrRuntimeProcessUncertain, err)
	case !sameDarwinRuntimeProcess(ids, snapshot):
		return darwinRuntimeProcessReusedProof(ids, system)
	default:
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: exact Darwin owned Runtime is retained",
			ErrRuntimeProcessLive)
	}
}

// terminateOwnedDarwinRuntimeProcess may signal only while the adapter retains
// the exact direct child without calling Wait. That contract prevents PID and
// PGID reuse: a live child or its zombie remains a numeric anchor throughout
// this function. Restart recovery deliberately does not call this helper.
func terminateOwnedDarwinRuntimeProcess(ctx context.Context, ids runtimeProcessIDs,
	system darwinRuntimeProcessSystem,
) (proof runtimeProcessPlatformProof, terminationErr error) {
	if system == nil || ctx == nil {
		return runtimeProcessPlatformProof{}, errors.New("Darwin owned process terminator is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return runtimeProcessPlatformProof{}, err
	}
	if err := validateRuntimeProcessIDs(ids, "darwin"); err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: invalid Darwin owned process IDs: %v",
			ErrRuntimeProcessUncertain, err)
	}
	bootID, err := system.BootID()
	if err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: read Darwin boot session: %v",
			ErrRuntimeProcessUncertain, err)
	}
	if bootID != runtimeProcessTokenBootID(ids) {
		return runtimeProcessPlatformProof{state: runtimeProcessGone,
			method: "boot_session_changed"}, nil
	}
	if ids.UID != system.EffectiveUID() {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: durable UID differs from daemon UID",
			ErrRuntimeProcessUncertain)
	}
	snapshot, err := system.Snapshot(ids.PID)
	if errors.Is(err, errDarwinRuntimeProcessNotExist) {
		return darwinRuntimeProcessAbsentProof(ids, system)
	}
	if err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: inspect retained Darwin leader: %v",
			ErrRuntimeProcessUncertain, err)
	}
	if !sameDarwinRuntimeProcess(ids, snapshot) {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: retained Darwin leader identity changed",
			ErrRuntimeProcessUncertain)
	}
	if snapshot.state == darwinRuntimeProcessZombie {
		live, liveErr := system.GroupHasLiveMembers(ctx, ids.PGID)
		if liveErr != nil {
			return runtimeProcessPlatformProof{}, fmt.Errorf("%w: inspect retained Darwin zombie group: %v",
				ErrRuntimeProcessUncertain, liveErr)
		}
		if !live {
			return runtimeProcessPlatformProof{state: runtimeProcessExactExited,
				method: "exact_process_exited"}, nil
		}
	}
	if err := system.SignalGroup(ids.PGID, syscall.SIGSTOP); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return proveOwnedDarwinRuntimeZombieExit(ctx, ids, system)
		}
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: stop retained Darwin process group: %v",
			ErrRuntimeProcessUncertain, err)
	}
	resumeRequired := true
	defer func() {
		if !resumeRequired || terminationErr == nil {
			return
		}
		if err := system.SignalGroup(ids.PGID, syscall.SIGCONT); err != nil &&
			!errors.Is(err, syscall.ESRCH) {
			terminationErr = errors.Join(terminationErr, fmt.Errorf(
				"%w: resume retained Darwin process group after failed termination: %v",
				ErrRuntimeProcessUncertain, err))
		}
	}()
	if err := waitOwnedDarwinRuntimeProcessGroupStopped(ctx, ids, system); err != nil {
		return runtimeProcessPlatformProof{}, err
	}
	anchor, err := system.Snapshot(ids.PID)
	if err != nil || !sameDarwinRuntimeProcess(ids, anchor) ||
		(anchor.state != darwinRuntimeProcessStopped && anchor.state != darwinRuntimeProcessZombie) {
		if err == nil {
			err = errors.New("retained leader is neither stopped nor a zombie")
		}
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: revalidate stopped Darwin group anchor: %v",
			ErrRuntimeProcessUncertain, err)
	}
	if err := system.SignalGroup(ids.PGID, syscall.SIGKILL); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			if _, proofErr := proveOwnedDarwinRuntimeZombieExit(ctx, ids, system); proofErr != nil {
				return runtimeProcessPlatformProof{}, proofErr
			}
			resumeRequired = false
			return runtimeProcessPlatformProof{state: runtimeProcessExactExited,
				method: "darwin_owned_group_kill", signals: []string{"SIGSTOP", "SIGKILL"}}, nil
		}
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: kill retained Darwin process group: %v",
			ErrRuntimeProcessUncertain, err)
	}
	resumeRequired = false
	if err := waitDarwinRuntimeProcessGroupExit(ctx, ids.PGID, system); err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: confirm killed Darwin group exit: %v",
			ErrRuntimeProcessUncertain, err)
	}
	return runtimeProcessPlatformProof{state: runtimeProcessExactExited,
		method: "darwin_owned_group_kill", signals: []string{"SIGSTOP", "SIGKILL"}}, nil
}

func waitOwnedDarwinRuntimeProcessGroupStopped(ctx context.Context, ids runtimeProcessIDs,
	system darwinRuntimeProcessSystem,
) error {
	bounded, cancel := context.WithTimeout(ctx, darwinRuntimeProcessStopWait)
	defer cancel()
	ticker := time.NewTicker(darwinRuntimeProcessPoll)
	defer ticker.Stop()
	stableStopped := 0
	for {
		unstopped, err := system.GroupHasUnstoppedMembers(bounded, ids.PGID)
		if err != nil {
			return fmt.Errorf("%w: scan stopped Darwin process group: %v",
				ErrRuntimeProcessUncertain, err)
		}
		if unstopped {
			stableStopped = 0
			// The atomic process-table snapshot found a member that had not yet
			// consumed the stop. Refresh the group signal; once a snapshot finds
			// every live member stopped, none of them can fork afterwards.
			if err := system.SignalGroup(ids.PGID, syscall.SIGSTOP); err != nil {
				if errors.Is(err, syscall.ESRCH) {
					if _, proofErr := proveOwnedDarwinRuntimeZombieExit(bounded, ids, system); proofErr != nil {
						return proofErr
					}
					return nil
				}
				return fmt.Errorf("%w: refresh Darwin process-group stop: %v",
					ErrRuntimeProcessUncertain, err)
			}
		} else {
			stableStopped++
		}
		if stableStopped == 2 {
			return nil
		}
		select {
		case <-bounded.Done():
			return fmt.Errorf("%w: wait for stopped Darwin process group: %v",
				ErrRuntimeProcessUncertain, bounded.Err())
		case <-ticker.C:
		}
	}
}

// proveOwnedDarwinRuntimeZombieExit is the only way an owned signal ESRCH is
// accepted. It rechecks the retained exact zombie around atomic process-table
// snapshots; a stale or ambiguous numeric group cannot authorize exit.
func proveOwnedDarwinRuntimeZombieExit(ctx context.Context, ids runtimeProcessIDs,
	system darwinRuntimeProcessSystem,
) (runtimeProcessPlatformProof, error) {
	snapshot, err := system.Snapshot(ids.PID)
	if err != nil || !sameDarwinRuntimeProcess(ids, snapshot) ||
		snapshot.state != darwinRuntimeProcessZombie {
		if err == nil {
			err = errors.New("retained leader is not the exact zombie")
		}
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: prove Darwin signal ESRCH: %v",
			ErrRuntimeProcessUncertain, err)
	}
	if err := waitDarwinRuntimeProcessGroupExit(ctx, ids.PGID, system); err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: prove Darwin signal ESRCH group exit: %v",
			ErrRuntimeProcessUncertain, err)
	}
	snapshot, err = system.Snapshot(ids.PID)
	if err != nil || !sameDarwinRuntimeProcess(ids, snapshot) ||
		snapshot.state != darwinRuntimeProcessZombie {
		if err == nil {
			err = errors.New("retained leader changed after empty group proof")
		}
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: finish Darwin signal ESRCH proof: %v",
			ErrRuntimeProcessUncertain, err)
	}
	return runtimeProcessPlatformProof{state: runtimeProcessExactExited,
		method: "exact_process_exited"}, nil
}

func waitDarwinRuntimeProcessGroupExit(ctx context.Context, pgid int,
	system darwinRuntimeProcessSystem,
) error {
	bounded, cancel := context.WithTimeout(ctx, darwinRuntimeProcessExitWait)
	defer cancel()
	ticker := time.NewTicker(darwinRuntimeProcessPoll)
	defer ticker.Stop()
	stableAbsence := 0
	for {
		live, err := system.GroupHasLiveMembers(bounded, pgid)
		if err != nil {
			return err
		}
		if live {
			stableAbsence = 0
		} else {
			stableAbsence++
		}
		if stableAbsence == 2 {
			return nil
		}
		select {
		case <-bounded.Done():
			return bounded.Err()
		case <-ticker.C:
		}
	}
}

func darwinRuntimeProcessAbsentProof(ids runtimeProcessIDs,
	system darwinRuntimeProcessSystem,
) (runtimeProcessPlatformProof, error) {
	exists, err := system.GroupExists(ids.PGID)
	if err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: inspect absent Darwin group: %v",
			ErrRuntimeProcessUncertain, err)
	}
	if exists {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: leader is absent while its process group exists",
			ErrRuntimeProcessUncertain)
	}
	return runtimeProcessPlatformProof{state: runtimeProcessGone,
		method: "process_and_group_absent"}, nil
}

func darwinRuntimeProcessReusedProof(ids runtimeProcessIDs,
	system darwinRuntimeProcessSystem,
) (runtimeProcessPlatformProof, error) {
	exists, err := system.GroupExists(ids.PGID)
	if err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: inspect reused Darwin group: %v",
			ErrRuntimeProcessUncertain, err)
	}
	if exists {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: PID was reused while the durable group exists",
			ErrRuntimeProcessUncertain)
	}
	return runtimeProcessPlatformProof{state: runtimeProcessReused,
		method: "pid_reused_group_absent"}, nil
}

func sameDarwinRuntimeProcess(ids runtimeProcessIDs, snapshot darwinRuntimeProcessSnapshot) bool {
	return snapshot.pid == ids.PID && snapshot.pgid == ids.PGID && snapshot.sid == ids.SID &&
		snapshot.uid == ids.UID && snapshot.startToken == ids.StartToken
}

func (systemDarwinRuntimeProcess) BootID() (string, error) {
	raw, err := unix.Sysctl("kern.bootsessionuuid")
	if err != nil {
		return "", err
	}
	parsed, err := uuid.Parse(strings.TrimSpace(strings.TrimRight(raw, "\x00")))
	if err != nil {
		return "", errors.New("Darwin boot session UUID is invalid")
	}
	return parsed.String(), nil
}

func (systemDarwinRuntimeProcess) EffectiveUID() uint32 {
	return uint32(unix.Geteuid())
}

func (systemDarwinRuntimeProcess) Snapshot(pid int) (darwinRuntimeProcessSnapshot, error) {
	first, err := darwinRuntimeProcessKinfo(pid)
	if err != nil {
		return darwinRuntimeProcessSnapshot{}, err
	}
	pgid, err := unix.Getpgid(pid)
	if errors.Is(err, unix.ESRCH) {
		return darwinRuntimeProcessStableZombie(pid, first)
	}
	if err != nil {
		return darwinRuntimeProcessSnapshot{}, err
	}
	sid, err := unix.Getsid(pid)
	if errors.Is(err, unix.ESRCH) {
		return darwinRuntimeProcessStableZombie(pid, first)
	}
	if err != nil {
		return darwinRuntimeProcessSnapshot{}, err
	}
	second, err := darwinRuntimeProcessKinfo(pid)
	if err != nil {
		return darwinRuntimeProcessSnapshot{}, err
	}
	if first.pid != second.pid || first.startToken != second.startToken || first.uid != second.uid ||
		first.pgid != second.pgid || first.pgid != pgid {
		return darwinRuntimeProcessSnapshot{}, errors.New("Darwin process identity changed during observation")
	}
	second.sid = sid
	return second, nil
}

func darwinRuntimeProcessKinfo(pid int) (darwinRuntimeProcessSnapshot, error) {
	info, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if errors.Is(err, unix.ESRCH) || errors.Is(err, unix.ENOENT) {
		return darwinRuntimeProcessSnapshot{}, errDarwinRuntimeProcessNotExist
	}
	if err != nil {
		return darwinRuntimeProcessSnapshot{}, err
	}
	if info == nil || info.Proc.P_pid == 0 {
		return darwinRuntimeProcessSnapshot{}, errDarwinRuntimeProcessNotExist
	}
	if info.Proc.P_pid != int32(pid) || info.Proc.P_starttime.Sec <= 0 ||
		info.Proc.P_starttime.Usec < 0 || info.Proc.P_starttime.Usec >= 1_000_000 || info.Eproc.Pgid <= 0 {
		return darwinRuntimeProcessSnapshot{}, errors.New("Darwin process kernel identity is invalid")
	}
	bootID, err := (systemDarwinRuntimeProcess{}).BootID()
	if err != nil {
		return darwinRuntimeProcessSnapshot{}, err
	}
	token := fmt.Sprintf("darwin:%s:%d:%d", bootID, info.Proc.P_starttime.Sec,
		info.Proc.P_starttime.Usec)
	return darwinRuntimeProcessSnapshot{pid: pid, pgid: int(info.Eproc.Pgid),
		uid: info.Eproc.Ucred.Uid, startToken: token, state: info.Proc.P_stat}, nil
}

func (systemDarwinRuntimeProcess) GroupExists(pgid int) (bool, error) {
	err := unix.Kill(-pgid, 0)
	switch {
	case err == nil, errors.Is(err, unix.EPERM):
		return true, nil
	case errors.Is(err, unix.ESRCH):
		return false, nil
	default:
		return false, err
	}
}

func (systemDarwinRuntimeProcess) GroupHasLiveMembers(ctx context.Context, pgid int) (bool, error) {
	live, _, err := scanDarwinRuntimeProcessGroup(ctx, pgid)
	return live, err
}

func (systemDarwinRuntimeProcess) GroupHasUnstoppedMembers(ctx context.Context, pgid int) (bool, error) {
	_, unstopped, err := scanDarwinRuntimeProcessGroup(ctx, pgid)
	return unstopped, err
}

func scanDarwinRuntimeProcessGroup(ctx context.Context, pgid int) (bool, bool, error) {
	if ctx == nil || pgid <= 0 {
		return false, false, errors.New("Darwin group scan input is invalid")
	}
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return false, false, err
	}
	if len(processes) > darwinRuntimeProcessEntryMax {
		return false, false, errors.New("Darwin process table exceeds its scan bound")
	}
	live := false
	for index := range processes {
		if err := ctx.Err(); err != nil {
			return false, false, err
		}
		process := &processes[index]
		if int(process.Eproc.Pgid) != pgid || process.Proc.P_stat == darwinRuntimeProcessZombie {
			continue
		}
		live = true
		if process.Proc.P_stat != darwinRuntimeProcessStopped {
			return true, true, nil
		}
	}
	return live, false, nil
}

func (systemDarwinRuntimeProcess) SignalGroup(pgid int, signal syscall.Signal) error {
	if pgid <= 1 {
		return errors.New("Darwin process group signal target is invalid")
	}
	return unix.Kill(-pgid, signal)
}
