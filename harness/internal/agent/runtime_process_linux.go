//go:build linux

package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

const (
	linuxRuntimeProcessStopWait = 2 * time.Second
	linuxRuntimeProcessExitWait = 2 * time.Second
	linuxRuntimeProcessPoll     = 5 * time.Millisecond
)

type linuxRuntimeProcessSnapshot struct {
	pid        int
	pgid       int
	sid        int
	uid        uint32
	startToken string
	state      byte
}

type linuxRuntimeProcessSystem interface {
	BootID() (string, error)
	EffectiveUID() uint32
	Snapshot(int) (linuxRuntimeProcessSnapshot, error)
	GroupExists(int) (bool, error)
	GroupHasLiveMembers(context.Context, int) (bool, error)
	GroupHasUnstoppedMembers(context.Context, int) (bool, error)
	SignalGroup(int, syscall.Signal) error
}

func captureSystemRuntimeProcess(pid int) (runtimeProcessIDs, error) {
	return captureLinuxRuntimeProcess(pid, systemLinuxRuntimeProcess{})
}

func recoverSystemRuntimeProcess(ctx context.Context,
	ids runtimeProcessIDs,
) (runtimeProcessPlatformProof, error) {
	return recoverLinuxRuntimeProcess(ctx, ids, systemLinuxRuntimeProcess{})
}

func observeOwnedSystemRuntimeProcess(ctx context.Context,
	ids runtimeProcessIDs,
) (runtimeProcessPlatformProof, error) {
	return observeLinuxRuntimeProcess(ctx, ids, systemLinuxRuntimeProcess{})
}

func terminateOwnedSystemRuntimeProcess(ctx context.Context,
	ids runtimeProcessIDs,
) (runtimeProcessPlatformProof, error) {
	return terminateOwnedLinuxRuntimeProcess(ctx, ids, systemLinuxRuntimeProcess{})
}

// checkSystemRuntimeProcessSupport probes the minimum restart-safe primitives
// used by the secure adapter default. The daemon itself need not be a session
// leader; Setsid is a requirement only for each managed Runtime child.
func checkSystemRuntimeProcessPrimitives() error {
	return checkLinuxRuntimeProcessSupport(os.Getpid(), systemLinuxRuntimeProcess{})
}

func checkLinuxRuntimeProcessSupport(pid int, system linuxRuntimeProcessSystem) error {
	if system == nil || pid <= 0 {
		return errors.New("Linux Runtime process support probe input is invalid")
	}
	bootID, err := system.BootID()
	if err != nil || !canonicalRuntimeProcessUUID(bootID) {
		if err == nil {
			err = errors.New("Linux boot ID is invalid")
		}
		return fmt.Errorf("read Linux boot ID: %w", err)
	}
	snapshot, err := system.Snapshot(pid)
	if err != nil {
		return fmt.Errorf("read Linux self process identity: %w", err)
	}
	if snapshot.pid != pid || snapshot.pgid <= 0 || snapshot.sid <= 0 ||
		snapshot.uid != system.EffectiveUID() || linuxRuntimeProcessExited(snapshot.state) ||
		runtimeProcessTokenBootID(runtimeProcessIDs{StartToken: snapshot.startToken}) != bootID {
		return errors.New("Linux self process identity is inconsistent")
	}
	if err := validateRuntimeProcessStartToken("linux", snapshot.startToken); err != nil {
		return fmt.Errorf("validate Linux self process identity: %w", err)
	}
	groupExists, err := system.GroupExists(snapshot.pgid)
	if err != nil || !groupExists {
		if err == nil {
			err = errors.New("Linux self process group is absent")
		}
		return fmt.Errorf("inspect Linux self process group: %w", err)
	}
	live, err := system.GroupHasLiveMembers(context.Background(), snapshot.pgid)
	if err != nil || !live {
		if err == nil {
			err = errors.New("Linux self process group has no live member")
		}
		return fmt.Errorf("scan Linux self process group: %w", err)
	}
	// kill(-1, 0) is the Linux broadcast form, not a process-group probe.
	// Containerized mnemond commonly runs in group 1, so only exercise the
	// negative-PGID syscall here when its argument is unambiguous. Every
	// managed Runtime leader is separately required to have PID/PGID > 1.
	if snapshot.pgid > 1 {
		if err := system.SignalGroup(snapshot.pgid, 0); err != nil {
			return fmt.Errorf("signal Linux self process group: %w", err)
		}
	}
	return nil
}

func captureLinuxRuntimeProcess(pid int,
	system linuxRuntimeProcessSystem,
) (runtimeProcessIDs, error) {
	if system == nil || pid <= 1 {
		return runtimeProcessIDs{}, errors.New("Linux process capture input is invalid")
	}
	snapshot, err := system.Snapshot(pid)
	if err != nil {
		return runtimeProcessIDs{}, err
	}
	if linuxRuntimeProcessExited(snapshot.state) {
		return runtimeProcessIDs{}, errors.New("initialized Linux process has already exited")
	}
	if snapshot.pid != pid || snapshot.pgid != pid || snapshot.sid != pid ||
		snapshot.uid != system.EffectiveUID() {
		return runtimeProcessIDs{}, errors.New("Linux process is not the exact isolated child")
	}
	ids := runtimeProcessIDs{SchemaVersion: runtimeProcessSchemaVersion, OS: "linux",
		PID: pid, PGID: snapshot.pgid, SID: snapshot.sid, UID: snapshot.uid,
		StartToken: snapshot.startToken}
	bound, bindErr := system.Snapshot(pid)
	if bindErr != nil || !sameLinuxRuntimeProcess(ids, bound) ||
		linuxRuntimeProcessExited(bound.state) {
		if bindErr == nil {
			bindErr = errors.New("Linux child changed while binding durable identity")
		}
		return runtimeProcessIDs{}, bindErr
	}
	return ids, nil
}

func recoverLinuxRuntimeProcess(ctx context.Context, ids runtimeProcessIDs,
	system linuxRuntimeProcessSystem,
) (runtimeProcessPlatformProof, error) {
	if system == nil {
		return runtimeProcessPlatformProof{}, errors.New("Linux process inspector is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return runtimeProcessPlatformProof{}, err
	}
	bootID, err := system.BootID()
	if err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("read Linux boot ID: %w", err)
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
	if errors.Is(err, errLinuxRuntimeProcessNotExist) {
		return linuxRuntimeProcessAbsentProof(ids, system)
	}
	if err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: inspect Linux leader: %v",
			ErrRuntimeProcessUncertain, err)
	}
	if !sameLinuxRuntimeProcess(ids, snapshot) {
		return linuxRuntimeProcessReusedProof(ids, system)
	}
	if linuxRuntimeProcessExited(snapshot.state) {
		return linuxRuntimeProcessExitedProof(ids, system)
	}

	// Restart recovery never owns Wait for this process. A pidfd would bind
	// only the leader, not the numeric process-group signal target, so it cannot
	// make group termination safe against leader reaping and PGID reuse. The
	// closed R5 rule is therefore the same on Linux and Darwin: exact live
	// Runtimes must exit through their closed stdin; startup remains unready
	// while a live orphan persists.
	return runtimeProcessPlatformProof{}, fmt.Errorf("%w: exact Linux Runtime must exit before restart recovery",
		ErrRuntimeProcessLive)
}

func observeLinuxRuntimeProcess(ctx context.Context, ids runtimeProcessIDs,
	system linuxRuntimeProcessSystem,
) (runtimeProcessPlatformProof, error) {
	if system == nil {
		return runtimeProcessPlatformProof{}, errors.New("Linux process inspector is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return runtimeProcessPlatformProof{}, err
	}
	bootID, err := system.BootID()
	if err != nil {
		return runtimeProcessPlatformProof{}, err
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
	case errors.Is(err, errLinuxRuntimeProcessNotExist):
		return linuxRuntimeProcessAbsentProof(ids, system)
	case err != nil:
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: inspect Linux leader: %v",
			ErrRuntimeProcessUncertain, err)
	case !sameLinuxRuntimeProcess(ids, snapshot):
		return linuxRuntimeProcessReusedProof(ids, system)
	case linuxRuntimeProcessExited(snapshot.state):
		// Observe is intentionally instantaneous. The retained, unreaped child
		// still anchors its PGID, so owned Terminate can now decide whether the
		// group needs a stop/kill barrier without an observation timeout
		// stranding a zombie or live descendant.
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: exact Linux Runtime is a zombie",
			ErrRuntimeProcessLive)
	default:
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: exact Linux Runtime is still live",
			ErrRuntimeProcessLive)
	}
}

// terminateOwnedLinuxRuntimeProcess may be called only while the adapter still
// owns the direct child and has never started Wait. That ownership is the
// safety boundary that keeps a zombie PID/PGID allocated while this function
// sends a negative-PGID signal. Restart recovery must never call this helper.
func terminateOwnedLinuxRuntimeProcess(ctx context.Context, ids runtimeProcessIDs,
	system linuxRuntimeProcessSystem,
) (runtimeProcessPlatformProof, error) {
	if system == nil || ctx == nil {
		return runtimeProcessPlatformProof{}, errors.New("Linux process inspector is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return runtimeProcessPlatformProof{}, err
	}
	if err := validateRuntimeProcessIDs(ids, "linux"); err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: invalid Linux owned process IDs: %v",
			ErrRuntimeProcessUncertain, err)
	}
	bootID, err := system.BootID()
	if err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("read Linux boot ID: %w", err)
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
	case errors.Is(err, errLinuxRuntimeProcessNotExist):
		return linuxRuntimeProcessAbsentProof(ids, system)
	case err != nil:
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: inspect owned Linux leader: %v",
			ErrRuntimeProcessUncertain, err)
	case !sameLinuxRuntimeProcess(ids, snapshot):
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: retained Linux leader identity changed",
			ErrRuntimeProcessUncertain)
	}
	return terminateOwnedLinuxRuntimeGroup(ctx, ids, system)
}

func terminateOwnedLinuxRuntimeGroup(ctx context.Context, ids runtimeProcessIDs,
	system linuxRuntimeProcessSystem,
) (proof runtimeProcessPlatformProof, terminationErr error) {
	// The caller owns the unreaped direct child, so its numeric PID/PGID cannot
	// be recycled whether the leader is live, stopped, or a zombie. This is the
	// sole Linux negative-PGID mutation path. SIGSTOP is a fork barrier: after
	// every non-zombie member is observed stopped, no descendant can create a
	// fresh member behind the final SIGKILL/proc scan.
	err := system.SignalGroup(ids.PGID, syscall.SIGSTOP)
	if errors.Is(err, unix.ESRCH) {
		snapshot, snapshotErr := system.Snapshot(ids.PID)
		if snapshotErr == nil && sameLinuxRuntimeProcess(ids, snapshot) &&
			linuxRuntimeProcessExited(snapshot.state) {
			return runtimeProcessPlatformProof{state: runtimeProcessExactExited,
				method: "exact_process_exited"}, nil
		}
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: owned Linux group vanished without an exact zombie",
			ErrRuntimeProcessUncertain)
	}
	if err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: stop owned Linux group: %v",
			ErrRuntimeProcessUncertain, err)
	}
	resumeRequired := true
	defer func() {
		if !resumeRequired || terminationErr == nil {
			return
		}
		if err := system.SignalGroup(ids.PGID, syscall.SIGCONT); err != nil &&
			!errors.Is(err, unix.ESRCH) {
			terminationErr = errors.Join(terminationErr, fmt.Errorf(
				"%w: resume owned Linux group: %v", ErrRuntimeProcessUncertain, err))
		}
	}()
	if err := waitOwnedLinuxRuntimeProcessAnchor(ctx, ids, system); err != nil {
		return runtimeProcessPlatformProof{}, err
	}
	if err := stopLinuxRuntimeProcessGroup(ctx, ids.PGID, system); err != nil {
		return runtimeProcessPlatformProof{}, err
	}
	if err := system.SignalGroup(ids.PGID, syscall.SIGKILL); err != nil &&
		!errors.Is(err, unix.ESRCH) {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: kill owned Linux group: %v",
			ErrRuntimeProcessUncertain, err)
	}
	resumeRequired = false
	if err := waitLinuxRuntimeProcessGroupExit(ctx, ids.PGID, system); err != nil {
		return runtimeProcessPlatformProof{}, err
	}
	return runtimeProcessPlatformProof{state: runtimeProcessExactExited,
		method: "linux_owned_group_stop_kill", signals: []string{"SIGSTOP", "SIGKILL"}}, nil
}

func stopLinuxRuntimeProcessGroup(ctx context.Context, pgid int,
	system linuxRuntimeProcessSystem,
) error {
	bounded, cancel := context.WithTimeout(ctx, linuxRuntimeProcessStopWait)
	defer cancel()
	ticker := time.NewTicker(linuxRuntimeProcessPoll)
	defer ticker.Stop()
	stableStopped := 0
	for {
		if err := system.SignalGroup(pgid, syscall.SIGSTOP); err != nil &&
			!errors.Is(err, unix.ESRCH) {
			return fmt.Errorf("%w: stop anchored Linux process group: %v",
				ErrRuntimeProcessUncertain, err)
		}
		unstopped, err := system.GroupHasUnstoppedMembers(bounded, pgid)
		if err != nil {
			return fmt.Errorf("%w: inspect stopped Linux process group: %v",
				ErrRuntimeProcessUncertain, err)
		}
		if unstopped {
			stableStopped = 0
		} else {
			stableStopped++
		}
		if stableStopped == 2 {
			return nil
		}
		select {
		case <-bounded.Done():
			return fmt.Errorf("%w: Linux process group did not reach a stop barrier: %v",
				ErrRuntimeProcessUncertain, bounded.Err())
		case <-ticker.C:
		}
	}
}

func waitOwnedLinuxRuntimeProcessAnchor(ctx context.Context, ids runtimeProcessIDs,
	system linuxRuntimeProcessSystem,
) error {
	bounded, cancel := context.WithTimeout(ctx, linuxRuntimeProcessStopWait)
	defer cancel()
	ticker := time.NewTicker(linuxRuntimeProcessPoll)
	defer ticker.Stop()
	for {
		snapshot, err := system.Snapshot(ids.PID)
		if err != nil || !sameLinuxRuntimeProcess(ids, snapshot) {
			return fmt.Errorf("%w: exact Linux leader changed before its owned stop anchor",
				ErrRuntimeProcessUncertain)
		}
		if linuxRuntimeProcessExited(snapshot.state) || snapshot.state == 'T' || snapshot.state == 't' {
			return nil
		}
		select {
		case <-bounded.Done():
			return fmt.Errorf("%w: wait for exact Linux stop anchor: %v",
				ErrRuntimeProcessUncertain, bounded.Err())
		case <-ticker.C:
		}
	}
}

func waitLinuxRuntimeProcessGroupExit(ctx context.Context, pgid int,
	system linuxRuntimeProcessSystem,
) error {
	bounded, cancel := context.WithTimeout(ctx, linuxRuntimeProcessExitWait)
	defer cancel()
	ticker := time.NewTicker(linuxRuntimeProcessPoll)
	defer ticker.Stop()
	stableAbsence := 0
	for {
		live, err := system.GroupHasLiveMembers(bounded, pgid)
		if err != nil {
			return fmt.Errorf("%w: inspect killed Linux group: %v",
				ErrRuntimeProcessUncertain, err)
		}
		if live {
			stableAbsence = 0
		} else {
			stableAbsence++
		}
		// One full /proc traversal can race a member created behind its
		// directory cursor. A second independent traversal can return absent
		// only after no previously observed live member remains able to fork.
		if stableAbsence == 2 {
			return nil
		}
		select {
		case <-bounded.Done():
			return fmt.Errorf("%w: killed Linux group retained live members: %v",
				ErrRuntimeProcessUncertain, bounded.Err())
		case <-ticker.C:
		}
	}
}

func linuxRuntimeProcessAbsentProof(ids runtimeProcessIDs,
	system linuxRuntimeProcessSystem,
) (runtimeProcessPlatformProof, error) {
	exists, err := system.GroupExists(ids.PGID)
	if err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: inspect absent Linux group: %v",
			ErrRuntimeProcessUncertain, err)
	}
	if exists {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: leader is absent while its process group exists",
			ErrRuntimeProcessUncertain)
	}
	return runtimeProcessPlatformProof{state: runtimeProcessGone,
		method: "process_and_group_absent"}, nil
}

func linuxRuntimeProcessReusedProof(ids runtimeProcessIDs,
	system linuxRuntimeProcessSystem,
) (runtimeProcessPlatformProof, error) {
	exists, err := system.GroupExists(ids.PGID)
	if err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: inspect reused Linux group: %v",
			ErrRuntimeProcessUncertain, err)
	}
	if exists {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: PID was reused while the durable group exists",
			ErrRuntimeProcessUncertain)
	}
	return runtimeProcessPlatformProof{state: runtimeProcessReused,
		method: "pid_reused_group_absent"}, nil
}

func linuxRuntimeProcessExitedProof(ids runtimeProcessIDs,
	system linuxRuntimeProcessSystem,
) (runtimeProcessPlatformProof, error) {
	// Restart recovery has no Wait ownership over a zombie and therefore may
	// not signal its numeric PGID. kill(-PGID, 0) is the kernel's atomic group
	// liveness query and avoids treating two racy /proc traversals as proof.
	exists, err := system.GroupExists(ids.PGID)
	if err != nil {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: inspect exited Linux group: %v",
			ErrRuntimeProcessUncertain, err)
	}
	if exists {
		return runtimeProcessPlatformProof{}, fmt.Errorf("%w: exact zombie retains a signalable process group",
			ErrRuntimeProcessUncertain)
	}
	return runtimeProcessPlatformProof{state: runtimeProcessExactExited,
		method: "exact_process_exited"}, nil
}

func sameLinuxRuntimeProcess(ids runtimeProcessIDs, snapshot linuxRuntimeProcessSnapshot) bool {
	return snapshot.pid == ids.PID && snapshot.pgid == ids.PGID && snapshot.sid == ids.SID &&
		snapshot.uid == ids.UID && snapshot.startToken == ids.StartToken
}

func linuxRuntimeProcessExited(state byte) bool {
	return state == 'Z' || state == 'X' || state == 'x'
}
