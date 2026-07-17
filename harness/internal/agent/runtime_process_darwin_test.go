//go:build darwin

package agent

import (
	"context"
	"errors"
	"os/exec"
	"syscall"
	"testing"
	"time"
)

type fakeDarwinRuntimeProcess struct {
	bootID          string
	uid             uint32
	snapshot        darwinRuntimeProcessSnapshot
	snapshotErr     error
	snapshots       []darwinRuntimeProcessSnapshot
	snapshotErrors  []error
	snapshotCalls   int
	groupExists     bool
	groupErr        error
	groupCalls      int
	liveMembers     bool
	liveErr         error
	liveResults     []bool
	liveErrors      []error
	liveCalls       int
	unstopped       bool
	unstoppedErr    error
	unstoppedResult []bool
	unstoppedErrors []error
	unstoppedCalls  int
	signalErrors    []error
	groupSignals    []syscall.Signal
}

func (system *fakeDarwinRuntimeProcess) BootID() (string, error) { return system.bootID, nil }
func (system *fakeDarwinRuntimeProcess) EffectiveUID() uint32    { return system.uid }
func (system *fakeDarwinRuntimeProcess) Snapshot(int) (darwinRuntimeProcessSnapshot, error) {
	index := system.snapshotCalls
	system.snapshotCalls++
	if index < len(system.snapshotErrors) && system.snapshotErrors[index] != nil {
		return darwinRuntimeProcessSnapshot{}, system.snapshotErrors[index]
	}
	if len(system.snapshots) != 0 {
		if index >= len(system.snapshots) {
			index = len(system.snapshots) - 1
		}
		return system.snapshots[index], nil
	}
	return system.snapshot, system.snapshotErr
}
func (system *fakeDarwinRuntimeProcess) GroupExists(int) (bool, error) {
	system.groupCalls++
	return system.groupExists, system.groupErr
}
func (system *fakeDarwinRuntimeProcess) GroupHasLiveMembers(context.Context, int) (bool, error) {
	index := system.liveCalls
	system.liveCalls++
	if index < len(system.liveErrors) && system.liveErrors[index] != nil {
		return false, system.liveErrors[index]
	}
	if len(system.liveResults) != 0 {
		if index >= len(system.liveResults) {
			index = len(system.liveResults) - 1
		}
		return system.liveResults[index], nil
	}
	return system.liveMembers, system.liveErr
}
func (system *fakeDarwinRuntimeProcess) GroupHasUnstoppedMembers(context.Context, int) (bool, error) {
	index := system.unstoppedCalls
	system.unstoppedCalls++
	if index < len(system.unstoppedErrors) && system.unstoppedErrors[index] != nil {
		return false, system.unstoppedErrors[index]
	}
	if len(system.unstoppedResult) != 0 {
		if index >= len(system.unstoppedResult) {
			index = len(system.unstoppedResult) - 1
		}
		return system.unstoppedResult[index], nil
	}
	return system.unstopped, system.unstoppedErr
}
func (system *fakeDarwinRuntimeProcess) SignalGroup(_ int, signal syscall.Signal) error {
	index := len(system.groupSignals)
	system.groupSignals = append(system.groupSignals, signal)
	if index < len(system.signalErrors) {
		return system.signalErrors[index]
	}
	return nil
}

func TestDarwinRuntimeProcessSupportProbe(t *testing.T) {
	const boot = "123e4567-e89b-12d3-a456-426614174000"
	self := darwinRuntimeProcessSnapshot{pid: 77, pgid: 7, sid: 7, uid: 501,
		startToken: "darwin:" + boot + ":10:20", state: 2}
	system := &fakeDarwinRuntimeProcess{bootID: boot, uid: 501, snapshot: self,
		groupExists: true, liveMembers: true}
	if err := checkDarwinRuntimeProcessSupport(77, system); err != nil {
		t.Fatal(err)
	}
	if system.groupCalls != 1 || system.liveCalls != 1 {
		t.Fatalf("support probe group/live calls = %d/%d", system.groupCalls, system.liveCalls)
	}

	bad := self
	bad.startToken = "darwin:223e4567-e89b-12d3-a456-426614174000:10:20"
	system = &fakeDarwinRuntimeProcess{bootID: boot, uid: 501, snapshot: bad}
	if err := checkDarwinRuntimeProcessSupport(77, system); err == nil {
		t.Fatal("support probe accepted inconsistent boot identity")
	}

	system = &fakeDarwinRuntimeProcess{bootID: boot, uid: 501, snapshot: self,
		groupErr: syscall.EPERM}
	if err := checkDarwinRuntimeProcessSupport(77, system); err == nil {
		t.Fatal("support probe accepted unavailable group observation")
	}
}

func TestDarwinRuntimeProcessSystemSupport(t *testing.T) {
	if err := checkSystemRuntimeProcessSupport(); err != nil {
		t.Fatal(err)
	}
}

func TestDarwinRuntimeProcessRecoveryIsObservationOnly(t *testing.T) {
	ids := runtimeProcessIDs{SchemaVersion: 1, OS: "darwin", PID: 42, PGID: 42, SID: 42,
		UID: 501, StartToken: "darwin:123e4567-e89b-12d3-a456-426614174000:10:20"}
	exact := darwinRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
		startToken: ids.StartToken, state: 2}
	tests := []struct {
		name       string
		system     fakeDarwinRuntimeProcess
		wantState  runtimeProcessState
		wantMethod string
		wantErr    error
		wantGroups int
		wantLive   int
	}{
		{name: "boot changed", system: fakeDarwinRuntimeProcess{
			bootID: "223e4567-e89b-12d3-a456-426614174000", uid: 501},
			wantState: runtimeProcessGone, wantMethod: "boot_session_changed"},
		{name: "absent", system: fakeDarwinRuntimeProcess{
			bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshotErr: errDarwinRuntimeProcessNotExist},
			wantState: runtimeProcessGone, wantMethod: "process_and_group_absent", wantGroups: 1},
		{name: "reused", system: fakeDarwinRuntimeProcess{
			bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshot: darwinRuntimeProcessSnapshot{pid: 42, pgid: 99, sid: 99, uid: 501,
				startToken: "darwin:123e4567-e89b-12d3-a456-426614174000:11:0"}},
			wantState: runtimeProcessReused, wantMethod: "pid_reused_group_absent", wantGroups: 1},
		{name: "exact live blocks", system: fakeDarwinRuntimeProcess{
			bootID: runtimeProcessTokenBootID(ids), uid: 501, snapshot: exact},
			wantErr: ErrRuntimeProcessLive},
		{name: "exact zombie has no signalable group", system: fakeDarwinRuntimeProcess{
			bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshot: darwinRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
				startToken: ids.StartToken, state: darwinRuntimeProcessZombie}},
			wantState: runtimeProcessExactExited, wantMethod: "exact_process_exited", wantLive: 2},
		{name: "absent leader with group blocks", system: fakeDarwinRuntimeProcess{
			bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshotErr: errDarwinRuntimeProcessNotExist, groupExists: true},
			wantErr: ErrRuntimeProcessUncertain, wantGroups: 1},
		{name: "reused PID with group blocks", system: fakeDarwinRuntimeProcess{
			bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshot: darwinRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
				startToken: "darwin:123e4567-e89b-12d3-a456-426614174000:11:0"}, groupExists: true},
			wantErr: ErrRuntimeProcessUncertain, wantGroups: 1},
		{name: "UID drift blocks", system: fakeDarwinRuntimeProcess{
			bootID: runtimeProcessTokenBootID(ids), uid: 502, snapshot: exact},
			wantErr: ErrRuntimeProcessUncertain},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			system := test.system
			proof, err := recoverDarwinRuntimeProcess(context.Background(), ids, &system)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("recovery error = %v, want %v", err, test.wantErr)
				}
			} else if err != nil || proof.state != test.wantState || proof.method != test.wantMethod ||
				len(proof.signals) != 0 {
				t.Fatalf("recovery = (%#v, %v)", proof, err)
			}
			if system.groupCalls != test.wantGroups {
				t.Fatalf("group probes = %d, want %d", system.groupCalls, test.wantGroups)
			}
			if system.liveCalls != test.wantLive {
				t.Fatalf("live probes = %d, want %d", system.liveCalls, test.wantLive)
			}
			if len(system.groupSignals) != 0 {
				t.Fatalf("restart recovery sent group signals %#v", system.groupSignals)
			}
		})
	}
}

func TestOwnedDarwinRuntimeProcessObservationIsImmediateAndSignalFree(t *testing.T) {
	ids := runtimeProcessIDs{SchemaVersion: 1, OS: "darwin", PID: 42, PGID: 42, SID: 42,
		UID: 501, StartToken: "darwin:123e4567-e89b-12d3-a456-426614174000:10:20"}
	exact := darwinRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
		startToken: ids.StartToken, state: 2}
	tests := []struct {
		name       string
		system     fakeDarwinRuntimeProcess
		wantState  runtimeProcessState
		wantMethod string
		wantErr    error
	}{
		{name: "absent", system: fakeDarwinRuntimeProcess{
			bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshotErr: errDarwinRuntimeProcessNotExist},
			wantState: runtimeProcessGone, wantMethod: "process_and_group_absent"},
		{name: "reused", system: fakeDarwinRuntimeProcess{
			bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshot: darwinRuntimeProcessSnapshot{pid: 42, pgid: 99, sid: 99, uid: 501,
				startToken: "darwin:123e4567-e89b-12d3-a456-426614174000:11:0"}},
			wantState: runtimeProcessReused, wantMethod: "pid_reused_group_absent"},
		{name: "exact live", system: fakeDarwinRuntimeProcess{
			bootID: runtimeProcessTokenBootID(ids), uid: 501, snapshot: exact},
			wantErr: ErrRuntimeProcessLive},
		{name: "exact zombie remains owned", system: fakeDarwinRuntimeProcess{
			bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshot: darwinRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
				startToken: ids.StartToken, state: darwinRuntimeProcessZombie}},
			wantErr: ErrRuntimeProcessLive},
		{name: "UID drift", system: fakeDarwinRuntimeProcess{
			bootID: runtimeProcessTokenBootID(ids), uid: 502, snapshot: exact},
			wantErr: ErrRuntimeProcessUncertain},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			system := test.system
			proof, err := observeOwnedDarwinRuntimeProcess(context.Background(), ids, &system)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("owned observation error = %v, want %v", err, test.wantErr)
				}
			} else if err != nil || proof.state != test.wantState ||
				proof.method != test.wantMethod || len(proof.signals) != 0 {
				t.Fatalf("owned observation = (%#v, %v)", proof, err)
			}
			if system.liveCalls != 0 || system.unstoppedCalls != 0 || len(system.groupSignals) != 0 {
				t.Fatalf("owned observation performed non-immediate group work: live=%d unstopped=%d signals=%#v",
					system.liveCalls, system.unstoppedCalls, system.groupSignals)
			}
		})
	}
}

func TestOwnedDarwinRuntimeProcessTerminationStopsThenKillsGroup(t *testing.T) {
	ids := runtimeProcessIDs{SchemaVersion: 1, OS: "darwin", PID: 42, PGID: 42, SID: 42,
		UID: 501, StartToken: "darwin:123e4567-e89b-12d3-a456-426614174000:10:20"}
	live := darwinRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
		startToken: ids.StartToken, state: 2}
	stopped := live
	stopped.state = darwinRuntimeProcessStopped
	zombie := live
	zombie.state = darwinRuntimeProcessZombie
	system := &fakeDarwinRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
		snapshots:       []darwinRuntimeProcessSnapshot{live, stopped, zombie},
		unstoppedResult: []bool{true, false, false}, liveResults: []bool{false, false}}
	proof, err := terminateOwnedDarwinRuntimeProcess(context.Background(), ids, system)
	if err != nil || proof.state != runtimeProcessExactExited ||
		proof.method != "darwin_owned_group_kill" ||
		len(proof.signals) != 2 || proof.signals[0] != "SIGSTOP" || proof.signals[1] != "SIGKILL" {
		t.Fatalf("owned termination = (%#v, %v)", proof, err)
	}
	if len(system.groupSignals) != 3 || system.groupSignals[0] != syscall.SIGSTOP ||
		system.groupSignals[1] != syscall.SIGSTOP || system.groupSignals[2] != syscall.SIGKILL {
		t.Fatalf("owned group signals = %#v", system.groupSignals)
	}
	if system.unstoppedCalls != 3 || system.liveCalls != 2 {
		t.Fatalf("owned group proof calls = unstopped %d, live %d",
			system.unstoppedCalls, system.liveCalls)
	}
}

func TestOwnedDarwinRuntimeProcessTerminationHandlesExactZombie(t *testing.T) {
	ids := runtimeProcessIDs{SchemaVersion: 1, OS: "darwin", PID: 42, PGID: 42, SID: 42,
		UID: 501, StartToken: "darwin:123e4567-e89b-12d3-a456-426614174000:10:20"}
	zombie := darwinRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
		startToken: ids.StartToken, state: darwinRuntimeProcessZombie}

	t.Run("without live descendants", func(t *testing.T) {
		system := &fakeDarwinRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshot: zombie}
		proof, err := terminateOwnedDarwinRuntimeProcess(context.Background(), ids, system)
		if err != nil || proof.state != runtimeProcessExactExited ||
			proof.method != "exact_process_exited" || len(proof.signals) != 0 {
			t.Fatalf("zombie termination = (%#v, %v)", proof, err)
		}
		if system.liveCalls != 1 || len(system.groupSignals) != 0 {
			t.Fatalf("zombie probes/signals = %d/%#v", system.liveCalls, system.groupSignals)
		}
	})

	t.Run("with live descendants", func(t *testing.T) {
		system := &fakeDarwinRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshots:   []darwinRuntimeProcessSnapshot{zombie, zombie, zombie},
			liveResults: []bool{true, false, false}, unstoppedResult: []bool{false, false}}
		proof, err := terminateOwnedDarwinRuntimeProcess(context.Background(), ids, system)
		if err != nil || proof.method != "darwin_owned_group_kill" {
			t.Fatalf("zombie descendant termination = (%#v, %v)", proof, err)
		}
		if len(system.groupSignals) != 2 || system.groupSignals[0] != syscall.SIGSTOP ||
			system.groupSignals[1] != syscall.SIGKILL {
			t.Fatalf("zombie descendant signals = %#v", system.groupSignals)
		}
	})
}

func TestOwnedDarwinRuntimeProcessSignalESRCHRequiresExactEmptyZombie(t *testing.T) {
	ids := runtimeProcessIDs{SchemaVersion: 1, OS: "darwin", PID: 42, PGID: 42, SID: 42,
		UID: 501, StartToken: "darwin:123e4567-e89b-12d3-a456-426614174000:10:20"}
	live := darwinRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
		startToken: ids.StartToken, state: 2}
	stopped := live
	stopped.state = darwinRuntimeProcessStopped
	zombie := live
	zombie.state = darwinRuntimeProcessZombie

	t.Run("SIGSTOP ESRCH accepted after zombie proof", func(t *testing.T) {
		system := &fakeDarwinRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshots:   []darwinRuntimeProcessSnapshot{live, zombie, zombie},
			liveResults: []bool{false, false}, signalErrors: []error{syscall.ESRCH}}
		proof, err := terminateOwnedDarwinRuntimeProcess(context.Background(), ids, system)
		if err != nil || proof.method != "exact_process_exited" || len(proof.signals) != 0 {
			t.Fatalf("SIGSTOP ESRCH proof = (%#v, %v)", proof, err)
		}
		if len(system.groupSignals) != 1 || system.groupSignals[0] != syscall.SIGSTOP {
			t.Fatalf("SIGSTOP ESRCH signals = %#v", system.groupSignals)
		}
	})

	t.Run("SIGSTOP ESRCH rejects a non-zombie leader", func(t *testing.T) {
		system := &fakeDarwinRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshots:    []darwinRuntimeProcessSnapshot{live, live},
			signalErrors: []error{syscall.ESRCH}}
		if _, err := terminateOwnedDarwinRuntimeProcess(context.Background(), ids, system); !errors.Is(err, ErrRuntimeProcessUncertain) {
			t.Fatalf("SIGSTOP ESRCH non-zombie error = %v", err)
		}
	})

	t.Run("SIGSTOP ESRCH rejects live zombie descendants", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		system := &fakeDarwinRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshots: []darwinRuntimeProcessSnapshot{live, zombie}, liveMembers: true,
			signalErrors: []error{syscall.ESRCH}}
		if _, err := terminateOwnedDarwinRuntimeProcess(ctx, ids, system); !errors.Is(err, ErrRuntimeProcessUncertain) {
			t.Fatalf("SIGSTOP ESRCH live-descendant error = %v", err)
		}
	})

	t.Run("refresh SIGSTOP ESRCH rechecks live zombie descendants", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		system := &fakeDarwinRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshots: []darwinRuntimeProcessSnapshot{live, zombie}, liveMembers: true,
			unstoppedResult: []bool{true}, signalErrors: []error{nil, syscall.ESRCH}}
		if _, err := terminateOwnedDarwinRuntimeProcess(ctx, ids, system); !errors.Is(err, ErrRuntimeProcessUncertain) {
			t.Fatalf("refresh SIGSTOP ESRCH live-descendant error = %v", err)
		}
		if len(system.groupSignals) != 3 || system.groupSignals[0] != syscall.SIGSTOP ||
			system.groupSignals[1] != syscall.SIGSTOP || system.groupSignals[2] != syscall.SIGCONT {
			t.Fatalf("refresh SIGSTOP ESRCH signals = %#v", system.groupSignals)
		}
	})

	t.Run("SIGKILL ESRCH closes after zombie proof", func(t *testing.T) {
		system := &fakeDarwinRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshots:       []darwinRuntimeProcessSnapshot{live, stopped, zombie, zombie},
			unstoppedResult: []bool{false, false}, liveResults: []bool{false, false},
			signalErrors: []error{nil, syscall.ESRCH}}
		proof, err := terminateOwnedDarwinRuntimeProcess(context.Background(), ids, system)
		if err != nil || proof.method != "darwin_owned_group_kill" ||
			len(proof.signals) != 2 || proof.signals[0] != "SIGSTOP" ||
			proof.signals[1] != "SIGKILL" {
			t.Fatalf("SIGKILL ESRCH proof = (%#v, %v)", proof, err)
		}
	})

	t.Run("SIGKILL ESRCH rejects a non-zombie leader", func(t *testing.T) {
		system := &fakeDarwinRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshots:       []darwinRuntimeProcessSnapshot{live, stopped, stopped},
			unstoppedResult: []bool{false, false}, signalErrors: []error{nil, syscall.ESRCH}}
		if _, err := terminateOwnedDarwinRuntimeProcess(context.Background(), ids, system); !errors.Is(err, ErrRuntimeProcessUncertain) {
			t.Fatalf("SIGKILL ESRCH non-zombie error = %v", err)
		}
	})
}

func TestOwnedDarwinRuntimeProcessNeverKillsBeforeGroupStopProof(t *testing.T) {
	ids := runtimeProcessIDs{SchemaVersion: 1, OS: "darwin", PID: 42, PGID: 42, SID: 42,
		UID: 501, StartToken: "darwin:123e4567-e89b-12d3-a456-426614174000:10:20"}
	live := darwinRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
		startToken: ids.StartToken, state: 2}
	system := &fakeDarwinRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
		snapshot: live, unstoppedErr: errors.New("process table unavailable")}
	if _, err := terminateOwnedDarwinRuntimeProcess(context.Background(), ids, system); !errors.Is(err, ErrRuntimeProcessUncertain) {
		t.Fatalf("unstopped scan error = %v", err)
	}
	if len(system.groupSignals) != 2 || system.groupSignals[0] != syscall.SIGSTOP ||
		system.groupSignals[1] != syscall.SIGCONT {
		t.Fatalf("signals before stop proof = %#v", system.groupSignals)
	}
}

func TestDarwinRuntimeProcessCaptureRequiresSetsidChild(t *testing.T) {
	command := exec.Command("/bin/sh", "-c", "exec /bin/sleep 30")
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		_ = command.Wait()
	}()
	ids, encoded, err := captureRuntimeProcessIDs(command.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	if ids.PID != command.Process.Pid || ids.PGID != ids.PID || ids.SID != ids.PID ||
		ids.OS != "darwin" || encoded.IsZero() {
		t.Fatalf("captured Darwin identity = %#v, JSON=%s", ids, encoded.String())
	}
	if parsed, err := parseRuntimeProcessIDs(encoded); err != nil || parsed != ids {
		t.Fatalf("captured identity parse = (%#v, %v)", parsed, err)
	}
}

func TestDarwinRuntimeProcessZombieNeverAuthorizesGroupSignal(t *testing.T) {
	ids := runtimeProcessIDs{SchemaVersion: 1, OS: "darwin", PID: 42, PGID: 42, SID: 42,
		UID: 501, StartToken: "darwin:123e4567-e89b-12d3-a456-426614174000:10:20"}
	system := &fakeDarwinRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
		snapshot: darwinRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
			startToken: ids.StartToken, state: darwinRuntimeProcessZombie}, groupExists: true,
		liveErr: errors.New("descendant observation unavailable")}
	if _, err := recoverDarwinRuntimeProcess(context.Background(), ids, system); !errors.Is(err, ErrRuntimeProcessUncertain) {
		t.Fatalf("zombie group recovery error = %v", err)
	}
}
