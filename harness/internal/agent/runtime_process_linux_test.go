//go:build linux

package agent

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

var errFakeLinuxRuntimeProcessSnapshotExhausted = errors.New("unexpected Linux snapshot call")

type fakeLinuxRuntimeProcess struct {
	bootID          string
	uid             uint32
	snapshots       []linuxRuntimeProcessSnapshot
	snapshotErrs    []error
	groupExists     bool
	groupErr        error
	liveMembers     []bool
	liveErr         error
	unstopped       []bool
	unstoppedErr    error
	groupSignals    []syscall.Signal
	groupSigErr     error
	groupSignalHook func(syscall.Signal)
}

func (system *fakeLinuxRuntimeProcess) BootID() (string, error) { return system.bootID, nil }
func (system *fakeLinuxRuntimeProcess) EffectiveUID() uint32    { return system.uid }
func (system *fakeLinuxRuntimeProcess) Snapshot(int) (linuxRuntimeProcessSnapshot, error) {
	if len(system.snapshots) == 0 && len(system.snapshotErrs) == 0 {
		return linuxRuntimeProcessSnapshot{}, errFakeLinuxRuntimeProcessSnapshotExhausted
	}
	var snapshot linuxRuntimeProcessSnapshot
	var err error
	if len(system.snapshots) != 0 {
		snapshot, system.snapshots = system.snapshots[0], system.snapshots[1:]
	}
	if len(system.snapshotErrs) != 0 {
		err, system.snapshotErrs = system.snapshotErrs[0], system.snapshotErrs[1:]
	}
	return snapshot, err
}
func (system *fakeLinuxRuntimeProcess) GroupExists(int) (bool, error) {
	return system.groupExists, system.groupErr
}
func (system *fakeLinuxRuntimeProcess) GroupHasLiveMembers(context.Context, int) (bool, error) {
	if system.liveErr != nil {
		return false, system.liveErr
	}
	if len(system.liveMembers) == 0 {
		return false, nil
	}
	value := system.liveMembers[0]
	system.liveMembers = system.liveMembers[1:]
	return value, nil
}
func (system *fakeLinuxRuntimeProcess) GroupHasUnstoppedMembers(context.Context, int) (bool, error) {
	if system.unstoppedErr != nil {
		return false, system.unstoppedErr
	}
	if len(system.unstopped) == 0 {
		return false, nil
	}
	value := system.unstopped[0]
	system.unstopped = system.unstopped[1:]
	return value, nil
}
func (system *fakeLinuxRuntimeProcess) SignalGroup(_ int, signal syscall.Signal) error {
	system.groupSignals = append(system.groupSignals, signal)
	if system.groupSignalHook != nil {
		system.groupSignalHook(signal)
	}
	return system.groupSigErr
}

func TestLinuxRuntimeProcessSupportProbe(t *testing.T) {
	const boot = "123e4567-e89b-12d3-a456-426614174000"
	self := linuxRuntimeProcessSnapshot{pid: 77, pgid: 7, sid: 7, uid: 501,
		startToken: "linux:" + boot + ":10", state: 'S'}
	system := &fakeLinuxRuntimeProcess{bootID: boot, uid: 501,
		snapshots: []linuxRuntimeProcessSnapshot{self}, groupExists: true,
		liveMembers: []bool{true}}
	if err := checkLinuxRuntimeProcessSupport(77, system); err != nil {
		t.Fatal(err)
	}
	if len(system.groupSignals) != 1 || system.groupSignals[0] != 0 {
		t.Fatalf("support probe group signals = %#v", system.groupSignals)
	}

	bad := self
	bad.startToken = "linux:223e4567-e89b-12d3-a456-426614174000:10"
	system = &fakeLinuxRuntimeProcess{bootID: boot, uid: 501,
		snapshots: []linuxRuntimeProcessSnapshot{bad}}
	if err := checkLinuxRuntimeProcessSupport(77, system); err == nil {
		t.Fatal("support probe accepted inconsistent boot identity")
	}

	system = &fakeLinuxRuntimeProcess{bootID: boot, uid: 501,
		snapshots: []linuxRuntimeProcessSnapshot{self}, groupExists: true,
		liveMembers: []bool{true}, groupSigErr: unix.EPERM}
	if err := checkLinuxRuntimeProcessSupport(77, system); err == nil {
		t.Fatal("support probe accepted blocked group signaling")
	}
}

func TestLinuxRuntimeProcessCaptureBindsStableExactChild(t *testing.T) {
	ids := linuxRuntimeProcessTestIDs()
	exact := linuxRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
		startToken: ids.StartToken, state: 'S'}
	reused := exact
	reused.startToken = "linux:123e4567-e89b-12d3-a456-426614174000:11"

	system := &fakeLinuxRuntimeProcess{uid: 501,
		snapshots: []linuxRuntimeProcessSnapshot{exact, exact}}
	captured, err := captureLinuxRuntimeProcess(42, system)
	if err != nil || captured != ids {
		t.Fatalf("capture = (%#v, %v)", captured, err)
	}

	system = &fakeLinuxRuntimeProcess{uid: 501,
		snapshots: []linuxRuntimeProcessSnapshot{exact, reused}}
	if _, err := captureLinuxRuntimeProcess(42, system); err == nil {
		t.Fatal("capture accepted identity drift between observations")
	}
}

func TestLinuxRuntimeProcessRestartRecoveryIsObservationOnly(t *testing.T) {
	ids := linuxRuntimeProcessTestIDs()
	exact := linuxRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
		startToken: ids.StartToken, state: 'S'}
	system := &fakeLinuxRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
		snapshots: []linuxRuntimeProcessSnapshot{exact}}
	if _, err := recoverLinuxRuntimeProcess(context.Background(), ids, system); !errors.Is(err, ErrRuntimeProcessLive) {
		t.Fatalf("live restart recovery error = %v", err)
	}
	if len(system.groupSignals) != 0 {
		t.Fatalf("restart recovery sent group signals %#v", system.groupSignals)
	}
}

func TestLinuxRuntimeProcessRecoveryNeverSignalsUncertainGroup(t *testing.T) {
	ids := linuxRuntimeProcessTestIDs()
	exact := linuxRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
		startToken: ids.StartToken, state: 'S'}
	reused := exact
	reused.startToken = "linux:123e4567-e89b-12d3-a456-426614174000:11"
	tests := []struct {
		name   string
		system fakeLinuxRuntimeProcess
		want   error
	}{
		{"absent leader with group", fakeLinuxRuntimeProcess{bootID: runtimeProcessTokenBootID(ids),
			uid: 501, snapshotErrs: []error{errLinuxRuntimeProcessNotExist}, groupExists: true},
			ErrRuntimeProcessUncertain},
		{"reused PID with group", fakeLinuxRuntimeProcess{bootID: runtimeProcessTokenBootID(ids),
			uid: 501, snapshots: []linuxRuntimeProcessSnapshot{reused}, groupExists: true},
			ErrRuntimeProcessUncertain},
		{"UID drift", fakeLinuxRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 502},
			ErrRuntimeProcessUncertain},
		{"restart zombie with live group", fakeLinuxRuntimeProcess{
			bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshots: []linuxRuntimeProcessSnapshot{{pid: 42, pgid: 42, sid: 42, uid: 501,
				startToken: ids.StartToken, state: 'Z'}}, groupExists: true},
			ErrRuntimeProcessUncertain},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			system := test.system
			if _, err := recoverLinuxRuntimeProcess(context.Background(), ids, &system); !errors.Is(err, test.want) {
				t.Fatalf("recovery error = %v, want %v", err, test.want)
			}
			if len(system.groupSignals) != 0 {
				t.Fatalf("uncertain recovery sent group signals %#v", system.groupSignals)
			}
		})
	}
}

func TestOwnedLinuxRuntimeProcessStopsThenKillsGroup(t *testing.T) {
	ids := linuxRuntimeProcessTestIDs()
	exact := linuxRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
		startToken: ids.StartToken, state: 'S'}
	stopped := exact
	stopped.state = 'T'
	system := &fakeLinuxRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
		snapshots: []linuxRuntimeProcessSnapshot{exact, stopped}, liveMembers: []bool{false, false}}
	proof, err := terminateOwnedLinuxRuntimeProcess(context.Background(), ids, system)
	if err != nil || proof.state != runtimeProcessExactExited ||
		proof.method != "linux_owned_group_stop_kill" {
		t.Fatalf("owned termination = (%#v, %v)", proof, err)
	}
	want := []syscall.Signal{syscall.SIGSTOP, syscall.SIGSTOP, syscall.SIGSTOP, syscall.SIGKILL}
	if len(system.groupSignals) != len(want) {
		t.Fatalf("owned signals = %#v", system.groupSignals)
	}
	for index := range want {
		if system.groupSignals[index] != want[index] {
			t.Fatalf("owned signals = %#v", system.groupSignals)
		}
	}
}

func TestOwnedLinuxRuntimeProcessRejectsBroadcastSignalAuthority(t *testing.T) {
	ids := linuxRuntimeProcessTestIDs()
	ids.PID, ids.PGID, ids.SID = 1, 1, 1
	system := &fakeLinuxRuntimeProcess{}
	if _, err := terminateOwnedLinuxRuntimeProcess(context.Background(), ids, system); err == nil ||
		!errors.Is(err, ErrRuntimeProcessUncertain) {
		t.Fatalf("invalid owned authority error = %v", err)
	}
	if len(system.groupSignals) != 0 {
		t.Fatalf("invalid owned authority sent signals = %#v", system.groupSignals)
	}
	for _, pgid := range []int{-1, 0, 1} {
		if err := (systemLinuxRuntimeProcess{}).SignalGroup(pgid, syscall.SIGKILL); err == nil {
			t.Fatalf("SignalGroup(%d) accepted broadcast authority", pgid)
		}
	}
}

func TestOwnedLinuxRuntimeProcessZombieStopFailureResumesGroup(t *testing.T) {
	ids := linuxRuntimeProcessTestIDs()
	zombie := linuxRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
		startToken: ids.StartToken, state: 'Z'}
	system := &fakeLinuxRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
		snapshots:    []linuxRuntimeProcessSnapshot{zombie, zombie},
		unstoppedErr: errors.New("process table unavailable")}
	if _, err := terminateOwnedLinuxRuntimeProcess(context.Background(), ids, system); !errors.Is(err, ErrRuntimeProcessUncertain) {
		t.Fatalf("owned zombie stop error = %v", err)
	}
	if len(system.groupSignals) != 3 || system.groupSignals[0] != syscall.SIGSTOP ||
		system.groupSignals[1] != syscall.SIGSTOP || system.groupSignals[2] != syscall.SIGCONT {
		t.Fatalf("owned zombie failure signals = %#v", system.groupSignals)
	}
}

func TestOwnedLinuxRuntimeProcessZombieStopsAndKillsAnchoredGroup(t *testing.T) {
	ids := linuxRuntimeProcessTestIDs()
	zombie := linuxRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
		startToken: ids.StartToken, state: 'Z'}
	system := &fakeLinuxRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
		snapshots: []linuxRuntimeProcessSnapshot{zombie, zombie}, liveMembers: []bool{false, false}}
	proof, err := terminateOwnedLinuxRuntimeProcess(context.Background(), ids, system)
	if err != nil || proof.state != runtimeProcessExactExited ||
		proof.method != "linux_owned_group_stop_kill" || len(proof.signals) != 2 {
		t.Fatalf("owned zombie termination = (%#v, %v)", proof, err)
	}
	want := []syscall.Signal{syscall.SIGSTOP, syscall.SIGSTOP, syscall.SIGSTOP, syscall.SIGKILL}
	if len(system.groupSignals) != len(want) {
		t.Fatalf("owned zombie signals = %#v", system.groupSignals)
	}
	for index := range want {
		if system.groupSignals[index] != want[index] {
			t.Fatalf("owned zombie signals = %#v", system.groupSignals)
		}
	}
}

func TestOwnedLinuxRuntimeProcessCancellationAfterStopResumesGroup(t *testing.T) {
	ids := linuxRuntimeProcessTestIDs()
	exact := linuxRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
		startToken: ids.StartToken, state: 'S'}
	system := &fakeLinuxRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
		snapshots: []linuxRuntimeProcessSnapshot{exact, exact}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	system.groupSignalHook = func(signal syscall.Signal) {
		if signal == syscall.SIGSTOP {
			cancel()
		}
	}
	if _, err := terminateOwnedLinuxRuntimeProcess(ctx, ids, system); !errors.Is(err, ErrRuntimeProcessUncertain) {
		t.Fatalf("canceled stopped recovery error = %v", err)
	}
	if len(system.groupSignals) != 2 || system.groupSignals[0] != syscall.SIGSTOP ||
		system.groupSignals[1] != syscall.SIGCONT {
		t.Fatalf("canceled owned signals = %#v", system.groupSignals)
	}
}

func TestLinuxRuntimeProcessObservationOnlyProofs(t *testing.T) {
	ids := linuxRuntimeProcessTestIDs()
	reused := linuxRuntimeProcessSnapshot{pid: 42, pgid: 99, sid: 99, uid: 501,
		startToken: "linux:123e4567-e89b-12d3-a456-426614174000:11", state: 'S'}
	zombie := linuxRuntimeProcessSnapshot{pid: 42, pgid: 42, sid: 42, uid: 501,
		startToken: ids.StartToken, state: 'Z'}
	tests := []struct {
		name       string
		system     fakeLinuxRuntimeProcess
		wantState  runtimeProcessState
		wantMethod string
	}{
		{"boot changed", fakeLinuxRuntimeProcess{bootID: "223e4567-e89b-12d3-a456-426614174000", uid: 501},
			runtimeProcessGone, "boot_session_changed"},
		{"absent", fakeLinuxRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshotErrs: []error{errLinuxRuntimeProcessNotExist}},
			runtimeProcessGone, "process_and_group_absent"},
		{"reused", fakeLinuxRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshots: []linuxRuntimeProcessSnapshot{reused}},
			runtimeProcessReused, "pid_reused_group_absent"},
		{"zombie", fakeLinuxRuntimeProcess{bootID: runtimeProcessTokenBootID(ids), uid: 501,
			snapshots: []linuxRuntimeProcessSnapshot{zombie}, liveMembers: []bool{false}},
			runtimeProcessExactExited, "exact_process_exited"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			system := test.system
			proof, err := recoverLinuxRuntimeProcess(context.Background(), ids, &system)
			if err != nil || proof.state != test.wantState || proof.method != test.wantMethod ||
				len(system.groupSignals) != 0 {
				t.Fatalf("observation proof = (%#v, %v), signals=%#v", proof, err,
					system.groupSignals)
			}
		})
	}
}

func linuxRuntimeProcessTestIDs() runtimeProcessIDs {
	return runtimeProcessIDs{SchemaVersion: 1, OS: "linux", PID: 42, PGID: 42, SID: 42,
		UID: 501, StartToken: "linux:123e4567-e89b-12d3-a456-426614174000:10"}
}
