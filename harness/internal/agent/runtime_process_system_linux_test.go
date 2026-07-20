//go:build linux

package agent

import (
	"context"
	"errors"
	"os/exec"
	"syscall"
	"testing"
)

func TestLinuxRuntimeProcessSystemSupport(t *testing.T) {
	if err := checkSystemRuntimeProcessSupport(); err != nil {
		t.Fatal(err)
	}
}

func TestLinuxRuntimeProcessStatParserHandlesClosedComm(t *testing.T) {
	const boot = "123e4567-e89b-12d3-a456-426614174000"
	// Fields after comm begin at state (field 3); starttime is tail index 19.
	raw := []byte("42 (name with ) close) S 1 42 42 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 99 0\n")
	snapshot, err := parseLinuxRuntimeProcessStat(42, boot, raw)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.pgid != 42 || snapshot.sid != 42 || snapshot.state != 'S' ||
		snapshot.startToken != "linux:"+boot+":99" {
		t.Fatalf("parsed stat = %#v", snapshot)
	}
	for _, malformed := range [][]byte{
		[]byte("42 no-comm S 1"),
		[]byte("43 (wrong pid) S 1 42 42"),
		[]byte("42 (short) S 1 42 42"),
		[]byte("42 (bad start) S 1 42 42 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 nope"),
	} {
		if _, err := parseLinuxRuntimeProcessStat(42, boot, malformed); err == nil {
			t.Fatalf("malformed stat accepted: %q", malformed)
		}
	}
	// Kernel threads report pgid 0 and sid 0 on hosts with an unrestricted
	// /proc view; they must classify as foreign, not as a malformed scan.
	foreign := []byte("2 (kthreadd) S 0 0 0 0 -1 0 0 0 0 0 0 0 0 0 0 0 1 0 23 0\n")
	if _, err := parseLinuxRuntimeProcessStat(2, boot, foreign); !errors.Is(err, errLinuxRuntimeProcessForeignIdentity) {
		t.Fatalf("kernel-thread stat classification = %v", err)
	}
}

func TestLinuxRuntimeProcessCaptureAndKillIsolatedGroup(t *testing.T) {
	command := exec.Command("/bin/sh", "-c", "exec /bin/sleep 30")
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if waited {
			return
		}
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		_ = command.Wait()
	}()
	ids, encoded, err := captureRuntimeProcessIDs(command.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	if ids.PID != command.Process.Pid || ids.PGID != ids.PID || ids.SID != ids.PID ||
		ids.OS != "linux" || encoded.IsZero() {
		t.Fatalf("captured Linux identity = %#v, JSON=%s", ids, encoded.String())
	}
	proof, err := terminateOwnedLinuxRuntimeProcess(context.Background(), ids,
		systemLinuxRuntimeProcess{})
	if err != nil || proof.method != "linux_owned_group_stop_kill" {
		t.Fatalf("owned live group termination = (%#v, %v)", proof, err)
	}
	_ = command.Wait()
	waited = true
}
