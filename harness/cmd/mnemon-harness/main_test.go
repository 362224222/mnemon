package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	t.Parallel()

	runCommand := func(ctx context.Context, args ...string) (string, string, error) {
		t.Helper()
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		exit := run(ctx, args, strings.NewReader(""), &stdout, &stderr)
		if exit != 0 {
			return stdout.String(), stderr.String(), fmt.Errorf("exit %d", exit)
		}
		return stdout.String(), stderr.String(), nil
	}

	wantHelp, wantStderr, err := runCommand(context.Background())
	if err != nil {
		t.Fatalf("empty invocation: %v", err)
	}
	if wantStderr != "" {
		t.Fatalf("empty invocation stderr = %q", wantStderr)
	}
	if wantHelp != helpText {
		t.Fatalf("empty invocation help mismatch:\n%s", wantHelp)
	}

	for _, arg := range []string{"-h", "--help", "help"} {
		arg := arg
		t.Run("help_"+strings.TrimLeft(arg, "-"), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			stdout, stderr, err := runCommand(ctx, arg)
			if err != nil || stderr != "" || stdout != wantHelp {
				t.Fatalf("run(%q) = stdout %q, stderr %q, err %v", arg, stdout, stderr, err)
			}
		})
	}

	for _, arg := range []string{"--version", "version"} {
		stdout, stderr, err := runCommand(context.Background(), arg)
		if err != nil || stderr != "" || stdout != "mnemon-harness version dev\n" {
			t.Fatalf("run(%q) = stdout %q, stderr %q, err %v", arg, stdout, stderr, err)
		}
	}

	for _, command := range []string{
		"sync", "connect", "hub", "tower", "session", "config", "loop",
		"control", "local", "daemon", "multica", "acceptance",
	} {
		stdout, stderr, err := runCommand(context.Background(), command)
		want := fmt.Sprintf("mnemon-harness: unknown command %q\n", command)
		if stdout != "" || stderr != want || err == nil || err.Error() != "exit 2" {
			t.Errorf("run(%q) = stdout %q, stderr %q, err %v; want %q", command, stdout, stderr, err, want)
		}
	}

	lowerHelp := strings.ToLower(wantHelp)
	if !strings.Contains(wantHelp,
		"mnemon-harness setup [--host auto|codex] [--project-root DIR]") {
		t.Error("ordinary help does not expose the complete setup entrypoint")
	}
	if !strings.Contains(wantHelp,
		"mnemon-harness eject [--host auto|codex] [--project-root DIR]") {
		t.Error("ordinary help does not expose the complete eject entrypoint")
	}
	if !strings.Contains(wantHelp, "mnemon-harness status") {
		t.Error("ordinary help does not expose status")
	}
	if !strings.Contains(wantHelp, "mnemon-harness doctor") {
		t.Error("ordinary help does not expose doctor")
	}
	for _, forbidden := range []string{
		"hub", "remote workspace", "multica", "generic capability", "mcp",
		"evolution", "tower", "session", "hook check", "agent current", "teamwork offer",
		"channel abandon", "confirm-channel", "confirm-peer", "reset --force",
	} {
		if strings.Contains(lowerHelp, forbidden) {
			t.Errorf("help contains retired vocabulary %q", forbidden)
		}
	}
}

func TestRunRoutesDoctorThroughThePublicDiagnosticBoundary(t *testing.T) {
	var received []string
	doctor := func(ctx context.Context, args []string, stdout, stderr io.Writer,
		gotVersion string,
	) int {
		if ctx == nil || gotVersion != version {
			t.Fatalf("doctor composition = context %v version %q", ctx, gotVersion)
		}
		received = append([]string(nil), args...)
		_, _ = io.WriteString(stdout, "doctor receipt\n")
		return 3
	}
	var stdout, stderr bytes.Buffer
	exit := runWithCommandRunners(context.Background(), []string{"doctor"},
		strings.NewReader("unrelated stdin"), &stdout, &stderr,
		commandRunners{setup: cliSetupMustNotRun(t), eject: func(context.Context, []string,
			io.Writer, io.Writer, string,
		) int {
			t.Fatal("doctor route invoked eject")
			return 1
		}, status: func(context.Context, []string, io.Writer, io.Writer, string) int {
			t.Fatal("doctor route invoked status")
			return 1
		}, doctor: doctor})
	if exit != 3 || len(received) != 0 || stdout.String() != "doctor receipt\n" ||
		stderr.Len() != 0 {
		t.Fatalf("doctor route = exit %d args %v stdout %q stderr %q", exit, received,
			stdout.String(), stderr.String())
	}
}

func TestRunRoutesStatusThroughThePublicObservationBoundary(t *testing.T) {
	var received []string
	status := func(ctx context.Context, args []string, stdout, stderr io.Writer,
		gotVersion string,
	) int {
		if ctx == nil || gotVersion != version {
			t.Fatalf("status composition = context %v version %q", ctx, gotVersion)
		}
		received = append([]string(nil), args...)
		_, _ = io.WriteString(stdout, "status receipt\n")
		return 5
	}
	var stdout, stderr bytes.Buffer
	exit := runWithCommandRunners(context.Background(), []string{"status"},
		strings.NewReader("unrelated stdin"), &stdout, &stderr,
		commandRunners{setup: cliSetupMustNotRun(t), eject: func(context.Context, []string,
			io.Writer, io.Writer, string,
		) int {
			t.Fatal("status route invoked eject")
			return 1
		}, status: status})
	if exit != 5 || len(received) != 0 || stdout.String() != "status receipt\n" ||
		stderr.Len() != 0 {
		t.Fatalf("status route = exit %d args %v stdout %q stderr %q", exit, received,
			stdout.String(), stderr.String())
	}
}

func TestRunRoutesEjectThroughThePublicLifecycleBoundary(t *testing.T) {
	var received []string
	eject := func(ctx context.Context, args []string, stdout, stderr io.Writer,
		gotVersion string,
	) int {
		if ctx == nil || gotVersion != version {
			t.Fatalf("eject composition = context %v version %q", ctx, gotVersion)
		}
		received = append([]string(nil), args...)
		_, _ = io.WriteString(stdout, "eject receipt\n")
		return 6
	}
	var stdout, stderr bytes.Buffer
	exit := runWithRunners(context.Background(), []string{"eject", "--host", "codex",
		"--project-root", "."}, strings.NewReader("unrelated stdin"), &stdout, &stderr,
		cliSetupMustNotRun(t), eject)
	if exit != 6 || !reflect.DeepEqual(received,
		[]string{"--host", "codex", "--project-root", "."}) ||
		stdout.String() != "eject receipt\n" || stderr.Len() != 0 {
		t.Fatalf("eject route = exit %d args %v stdout %q stderr %q", exit, received,
			stdout.String(), stderr.String())
	}
}

func TestRunRoutesResetWhileKeepingItOutOfHelp(t *testing.T) {
	var received []string
	reset := func(ctx context.Context, args []string, stdout, stderr io.Writer,
		gotVersion string,
	) int {
		if ctx == nil || gotVersion != version {
			t.Fatalf("reset composition = context %v version %q", ctx, gotVersion)
		}
		received = append([]string(nil), args...)
		_, _ = io.WriteString(stdout, "reset receipt\n")
		return 4
	}
	var stdout, stderr bytes.Buffer
	exit := runWithCommandRunners(context.Background(),
		[]string{"reset", "--force", "--confirm-peer", "peer"}, strings.NewReader(""),
		&stdout, &stderr, commandRunners{reset: reset})
	if exit != 4 || !reflect.DeepEqual(received,
		[]string{"--force", "--confirm-peer", "peer"}) ||
		stdout.String() != "reset receipt\n" || stderr.Len() != 0 {
		t.Fatalf("reset route = exit %d args %v stdout %q stderr %q", exit, received,
			stdout.String(), stderr.String())
	}
}

func TestRunRoutesAgencyBeforeLegacyCommandDispatch(t *testing.T) {
	var received []string
	agency := func(ctx context.Context, args []string, stdin io.Reader,
		stdout, stderr io.Writer,
	) (bool, int) {
		if ctx == nil || stdin == nil || stderr == nil {
			t.Fatal("agency composition is incomplete")
		}
		received = append([]string(nil), args...)
		_, _ = io.WriteString(stdout, "agency receipt\n")
		return true, 8
	}
	var stdout, stderr bytes.Buffer
	exit := runWithCommandRunners(context.Background(),
		[]string{"agent", "current", "--json"}, strings.NewReader(""),
		&stdout, &stderr, commandRunners{agency: agency})
	if exit != 8 || !reflect.DeepEqual(received,
		[]string{"agent", "current", "--json"}) ||
		stdout.String() != "agency receipt\n" || stderr.Len() != 0 {
		t.Fatalf("agency route = exit %d args %v stdout %q stderr %q", exit,
			received, stdout.String(), stderr.String())
	}
}

func cliSetupMustNotRun(t *testing.T) setupRunner {
	t.Helper()
	return func(context.Context, []string, io.Writer, io.Writer, string) int {
		t.Fatal("eject route invoked setup")
		return 1
	}
}

func TestRunRoutesSetupAndOnlyPassesCommandArgumentsAndVersion(t *testing.T) {
	var received []string
	setup := func(ctx context.Context, args []string, stdout, stderr io.Writer,
		gotVersion string,
	) int {
		if ctx == nil || gotVersion != version {
			t.Fatalf("setup composition = context %v version %q", ctx, gotVersion)
		}
		received = append([]string(nil), args...)
		_, _ = io.WriteString(stdout, "setup receipt\n")
		return 7
	}
	var stdout, stderr bytes.Buffer
	exit := runWithSetup(context.Background(), []string{"setup", "--host", "codex",
		"--project-root", "."}, strings.NewReader("unrelated stdin"), &stdout, &stderr, setup)
	if exit != 7 || !reflect.DeepEqual(received,
		[]string{"--host", "codex", "--project-root", "."}) ||
		stdout.String() != "setup receipt\n" || stderr.Len() != 0 {
		t.Fatalf("setup route = exit %d args %v stdout %q stderr %q", exit, received,
			stdout.String(), stderr.String())
	}
}
