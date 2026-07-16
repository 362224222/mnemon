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
		"mnemon-harness setup [--host auto|codex|claude-code] [--project-root DIR]") {
		t.Error("ordinary help does not expose the complete setup entrypoint")
	}
	for _, forbidden := range []string{
		"hub", "remote workspace", "multica", "generic capability", "mcp",
		"evolution", "tower", "session", "hook check", "agent current", "teamwork offer",
	} {
		if strings.Contains(lowerHelp, forbidden) {
			t.Errorf("help contains retired vocabulary %q", forbidden)
		}
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
