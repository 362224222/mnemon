package main

import (
	"bytes"
	"context"
	"fmt"
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
	for _, forbidden := range []string{
		"hub", "remote workspace", "multica", "generic capability", "mcp",
		"evolution", "tower", "session", "hook check", "agent current", "teamwork offer",
	} {
		if strings.Contains(lowerHelp, forbidden) {
			t.Errorf("help contains retired vocabulary %q", forbidden)
		}
	}
}
