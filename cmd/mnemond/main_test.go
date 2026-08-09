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

func TestRunHasOnlyTheMnemondSurface(t *testing.T) {
	t.Parallel()
	command := func(args ...string) (string, string, int) {
		var stdout, stderr bytes.Buffer
		exit := runWithCommandRunners(context.Background(), args, strings.NewReader(""),
			&stdout, &stderr, commandRunners{})
		return stdout.String(), stderr.String(), exit
	}
	help, stderr, exit := command()
	if exit != 0 || stderr != "" || help != helpText {
		t.Fatalf("empty invocation = (%q, %q, %d)", help, stderr, exit)
	}
	for _, argument := range []string{"-h", "--help", "help"} {
		stdout, stderr, exit := command(argument)
		if exit != 0 || stdout != help || stderr != "" {
			t.Fatalf("help %q = (%q, %q, %d)", argument, stdout, stderr, exit)
		}
	}
	for _, argument := range []string{"--version", "version"} {
		stdout, stderr, exit := command(argument)
		if exit != 0 || stdout != "mnemond version dev\n" || stderr != "" {
			t.Fatalf("version %q = (%q, %q, %d)", argument, stdout, stderr, exit)
		}
	}
	lower := strings.ToLower(help)
	for _, required := range []string{"mnemond setup", "mnemond peer prepare", "mnemond serve"} {
		if !strings.Contains(lower, required) {
			t.Fatalf("help lacks product command %q", required)
		}
	}
	for _, forbidden := range []string{"r5", "channel", "teamwork", "codex", "eject",
		"doctor", "status", "reset", "managed", "review", "workflow"} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("help contains retired or case-specific vocabulary %q", forbidden)
		}
	}
}

func TestRunRoutesServeAndReportsItsFailure(t *testing.T) {
	serveFailure := fmt.Errorf("serve failed")
	var received []string
	serve := func(ctx context.Context, args []string) error {
		if ctx == nil {
			t.Fatal("serve received nil context")
		}
		received = append([]string(nil), args...)
		return serveFailure
	}
	var stdout, stderr bytes.Buffer
	exit := runWithCommandRunners(context.Background(),
		[]string{"serve", "--state-dir", "/workspace/.mnemon/agency"},
		strings.NewReader(""), &stdout, &stderr, commandRunners{serve: serve})
	if exit != 1 || stdout.Len() != 0 ||
		!reflect.DeepEqual(received, []string{"--state-dir", "/workspace/.mnemon/agency"}) ||
		stderr.String() != "mnemond: serve failed\n" {
		t.Fatalf("serve route = exit %d args %#v stdout %q stderr %q",
			exit, received, stdout.String(), stderr.String())
	}
}

func TestRunRoutesPeerBootstrapAndOnlyItsArguments(t *testing.T) {
	var received []string
	peer := func(ctx context.Context, args []string, stdin io.Reader,
		stdout, stderr io.Writer,
	) int {
		if ctx == nil || stdin == nil || stderr == nil {
			t.Fatal("peer composition is incomplete")
		}
		received = append([]string(nil), args...)
		_, _ = io.WriteString(stdout, "peer receipt\n")
		return 9
	}
	args := []string{"peer", "enroll", "--alias", "peer-b", "--project-root", "/workspace"}
	var stdout, stderr bytes.Buffer
	exit := runWithCommandRunners(context.Background(), args, strings.NewReader("card"),
		&stdout, &stderr, commandRunners{peer: peer})
	want := args[1:]
	if exit != 9 || !reflect.DeepEqual(received, want) ||
		stdout.String() != "peer receipt\n" || stderr.Len() != 0 {
		t.Fatalf("peer route = exit %d args %#v stdout %q stderr %q",
			exit, received, stdout.String(), stderr.String())
	}
}

func TestRunRoutesSetupAndOnlyItsArguments(t *testing.T) {
	var received []string
	setup := func(ctx context.Context, args []string, stdout, stderr io.Writer) int {
		if ctx == nil || stderr == nil {
			t.Fatal("setup composition is incomplete")
		}
		received = append([]string(nil), args...)
		_, _ = io.WriteString(stdout, "setup receipt\n")
		return 7
	}
	var stdout, stderr bytes.Buffer
	exit := runWithCommandRunners(context.Background(), []string{"setup", "--runtime", "pi",
		"--project-root", "/workspace"}, strings.NewReader("ignored"), &stdout, &stderr,
		commandRunners{setup: setup})
	want := []string{"--runtime", "pi", "--project-root", "/workspace"}
	if exit != 7 || !reflect.DeepEqual(received, want) ||
		stdout.String() != "setup receipt\n" || stderr.Len() != 0 {
		t.Fatalf("setup route = exit %d args %#v stdout %q stderr %q",
			exit, received, stdout.String(), stderr.String())
	}
}

func TestRunRoutesOnlyHiddenR7AgentTerminalCommands(t *testing.T) {
	for _, args := range [][]string{{"hook", "attach", "--json"},
		{"agent", "current", "--json"}, {"agent", "submit", "--json"},
		{"artifact", "capture", "--json"}, {"artifact", "read", "artifact:offered"}} {
		args := args
		t.Run(strings.Join(args[:2], "_"), func(t *testing.T) {
			var received []string
			terminal := func(ctx context.Context, got []string, stdin io.Reader,
				stdout, stderr io.Writer,
			) int {
				if ctx == nil || stdin == nil || stderr == nil {
					t.Fatal("terminal composition is incomplete")
				}
				received = append([]string(nil), got...)
				_, _ = io.WriteString(stdout, "terminal receipt\n")
				return 8
			}
			var stdout, stderr bytes.Buffer
			exit := runWithCommandRunners(context.Background(), args, strings.NewReader("input"),
				&stdout, &stderr, commandRunners{terminal: terminal})
			if exit != 8 || !reflect.DeepEqual(received, args) ||
				stdout.String() != "terminal receipt\n" || stderr.Len() != 0 {
				t.Fatalf("terminal route = exit %d args %#v stdout %q stderr %q",
					exit, received, stdout.String(), stderr.String())
			}
		})
	}
}

func TestRunRejectsEveryRetiredOrUnknownCommand(t *testing.T) {
	for _, command := range []string{"channel", "teamwork", "status", "doctor", "eject",
		"reset", "agency", "sync", "daemon"} {
		var stdout, stderr bytes.Buffer
		exit := runWithCommandRunners(context.Background(), []string{command},
			strings.NewReader(""), &stdout, &stderr, commandRunners{
				setup: func(context.Context, []string, io.Writer, io.Writer) int {
					t.Fatal("unknown command invoked setup")
					return 1
				},
				terminal: func(context.Context, []string, io.Reader, io.Writer, io.Writer) int {
					t.Fatal("unknown command invoked terminal")
					return 1
				},
				peer: func(context.Context, []string, io.Reader, io.Writer, io.Writer) int {
					t.Fatal("unknown command invoked peer")
					return 1
				},
			})
		want := fmt.Sprintf("mnemond: unknown command %q\n", command)
		if exit != 2 || stdout.Len() != 0 || stderr.String() != want {
			t.Fatalf("unknown %q = exit %d stdout %q stderr %q",
				command, exit, stdout.String(), stderr.String())
		}
	}
}
