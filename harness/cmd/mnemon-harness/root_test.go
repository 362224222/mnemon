package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestRootHelpUsesLocalFirstProductSurface(t *testing.T) {
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetArgs([]string{"--help"})
	t.Cleanup(func() {
		rootCmd.SetOut(os.Stdout)
		rootCmd.SetErr(os.Stderr)
		rootCmd.SetArgs(nil)
	})

	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("root help returned error: %v", err)
	}
	got := out.String()
	for _, want := range []string{"event-driven", "collaboration substrate", "Teamwork is a profile", "Agent Integration", "Local Mnemon", "setup", "local", "config", "daemon", "doctor", "session", "agent", "connect"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected root help to contain %q:\n%s", want, got)
		}
	}
	for _, blocked := range []string{"completion", "eval", "goal", "coordination", "runner", "supervisor", "proposal"} {
		if strings.Contains(got, blocked) {
			t.Fatalf("root help leaked unsupported product term %q:\n%s", blocked, got)
		}
	}
	for _, blocked := range []string{"  multica", "  tower", "  token"} {
		if strings.Contains(got, blocked) {
			t.Fatalf("root help leaked debug command %q:\n%s", strings.TrimSpace(blocked), got)
		}
	}
}

func TestRootDoesNotExposeAcceptanceCommands(t *testing.T) {
	commands := map[string]bool{}
	for _, cmd := range rootCmd.Commands() {
		commands[cmd.Name()] = true
	}
	for _, blocked := range []string{"acceptance", "r1-codex", "r1-prod-sim", "r1-task-sim", "r1-github-mesh-task-suite"} {
		if commands[blocked] {
			t.Fatalf("mnemon-harness must not expose test-only acceptance command %q", blocked)
		}
	}
}

func TestProductHelpDoesNotExposeInternalVocabulary(t *testing.T) {
	for _, args := range [][]string{
		{"setup", "--help"},
		{"local", "run", "--help"},
		{"status", "--help"},
		{"sync", "--help"},
		{"sync", "connect", "--help"},
		{"agent", "--help"},
		{"agent", "add", "--help"},
		{"connect", "--help"},
		{"connect", "multica", "--help"},
		{"session", "--help"},
		{"session", "start", "--help"},
		{"session", "attach", "--help"},
		{"multica", "--help"},
	} {
		got := executeRootForHelp(t, args...)
		for _, blocked := range []string{"binding", "channel", "projection", "kernel", "runtime", "sync cursor", "token file", "control-agent", "import-issue", "project-comment", "provision", "participant"} {
			if strings.Contains(strings.ToLower(got), blocked) {
				t.Fatalf("%q help leaked internal term %q:\n%s", strings.Join(args, " "), blocked, got)
			}
		}
	}
}

func TestInternalCommandsStayHidden(t *testing.T) {
	for _, path := range [][]string{
		{"control"},
		{"loop"},
		{"multica"},
		{"multica", "import-issue"},
		{"multica", "participant"},
		{"multica", "project-comment"},
		{"multica", "provision"},
		{"token"},
		{"tower"},
	} {
		cmd, _, err := rootCmd.Find(path)
		if err != nil {
			t.Fatalf("find command %q: %v", strings.Join(path, " "), err)
		}
		if cmd == nil || cmd.Name() != path[len(path)-1] {
			t.Fatalf("command %q resolved to %+v", strings.Join(path, " "), cmd)
		}
		if !cmd.Hidden {
			t.Fatalf("internal/debug command %q must remain hidden from public help", strings.Join(path, " "))
		}
	}
}

func executeRootForHelp(t *testing.T, args ...string) string {
	t.Helper()
	var out bytes.Buffer
	rootCmd.SetOut(&out)
	rootCmd.SetErr(&out)
	rootCmd.SetArgs(args)
	t.Cleanup(func() {
		rootCmd.SetOut(os.Stdout)
		rootCmd.SetErr(os.Stderr)
		rootCmd.SetArgs(nil)
	})
	if err := rootCmd.Execute(); err != nil {
		t.Fatalf("root %v returned error: %v", args, err)
	}
	return out.String()
}
