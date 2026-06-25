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
	for _, want := range []string{"Agent Integration", "Local Mnemon", "Remote Workspace", "standard events", "setup", "local"} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected root help to contain %q:\n%s", want, got)
		}
	}
	for _, blocked := range []string{"completion", "eval", "goal", "coordination", "runner", "supervisor", "daemon", "proposal"} {
		if strings.Contains(got, blocked) {
			t.Fatalf("root help leaked unsupported product term %q:\n%s", blocked, got)
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
	} {
		got := executeRootForHelp(t, args...)
		for _, blocked := range []string{"binding", "channel", "projection", "kernel", "runtime", "sync cursor", "token file", "control-agent"} {
			if strings.Contains(strings.ToLower(got), blocked) {
				t.Fatalf("%q help leaked internal term %q:\n%s", strings.Join(args, " "), blocked, got)
			}
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
