package main

import (
	"context"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

func TestMulticaProvisionAcceptanceCommandRegistered(t *testing.T) {
	commands := map[string]bool{}
	for _, cmd := range rootCmd.Commands() {
		commands[cmd.Name()] = true
	}
	if !commands["multica-provision"] {
		t.Fatalf("mnemon-acceptance should expose multica-provision command: %v", commands)
	}
}

func TestMulticaProvisionAcceptanceBuildsHiddenHarnessBridge(t *testing.T) {
	oldWorkspace := acceptanceMulticaProvisionWorkspaceID
	oldProfile := acceptanceMulticaProvisionProfile
	oldRuntimeCommand := acceptanceMulticaProvisionRuntimeCommand
	oldRuntimePath := acceptanceMulticaProvisionRuntimePath
	oldWait := acceptanceMulticaProvisionWait
	acceptanceMulticaProvisionWorkspaceID = "ws-1"
	acceptanceMulticaProvisionProfile = "desktop-api.multica.ai"
	acceptanceMulticaProvisionRuntimeCommand = "mnemon-multica-runtime"
	acceptanceMulticaProvisionRuntimePath = "/tmp/mnemon-multica-runtime"
	acceptanceMulticaProvisionWait = 3 * time.Second
	t.Cleanup(func() {
		acceptanceMulticaProvisionWorkspaceID = oldWorkspace
		acceptanceMulticaProvisionProfile = oldProfile
		acceptanceMulticaProvisionRuntimeCommand = oldRuntimeCommand
		acceptanceMulticaProvisionRuntimePath = oldRuntimePath
		acceptanceMulticaProvisionWait = oldWait
	})

	args := buildAcceptanceMulticaProvisionArgs()
	for _, want := range []string{
		"multica",
		"--json",
		"provision",
		"--acceptance-bridge",
		"--multica-workspace-id",
		"ws-1",
		"--runtime-command",
		"mnemon-multica-runtime",
		"--runtime-path",
		"/tmp/mnemon-multica-runtime",
		"--wait",
		"3s",
	} {
		if !slices.Contains(args, want) {
			t.Fatalf("args missing %q: %v", want, args)
		}
	}
	if strings.Join(args, " ") == "" {
		t.Fatal("args must not be empty")
	}
}

func TestMulticaProvisionAcceptanceRunsHarnessBridge(t *testing.T) {
	oldRunner := runAcceptanceMulticaProvisionHarness
	oldCommand := acceptanceMulticaProvisionHarnessCommand
	oldWorkspace := acceptanceMulticaProvisionWorkspaceID
	defer func() {
		runAcceptanceMulticaProvisionHarness = oldRunner
		acceptanceMulticaProvisionHarnessCommand = oldCommand
		acceptanceMulticaProvisionWorkspaceID = oldWorkspace
	}()

	var gotCommand string
	var gotArgs []string
	runAcceptanceMulticaProvisionHarness = func(ctx context.Context, command string, args []string, stdout, stderr io.Writer) error {
		gotCommand = command
		gotArgs = append([]string(nil), args...)
		return nil
	}
	acceptanceMulticaProvisionHarnessCommand = "/tmp/mnemon-harness"
	acceptanceMulticaProvisionWorkspaceID = "ws-acceptance"

	cmd := &cobra.Command{}
	if err := acceptanceMulticaProvisionCmd.RunE(cmd, nil); err != nil {
		t.Fatal(err)
	}
	if gotCommand != "/tmp/mnemon-harness" {
		t.Fatalf("command = %q", gotCommand)
	}
	if !slices.Contains(gotArgs, "provision") || !slices.Contains(gotArgs, "--acceptance-bridge") || !slices.Contains(gotArgs, "ws-acceptance") {
		t.Fatalf("hidden harness provision args not propagated: %v", gotArgs)
	}
}
