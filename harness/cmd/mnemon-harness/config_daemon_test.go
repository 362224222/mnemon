package main

import (
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/productconfig"
)

func TestConfigValidateReadsProductConfig(t *testing.T) {
	root := t.TempDir()
	cfg := productconfig.Default()
	cfg.Participants = []productconfig.Participant{{
		Principal: "planner@team",
		HostRuntime: productconfig.HostRuntime{
			Kind: productconfig.RuntimeKindCodex,
			Mode: productconfig.RuntimeModeManaged,
		},
	}}
	if err := productconfig.Save(productconfig.DefaultPath(root, ""), cfg); err != nil {
		t.Fatal(err)
	}
	oldRoot, oldPath := configRoot, configPath
	configRoot, configPath = root, ""
	t.Cleanup(func() { configRoot, configPath = oldRoot, oldPath })

	cmd, out := testCommand()
	if err := runConfigValidate(cmd, nil); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, "Harness config: valid") || !strings.Contains(got, "Participants: 1") {
		t.Fatalf("unexpected config validate output:\n%s", got)
	}
}

func TestDaemonStatusDoesNotMutateMissingConfig(t *testing.T) {
	oldRoot := daemonRoot
	daemonRoot = t.TempDir()
	t.Cleanup(func() { daemonRoot = oldRoot })

	cmd, out := testCommand()
	if err := runDaemonStatus(cmd, nil); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{"Harness config: not configured", "Harness daemon: not running"} {
		if !strings.Contains(got, want) {
			t.Fatalf("daemon status missing %q:\n%s", want, got)
		}
	}
}
