package main

import (
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/daemon"
	"github.com/mnemon-dev/mnemon/harness/internal/productconfig"
)

func TestDoctorReportsMissingConfigurationWithoutMutating(t *testing.T) {
	root := t.TempDir()
	oldRoot := doctorRoot
	doctorRoot = root
	t.Cleanup(func() { doctorRoot = oldRoot })

	cmd, out := testCommand()
	if err := runDoctor(cmd, nil); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"Harness doctor",
		"- Product config: missing",
		"- Local Mnemon config: missing",
		"- Daemon snapshot: missing",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("doctor output missing %q:\n%s", want, got)
		}
	}
}

func TestDoctorReportsConfiguredProductSurface(t *testing.T) {
	root := t.TempDir()
	cfg := productconfig.Default()
	cfg.Connections.Multica = productconfig.MulticaConnection{Enabled: true, Workspace: "ws-multica", RuntimeBinary: "mnemon-multica-runtime"}
	cfg.Participants = []productconfig.Participant{{
		Principal: "planner@team",
		HostRuntime: productconfig.HostRuntime{
			Kind: productconfig.RuntimeKindCodex,
			Mode: productconfig.RuntimeModeManaged,
		},
	}}
	cfg.Daemon.InteractionWatchers = []string{productconfig.ConnectionMultica}
	cfg.Daemon.DriveSources = []string{productconfig.DriveManagedLocal}
	cfg.Daemon.DisplaySurfaces = []string{productconfig.ConnectionMultica}
	if err := productconfig.Save(productconfig.DefaultPath(root, ""), cfg); err != nil {
		t.Fatal(err)
	}
	if err := daemon.NewFileSnapshotStore(daemon.StatusSnapshotPath(root, "")).Save(daemon.Snapshot{
		StartedAt: time.Date(2026, 6, 29, 9, 0, 0, 0, time.UTC),
		Workers: map[string]daemon.WorkerSnapshot{
			"multica-watch": {Kind: daemon.WorkerInteraction, Status: "idle"},
			"managed-drive": {Kind: daemon.WorkerDrive, Status: "idle"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	oldRoot := doctorRoot
	doctorRoot = root
	t.Cleanup(func() { doctorRoot = oldRoot })

	cmd, out := testCommand()
	if err := runDoctor(cmd, nil); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		"- Product config: configured",
		"- Participants: 1",
		"- Daemon roles: watchers=1 drive=1 surfaces=1",
		"- Connections: multica",
		"- Local Mnemon config: missing",
		"- Daemon snapshot: workers=2",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("doctor output missing %q:\n%s", want, got)
		}
	}
}
