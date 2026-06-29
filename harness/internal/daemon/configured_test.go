package daemon

import (
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/productconfig"
)

func TestConfiguredSnapshotIncludesConfiguredRoles(t *testing.T) {
	now := time.Date(2026, 6, 29, 9, 50, 0, 0, time.UTC)
	cfg := productconfig.Default()
	cfg.Daemon.InteractionWatchers = []string{" ", productconfig.ConnectionMultica}
	cfg.Daemon.DriveSources = []string{"", productconfig.DriveManagedLocal}
	cfg.Daemon.ProjectionSurfaces = []string{"\t", productconfig.ConnectionMultica}

	snapshot := ConfiguredSnapshot(cfg, now)
	for name, want := range map[string]WorkerKind{
		"multica-watch":    WorkerInteraction,
		"managed-drive":    WorkerDrive,
		"multica-project":  WorkerProjection,
		"status-readiness": WorkerStatus,
	} {
		worker, ok := snapshot.Workers[name]
		if !ok {
			t.Fatalf("snapshot missing worker %q: %+v", name, snapshot.Workers)
		}
		if worker.Kind != want || worker.Status != "configured" || !worker.StartedAt.Equal(now) || !worker.UpdatedAt.Equal(now) {
			t.Fatalf("worker %q mismatch: %+v", name, worker)
		}
	}
	for _, name := range []string{"-watch", "-drive", "-project"} {
		if _, ok := snapshot.Workers[name]; ok {
			t.Fatalf("snapshot should skip empty worker name %q: %+v", name, snapshot.Workers)
		}
	}
}

func TestConfiguredSnapshotKeepsMnemonhubAsExchangeWatcher(t *testing.T) {
	now := time.Date(2026, 6, 29, 10, 5, 0, 0, time.UTC)
	cfg := productconfig.Default()
	cfg.Connections.Mnemonhub = productconfig.MnemonhubConnection{Enabled: true, Endpoint: "https://hub.example.invalid"}
	cfg.Daemon.InteractionWatchers = []string{productconfig.ConnectionMnemonhub}

	snapshot := ConfiguredSnapshot(cfg, now)
	worker, ok := snapshot.Workers["mnemonhub-watch"]
	if !ok {
		t.Fatalf("snapshot missing mnemonhub watcher: %+v", snapshot.Workers)
	}
	if worker.Kind != WorkerInteraction || worker.Status != "configured" || worker.Message != "watcher=mnemonhub" {
		t.Fatalf("mnemonhub watcher mismatch: %+v", worker)
	}
	if _, ok := snapshot.Workers["mnemonhub-project"]; ok {
		t.Fatalf("mnemonhub must remain a remote exchange watcher, not projection worker: %+v", snapshot.Workers)
	}
}

func TestRoleDetailsDescribeDaemonBoundaries(t *testing.T) {
	cfg := productconfig.Default()
	cfg.Daemon.InteractionWatchers = []string{
		productconfig.ConnectionMultica,
		productconfig.ConnectionMnemonhub,
	}
	cfg.Daemon.DriveSources = []string{productconfig.DriveManagedLocal}
	cfg.Daemon.ProjectionSurfaces = []string{productconfig.ConnectionMultica}

	details := RoleDetails(cfg)
	if len(details) != 4 {
		t.Fatalf("role details mismatch: %+v", details)
	}
	for _, want := range []ConfiguredRoleDetail{
		{WorkerName: "multica-watch", Kind: WorkerInteraction, Label: "watcher", Value: productconfig.ConnectionMultica, Boundary: "activation-carrier"},
		{WorkerName: "mnemonhub-watch", Kind: WorkerInteraction, Label: "watcher", Value: productconfig.ConnectionMnemonhub, Boundary: "remote-exchange"},
		{WorkerName: "managed-drive", Kind: WorkerDrive, Label: "drive", Value: productconfig.DriveManagedLocal, Boundary: "managed-runtime"},
		{WorkerName: "multica-project", Kind: WorkerProjection, Label: "surface", Value: productconfig.ConnectionMultica, Boundary: "projection-surface"},
	} {
		if !hasRoleDetail(details, want) {
			t.Fatalf("missing role detail %+v in %+v", want, details)
		}
	}
}

func TestRoleSummaryReflectsConfiguredDaemonRoles(t *testing.T) {
	cfg := productconfig.Default()
	cfg.Daemon.InteractionWatchers = []string{productconfig.ConnectionMultica, productconfig.ConnectionGitHub}
	cfg.Daemon.DriveSources = []string{productconfig.DriveManagedLocal}
	cfg.Daemon.ProjectionSurfaces = []string{productconfig.ConnectionMultica}

	got := RoleSummary(cfg)
	if got.InteractionWatchers != 2 || got.DriveSources != 1 || got.ProjectionSurfaces != 1 {
		t.Fatalf("role summary mismatch: %+v", got)
	}
}

func hasRoleDetail(details []ConfiguredRoleDetail, want ConfiguredRoleDetail) bool {
	for _, detail := range details {
		if detail == want {
			return true
		}
	}
	return false
}
