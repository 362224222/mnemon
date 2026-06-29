package daemon

import (
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/productconfig"
)

type ConfiguredRoleSummary struct {
	InteractionWatchers int
	DriveSources        int
	ProjectionSurfaces  int
}

func RoleSummary(cfg productconfig.Config) ConfiguredRoleSummary {
	return ConfiguredRoleSummary{
		InteractionWatchers: len(cfg.Daemon.InteractionWatchers),
		DriveSources:        len(cfg.Daemon.DriveSources),
		ProjectionSurfaces:  len(cfg.Daemon.ProjectionSurfaces),
	}
}

func ConfiguredSnapshot(cfg productconfig.Config, now time.Time) Snapshot {
	snapshot := Snapshot{StartedAt: now.UTC(), Workers: map[string]WorkerSnapshot{}}
	add := func(name string, kind WorkerKind, message string) {
		name = workerName(name)
		if name == "" {
			return
		}
		snapshot.Workers[name] = WorkerSnapshot{
			Kind:      kind,
			Status:    "configured",
			Message:   strings.TrimSpace(message),
			StartedAt: snapshot.StartedAt,
			UpdatedAt: snapshot.StartedAt,
		}
	}
	for _, watcher := range cfg.Daemon.InteractionWatchers {
		watcher = strings.TrimSpace(watcher)
		if watcher == "" {
			continue
		}
		add(watcher+"-watch", WorkerInteraction, "watcher="+watcher)
	}
	for _, source := range cfg.Daemon.DriveSources {
		source = strings.TrimSpace(source)
		if source == "" {
			continue
		}
		name := source + "-drive"
		if source == productconfig.DriveManagedLocal {
			name = "managed-drive"
		}
		add(name, WorkerDrive, "drive="+source)
	}
	for _, surface := range cfg.Daemon.ProjectionSurfaces {
		surface = strings.TrimSpace(surface)
		if surface == "" {
			continue
		}
		add(surface+"-project", WorkerProjection, "surface="+surface)
	}
	if len(snapshot.Workers) > 0 {
		add("status-readiness", WorkerStatus, "daemon status snapshot")
	}
	return snapshot
}

func workerName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	replacer := strings.NewReplacer(" ", "-", "/", "-", "\\", "-", ":", "-")
	return replacer.Replace(value)
}
