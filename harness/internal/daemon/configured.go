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

type ConfiguredRoleDetail struct {
	WorkerName string
	Kind       WorkerKind
	Label      string
	Value      string
	Boundary   string
}

func RoleSummary(cfg productconfig.Config) ConfiguredRoleSummary {
	var summary ConfiguredRoleSummary
	for _, role := range RoleDetails(cfg) {
		switch role.Label {
		case "watcher":
			summary.InteractionWatchers++
		case "drive":
			summary.DriveSources++
		case "surface":
			summary.ProjectionSurfaces++
		}
	}
	return summary
}

func RoleDetails(cfg productconfig.Config) []ConfiguredRoleDetail {
	var details []ConfiguredRoleDetail
	for _, watcher := range cfg.Daemon.InteractionWatchers {
		watcher = strings.TrimSpace(watcher)
		if watcher == "" {
			continue
		}
		boundary := "external-interaction"
		if watcher == productconfig.ConnectionMultica {
			boundary = "activation-carrier"
		}
		if watcher == productconfig.ConnectionMnemonhub {
			boundary = "remote-exchange"
		}
		details = append(details, ConfiguredRoleDetail{
			WorkerName: workerName(watcher + "-watch"),
			Kind:       WorkerInteraction,
			Label:      "watcher",
			Value:      watcher,
			Boundary:   boundary,
		})
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
		details = append(details, ConfiguredRoleDetail{
			WorkerName: workerName(name),
			Kind:       WorkerDrive,
			Label:      "drive",
			Value:      source,
			Boundary:   "managed-runtime",
		})
	}
	for _, surface := range cfg.Daemon.ProjectionSurfaces {
		surface = strings.TrimSpace(surface)
		if surface == "" {
			continue
		}
		details = append(details, ConfiguredRoleDetail{
			WorkerName: workerName(surface + "-project"),
			Kind:       WorkerProjection,
			Label:      "surface",
			Value:      surface,
			Boundary:   "projection-surface",
		})
	}
	return details
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
	for _, role := range RoleDetails(cfg) {
		add(role.WorkerName, role.Kind, role.Label+"="+role.Value)
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
