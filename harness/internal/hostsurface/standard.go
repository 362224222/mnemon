package hostsurface

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

const standardShimMarker = "mnemon-r1"

type StandardHostOptions struct {
	Host        string
	ProjectRoot string
	DryRun      bool
	Stdout      io.Writer
	Stderr      io.Writer
}

// InstallStandardHost installs the R1 static host integration. It writes only shim mechanics and host
// registration, leaving governed content to render/pull.
func InstallStandardHost(ctx context.Context, opts StandardHostOptions) (Report, error) {
	_ = ctx
	core, err := newStandardCore(opts)
	if err != nil {
		return Report{}, err
	}
	core.beginManaged(standardShimMarker)
	hookDir := pathJoin(core.paths.configDir, "hooks", standardShimMarker)
	var files []string
	for _, timing := range hookTimings {
		body, err := RenderStandardThinHook(opts.Host, timing)
		if err != nil {
			return Report{}, err
		}
		target := pathJoin(hookDir, timing+".sh")
		if err := core.projectManagedBytes([]byte(body), target, 0o755); err != nil {
			return Report{}, err
		}
		files = append(files, target)
	}
	if err := patchStandardHostRegistration(core); err != nil {
		return Report{}, err
	}
	files = append(files, standardRegistrationPath(core))
	sort.Strings(files)
	ownership := projectionOwnership{
		Files:         files,
		Dirs:          []string{hookDir},
		Hashes:        core.managed.next,
		Preserved:     core.managed.conflicts,
		MarkerVersion: managedMarkerVersion,
	}
	if err := writeStandardHostManifest(core, ownership); err != nil {
		return Report{}, err
	}
	core.printf("Installed Mnemon R1 shim for %s.\n", core.host)
	return Report{Conflicts: core.managed.conflicts}, nil
}

// UninstallStandardHost removes the R1 static host integration while preserving user-edited files.
func UninstallStandardHost(ctx context.Context, opts StandardHostOptions) (Report, error) {
	_ = ctx
	core, err := newStandardCore(opts)
	if err != nil {
		return Report{}, err
	}
	core.beginManaged(standardShimMarker)
	if err := unpatchStandardHostRegistration(core); err != nil {
		return Report{}, err
	}
	if err := core.removeManagedTree(pathJoin(core.paths.configDir, "hooks", standardShimMarker)); err != nil {
		return Report{}, err
	}
	if err := core.removeHostManifestLoop(standardShimMarker); err != nil {
		return Report{}, err
	}
	core.printf("Removed Mnemon R1 shim from %s.\n", core.paths.configDir)
	return Report{Conflicts: core.managed.conflicts}, nil
}

func newStandardCore(opts StandardHostOptions) (projectorCore, error) {
	if opts.ProjectRoot == "" {
		cwd, err := os.Getwd()
		if err != nil {
			return projectorCore{}, fmt.Errorf("resolve project root: %w", err)
		}
		opts.ProjectRoot = cwd
	}
	projectRoot, err := filepath.Abs(opts.ProjectRoot)
	if err != nil {
		return projectorCore{}, fmt.Errorf("resolve project root: %w", err)
	}
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}
	var paths corePaths
	switch opts.Host {
	case "codex":
		paths = corePaths{configDir: ".codex", mnemonDir: ".mnemon"}
	case "claude-code":
		paths = corePaths{configDir: ".claude", mnemonDir: ".mnemon"}
	default:
		return projectorCore{}, fmt.Errorf("unsupported host %q", opts.Host)
	}
	return projectorCore{
		host:        opts.Host,
		projectRoot: projectRoot,
		paths:       paths,
		stdout:      opts.Stdout,
		stderr:      opts.Stderr,
		dryRun:      opts.DryRun,
		managed:     newManagedState(),
	}, nil
}

func patchStandardHostRegistration(core projectorCore) error {
	switch core.host {
	case "codex":
		if core.dryRun {
			core.printf("would patch %s\n", standardRegistrationPath(core))
			return nil
		}
		return patchCodexHooks(core.resolve(standardRegistrationPath(core)), core.paths.configDir, standardShimMarker, codexHookOptions{Remind: true, Nudge: true, Compact: true})
	case "claude-code":
		if core.dryRun {
			core.printf("would patch %s\n", standardRegistrationPath(core))
			return nil
		}
		return patchClaudeSettings(core.resolve(standardRegistrationPath(core)), core.paths.configDir, standardShimMarker, claudeHookOptions{Remind: true, Nudge: true, Compact: true})
	default:
		return fmt.Errorf("unsupported host %q", core.host)
	}
}

func unpatchStandardHostRegistration(core projectorCore) error {
	switch core.host {
	case "codex":
		return unpatchCodexHooks(core.resolve(standardRegistrationPath(core)), standardShimMarker)
	case "claude-code":
		return unpatchClaudeSettings(core.resolve(standardRegistrationPath(core)), standardShimMarker)
	default:
		return fmt.Errorf("unsupported host %q", core.host)
	}
}

func standardRegistrationPath(core projectorCore) string {
	switch core.host {
	case "codex":
		return pathJoin(core.paths.configDir, "hooks.json")
	case "claude-code":
		return pathJoin(core.paths.configDir, "settings.json")
	default:
		return ""
	}
}

func writeStandardHostManifest(core projectorCore, ownership projectionOwnership) error {
	manifestPath := core.resolve(core.hostManifestPath())
	manifest := hostProjectionManifest{
		SchemaVersion: 2,
		Host:          core.host,
		Loops:         map[string]hostManifestLoop{},
	}
	if data, err := os.ReadFile(manifestPath); err == nil && len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, &manifest); err != nil {
			return fmt.Errorf("parse host manifest %s: %w", core.hostManifestPath(), err)
		}
	}
	if manifest.Loops == nil {
		manifest.Loops = map[string]hostManifestLoop{}
	}
	manifest.SchemaVersion = 2
	manifest.Host = core.host
	manifest.UpdatedAt = nowUTC()
	manifest.ProjectRoot = core.projectRoot
	manifest.MnemonDir = core.paths.mnemonDir
	manifest.Loops[standardShimMarker] = hostManifestLoop{
		LoopPath:     pathJoin(core.paths.configDir, "hooks", standardShimMarker),
		StatePath:    pathJoin(core.paths.configDir, "hooks", standardShimMarker),
		IntentPolicy: "render",
		StatusPath:   renderAuditRelPathPlaceholder,
		Projection: map[string]any{
			"path":     core.paths.configDir,
			"surfaces": []string{"static render hooks"},
		},
		Reality: map[string]any{
			"surfaces": []string{"render presentation"},
		},
		Reconcile: map[string]any{
			"actions": []string{"install", "uninstall"},
		},
		LifecycleMapping: map[string]string{
			"prime":   "SessionStart",
			"remind":  "UserPromptSubmit",
			"nudge":   "Stop",
			"compact": "PreCompact",
		},
		Surfaces: map[string]string{
			"hooks": pathJoin(core.paths.configDir, "hooks", standardShimMarker),
		},
		Ownership: ownership,
	}
	return core.writeJSON(core.hostManifestPath(), manifest, 0o644)
}

const renderAuditRelPathPlaceholder = ".mnemon/harness/local/render-audit.jsonl"
