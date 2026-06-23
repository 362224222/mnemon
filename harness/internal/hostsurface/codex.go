package hostsurface

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/manifest"
)

type CodexOptions struct {
	ProjectRoot string
	Loops       []string
	HostArgs    []string
	Stdout      io.Writer
	Stderr      io.Writer
}

type codexHostOptions struct {
	global            bool
	configDir         string
	configDirExplicit bool
	storeName         string
	hostSkillsDir     string
	dryRun            bool
	purgeMemory       bool
	purgeLibrary      bool
	thinRenderShim    bool
}

type codexProjector struct {
	projectorCore
	hostOptions codexHostOptions
}

type hostProjectionManifest struct {
	SchemaVersion int                         `json:"schema_version"`
	Host          string                      `json:"host"`
	UpdatedAt     string                      `json:"updated_at,omitempty"`
	ProjectRoot   string                      `json:"project_root,omitempty"`
	MnemonDir     string                      `json:"mnemon_dir,omitempty"`
	Store         string                      `json:"store,omitempty"`
	Loops         map[string]hostManifestLoop `json:"loops,omitempty"`
}

type hostManifestLoop struct {
	LoopPath         string              `json:"loop_path"`
	LoopVersion      string              `json:"loop_version,omitempty"`
	StatePath        string              `json:"state_path"`
	IntentPolicy     string              `json:"intent_policy"`
	StatusPath       string              `json:"status_path"`
	Projection       map[string]any      `json:"projection"`
	Reality          map[string]any      `json:"reality"`
	Reconcile        map[string]any      `json:"reconcile"`
	LifecycleMapping map[string]string   `json:"lifecycle_mapping"`
	Surfaces         map[string]string   `json:"surfaces"`
	Ownership        projectionOwnership `json:"ownership"`
}

type projectionOwnership struct {
	Files         []string          `json:"files,omitempty"`
	Dirs          []string          `json:"dirs,omitempty"`
	Hashes        map[string]string `json:"hashes,omitempty"`         // managed definition file -> hash we last wrote (no-clobber marker)
	Preserved     []string          `json:"preserved,omitempty"`      // managed paths we declined to write (user/pre-existing) -> never delete on uninstall
	MarkerVersion int               `json:"marker_version,omitempty"` // ownership-hash scheme version
}

func RunCodexProjector(ctx context.Context, action string, opts CodexOptions) error {
	if action != "install" && action != "uninstall" {
		return fmt.Errorf("unsupported Codex projector action: %s", action)
	}
	projector, loops, err := newCodexProjector(action, opts)
	if err != nil {
		return err
	}
	for _, loopName := range loops {
		loop, binding, err := resolveLoopAndBinding("codex", loopName, projector.projectRoot, projector.paths.configDir)
		if err != nil {
			return err
		}
		switch action {
		case "install":
			// --dry-run runs the SAME install path with the core write gates suppressing every
			// write: the report comes from the real classifier (would write / would preserve),
			// never from a parallel desired-files model that can drift from installLoop.
			if err := projector.installLoop(ctx, loop, binding); err != nil {
				return fmt.Errorf("install codex/%s: %w", loopName, err)
			}
		case "uninstall":
			if err := projector.uninstallLoop(loop); err != nil {
				return fmt.Errorf("uninstall codex/%s: %w", loopName, err)
			}
		}
	}
	return nil
}

// RunCodexProjectorReport installs/re-projects the Codex projection under the no-clobber policy and
// returns the managed files it preserved because the user edited them.
func RunCodexProjectorReport(ctx context.Context, opts CodexOptions) (Report, error) {
	projector, loops, err := newCodexProjector("install", opts)
	if err != nil {
		return Report{}, err
	}
	for _, loopName := range loops {
		loop, binding, err := resolveLoopAndBinding("codex", loopName, projector.projectRoot, projector.paths.configDir)
		if err != nil {
			return Report{}, err
		}
		if err := projector.installLoop(ctx, loop, binding); err != nil {
			return Report{}, fmt.Errorf("install codex/%s: %w", loopName, err)
		}
	}
	return Report{Conflicts: projector.managed.conflicts}, nil
}

func newCodexProjector(action string, opts CodexOptions) (codexProjector, []string, error) {
	var err error
	if opts.ProjectRoot == "" {
		opts.ProjectRoot, err = os.Getwd()
		if err != nil {
			return codexProjector{}, nil, fmt.Errorf("resolve project root: %w", err)
		}
	}
	projectRoot, err := filepath.Abs(opts.ProjectRoot)
	if err != nil {
		return codexProjector{}, nil, fmt.Errorf("resolve project root: %w", err)
	}
	hostOptions, err := parseCodexHostOptions(opts.HostArgs)
	if err != nil {
		return codexProjector{}, nil, err
	}
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}
	if _, err := manifest.ValidateFS(assets.FS); err != nil {
		return codexProjector{}, nil, err
	}
	loops := append([]string(nil), opts.Loops...)
	if len(loops) == 0 {
		return codexProjector{}, nil, errors.New("at least one --loop is required")
	}
	sort.Strings(loops)

	return codexProjector{
		projectorCore: projectorCore{
			host:        "codex",
			projectRoot: projectRoot,
			paths:       codexProjectorPaths(hostOptions),
			stdout:      opts.Stdout,
			stderr:      opts.Stderr,
			managed:     newManagedState(),
			assetFS:     newLoopAssetOverlay(projectRoot),

			skillsDirOverride: hostOptions.hostSkillsDir,
			purgeMemory:       hostOptions.purgeMemory,
			purgeLibrary:      hostOptions.purgeLibrary,
			dryRun:            hostOptions.dryRun,
			thinRenderShim:    hostOptions.thinRenderShim,
		},
		hostOptions: hostOptions,
	}, loops, nil
}

func parseCodexHostOptions(args []string) (codexHostOptions, error) {
	parsed := codexHostOptions{configDir: ".codex"}
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch arg {
		case "--global":
			parsed.global = true
		case "--config-dir":
			if i+1 >= len(args) {
				return parsed, errors.New("missing value for --config-dir")
			}
			parsed.configDir = args[i+1]
			parsed.configDirExplicit = true
			i++
		case "--store":
			if i+1 >= len(args) {
				return parsed, errors.New("missing value for --store")
			}
			parsed.storeName = args[i+1]
			i++
		case "--host-skills-dir":
			if i+1 >= len(args) {
				return parsed, errors.New("missing value for --host-skills-dir")
			}
			parsed.hostSkillsDir = args[i+1]
			i++
		case "--dry-run":
			parsed.dryRun = true
		case "--thin-render-shim":
			parsed.thinRenderShim = true
		case "--purge-memory":
			parsed.purgeMemory = true
		case "--purge-library":
			parsed.purgeLibrary = true
		default:
			return parsed, fmt.Errorf("unsupported Codex host option: %s", arg)
		}
	}
	return parsed, nil
}

func codexProjectorPaths(opts codexHostOptions) corePaths {
	if opts.global && !opts.configDirExplicit {
		home := os.Getenv("HOME")
		configDir := filepath.Join(home, ".codex")
		mnemonDir := os.Getenv("MNEMON_HARNESS_STATE_DIR")
		if mnemonDir == "" {
			mnemonDir = filepath.Join(home, ".mnemon")
		}
		return corePaths{configDir: filepath.ToSlash(configDir), mnemonDir: filepath.ToSlash(mnemonDir)}
	}
	mnemonDir := os.Getenv("MNEMON_HARNESS_STATE_DIR")
	if mnemonDir == "" {
		mnemonDir = ".mnemon"
	}
	return corePaths{configDir: filepath.ToSlash(opts.configDir), mnemonDir: filepath.ToSlash(mnemonDir)}
}

func (p codexProjector) installLoop(ctx context.Context, loop manifest.LoopManifest, binding manifest.BindingManifest) error {
	p.beginManaged(loop.Name)
	if err := p.copyCommonCanonicalAssets(loop); err != nil {
		return err
	}
	if err := p.prepareLoopState(loop); err != nil {
		return err
	}
	if err := p.writeRuntimeEnv(loop, binding); err != nil {
		return err
	}
	if err := p.projectManaged(p.loopAsset(loop, loop.Assets.Guide), pathJoin(binding.RuntimeSurface, "GUIDE.md"), 0o644); err != nil {
		return err
	}
	if err := p.projectRuntimeMirrors(loop, binding); err != nil {
		return err
	}
	if err := p.projectSkills(loop, binding); err != nil {
		return err
	}
	if err := p.projectHooks(loop, binding); err != nil {
		return err
	}
	if loop.HasHooks() {
		if err := p.patchHooks(loop); err != nil {
			return err
		}
	}
	if loop.Store != nil && loop.Store.Native && p.hostOptions.storeName != "" {
		if err := p.ensureStore(ctx, p.hostOptions.storeName); err != nil {
			return err
		}
	}
	ownership := p.loopOwnership(loop, binding)
	ownership.Hashes = p.managed.next
	ownership.Preserved = p.managed.conflicts
	ownership.MarkerVersion = managedMarkerVersion
	if err := p.writeHostManifest(loop, binding, ownership); err != nil {
		return err
	}
	if err := p.writeLoopStatus(loop, binding); err != nil {
		return err
	}
	p.printf("Installed Mnemon %s loop for Codex.\n", loop.Name)
	p.printf("Config:       %s\n", p.paths.configDir)
	p.printf("State:        %s\n", p.stateDir(loop.Name))
	if hostSkills := p.hostSkillsDir(loop.Name); hostSkills != "" {
		p.printf("Host skills:  %s\n", hostSkills)
	}
	return nil
}

func (p codexProjector) uninstallLoop(loop manifest.LoopManifest) error {
	binding, err := manifest.LoadBinding(assets.FS, "codex", loop.Name)
	if err != nil {
		return err
	}
	p.beginManaged(loop.Name) // load recorded ownership so uninstall preserves user-edited/foreign skills
	if loop.HasHooks() {
		if err := p.unpatchHooks(loop.Name); err != nil {
			return err
		}
	}
	hostSkillsDir := p.installedHostSkillsDir(loop.Name, binding)
	if loop.Name == "skill" {
		if err := p.removeGeneratedSkillViews(hostSkillsDir); err != nil {
			return err
		}
	}
	for _, skill := range loop.Assets.Skills {
		if err := p.removeManagedSkill(pathJoin(hostSkillsDir, skillID(skill), "SKILL.md")); err != nil {
			return err
		}
	}
	if err := p.removeManagedTree(pathJoin(p.paths.configDir, "hooks", "mnemon-"+loop.Name)); err != nil {
		return err
	}
	if err := p.removeManagedTree(binding.RuntimeSurface); err != nil {
		return err
	}
	if err := p.removeCanonicalState(loop); err != nil {
		return err
	}
	if err := p.removeHostManifestLoop(loop.Name); err != nil {
		return err
	}
	p.printf("Removed Mnemon %s loop from %s.\n", loop.Name, p.paths.configDir)
	return nil
}

// projectRuntimeMirrors seeds each declared runtime_file as a managed mirror in the runtime surface
// (PD4 — no loop.Name gate; the declared runtime_files list drives it, so memory seeds MEMORY.md and
// a loop declaring none is a no-op).
func (p codexProjector) projectRuntimeMirrors(loop manifest.LoopManifest, binding manifest.BindingManifest) error {
	for _, runtimeFile := range loop.Assets.RuntimeFiles {
		// Hash-recorded too: seeds the mirror on first install, preserves a live (prime-regenerated) or
		// user-edited mirror on re-setup and uninstall instead of clobbering/deleting it.
		if err := p.projectManaged(p.loopAsset(loop, runtimeFile), pathJoin(binding.RuntimeSurface, runtimeFile), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func (p codexProjector) projectSkills(loop manifest.LoopManifest, binding manifest.BindingManifest) error {
	hostSkillsDir := p.hostSkillsDir(loop.Name)
	for _, skill := range loop.Assets.Skills {
		target := pathJoin(hostSkillsDir, skillID(skill), "SKILL.md")
		content, err := p.projectedSkillContent(loop, binding, skill)
		if err != nil {
			return err
		}
		if err := p.projectManagedBytes(content, target, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func (p codexProjector) projectedSkillContent(loop manifest.LoopManifest, binding manifest.BindingManifest, skill string) ([]byte, error) {
	// canonicalSkillContent expands the payload-contract marker in place; the codex runtimeNote
	// still appends at the end, after whatever position the contract section occupies.
	content, err := p.canonicalSkillContent(loop, skill)
	if err != nil {
		return nil, err
	}
	note := runtimeNote(loopDirVarName(loop.Name), pathJoin(binding.RuntimeSurface, "env.sh"), p.stateDir(loop.Name))
	return append(content, []byte(note)...), nil
}

func (p codexProjector) patchHooks(loop manifest.LoopManifest) error {
	if p.dryRun {
		p.printf("would patch %s\n", pathJoin(p.paths.configDir, "hooks.json"))
		return nil
	}
	return patchCodexHooks(p.resolve(pathJoin(p.paths.configDir, "hooks.json")), p.paths.configDir, "mnemon-"+loop.Name, p.hookOptions(loop))
}

func (p codexProjector) unpatchHooks(loopName string) error {
	return unpatchCodexHooks(p.resolve(pathJoin(p.paths.configDir, "hooks.json")), "mnemon-"+loopName)
}

// hookOptions reads the loop's declared per-loop hook intent (PD4 — no loop.Name switch); codex
// applies all three bits directly. Absent declaration = no hooks.
func (p codexProjector) hookOptions(loop manifest.LoopManifest) codexHookOptions {
	if loop.HookOptions == nil {
		return codexHookOptions{}
	}
	return codexHookOptions{Remind: loop.HookOptions.Remind, Nudge: loop.HookOptions.Nudge, Compact: loop.HookOptions.Compact}
}

func (p codexProjector) writeHostManifest(loop manifest.LoopManifest, binding manifest.BindingManifest, ownership projectionOwnership) error {
	manifestPath := p.resolve(p.hostManifestPath())
	manifest := hostProjectionManifest{
		SchemaVersion: 2,
		Host:          "codex",
		Loops:         map[string]hostManifestLoop{},
	}
	if data, err := os.ReadFile(manifestPath); err == nil && len(bytes.TrimSpace(data)) > 0 {
		if err := json.Unmarshal(data, &manifest); err != nil {
			return fmt.Errorf("parse host manifest %s: %w", p.hostManifestPath(), err)
		}
	}
	if manifest.Loops == nil {
		manifest.Loops = map[string]hostManifestLoop{}
	}
	manifest.SchemaVersion = 2
	manifest.Host = "codex"
	manifest.UpdatedAt = nowUTC()
	manifest.ProjectRoot = p.projectRoot
	manifest.MnemonDir = p.paths.mnemonDir
	if p.hostOptions.storeName != "" {
		manifest.Store = p.hostOptions.storeName
	} else {
		manifest.Store = "default"
	}
	surfaces := map[string]string{
		"skills":  p.hostSkillsDir(loop.Name),
		"runtime": binding.RuntimeSurface,
	}
	if loop.HasHooks() {
		surfaces["hooks"] = pathJoin(binding.ProjectionPath, "hooks", "mnemon-"+loop.Name)
	}
	manifest.Loops[loop.Name] = hostManifestLoop{
		LoopPath:    p.stateDir(loop.Name),
		LoopVersion: loop.Version,
		StatePath:   p.stateDir(loop.Name),
		IntentPolicy: pathJoin(
			p.stateDir(loop.Name),
			"GUIDE.md",
		),
		StatusPath: pathJoin(p.stateDir(loop.Name), "status.json"),
		Projection: map[string]any{
			"path":     p.paths.configDir,
			"surfaces": loop.Surfaces.Projection,
		},
		Reality: map[string]any{
			"surfaces": loop.Surfaces.Observation,
		},
		Reconcile: map[string]any{
			"actions": binding.Reconcile,
		},
		LifecycleMapping: binding.LifecycleMapping,
		Surfaces:         surfaces,
		Ownership:        ownership,
	}
	return p.writeJSON(p.hostManifestPath(), manifest, 0o644)
}

func (p codexProjector) loopOwnership(loop manifest.LoopManifest, binding manifest.BindingManifest) projectionOwnership {
	files := []string{
		pathJoin(p.stateDir(loop.Name), "GUIDE.md"),
		pathJoin(p.stateDir(loop.Name), "env.sh"),
		pathJoin(p.stateDir(loop.Name), "loop.json"),
		pathJoin(p.stateDir(loop.Name), "status.json"),
		pathJoin(binding.RuntimeSurface, "env.sh"),
		pathJoin(binding.RuntimeSurface, "GUIDE.md"),
	}
	for _, runtimeFile := range loop.Assets.RuntimeFiles {
		if loop.Name == "memory" {
			continue
		}
		files = append(files, pathJoin(p.stateDir(loop.Name), runtimeFile))
	}
	for _, skill := range loop.Assets.Skills {
		files = append(files, pathJoin(p.hostSkillsDir(loop.Name), skillID(skill), "SKILL.md"))
	}
	if loop.HasHooks() {
		files = append(files, pathJoin(binding.ProjectionPath, "hooks.json"))
	}
	// Ownership enumerates the generated hook shells from the same intents source projectHooks
	// writes them from, so the audit stays truthful to what install can produce.
	hookTimings, _ := DeclaredHookTimings(p.assets(), loop.Name)
	for _, phase := range hookTimings {
		hook := pathJoin(binding.ProjectionPath, "hooks", "mnemon-"+loop.Name, phase+".sh")
		if p.exists(hook) || p.hostHookExists(loop.Name, phase) {
			files = append(files, hook)
		}
	}
	dirs := []string{binding.RuntimeSurface}
	if loop.HasHooks() {
		dirs = append(dirs, pathJoin(binding.ProjectionPath, "hooks", "mnemon-"+loop.Name))
	}
	sort.Strings(files)
	sort.Strings(dirs)
	return projectionOwnership{Files: files, Dirs: dirs}
}

func runtimeNote(loopDirVar, runtimeFile, canonicalLoopDir string) string {
	return fmt.Sprintf(`

## Codex Projection

This skill is projected by the Mnemon Codex host adapter.

- Canonical loop directory: %s
- Runtime env file: %s
- Before following the procedure, source the runtime env file when the expected
  environment variables are not already exported.
- The canonical loop directory is the location for GUIDE.md, runtime files,
  and loop state. Do not look for loop-owned state in the workspace root.
- If %s is not already exported, use the canonical loop directory above.
`, markdownCode(canonicalLoopDir), markdownCode(runtimeFile), markdownCode(loopDirVar))
}

func loopEnvName(loopName string) string {
	return "MNEMON_" + strings.ToUpper(strings.ReplaceAll(loopName, "-", "_")) + "_LOOP_ENV"
}

func loopDirVarName(loopName string) string {
	return "MNEMON_" + strings.ToUpper(strings.ReplaceAll(loopName, "-", "_")) + "_LOOP_DIR"
}

func exportLine(key, value string) string {
	return fmt.Sprintf("export %s=\"%s\"", key, escapeDoubleQuoted(value))
}

func escapeDoubleQuoted(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	value = strings.ReplaceAll(value, "$", `\$`)
	value = strings.ReplaceAll(value, "`", "\\`")
	return value
}

func markdownCode(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "\\`") + "`"
}

func nowUTC() string {
	return time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
}
