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

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/manifest"
)

type ClaudeOptions struct {
	ProjectRoot string
	Loops       []string
	HostArgs    []string
	Stdout      io.Writer
	Stderr      io.Writer
}

type claudeHostOptions struct {
	global            bool
	configDir         string
	configDirExplicit bool
	storeName         string
	hostSkillsDir     string
	remind            bool
	remindSet         bool
	nudge             bool
	compact           bool
	purgeMemory       bool
	purgeLibrary      bool
	dryRun            bool
	thinRenderShim    bool
}

type claudeProjector struct {
	projectorCore
	hostOptions claudeHostOptions
}

func newClaudeProjector(opts ClaudeOptions) (claudeProjector, []string, error) {
	var err error
	if opts.ProjectRoot == "" {
		opts.ProjectRoot, err = os.Getwd()
		if err != nil {
			return claudeProjector{}, nil, fmt.Errorf("resolve project root: %w", err)
		}
	}
	projectRoot, err := filepath.Abs(opts.ProjectRoot)
	if err != nil {
		return claudeProjector{}, nil, fmt.Errorf("resolve project root: %w", err)
	}
	hostOptions, err := parseClaudeHostOptions(opts.HostArgs)
	if err != nil {
		return claudeProjector{}, nil, err
	}
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}
	if _, err := manifest.ValidateFS(assets.FS); err != nil {
		return claudeProjector{}, nil, err
	}
	loops := append([]string(nil), opts.Loops...)
	if len(loops) == 0 {
		return claudeProjector{}, nil, errors.New("at least one --loop is required")
	}
	sort.Strings(loops)
	return claudeProjector{
		projectorCore: projectorCore{
			host:        "claude-code",
			projectRoot: projectRoot,
			paths:       claudeProjectorPaths(hostOptions),
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

func RunClaudeProjector(ctx context.Context, action string, opts ClaudeOptions) error {
	if action != "install" && action != "uninstall" {
		return fmt.Errorf("unsupported Claude Code projector action: %s", action)
	}
	projector, loops, err := newClaudeProjector(opts)
	if err != nil {
		return err
	}
	for _, loopName := range loops {
		loop, binding, err := resolveLoopAndBinding("claude-code", loopName, projector.projectRoot, projector.paths.configDir)
		if err != nil {
			return err
		}
		switch action {
		case "install":
			if err := projector.installLoop(ctx, loop, binding); err != nil {
				return fmt.Errorf("install claude-code/%s: %w", loopName, err)
			}
		case "uninstall":
			if err := projector.uninstallLoop(loop, binding); err != nil {
				return fmt.Errorf("uninstall claude-code/%s: %w", loopName, err)
			}
		}
	}
	return nil
}

// RunClaudeProjectorReport installs/re-projects the Claude Code projection under the no-clobber policy
// and returns the managed files it preserved because the user edited them.
func RunClaudeProjectorReport(ctx context.Context, opts ClaudeOptions) (Report, error) {
	projector, loops, err := newClaudeProjector(opts)
	if err != nil {
		return Report{}, err
	}
	for _, loopName := range loops {
		loop, binding, err := resolveLoopAndBinding("claude-code", loopName, projector.projectRoot, projector.paths.configDir)
		if err != nil {
			return Report{}, err
		}
		if err := projector.installLoop(ctx, loop, binding); err != nil {
			return Report{}, fmt.Errorf("install claude-code/%s: %w", loopName, err)
		}
	}
	return Report{Conflicts: projector.managed.conflicts}, nil
}

func parseClaudeHostOptions(args []string) (claudeHostOptions, error) {
	parsed := claudeHostOptions{
		configDir: ".claude",
		nudge:     true,
		compact:   true,
	}
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
		case "--with-remind":
			parsed.remind = true
			parsed.remindSet = true
		case "--no-remind":
			parsed.remind = false
			parsed.remindSet = true
		case "--no-nudge":
			parsed.nudge = false
		case "--no-compact":
			parsed.compact = false
		case "--purge-memory":
			parsed.purgeMemory = true
		case "--purge-library":
			parsed.purgeLibrary = true
		case "--dry-run":
			parsed.dryRun = true
		case "--thin-render-shim":
			parsed.thinRenderShim = true
		default:
			return parsed, fmt.Errorf("unsupported Claude Code host option: %s", arg)
		}
	}
	return parsed, nil
}

func claudeProjectorPaths(opts claudeHostOptions) corePaths {
	if opts.global && !opts.configDirExplicit {
		home := os.Getenv("HOME")
		configDir := filepath.Join(home, ".claude")
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

func (p claudeProjector) installLoop(ctx context.Context, loop manifest.LoopManifest, binding manifest.BindingManifest) error {
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
	if err := p.projectSkills(loop, binding); err != nil {
		return err
	}
	if err := p.projectAgents(loop, binding); err != nil {
		return err
	}
	if err := p.projectHooks(loop, binding); err != nil {
		return err
	}
	if loop.HasHooks() {
		if err := p.patchSettings(loop); err != nil {
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
	p.printf("Installed Mnemon %s loop for Claude Code.\n", loop.Name)
	p.printf("Config:       %s\n", p.paths.configDir)
	p.printf("State:        %s\n", p.stateDir(loop.Name))
	for _, runtimeFile := range loop.Assets.RuntimeFiles {
		p.printf("Mirror:       %s\n", pathJoin(p.stateDir(loop.Name), runtimeFile))
	}
	if hostSkills := p.hostSkillsDir(loop.Name); hostSkills != "" {
		p.printf("Host skills:  %s\n", hostSkills)
	}
	return nil
}

func (p claudeProjector) uninstallLoop(loop manifest.LoopManifest, binding manifest.BindingManifest) error {
	p.beginManaged(loop.Name) // load recorded ownership so uninstall preserves user-edited/foreign skills
	if loop.HasHooks() {
		if err := p.unpatchSettings(loop.Name); err != nil {
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
	for _, subagent := range loop.Assets.Subagents {
		if err := p.removeManagedFile(pathJoin(p.paths.configDir, "agents", agentFile(loop.Name, subagent))); err != nil {
			return fmt.Errorf("remove projected agent: %w", err)
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

func (p claudeProjector) projectSkills(loop manifest.LoopManifest, binding manifest.BindingManifest) error {
	hostSkillsDir := p.hostSkillsDir(loop.Name)
	for _, skill := range loop.Assets.Skills {
		target := pathJoin(hostSkillsDir, skillID(skill), "SKILL.md")
		// canonicalSkillContent expands the payload-contract marker; skills without the
		// marker still project byte-identically to their canonical asset.
		content, err := p.canonicalSkillContent(loop, skill)
		if err != nil {
			return err
		}
		if err := p.projectManagedBytes(content, target, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func (p claudeProjector) projectAgents(loop manifest.LoopManifest, binding manifest.BindingManifest) error {
	for _, subagent := range loop.Assets.Subagents {
		target := pathJoin(binding.ProjectionPath, "agents", agentFile(loop.Name, subagent))
		if err := p.projectManaged(p.loopAsset(loop, subagent), target, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func (p claudeProjector) patchSettings(loop manifest.LoopManifest) error {
	if p.dryRun {
		p.printf("would patch %s\n", pathJoin(p.paths.configDir, "settings.json"))
		return nil
	}
	return patchClaudeSettings(p.resolve(pathJoin(p.paths.configDir, "settings.json")), p.paths.configDir, "mnemon-"+loop.Name, p.hookOptions(loop))
}

func (p claudeProjector) unpatchSettings(loopName string) error {
	return unpatchClaudeSettings(p.resolve(pathJoin(p.paths.configDir, "settings.json")), "mnemon-"+loopName)
}

// hookOptions takes Remind from the loop's DECLARATION (PD4 — no loop.Name default), which the
// operator --remind still overrides; Nudge/Compact stay claude operator-flag-driven (preserving
// claude's semantics, where the declared nudge/compact are codex's concern).
func (p claudeProjector) hookOptions(loop manifest.LoopManifest) claudeHookOptions {
	remind := false
	if loop.HookOptions != nil {
		remind = loop.HookOptions.Remind
	}
	if p.hostOptions.remindSet {
		remind = p.hostOptions.remind
	}
	return claudeHookOptions{
		Remind:  remind,
		Nudge:   p.hostOptions.nudge,
		Compact: p.hostOptions.compact,
	}
}

func (p claudeProjector) writeHostManifest(loop manifest.LoopManifest, binding manifest.BindingManifest, ownership projectionOwnership) error {
	manifestPath := p.resolve(p.hostManifestPath())
	manifest := hostProjectionManifest{
		SchemaVersion: 2,
		Host:          "claude-code",
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
	manifest.Host = "claude-code"
	manifest.UpdatedAt = nowUTC()
	manifest.ProjectRoot = p.projectRoot
	manifest.MnemonDir = p.paths.mnemonDir
	if p.hostOptions.storeName != "" {
		manifest.Store = p.hostOptions.storeName
	} else {
		manifest.Store = "default"
	}
	manifest.Loops[loop.Name] = hostManifestLoop{
		LoopPath:     p.stateDir(loop.Name),
		LoopVersion:  loop.Version,
		StatePath:    p.stateDir(loop.Name),
		IntentPolicy: pathJoin(p.stateDir(loop.Name), "GUIDE.md"),
		StatusPath:   pathJoin(p.stateDir(loop.Name), "status.json"),
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
		Surfaces: map[string]string{
			"skills":  p.hostSkillsDir(loop.Name),
			"runtime": binding.RuntimeSurface,
		},
		Ownership: ownership,
	}
	return p.writeJSON(p.hostManifestPath(), manifest, 0o644)
}

func (p claudeProjector) loopOwnership(loop manifest.LoopManifest, binding manifest.BindingManifest) projectionOwnership {
	files := []string{
		pathJoin(p.stateDir(loop.Name), "GUIDE.md"),
		pathJoin(p.stateDir(loop.Name), "env.sh"),
		pathJoin(p.stateDir(loop.Name), "loop.json"),
		pathJoin(p.stateDir(loop.Name), "status.json"),
		pathJoin(binding.RuntimeSurface, "env.sh"),
		pathJoin(binding.RuntimeSurface, "GUIDE.md"),
	}
	if loop.HasHooks() {
		files = append(files, pathJoin(binding.ProjectionPath, "settings.json"))
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
	for _, subagent := range loop.Assets.Subagents {
		files = append(files, pathJoin(binding.ProjectionPath, "agents", agentFile(loop.Name, subagent)))
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
	sort.Strings(files)
	return projectionOwnership{
		Files: files,
		Dirs:  []string{binding.RuntimeSurface, pathJoin(binding.ProjectionPath, "hooks", "mnemon-"+loop.Name)},
	}
}
