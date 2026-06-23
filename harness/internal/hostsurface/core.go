package hostsurface

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/manifest"
)

// corePaths is the host config dir + the project-local mnemon state dir.
type corePaths struct {
	configDir string
	mnemonDir string
}

// projectorCore is host-io logic shared by each backend (codex, claude-code,
// ...): path resolution, file writes, manifest paths, and common helpers. It is
// composition, not a frozen host adapter interface; each concrete projector adds
// only its host-specific surfaces.
type projectorCore struct {
	host        string // "codex" | "claude-code"
	projectRoot string
	paths       corePaths
	// shared host options (identical across hosts; set by each option parser)
	skillsDirOverride string // --host-skills-dir
	purgeMemory       bool   // --purge-memory
	purgeLibrary      bool   // --purge-library
	dryRun            bool   // --dry-run: report would-write/would-preserve, write nothing
	thinRenderShim    bool   // --thin-render-shim: install static render hook mechanics
	stdout            io.Writer
	stderr            io.Writer
	managed           *managedState // no-clobber projection state for managed definition files
	// assetFS resolves a loop's projected assets: the embedded asset FS overlaid with external
	// packages under .mnemon/loops, so a read of "loops/<name>/<rel>" serves the embedded asset
	// when present, else the external package's file (PD4 AssetRoot). Host mechanics ("hosts/...")
	// and hook fragments are NEVER served externally — those reads stay on assets.FS directly. Nil
	// (zero-value test projectors) falls back to the embedded assets.FS.
	assetFS fs.FS
}

// loopAssetOverlay overlays the embedded asset FS with external loop packages. A read of
// "loops/<name>/<rel>" tries embedded first (first-party loops), then the external package's
// "<name>/<rel>" under .mnemon/loops. Any other path (hosts/..., fragments) resolves embedded-only.
type loopAssetOverlay struct {
	embedded fs.FS
	external fs.FS // os.DirFS(projectRoot/.mnemon/loops); reads fail closed when the dir is absent
}

func (o loopAssetOverlay) Open(name string) (fs.File, error) {
	f, err := o.embedded.Open(name)
	if err == nil {
		return f, nil
	}
	if o.external != nil {
		if rest, ok := strings.CutPrefix(name, "loops/"); ok {
			return o.external.Open(rest)
		}
	}
	return f, err
}

func newLoopAssetOverlay(projectRoot string) fs.FS {
	return loopAssetOverlay{embedded: assets.FS, external: os.DirFS(filepath.Join(projectRoot, ".mnemon", "loops"))}
}

// assets resolves the loop-asset FS (overlay), falling back to the embedded assets.FS for
// zero-value projectors used in unit tests.
func (c projectorCore) assets() fs.FS {
	if c.assetFS != nil {
		return c.assetFS
	}
	return assets.FS
}

// resolveLoopAndBinding loads a loop's manifest (embedded or external, via the overlay) and its
// projection binding: a first-party loop ships an embedded binding; an EXTERNAL package ships none,
// so the binding is derived host-side (PD4). The asset-carrying external package thus projects with
// the same machinery as a builtin.
func resolveLoopAndBinding(host, loopName, projectRoot, configDir string) (manifest.LoopManifest, manifest.BindingManifest, error) {
	overlay := newLoopAssetOverlay(projectRoot)
	loop, err := manifest.LoadLoop(overlay, loopName)
	if err != nil {
		return manifest.LoopManifest{}, manifest.BindingManifest{}, err
	}
	// External package (no embedded loop.json): screen its host assets at load (the external-trust
	// rules — reviewed first-party loops skip this).
	if _, serr := fs.Stat(assets.FS, "loops/"+loopName+"/loop.json"); errors.Is(serr, fs.ErrNotExist) {
		if verr := validateExternalLoopAssets(overlay, loop); verr != nil {
			return manifest.LoopManifest{}, manifest.BindingManifest{}, verr
		}
	}
	binding, err := manifest.LoadBinding(assets.FS, host, loopName)
	if errors.Is(err, fs.ErrNotExist) {
		binding, err = deriveBinding(host, loopName, configDir)
	}
	if err != nil {
		return manifest.LoopManifest{}, manifest.BindingManifest{}, err
	}
	return loop, binding, nil
}

// validateExternalLoopAssets enforces loop-package-v2's external-trust rules on an external package's
// host assets at load (PD3/PD4, red-team MUST): the package may carry intents/skills, but the code
// faces stay closed —
//   - an `include` intent section is rejected (hook fragments are embedded-only);
//   - a control-observe action's event_type must be the shared bootstrap (session.observed) or the
//     package's OWN <kind>.* family — never another loop's family (the confused-deputy guard); and
//   - a skill template's external_id_recipe is rejected (a shell recipe spliced into a bash fence).
func validateExternalLoopAssets(fsys fs.FS, loop manifest.LoopManifest) error {
	pkg := ".mnemon/loops/" + loop.Name
	ownFamily := loop.Name + "."
	if raw, err := fs.ReadFile(fsys, "loops/"+loop.Name+"/hooks/intents.json"); err == nil {
		intents, derr := decodeHookIntents(raw)
		if derr != nil {
			return fmt.Errorf("external package %s: parse hooks/intents.json: %w", pkg, derr)
		}
		for timing, ti := range intents.Hooks {
			for _, section := range ti.Sections {
				if section.Type == sectionInclude {
					return fmt.Errorf("external package %s: hook intents (%s) declare an `include` section; fragments are embedded-only (fail-closed)", pkg, timing)
				}
				for _, action := range section.Actions {
					if action.Type == actionObserve && action.EventType != "session.observed" && !strings.HasPrefix(action.EventType, ownFamily) {
						return fmt.Errorf("external package %s: hook observe event_type %q must be session.observed or the package's own %s* family (confused-deputy; fail-closed)", pkg, action.EventType, ownFamily)
					}
				}
			}
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("external package %s: read hooks/intents.json: %w", pkg, err)
	}
	for _, skill := range loop.Assets.Skills {
		tmplPath := "loops/" + loop.Name + "/" + path.Dir(skill) + "/template.json"
		raw, err := fs.ReadFile(fsys, tmplPath)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("external package %s: read %s: %w", pkg, tmplPath, err)
		}
		tmpl, derr := decodeSkillTemplate(raw)
		if derr != nil {
			return fmt.Errorf("external package %s: parse %s: %w", pkg, tmplPath, derr)
		}
		if strings.TrimSpace(tmpl.ExternalIDRecipe) != "" {
			return fmt.Errorf("external package %s: skill template %s declares external_id_recipe (a shell recipe); embedded-only (fail-closed)", pkg, tmplPath)
		}
	}
	return nil
}

// deriveBinding builds a host-side projection binding for an external loop package (which ships no
// binding): projection path = host config dir, runtime surface = <config>/mnemon-<loop>, lifecycle
// mapping copied from the host's host.json (the four hook timings), reconcile empty (manifest
// metadata only). Host mechanics come from the embedded host.json — never the external package.
func deriveBinding(host, loopName, configDir string) (manifest.BindingManifest, error) {
	raw, err := fs.ReadFile(assets.FS, "hosts/"+host+"/host.json")
	if err != nil {
		return manifest.BindingManifest{}, fmt.Errorf("derive binding: read host %s mechanics: %w", host, err)
	}
	var hostDoc struct {
		LifecycleMapping map[string]string `json:"lifecycle_mapping"`
	}
	if err := json.Unmarshal(raw, &hostDoc); err != nil {
		return manifest.BindingManifest{}, fmt.Errorf("derive binding: parse host %s: %w", host, err)
	}
	lifecycle := map[string]string{}
	for _, timing := range hookTimings {
		if ev, ok := hostDoc.LifecycleMapping[timing]; ok {
			lifecycle[timing] = ev
		}
	}
	return manifest.BindingManifest{
		SchemaVersion:    1,
		Name:             host + "." + loopName,
		Host:             host,
		Loop:             loopName,
		ProjectionPath:   configDir,
		RuntimeSurface:   pathJoin(configDir, "mnemon-"+loopName),
		LifecycleMapping: lifecycle,
		Reconcile:        []string{},
	}, nil
}

// pathJoin is the package's display-path primitive: forward-slash joins for the host
// surface (.codex/.claude) regardless of OS, so projected refs read identically on
// every platform. It lives with projectorCore (the host-io core) rather than a
// backend file because every backend joins paths through it.
func pathJoin(base string, elems ...string) string {
	parts := append([]string{base}, elems...)
	return path.Join(parts...)
}

func (c projectorCore) resolve(displayPath string) string {
	if filepath.IsAbs(displayPath) {
		return filepath.Clean(displayPath)
	}
	return filepath.Join(c.projectRoot, filepath.FromSlash(displayPath))
}

func (c projectorCore) exists(displayPath string) bool {
	_, err := os.Stat(c.resolve(displayPath))
	return err == nil
}

// copyFile reads src from the embedded asset FS (a forward-slash key like "loops/<loop>/GUIDE.md")
// and writes it to the on-disk host surface at dstDisplay.
func (c projectorCore) copyFile(src, dstDisplay string, mode os.FileMode) error {
	data, err := fs.ReadFile(c.assets(), src)
	if err != nil {
		return fmt.Errorf("read %s: %w", src, err)
	}
	return c.writeFile(dstDisplay, data, mode)
}

func (c projectorCore) copyFileIfMissing(src, dstDisplay string, mode os.FileMode) error {
	if _, err := os.Stat(c.resolve(dstDisplay)); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat %s: %w", dstDisplay, err)
	}
	return c.copyFile(src, dstDisplay, mode)
}

func (c projectorCore) writeFile(dstDisplay string, data []byte, mode os.FileMode) error {
	if c.dryRun {
		c.printf("would write %s\n", dstDisplay)
		return nil
	}
	dst := c.resolve(dstDisplay)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", filepath.Dir(dst), err)
	}
	if err := os.WriteFile(dst, data, mode); err != nil {
		return fmt.Errorf("write %s: %w", dstDisplay, err)
	}
	if err := os.Chmod(dst, mode); err != nil {
		return fmt.Errorf("chmod %s: %w", dstDisplay, err)
	}
	return nil
}

func (c projectorCore) writeJSON(dstDisplay string, value any, mode os.FileMode) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", dstDisplay, err)
	}
	data = append(data, '\n')
	return c.writeFile(dstDisplay, data, mode)
}

func (c projectorCore) printf(format string, args ...any) {
	fmt.Fprintf(c.stdout, format, args...)
}

func (c projectorCore) stateDir(loopName string) string {
	return pathJoin(c.paths.mnemonDir, "harness", loopName)
}

func (c projectorCore) hostManifestPath() string {
	return pathJoin(c.paths.mnemonDir, "hosts", c.host, "manifest.json")
}

// loopAsset returns the embedded-FS key (forward slashes) for a loop's projected asset.
func (c projectorCore) loopAsset(loop manifest.LoopManifest, rel string) string {
	return path.Join("loops", loop.Name, rel)
}

func (c projectorCore) readExportValue(displayPath, key string) (string, bool) {
	data, err := os.ReadFile(c.resolve(displayPath))
	if err != nil {
		return "", false
	}
	prefix := "export " + key + "="
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		value := strings.TrimPrefix(line, prefix)
		value = strings.Trim(value, `"`)
		return value, true
	}
	return "", false
}

func (c projectorCore) removeCommonStateFiles(stateDir string) error {
	for _, name := range []string{"GUIDE.md", "env.sh", "loop.json", "status.json"} {
		if err := os.Remove(c.resolve(pathJoin(stateDir, name))); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove %s: %w", name, err)
		}
	}
	_ = os.Remove(c.resolve(stateDir))
	return nil
}

func (c projectorCore) removeHostManifestLoop(loopName string) error {
	manifestPath := c.resolve(c.hostManifestPath())
	data, err := os.ReadFile(manifestPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read host manifest %s: %w", c.hostManifestPath(), err)
	}
	var manifest hostProjectionManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return fmt.Errorf("parse host manifest %s: %w", c.hostManifestPath(), err)
	}
	delete(manifest.Loops, loopName)
	if len(manifest.Loops) == 0 {
		if err := os.Remove(manifestPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove host manifest: %w", err)
		}
		return nil
	}
	manifest.UpdatedAt = nowUTC()
	return c.writeJSON(c.hostManifestPath(), manifest, 0o644)
}

// hostHookExists answers from the loop's declared intents. The error path collapses to false by
// signature; that only affects uninstall bookkeeping (Ownership.Files) — an invalid intents file
// cannot reach an installed workspace, because loop validate and projectHooks both fail closed on
// it first.
func (c projectorCore) hostHookExists(loopName, phase string) bool {
	timings, err := DeclaredHookTimings(c.assets(), loopName)
	if err != nil {
		return false
	}
	for _, t := range timings {
		if t == phase {
			return true
		}
	}
	return false
}

func skillID(skillPath string) string {
	dir := path.Dir(skillPath)
	if dir == "." || dir == "/" {
		return strings.TrimSuffix(path.Base(skillPath), path.Ext(skillPath))
	}
	return path.Base(dir)
}

func agentFile(loopName, subagentPath string) string {
	base := strings.TrimSuffix(path.Base(subagentPath), path.Ext(subagentPath))
	switch loopName + "." + base {
	case "skill.curator":
		return "mnemon-skill-curator.md"
	default:
		return "mnemon-" + base + ".md"
	}
}

// ---- methods shared verbatim by every host projector (hoisted from the per-host
// adapters; the displayJoin/pathJoin split was cosmetic — displayJoin called pathJoin) ----

func (p projectorCore) copyCommonCanonicalAssets(loop manifest.LoopManifest) error {
	for _, asset := range []struct {
		rel  string
		name string
		mode os.FileMode
	}{
		{rel: loop.Assets.Guide, name: "GUIDE.md", mode: 0o644},
		{rel: loop.Assets.Env, name: "env.sh", mode: 0o755},
		{rel: "loop.json", name: "loop.json", mode: 0o644},
	} {
		if err := p.copyFile(p.loopAsset(loop, asset.rel), pathJoin(p.stateDir(loop.Name), asset.name), asset.mode); err != nil {
			return err
		}
	}
	return nil
}

// prepareLoopState seeds the loop's state dir from its DECLARATIONS (PD4 — no loop-name switch):
// each declared runtime_file is copied in if missing (the mirror seed), and each declared state_dir
// is created. memory declares MEMORY.md; skill declares its skills/proposals/reports dirs.
func (p projectorCore) prepareLoopState(loop manifest.LoopManifest) error {
	for _, runtimeFile := range loop.Assets.RuntimeFiles {
		if err := p.copyFileIfMissing(p.loopAsset(loop, runtimeFile), pathJoin(p.stateDir(loop.Name), runtimeFile), 0o644); err != nil {
			return err
		}
	}
	for _, dir := range loop.StateDirs {
		if p.dryRun {
			continue
		}
		if err := os.MkdirAll(p.resolve(pathJoin(p.stateDir(loop.Name), dir)), 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}
	return nil
}

func (p projectorCore) hostSkillsDir(loopName string) string {
	if p.skillsDirOverride != "" && loopName != "memory" {
		return filepath.ToSlash(p.skillsDirOverride)
	}
	return pathJoin(p.paths.configDir, "skills")
}

func (p projectorCore) installedHostSkillsDir(loopName string, binding manifest.BindingManifest) string {
	envPath := pathJoin(binding.RuntimeSurface, "env.sh")
	envVar := "MNEMON_" + strings.ToUpper(strings.ReplaceAll(loopName, "-", "_")) + "_LOOP_HOST_SKILLS_DIR"
	if value, ok := p.readExportValue(envPath, envVar); ok {
		return value
	}
	return p.hostSkillsDir(loopName)
}

func (p projectorCore) ensureStore(ctx context.Context, storeName string) error {
	if p.dryRun {
		p.printf("would ensure mnemon store %q\n", storeName)
		return nil
	}
	mnemon, err := exec.LookPath("mnemon")
	if err != nil {
		return fmt.Errorf("mnemon binary not found in PATH; build or install it before setting a %s memory store", p.host)
	}
	list := exec.CommandContext(ctx, mnemon, "store", "list")
	list.Dir = p.projectRoot
	list.Stderr = p.stderr
	output, err := list.Output()
	if err != nil {
		return fmt.Errorf("mnemon store list: %w", err)
	}
	if !storeListContains(output, storeName) {
		create := exec.CommandContext(ctx, mnemon, "store", "create", storeName)
		create.Dir = p.projectRoot
		create.Stdout = io.Discard
		create.Stderr = p.stderr
		if err := create.Run(); err != nil {
			return fmt.Errorf("mnemon store create %s: %w", storeName, err)
		}
	}
	set := exec.CommandContext(ctx, mnemon, "store", "set", storeName)
	set.Dir = p.projectRoot
	set.Stdout = io.Discard
	set.Stderr = p.stderr
	if err := set.Run(); err != nil {
		return fmt.Errorf("mnemon store set %s: %w", storeName, err)
	}
	return nil
}

// projectHooks installs the GENERATED hook shells for every timing the loop's intents declare.
// Render errors fail the install closed — a half-migrated loop must never silently install with
// zero hooks (the legacy code skipped absent asset files, which would have masked exactly that).
func (p projectorCore) projectHooks(loop manifest.LoopManifest, binding manifest.BindingManifest) error {
	timings, err := DeclaredHookTimings(p.assets(), loop.Name)
	if err != nil {
		return fmt.Errorf("hook intents for %s: %w", loop.Name, err)
	}
	if len(timings) == 0 && hasHookIntents(p.assets(), loop.Name) {
		return fmt.Errorf("loop %s declares hook intents but renders zero hook timings: refusing to install zero hooks", loop.Name)
	}
	for _, phase := range timings {
		content, err := p.renderHook(loop.Name, phase)
		if err != nil {
			return fmt.Errorf("render hook %s/%s for %s: %w", loop.Name, phase, p.host, err)
		}
		target := pathJoin(binding.ProjectionPath, "hooks", "mnemon-"+loop.Name, phase+".sh")
		if err := p.projectManagedBytes([]byte(content), target, 0o755); err != nil {
			return err
		}
	}
	return nil
}

func (p projectorCore) renderHook(loopName, phase string) (string, error) {
	if p.thinRenderShim {
		return RenderStandardThinHook(p.host, phase)
	}
	return RenderHook(p.assets(), loopName, p.host, phase)
}

func (p projectorCore) removeCanonicalState(loop manifest.LoopManifest) error {
	stateDir := p.stateDir(loop.Name)
	switch loop.Name {
	case "memory":
		if p.purgeMemory {
			return os.RemoveAll(p.resolve(stateDir))
		}
		return p.removeCommonStateFiles(stateDir)
	case "skill":
		if p.purgeLibrary {
			return os.RemoveAll(p.resolve(stateDir))
		}
		if err := p.removeCommonStateFiles(stateDir); err != nil {
			return err
		}
		for _, dir := range []string{"reports", "proposals"} {
			_ = os.Remove(p.resolve(pathJoin(stateDir, dir)))
		}
		_ = os.Remove(p.resolve(stateDir))
	default:
		return p.removeCommonStateFiles(stateDir)
	}
	return nil
}

func (p projectorCore) writeLoopStatus(loop manifest.LoopManifest, binding manifest.BindingManifest) error {
	status := map[string]any{
		"schema_version":  2,
		"loop":            loop.Name,
		"host":            p.host,
		"phase":           "projected",
		"updated_at":      nowUTC(),
		"project_root":    p.projectRoot,
		"projection_path": p.paths.configDir,
		"state_path":      p.stateDir(loop.Name),
		"surfaces":        loop.Surfaces,
	}
	return p.writeJSON(pathJoin(p.stateDir(loop.Name), "status.json"), status, 0o644)
}

// runtimeEnvContent renders the loop's runtime env.sh from its DECLARATIONS (PD4 — no loop.Name
// switch): the structural base (env-file + loop-dir vars) plus each declared env var, with the
// closed projector variables (${state_dir}, ${host_skills_dir}) substituted to real paths and the
// runtime ${VAR:-default} refs passed through for bash. Values are validated at load (the env
// injection lock), so they splice safely into this sourced file.
func (p projectorCore) runtimeEnvContent(loop manifest.LoopManifest, binding manifest.BindingManifest) []byte {
	envName := loopEnvName(loop.Name)
	loopDirVar := loopDirVarName(loop.Name)
	stateDir := p.stateDir(loop.Name)
	lines := []string{
		"#!/usr/bin/env bash",
		exportLine(envName, pathJoin(stateDir, "env.sh")),
		exportLine(loopDirVar, stateDir),
	}
	// Escape the projector-var PATH substitutions (matching the original exportLine behavior for
	// these values); the declared literals and runtime ${VAR:-default} refs are validated safe and
	// pass through raw so bash still expands them.
	projVars := map[string]string{
		"state_dir":       escapeDoubleQuoted(stateDir),
		"host_skills_dir": escapeDoubleQuoted(p.hostSkillsDir(loop.Name)),
	}
	for _, e := range loop.Env {
		lines = append(lines, `export `+e.Name+`="`+manifest.SubstituteEnvValue(e.Value, projVars)+`"`)
	}
	content := strings.Join(lines, "\n") + "\n"
	return []byte(content)
}

func (p projectorCore) writeRuntimeEnv(loop manifest.LoopManifest, binding manifest.BindingManifest) error {
	// Route through projectManaged so env.sh is hash-recorded: a pre-existing/edited one is preserved
	// on install and on uninstall, like every other managed runtime-surface file.
	return p.projectManagedBytes(p.runtimeEnvContent(loop, binding), pathJoin(binding.RuntimeSurface, "env.sh"), 0o755)
}

// removeGeneratedSkillViews removes the host skill-view dirs the skill prime generated (marked by
// .mnemon-skill-generated), leaving any user-authored host skill untouched. It is host-agnostic (both
// hosts' skill primes write the same marker), so it lives on projectorCore.
func (c projectorCore) removeGeneratedSkillViews(hostSkillsDir string) error {
	entries, err := os.ReadDir(c.resolve(hostSkillsDir))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read host skills dir: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		skillDir := pathJoin(hostSkillsDir, entry.Name())
		marker := pathJoin(skillDir, ".mnemon-skill-generated")
		if _, err := os.Stat(c.resolve(marker)); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return fmt.Errorf("stat generated skill marker: %w", err)
		}
		if err := os.RemoveAll(c.resolve(skillDir)); err != nil {
			return fmt.Errorf("remove generated skill view: %w", err)
		}
	}
	return nil
}

func storeListContains(output []byte, storeName string) bool {
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		line = strings.TrimLeft(line, "* ")
		if strings.TrimSpace(line) == storeName {
			return true
		}
	}
	return false
}
