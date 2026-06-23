package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/capability"
	"github.com/mnemon-dev/mnemon/harness/internal/channel"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/manifest"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

// SetupOptions configures the `mnemon-harness setup` front door: project a loop into a host runtime
// AND wire the channel (binding entry + optional token + runtime env), so a host agent reaches the
// governed control plane through one channel.
type SetupOptions struct {
	Host           string   // host runtime id, e.g. "codex"
	Loops          []string // loops to project, e.g. ["memory"]
	ControlURL     string   // channel endpoint, e.g. "http://127.0.0.1:8787"
	Principal      string   // authenticated principal, e.g. "codex@project"
	ActorKind      string   // "host-agent" (default) or "control-agent"
	UseToken       bool     // generate + reference a bearer token file (vs trusted-header auth)
	TokenExplicit  bool     // true when the caller explicitly set UseToken
	ProjectRoot    string   // host projection working dir (defaults to the facade root)
	DryRun         bool     // print all projection + channel changes without writing
	ThinRenderShim bool     // install R1 static render hooks on the setup path
}

// SetupResult records the channel artifact paths setup wrote (or would write, on dry-run).
type SetupResult struct {
	BindingFile string
	TokenFile   string
	EnvFile     string
	ConfigFile  string
	Changes     []string
}

func channelBase(projectRoot string) string {
	return filepath.Join(projectRoot, ".mnemon", "harness", "channel")
}

func localBase(projectRoot string) string {
	return filepath.Join(projectRoot, ".mnemon", "harness", "local")
}

func sanitizePrincipal(p string) string {
	return strings.NewReplacer("@", "-", "/", "-", ":", "-").Replace(p)
}

// validateProductLoops fail-closes setup to loops that are BOTH a built-in capability
// (capability.EmbeddedCatalog()) AND carry projectable assets for the host (manifest.LoopsForHost over the
// embedded FS) — derived, not hardcoded, so a future loop whose assets land is admitted without
// editing a literal. Today the intersection is exactly {memory, skill} (the whole builtin set
// since the P1 note/decision demotion to external-package fixtures).
// A requested loop that is instead an EXTERNAL capability package under projectRoot gets the
// pinned admission-vs-projection diagnosis: external packages carry no host assets in v1.
func validateProductLoops(host string, loops []string, projectRoot string) error {
	hostLoops, err := manifest.LoopsForHost(assets.FS, host)
	if err != nil {
		return fmt.Errorf("setup: discover %s loops: %w", host, err)
	}
	available := map[string]bool{}
	var names []string
	for _, loop := range hostLoops {
		if _, ok := capability.EmbeddedCatalog()[loop]; ok && !available[loop] {
			available[loop] = true
			names = append(names, loop)
		}
	}
	sort.Strings(names)
	for _, loop := range loops {
		loop = strings.TrimSpace(loop)
		if loop == "" {
			return fmt.Errorf("setup loop id cannot be empty")
		}
		if !available[loop] {
			if isExternalPackage(projectRoot, loop) {
				// loop-package-v2 (PD4): an external package that ships a loop.json declares host
				// assets and projects through the same machinery as a builtin; one carrying only a
				// capability.json (admission-equal, no host assets) is still refused.
				if hasExternalLoopManifest(projectRoot, loop) {
					continue
				}
				return fmt.Errorf("loop %q: external package declares no host assets (no loop.json); enable via config.loops + binding", loop)
			}
			return fmt.Errorf("unsupported product loop %q for host %s; available: %s", loop, host, strings.Join(names, ", "))
		}
	}
	return nil
}

// isExternalPackage reports whether loop names an external capability package under the project
// root. Presence check only: setup never LOADS external packages — they carry no host assets, so
// there is nothing for setup to project.
func isExternalPackage(projectRoot, loop string) bool {
	fi, err := os.Stat(filepath.Join(projectRoot, ".mnemon", "loops", loop, "capability.json"))
	return err == nil && fi.Mode().IsRegular()
}

// hasExternalLoopManifest reports whether an external package ships a loop.json — the signal that it
// carries host projection assets (loop-package-v2). Presence check only; the projector validates the
// manifest at load.
func hasExternalLoopManifest(projectRoot, loop string) bool {
	fi, err := os.Stat(filepath.Join(projectRoot, ".mnemon", "loops", loop, "loop.json"))
	return err == nil && fi.Mode().IsRegular()
}

// Setup projects the selected loops into the host and writes the Local Mnemon
// channel artifacts. On DryRun it prints every projection + channel change
// without writing.
func (h *Harness) Setup(ctx context.Context, out, errw io.Writer, opts SetupOptions) (SetupResult, error) {
	opts = h.defaultSetupOptions(opts)
	if opts.Host == "" {
		return SetupResult{}, fmt.Errorf("setup requires --host")
	}
	// No --loop is valid (P3): the coordination package (project_intent/assignment/progress_digest)
	// is default-enabled at boot, so `setup --host codex` alone wires a host that can govern the
	// AgentTeam nouns out of the box. --loop adds the optional packages (memory/skill) on top.
	if err := validateProductLoops(opts.Host, opts.Loops, opts.ProjectRoot); err != nil {
		return SetupResult{}, err
	}
	projectRoot := opts.ProjectRoot

	// 1. Project loop assets. Dry-run lowers to the projector's own --dry-run so projection changes
	//    print without writing. Skipped when no --loop is named (P3): the default-enabled coordination
	//    package is governance-only — there are no host assets to project — and step 2 still wires the
	//    channel so the host can govern the coordination kinds.
	if len(opts.Loops) > 0 {
		action, hostArgs := "install", []string(nil)
		if opts.DryRun {
			hostArgs = []string{"--dry-run"}
		}
		if opts.ThinRenderShim {
			hostArgs = append(hostArgs, "--thin-render-shim")
		}
		var projectorOut bytes.Buffer
		if err := h.LoopProject(ctx, &projectorOut, errw, action, projectRoot, opts.Host, opts.Loops, hostArgs); err != nil {
			return SetupResult{}, fmt.Errorf("setup: project loop assets: %w", err)
		}
	}

	// 2. Channel artifacts.
	base := channelBase(projectRoot)
	defer tightenHarnessDirs(projectRoot) // 重跑校正:即使目录先以宽权限存在(如 local run 先行)
	bindingFile := filepath.Join(base, "bindings.json")
	envFile := filepath.Join(localBase(projectRoot), "env.sh")
	configFile := filepath.Join(localBase(projectRoot), "config.json")
	compatEnvFile := filepath.Join(base, "env.sh")
	tokenRel := ""
	tokenFile := ""
	if opts.UseToken {
		tokenRel = filepath.ToSlash(filepath.Join(".mnemon", "harness", "channel", "credentials", sanitizePrincipal(opts.Principal)+".token"))
		tokenFile = filepath.Join(projectRoot, filepath.FromSlash(tokenRel))
	}

	binding := h.channelBinding(opts)
	res := SetupResult{BindingFile: bindingFile, TokenFile: tokenFile, EnvFile: envFile, ConfigFile: configFile}

	if opts.DryRun {
		res.Changes = append(res.Changes,
			fmt.Sprintf("would upsert channel binding for %s in %s", opts.Principal, bindingFile),
			fmt.Sprintf("would write Local Mnemon config %s", configFile),
			fmt.Sprintf("would write Local Mnemon env %s", envFile),
			fmt.Sprintf("would write compatibility env %s", compatEnvFile))
		if opts.UseToken {
			res.Changes = append(res.Changes, fmt.Sprintf("would write bearer token file %s", tokenFile))
		}
		writeSetupSummary(out, opts, true)
		return res, nil
	}

	if opts.UseToken {
		if err := writeTokenFile(tokenFile); err != nil {
			return res, err
		}
		res.Changes = append(res.Changes, "wrote bearer token file "+tokenFile)
	}
	if err := channel.MergeBinding(bindingFile, binding, tokenRel); err != nil {
		return res, fmt.Errorf("setup: merge binding: %w", err)
	}
	res.Changes = append(res.Changes, "upserted channel binding for "+opts.Principal+" in "+bindingFile)
	// Config + env reflect ALL enabled loops (the union with any prior setup), so installing skill
	// after memory leaves both the config AND the env naming both loops (additive, symmetric).
	effectiveLoops := unionLoops(existingConfigLoops(configFile), opts.Loops)
	if err := writeLocalConfig(configFile, opts, effectiveLoops); err != nil {
		return res, err
	}
	res.Changes = append(res.Changes, "wrote Local Mnemon config "+configFile)
	if err := writeLocalEnv(envFile, opts, tokenRel, effectiveLoops); err != nil {
		return res, err
	}
	res.Changes = append(res.Changes, "wrote Local Mnemon env "+envFile)
	if err := writeLocalEnv(compatEnvFile, opts, tokenRel, effectiveLoops); err != nil {
		return res, err
	}
	res.Changes = append(res.Changes, "wrote compatibility env "+compatEnvFile)
	writeSetupSummary(out, opts, false)
	return res, nil
}

func (h *Harness) defaultSetupOptions(opts SetupOptions) SetupOptions {
	opts.Host = strings.TrimSpace(opts.Host)
	if opts.ProjectRoot == "" {
		opts.ProjectRoot = h.root
	}
	if opts.Principal == "" && opts.Host != "" {
		opts.Principal = opts.Host + "@project"
	}
	if opts.ControlURL == "" {
		opts.ControlURL = "http://127.0.0.1:8787"
	}
	if opts.ActorKind == "" {
		opts.ActorKind = string(contract.KindHostAgent)
	}
	opts.ThinRenderShim = true
	if !opts.TokenExplicit {
		opts.UseToken = true
	}
	return opts
}

func writeSetupSummary(out io.Writer, opts SetupOptions, dryRun bool) {
	action := "installed"
	local := "ready"
	if dryRun {
		action = "dry-run install"
		local = "would be ready"
	}
	fmt.Fprintf(out, "Agent Integration: %s for %s (%s)\n", action, displayHost(opts.Host), strings.Join(opts.Loops, ", "))
	fmt.Fprintf(out, "Local Mnemon: %s\n", local)
	fmt.Fprintln(out, "Remote Workspace: not connected")
}

func displayHost(host string) string {
	switch host {
	case "codex":
		return "Codex"
	case "claude-code":
		return "Claude Code"
	default:
		return host
	}
}

func (h *Harness) channelBinding(opts SetupOptions) channel.ChannelBinding {
	kind := contract.KindHostAgent
	if opts.ActorKind == string(contract.KindControlAgent) {
		kind = contract.KindControlAgent
	}
	observed := []string{"session.observed"}
	var scope []contract.ResourceRef
	for _, loop := range opts.Loops {
		observed = append(observed, loop+".write_candidate.observed")
		scope = append(scope, contract.ResourceRef{Kind: contract.ResourceKind(loop), ID: "project"})
	}
	return channel.ChannelBinding{
		Principal:            contract.ActorID(opts.Principal),
		ActorKind:            kind,
		Transport:            channel.TransportHTTP,
		Endpoint:             opts.ControlURL,
		AllowedVerbs:         []channel.Verb{channel.VerbObserve, channel.VerbPull, channel.VerbRender, channel.VerbStatus},
		AllowedObservedTypes: observed,
		SubscriptionScope:    scope,
		IdempotencyNamespace: "host:" + opts.Principal,
	}
}

func writeTokenFile(path string) error {
	// Idempotent: keep an existing token so a running Local Mnemon (which holds it in memory) does not
	// get locked out by a rerun rotating it.
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Errorf("generate token: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(hex.EncodeToString(buf)+"\n"), 0o600)
}

// existingConfigLoops returns the loops recorded in an existing local config (nil if absent), so a
// rerun can union them with the loops being installed.
func existingConfigLoops(path string) []string {
	prev, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var existing struct {
		Loops []string `json:"loops"`
	}
	if json.Unmarshal(prev, &existing) != nil {
		return nil
	}
	return existing.Loops
}

// existingConfigHosts returns the per-host installed-loops map from an existing local config (nil
// if absent), so a rerun — possibly for another host — merges rather than clobbers.
func existingConfigHosts(path string) map[string][]string {
	prev, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var existing struct {
		Hosts map[string][]string `json:"hosts"`
	}
	if json.Unmarshal(prev, &existing) != nil {
		return nil
	}
	return existing.Hosts
}

// existingConfigMirrorMode preserves a user-chosen mirror_mode across setup reruns (setup has no
// flag for it; clobbering a hand-edited "manual" back to the default would be a silent override).
func existingConfigMirrorMode(path string) string {
	prev, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	var existing struct {
		MirrorMode string `json:"mirror_mode"`
	}
	if json.Unmarshal(prev, &existing) != nil {
		return ""
	}
	return existing.MirrorMode
}

func writeLocalConfig(path string, opts SetupOptions, loops []string) error {
	// hosts records which loops are PROJECTED per host — the background driver's re-projection
	// authority (loops alone cannot say which host surfaces exist). Old installs without the key
	// simply get no background re-projection until the next setup run records it.
	hosts := existingConfigHosts(path)
	if hosts == nil {
		hosts = map[string][]string{}
	}
	hosts[opts.Host] = unionLoops(hosts[opts.Host], opts.Loops)
	mirrorMode := existingConfigMirrorMode(path)
	if mirrorMode == "" {
		mirrorMode = "prime-refresh"
	}
	doc := map[string]any{
		"schema_version": 1,
		"mode":           "local",
		"endpoint":       opts.ControlURL,
		"principal":      opts.Principal,
		"loops":          loops,
		"hosts":          hosts,
		"mirror_mode":    mirrorMode,
		"binding_file":   filepath.ToSlash(filepath.Join(".mnemon", "harness", "channel", "bindings.json")),
		"store_path":     filepath.ToSlash(runtime.DefaultStorePath),
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}

func writeLocalEnv(path string, opts SetupOptions, tokenRel string, loops []string) error {
	var b strings.Builder
	b.WriteString("# Managed by mnemon-harness setup - Local Mnemon environment.\n")
	b.WriteString(exportLine("MNEMON_HARNESS_BIN", "mnemon-harness"))
	b.WriteString(exportLine("MNEMON_CONTROL_ADDR", opts.ControlURL))
	b.WriteString(exportLine("MNEMON_CONTROL_PRINCIPAL", opts.Principal))
	if tokenRel != "" {
		b.WriteString(exportLine("MNEMON_CONTROL_TOKEN_FILE", tokenRel))
	}
	for _, loop := range loops {
		b.WriteString(exportLine("MNEMON_"+strings.ToUpper(loop)+"_LOOP_DIR", filepath.ToSlash(filepath.Join(".mnemon", "harness", loop))))
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func exportLine(key, value string) string {
	return fmt.Sprintf("export %s=%q\n", key, value)
}

func unionLoops(a, b []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(a)+len(b))
	for _, ls := range [][]string{a, b} {
		for _, l := range ls {
			if !seen[l] {
				seen[l] = true
				out = append(out, l)
			}
		}
	}
	return out
}

// SetupStatus reports the public setup state without exposing local transport
// details. Debug/internal commands can inspect binding files directly.
func (h *Harness) SetupStatus(projectRoot, principal string) ([]string, error) {
	if projectRoot == "" {
		projectRoot = h.root
	}
	bindingFile := filepath.Join(channelBase(projectRoot), "bindings.json")
	loaded, err := channel.LoadBindingFile(projectRoot, bindingFile)
	if err != nil {
		return []string{
			"Agent Integration: not installed",
			"Local Mnemon: not configured",
			"Remote Workspace: not connected",
		}, nil
	}
	found := principal == ""
	for _, b := range loaded.Bindings {
		if principal != "" && string(b.Principal) == principal {
			found = true
			break
		}
	}
	if !found {
		return []string{
			"Agent Integration: installed",
			"Local Mnemon: not configured for this agent",
			"Remote Workspace: not connected",
		}, nil
	}
	return []string{
		"Agent Integration: installed",
		"Local Mnemon: ready",
		"Remote Workspace: not connected",
	}, nil
}

// SetupUninstall reverses setup: it removes projected loop assets and the
// principal's channel binding + token file while preserving sibling bindings.
func (h *Harness) SetupUninstall(ctx context.Context, out, errw io.Writer, opts SetupOptions) error {
	projectRoot := opts.ProjectRoot
	if projectRoot == "" {
		projectRoot = h.root
	}
	if err := h.LoopProject(ctx, out, errw, "uninstall", projectRoot, opts.Host, opts.Loops, nil); err != nil {
		return fmt.Errorf("setup uninstall: remove projected loop assets: %w", err)
	}
	base := channelBase(projectRoot)
	if opts.Principal != "" {
		removed, err := channel.RemoveBinding(filepath.Join(base, "bindings.json"), contract.ActorID(opts.Principal))
		if err != nil {
			return fmt.Errorf("setup uninstall: remove binding: %w", err)
		}
		if removed {
			fmt.Fprintf(out, "setup uninstall: removed channel binding for %s\n", opts.Principal)
		}
		for _, dir := range []string{"credentials", "tokens"} {
			tokenFile := filepath.Join(base, dir, sanitizePrincipal(opts.Principal)+".token")
			if err := os.Remove(tokenFile); err == nil {
				fmt.Fprintf(out, "setup uninstall: removed token file %s\n", tokenFile)
			}
		}
	}
	return nil
}

// tightenHarnessDirs enforces the T1 permission floor on the PRIVATE harness state tree:
// .mnemon/harness itself (path-blocking for everything beneath), the local/channel state dirs,
// and both credentials dirs are owner-only (0700). Files keep their own modes (tokens 0600).
// Idempotent and correction-oriented: a dir created earlier at 0755 (e.g. by a pre-setup
// `local run`) is tightened on the next setup. Same-user hooks/CLI are unaffected.
func tightenHarnessDirs(projectRoot string) {
	for _, rel := range []string{
		filepath.Join(".mnemon", "harness"),
		filepath.Join(".mnemon", "harness", "local"),
		filepath.Join(".mnemon", "harness", "channel"),
		filepath.Join(".mnemon", "harness", "channel", "credentials"),
		filepath.Join(".mnemon", "harness", "sync", "credentials"),
	} {
		p := filepath.Join(projectRoot, rel)
		if st, err := os.Stat(p); err == nil && st.IsDir() {
			_ = os.Chmod(p, 0o700)
		}
	}
}
