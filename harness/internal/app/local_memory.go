package app

import (
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/assembler"
	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/capability"
	"github.com/mnemon-dev/mnemon/harness/internal/channel"
	"github.com/mnemon-dev/mnemon/harness/internal/config"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/driver"
	"github.com/mnemon-dev/mnemon/harness/internal/hostsurface"
	"path/filepath"

	"github.com/mnemon-dev/mnemon/harness/internal/kernel"
	"github.com/mnemon-dev/mnemon/harness/internal/manifest"
	"github.com/mnemon-dev/mnemon/harness/internal/rule"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

// OpenLocalRuntime boots Local Mnemon over the select-only assembler: loops (from the setup-written
// localConfig) enable capabilities; bindings stay the source of truth for observe/pull/status scope.
// An empty loops list (the hidden `local run --bindings` path, which has no localConfig) derives
// enablement from the binding scope kinds ∩ catalog. catalog selects the capability universe
// (nil = capability.EmbeddedCatalog()); the serve path passes the boot-resolved external-merged catalog.
// The assembled policy is then merged with the sync-import half (withSyncImport), so the SERVING
// runtime can import pulled commits in-process (v1.1 #2) without a second runtime boot.
func OpenLocalRuntime(storePath string, loaded channel.LoadedBindings, loops []string, catalog map[string]capability.Capability) (*runtime.Runtime, error) {
	cat := resolveSyncCatalog(catalog)
	if len(loops) == 0 {
		loops = loopsFromBindings(loaded.Bindings, cat)
	}
	loops = withDefaultEnabledLoops(loops, cat)
	bindings := withDefaultEnabledGrants(loaded.Bindings, cat)
	rc, err := assembler.Assemble(capabilityFileFromLoops(loops), bindings, cat)
	if err != nil {
		return nil, err
	}
	return runtime.OpenRuntime(storePath, withSyncImport(rc, bindings, cat))
}

// withSyncImport merges the sync-import half into an assembled runtime policy (v1.1 #2): sync@local
// gets one import rule per importable capability (descriptor-derived, PD6) + the skipped-kind deny
// rule, kernel authority for the importable kinds, and a subscription covering the binding scope's
// syncable refs (the import rules read the current resource through this view to merge against).
// Co-existence is by construction: the added rules Handle only the <kind>.remote_commit.observed /
// sync.* observation types AND gate on the sync principal, so host-agent events never match them and
// host rules never see the import events — pinned by a test. catalog selects the importable universe
// (nil = embedded first-party).
func withSyncImport(rc runtime.RuntimeConfig, bindings []channel.ChannelBinding, catalog map[string]capability.Capability) runtime.RuntimeConfig {
	catalog = resolveSyncCatalog(catalog)
	rules := append([]rule.Rule(nil), rc.Rules.Rules()...)
	rules = append(rules, capability.RemoteImportRules(catalog, contract.SyncImportActor)...)
	rules = append(rules, capability.SyncImportSkippedRule(contract.SyncImportActor))
	rc.Rules = rule.NewRuleSet(rules...)
	if rc.Subs == nil {
		rc.Subs = map[contract.ActorID]contract.Subscription{}
	}
	rc.Subs[contract.SyncImportActor] = contract.Subscription{Actor: contract.SyncImportActor, Refs: syncableScopeRefs(bindings, catalog)}
	if rc.Authority.Allow == nil {
		rc.Authority.Allow = map[contract.ActorID][]contract.ResourceKind{}
	}
	rc.Authority.Allow[contract.SyncImportActor] = capability.ImportableKinds(catalog)
	// Inject the produce surface: this replica emits sync commits for exactly the kinds its catalog
	// imports (sync-abi-v2 §4). The runtime stays capability-free — the app fills the kind slice.
	rc.SyncableKinds = capability.ImportableKinds(catalog)
	return rc
}

// resolveSyncCatalog resolves the catalog the sync-import path derives its rules/authority/guard
// from: nil falls back to the embedded first-party catalog (memory/skill), so callers without a
// boot-resolved catalog still get the first-party importable kinds.
func resolveSyncCatalog(catalog map[string]capability.Capability) map[string]capability.Capability {
	if catalog == nil {
		return capability.EmbeddedCatalog()
	}
	return catalog
}

// syncableScopeRefs collects the deduped binding-scope refs of importable kinds — the resources a
// pulled commit may target on this replica (the same canonical refs the host loops govern). The
// importable-kind set is descriptor-derived from the catalog (PD6), not a hardcoded constant.
func syncableScopeRefs(bindings []channel.ChannelBinding, catalog map[string]capability.Capability) []contract.ResourceRef {
	syncable := map[contract.ResourceKind]bool{}
	for _, k := range capability.ImportableKinds(catalog) {
		syncable[k] = true
	}
	seen := map[contract.ResourceRef]bool{}
	var refs []contract.ResourceRef
	for _, b := range bindings {
		for _, ref := range b.SubscriptionScope {
			if syncable[ref.Kind] && !seen[ref] {
				seen[ref] = true
				refs = append(refs, ref)
			}
		}
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].Kind != refs[j].Kind {
			return refs[i].Kind < refs[j].Kind
		}
		return refs[i].ID < refs[j].ID
	})
	return refs
}

// LocalRuntimeConfigFromBindings derives Local Mnemon's policy from the installed Agent Integration
// bindings alone (enablement = binding scope kinds ∩ catalog; nil = Builtins). It is the
// bindings-only convenience over the same select-only assembly OpenLocalRuntime uses.
func LocalRuntimeConfigFromBindings(bindings []channel.ChannelBinding, catalog map[string]capability.Capability) (runtime.RuntimeConfig, error) {
	cat := resolveSyncCatalog(catalog)
	loops := withDefaultEnabledLoops(loopsFromBindings(bindings, cat), cat)
	return assembler.Assemble(capabilityFileFromLoops(loops), withDefaultEnabledGrants(bindings, cat), cat)
}

// defaultEnabledCaps returns the catalog's default-enabled capabilities (the coordination package),
// sorted by kind for determinism — the kinds the local boot governs without an explicit --loop (P3).
func defaultEnabledCaps(catalog map[string]capability.Capability) []capability.Capability {
	var caps []capability.Capability
	for _, c := range catalog {
		if c.DefaultEnabled {
			caps = append(caps, c)
		}
	}
	sort.Slice(caps, func(i, j int) bool { return caps[i].ResourceKind < caps[j].ResourceKind })
	return caps
}

// withDefaultEnabledLoops unions the catalog's default-enabled kinds into the enabled-loops list, so
// the assembler builds their rules even when no --loop named them.
func withDefaultEnabledLoops(loops []string, catalog map[string]capability.Capability) []string {
	for _, c := range defaultEnabledCaps(catalog) {
		if !containsLoop(loops, c.Name) {
			loops = append(loops, c.Name)
		}
	}
	return loops
}

// withDefaultEnabledGrants grants every host-agent binding the default-enabled kinds' observe type +
// project-scope ref (in-memory, never rewriting the on-disk binding): the catalog-driven IMPLICIT
// grant that sits beside the binding's EXPLICIT --loop grants, so a default-enabled kind is
// governable + pullable from setup alone (P3). The assembler and the channel authorizer both read
// this same augmented list, so rules, authority, and authz stay consistent.
func withDefaultEnabledGrants(bindings []channel.ChannelBinding, catalog map[string]capability.Capability) []channel.ChannelBinding {
	defaults := defaultEnabledCaps(catalog)
	if len(defaults) == 0 {
		return bindings
	}
	out := make([]channel.ChannelBinding, len(bindings))
	for i, b := range bindings {
		// host-agents AND control-agents (operators) both govern the default-enabled kinds — an operator
		// proposes loopdefs and approves high-risk candidates, so it needs the same default grant (P3e).
		if b.ActorKind == contract.KindHostAgent || b.ActorKind == contract.KindControlAgent {
			// An EMPTY AllowedObservedTypes already means allow-all (AllowsObservedType returns true),
			// so coordination is permitted without listing it — and appending here would flip the
			// binding to an explicit allow-list that EXCLUDES everything else. Only extend an explicit
			// (non-empty) list, which is what setup writes.
			explicitTypes := len(b.AllowedObservedTypes) > 0
			for _, c := range defaults {
				if explicitTypes {
					b.AllowedObservedTypes = appendUniqueString(b.AllowedObservedTypes, c.ObservedType)
				}
				b.SubscriptionScope = appendUniqueRef(b.SubscriptionScope, contract.ResourceRef{Kind: c.ResourceKind, ID: "project"})
			}
		}
		out[i] = b
	}
	return out
}

// appendUniqueString / appendUniqueRef append v only if absent, returning a NEW backing array when
// they grow (so augmenting a binding copy never mutates the caller's slice).
func appendUniqueString(s []string, v string) []string {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(append([]string(nil), s...), v)
}

func appendUniqueRef(s []contract.ResourceRef, v contract.ResourceRef) []contract.ResourceRef {
	for _, x := range s {
		if x == v {
			return s
		}
	}
	return append(append([]contract.ResourceRef(nil), s...), v)
}

// capabilityFileFromLoops constructs the in-memory config.File for the enabled loops. The on-disk
// localConfig (schema_version 1) stays the enablement authority; config.Load parses the FUTURE
// on-disk form and is not yet the boot reader (do not migrate until a capability needs a knob the
// loops list cannot express).
func capabilityFileFromLoops(loops []string) config.File {
	caps := make(map[string]config.CapabilityConfig, len(loops))
	for _, loop := range loops {
		caps[loop] = config.CapabilityConfig{Enabled: true, ResourceRef: loop + "/project", RuleRef: "native:" + loop}
	}
	return config.File{Capabilities: caps}
}

// loopsFromBindings derives capability enablement from binding scope kinds ∩ catalog (nil =
// Builtins). config.loops stays the product-path authority — this derivation only runs when the
// loops list is empty (the hidden bindings-only path).
func loopsFromBindings(bindings []channel.ChannelBinding, catalog map[string]capability.Capability) []string {
	if catalog == nil {
		catalog = capability.EmbeddedCatalog()
	}
	seen := map[string]bool{}
	var loops []string
	for _, b := range bindings {
		for _, ref := range b.SubscriptionScope {
			id := string(ref.Kind)
			if _, ok := catalog[id]; ok && !seen[id] {
				seen[id] = true
				loops = append(loops, id)
			}
		}
	}
	sort.Strings(loops)
	return loops
}

// ServeOptions carries the boot-config state the serve path needs beyond bindings: capability
// enablement (Loops), the per-host projected loops (Hosts — the background driver's re-projection
// authority), and the project root the host surfaces live under.
type ServeOptions struct {
	Loops          []string
	Hosts          map[string][]string
	ProjectRoot    string
	MirrorMode     string // "manual" | "prime-refresh" (driver-side mirror regeneration gate)
	IgnoreExternal bool   // boot the embedded-only catalog, naming each ignored external package on stderr
	// AllowInsecureRemote is the sync worker's T2 downgrade override (v1.1 #3): permit a plaintext
	// non-loopback remote endpoint. Default false — fail closed.
	AllowInsecureRemote bool
	SyncInterval        time.Duration // sync worker cadence; <= 0 = default (30s)
}

// RunLocalHTTPServerWithBindings serves Local Mnemon from a binding manifest. It is the product boot
// path used by `mnemon-harness local run`. When opts.Hosts is non-empty it co-hosts the Background
// Driver (plan 3.4): one goroutine in the SAME process — never a second store opener — driving
// Tick + DrainOutbox and re-projecting each recorded host's managed definition files when an
// invalidation drained. A driver error stops the driver (logged to stderr); the hot path serves on.
func RunLocalHTTPServerWithBindings(ctx context.Context, addr, storePath string, loaded channel.LoadedBindings, opts ServeOptions, out io.Writer) error {
	catalog, ignored, err := resolveBootCatalog(opts.ProjectRoot, opts.IgnoreExternal, os.Stderr)
	if err != nil {
		return err
	}
	rt, err := OpenLocalRuntime(storePath, loaded, disableIgnoredLoops(opts.Loops, ignored, os.Stderr), catalog)
	if err != nil {
		return err
	}
	// Record the G4 activation ledger for any materialized loopdef packages this boot is governing —
	// once, at boot (the reload that re-assembled them is the activation), never on a Tick watch (G1).
	if err := emitLoopdefActivations(rt, opts.ProjectRoot); err != nil {
		fmt.Fprintf(os.Stderr, "mnemon-harness: loopdef activation ledger: %v\n", err)
	}
	// Shutdown ordering (MED-5): the background driver and sync worker write through rt's open store
	// on their own goroutines. rt.Close() must not race a mid-flight worker store write, so JOIN both
	// goroutines (they exit promptly on ctx cancel) BEFORE closing the store. Defers run LIFO, so the
	// later-registered wg.Wait() runs FIRST — after ServeRuntime returns (ctx cancelled), then the
	// store closes on a quiesced runtime.
	defer rt.Close()
	var wg sync.WaitGroup
	defer wg.Wait()
	if reproject := serveReproject(rt, loaded, opts.Hosts, opts.ProjectRoot, opts.MirrorMode, catalog); reproject != nil {
		d := driver.New(rt, swallowReprojectErrors(reproject, os.Stderr), 0)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := d.Run(ctx); err != nil && ctx.Err() == nil {
				fmt.Fprintf(os.Stderr, "mnemon-harness: background driver stopped: %v\n", err)
			}
		}()
	}
	// The sync worker runs on its OWN goroutine/cadence (never inside driver.Tick — a slow remote
	// must not stall the governed loop; the client is timeout-bounded regardless, v1.1 #2/#10). It
	// self-gates on remotes.json presence: no remote configured = zero sync activity (I13).
	wg.Add(1)
	go func() {
		defer wg.Done()
		RunSyncWorker(ctx, rt, SyncWorkerOptions{
			ProjectRoot:         opts.ProjectRoot,
			AllowInsecureRemote: opts.AllowInsecureRemote,
			Interval:            opts.SyncInterval,
			Catalog:             catalog,
		}, os.Stderr)
	}()
	return ServeLocalHTTP(ctx, addr, rt, channel.NewBindingAuthenticator(loaded), loaded, opts.ProjectRoot, out)
}

// resolveBootCatalog resolves the capability catalog ONCE at boot. Default: embedded Builtins +
// every external package under <projectRoot>/.mnemon/loops via capability.ResolveCatalog
// (requiredFields = kernel.DefaultSchemaGuard().Required — app owns the kernel import; capability
// stays a contract-level leaf), fail-closed: a bad external package REFUSES to start Local Mnemon
// — the directory's presence is a contract, not a hint. ignoreExternal is the operator escape
// hatch (`local run --ignore-external`): boot the embedded-only catalog and name each ignored
// package on errw, one line per package, so what is offline is visible, never silent. The second
// return is those ignored package names — the serve path must drop them from the enabled loops
// too (disableIgnoredLoops), or an enabled-then-corrupted package would still sink the boot on
// `unknown rule_ref`.
func resolveBootCatalog(projectRoot string, ignoreExternal bool, errw io.Writer) (map[string]capability.Capability, []string, error) {
	if !ignoreExternal {
		catalog, err := capability.ResolveCatalog(projectRoot, kernel.DefaultSchemaGuard().Required)
		return catalog, nil, err
	}
	entries, err := os.ReadDir(filepath.Join(projectRoot, ".mnemon", "loops"))
	if err != nil {
		return capability.EmbeddedCatalog(), nil, nil // absent (or unreadable) external root: nothing to ignore
	}
	var ignored []string
	for _, e := range entries {
		if e.IsDir() || e.Type()&os.ModeSymlink != 0 {
			ignored = append(ignored, e.Name())
			fmt.Fprintf(errw, "mnemon-harness: --ignore-external: ignoring external package .mnemon/loops/%s\n", e.Name())
		}
	}
	return capability.EmbeddedCatalog(), ignored, nil
}

// SyncImportCatalog resolves the capability catalog the OFFLINE `sync pull` verb derives its import
// rules from (descriptor-derived, PD6): the embedded first-party catalog plus every external package
// under <projectRoot>/.mnemon/loops, so a remote commit of an external importable kind imports the
// same way the in-process worker imports it. Unlike serve boot, the manual pull verb degrades to the
// embedded catalog (with a stderr warning) when an external package is unreadable — a corrupt loop
// must not block importing first-party memory/skill commits.
func SyncImportCatalog(projectRoot string, errw io.Writer) map[string]capability.Capability {
	catalog, err := capability.ResolveCatalog(projectRoot, kernel.DefaultSchemaGuard().Required)
	if err != nil {
		fmt.Fprintf(errw, "mnemon-harness: sync import: external package unreadable, importing first-party kinds only: %v\n", err)
		return capability.EmbeddedCatalog()
	}
	return catalog
}

// disableIgnoredLoops is the loop-list half of --ignore-external: the PRIMARY ignore scenario is
// an external package the operator already ENABLED (config.loops carries its name) that has since
// gone bad. Ignoring only the catalog would still sink boot — the assembler would fail on
// `unknown rule_ref "native:<name>"` — so the ignored package names are dropped from the enabled
// loops too, one stderr line per disabled loop, visible, never silent. Names that match no
// ignored package pass through untouched (a typo in config.loops keeps its diagnostic).
func disableIgnoredLoops(loops, ignored []string, errw io.Writer) []string {
	if len(ignored) == 0 {
		return loops
	}
	skip := map[string]bool{}
	for _, name := range ignored {
		skip[name] = true
	}
	kept := make([]string, 0, len(loops))
	for _, loop := range loops {
		if skip[loop] {
			fmt.Fprintf(errw, "mnemon-harness: --ignore-external: disabling loop %s\n", loop)
			continue
		}
		kept = append(kept, loop)
	}
	return kept
}

// serveReproject builds the driver's reproject callback: (a) re-project every recorded host's
// managed DEFINITION files under no-clobber (cheap no-op when unchanged), and (b) when the
// drained refs touch the memory kind and MirrorMode permits, regenerate each host's derived
// MEMORY.md mirror from a fresh scoped projection (I11: derived, freely regenerated — never
// routed through conflict-preserve). nil when no hosts are recorded — old installs get no
// background re-projection until a setup rerun records the hosts map.
//
// Mirror scope reconciliation: only the memory loop carries a runtime mirror today; the
// loop-declared generic version replaces this helper when loop packages carry mirror
// declarations (stage 3 final form / stage 5 external packages — the stage-2 render catalog
// is the building block, not the trigger).
func serveReproject(rt *runtime.Runtime, loaded channel.LoadedBindings, hosts map[string][]string, projectRoot, mirrorMode string, catalog map[string]capability.Capability) func(refs []contract.ResourceRef) error {
	if len(hosts) == 0 {
		return nil
	}
	catalog = resolveSyncCatalog(catalog) // never nil at the budget-shaping site
	names := make([]string, 0, len(hosts))
	for h := range hosts {
		names = append(names, h)
	}
	sort.Strings(names)
	return func(refs []contract.ResourceRef) error {
		for _, host := range names {
			if len(hosts[host]) == 0 {
				continue
			}
			if _, err := hostsurface.ReProject(hostsurface.ProjectContext{
				Host:        host,
				ProjectRoot: projectRoot,
				Loops:       hosts[host],
			}, refs); err != nil {
				return fmt.Errorf("re-project %s: %w", host, err)
			}
		}
		// D-loop materialize (Δ2/G5): an admitted loopdef draft writes its managed package to
		// .mnemon/loops/ — the driver bridge, not the runtime. Writes only; activation is a separate
		// explicit reload (G1/G3).
		if refsTouchKind(refs, "loopdef") {
			if err := materializeLoopdefs(rt, projectRoot); err != nil {
				return fmt.Errorf("materialize loopdefs: %w", err)
			}
		}
		if mirrorMode == "manual" || !refsTouchKind(refs, "memory") {
			return nil
		}
		mbind, ok := mirrorPrincipal(loaded.Bindings)
		if !ok {
			return nil // no memory-scoped host-agent binding: nothing to mirror
		}
		proj, err := rt.API().PullProjection(mbind.Principal, contract.Subscription{Actor: mbind.Principal})
		if err != nil {
			return fmt.Errorf("mirror projection: %w", err)
		}
		// Budget the DERIVED MIRROR to the endpoint's declared tier (P4): a LOCAL presentation
		// transform on what this host sees, never a hub-side reduction (I11 — local decides). The
		// Digest still attests the full authoritative scope; hot/empty budget is exact passthrough.
		proj = budgetShapeProjection(proj, catalog, mbind.Budget)
		for _, host := range names {
			if !containsLoop(hosts[host], "memory") {
				continue
			}
			binding, err := manifest.LoadBinding(assets.FS, host, "memory")
			if err != nil {
				return fmt.Errorf("mirror binding %s: %w", host, err)
			}
			path := filepath.Join(projectRoot, filepath.FromSlash(binding.RuntimeSurface), "MEMORY.md")
			if err := hostsurface.WriteMemoryMirror(path, proj); err != nil {
				return fmt.Errorf("mirror %s: %w", host, err)
			}
		}
		return nil
	}
}

// swallowReprojectErrors keeps the background driver alive across reproject failures: the driver
// stops on the FIRST Tick error, and a transient mirror/file failure must never permanently kill
// outbox draining (and with it, pruning) for the process lifetime. Reproject is best-effort —
// log and continue; store-level Tick errors still stop the driver.
func swallowReprojectErrors(reproject func(refs []contract.ResourceRef) error, errw io.Writer) func(refs []contract.ResourceRef) error {
	return func(refs []contract.ResourceRef) error {
		if err := reproject(refs); err != nil {
			fmt.Fprintf(errw, "mnemon-harness: background re-projection: %v\n", err)
		}
		return nil
	}
}

// refsTouchKind reports whether any drained ref is of kind (selective refresh: a skill-only
// write does not regenerate the memory mirror).
func refsTouchKind(refs []contract.ResourceRef, kind contract.ResourceKind) bool {
	for _, r := range refs {
		if r.Kind == kind {
			return true
		}
	}
	return false
}

// mirrorPrincipal picks the projection identity for mirror regeneration: the first (by
// principal, deterministic) host-agent binding whose scope covers the memory kind. The memory
// resource is shared, so any in-scope principal projects identical content.
// mirrorPrincipal returns the binding whose derived memory mirror is written (the lexically-first
// memory-scoped host-agent). The whole binding is returned, not just the principal, so the caller can
// budget the mirror to that endpoint's declared tier (P4).
func mirrorPrincipal(bindings []channel.ChannelBinding) (channel.ChannelBinding, bool) {
	var candidates []channel.ChannelBinding
	for _, b := range bindings {
		if b.ActorKind != contract.KindHostAgent {
			continue
		}
		for _, ref := range b.SubscriptionScope {
			if ref.Kind == "memory" {
				candidates = append(candidates, b)
				break
			}
		}
	}
	if len(candidates) == 0 {
		return channel.ChannelBinding{}, false
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Principal < candidates[j].Principal })
	return candidates[0], true
}

func containsLoop(loops []string, name string) bool {
	for _, l := range loops {
		if l == name {
			return true
		}
	}
	return false
}

func OpenSyncImportRuntime(storePath string, refs []contract.ResourceRef, catalog map[string]capability.Capability) (*runtime.Runtime, error) {
	return runtime.OpenRuntime(storePath, SyncImportRuntimeConfig(refs, catalog))
}

// SyncImportRuntimeConfig is the sync-import policy, fully descriptor-derived (PD6): one import rule
// per importable capability (each selecting its declared closed-set merge strategy), kernel authority
// for exactly the importable kinds, and a guard registering each importable kind's required header
// onto the governance base. The skipped-kind deny rule (v1.1 #4) keeps any OTHER pulled kind a
// durable diagnostic instead of a silent drop — the same rule set withSyncImport merges into the
// serving runtime, so the offline and in-process import paths share one policy. catalog selects the
// importable universe (nil = embedded first-party).
func SyncImportRuntimeConfig(refs []contract.ResourceRef, catalog map[string]capability.Capability) runtime.RuntimeConfig {
	catalog = resolveSyncCatalog(catalog)
	extra := map[contract.ResourceKind][]string{}
	for _, cap := range catalog {
		if cap.Sync.Importable {
			extra[cap.ResourceKind] = cap.RequiredHeader
		}
	}
	rules := append(capability.RemoteImportRules(catalog, contract.SyncImportActor),
		capability.SyncImportSkippedRule(contract.SyncImportActor))
	return runtime.RuntimeConfig{
		Subs: map[contract.ActorID]contract.Subscription{
			contract.SyncImportActor: {Actor: contract.SyncImportActor, Refs: refs},
		},
		Rules: rule.NewRuleSet(rules...),
		Authority: kernel.AuthorityRules{Allow: map[contract.ActorID][]contract.ResourceKind{
			contract.SyncImportActor: capability.ImportableKinds(catalog),
		}},
		SchemaGuard: kernel.SchemaGuardWith(extra),
	}
}
