package policy

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// externalRootRel is the ONE external capability root of v1: <project root>/.mnemon/loops.
// LoadExternal takes an fs.FS for testability, but by frozen v1 contract that fsys is always
// rooted here, so every loader error names the real package path under this prefix.
const externalRootRel = ".mnemon/loops"

// externalIdentifierPattern pins every spec-authored IDENTIFIER surface of an external package:
// field names, items_field, and render static KEYS. Identifiers are class-⑧ surfaces the text
// scan cannot judge (they land verbatim in payload contracts, headers, and deny messages as bare
// tokens), so they are pattern-locked instead of scanned. Underscore is allowed — descriptor
// shapes such as items_field carry it — which is why this is a separate pattern from
// specNamePattern (one grammar across the boundary).
var externalIdentifierPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

func externalPkgPath(name string) string { return externalRootRel + "/" + name }

// LoadExternal compiles every external capability package under fsys. Each TOP-LEVEL directory is
// one package — `<name>/capability.json` in capability-spec-v1 form, strict-decoded through the
// same decodeSpec + FromSpec machinery the embedded loader uses — loaded in lexicographic package
// order (deterministic). Non-directory top-level entries (stray files, .DS_Store) are not
// packages and are ignored.
//
// fs.ErrNotExist on the root listing is the NORMAL pre-stage-5 install: empty catalog, never an
// error — a missing .mnemon/loops must not break a single existing installation.
//
// Everything else fails closed with the package path in the message: ① bad JSON / trailing
// garbage / unknown JSON keys (decodeSpec); ② unknown spec vocabulary and ③ a resource kind
// outside contract.KindCatalog (FromSpec); ⑥ any hooks/ or skills/ presence (no host projection
// assets in v1 — deliberately WIDER than loop-package-v1's minimum obligation); ⑦ statically
// derived header keys that cannot satisfy requiredFields (the kernel SchemaGuard lockstep, at
// LOAD time); ⑧ unsafe spec surfaces, external only, in two halves — VALUES are scanned
// (scanExternalSpecText), IDENTIFIERS are pattern-locked (checkExternalSpecIdentifiers);
// ⑨ directory ≠ name ≠ kind or an off-pattern directory name; ⑪ a kernel-internal reserved kind
// (externalReservedKinds). Classes ④⑤ (shadowing/dups beyond one package) are enforced by the
// shared specRegistry here and the four-axis merge in ResolveCatalog; class ⑩ (symlinks) needs
// the real OS path — fs.FS has no lstat — and lives in ResolveCatalog's screening.
func LoadExternal(fsys fs.FS, requiredFields map[contract.ResourceKind][]string) (map[string]Capability, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]Capability{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read external capability root %s: %w", externalRootRel, err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	out := map[string]Capability{}
	reg := newSpecRegistry()
	for _, name := range names {
		cap, err := loadExternalPackage(fsys, name, requiredFields)
		if err != nil {
			return nil, err
		}
		if err := reg.claim("external package "+externalPkgPath(name), cap); err != nil {
			return nil, err
		}
		out[cap.Name] = cap
	}
	return out, nil
}

func loadExternalPackage(fsys fs.FS, name string, requiredFields map[contract.ResourceKind][]string) (Capability, error) {
	pkg := externalPkgPath(name)
	// class ⑨ (pattern first): the directory IS the capability name, which IS the event-family
	// segment — ONE grammar (specNamePattern, no dash) on both sides of the boundary, so a name
	// can never pass the directory door and then die in FromSpec (or vice versa). Also kills
	// case aliasing ("Goal" vs "goal") and path-meaningful names.
	if !specNamePattern.MatchString(name) {
		return Capability{}, fmt.Errorf("external package %s: directory name must match %s (fail-closed)", pkg, specNamePattern)
	}
	// Class ⑥ (loop-package-v2): an external package MAY carry host assets, but the hook-fragment
	// CODE face stays embedded-only and every projected prose asset is injection-scanned.
	if err := scanExternalPackageAssets(fsys, name, pkg); err != nil {
		return Capability{}, err
	}
	raw, err := fs.ReadFile(fsys, path.Join(name, "capability.json"))
	if err != nil {
		return Capability{}, fmt.Errorf("external package %s: read capability.json: %w", pkg, err)
	}
	spec, err := decodeSpec(raw) // class ①
	if err != nil {
		return Capability{}, fmt.Errorf("external package %s: parse capability.json: %w", pkg, err)
	}
	// Class ⑨ (name second), directory-as-declaration: the directory IS the name claim.
	if spec.Name != name {
		return Capability{}, fmt.Errorf("external package %s: directory name %q must equal spec name %q (directory-as-declaration)", pkg, name, spec.Name)
	}
	// classes ②③ + every FromSpec fail-closed check, INCLUDING the G8 kind reservation (class ⑪):
	// FromSpec rejects a governance/mnemon/reserved-family kind for embedded and external specs
	// alike, so the external loader no longer needs its own deny-list.
	cap, err := FromSpec(spec)
	if err != nil {
		return Capability{}, fmt.Errorf("external package %s: %w", pkg, err)
	}
	// Class ⑨ (kind third): directory == name == kind in v1. Enablement derives the catalog entry
	// from the binding scope KIND — a name/kind divergence would make the package unreachable (or
	// reachable under a name the operator never wrote).
	if spec.ResourceKind != spec.Name {
		return Capability{}, fmt.Errorf("external package %s: spec name %q must equal resource_kind %q (directory == name == kind in v1; enablement derives the catalog entry from the binding scope kind)", pkg, spec.Name, spec.ResourceKind)
	}
	if err := checkExternalSpecIdentifiers(spec); err != nil { // class ⑧ (identifier half)
		return Capability{}, fmt.Errorf("external package %s: %w", pkg, err)
	}
	if err := scanExternalSpecText(spec); err != nil { // class ⑧ (value half)
		return Capability{}, fmt.Errorf("external package %s: %w", pkg, err)
	}
	if err := headerCoversRequired(spec, requiredFields); err != nil { // class ⑦
		return Capability{}, fmt.Errorf("external package %s: %w", pkg, err)
	}
	return cap, nil
}

// checkExternalSpecIdentifiers is the IDENTIFIER half of class ⑧ (external only): every field
// name, the items_field, and every render static KEY must match externalIdentifierPattern.
// These are the spec surfaces the text scan cannot judge — a bare token is never
// injection-shaped, yet it lands verbatim in payload contracts, headers, and deny messages — so
// they are pattern-locked, fail-closed, naming the offending identifier (the caller prefixes the
// package path). The spec name needs no entry here: it is pattern-locked via directory == name
// (class ⑨). Validator field REFERENCES (default-from, bullet-list) resolve to declared fields
// in FromSpec, so they are transitively covered.
func checkExternalSpecIdentifiers(spec CapabilitySpec) error {
	if !externalIdentifierPattern.MatchString(spec.ItemsField) {
		return fmt.Errorf("spec identifier items_field %q must match %s (fail-closed)", spec.ItemsField, externalIdentifierPattern)
	}
	for _, f := range spec.Fields {
		if !externalIdentifierPattern.MatchString(f.Name) {
			return fmt.Errorf("spec identifier field name %q must match %s (fail-closed)", f.Name, externalIdentifierPattern)
		}
	}
	for _, k := range sortedStaticKeys(spec.Render.Static) {
		if !externalIdentifierPattern.MatchString(k) {
			return fmt.Errorf("spec identifier render static key %q must match %s (fail-closed)", k, externalIdentifierPattern)
		}
	}
	return nil
}

// scanExternalSpecText is the VALUE half of class ⑧: the embedded safety scanners run over every
// spec-authored free-text surface of an EXTERNAL spec — the name, each enum validator's deny
// message, each default validator's value (free prose that lands verbatim in items when the host
// omits the field), each render static value, and the bullet-list title. These strings flow into
// deny messages, governed items, and rendered governed content; embedded spec text is reviewed
// code (pinned by the golden tests), external spec text is untrusted input — scanned at load
// time, fail-closed. External path only by design.
func scanExternalSpecText(spec CapabilitySpec) error {
	type surface struct{ where, text string }
	surfaces := []surface{{"name", spec.Name}}
	for _, f := range spec.Fields {
		for _, v := range f.Validators {
			switch v.ID {
			case "enum":
				surfaces = append(surfaces, surface{fmt.Sprintf("field %q enum message", f.Name), v.Params["message"]})
			case "default":
				surfaces = append(surfaces, surface{fmt.Sprintf("field %q default value", f.Name), v.Params["value"]})
			}
		}
	}
	for _, k := range sortedStaticKeys(spec.Render.Static) {
		surfaces = append(surfaces, surface{fmt.Sprintf("render static %q", k), spec.Render.Static[k]})
	}
	if c := spec.Render.Content; c != nil && c.Member == "bullet-list" {
		surfaces = append(surfaces, surface{"render bullet-list title", c.Params["title"]})
	}
	for _, s := range surfaces {
		if containsSecretLikeContent(s.text) || containsPromptInjectionShape(s.text) {
			return fmt.Errorf("unsafe spec text in %s (secret-like or prompt-injection-shaped; external spec text is untrusted input)", s.where)
		}
	}
	return nil
}

// scanExternalPackageAssets is the loop-package-v2 host-asset safety gate at the capability-loader
// level (class ⑥, no longer a blanket reject): an external package MAY carry host assets, but
//   - the hook-fragment CODE face stays embedded-only: hooks/fragments/ presence fails closed (the
//     renderer never reads an external fragment, but its presence must fail LOUD, not silently
//     no-op), and
//   - every projected prose asset (GUIDE.md, hooks/intents.json, skills/**) is scanned for
//     prompt-injection SHAPE — documentation-grade (containsPromptInjectionShape), NOT the content
//     secret scan, since honest documentation may discuss secrets.
//
// The deeper STRUCTURAL checks — the `include` intent, a template `external_id_recipe`, and that a
// control-observe action's event_type equals the package's own observed_type — run in the projector
// loader where the schema-aware parsers live (loop-package-v2 enforcement map); a capability leaf
// must not duplicate the hostagent intents/template schema.
func scanExternalPackageAssets(fsys fs.FS, name, pkg string) error {
	if _, err := fs.Stat(fsys, path.Join(name, "hooks", "fragments")); err == nil {
		return fmt.Errorf("external package %s: hooks/fragments/ is forbidden (shell fragments are embedded-only; fail-closed)", pkg)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("external package %s: stat hooks/fragments/: %w", pkg, err)
	}
	for _, rel := range []string{"GUIDE.md", path.Join("hooks", "intents.json")} {
		if err := scanExternalAssetText(fsys, path.Join(name, rel), pkg); err != nil {
			return err
		}
	}
	skillsRoot := path.Join(name, "skills")
	if info, err := fs.Stat(fsys, skillsRoot); err == nil && info.IsDir() {
		if err := fs.WalkDir(fsys, skillsRoot, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			return scanExternalAssetText(fsys, p, pkg)
		}); err != nil {
			return fmt.Errorf("external package %s: scan skills/: %w", pkg, err)
		}
	} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("external package %s: stat skills/: %w", pkg, err)
	}
	return nil
}

// scanExternalAssetText injection-scans one projected text asset (absent = inert, skipped).
func scanExternalAssetText(fsys fs.FS, full, pkg string) error {
	raw, err := fs.ReadFile(fsys, full)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("external package %s: read %s: %w", pkg, full, err)
	}
	if containsPromptInjectionShape(string(raw)) {
		return fmt.Errorf("external package %s: %s contains prompt-injection-shaped text (untrusted projected prose; fail-closed)", pkg, full)
	}
	return nil
}

// sortedStaticKeys keeps both class-⑧ halves deterministic: the FIRST offending static key (by
// sort order) is the one named, run after run.
func sortedStaticKeys(static map[string]string) []string {
	keys := make([]string, 0, len(static))
	for k := range static {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// headerCoversRequired is the load-time kernel-schema lockstep (class ⑦): the keys a capability's
// write STATICALLY produces — render static keys, "content" when a content member is selected,
// the items field, and the stamped "updated_by" — must cover every field requiredFields demands
// for the kind. Derived from the spec alone (no payload synthesis), so a package that could only
// ever produce kernel-rejected writes fails at load, not at runtime.
func headerCoversRequired(spec CapabilitySpec, requiredFields map[contract.ResourceKind][]string) error {
	produced := map[string]bool{spec.ItemsField: true, "updated_by": true}
	for k := range spec.Render.Static {
		produced[k] = true
	}
	if spec.Render.Content != nil {
		produced["content"] = true
	}
	for _, f := range requiredFields[contract.ResourceKind(spec.ResourceKind)] {
		if !produced[f] {
			return fmt.Errorf("rendered header cannot satisfy the kernel schema: kind %q requires %q but the statically produced keys never carry it (fail-closed)", spec.ResourceKind, f)
		}
	}
	return nil
}

// ResolveCatalog builds the boot capability catalog: the embedded Builtins plus every external
// capability package under <projectRoot>/.mnemon/loops — the ONLY external root in v1. It is the
// one production entry point: symlink screening on the real path (class ⑩), LoadExternal over
// os.DirFS, then a merge with FOUR-axis shadowing rejection (name, observed type, proposed type,
// resource kind) — an external package may never shadow an embedded capability, and two externals
// may not share a kind. A missing .mnemon/loops resolves to the embedded catalog.
func ResolveCatalog(projectRoot string, requiredFields map[contract.ResourceKind][]string) (map[string]Capability, error) {
	rootDir := filepath.Join(projectRoot, filepath.FromSlash(externalRootRel))
	if err := screenExternalSymlinks(rootDir); err != nil {
		return nil, err
	}
	external, err := LoadExternal(os.DirFS(rootDir), requiredFields)
	if err != nil {
		return nil, err
	}
	return mergeExternal(embeddedCatalog, external)
}

// screenExternalSymlinks is fault class ⑩, on the REAL path because fs.FS has no lstat: the
// external ROOT itself, a package directory, or a capability.json arriving via symlink is
// rejected before any fsys is built. Without this, os.DirFS would silently TRAVERSE a symlinked
// root, silently SKIP a symlinked dir (not IsDir to ReadDir) and silently FOLLOW a symlinked
// capability.json — and silent is the one thing this loader must never be. An unknown lstat
// error is never treated as absence (only fs.ErrNotExist is).
func screenExternalSymlinks(rootDir string) error {
	if fi, err := os.Lstat(rootDir); err == nil {
		if fi.Mode()&fs.ModeSymlink != 0 {
			return fmt.Errorf("external capability root %s: symlinked root directory rejected (fail-closed)", externalRootRel)
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("lstat external capability root %s: %w", externalRootRel, err)
	}
	entries, err := os.ReadDir(rootDir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read external capability root %s: %w", externalRootRel, err)
	}
	for _, e := range entries {
		pkg := externalPkgPath(e.Name())
		if e.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("external package %s: symlinked package directory rejected (fail-closed)", pkg)
		}
		if !e.IsDir() {
			continue
		}
		if fi, err := os.Lstat(filepath.Join(rootDir, e.Name(), "capability.json")); err == nil && fi.Mode()&fs.ModeSymlink != 0 {
			return fmt.Errorf("external package %s: symlinked capability.json rejected (fail-closed)", pkg)
		}
	}
	return nil
}

// mergeExternal merges the external catalog into a FRESH copy of the embedded one with four-axis
// shadowing rejection. The first three axes reuse the shared specRegistry; the resource-kind axis
// is merge-time only: external may not claim a kind an embedded capability claims, and two
// externals may not share a kind (each external package owns its event family AND its kind).
// Deterministic order (sorted names) keeps the first error stable.
func mergeExternal(embedded, external map[string]Capability) (map[string]Capability, error) {
	merged := make(map[string]Capability, len(embedded)+len(external))
	reg := newSpecRegistry()
	kinds := map[contract.ResourceKind]string{}
	for _, n := range sortedKeys(embedded) {
		c := embedded[n]
		if err := reg.claim("embedded capability "+n, c); err != nil {
			return nil, err
		}
		kinds[c.ResourceKind] = c.Name
		merged[n] = c
	}
	for _, n := range sortedKeys(external) {
		c := external[n]
		src := "external package " + externalPkgPath(n)
		if err := reg.claim(src, c); err != nil {
			return nil, err
		}
		if prev, dup := kinds[c.ResourceKind]; dup {
			return nil, fmt.Errorf("%s: resource_kind %q already claimed by capability %q (external packages may not shadow)", src, c.ResourceKind, prev)
		}
		kinds[c.ResourceKind] = c.Name
		merged[n] = c
	}
	return merged, nil
}

func sortedKeys(m map[string]Capability) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
