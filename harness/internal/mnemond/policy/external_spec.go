package policy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// ExternalSpec is the JSON adapter input for an external event package
// (.mnemon/loops/<name>/capability.json). It can only SELECT closed Go members
// (validators, renders, sync merge strategies, risk tiers); it cannot define behavior.
type ExternalSpec struct {
	SchemaVersion int         `json:"schema_version"` // external event package spec v1
	Name          string      `json:"name"`
	ObservedType  string      `json:"observed_type"`
	ProposedType  string      `json:"proposed_type"`
	ResourceKind  string      `json:"resource_kind"`
	ItemsField    string      `json:"items_field"`
	Fields        []FieldSpec `json:"fields"`
	Render        RenderSpec  `json:"render"`
	// Required SELECTS the kind's kernel-required header fields from the render-produced keys.
	// Omitted = every produced key is required; when present,
	// each entry must be a render-produced key (a kind cannot require a field its writes never carry).
	// It is the single source the assembly-time SchemaGuard derives a user kind's required set from.
	Required []string `json:"required,omitempty"`
	// Sync declares whether this event package's kind is imported from Remote Workspace pulls, and which
	// CLOSED merge strategy the import uses. Omitted = not importable.
	Sync *SyncSpec `json:"sync,omitempty"`
	// DefaultEnabled opts the kind into governance on EVERY local boot, without an explicit `--loop`.
	// The boot grants
	// every host-agent principal the kind's observe + scope, so a default-enabled kind is governable
	// from setup alone. Omitted = opt-in (enabled only when named in config.loops / a binding scope).
	DefaultEnabled bool `json:"default_enabled,omitempty"`
	// Risk is the kind's governance risk tier (P3, CLOSED set): "" / "low" = no gate; "mid" requires
	// the candidate to carry non-empty `evidence`; "high" requires an operator (control-agent)
	// principal — an agent's high-risk candidate is denied with a durable diagnostic (Inbox) and a
	// human re-submits. The tier maps to a generated risk-gate rule (define≠select), never a new
	// kernel verdict/state.
	Risk string `json:"risk,omitempty"`
}

// SyncSpec is the external sync-import descriptor: a kind opts into remote import (Importable) and selects a
// merge strategy from the CLOSED set. The strategies encapsulate the per-shape append/conflict
// logic; a kind SELECTS one, it never defines behavior (define≠select).
type SyncSpec struct {
	Importable bool   `json:"importable"`
	Merge      string `json:"merge"` // closed set: see syncMergeStrategies
}

// syncMergeStrategies is the CLOSED set of remote-import merge strategies a spec may select.
var syncMergeStrategies = map[string]bool{"entry-dedup": true, "declaration-dedup": true, "item-dedup": true}

// riskTiers is the CLOSED set of governance risk tiers a spec may select (empty = low = no gate).
var riskTiers = map[string]bool{"low": true, "mid": true, "high": true}

type FieldSpec struct {
	Name       string         `json:"name"`
	Validators []ValidatorRef `json:"validators,omitempty"`
}

type ValidatorRef struct {
	ID     string            `json:"id"`
	Params map[string]string `json:"params,omitempty"`
}

type RenderSpec struct {
	Content *ContentRender    `json:"content,omitempty"` // nil = no rendered content header
	Static  map[string]string `json:"static,omitempty"`  // literal header fields
}

type ContentRender struct {
	Member string            `json:"member"`
	Params map[string]string `json:"params,omitempty"`
}

type eventPackageDefinition struct {
	Name           string
	ObservedType   string
	ProposedType   string
	ResourceKind   contract.ResourceKind
	ItemsField     string
	Fields         []FieldSpec
	Render         RenderSpec
	Required       []string
	Sync           SyncOptions
	SyncSet        bool
	DefaultEnabled bool
	Risk           string
	Source         string
}

// CompileExternalSpec compiles an ExternalSpec into an EventPackage, fail-closed on everything the spec gets
// wrong: unknown/missing core fields, a resource kind outside contract.KindCatalog, duplicate
// field names, unknown validator/render members, bad or extra member params, forward
// default-from references, list:strings sharing a field with other validators, and render keys
// colliding with the reserved items/updated_by keys.
//
// The compiled Decode contract (parity-frozen, external event package spec v1):
//   - ONLY declared fields are processed; payload keys outside the declared set NEVER enter the
//     Item (no leakage into governed state).
//   - For each string field, in declaration order: raw = strings.TrimSpace(stringField(payload,
//     name)); validators run in declared order against the processed value, first error rejects;
//     the processed (trimmed/defaulted) value is what lands in the Item — and EVERY declared
//     string field emits its key (possibly ""), matching the handwritten decoders.
//   - list validators are the exception: they use stringSliceField's full semantics ([]string /
//     []any dropping non-strings / comma-separated string; trimmed, empties compacted) and OMIT
//     the key when the list is empty, except list:strings-required rejects an empty list.
//   - Deny messages are protocol surface: "<name> candidate denied: <member message>".
func CompileExternalSpec(spec ExternalSpec) (EventPackage, error) {
	if spec.SchemaVersion != 1 {
		return EventPackage{}, fmt.Errorf("external event package spec %q: schema_version %d unsupported (want 1)", spec.Name, spec.SchemaVersion)
	}
	var sync SyncOptions
	syncSet := spec.Sync != nil
	if syncSet {
		sync = SyncOptions{Importable: spec.Sync.Importable, Merge: spec.Sync.Merge}
	}
	return compileEventPackage(eventPackageDefinition{
		Name:           spec.Name,
		ObservedType:   spec.ObservedType,
		ProposedType:   spec.ProposedType,
		ResourceKind:   contract.ResourceKind(spec.ResourceKind),
		ItemsField:     spec.ItemsField,
		Fields:         spec.Fields,
		Render:         spec.Render,
		Required:       spec.Required,
		Sync:           sync,
		SyncSet:        syncSet,
		DefaultEnabled: spec.DefaultEnabled,
		Risk:           spec.Risk,
		Source:         "external event package spec",
	})
}

func compileEventPackage(def eventPackageDefinition) (EventPackage, error) {
	source := def.Source
	if source == "" {
		source = "event package"
	}
	kind := string(def.ResourceKind)
	for _, req := range []struct{ name, v string }{
		{"name", def.Name}, {"observed_type", def.ObservedType}, {"proposed_type", def.ProposedType},
		{"resource_kind", kind}, {"items_field", def.ItemsField},
	} {
		if strings.TrimSpace(req.v) == "" {
			return EventPackage{}, fmt.Errorf("%s %q: missing %s", source, def.Name, req.name)
		}
	}
	// Event-type grammar lock: the platform's event types are a
	// CLOSED table of forms over the spec's family segment (eventTypeGrammar). A spec may DECLARE
	// only the two declarable forms — observed_type = <kind>.write_candidate.observed and
	// proposed_type = <kind>.write.proposed — each validated for EQUALITY against the form
	// instantiated with the spec's OWN family, so the event family is bound to the kind, never an
	// open parameter. Without this, a free-form proposed_type compiles, its rule fires, the bridge
	// mints the proposal as a trusted event, and the reconciler (which consumes ONLY *.proposed)
	// silently skips the canonical write: bootable but irreducible. The name doubles as the
	// family segment, so it must use the intake type charset (lowercase, digits, underscore).
	if !specNamePattern.MatchString(def.Name) {
		return EventPackage{}, fmt.Errorf("%s %q: name must match %s (it is the event-family segment)", source, def.Name, specNamePattern.String())
	}
	// Reservation: the system-derived forms (e.g. <kind>.remote_synced_event.observed, the sync-import
	// observation the platform mints) are NEVER spec-declarable — reject them before the equality
	// check so the error names the real reason, not a generic grammar miss.
	for _, decl := range []struct{ role, val string }{{"observed_type", def.ObservedType}, {"proposed_type", def.ProposedType}} {
		for _, form := range eventTypeGrammar {
			if !form.declarable && decl.val == def.Name+form.suffix {
				return EventPackage{}, fmt.Errorf("%s %q: %s %q is a system-derived form, not spec-declarable", source, def.Name, decl.role, decl.val)
			}
		}
	}
	if want := observedEventType(def.Name); def.ObservedType != want {
		return EventPackage{}, fmt.Errorf("%s %q: observed_type %q must be %q (frozen type grammar)", source, def.Name, def.ObservedType, want)
	}
	if want := proposedEventType(def.Name); def.ProposedType != want {
		return EventPackage{}, fmt.Errorf("%s %q: proposed_type %q must be %q (frozen type grammar; the reconciler consumes only *.proposed)", source, def.Name, def.ProposedType, want)
	}
	// G8 reservation: an event package declares its own resource kind — it needs no
	// pre-registration in a compiled catalog (the assembly-time SchemaGuard learns the kind from
	// this spec's required header). But it may NOT claim a kernel-internal governance kind (whose
	// writes are kernel-produced), the reserved `mnemon` namespace, or a reserved system event family
	// whose diagnostics share a domain (sync/session/remote) — else an untrusted package could mint
	// events that confound the control-plane or import-diagnostic families.
	if err := reserveKind(source, def.Name, kind); err != nil {
		return EventPackage{}, err
	}
	declared := map[string]bool{}
	for _, f := range def.Fields {
		if strings.TrimSpace(f.Name) == "" {
			return EventPackage{}, fmt.Errorf("%s %q: field with empty name", source, def.Name)
		}
		if declared[f.Name] {
			return EventPackage{}, fmt.Errorf("%s %q: duplicate field %q", source, def.Name, f.Name)
		}
		isList := false
		for _, v := range f.Validators {
			schema, ok := validatorCatalog[v.ID]
			if !ok {
				return EventPackage{}, fmt.Errorf("%s %q field %q: unknown validator %q (fail-closed)", source, def.Name, f.Name, v.ID)
			}
			if err := checkParams(v.Params, schema); err != nil {
				return EventPackage{}, fmt.Errorf("%s %q field %q validator %q: %w", source, def.Name, f.Name, v.ID, err)
			}
			switch v.ID {
			case "required":
				if s := v.Params["missing_style"]; s != "empty" && s != "missing" {
					return EventPackage{}, fmt.Errorf("%s %q field %q: missing_style %q must be empty|missing", source, def.Name, f.Name, s)
				}
			case "default-from":
				if !declared[v.Params["field"]] {
					return EventPackage{}, fmt.Errorf("%s %q field %q: default-from %q must reference a previously declared field", source, def.Name, f.Name, v.Params["field"])
				}
			case "list:strings", "list:strings-required":
				isList = true
			}
		}
		if isList && len(f.Validators) != 1 {
			return EventPackage{}, fmt.Errorf("%s %q field %q: list validators must be the field's only validator", source, def.Name, f.Name)
		}
		declared[f.Name] = true
	}

	// Render: member + params + reserved-key collision guards.
	produced := map[string]bool{}
	for k := range def.Render.Static {
		produced[k] = true
	}
	if c := def.Render.Content; c != nil {
		schema, ok := renderCatalog[c.Member]
		if !ok {
			return EventPackage{}, fmt.Errorf("%s %q: unknown render %q (fail-closed)", source, def.Name, c.Member)
		}
		if err := checkParams(c.Params, schema); err != nil {
			return EventPackage{}, fmt.Errorf("%s %q render %q: %w", source, def.Name, c.Member, err)
		}
		if c.Member == "bullet-list" && !declared[c.Params["field"]] {
			return EventPackage{}, fmt.Errorf("%s %q render bullet-list: field %q not declared", source, def.Name, c.Params["field"])
		}
		if produced["content"] {
			return EventPackage{}, fmt.Errorf("%s %q: render static and content slot both produce \"content\"", source, def.Name)
		}
		produced["content"] = true
	}
	for k := range produced {
		if k == def.ItemsField || k == "updated_by" {
			return EventPackage{}, fmt.Errorf("%s %q: render key %q collides with a reserved resource key", source, def.Name, k)
		}
	}

	// Required-derivation: a kind's kernel-required header fields are the
	// render-produced keys, or — when `required` is declared — exactly that subset. A declared
	// field that the render never produces is unsatisfiable (no write would carry it), so reject it.
	required, err := requiredHeader(source, def.Name, def.Required, produced)
	if err != nil {
		return EventPackage{}, err
	}

	// Sync descriptor: an importable kind selects a merge strategy from the CLOSED set.
	sync := def.Sync
	if def.SyncSet {
		if sync.Importable && !syncMergeStrategies[sync.Merge] {
			return EventPackage{}, fmt.Errorf("%s %q: sync merge %q not in the closed set (entry-dedup|declaration-dedup|item-dedup)", source, def.Name, sync.Merge)
		}
		if !sync.Importable && sync.Merge != "" {
			return EventPackage{}, fmt.Errorf("%s %q: sync merge %q set on a non-importable kind", source, def.Name, sync.Merge)
		}
	}

	// Risk tier: select from the CLOSED set (empty = low = no gate).
	risk := def.Risk
	if risk == "" {
		risk = "low"
	}
	if !riskTiers[risk] {
		return EventPackage{}, fmt.Errorf("%s %q: risk %q not in the closed set (low|mid|high)", source, def.Name, def.Risk)
	}
	return EventPackage{
		Name:           def.Name,
		ObservedType:   def.ObservedType,
		ProposedType:   def.ProposedType,
		ResourceKind:   def.ResourceKind,
		ItemsField:     def.ItemsField,
		Decode:         compileDecode(def.Name, def.Fields),
		Header:         compileHeader(def.Render),
		RequiredHeader: required,
		Risk:           risk,
		Sync:           sync,
		DefaultEnabled: def.DefaultEnabled,
	}, nil
}

// requiredHeader resolves a spec's kernel-required header fields: the declared `required` subset
// (each entry validated to be a render-produced key), or every produced key sorted when omitted.
func requiredHeader(source, name string, required []string, produced map[string]bool) ([]string, error) {
	if len(required) > 0 {
		out := make([]string, 0, len(required))
		for _, f := range required {
			if !produced[f] {
				return nil, fmt.Errorf("%s %q: required field %q is not one the render produces (fail-closed)", source, name, f)
			}
			out = append(out, f)
		}
		return out, nil
	}
	out := make([]string, 0, len(produced))
	for k := range produced {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

// LoadSpec reads capabilities/<name>.json from fsys and strictly decodes it into its external DATA form,
// for consumers that need the spec itself rather than the compiled EventPackage (e.g. the SKILL
// payload-contract generator). It goes through decodeSpec — the one fail-closed decode path —
// so there is no second, weaker decoding scheme to drift from it.
func LoadSpec(fsys fs.FS, name string) (ExternalSpec, error) {
	raw, err := fs.ReadFile(fsys, path.Join("capabilities", name+".json"))
	if err != nil {
		return ExternalSpec{}, fmt.Errorf("read external event package spec %s: %w", name, err)
	}
	spec, err := decodeSpec(raw)
	if err != nil {
		return ExternalSpec{}, fmt.Errorf("parse external event package spec %s: %w", name, err)
	}
	return spec, nil
}

// decodeSpec is the ONE way a ExternalSpec is read from JSON: DisallowUnknownFields makes the
// frozen protocol surface fail-closed at the SYNTAX level too — an unknown key anywhere (top
// level, field object, validator object, render object) rejects the spec instead of silently
// compiling a typo into default behavior. Production loading and the golden tests share it.
func decodeSpec(raw []byte) (ExternalSpec, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var spec ExternalSpec
	if err := dec.Decode(&spec); err != nil {
		return ExternalSpec{}, err
	}
	// Exactly ONE JSON value: Decoder.Decode reads the first value and would silently ignore
	// anything after it ({spec}{garbage} would pass) — LOOSER than the frozen fail-closed
	// contract allows. Require io.EOF on a second read.
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); err != io.EOF {
		return ExternalSpec{}, fmt.Errorf("trailing data after external event package spec (want a single JSON object)")
	}
	return spec, nil
}

// specNamePattern pins event package names to the intake event-type segment charset (server-side
// validateObservedType allows [a-z0-9._]) — a name is the event-family segment by frozen grammar.
var specNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// reservedKindFamilies are system event families whose `<family>.diagnostic` / `<family>.*`
// events the platform mints (sync-import skip, host session, remote material). A declared kind here
// would let an untrusted package emit events the runtime routes by first-segment domain (G8).
var reservedKindFamilies = map[string]bool{"sync": true, "session": true, "remote": true}

// reserveKind is the G8 namespace gate for a declared resource kind: reject a
// governance kind, the `mnemon` namespace, or a reserved system event family.
func reserveKind(source, name, kind string) error {
	if contract.GovernanceKinds[contract.ResourceKind(kind)] {
		return fmt.Errorf("%s %q: resource_kind %q is a reserved kernel-internal governance kind (fail-closed)", source, name, kind)
	}
	if kind == "mnemon" || strings.HasPrefix(kind, "mnemon_") {
		return fmt.Errorf("%s %q: resource_kind %q uses the reserved mnemon namespace (fail-closed)", source, name, kind)
	}
	if reservedKindFamilies[kind] {
		return fmt.Errorf("%s %q: resource_kind %q is a reserved system event family (fail-closed)", source, name, kind)
	}
	return nil
}

// eventTypeGrammar is the CLOSED table of event-type forms the platform recognises, each a suffix
// over an event package's family segment (= its kind). `declarable` forms are what an external author
// may write in a spec (observed_type / proposed_type), validated for equality against the family;
// non-declarable forms are SYSTEM-DERIVED — the platform mints them and CompileExternalSpec rejects any spec
// that tries to declare one. New event families are added here (a table row), not by reshaping the
// compile path — the G7 extension point. The sync-import observation form is wired in PD6.
type eventTypeForm struct {
	suffix     string
	declarable bool
}

var eventTypeGrammar = []eventTypeForm{
	{suffix: eventTypeObservedSuffix, declarable: true},
	{suffix: eventTypeProposedSuffix, declarable: true},
	{suffix: ".remote_synced_event.observed", declarable: false}, // sync-import observation (system-derived; PD6)
}

const (
	eventTypeObservedSuffix = ".write_candidate.observed"
	eventTypeProposedSuffix = ".write.proposed"
)

func observedEventType(name string) string { return name + eventTypeObservedSuffix }

func proposedEventType(name string) string { return name + eventTypeProposedSuffix }

type paramSchema struct{ required, optional []string }

func checkParams(params map[string]string, schema paramSchema) error {
	allowed := map[string]bool{}
	for _, k := range schema.required {
		if strings.TrimSpace(params[k]) == "" {
			return fmt.Errorf("missing param %q", k)
		}
		allowed[k] = true
	}
	for _, k := range schema.optional {
		allowed[k] = true
	}
	for k := range params {
		if !allowed[k] {
			return fmt.Errorf("unknown param %q (fail-closed)", k)
		}
	}
	return nil
}
