package capability

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

// CapabilitySpec is the DATA form of a built-in capability: what a capability author declares in
// assets/capabilities/<name>.json. FromSpec compiles it against the two CLOSED catalogs
// (validators, renders) — a spec can only SELECT compiled members, never define behavior (A3/I8);
// anything unknown fails closed.
type CapabilitySpec struct {
	SchemaVersion int         `json:"schema_version"` // capability spec v1
	Name          string      `json:"name"`
	ObservedType  string      `json:"observed_type"`
	ProposedType  string      `json:"proposed_type"`
	ResourceKind  string      `json:"resource_kind"`
	ItemsField    string      `json:"items_field"`
	Fields        []FieldSpec `json:"fields"`
	Render        RenderSpec  `json:"render"`
	// Required SELECTS the kind's kernel-required header fields from the render-produced keys
	// (capability-spec v2 §Declared kind). Omitted = every produced key is required; when present,
	// each entry must be a render-produced key (a kind cannot require a field its writes never carry).
	// It is the single source the assembly-time SchemaGuard derives a user kind's required set from.
	Required []string `json:"required,omitempty"`
	// Sync declares whether this capability's kind is imported from Remote Workspace pulls, and which
	// CLOSED merge strategy the import uses (capability-spec v2 §Sync). Omitted = not importable.
	Sync *SyncSpec `json:"sync,omitempty"`
	// DefaultEnabled opts the kind into governance on EVERY local boot, without an explicit `--loop`
	// (P3: the coordination package is on out of the box; memory/skill stay opt-in). The boot grants
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

// SyncSpec is the sync-import descriptor: a kind opts into remote import (Importable) and selects a
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

// FromSpec compiles a CapabilitySpec into a Capability, fail-closed on everything the spec gets
// wrong: unknown/missing core fields, a resource kind outside contract.KindCatalog, duplicate
// field names, unknown validator/render members, bad or extra member params, forward
// default-from references, list:strings sharing a field with other validators, and render keys
// colliding with the reserved items/updated_by keys.
//
// The compiled Decode contract (parity-frozen, capability spec v1):
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
func FromSpec(spec CapabilitySpec) (Capability, error) {
	if spec.SchemaVersion != 1 {
		return Capability{}, fmt.Errorf("capability spec %q: schema_version %d unsupported (want 1)", spec.Name, spec.SchemaVersion)
	}
	for _, req := range []struct{ name, v string }{
		{"name", spec.Name}, {"observed_type", spec.ObservedType}, {"proposed_type", spec.ProposedType},
		{"resource_kind", spec.ResourceKind}, {"items_field", spec.ItemsField},
	} {
		if strings.TrimSpace(req.v) == "" {
			return Capability{}, fmt.Errorf("capability spec %q: missing %s", spec.Name, req.name)
		}
	}
	// Event-type grammar lock (capability-spec v2 §Grammar): the platform's event types are a
	// CLOSED table of forms over the spec's family segment (eventTypeGrammar). A spec may DECLARE
	// only the two declarable forms — observed_type = <kind>.write_candidate.observed and
	// proposed_type = <kind>.write.proposed — each validated for EQUALITY against the form
	// instantiated with the spec's OWN family, so the event family is bound to the kind, never an
	// open parameter. Without this, a free-form proposed_type compiles, its rule fires, the bridge
	// mints the proposal as a trusted event, and the reconciler (which consumes ONLY *.proposed)
	// silently skips the canonical write: bootable but irreducible. The name doubles as the
	// family segment, so it must use the intake type charset (lowercase, digits, underscore).
	if !specNamePattern.MatchString(spec.Name) {
		return Capability{}, fmt.Errorf("capability spec %q: name must match %s (it is the event-family segment)", spec.Name, specNamePattern.String())
	}
	// Reservation: the system-derived forms (e.g. <kind>.remote_commit.observed, the sync-import
	// observation the platform mints) are NEVER spec-declarable — reject them before the equality
	// check so the error names the real reason, not a generic grammar miss.
	for _, decl := range []struct{ role, val string }{{"observed_type", spec.ObservedType}, {"proposed_type", spec.ProposedType}} {
		for _, form := range eventTypeGrammar {
			if !form.declarable && decl.val == spec.Name+form.suffix {
				return Capability{}, fmt.Errorf("capability spec %q: %s %q is a system-derived form, not spec-declarable", spec.Name, decl.role, decl.val)
			}
		}
	}
	if want := spec.Name + eventTypeObservedSuffix; spec.ObservedType != want {
		return Capability{}, fmt.Errorf("capability spec %q: observed_type %q must be %q (frozen type grammar)", spec.Name, spec.ObservedType, want)
	}
	if want := spec.Name + eventTypeProposedSuffix; spec.ProposedType != want {
		return Capability{}, fmt.Errorf("capability spec %q: proposed_type %q must be %q (frozen type grammar; the reconciler consumes only *.proposed)", spec.Name, spec.ProposedType, want)
	}
	// G8 reservation (capability-spec v2): a spec DECLARES its own resource kind — it needs no
	// pre-registration in a compiled catalog (the assembly-time SchemaGuard learns the kind from
	// this spec's required header). But it may NOT claim a kernel-internal governance kind (whose
	// writes are kernel-produced), the reserved `mnemon` namespace, or a first-party event family
	// whose diagnostics share a domain (sync/session/remote) — else an untrusted package could mint
	// events that confound the control-plane or import-diagnostic families.
	if err := reserveKind(spec.Name, spec.ResourceKind); err != nil {
		return Capability{}, err
	}
	declared := map[string]bool{}
	for _, f := range spec.Fields {
		if strings.TrimSpace(f.Name) == "" {
			return Capability{}, fmt.Errorf("capability spec %q: field with empty name", spec.Name)
		}
		if declared[f.Name] {
			return Capability{}, fmt.Errorf("capability spec %q: duplicate field %q", spec.Name, f.Name)
		}
		isList := false
		for _, v := range f.Validators {
			schema, ok := validatorCatalog[v.ID]
			if !ok {
				return Capability{}, fmt.Errorf("capability spec %q field %q: unknown validator %q (fail-closed)", spec.Name, f.Name, v.ID)
			}
			if err := checkParams(v.Params, schema); err != nil {
				return Capability{}, fmt.Errorf("capability spec %q field %q validator %q: %w", spec.Name, f.Name, v.ID, err)
			}
			switch v.ID {
			case "required":
				if s := v.Params["missing_style"]; s != "empty" && s != "missing" {
					return Capability{}, fmt.Errorf("capability spec %q field %q: missing_style %q must be empty|missing", spec.Name, f.Name, s)
				}
			case "default-from":
				if !declared[v.Params["field"]] {
					return Capability{}, fmt.Errorf("capability spec %q field %q: default-from %q must reference a previously declared field", spec.Name, f.Name, v.Params["field"])
				}
			case "list:strings", "list:strings-required":
				isList = true
			}
		}
		if isList && len(f.Validators) != 1 {
			return Capability{}, fmt.Errorf("capability spec %q field %q: list validators must be the field's only validator", spec.Name, f.Name)
		}
		declared[f.Name] = true
	}

	// Render: member + params + reserved-key collision guards.
	produced := map[string]bool{}
	for k := range spec.Render.Static {
		produced[k] = true
	}
	if c := spec.Render.Content; c != nil {
		schema, ok := renderCatalog[c.Member]
		if !ok {
			return Capability{}, fmt.Errorf("capability spec %q: unknown render %q (fail-closed)", spec.Name, c.Member)
		}
		if err := checkParams(c.Params, schema); err != nil {
			return Capability{}, fmt.Errorf("capability spec %q render %q: %w", spec.Name, c.Member, err)
		}
		if c.Member == "bullet-list" && !declared[c.Params["field"]] {
			return Capability{}, fmt.Errorf("capability spec %q render bullet-list: field %q not declared", spec.Name, c.Params["field"])
		}
		if produced["content"] {
			return Capability{}, fmt.Errorf("capability spec %q: render static and content slot both produce \"content\"", spec.Name)
		}
		produced["content"] = true
	}
	for k := range produced {
		if k == spec.ItemsField || k == "updated_by" {
			return Capability{}, fmt.Errorf("capability spec %q: render key %q collides with a reserved resource key", spec.Name, k)
		}
	}

	// Required-derivation (capability-spec v2): a kind's kernel-required header fields are the
	// render-produced keys, or — when `required` is declared — exactly that subset. A declared
	// field that the render never produces is unsatisfiable (no write would carry it), so reject it.
	required, err := requiredHeader(spec, produced)
	if err != nil {
		return Capability{}, err
	}

	// Sync descriptor: an importable kind selects a merge strategy from the CLOSED set (fail-closed
	// on an unknown strategy or a non-importable kind that names one).
	var sync SyncOptions
	if spec.Sync != nil {
		sync = SyncOptions{Importable: spec.Sync.Importable, Merge: spec.Sync.Merge}
		if sync.Importable && !syncMergeStrategies[sync.Merge] {
			return Capability{}, fmt.Errorf("capability spec %q: sync merge %q not in the closed set (entry-dedup|declaration-dedup|item-dedup)", spec.Name, sync.Merge)
		}
		if !sync.Importable && sync.Merge != "" {
			return Capability{}, fmt.Errorf("capability spec %q: sync merge %q set on a non-importable kind", spec.Name, sync.Merge)
		}
	}

	// Risk tier: select from the CLOSED set (empty = low = no gate).
	risk := spec.Risk
	if risk == "" {
		risk = "low"
	}
	if !riskTiers[risk] {
		return Capability{}, fmt.Errorf("capability spec %q: risk %q not in the closed set (low|mid|high)", spec.Name, spec.Risk)
	}
	// S4/G2: the loopdef kind (the D-loop's event-model-evolution kind) is permanently high-risk — a
	// loopdef spec (first-party, or one that arrives synced/materialized) may not declare a lower tier
	// and so dodge the operator gate.
	if spec.ResourceKind == "loopdef" && risk != "high" {
		return Capability{}, fmt.Errorf("capability spec %q: a loopdef kind must be risk:high (G2, non-overridable)", spec.Name)
	}

	return Capability{
		Name:           spec.Name,
		ObservedType:   spec.ObservedType,
		ProposedType:   spec.ProposedType,
		ResourceKind:   contract.ResourceKind(spec.ResourceKind),
		ItemsField:     spec.ItemsField,
		Decode:         compileDecode(spec),
		Header:         compileHeader(spec),
		RequiredHeader: required,
		Risk:           risk,
		Sync:           sync,
		DefaultEnabled: spec.DefaultEnabled,
	}, nil
}

// requiredHeader resolves a spec's kernel-required header fields: the declared `required` subset
// (each entry validated to be a render-produced key), or every produced key sorted when omitted.
func requiredHeader(spec CapabilitySpec, produced map[string]bool) ([]string, error) {
	if len(spec.Required) > 0 {
		out := make([]string, 0, len(spec.Required))
		for _, f := range spec.Required {
			if !produced[f] {
				return nil, fmt.Errorf("capability spec %q: required field %q is not one the render produces (fail-closed)", spec.Name, f)
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

// LoadSpec reads capabilities/<name>.json from fsys and strictly decodes it into its DATA form,
// for consumers that need the spec itself rather than the compiled Capability (e.g. the SKILL
// payload-contract generator). It goes through decodeSpec — the one fail-closed decode path —
// so there is no second, weaker decoding scheme to drift from it.
func LoadSpec(fsys fs.FS, name string) (CapabilitySpec, error) {
	raw, err := fs.ReadFile(fsys, path.Join("capabilities", name+".json"))
	if err != nil {
		return CapabilitySpec{}, fmt.Errorf("read capability spec %s: %w", name, err)
	}
	spec, err := decodeSpec(raw)
	if err != nil {
		return CapabilitySpec{}, fmt.Errorf("parse capability spec %s: %w", name, err)
	}
	return spec, nil
}

// decodeSpec is the ONE way a CapabilitySpec is read from JSON: DisallowUnknownFields makes the
// frozen protocol surface fail-closed at the SYNTAX level too — an unknown key anywhere (top
// level, field object, validator object, render object) rejects the spec instead of silently
// compiling a typo into default behavior. Production loading and the golden tests share it.
func decodeSpec(raw []byte) (CapabilitySpec, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var spec CapabilitySpec
	if err := dec.Decode(&spec); err != nil {
		return CapabilitySpec{}, err
	}
	// Exactly ONE JSON value: Decoder.Decode reads the first value and would silently ignore
	// anything after it ({spec}{garbage} would pass) — LOOSER than the frozen fail-closed
	// contract allows. Require io.EOF on a second read.
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); err != io.EOF {
		return CapabilitySpec{}, fmt.Errorf("trailing data after capability spec (want a single JSON object)")
	}
	return spec, nil
}

// specNamePattern pins capability names to the intake event-type segment charset (server-side
// validateObservedType allows [a-z0-9._]) — a name is the event-family segment by frozen grammar.
var specNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// reservedKindFamilies are first-party event families whose `<family>.diagnostic` / `<family>.*`
// events the platform mints (sync-import skip, host session, remote commit). A declared kind here
// would let an untrusted package emit events the runtime routes by first-segment domain (G8).
var reservedKindFamilies = map[string]bool{"sync": true, "session": true, "remote": true}

// reserveKind is the G8 namespace gate for a declared resource kind (capability-spec v2): reject a
// governance kind, the `mnemon` namespace, or a reserved first-party event family.
func reserveKind(name, kind string) error {
	if contract.GovernanceKinds[contract.ResourceKind(kind)] {
		return fmt.Errorf("capability spec %q: resource_kind %q is a reserved kernel-internal governance kind (fail-closed)", name, kind)
	}
	if kind == "mnemon" || strings.HasPrefix(kind, "mnemon_") {
		return fmt.Errorf("capability spec %q: resource_kind %q uses the reserved mnemon namespace (fail-closed)", name, kind)
	}
	if reservedKindFamilies[kind] {
		return fmt.Errorf("capability spec %q: resource_kind %q is a reserved first-party event family (fail-closed)", name, kind)
	}
	return nil
}

// eventTypeGrammar is the CLOSED table of event-type forms the platform recognises, each a suffix
// over a capability's family segment (= its kind). `declarable` forms are what a capability author
// may write in a spec (observed_type / proposed_type), validated for equality against the family;
// non-declarable forms are SYSTEM-DERIVED — the platform mints them and FromSpec rejects any spec
// that tries to declare one. New event families are added here (a table row), not by reshaping the
// compile path — the G7 extension point. The sync-import observation form is wired in PD6.
type eventTypeForm struct {
	suffix     string
	declarable bool
}

var eventTypeGrammar = []eventTypeForm{
	{suffix: eventTypeObservedSuffix, declarable: true},
	{suffix: eventTypeProposedSuffix, declarable: true},
	{suffix: ".remote_commit.observed", declarable: false}, // sync-import observation (system-derived; PD6)
}

const (
	eventTypeObservedSuffix = ".write_candidate.observed"
	eventTypeProposedSuffix = ".write.proposed"
)

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
