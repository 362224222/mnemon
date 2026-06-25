# Loop Package v1 (frozen) — the loop-author face

> Superseded by `loop-package-v2.md` (P2, 2026-06-12). v2 lets an external package carry host
> projection assets (this document's admission-equal-only restriction is lifted), while keeping the
> three code faces — hook fragments, the `include` intent, and a `template.json` recipe — embedded-
> only. This document remains the v1 record; the live loader/projector follow v2.

What a loop package may carry and what each part means. Together with capability-spec-v1 this is
the complete authoring surface for a loop; host-mechanics-v1 is the separate face host-adapter
authors consume. Loop packages are 100% host-neutral — nothing in a package may name a host.

## Package contents

```text
loops/<name>/
  loop.json            projection manifest (assets, surfaces, control model)   [stage-2 era]
  capability ref       assets/capabilities/<kind>.json (capability-spec-v1)
  hooks/intents.json   WHAT each lifecycle hook does (this document)
  hooks/fragments/*.sh imperative escape hatch, stitched at GENERATION time
  skills/<id>/SKILL.md judgment prose + <!-- mnemon:payload-contract --> marker
  skills/<id>/template.json  enum docs + external-id recipe feeding the generated contract
  GUIDE.md, env.sh     teaching + runtime surface assets
```

## Hook intents (closed vocabulary)

`hooks/intents.json` (schema_version 1, strict-decoded: unknown keys/members/params, trailing
data, wrong schema_version all fail closed):

- **Timings**: `prime | remind | nudge | compact` — the four lifecycle moments.
- **Gates** (per timing, ordered): `once-per-session-marker{marker}` · `two-phase-marker{marker}`
  · `if-input-field{field}` · `threshold{metric: file-non-empty-lines|usage-event-count,
  cmp: gt|ge, …env/default params}`.
- **Sections** (ordered): `env-prologue` · `local-env-control` · `control-env` · `banner{lines}` ·
  `control-call{actions}` · `file-emit{var,path,header}` · `include{fragment}`; control actions:
  `observe{event_type, external_id_prefix, payload}` · `status` · `pull-mirror{…}`.
- **Response** (per timing): `role: one-liner | message | block` with `text` or threshold-selected
  `over`/`under` slots.
- **Wording convention**: intents carry the canonical default text for every slot; hosts override
  via host.json `wording_overrides` (host-mechanics-v1). Slots are PROSE ONLY — the decoder rejects
  `"`, `` ` ``, `\`, newlines and `$(`; everything else is inert because every slot
  interpolation site in the compiled templates is double-quoted (that quoting context is part of
  the frozen template contract).

The vocabulary is CLOSED: a member not listed here does not exist; adding one is a versioned
change to this document plus the compiled catalog.

## Fragments (frozen restriction)

Fragments are loop-side shell bodies referenced by `include{fragment}`. They are concatenated
into the generated hook at GENERATION time and never evaluated by the generator or the runtime.
**v1: fragments are valid only in EMBEDDED loop packages.** Originally this was enforced
structurally (the renderer reads fragments exclusively from the embedded asset FS, and no
external loader existed); since stage 5 the external loader also enforces it directly — fault
class ⑥ rejects any `hooks/` or `skills/` presence in an external package.
**Binding stage-5 obligation: any external-package loader MUST reject a package containing
`hooks/fragments/`, an `include` intent, or a `skills/*/template.json` whose recipe/notes were not
shipped embedded — fail closed, with a regression test, before external packages gain "same
rights" loading.** (template.json recipe/notes are LLM-facing and recipe is shell-by-design; they
carry the same trust requirement as fragments.) Relaxing any of this requires a new version of
this document. (Stage 5 discharged this obligation, wider than the minimum: see "External
capability packages (v1, landed)" — fault class ⑥ rejects ANY `hooks/` or `skills/` presence.)

## SKILL generation rule

`SKILL.md` keeps frontmatter + judgment prose (when to use, what to reject, confidence guidance)
and marks the payload-mechanics position with `<!-- mnemon:payload-contract -->`. At projection
the marker is replaced by a section GENERATED from the capability spec (fields, requiredness,
enum values, safety scans) plus `template.json` (external-id recipe, enum docs). Single source:
a spec field rename changes the projected SKILL or breaks the token-coverage gate — there is no
reverse dependency. Skill template-instance renaming machinery (`<loop>-set/get` for arbitrary
loops) is deliberately deferred until an external package needs it.

## External capability packages (v1, landed)

Stage 5 landed the external-package loader. An external package is a directory under the PROJECT
ROOT — `.mnemon/loops/` is the ONLY external root in v1:

```text
.mnemon/loops/<name>/
  capability.json      capability-spec-v1, strict-decoded by the SAME decodeSpec+FromSpec
                       machinery as embedded specs
  GUIDE.md, docs       optional and inert — never loaded by the harness
```

**Directory-as-declaration**: the package directory name IS the capability name. It must equal
`capability.json`'s `name` AND its `resource_kind` — **directory == name == kind for v1** — and
match `^[a-z][a-z0-9_]*$` — the ONE grammar shared with the spec name / event-family segment
(capability-spec-v1 Type grammar; no dash) — (fault class ⑨ — kills case aliasing, path-meaningful names, and
name/kind divergence: enablement derives the catalog entry from the binding scope KIND, so a
divergent package would be unreachable or confusing). Putting the directory in place declares the
capability; enabling it is the same `config.loops` + binding scope/types edit the note/decision
external packages use as the worked precedent. (Errata: this sentence previously called
note/decision an embedded precedent; they were demoted from embedded builtins to external-package
fixtures in P1 — the enablement mechanism is unchanged.) The kernel-internal kinds `lease`,
`budget`, `receipt`, `coordination` are
control-plane job/coordination lanes and may never be claimed by an external package (fault
class ⑪, fail-closed at load).

**Admission-equal rights only — an operator-visible deviation, stated openly.** An external
package is the EQUAL of an embedded capability for admission and governance (same generic kind,
same fail-closed compile, same kernel authority derivation, same pull surface), but it carries
NO host projection assets in v1: no hooks, no skills, no GUIDE projection. `setup --loop
<external>` fails with the pinned message `external packages carry no host assets; enable via
config.loops + binding`; refresh/uninstall never touch external packages. Projection-equal
rights require a new version of this document.

The loader is fail-closed end to end (`capability.LoadExternal` + `capability.ResolveCatalog`,
table-tested in `external_test.go`; every error names the package path). Each obligation of this
document maps to an enforcing fault class:

| obligation | enforcement |
|---|---|
| `hooks/fragments/` present → fail closed | class ⑥, deliberately WIDER: ANY `hooks/` presence (even empty) rejects the whole package |
| `include` intent → fail closed | subsumed by class ⑥: no `hooks/` may exist, so no intents.json is ever read |
| `skills/*/template.json` not shipped embedded → fail closed | class ⑥, WIDER: ANY `skills/` presence rejects the package |
| strict spec decode | class ① bad JSON / trailing data / unknown keys (decodeSpec); ② unknown vocabulary, ③ kind outside KindCatalog (FromSpec) |
| no shadowing | class ④ four-axis merge rejection — name, observed type, proposed type, resource kind — external may not claim what embedded claims; ⑤ two externals may not collide either (incl. sharing a kind) |
| kernel-satisfiable | class ⑦ load-time SchemaGuard lockstep: statically derived header keys (static ∪ content ∪ items_field ∪ updated_by) must cover the kind's required fields |
| untrusted spec surfaces | class ⑧, EXTERNAL ONLY, two halves. VALUES → scanned by the secret + prompt-injection scanners: enum deny messages, `default` validator values, render static values, the bullet-list title. IDENTIFIERS → pattern-locked to `^[a-z][a-z0-9_-]*$` (underscore allowed for descriptor field names such as `items_field`): field names, `items_field`, render static keys. The spec `name` is pattern-locked via directory == name (class ⑨) and scanned as belt-and-braces |
| no kernel-internal kinds | class ⑪: `lease`/`budget`/`receipt`/`coordination` are deny-listed for external claim |
| no symlinks | class ⑩: a symlinked external root, package dir, or capability.json is rejected by ResolveCatalog's lstat screening on the real path |

A bad package REFUSES `local run` boot — the directory's presence is a contract, not a hint;
`local run --ignore-external` is the operator escape hatch (embedded-only catalog, each ignored
package named on stderr). `loop validate` reports each loadable package as
`external capability <name>: OK` and goes red on any loader failure. In v1, sync-import stayed
limited to a fixed embedded set — narrower than the catalog: pushes were kind-agnostic, but the
puller imported only that fixed set and dropped every other kind; external capabilities had no
remote producer in v1. This is superseded by `sync-abi-v2.md`.

## Migration provenance

The generated hooks were proven byte-identical to the 16 retired handwritten shells (empty patch
table) before the legacy assets were deleted; the standing pin is the golden-hash table in
hookgen's tests. Two deliberate unifications are recorded in history: claude compact reason
escaping (closed an injection face) and claude skill prime session-dedup marker.
