# Loop Package v2 — the loop-author face

> Revises `loop-package-v1.md` under the R1 no-forward-compat channel (P2, 2026-06-12). The headline
> change: an EXTERNAL package may now carry host projection assets (it was admission-equal only in
> v1). harness/ has no external loop authors yet; v2 makes the breaking change P2 needs and carries
> a version number. Independent review: the three P2 face texts (this, capability-spec-v2,
> sync-abi-v2) get a dedicated adversarial doc-text review at P2 close (PD9), after their last
> amendment — that review is this revision's freeze condition.
>
> **What changed from v1** (everything else in v1 still holds — read it for the hook-intent
> vocabulary, the SKILL generation rule, and directory-as-declaration):
> 1. External packages carry host assets (loop.json / GUIDE / hooks / skills / runtime files) — the
>    v1 §"External capability packages" admission-equal-only restriction is lifted (D4).
> 2. The three CODE faces stay closed for external packages: hook `fragments`, the `include` intent,
>    and a `template.json` `external_id_recipe`. v1 §Fragments named these as the binding stage-5
>    obligation; v2 keeps them embedded-only and makes the rejection explicit (not a render-time
>    path-miss).
> 3. Prose assets (GUIDE.md, SKILL.md) carry a documentation-grade injection scan, not the
>    content-grade secret scan (a legitimate GUIDE may honestly discuss "private keys").
> 4. The v1 "sync-import stays fixed-embedded-only … external capabilities have no remote producer"
>    sentence is superseded by `sync-abi-v2.md` (PD6, descriptor-derived sync).

## Package contents (external packages, v2)

An external package under `.mnemon/loops/<name>/` may now mirror an embedded loop package:

```text
.mnemon/loops/<name>/
  capability.json      capability-spec-v2, strict-decoded by the SAME decodeSpec + FromSpec
  loop.json            projection manifest (declarative surfaces; v2 fields land with the projector, PD4)
  GUIDE.md             teaching prose (projected; documentation-grade injection scan)
  hooks/intents.json   WHAT each lifecycle hook does (closed vocabulary; include intent forbidden)
  skills/<id>/SKILL.md judgment prose + payload-contract marker (documentation-grade injection scan)
  skills/<id>/template.json  enum docs (external_id_recipe forbidden — see Closed code faces)
  runtime files        host-neutral runtime surface assets
```

Directory-as-declaration is unchanged: directory == name == kind, `^[a-z][a-z0-9_]*$`. The kernel
governance kinds and the reserved namespaces are unchanged (capability-spec-v2 §G8).

## Closed code faces (embedded-only; external = fail-closed)

Three sub-faces are shell-by-design or splice executable text; they remain valid ONLY in embedded,
reviewed loop packages, and an external package carrying any of them fails closed at load, naming
the package path:

- **Hook fragments** (`hooks/fragments/*.sh`): concatenated verbatim into a generated hook. The hook
  renderer reads fragments EXCLUSIVELY from the embedded asset FS — never from an external package's
  asset root — so an external fragment is unreadable by construction AND its directory presence is
  rejected at load.
- **The `include` intent** (`hooks/intents.json` section `type: include`): splices a fragment. An
  external intents.json declaring it is rejected at load, not left to a render-time path-miss.
- **A `template.json` `external_id_recipe`**: a one-line shell recipe spliced into a bash fence the
  agent is taught to run. An external template carrying it is rejected at load. (A future closed
  recipe vocabulary — `{timestamp, uuid, slug}` — may reopen this additively.)

The first (fragment directory presence) and the prose scan below are enforced by the capability
loader (PD3). The deeper intents/template checks (the `include` section, the recipe, and that each
control-observe action's `event_type` equals the package's own `observed_type`) are enforced by the
projection loader where the schema-aware parsers live (setup/refresh) — fail-loud at load (PD4).

## Prose scanning (documentation-grade)

GUIDE.md and SKILL.md are projected verbatim into host context, so their free text is scanned for
prompt-injection SHAPE (the closed marker set: "ignore previous instructions", "reveal the system
prompt", …). They are NOT run through the content-grade secret scanner: documentation legitimately
discusses secrets ("never store API keys in memory") and a secret-marker substring match would
fail-close honest security guidance. Embedded GUIDEs are reviewed code and unscanned; external prose
is untrusted input and scanned, fail-closed, naming the package path.

## loop.json v2 declarative fields

v2 moves the per-loop HOOK / STORE / ENV / STATE-DIR special-casing out of the projector into
closed-set loop.json declarations the projector consumes:

- `hook_options` — the `{remind, nudge, compact}` flags; `LoopManifest.HasHooks()` is the
  declarative replacement for the hardcoded `loop.Name == "memory" || "skill"` hooks-enabled gate.
- `store` — `{native: true}`, the store-backed gate that was hardcoded to memory.
- `env` — host-neutral runtime env vars, names namespaced `^MNEMON_…` and values in a CLOSED
  shell-safe grammar (closed projector vars `${state_dir}`/`${host_skills_dir}`, runtime bash refs,
  safe literals) — the env injection lock, so an external package can never splice shell into a
  sourced file.
- `state_dirs` — loop state directories the projector creates at install (safe relative paths).

The dead `host_adapters` field is removed in the same revision.

**Not yet declarative (deferred — the "投影器特判" residue PD4 left to a later, golden-managed pass):**
MIRROR regeneration is still kind-hardcoded (`mirror.go` regenerates only `memory` content; there is
NO `surfaces.mirror` field), and a few host operator actions remain special-cased by loop name
(skill-view purge, the memory runtime-file skip, memory purge). The projector is generic for
hooks/store/env/state-dirs, not yet for mirror/purge.

## Enforcement map (v2)

| obligation | enforcement |
|---|---|
| external package carries host assets | ALLOWED (v2) — `hooks/`/`skills/` presence no longer rejects |
| `hooks/fragments/` present → fail closed | capability loader: directory presence rejected (PD3) |
| GUIDE.md / SKILL.md prose | capability loader: documentation-grade injection scan (PD3) |
| `include` intent → fail closed | projection loader: rejected at setup/refresh, fail-loud (PD4 ✓) |
| `template.json` `external_id_recipe` → fail closed | projection loader: rejected (PD4 ✓) |
| control-observe `event_type` ∈ {session.observed, own family} | projection loader: confused-deputy guard (PD4 ✓) |
| loop.json `env` shell-safe grammar + namespaced names | projector env sink: closed grammar (PD4) |
| strict spec decode / no shadowing / untrusted spec surfaces / no symlinks | unchanged from v1 (capability loader) |

A bad package still REFUSES `local run` boot; `--ignore-external` is the operator escape hatch;
`loop validate` reports each loadable package and goes red on any loader failure.
