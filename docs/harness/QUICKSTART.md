# mnemon-harness — Quickstart

Two paths, each from nothing to something running and governed. Commands below
use the Codex-only R5 Core host surface; substitute your own ports.

> Status: experimental. This shows the governed-event loop working end to end;
> it does not claim production readiness.

---

## Path A — operator: from install to your first governed decision

Goal: stand up Local Mnemon, observe one candidate, and see it admitted as a
governed decision on the Control Tower.

```sh
# 1. install the integration for your host
mnemon-harness setup --host codex \
  --principal codex@project --control-url http://127.0.0.1:8801

# 2. start Local Mnemon (the local governance daemon)
mnemon-harness local run &

# 3. observe a candidate — Local Mnemon admits it through its rules (ticked=true)
mnemon-harness control observe \
  --addr http://127.0.0.1:8801 --principal codex@project \
  --token-file .mnemon/harness/channel/credentials/codex-project.token \
  --type progress_digest.write_candidate.observed --external-id q1 \
  --payload '{"summary":"my first governed event"}'
# -> observed seq=1 dup=false ticked=true

# 4. stop the daemon, then read the Control Tower (it needs exclusive store access)
#    (kill the `local run` above, then:)
mnemon-harness tower --dump
```

The Tower prints the four pages. The decision appears on **LEDGER**, attributed
to its proposer:

```
# LEDGER
  dec_… by codex@project -> progress_digest
```

That is the whole point: a candidate became a **governed, attributed decision**
— not a silent write.

---

## Path B — capability author: from an empty directory to your own kind governing

Goal: declare a new event kind as a loop package and watch it govern, with no
code — a capability is **data that SELECTS from a closed catalog** of validators
and renderers, never new behavior.

Start from a working install (Path A, or `setup --host codex …`).

```sh
# 1. drop a loop package: .mnemon/loops/<name>/capability.json
mkdir -p .mnemon/loops/note
cat > .mnemon/loops/note/capability.json <<'JSON'
{
  "schema_version": 1,
  "name": "note",
  "observed_type": "note.write_candidate.observed",
  "proposed_type": "note.write.proposed",
  "resource_kind": "note",
  "items_field": "items",
  "fields": [
    { "name": "text", "validators": [ {"id": "required", "params": {"missing_style": "empty"}}, {"id": "safety:unsafe"} ] }
  ],
  "render": { "content": { "member": "bullet-list", "params": {"title": "# Notes", "field": "text"} } }
}
JSON

# 2. enable it. A package with host assets (a loop.json) is wired by
#    `setup --loop <name>`. A governance-only kind like this (no host assets)
#    is enabled by adding it to config.loops + the binding scope:
#      .mnemon/harness/local/config.json     -> "loops": [..., "note"]
#      .mnemon/harness/channel/bindings.json -> the binding gains
#          allowed_observed_types: "note.write_candidate.observed"
#          subscription_scope:     {"kind":"note","id":"project"}

# 3. run + observe your new kind — it governs through the SAME path as embedded descriptors
mnemon-harness local run &
mnemon-harness control observe \
  --addr http://127.0.0.1:8803 --principal codex@project \
  --token-file .mnemon/harness/channel/credentials/codex-project.token \
  --type note.write_candidate.observed --external-id n1 \
  --payload '{"text":"governed by a kind I declared"}'
# -> observed seq=1 dup=false ticked=true
```

Your `note` kind admits, renders, and (if you connect a Remote Workspace) syncs
— with no per-kind code. The full schema (validators, render members, risk
tiers, sync strategies) is in [`loop-package-v2.md`](loop-package-v2.md) and
[`capability-spec-v2.md`](capability-spec-v2.md).

---

## What you just used

| You ran | The protocol object |
|---|---|
| `control observe` | an Event admitted at the Channel boundary |
| `ticked=true` | the kernel decided it (a Decision) |
| `tower` LEDGER | the accepted Decision + its attribution |
| `capability.json` | a governed, versioned event-model declaration |

Next: connect a Remote Workspace (`sync`) to share one governed state across
machines, or read [`USAGE.md`](USAGE.md) for the full command surface.
