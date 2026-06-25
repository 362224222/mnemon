# Sync ABI v2

> Revises `sync-abi-v1.md` under the R1 no-forward-compat channel (P2 / PD6, 2026-06-12). The
> Remote Workspace MVP froze at v1 against two consumers (runtime co-hosted hub + `mnemon-hub`);
> v2 makes the one breaking change PD6 needs — the syncable-kind set is no longer a hardcoded
> global constant — and carries a version number. Independent review: the three P2 face texts
> (`capability-spec-v2`, `loop-package-v2`, this) get a dedicated adversarial doc-text review at P2
> close (PD9), after their last amendment — that review is this revision's freeze condition.
>
> **What changed from v1** (everything else in v1 still holds — read it for the wire verbs §1,
> grants §2, DTOs §3, the no-remote-reducer deviation §5, attribution map §7, T2 boundary §8,
> store ownership §9):
> 1. The hub's accept surface is the replica's **grant scope**, not a global syncable-kind set;
>    `contract.SyncableResourceKinds` is deleted.
> 2. The replica's produce surface is its **catalog's importable kinds**, descriptor-derived and
>    injected as `runtime.RuntimeConfig.SyncableKinds`.
> 3. The sync-import observation renames `remote.<kind>.commit_observed` →
>    `<kind>.remote_synced_event.observed` (the system-derived form of the `capability-spec-v2` grammar),
>    so the import diagnostic domain moves `remote.diagnostic` → `<kind>.diagnostic`.
> 4. An importable kind is selected by a `sync` descriptor block in its capability spec, under a
>    closed-set merge strategy — no hardcoded kind list anywhere.

## 1. The Sync descriptor block (capability-spec-v2 consumer)

A capability spec opts its kind into Remote Workspace import with a `sync` block (the consumer
`capability-spec-v2.md` §Sync defers to here):

```json
"sync": { "importable": true, "merge": "entry-dedup" }
```

- `importable` (bool) — opts the kind into Remote Workspace import. An importable kind is also a
  PRODUCE kind: this replica emits sync commits for it (§4).
- `merge` (string, required when importable) — selects ONE closed-set import strategy (`FromSpec`
  fails closed on any other value):
  - `entry-dedup` — merge non-conflicting ENTRIES by id into the resource's entry list, synthesizing
    one entry from a bare `content` field when the commit carries none; reject a
    same-id/different-content divergence.
  - `declaration-dedup` — merge non-conflicting DECLARATIONS by id, VALIDATING each imported
    declaration on the receiving side (id format, status enum, secret/injection scan — I15, receiving
    admission is not relaxed); reject a same-id/different-content conflict.
  - `item-dedup` — merge non-conflicting generic items by id, preserving every item field.

The strategy is parameterized by the capability (kind + proposed type), so the kind name appears in
NO platform code on the produce, accept, or import surface — a new importable kind is a descriptor
edit, not a code edit. The embedded importable set is descriptor-derived from the embedded catalog
(`agent_profile`, `teamwork_signal`, `project_intent`, `assignment`, `progress_digest`), and an
external declared kind that ships a `sync` block imports the same way (proven by the `journal` arm
of `run_sync_pair`).

## 4. Hub adjudication semantics (revises v1 §4)

The hub keeps an **append-only event log** of accepted commits (`sync_remote_commits`, monotonically
sequenced by `remote_seq`). Push adjudicates per commit:

- **accepted** — first sight of `(principal, OriginReplicaID, LocalDecisionID)` and the commit
  validates: provenance fields present, resource ref present, fields present, `FieldsDigest` matches
  `Fields`, **and the ref clamps into the grant scope**. The commit is appended; `next_cursor`
  advances.
- **rejected** — a structural-validation or scope-clamp failure; `diagnostic` names the reason. A
  ref whose kind or id is outside the grant scope is rejected here with the clamp's diagnostic
  (`"… is outside principal …"`). Nothing is appended; a rejected commit may be corrected and
  re-pushed under a NEW decision id.
- **conflict** — idempotency-key reuse with different content ONLY (unchanged from v1).

**The accept surface is the grant scope, not a global syncable-kind set.** v1 gated each commit's
kind against `contract.SyncableResourceKinds` — a hardcoded constant SHARED by the
hub accept path and the local produce path so the two "could not drift". PD6 deletes that constant.
The hub (its own trust domain — it imports no capability catalog) carries no notion of "syncable
kinds": its sole accept authority is the per-replica grant scope, already enforced per commit by the
one ref-level clamp (`contract.ClampRefs`, v1 §2). A kind absent from a replica's grant is rejected
as out-of-scope at the clamp, so a separate kind-level check is redundant and is removed —
`validateSyncCommit` is now purely STRUCTURAL (provenance / ref present / fields / digest).

**The produce surface is descriptor-derived.** The local decision sink produces a sync commit for a
host decision when the decision's kind is in the replica's **importable kinds**
(`capability.ImportableKinds(catalog)`), injected into the serving runtime as
`runtime.RuntimeConfig.SyncableKinds` (a plain `contract.ResourceKind` slice — the runtime stays
capability-free; the app fills it). The sync-import principal is excluded, so imported writes never
re-emit.

**Produce ⇄ accept alignment (replaces the "cannot drift" invariant).** The two surfaces are no
longer a shared compile-time constant: the produce set is the replica's catalog importable kinds, the
accept set is the hub's per-replica grant scope. They align by CONFIGURATION (the operator grants a
replica scope over the kinds it syncs), and a mismatch is not silent — it surfaces as a per-commit
`rejected` result with the clamp diagnostic. Visibility, not a compile-time guarantee, is the v2
safety property.

**Replay idempotency**: unchanged from v1 — re-pushing an identical batch appends zero new rows;
pull replay is idempotent by cursor.

## 6. Puller-side import (revises v1 §6)

Pulled commits re-enter the puller's Event Intake under `sync@local` (`contract.SyncImportActor`),
never bypassing the kernel. Exactly-once is the intake dedupe over the six-part key (unchanged):

```
ExternalID = "pull:<remote_id>:<OriginReplicaID>:<LocalDecisionID>:<Kind>:<ID>"
```

- An **importable kind** (descriptor-derived: any kind whose spec declares `sync.importable`) ingests
  its `<kind>.remote_synced_event.observed` event — the system-derived form of
  the `capability-spec-v2` event grammar (v1's `remote.<kind>.commit_observed` is renamed). The
  kind's declared merge strategy (§1) merges non-conflicting items and DENIES a
  same-id/different-content divergence with a durable `<kind>.diagnostic` — the import diagnostic now
  lands in the kind's own domain (v1's `remote.diagnostic`), because the diagnostic domain is the
  prefix of the trigger type before the first dot.
- A kind with **no import mapping** (a hub serving a kind this replica's catalog does not import)
  ingests `sync.import_skipped.observed` with `ExternalID = <six-part key> + ":skipped"` and payload
  `{kind, origin_replica_id, local_decision_id, remote_id}`; a deny rule turns it into a durable
  `sync.diagnostic` naming the kind. Exactly-once; the pull cursor still advances — the skip is
  visible, never silent, and never wedges the stream. (Unchanged from v1 except that the importable
  set is now descriptor-derived: `policy.RemoteSyncedEventType(catalog, kind)` returns the
  observation type for an importable kind and "no mapping" otherwise.)

The pull cursor is durable per remote (`sync_pull:<remote_id>`), advanced only after the batch is
imported.

## Consumers and verification

Two consumers, unchanged from v1: the runtime co-hosted hub (`mnemon-harness local run` serving
`/sync/*`) and the standalone `mnemon-hub` binary — ONE wire, two hostings. The PD6 descriptor-derived
path is verified at the Go integration layer (`policy` import-dispatch + importable-kind pins,
`mnemonhub` accept, `app` sync import) and end-to-end by `run_sync_pair`, which carries embedded
`progress_digest`, external `journal`, and embedded `assignment` across the TLS hub. The journal
round-trip proves the produce/accept/import surfaces are kind-agnostic.
