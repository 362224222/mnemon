# R7 Module Layout

Status: **ACTIVE**, alongside `r7-core-contract.md`. C0 through C6 are
complete. This document owns the Harness module structure and the cut order
that reached it. It does not own behavior.

On any conflict, `docs/harness/r7-core-contract.md` wins. Nothing here adds,
weakens, or reinterprets an invariant, a gate, or an evidence binding. Where a
package boundary exists, this document names the contract rule that forces it;
a boundary with no such rule is not justified and should not be created.

There is no forward compatibility requirement. R5 behavior, schema, and state
are not preserved. The R5 implementation was deleted in the same candidate
tree that marked R7 ACTIVE and R5 RETIRED.

## 1. Audit

The measurements below are the pre-cut baseline retained to explain what C6
deleted. The current tree has completed C0 through C6:
`cas`, `peerlink`, `daemon`, and `attach` exist at their target boundaries;
`cmd/mnemond` serves only `daemon`; local and peer inputs converge on authority
admission; and the purified Agent terminal imports only `agency` under its
final `internal/cli` name. The R5 wing is gone. The historical contamination
notes must not be read as remaining migration work.

Measured on `feat/r5-architecture`, non-test Go under `harness/`:

```
non-test total   119,499
test total       110,992
```

Reproduce:

```sh
cd harness
find internal cmd -name '*.go' ! -name '*_test.go' | xargs cat | wc -l
for p in internal/*/; do
  printf '%-28s %s\n' "$p" \
    "$(find $p -maxdepth 1 -name '*.go' ! -name '*_test.go' | xargs cat | wc -l)"
done
grep -rho 'mnemon/harness/internal/[a-z0-9/]*' internal/<pkg>/*.go | sort -u
```

### 1.1 The R7 spine is already a clean subtree

```
package        non-test   internal imports
---------------------------------------------------
agency            4,007   (none)
authority         4,437   agency
selector          2,482   agency          (no importers; already deletable)
```

`agency` held canonical values, wire shapes, and parse. `authority` held the
two domain states and the durable mechanisms. `selector` was already a
deletable R8 island. At this baseline, the remaining authority work was to
converge BoundIntent and VerifiedPeerDelivery on one domain admission
implementation; the current candidate has completed that work.

### 1.2 The two former contamination points

```
agencycli         1,449   agency + localapi + model + node
peer/agency_*.go  1,818   agency + model + store + testkit + libp2p
```

At the baseline, the final name `internal/cli` was occupied by the R5 CLI.
`agencycli` was purified in place and renamed only in C6, when the old package
was deleted.

### 1.3 R7 composition formerly hosted inside an R5 package

```
node/agency_daemon.go, agency_service.go, agency_boundary.go,
node/provision.go, node/r7_artifact_adapter.go        852   agency + authority
```

The R7 authority composition was clean, but its control client/server mechanics
also spanned `agencycli` and `localapi/agency_*`. The cut moved that complete
socket boundary; moving only the `node` files would have retained an R5
dependency.

### 1.4 Reusable mechanism formerly coupled to R5 only through primitives

```
artifact          4,941   model     (model.Digest x83, model.Sum x61, model.JSON,
                                     model.CanonicalMarshal, plus R5-only IDs)
testkit             467   model
integration       3,364   assets + model
assets              414   (none)    (contains assets/r7/{hook-cue.txt,
                                     mnemond.md, pi/mnemond.ts})
```

`agency` already defines its own canonical primitives, so this is a retarget,
not a rewrite.

### 1.5 R5 wing

```
store   35,668   node 13,421   peer 15,950   agent 9,268   model 7,968
localapi 6,315   cli 4,977     teamwork 1,988   event + event/semantic 1,710
```

Inside `store`: channel 7,664 / peer 9,436 / artifact 4,520 / agent 3,359 /
local 2,276 / current 1,377 / managed 1,389 / work 1,169 / gossip 726.

## 2. Target layout

Seven R7 internal packages exist because a contract rule forces each boundary.
An eighth, `selector`, is an optional R8 island and must be deletable. Test
helpers stay beside the tests that own them; R7 does not create a package
merely to preserve the R5 `testkit` name.

```
cmd/
  mnemon-harness      thin main
  mnemond             thin main

internal/
  agency        canonical values and wire
                Event / Intent / BoundIntent / VerifiedPeerDelivery / Receipt /
                View / PeerDelivery, canonical encode and parse, Digest / JSON
                imports: none
                forced by: P-01 (machine-owned fields), P-02 (closed structure)

  authority     the R7 kernel
                one admission; Handling and Reference; Operation, Attachment,
                claim, attempt epoch; the single SQLite writer
                imports: agency
                forced by: P-01, P-03..P-08

  cas           content-addressed storage
                capture / digest / pin / pull
                imports: agency
                forced by: P-05 (capture and verification complete before the
                admission transaction) and P-09 (refs, not bytes)

  peerlink      peer transport
                mutually authenticated bounded frames, outbox delivery,
                Artifact pull, inbox receipt
                imports: agency, cas
                forced by: P-06 (delivery is a closed internal lifecycle and the
                transport is replaceable)

  daemon        composition root
                process lifecycle, control socket, worker supervision, shutdown
                budget
                imports: agency, authority, cas, peerlink
                forced by: single-writer ownership and bounded worker rules

  cli           Agent Action Terminal
                View, Intent, Receipt, private binding journal, process lock
                imports: agency
                forced by: contract section 2 (private binding material lives
                outside the model's reach)

  attach        setup, Hook, and Agent-facing projection
                Runtime adapters; owns and embeds the R7 assets directly
                imports: agency
                forced by: Runtime differences must not reach authority

  selector      R8 only, optional and deletable
                SelectionDescriptor, SelectionState, round loop, observation
                imports: agency
                forced by: the R8 deletion gate

```

Dependencies form a line with no back edges:

```
cmd --> cli --------------------> agency
cmd --> daemon --> authority ---> agency
              +--> cas ---------> agency
              +--> peerlink ----> agency, cas
      --> attach ---------------> agency

selector -------------------------> agency      (unwired; deleting the
                                                 directory affects no one)
```

No package may be created for symmetry. If a proposed package cannot name the
contract rule that forces it, it does not exist.

## 3. Per-package disposition

| Pre-cut package | Non-test | Disposition | Note |
|---|---:|---|---|
| `agency` | 4,007 | keep | already the target shape |
| `authority` | 4,437 | keep | already the target shape |
| `selector` | 2,482 | keep, unwired | stays deletable until R8 is authorized |
| `agencycli` | 1,449 | purify, then rename to `cli` in C6 | own its Unix client; drop localapi, model, node |
| `peer/agency_*.go` | 1,818 | rewrite as `peerlink` | drop model, store, libp2p; preserve authenticated identity with an explicit standard-library handshake |
| `node` R7 files + `localapi/agency_*` | 852+ | extract to `daemon` | move the complete control socket plus generic lifecycle, supervisor, and shutdown mechanics |
| `artifact` | 4,941 | extract the narrow CAS core | primitives move from `model` to `agency`; closure/staging domain code is deleted |
| `integration` + `assets` | 3,778 | shrink to `attach` | keep R7 projection; delete Codex and Teamwork assets |
| `testkit` | 467 | delete | R5 Channel/libp2p fixtures only; no R7 importer or contract-forced boundary |
| `store` | 35,668 | delete | R7 has its own store in `authority` |
| `node` remainder | ~12,570 | delete | channel, control_channel, control_agent, work, gossip |
| `peer` remainder | ~14,130 | delete | channel, gossip, enrollment, libp2p |
| `model` | 7,968 | delete | primitives already exist in `agency`; domain types leave with R5 |
| `agent`, `localapi`, `cli`, `teamwork`, `event`, `event/semantic` | 24,258 | delete | R5 only |

Approximate outcome: **~100,000 non-test lines deleted, ~20,000 kept or
transformed.** Tests follow the same proportion. Line count is not the goal;
it is the visible consequence of removing a second domain model.

## 4. Cut order

Every step leaves the tree building and its own tests green. No step performs a
bulk delete except C6.

```
C0  darwin build repaired; CI runs the same gate set as the local merge gate
C1  cli        purify agencycli in place; own its Unix client; defer rename
C2  cas        extract the agency-native CAS core before its consumers
C3  peerlink   replace peer/agency_*.go with bounded, mutually authenticated
               standard-library transport; drop model, store, libp2p
C4  daemon     extract the complete R7 control socket, composition, workers,
               and generic lifecycle machinery
               *** cmd/mnemond serves R7 only from this point ***
C5  attach     shrink integration and assets to the R7 projection
C6  complete: delete the R5 wing in the same candidate tree as the contract
    switch
C7  steady-state gates
```

C0 is first because no later step can be verified without it.

`peerlink` authenticates both endpoints before accepting a claimed Peer
identity. Frame fields are never treated as authentication. This preserves the
security property previously supplied by the libp2p secure channel without
retaining libp2p as an architectural dependency.

### 4.1 C4 removes the two-writer boundary

The transitional rules for serving `node.db` and `agency.db` from one daemon —
strict-open, one admission gate around both request families, drain ordering,
asymmetric mutation preflight — exist only because one daemon serves both.
After C4 it does not.

> Two writers only need to coexist **in the source tree**, not in a served
> process.

R5 packages kept compiling and running their own tests until C6, while
`cmd/mnemond` pointed at `daemon` alone. The transitional boundary was therefore
never built and could not harden into a permanent framework.

### 4.2 C6 was the switch

Deleting the R5 implementation and retiring the R5 contract are one event.
Separating them produces a window in which an ACTIVE contract governs code that
no longer exists.

C6 landed in the exact candidate tree that:

- has R7 evidence at 10/10 with every section 9 gate green;
- marks R7 ACTIVE and R5 RETIRED;
- points every parser, registry, Make target, and CI gate at R7;

and satisfies `G-R7-AUTHORITY-CUTOVER`.

## 5. Steady-state structural oracles

These checks are machine-readable evidence under the closed Core gates; they
are not additional Gate identifiers. R8 deletion remains an R8 authorization
condition and cannot expand the R7 gate set.

| Existing binding | Structural oracle |
|---|---|
| `G-R7-AUTHORITY-CUTOVER` | `agency` has an empty internal import set. |
| `G-R7-AUTHORITY-CUTOVER` | `authority`'s internal import set is exactly `{agency}`. |
| R8 deletion condition | After removing `internal/selector`, the build and every R7 conformance suite pass. |
| `G-R7-NO-CASE-KIND` | No production Go contains `channel`, `teamwork`, `review`, `contract-net`, `blackboard`, or `memory.wiki` as a semantic identifier. They may appear only as opaque `kind` values in testdata. |
| `G-R7-AUTHORITY-CUTOVER` | `internal/` contains exactly the seven R7 packages in section 2 and may additionally contain only the optional `selector`; no dependency edge contradicts the graph there. |

One human-readable check accompanies them: **every package states what it owns
in one sentence.** Today's `store` cannot — it holds channel, peer, artifact,
work, gossip, and agent state at once. All seven R7 targets can; the optional
R8 selector owns only its private selection state.

## 6. What this document does not authorize

- Any change to `r7-core-contract.md` behavior, invariants, gates, or evidence
  bindings.
- Wiring `selector`. R8 activation is gated by its own preconditions, including
  a proven local outcome projection.
- Any release-path change. The root `mnemon`, `mnemon setup`, and Legacy Memory
  are untouched, and no release command may import `harness/`.
- Restoring an R5 package, compatibility path, or second domain model after C6.
- Creating a package that no contract rule forces.
