# R7 Core Contract

Status: **ACTIVE**. This is the sole tracked merge and release authority for
the experimental Harness.

This document is the single tracked authority for the experimental Harness. It
defines an event physics small enough to be fully proven: ten machine
invariants, two mutable domain states, and one admission entry point.
Collaboration patterns are not implemented here. They are written as
Agent-facing descriptions and test fixtures on top of this physics.

The derivation history, research evidence, and superseded R7 design material
live in `.mnemon-dev/architecture/r7/` and `.mnemon-dev/research/`. Those
documents may explain why a rule exists. They cannot add a requirement.

R7 is built from `harness/`. The root `mnemon` release path, `mnemon setup`,
and Legacy Memory are unchanged, and no release command may import `harness/`.

## 1. Product outcome

One Agent, one workspace, one setup, one ordinary task.

Work that mnemond has accepted survives context compaction, process exit, and
daemon restart. A later turn resumes it from durable state rather than from a
transcript. No final answer, process exit, idle report, transport ACK, or
provider success can cause mnemond to project completion.

The same physics carries collaboration between nodes. A remote target is a
different topology, not a different subsystem.

The user runs no governance command during this path.

## 2. Agent model

For an Agent, the entire system is one sentence:

> See the current world, propose one intent, then confirm whether that intent
> actually took effect.

```
Agent:    View -> Intent -------------------------------> Receipt -> View'
Machine:          BoundIntent -----------+
                   VerifiedPeerDelivery -+-> admission -> local Event
```

The Agent's world contains only: the bounded View, the intents it may submit,
and the Receipt it reads back. A View separates machine-derived state and
allowed consequences from semantic payloads, Artifact content, and remote
claims. The latter are content to reason about, never authority fields.

The CLI binds an Intent to the exact authority that produced its View. Operation
keys, attachment leases, current Handling identity, Reference head identity,
digests, principal identity, and fences are private binding material. They must
be correct, and they must not appear as fields the model can invent or edit.
Every requested consequence, target alias, and opaque handle must have been
offered by that exact View; a known but unoffered choice fails closed.
`kind` and a first-publish `reference_key` are the only open semantic labels;
they are bounded candidates rather than authority handles.

`AdmissionRequest` is the internal union of `BoundIntent` and
`VerifiedPeerDelivery`. One request has one durable operation outcome and one
Receipt. Acceptance creates exactly one local Event; rejection creates no
Event; replay returns the original Receipt and creates nothing. The accepted
Event is the immutable record of the complete admitted action; there is no
second Event creation step.

An accepted Event may have three domain consequences, and nothing else at the
domain layer:

```
accepted Event
  |-- advance, resolve, or create bounded Handlings
  |-- update a Reference head           publish, supersede, or retract
  `-- reference an Artifact             bytes stay in the CAS
```

Events and Artifacts are immutable. Everything an Agent needs to continue is
projected from them and the two mutable states in section 3.
Admission may also write the closed replay, claim, claim-disposition, and
peer-delivery records required by P-03 through P-07. Those records enforce the
physics; they are not Agent-declarable domain consequences.

## 3. The two persistent effects

These are the only mutable domain states in the Core. Claim occupancy,
operation replay, claim disposition, and peer delivery are internal mechanisms,
not additional domain states.

### 3.1 Handling

A Handling is durable responsibility: who still needs to do what.

```
Handling
  subject / causal reference
  exactly one target AgentPrincipal
  current accepted Event head
  domain state: open | terminal(outcome)
  claim occupancy: none | live(attachment, lease, fence)
```

A Handling advances or closes only through explicit Intent under admission
(P-04). `pending` and `claimed` are projections of one open Handling and its
claim occupancy; they are not domain states. At most one live claim exists per
Handling. A claim may be issued only to an attachment authenticated for the
Handling's target Principal. Every subject-bound admission is privately bound
to that same attachment and current fence; a wrong Principal, wrong attachment,
or stale fence fails closed. An accepted advance or terminal resolution updates
the Handling head and settles the current claim atomically; advance leaves the
Handling open and claimable by a later turn.

### 3.2 Active Reference

An Active Reference is durable currency: which version of some content is in
force.

```
Reference head
  key
  accepted Event ID and Event digest
  Artifact digest, when active
  state: active | retracted
  lineage (publish -> supersede* -> retract -> supersede ...)
```

Permitted effects: `publish`, `supersede`, `retract`. Head changes use CAS
(P-08).

The active set is a projection of accepted lineage:

```
active_reference[key] = current locally accepted head, only when state=active
```

A retraction is a tombstone; it never reveals an older version as current. A
later `supersede` may replace an exact tombstone and make the key active again.

An implementation may maintain a materialized index inside the same
transaction, but the lineage remains the authority: deleting and rebuilding the
index must yield the same set.

### 3.3 Reference must not become a second Handling

A Reference has **no owner, no claim, no lease, no workflow status, and no
terminal state**. It has a head and a retraction.

Any proposal that adds `reviewing`, `pending_approval`, `assigned_to`, or an
equivalent field to a Reference is rejected. That is a Handling wearing the
wrong name.

### 3.4 What this replaces

Registries, skills, playbooks, project notes, and collaboration descriptions
are all Artifacts under a Reference key. None of them is a subsystem.

```
review-playbook.md      -> Event + reference.publish
review-playbook-v2.md   -> Event + reference.supersede(expected = v1 head)
found to be wrong       -> Event + reference.retract(expected = v2 head)
```

mnemond does not understand Wiki, Skill, Review, or Playbook. It understands
Artifact refs, supersession, retraction, and projection.

## 4. Semantic labels and structure

> **Names are open. Consequences are closed.**

`kind` and a first-publish `reference_key` are bounded opaque semantic labels.
mnemond validates that each is non-empty, within its length limit, and within
its permitted character set. It never interprets their semantic meaning;
Reference-key equality is used only for local lookup and CAS. `review.request`,
`evidence.challenge`, and any valid label a future Agent invents traverse the
same generic path.

There is no kind or Reference-key registry, no schema loader, and no dispatch
by name.

Every BoundIntent matches exactly one closed family:

```
root-handling:
  subject_handling=none; handling_action=none; successors=1..N; reference=none
subject-handling:
  subject_handling=one;
  handling_action=advance | resolve(completed | declined | unresolved)
  successors=0..N; reference=none
reference:
  subject_handling=none; handling_action=none; successors=0;
  reference=publish | supersede | retract
```

`subject_handling=none` describes only this request: it does not advance,
resolve, consume, or fence a Handling. It does not assert that the Principal or
attachment has no live claim. A Reference request may therefore be admitted
while the same attachment is working on a Handling, and it leaves that
Handling's head, claim, and fence unchanged.

`reference=none` likewise names only the Reference *action*. Any family may
additionally cite View-offered Reference handles as provenance: the exact
Reference versions that were in force for this work. Admission records the
cited head identities on the Event. A citation performs no CAS, changes no
head, and can never substitute for a Reference action. This is what allows a
later projection to attribute a Handling's terminal outcome to the exact
Reference version that guided it, without any new state or domain object.

Each successor has exactly one target, and the list is bounded. A
subject-handling request may create successors; this is the handoff/fan-out
primitive. A causation or correlation handle is provenance only: it cannot turn
a root-handling request into a subject-handling request or relax
`may_initiate`. Artifact candidates and refs are bounded in every family.
`publish`/`supersede` attach exactly one verified Artifact, supplied either as a
candidate or as a View-offered handle; `retract` attaches none; `completed`
satisfies P-10. No other combination is legal.

Every accepted request already produces its one Event. An unknown structural
consequence or an illegal combination fails closed. An unknown `kind` does not
fail — there is no meaning to close over.

Peer delivery is not a declarable consequence. For a local target, admission
creates a local Handling. For a remote target, the same admission atomically
creates a durable PeerDelivery obligation; it does not create a locally
claimable Handling for a remote Principal. Every ordinary remote-directed
PeerDelivery outbox is bound in that transaction to exactly one open local
Handling for the source Principal and to one expected reply root. The expected
root is machine-derived from the accepted outbound Event: its correlation when
present, otherwise its own Event identity. A subject advance binds its current
Handling. A root or ordinary resolve binds the newly created successor targeted
to the source Principal. If the required source-local anchor is absent or not
unique, admission fails closed. The binding is private machine authority, not
an Agent-provided label and not a property inferred later from semantic
content.

A request that would export its only responsibility therefore fails closed.
The sole exception is a terminal disposition of an imported current: it may
close the responder's Handling while returning exactly one PeerDelivery to the
View-sealed `reply_target`, correlated through the exact View-sealed `reply_to`.
That terminal-reply outbox binds no reply anchor and no expected reply root; it
cannot solicit another response. The requester already retains the exact local
responsibility anchor bound by its earlier outbound admission.

```
ordinary remote action = PeerDelivery(s) + exact open reply-anchor binding(s)

root-handling:     newly created source-local successor is the anchor
subject advance:  the still-open current Handling is the anchor
ordinary resolve: newly created source-local successor is the anchor
terminal reply:   exactly reply_target + exactly reply_to; no anchor binding
```

The requester's bound local anchor is not closed by a remote Receipt,
rejection, delivery expiry, or returning disposition. A later local Intent must
advance or resolve it explicitly. A returning disposition is re-admitted as a
separate no-reply integration Handling and cannot acquire reply authority merely
because it shares a root. If an Agent later submits a new explicit ordinary
outbound Intent while working that imported integration — for example, a
subject advance with a remote successor — that new admission may bind the still
open integration Handling to its new outbox and expected root. Only replies
that satisfy that new outbox binding may then use it as an anchor. The
terminal-reply exception itself closes only the responder's imported current
and cannot be used for root initiation, advance, fan-out, redirect, or a
different provenance handle that happens to resolve to the same Event.
The target node re-admits the signed delivery and creates its own local Event
and local Handling. An Agent that wants a peer to consider a description
targets that peer and references the Artifact; whether the peer adopts it is
the peer's own local Intent (section 7.1).

### 4.1 Authority fields

An Agent-visible Intent may provide:

- opaque `kind`;
- bounded semantic payload;
- a bounded opaque `reference_key` candidate, only for first publish;
- a bounded successor list, each entry carrying exactly one target alias or
  `self`;
- opaque causation, correlation, Handling, and Reference handles previously
  offered by the View;
- Artifact candidates or opaque Artifact handles previously offered by the
  View;
- closed structural consequence declarations.

The CLI-held private binding adds:

- authenticated attachment and source context;
- exact View authority/read-set digest and the offered consequence, target, and
  handle set;
- caller-stable operation key and independent request digest;
- exact current Handling identity and fence, when subject-bound;
- `expected=absent` for a first-publish key, or the exact current Reference
  head when superseding or retracting;
- the local Event and Artifact identities behind any opaque handles.

A peer admission request instead carries a signed PeerDelivery envelope and an
independently verified peer context. Origin Event identity, sequence, digest,
closed consequence, total target count, and causation remain provenance
evidence. They are never copied into the receiving node's canonical Event
fields. Its consequence subset is strictly smaller: it may create one new local
targeted Handling and bind provenance plus required Artifact refs. It cannot
advance or resolve an existing Handling, mutate a Reference, create completion,
or create multiple local successors. Adoption and every later consequence
require a local BoundIntent. The staged envelope becomes a
`VerifiedPeerDelivery` AdmissionRequest only after P-09 has verified every
required Artifact.

A signed terminal origin consequence with exactly one origin target identifies
only a terminal-reply *candidate*. Before accepting it, the receiver must find
a persisted ordinary outbox binding on the same authenticated route whose
expected reply root exactly equals the signed origin correlation and whose
exact bound Handling is still open for the locally resolved target Principal.
Missing correlation, no such outbox binding, a closed bound anchor, the wrong
Principal, or a different route rejects the candidate without creating an
Event. The Handling's creation source is irrelevant: an imported integration
cannot satisfy a prior binding merely by sharing its root, but it can become the
anchor of a later outbox only through a new explicit local outbound Intent as
described above. Additional same-root Handlings neither grant authority nor
invalidate the exact bound anchor. Semantic `kind` and payload are never
consulted. An accepted candidate still creates one ordinary local integration
Handling with no reply capability; it does not advance or resolve the bound
requester anchor.

mnemond generates or resolves:

- canonical Event ID, accepted timestamp, local origin sequence, digest;
- source AgentPrincipalID, from the verified actor context;
- the stable local AgentPrincipalID that `self` or a local alias resolves to;
- the enrolled peer route and opaque remote target alias for a remote target;
- operation outcome, Receipt, and PeerDelivery identity.

`self` is never persisted as a literal. It is resolved at admission time, so
its meaning cannot drift when the Runtime changes.

The origin node never assigns a remote node's AgentPrincipalID. The receiving
node owns resolution of the opaque remote alias to one locally authorized
AgentPrincipal.

A local Agent Intent carrying any mnemond-owned field is **rejected**, not
sanitized and accepted. Signed origin fields in a PeerDelivery are accepted
only as provenance under P-06; they cannot override local authority fields.

## 5. The ten machine invariants

Each invariant names one failure it prevents. Each must have independent test
evidence before this contract can activate (section 10).

**P-01 Admission owns facts.**
mnemond generates Event identity, accepted time, origin sequence, digest, and
Receipt, and resolves source and target from authenticated state. Local Agent
and peer inputs enter with distinct verified actor contexts but share one domain
admission implementation. Machine dispositions use a separate closed internal
path and cannot create Events. Model text and peer origin fields never become
local authority. For a fresh operation with no recorded outcome, a BoundIntent
is admissible only when its View authority digest is exact for the machine-owned
read-set and every selected consequence, successor target, and opaque handle
belongs to that View's offered set. Semantic text changes outside that read-set
do not invalidate an otherwise current binding. P-07 defines the only replay
exception to revalidating mutable authority.

An Agent View has at most one writable `current`. Its `reply_to` is a
provenance-only handle for one machine-derived stable correlation root.
`reply_required` is an explicit machine-derived Boolean. Ordinary work whose
durable Handling was created by a directly imported Event on an active route
projects `reply_required=true` and `reply_target`: the machine-derived public
alias of its authenticated immediate sender, sealed as one exact offered remote
target. Local work, an unavailable route, and a correlated terminal response
project `reply_required=false` and no `reply_target`. The latter remains an
explicit local integration Handling but cannot acquire the responder's
terminal-reply exception from that creation Event. A later explicit local
ordinary outbound admission may still bind it as that new action's requester
anchor. The correlation root and any responder reply capability derive from
the immutable Handling creation Event and survive local advances. Opaque handles
are valid only in the exact View that offers them; no handle may be carried
across Views. Peer route targets are offered only to an attachment whose
Principal is that route's fixed local target. `reply_target` exposes no RouteID,
PeerID, remote target alias, or Principal. A bounded `related_open` projection may show
locally accepted open Events whose correlation equals that root, but it exposes
no Handling handle, fence, claim, or second writable subject. `outstanding`
reports the exact local open and related counts, the projected prefix, and
whether evidence was truncated. Semantic `kind` never changes these projection
rules.

**P-02 Open labels, closed structure.**
`kind` and a first-publish `reference_key` are bounded opaque labels with no
registry or semantic dispatch. Structural consequences are the closed set in
section 4. Adding a collaboration pattern or Reference key adds no Go branch.

**P-03 Durable Handling.**
Every accepted local successor target creates a durable Handling for exactly
one local AgentPrincipal. One Event may create a bounded number of such
Handlings; each remains independently claimable. An attachment is one verified
Runtime lifecycle boundary at which a turn may act. A root BoundIntent is any
BoundIntent with `subject_handling=none` that creates successors, and it
requires `may_initiate = true` on a local Agent attachment. This remains true
when it names an older Event through causation or correlation; those handles
carry provenance only.
The attachment verifier generates `may_initiate`; it never appears in an Intent
and cannot be changed by any projection or collaboration description. T0 asks
the owner-installed Host Hook to issue this attachment at an interactive
Runtime boundary and has no machine-driven wake path. The Hook gives the local
terminal a fresh private Host-boundary nonce at `before_agent_start`. Its digest
enters both the owner-private journal and authority as the stable
attachment-begin operation; neither value enters Agent View. Before boundary
end, same digest and same authenticated request exactly replay the byte-stable
attachment proof, including across daemon response loss, missing journal
commit, and daemon restart. The same operation with different authenticated
semantics fails closed. An expired replayed proof cannot be committed to a
local journal or reported ready, and an ended boundary cannot be revived by
replaying attachment begin. Beginning a different boundary idempotently ends
the predecessor, releases only its claim occupancy, and issues the replacement
in one authority transaction. A partial unique index permits at most one
unended interactive attachment per Principal. This makes a fresh private nonce
converge even when the predecessor's CLI journal was lost. When a same-boundary
journal does exist, the CLI must replay attachment begin against authority and
compare the returned ID, credential, and expiry exactly before reporting ready;
the journal alone is never proof of live authority. The Pi Host retries attach
a fixed small number of times with the same nonce and emits no cue unless one
attempt succeeds. Once any accepted Receipt reaches stdout, the journal
durably removes its replay-only Intent material before attempting boundary end.
Agent commands can never reactivate that presented terminal phase. A later
Hook end or different boundary can therefore finish End and clear it without
replaying the old Intent. Before presentation, Hook end fails
closed and preserves the exact Receipt replay rather than ending the attachment
beneath it. Host shutdown may end the current boundary eagerly; after crash or
hard timeout, the next interactive boundary performs the same deterministic
reconciliation. The owner-only socket and private journal exclude other OS
users, but T0 does not attest the Hook process against another same-UID process
with arbitrary local shell and file access.
Thus T0 proves that an Agent cannot set or alter `may_initiate` in an Intent; it
does not cryptographically prove that the Host callback physically occurred.
Peer-originated initiation is governed separately by P-06.
A claim and every use of its fence require an attachment authenticated for the
Handling's target Principal.

**P-04 Explicit progress, mechanical disposition.**
Domain progress comes only from explicit Intent under admission. A
fresh subject-bound admission must match the live claim fence; a released,
expired, or superseded fence fails closed. Replay of an already recorded
operation follows P-07 and does not re-run this check. T0 has exactly two
machine claim dispositions: lease expiry and Host-boundary end, whether
explicit or performed inside fresh-boundary replacement. Both
clear claim occupancy only; neither changes the Handling's domain state,
creates an Event, or yields any terminal outcome. Host-boundary end is an
authenticated callback path, not a model Intent and not a background worker.
A fresh interactive `current` request settles at most a bounded number of
expired claims before selecting work. Among a fixed bounded set of open
Handlings, fresh boundaries deterministically prefer the least previously
claimed responsibility; releasing one old claim cannot let it monopolize every
later turn. T0 has no background wake or retry worker. Every other machine
disposition is outside T0.

**P-05 Atomic successors.**
One accepted AdmissionRequest commits in one transaction: its one local Event,
its Receipt, any allowed advance or resolution of the current Handling,
successor local Handlings, durable PeerDelivery obligations for remote targets,
an allowed Reference head change, and Artifact pins. Artifact capture and hash
verification finish before this transaction; only verified refs and pins enter
it. Partial commit is a contract violation.

**P-06 Local authority, federated candidates.**
`self` targets and peer targets use identical event semantics. A PeerDelivery is
an authenticated candidate, never a local fact, and produces a new local Event
and, when targeted, a local Handling only through local admission. The receiving
node preserves origin identity and causation as provenance but generates its
own Event identity, digest, sequence, and Receipt. A peer may begin a local
causal chain only when the local peer policy authorizes both initiation and the
resolved target Principal. Inbound peer admission is limited to the consequence
subset in section 4.1. Every ordinary origin outbox is atomically bound to its
exact open source-local reply anchor, expected reply root, route, and Principal.
Subject advance binds the current Handling; root initiation and ordinary
resolve bind their newly created source-local successor. Origin admission
rejects any ordinary remote action that cannot make this exact binding. The
only exception is a terminal Intent over an imported current with exactly one
remote successor equal to its sealed `reply_target` and an exact correlation
handle equal to its sealed `reply_to`; that transaction may close the
responder's current without a local successor and records no reply-anchor
binding because the requester retains the anchor bound by its earlier outbound
admission. Therefore remote rejection, missing Artifact, delivery expiry, or a
returning disposition cannot erase the requester's accepted responsibility.

Peer delivery has one closed internal lifecycle. The origin atomically creates
an outbox record with a stable delivery ID derived from origin Event, enrolled
peer route, and opaque target alias. The signed envelope also carries the
origin Event's closed consequence and total target count; those fields describe
origin structure but grant no receiving authority by themselves. Its durable
states are `pending`, `settled` by a signed remote admission Receipt, or
`expired` by delivery TTL. A transport ACK does not settle it. The receiver
stages by delivery ID and envelope digest; its inbox states are `staged`,
`settled` with the stable admission Receipt, or `expired`. Same ID/same digest
replays that Receipt, while same ID/different digest conflicts. Missing
Artifacts keep the envelope staged without creating a local Event. Either-side
delivery expiry changes no domain state and never means completed.

An accepted Event's correlation is copied unchanged into PeerDelivery. For a
Handling created by an imported Event, every receiving View derives `reply_to`
from the authenticated origin correlation, or from the origin Event when none
exists. When the immediate inbound route remains active, the same View
independently derives `reply_target` from that inbox route's public alias. A
local advance changes the Handling head but not this creation-bound reply
context. A later Intent can therefore preserve one conversation root and
address its authenticated sender without a transport rewrite or semantic target
inference. The terminal exception requires those exact opaque handles, not
merely another handle resolving to the same Event or route. On return, terminal
origin structure is only a candidate: local admission additionally requires the
exact authenticated route, expected reply root, local target Principal, and
still-open Handling stored in an ordinary outbox binding. The Handling's
creation source grants no authority. An accepted returning delivery is
re-admitted as a new local Event and integration Handling; that Handling
explicitly projects `reply_required=false` with no `reply_target`, cannot bounce
the disposition, and does not advance or resolve the bound requester anchor.
It may become a reply anchor only if a later explicit local ordinary outbound
Intent creates a new outbox binding to it. Only after local admission may the
response appear as read-only related evidence beside the still-open origin
Handling.

**P-07 Exactly-once effect.**
Every attachment-begin request, CurrentRequest, AdmissionRequest, and machine disposition carries a
stable operation key and an independent request digest, and its outcome is
durable. Attachment begin uses the private Host-boundary digest, while Current
and BoundIntent use CLI-held keys; PeerDelivery uses its stable delivery ID;
machine disposition derives a stable key from the claim identity, fence, and
expiry. After authenticating the actor and operation namespace and
independently recomputing the request digest, mnemond looks up the operation
outcome before validating attachment expiry, mutable View authority, fence,
Handling head, or Reference head. Same key with the same digest returns the
original byte-stable frozen View, admission Receipt, or internal disposition
outcome even when those mutable inputs are now stale. Same key with a different
digest is a stable conflict. Only a previously unseen key proceeds to fresh
execution. Replay never produces a second claim, local Event, disposition, or
durable mutation. Attachment-begin replay preserves the original expiry rather
than renewing it; the CLI rejects an already-expired or locally divergent proof
before reporting the boundary ready. Boundary end is itself idempotent, and an
ended boundary rejects a later attachment-begin replay rather than reopening
its authority. A retrying Host reuses one nonce, so response loss cannot create
a second attachment.

The frozen View includes its exact `current`, `reply_to`, optional
`reply_target`, related prefix, outstanding counts, and Reference evidence.
Later Events never appear in an exact Current replay; only a fresh Current
operation may project them.

**P-08 CAS lineage.**
The first `reference.publish` supplies a bounded opaque `reference_key`
candidate; it needs no pre-existing Reference handle or registry entry. The CLI
privately binds `expected = absent`, and admission accepts only when no local
lineage head exists for that exact key. Every later
`reference.supersede` or `reference.retract` privately binds the expected head
Event ID and head Event digest and is accepted only when both equal the local
current head. `supersede` replaces either an active head or an exact retracted
tombstone with a new active Artifact. `retract` replaces an active head with a
retracted tombstone; retracting a tombstone fails closed except as
same-operation replay. Reference actions may name only locally accepted heads;
forward references are rejected, so lineage cannot cycle. Of two concurrent
head mutations, exactly one succeeds. There is no merge, winner selection,
last-write-wins, or CRDT. A provenance citation records an exact locally
accepted head on the Event without performing CAS or changing it; only
`publish`, `supersede`, and `retract` mutate a head.

**P-09 References, not bytes; always bounded.**
An Event carries locally hash-verified Artifact digests and refs; content lives
in the CAS. PeerDelivery arrival does not imply Artifact availability. An
inbound delivery may be durably staged, but every Artifact ref it names is
required and local admission waits until all bytes are present and match their
digests. A BoundIntent likewise cannot activate a Reference or satisfy
completion with an unavailable ref. Fan-out, causal hop, TTL, payload size,
Artifact size, provenance citations per Event, and pending Handling count are
bounded by machine-owned configuration. T0 admits at most eight Artifact refs
per Event, keeps at most eight active References and eight active Peer routes,
and limits semantic payload to 4 KiB of JSON-encoded string content; larger
content belongs in an Artifact. Agent and peer input that exceeds a bound fails
closed rather than being truncated, and no semantic payload can raise a bound.
The Pi reference attachment separates two closed budgets in one governed run:
at most sixteen exploration tool calls and at most two calls to one native
Effect-settlement tool. The latter executes only the fixed
`mnemon-harness agent submit --json` argv without a shell, accepts one bounded
Intent object on stdin, and returns only the bounded Agent-terminal result. It
does not inspect Event kind or payload, choose an Intent, or imply acceptance.
The first excess exploration call is blocked before execution and later
exploration calls remain blocked. At cutoff, only the settlement tool may
remain active, and only when it was already present in the Host tool snapshot;
the attachment never widens the Host allowlist. The two-call limit permits one
correction after rejection. Once cutoff has occurred, the final settlement
attempt disables tools and leaves one bounded final-response turn. If no
settlement is submitted, the remaining post-cutoff turns are bounded by the
unused settlement slots. Automatic retry or compaction cannot refresh either budget:
the attachment restores the exact tool set captured at cutoff only after Pi
reports the whole run settled. Attachment failure leaves an ordinary Pi run
untouched. A failed tool restore retains the exact snapshot and prevents a
later governed run until restoration succeeds. These are Runtime attention
bounds; they create no Event, Receipt, completion, or other domain fact.
These cross-field maxima are tested together, not only one at a time, so every
accepted responsibility remains representable within the 16 KiB private and
Agent View envelopes. Internal
expiry maintenance processes at most a fixed number of exact claims per fresh
`current`; excess expired claims remain byte-for-byte unsettled for later
natural turns and do not make the bounded `current` fail.
The combined JSON-encoded string content of writable current and projected
related semantic payloads is bounded independently. JSON escaping therefore
consumes the focus budget exactly as it consumes the final canonical View. No
accepted current can exceed that focus budget; related evidence is omitted with
an explicit truncation fact before the combination can exceed it or make the
whole Current operation fail.

**P-10 Evidence-backed completion.**
`completed` is one closed terminal outcome and requires at least one locally
available, hash-verified Artifact attached by the resolving Event. Every other
terminal outcome needs no Artifact and must never project as completed. Final
answers, process exit, Runtime idle, provider success, transport ACK, and
Handling dispositions cannot produce this outcome.

These are conformance invariants, not product concepts. They belong in tests
and in the Core, not in the Agent-facing projection.

## 6. Completion

`terminal` is the Handling domain state. `completed` is one machine-readable
terminal outcome under P-10, not a synonym for terminal.

```
completed
  requires at least one locally available, hash-verified Artifact
  attached by the resolving Event

every other terminal outcome
  declined | unresolved
  needs no Artifact
  must never project as completed
```

This floor cannot be relaxed by any collaboration description, `kind`, or
projection. It prevents artifactless protocol completion; it does not prove
that an Artifact is meaningful or correct. The non-success terminal outcomes
let an Agent close work honestly without manufacturing an Artifact merely to
end responsibility.

T0 does not support owner-attested completion, owner cancellation, or owner
abandonment. Adding any of them requires a separately authenticated owner input
surface, operation replay, Handling authorization, and conformance evidence.
Sharing a UID with the user is not owner presence and cannot open that path.

An accepted Receipt proves that a contribution was accepted under this
contract. A rejected Receipt proves only the stable rejection outcome. Neither
proves that the natural-language result is correct in the world.

## 7. Federation

A remote target uses the same physics as `self`. What differs is that a
PeerDelivery crosses a trust boundary and must be re-admitted locally (P-06).
The receiving node must already have enrolled the origin peer under a durable
identity and trust policy; an otherwise blank node cannot authenticate a delivery.

### 7.1 Using a remote description is not adopting it

```
A has locally accepted playbook v2
        |
        v  accepted Event + durable PeerDelivery
B receives a signed remote request that references the exact Artifact
        |
        v  fetch + verify bytes, then local re-admission
B creates its own local Event and Handling
        |
        +-- handle this request with v2   read the exact Artifact; B's
        |                                 Reference head is unchanged
        |
        `-- adopt v2 locally              a separate local publish/supersede
                                          Intent through B's own admission
```

This is what allows a remote Agent to work under a new collaboration scheme
without any scheme synchronization, global consistency, or CRDT.

### 7.2 No global convergence

Two nodes may hold different heads for the same Reference key, indefinitely.
R7 promises that each local authority has an explicit causal chain with no
silent overwrite. It does not promise that nodes converge.

**A fork is explicit autonomy, not a synchronization failure.** An
implementation must not "fix" it — doing so reintroduces global ordering.

## 8. What R7 T0 does not guarantee

T0 permits the source and the target of an Event to be the same
AgentPrincipal. Self-review is structurally possible.

T0 is opportunistic and Hook-driven: it does not launch, wake, or retry Agent
Runtime processes in the background. A pending Handling waits for the next
eligible interactive boundary. Managed wake is outside T0 and requires a
separate future contract; R7 T0 reserves no managed-wake mechanics.

Guaranteed:

- explicit action;
- durable responsibility;
- idempotent submission;
- the completion evidence floor of P-10;
- Receipt-based completion, and therefore no protocol false completion.

Not guaranteed:

- an independent reviewer;
- separation of proposer and judge;
- multi-model consensus;
- objective correctness of any business result;
- owner-attested completion; or
- process-level attestation that distinguishes the installed Hook callback
  from every other same-UID local process; or
- governance of tool and Runtime side effects that occur outside mnemond.

A collaboration description may require an Agent to choose a different target.
mnemond does not promote that requirement into a global rule.

## 9. Conformance gates

| Gate | Requirement |
|---|---|
| `G-R7-CORE` | Unit, race, process, and pinned Pi Runtime suites cover P-01 through P-10 and every named sub-assertion in section 10 with independent oracles. |
| `G-R7-CASES` | `review/`, `contract-net/`, and `blackboard/` fixtures all exist under `harness/testdata/r7/cases/`, run, and pass their independent deterministic oracles. |
| `G-R7-PATTERN-FREE` | In a temporary copy, deleting both `harness/testdata/r7/examples/` and `harness/testdata/r7/cases/` leaves the P-01 through P-10 Core conformance command passing; case acceptance and case-presence checks are excluded from this deletion run. |
| `G-R7-NO-CASE-KIND` | No production Go source contains a case-specific kind literal such as `review.request`. |
| `G-R7-CASE-DATA-ONLY` | Every executable or behavior-bearing case definition, prompt, playbook, expected output, oracle, and fixture is confined to `harness/testdata/r7/cases/`. Files under `harness/testdata/r7/examples/` are non-executable generic syntax illustrations; runners and case oracles never read them. Contracts and registries may name cases and gates but cannot encode their behavior; no production Go, schema, managed asset, or example contains case behavior. |
| `G-R7-ONE-PATH` | One candidate binary digest runs all three cases through the same CLI, Event structure, Handling lifecycle, and peer path. |
| `G-R7-CONTINUITY` | After daemon restart, a fresh Runtime process with no prior transcript resumes from a newly projected View derived from durable Event, Handling, Reference, and Artifact state. |
| `G-R7-FEDERATION` | On a pre-enrolled node retaining durable identity and trust, a fresh Runtime resumes from locally re-admitted PeerDelivery, verified Artifact, and referenced description alone. |
| `G-R7-ROOT-ISOLATION` | No release-path command imports `harness/`. |
| `G-R7-AUTHORITY-CUTOVER` | In activation mode, the candidate tree has exactly one ACTIVE Core contract, marks R5 and older authority claims RETIRED/HISTORICAL, points every parser, registry, Make/CI gate, and active Harness document to R7, and is the exact tree bound by the activation report. |

The generality proof is the conjunction of P-02, `G-R7-CASES`,
`G-R7-PATTERN-FREE`, `G-R7-NO-CASE-KIND`, `G-R7-CASE-DATA-ONLY`, and
`G-R7-ONE-PATH`. This replaces
"two Go packages share an interface", which can be satisfied by designing for
symmetry. The data-only gate is change-isolation evidence, not by itself a
claim that the protocol is universal.

## 10. Evidence bindings

Each invariant and gate has an evidence result derived from the current run;
that result is never written into this document as contract lifecycle status.
The tracked `harness/test/contracts/r7-requirements.json` is the sole evidence
binding authority. Its versioned schema contains two closed lists:

- `invariants`: P-01 through P-10, each bound to exact
  `package::test-symbol` or deterministic oracle names;
- `gates`: every gate in section 9, each bound to its required step IDs, exact
  argv, and independent oracle.

A machine-generated gate report binds the source tree, this contract digest,
requirements registry digest, exact executed commands, exit results, and
output digests. The validator rejects a missing, unknown, duplicate, skipped,
or unexecuted binding; commands hard-coded only in validator code have no
authority. This contract cannot activate while any invariant or gate is
unbound, partially proven, or failing.

| ID | Required independently asserted behavior |
|---|---|
| P-01 | Forged authority fields fail; authenticated actor context determines source; imported origin fields cannot override local identity; for fresh operations, stale View authority digest and every unoffered known consequence, successor target, or opaque handle fail; related evidence is provenance-only and cannot become a writable subject. |
| P-02 | Unknown valid kind and first-publish Reference key traverse the generic path without registration; unknown consequence and every illegal consequence combination fail; no case-specific dispatch exists. |
| P-03 | Interactive root initiation succeeds; a private Host-boundary nonce binds one attachment; authority permits one unended attachment per Principal; same-boundary begin exactly replays and must match the private journal across response loss, missing journal commit, and restart; a fresh nonce atomically replaces its predecessor even when the journal is absent; expired, ended, or divergent outcomes never report ready; Pi retries only the same nonce and emits no cue on failure; a new boundary or Hook end finishes a presented terminal without replaying its old Intent; T0 exposes no managed-wake issuance path; every accepted local target creates exactly one Handling; wrong-Principal and wrong-attachment claim fail. |
| P-04 | At most one live claim exists; a fresh operation with stale fence fails; accepted advance updates the Handling head and releases the claim; bounded lease-expiry and Host-boundary-end dispositions, including transactional boundary replacement, clear occupancy but cannot change domain state, create an Event, or create completion; repeated fresh boundaries over a fixed bounded open set select the least previously claimed Handling and cannot be monopolized by one old responsibility. |
| P-05 | Fault injection at each BoundIntent and VerifiedPeerDelivery transaction boundary yields either the whole local outcome or none, including outbox obligation and Reference head where allowed. |
| P-06 | Authenticated delivery is re-admitted under the restricted peer subset; rejection creates no receiving fact; acceptance creates a new receiving Event, preserves provenance and an unchanged stable correlation root, resolves the target locally, and follows the bounded outbox/inbox lifecycle; a View offers a route only to its fixed local-target Principal; ordinary imported work preserves `reply_required=true` and its authenticated reply context across local advances; every ordinary remote-directed Event atomically binds each outbox to its exact source-local open reply anchor and machine-derived expected reply root — the current Handling for subject advance, or a new source-local successor for root and ordinary resolve — while an exact correlated terminal disposition closes only the responder's imported Handling, creates exactly one unanchored return delivery, and cannot request another reply; terminal return admission requires the exact bound route, root, Principal, and still-open Handling; missing correlation or binding, a closed anchor, wrong Principal, and wrong route reject regardless of `kind` or payload; an imported integration standing alone grants no reply authority, but a later explicit local ordinary outbound Intent may bind that still-open Handling as its new anchor; an accepted terminal response creates a separate integration Handling with `reply_required=false`, no `reply_target`, and no automatic effect on the requester anchor; stale routes, remote rejection, and expiry cannot change these rules. |
| P-07 | Same key/same digest replays the byte-stable attachment-begin proof, frozen View including its focus projection, admission Receipt, or internal outcome before the relevant mutable validation; same key/different digest conflicts; Host retries reuse one nonce and compare replayed proof with private journal authority; response loss, missing journal commit, restart, and retry create at most one attachment, claim, local Event, or machine disposition; begin replay never renews expiry or revives an ended boundary. |
| P-08 | Valid first-publish key creation without a prior handle, invalid key rejection, first-publish CAS, concurrent first publish, active supersede, tombstone retract/reactivation, stale head, forward reference, concurrent mutation, and replay all match section 5; a provenance citation records the exact head, mutates nothing, and cannot stand in for a Reference action. |
| P-09 | Any missing or mismatched Artifact keeps peer input unadmitted and cannot activate a Reference or complete; every Agent/peer resource bound fails closed independently and payload cannot raise it; Pi executes no more than sixteen exploration calls plus two calls to its fixed no-shell Effect-settlement tool per governed run, never re-enables a Host-disabled tool, blocks excess calls, and after cutoff allows only one bounded final-response turn after the last settlement attempt; automatic continuation cannot refresh either budget; the maximum accepted payload, Artifact, Reference, route, current, and related combination remains representable; related evidence has explicit prefix/count/truncation, and JSON-escaped current plus related payloads cannot overflow the focus or canonical View budget; more than the expiry-maintenance limit settles only the bounded prefix and leaves all excess claims unchanged for later natural turns. |
| P-10 | Only explicit completed with attached verified Artifact projects completed; other terminal outcomes close without Artifact; final/exit/idle/provider/ACK/disposition cannot complete. |

Ten rows is the point. A row may bind several test symbols, but it is verified
only when every named behavior in that row has independent evidence. A ledger
small enough to close is worth more than a comprehensive one that never does.

## 11. Activation and retirement

There is never a period in which an unmeasured contract holds authority.

```
The activated tree, with evidence 10/10 and every gate in section 9 green
  R7 marked ACTIVE
  R5 marked RETIRED
  all contract parsers, registries, Make targets, CI, and active Harness docs
    point to R7
  make harness-verify runs the R7 gate

On a non-authority branch this remains a candidate status marker. The switch
becomes effective atomically when that unchanged tree reaches the protected
authority branch.
```

Status markers on a non-authority branch are candidates, not authority. CI
proves the candidate tree rather than a commit hash, so the same tree can become
authoritative after merge without a self-referential evidence cycle.

On retirement, `docs/harness/r5-core-contract.md` records in its header: the
contract that supersedes it, the reason, that retirement takes effect in the
first authority-branch commit containing that header, that its evidence ledger
stops growing, that it is reproducible only on a historical branch or tag, and
that it no longer constrains the active Harness. It must not embed the hash of
the commit that contains its own retirement text.

The candidate and authority gates machine-check that exactly one tracked Core
contract is marked ACTIVE. At the same switch, every older tracked Harness
document that calls itself an authority, frozen face, or active ABI is either
updated for R7 or explicitly marked HISTORICAL/RETIRED; active quickstarts must
not teach an R7-forbidden registry or schema-loader path.

### 11.1 Engineering prerequisites

These are prerequisites for running the evidence, not part of the protocol
model:

- `cd harness && go build ./...` succeeds on darwin;
- the contract tool parses `PROPOSED | ACTIVE | RETIRED`, rejects an activation
  candidate tree without exactly one ACTIVE Core contract, and binds reports to
  the exact candidate tree;
- the fast release-path CI gate cannot generate or claim a complete R7 evidence
  report; Harness-affecting changes and scheduled verification run the complete
  `make harness-verify` gate.

Without them the ledger cannot be produced on the maintainer's own machine or
enforced on merge, and `10/10` would be a number in a document.

## 12. Non-normative material

Nothing in this section is a requirement.

| Artifact | Role |
|---|---|
| `harness/.../mnemond.md` | One-page Agent-facing projection: how to read a View, submit an Intent, read a Receipt, and save or supersede a collaboration description. It is a projection, never authority, and must not contain an ordered workflow. |
| `harness/testdata/r7/examples/view-intent-receipt.md` | A non-executable, pattern-neutral syntax illustration. It contains no topology, fault schedule, expected outcome, or oracle; runners never read it. It must be deletable — see `G-R7-PATTERN-FREE`. |
| Case 1, Case 2, Case 3 | Review, Contract Net, and Blackboard exist only as Markdown descriptions, fixtures, and independent oracles under `harness/testdata/r7/cases/`; each case directory is its sole behavior authority. |
| `.mnemon-dev/architecture/r7/` | Derivation history. No authority. |
| `.mnemon-dev/research/magent-wiki/` | A pattern corpus an Agent may read on demand. No pattern is built in. |

The intended growth path is that an Agent meets a problem, designs an event
interaction for it, uses it on real work, saves the effective version as an
Artifact under a Reference key, and a later Agent discovers, adjusts, or
supersedes it. What evolves is the team's collaboration knowledge. The rules in
section 5 do not.
