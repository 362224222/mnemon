# R5 Core Contract

Status: release contract for the experimental Harness R5 Core.

This document is the tracked authority for the first complete R5 collaboration
slice. It replaces the ignored R5 planning documents as merge and release
authority for this slice. Those documents remain design history and may explain
why a mechanism exists, but they cannot add a release requirement.

## 1. Product outcome

R5 Core proves one supportable Codex-only collaboration path with three
participants and one Channel:

1. each blank workspace is set up once;
2. the entry participant creates a Channel and two known participants join;
3. one natural prompt creates the business request;
4. one remote participant performs the delegated work;
5. the other remote participant performs an independent review;
6. the review may request one or more rework iterations;
7. the corrected result returns to the entry participant;
8. only explicitly selected Artifacts move between participants; and
9. response loss, duplicate delivery, temporary offline periods, daemon
   restart, and concurrent Runtime attempts do not lose or duplicate the
   semantic result.

The user does not manually start a daemon or operate peers, topics, publish,
pull, wake, or sync commands during this path.

R5 remains experimental and is built from `harness/`. The root `mnemon`
release path remains a separate product.

## 2. Deliberate non-goals

The following capabilities are not part of R5 Core and must not remain as
pending release requirements:

- Claude projection or Claude Runtime support;
- more than one Channel in the release narrative, overlapping Channel
  workflows, `offer --to auto`, `offer --to team`, broad reviewer expansion,
  nested governance, or organization policy;
- DHT, anonymous discovery, transitive trust, capability matching, RBAC,
  automatic topic bridges, MCP emission, or a general action/capability
  framework;
- replacement of the existing Gossip plus origin Pull transport solely to
  reduce code size;
- automatic eviction of accepted Artifacts;
- candidate CLI or local API routes whose only caller is an E2E assertion,
  including `channel replay-probe`;
- scripted policy, marker, receipt, or result files presented as Live Host
  evidence;
- strict production-file/test-file basename pairing; and
- preservation of a per-violation quality baseline after the surviving source
  can be protected by focused gates.

Removed behavior is deleted through its complete model, durable state, worker,
API, test, fixture, and documentation path. It is not retained behind a
permanent feature flag.

## 3. Authority, trust, and privacy

Node identity and Channel membership are signed. The Channel owner signs a
bounded roster. Events, publications, operations, and Artifacts have canonical
identities bound to their content or request digest. SQLite is the durable
single-writer authority for local state, and accepted remote input crosses one
durable Inbox.

An Event audience is an application authorization boundary, not a
confidentiality claim. A Channel member participating in transport may observe
the signed Event envelope and content distributed on that Channel. Secrets
that must be hidden from Channel members must not be placed in Event content.
Artifact bytes are fetched only by an authorized participant and are verified
against their content root.

Profile credentials, Node keys, claim tokens, grant bearer material, provider
credentials, local filesystem paths, and other secrets must not enter a
publication, ordinary status output, log, or committed evidence.

Validation fails closed on unknown authority, invalid signature, digest
mismatch, scope mismatch, stale fence, corrupted durable state, or an
out-of-bounds frame. No fallback authority may silently accept such input.

## 4. Closed action set

R5 Core has exactly these managed Teamwork actions:

| Action | Purpose |
|---|---|
| `teamwork.offer` | Offer the root work or an independent review task. |
| `teamwork.accept` | Accept an offered task. |
| `teamwork.decline` | Decline an offered task with a reason. |
| `teamwork.deliver` | Return work, review, or corrected work with selected Artifacts. |
| `teamwork.rework` | Request correction of a delivered result. |
| `teamwork.close` | Accept the final delivered result and close the root work. |
| `teamwork.cancel` | Terminate non-final home work safely. |

Expiry remains a bounded system transition rather than an additional Host
action. Concurrent terminal transitions have one durable outcome. No generic
action registration framework is required by this contract.

## 5. Network and recovery model

The initial transport is official Go libp2p GossipSub between explicitly known
Channel members plus bounded origin Pull repair. Both paths validate the same
canonical publication bytes and converge on the same Inbox transaction.
There is no discovery plane or automatic cross-Channel forwarding.

Network I/O never occurs while a global Channel or mesh lock is held.
Enrollment prepares a bounded reservation, performs I/O outside the global
lock, and commits only after rechecking authority and fence. Every goroutine
has an owner, cancellation path, bounded work, and a bounded wait path.

A caller supplies a stable operation key and request digest for every durable
mutation that can outlive a response. Retrying the same key and digest returns
the committed semantic result. Reusing a key with a different digest fails.
Sensitive bearer material is returned through a dedicated secret result and is
not copied into an ordinary plaintext receipt.

## 6. Artifact retention

R5 Core accepts only explicitly selected, bounded workspace-relative paths.
Capture records the content root, producer, Work, source, authorization, and
required pin provenance. Transfer is resumable or safely restartable, verifies
the complete digest, and quarantines or removes partial invalid content before
it can become authoritative.

Accepted Artifacts and their required provenance pins are retained
immutably. Automatic cleanup is limited to unaccepted, failed, expired, or
orphan staging. Accepted content is removed only by an explicit eject or
abandon operation defined by a later contract.

## 7. Status and diagnostics

Ordinary `status` is a bounded operational projection, not publication
history. It reports current integration, daemon, Channel, peer, cursor/gap,
retry, Work, and diagnostic state using bounded summaries and counters.
`doctor` may add diagnostics but must not require full retained history.

The projection remains authoritative with at least 1,000 retained
publications. A bounded query result is not treated as proof that no newer
state exists; aggregate state or a cursor supplies that authority.

Permanent retry failure becomes a fenced terminal state with a public
diagnostic and an explicit recovery policy. Scoped repair wakeups preserve
their Channel and peer scope. Only an explicit global-authority change may
invalidate all schedules.

## 8. Canonical requirements

Every row below is one canonical MUST clause. The Owner column names the
single subsystem responsible for the clause. The Evidence column names one
primary closed gate and the lowest evidence layers that can prove the
behavior. Supporting evidence may run in an earlier gate, but it does not
create another requirement owner.

Exact accepted commits, test symbols, scenario keys, and evidence identifiers
are maintained in `harness/test/contracts/requirements.json`. That registry is
an evidence projection of these clauses, not a second requirements authority.
Its status is derived from validated evidence. A requirement is release-ready
only when its registry status is `verified`.

| ID | Level | Canonical clause | Owner | Evidence |
|---|---|---|---|---|
| SC-01 | MUST | Root release packages must not import Harness, and root build, help, setup, and legacy persistence behavior must remain unchanged. | `harness/test/contracts` | `G-ROOT` static + process |
| SC-02 | MUST | The release workflow must complete the three-participant, one-Channel Codex path in section 1 from one natural entry prompt without manual transport operations. | `harness/test/e2e` | `G-LIVE` Live with paired Docker |
| SC-03 | MUST | R5 Core must project the real Codex Hook, Skill, Guide, registration, and closed action schemas, and must not install or invoke Claude or a generic emitter. | `harness/internal/integration` | `G-PROCESS` unit + process |
| SC-04 | MUST | Host projection directories must remain generated surfaces while durable canonical project state remains under the Harness state root. | `harness/internal/integration` | `G-CONTRACT` static + unit |
| LC-01 | MUST | Setup must be idempotent, preserve unrelated user files, verify managed asset revisions, and support a clean eject of only owned assets. | `harness/internal/integration` | `G-PROCESS` process |
| LC-02 | MUST | A managed invocation must automatically ensure exactly one bounded local daemon and one SQLite writer without requiring a user daemon command. | `harness/internal/node` | `G-PROCESS` process |
| LC-03 | MUST | Every response-loss-sensitive durable mutation must use a caller-stable operation key plus an independently verified request digest and must replay the committed result without repeating the mutation. | `harness/internal/store` | `G-PROCESS` unit + process |
| LC-04 | MUST | Concurrent Runtime attempts must yield one fenced `current` owner and an observable loser; a `none/none` outcome must fail the concurrency oracle. | `harness/internal/agent` | `G-PROCESS` process + Docker |
| LC-05 | MUST | A managed Host turn must link Hook invocation, current claim, context, action, resolve, and terminal receipt without trusting an Agent completion statement. | `harness/internal/agent` | `G-LIVE` Docker + Live |
| CH-01 | MUST | A Channel must have a canonical identifier, signed owner authority, and an owner-signed roster with monotonic revision, a hard cap of eight active members, and exactly three members in release evidence. | `harness/internal/peer` | `G-UNIT` unit + process |
| CH-02 | MUST | Channel create and invite must satisfy LC-03, and invite bearer material must not be persisted in an ordinary plaintext operation receipt. | `harness/internal/peer` | `G-PROCESS` process |
| CH-03 | MUST | Join must reserve bounded local state, perform network I/O without global Channel or mesh locks, and commit only after authority and fence revalidation. | `harness/internal/peer` | `G-PROCESS` unit + process + race |
| CH-04 | MUST | Membership changes must be signed, monotonic, isolated by Channel, and terminal leave or removal must prevent later ordinary member traffic from being accepted. | `harness/internal/peer` | `G-PROCESS` unit + process |
| CH-05 | MUST | Gossip delivery and origin Pull repair must validate identical canonical publication bytes and converge exactly once through the durable Inbox. | `harness/internal/peer` | `G-DOCKER` process + Docker |
| CH-06 | MUST | Baseline, cursor, acknowledgement, and gap state must recover after offline periods and restart, and repair wakeups must retain their Channel and peer scope. | `harness/internal/store` | `G-DOCKER` process + Docker |
| CH-07 | MUST | Audience authorization and Channel-member transport visibility must follow section 3, with no claim that audience filtering provides content confidentiality. | `harness/internal/event` | `G-CONTRACT` static + unit |
| CH-08 | MUST | Channel frames, rosters, queues, peers, workers, Pull pages, and retry work must be bounded, with no DHT, anonymous discovery, or automatic topic bridge. | `harness/internal/peer` | `G-UNIT` unit + process |
| EW-01 | MUST | Event, publication, operation, Work, Handling, and result identities must use canonical encoding and bind immutable content, scope, and authority. | `harness/internal/model` | `G-UNIT` unit + fuzz |
| EW-02 | MUST | Accepting a business Event must atomically commit its Event, Work, Handling, publication, and required Artifact pin transitions or commit none of them. | `harness/internal/store` | `G-PROCESS` unit + process |
| EW-03 | MUST | Duplicate Gossip, Pull, retry, restart, and response-loss delivery must resolve to one durable Inbox classification and one semantic application. | `harness/internal/store` | `G-PROCESS` process + Docker |
| EW-04 | MUST | A Work must have one home authority, one active assignee or reviewer transition, monotonic iteration, and one race-safe terminal result. | `harness/internal/teamwork` | `G-PROCESS` unit + process |
| EW-05 | MUST | The seven actions in section 4 and bounded expiry must express offer to one explicit Channel-local participant, independent review, optional rework, final delivery, close, and safe decline or cancel, with no `auto`, `team`, batch, or generic scheduler surface. | `harness/internal/teamwork` | `G-DOCKER` unit + Docker |
| EW-06 | MUST | No managed action may implicitly cross a Channel or create another Channel, and the release evidence must use exactly one Channel. | `harness/internal/teamwork` | `G-DOCKER` static + Docker |
| AR-01 | MUST | Artifact authority must bind content root, producer, Work, source, authorization, and pin provenance, and evidence must expose those fields together. | `harness/internal/artifact` | `G-EVIDENCE` unit + process |
| AR-02 | MUST | Only explicitly selected workspace-relative Artifact paths may be captured or fetched, and only an authorized participant may fetch their bytes. | `harness/internal/artifact` | `G-DOCKER` unit + Docker |
| AR-03 | MUST | Artifact entry count, path length, root count, total size, frame size, and secret or traversal rejection must be bounded and fail closed. | `harness/internal/artifact` | `G-UNIT` unit + fuzz |
| AR-04 | MUST | Interrupted Artifact transfer must resume or restart safely, verify the complete digest before acceptance, and quarantine or remove invalid partial data. | `harness/internal/artifact` | `G-DOCKER` process + Docker |
| AR-05 | MUST | A delivered result must not become authoritative until every required Artifact closure member and provenance edge is present and verified. | `harness/internal/store` | `G-PROCESS` unit + process |
| AR-06 | MUST | Accepted Artifacts and required pins must not be automatically evicted; automatic cleanup may remove only unaccepted, failed, expired, or orphan staging. | `harness/internal/store` | `G-PROCESS` unit + process |
| OP-01 | MUST | Ordinary status and doctor must use bounded current-state queries and remain authoritative, bounded in size, and within the recorded latency threshold after at least 1,000 retained publications. | `harness/internal/store` | `G-PROCESS` unit + process |
| OP-02 | MUST | Graceful shutdown must use one process-level deadline observed by listeners, handlers, workers, and goroutine drains, and budget exhaustion must return control to the executable or supervisor. | `harness/internal/node` | `G-PROCESS` unit + process |
| OP-03 | MUST | Durable retries must have explicit time or attempt bounds, and permanent failure must reach a fenced terminal state with a public diagnostic and defined recovery policy. | `harness/internal/store` | `G-PROCESS` unit + process |
| OP-04 | MUST | No network I/O or unknown or reentrant callback may run while a global runtime lock or SQLite sole-connection transaction is held. | `harness/internal/node` | `G-PROCESS` static + race + process |
| OP-05 | MUST | Every worker and claim must have an owner, lease, attempt, fence, cancellation path, bounded work, and a wait path that is joined before ownership ends. | `harness/internal/store` | `G-PROCESS` unit + race + process |
| EV-01 | MUST | The tracked evidence registry must contain exactly these 42 IDs, reject unknown gates and pending MUST entries, and derive every `verified` state from behavioral evidence. | `harness/test/contracts` | `G-CONTRACT` static + unit |
| EV-02 | MUST | Every declared fault must record a public precondition, external fault action at the declared phase, and public postcondition; absence of the precondition or action must fail. | `harness/test/e2e` | `G-EVIDENCE` Docker |
| EV-03 | MUST | The Hermetic suite must use isolated homes, workspaces, state, keys, real processes, and real network partition to prove the full workflow and required recovery cases. | `harness/test/e2e` | `G-DOCKER` Docker |
| EV-04 | MUST | Live evidence must use the real Codex Host path and Runtime-neutral public receipts, contain no scripted-only inputs, and pair with Hermetic evidence by exact commit and image digest. | `harness/test/e2e` | `G-LIVE` Live + evidence validation |
| EV-05 | MUST | Artifact, ordered DAG provenance, cursor and gap, managed Host, and performance claims must be checked by independent oracles over the exact supporting fields, samples, and timestamps. | `harness/test/e2e` | `G-EVIDENCE` unit + Docker + Live |
| EV-06 | MUST | Untrusted canonical JSON and network frame parsers must have positive and negative unit coverage plus bounded fuzz targets. | `harness/internal/model` | `G-UNIT` unit + fuzz |
| EV-07 | MUST | CI must enforce root build and tests plus Harness build, unit, race, process, Hermetic Docker, contract, quality, and evidence validation through the same merge gate used locally. | `.github/workflows` | `G-CONTRACT` CI |
| EV-08 | MUST | Release review must bind commit, tree, and image; contain no credentials or generated run data in Git; show a clean tree and logical commits; and provide complete PR scope, validation, risks, removals, and deferred work. | `harness/test/contracts` | `G-EVIDENCE` static + evidence validation |

## 9. Closed evidence gates

Only these gate identifiers are valid:

| Gate | Closed only when |
|---|---|
| `G-CONTRACT` | The canonical clause projection, exact ID set, owner paths, evidence bindings, and all-verified rule pass. |
| `G-ROOT` | Root build, unit/E2E behavior, persistent data expectations, and the no-Harness-import check pass. |
| `G-UNIT` | Harness build, unit, race, parser and codec fuzz smoke, signature, digest, transition, authorization, CAS, and bounds checks pass. |
| `G-PROCESS` | Real-process and SQLite crash-window tests for setup, response loss, lease and fence, restart, shutdown, terminal retry, scoped wake, and 1,000-publication status pass. |
| `G-DOCKER` | A fresh three-node Docker run proves the complete workflow, real network partition and reconnect, Pull repair, Artifact transfer, and required recovery cases. |
| `G-EVIDENCE` | Independent fault, Artifact, DAG, cursor, Host, performance, redaction, and commit/tree/image evidence validators pass. |
| `G-LIVE` | A fresh natural-prompt Codex run proves Host integration, remote work, independent review, optional rework, final result, and an independent workspace oracle using the paired commit and image. |

A gate name outside this table is invalid. A gate cannot close from a test name,
commit hash, Agent statement, or generated marker alone.

The merge and release compositions are:

```text
core-verify =
  G-CONTRACT + G-ROOT + G-UNIT + G-PROCESS + G-DOCKER + G-EVIDENCE

core-release-verify =
  core-verify + G-LIVE + zero pending in-scope MUST
```

## 10. Merge and release rule

R5 Core is mergeable only when:

- all 42 requirement records are `verified` and all seven gates above close;
- every review blocker within this contract is fixed or the corresponding
  candidate surface is removed;
- the Hermetic and Live evidence pair names the exact same source commit and
  candidate image digest;
- the final diff against `master` is reviewed for behavior, dependency,
  schema, evidence, and root-path impact;
- no `.testdata`, generated evidence, Live transcript, temporary JSON, log,
  credential, or Host-local configuration is tracked;
- the worktree is clean and the work is split into logical Conventional
  Commits without history rewriting; and
- a PR title and body record scope, removals, behavior changes, validation,
  risks, and deferred work.

Live provider unavailability is a release blocker, not permission to substitute
scripted evidence. If remote authentication alone prevents PR submission, the
exact branch and complete PR text are the handoff artifact.
