# R5 Implementation Review

| Review metadata | Value |
|---|---|
| Date | 2026-07-26 |
| Branch | `feat/r5-architecture` |
| Reviewed commit | `225b43e` |
| Comparison base | local `master` |
| Scope | review only; no production or test implementation was changed |

## 1. Executive conclusion

R5 has implemented a substantial amount of real infrastructure, and several of
its core directions are sound: the experimental Harness is kept out of the root
release import graph; local state has a single writer; Event, publication,
Artifact and operation identities are digest-bound; remote input is checked
fail-closed; work queues and protocol frames are bounded; Gossip and Pull
converge on one durable Inbox.

However, the current state **must not be described as R5 complete or
release-ready**.

The main reason is not merely that Live Codex evidence is absent. The review
found four release-blocking issues:

1. a healthy node's ordinary `status` path fails when its retained local
   Event/Inbox history reaches a 65th publication;
2. several Docker fault cases are evaluated after business completion and can
   report success without injecting the declared fault;
3. the Live five-case runner depends on evidence that is collected or generated
   only in scripted mode, making the current Live gate internally inconsistent;
4. all 103 requirements remain `pending`, while the requirement-registry
   validator still passes and the release target has no all-MUST closure check.

There are also independent implementation risks around response-loss
idempotency, locks held across network I/O, unbounded shutdown waits, durable
leave retries, and cross-Channel wakeup scope.

The implementation shows material overengineering signals in its current T0
form. The clearest signals are:

- 98,013 lines of `harness/cmd` and `harness/internal` production Go, plus
  101,192 lines of Harness Go tests;
- a 3,149-line schema with 47 tables and 170 triggers;
- a 3,127-line E2E case runner;
- a quality baseline that accepts 905 existing threshold violations;
- elaborate accepted-Artifact GC and internal response-replay machinery that
  exceed the stated T0 retention goal;
- a proof system whose complexity has not prevented false-positive fault
  evidence or an uncloseable Live path.

This does **not** mean all of the durability and security machinery should be
removed. Signatures, request identity, durable Inbox, fencing, content
digests, bounded resource use and independent oracles are necessary
complexity. The simplification target should be duplicated authority,
over-expanded T0 scope and evidence machinery that does not prove what it
claims.

Two simplification paths are possible:

- If the current 103-clause contract remains frozen, only implementation and
  evidence internals can be simplified; the overall system will remain large.
- If the product goal is allowed to outrank the frozen implementation contract,
  a smaller R5 can use direct signed push to known members plus origin Pull
  repair, defer accepted-Artifact GC and Claude projection, and prove one real
  vertical slice before expanding to five six-node scenarios.

The recommended immediate action is **not** a transport rewrite. First restore
truth in status and evidence gates, resolve the concrete concurrency/recovery
defects, and then decide explicitly whether the 103-clause contract or the
smaller product outcome is the authority.

## 2. Review basis and limitations

### 2.1 Material reviewed

The review covered:

- the R5 product briefs and the documents under
  `.mnemon-dev/architecture/r5/`;
- `docs/development/go-engineering-standard.md`;
- Harness model, Store, Node, peer, Agent, Artifact, CLI and local API code;
- SQLite schema, triggers and durable worker state;
- unit, process, Docker, Hermetic and Live evidence machinery;
- requirement registries, quality baselines and architecture debt manifests;
- the current scripted five-case evidence bundle;
- the diff and dependency boundary relative to local `master`.

### 2.2 Validation performed

This was primarily a static and evidence review. One focused contract test was
run:

```text
go test ./harness/test/contracts \
  -run '^TestRequirementsRegistryIsClosedAndEvidenceBacked$' -count=1
```

It passed while all 103 registry entries were still `pending`, which directly
confirms one of the findings below.

The existing current-commit scripted bundle at
`.testdata/r5/runs/20260722T025421Z-scripted-all-five-3799e1755030/`
reports a five-case pass. It was inspected as evidence, not rerun in this
review. No Live provider credential was used and there is no current
`.testdata/r5/latest-codex-run` pointer. Findings described as Live failures are
therefore static conclusions about the runner's required inputs, not the result
of a credentialed Live execution.

### 2.3 Scale observed

| Measure | Current value |
|---|---:|
| Diff from `master` | 1,333 files, +225,638 / -64,371 lines |
| Harness `cmd`/`internal` production Go, excluding the quality tool | 352 files, 98,013 lines |
| Harness Go tests | 370 files, 101,192 lines |
| Store production files | 96 |
| SQLite schema | 3,149 lines |
| SQLite tables / triggers / indexes | 47 / 170 / 14 |
| E2E `run_case.sh` | 3,127 lines |
| Quality-baseline debt entries/violations | 905 |
| Architecture-debt entries | 6 |
| Root release dependency closure | 230 packages |
| Harness binary dependency closure | 598 packages |

The R5 current-system audit describes the preceding Harness as approximately
34,506 production lines and 23,808 test lines
(`.mnemon-dev/architecture/r5/current-system-audit.md:331-349`). The new
implementation is therefore about 2.8 times larger in production code and more
than four times larger in tests, despite the stated clean-cut/contraction goal.
This ratio is only a signal; it is not by itself proof of bad design.

## 3. What is architecturally sound

The following properties should be preserved during any simplification:

1. **Release-path isolation.** `go list -deps . ./cmd/...` contains no
   `harness` import. R5 remains under the experimental `harness/` surface rather
   than becoming an implementation dependency of root `mnemon setup`.
2. **Explicit authority.** Channel identity, roster heads, origin identity,
   audience, operation identity, claim ownership and Artifact provenance are
   modeled explicitly rather than inferred from transport or filenames.
3. **One durable receive path.** Gossip delivery and Pull repair use the same
   canonical signed publication bytes and enter the same durable Inbox.
4. **Durable idempotency where implemented.** Teamwork action/resolve paths use
   operation key plus request digest and terminal receipts rather than relying
   on best-effort deduplication.
5. **Fail-closed boundaries.** Local API authentication, signature checks,
   roster binding, frame limits, path validation and CAS digests generally fail
   closed.
6. **Bounded concurrency intent.** Workers, leases, queues, frames, channel
   membership and Artifact closure all have declared bounds and cancellation
   ownership.
7. **Independent evidence mechanisms worth retaining.** Container isolation,
   no shared state, hidden networkless task oracles, evidence hash inventory,
   redaction scans, commit/tree/image binding and Live/Hermetic image pairing
   are appropriate controls.
8. **Disciplined non-goals.** No production Hub, DHT discovery, RBAC/PKI,
   consensus, CRDT, generic scheduler, generic capability loader or MCP action
   surface was found.

The problem is therefore not that R5 has no coherent architecture. It is that
the T0 contract and proof machinery grew beyond the smallest useful product,
and several of the resulting invariants are either duplicated or not actually
proved.

## 4. Findings summary

| ID | Severity | Finding | Classification |
|---|---|---|---|
| R5-001 | P0 | `status` fails and `doctor` loses authoritative online observation after 64 retained publications | confirmed implementation defect |
| R5-002 | P0 | declared Docker faults can pass as post-completion probes | confirmed false-positive evidence |
| R5-003 | P0 | current Live five-case gate depends on scripted-only evidence | confirmed static inconsistency |
| R5-004 | P0 | 103/103 requirements are pending while the registry validator passes | confirmed release-gate defect |
| R5-005 | P1 | Channel create/invite retries can repeat durable mutation | confirmed static idempotency gap |
| R5-006 | P1 | Channel Join holds global locks across network I/O | confirmed lock-inversion/availability risk |
| R5-007 | P1 | daemon shutdown has unbounded waits | confirmed availability risk |
| R5-008 | P1 | permanent leave failures retry indefinitely; scoped wakeups become global | confirmed recovery defects |
| R5-009 | P1 | normative clauses are unavailable in a clean tracked checkout and entry points conflict | confirmed governance defect |
| R5-010 | P1 | Artifact/pin/provenance assertions lack supporting evidence fields | confirmed false-positive oracle |
| R5-011 | P1 | exact DAG, cursor isolation and reviewer identity have weak or same-source oracles | confirmed evidence gap |
| R5-012 | P1 | performance and Host Hook transcript contracts are not gated | confirmed evidence gap |
| R5-013 | P1 | Channel transport visibility conflicts with product privacy wording | product/security semantics gap |
| R5-014 | P1 | candidate `replay-probe` appears primarily E2E-driven | confirmed non-test surface expansion |
| R5-015 | P1 | CI does not run the documented full R5 merge gate | confirmed merge-protection gap |
| R5-016 | P2 | quality ratchet freezes a very large existing debt baseline | maintainability risk |
| R5-017 | P2 | accepted-Artifact GC is disproportionate to the T0 retention contract | overengineering |
| R5-018 | P2 | durable receipts, state authorities and callback surfaces are duplicated | overengineering/risk |
| R5-019 | P2 | root module, schema-v1 reset policy and parser coverage weaken isolation/operability | lifecycle risk |
| R5-020 | P3 | strict basename tests and dual Host scope add avoidable T0 cost | scope/process overhead |

Severity meanings in this report:

- **P0**: blocks an honest completion/release claim.
- **P1**: should be resolved before a user-facing beta.
- **P2**: material maintenance or evolution risk.
- **P3**: useful simplification after higher-priority correctness work.

## 5. Detailed findings

### R5-001 — Ordinary status has a 64-retained-publication observation ceiling

`model.MaxChannelStatusPublications` is 64 and is documented as the bound for a
complete, non-paginated status evidence snapshot
(`harness/internal/model/validation.go:32-34`).

`ReadChannelStatusAuthority` reads the union of every local Event and remote
Inbox publication across all Channels with `LIMIT 65`; row 65 returns
`ErrChannelStatusAuthority`
(`harness/internal/store/channel_status.go:53-95`). Ordinary status
unconditionally calls this full authority reader
(`harness/internal/node/controller_composition.go:152-178`). The unit test
explicitly freezes failure above the bound
(`harness/internal/store/channel_status_test.go:96-105`).

This is not only a theoretical scale issue. The performance contract asks for
at least 100 Event samples
(`.mnemon-dev/architecture/r5/docker-live-acceptance.md:358-379`), while one
node cannot retain and then observe that many publications through normal
status. The contract does not require all 100 samples to reside in one
database, so this is a strong design inconsistency rather than proof that the
performance run itself must fail. `doctor` begins with the same status request
(`harness/internal/cli/doctor.go:171-185`); when the daemon still owns the
lifecycle lease, it loses authoritative online observation and normally falls
back to an inconclusive result.

**Impact:** a healthy, active node eventually loses its normal operational
observation surface. The limit is effectively lifetime-wide unless retained
Channel history is removed.

**Simpler design:** use one bounded `ChannelObservation` for readiness, stage,
lag, queue counts and latest cursors. Move full publication evidence to a
separate paginated/export-only diagnostic path. Health should never scan or
materialize complete retained history.

### R5-002 — Several declared fault tests do not inject the declared fault

The scenario manifests declare exact phases such as handling pending, active
parallel review, direct Artifact pull, and before Runtime launch. The actual
workflow waits for the business result and final ready state, collects
evidence, and only then evaluates most declared faults
(`harness/test/e2e/runner/run_case.sh:3108-3121`).

Concrete examples:

- `dual-runtime-on-c`, `dual-runtime-on-e`, and
  `preclaim-attachment-rename-crash` all use the same helper that launches two
  `agent current` commands
  (`run_case.sh:1667-1753,1981-2029`). It neither launches two Runtime
  processes nor crashes between attachment staging and rename.
- Its predicate accepts `none/none`, because zero actionable responses satisfy
  “at most one actionable” and the loser condition
  (`run_case.sh:1716-1730`).
- In the current passed bundle, both `dual-runtime-on-c` and
  `preclaim-attachment-rename-crash` contain statuses `["none","none"]` while
  all fault booleans are true.
- `large-artifact-receiver-restart` is declared during direct Artifact pull but
  is implemented as a generic restart after the result path has completed
  (`run_case.sh:1961-1969`).

**Impact:** the Hermetic bundle proves that the system remains healthy after
some late probes; it does not prove the declared crash gaps or ownership races.
This invalidates the corresponding fault claims even though the suite reports
`passed`.

**Simpler evidence model:** every fault should have exactly three machine
records:

1. a public precondition receipt proving the declared phase;
2. one external fault action receipt;
3. a public postcondition receipt.

Precise DB/rename crash windows belong in process-seam tests. Docker should
prove only faults it can genuinely inject from outside. A concurrent Runtime
gate must observe one owner and one stable loser; `none/none` must fail.

### R5-003 — The Live suite currently relies on scripted-only evidence

Live and scripted runs use the same manifests and final assertion validation,
but important fixtures and receipts are mode-specific:

- `.r5/policy`, scripted scenario state and scripted task application are
  installed only for scripted mode
  (`harness/test/e2e/runner/run_case.sh:365-421`);
- `.r5/runtime` receipts are copied into evidence only when
  `runtime=scripted` (`run_case.sh:2783-2812`);
- payment receipt-loss evidence is generated by a scripted `/dev/full` path
  (`harness/test/e2e/docker/scripted-agent-turn.sh:59-93,195-201`) and its
  oracle searches `output/runtime/C` (`run_case.sh:819-835`);
- Team expansion and parent-stale assertions also scan scripted runtime files
  (`run_case.sh:1756-1764,2262-2305`);
- the offline repair injection silently returns if a scripted marker is absent
  (`run_case.sh:2672-2685`);
- the evidence validator still requires all declared Live faults and assertions
  to pass (`harness/test/e2e/runner/validate_evidence.sh:214-249`).

**Impact:** without a credentialed execution this review cannot quote the exact
runtime error, but the required Live evidence cannot be produced through the
paths the validator later reads. The current `release-verify` path is therefore
not just unexecuted; it is statically inconsistent.

**Simpler design:** separate:

- a deterministic Hermetic system/fault profile;
- a real Codex task/experience profile;
- a small runtime-neutral public receipt format shared by both.

Pair the two profiles by commit and image digest. Do not require Live Codex to
recreate scripted-private fault fixtures.

### R5-004 — Requirement closure can be green with no verified requirements

The normative completion goal requires every MUST to be `verified`
(`.mnemon-dev/architecture/r5/autonomous-implementation-goal.md:178-196`).
The registry contains 103 entries: 101 MUST and 2 SHOULD in the normative
table. All 103 current registry entries are `pending`.

Only eight entries currently have both an accepted commit and a test symbol;
95 lack that binding. The validator nevertheless accepts both `pending` and
`verified`, and requires complete evidence only after an entry is already
marked `verified`
(`harness/test/contracts/requirements_test.go:293-330`).

`make test-evidence` calls this permissive test, and `release-verify` adds no
all-MUST-verified check (`Makefile:135-159`). This review ran the focused
registry test—not the complete `make test-evidence` or `release-verify`
targets—and confirmed that the registry validator passes in the all-pending
state.

There is a second closure defect: 43 requirements refer to `G-PROFILE`, but the
frozen verification matrix defines no such gate
(`.mnemon-dev/architecture/r5/requirements-and-gates.md:166-182`). The current
validator checks that gate names are non-empty/sorted, not that they belong to
the normative closed set.

**Impact:** the requirement registry currently proves catalog shape, not
completion. A successful release command would not establish that the MUST
contract was delivered.

**Simpler design:** use one shared requirement loader:

- merge gate: permits `pending`, but reports coverage;
- release gate: requires all MUST `verified`;
- SHOULD: either `verified` or an explicit waiver;
- gate names: validated against the closed normative registry;
- verification status: derived from passing evidence, not just from the
  existence of a commit hash and test function.

### R5-005 — Channel create and invite lack response-loss identity

All Channel POST routes use `headerPolicy{}` and the client sends only
authentication, not an operation key
(`harness/internal/localapi/channel_routes.go:233-252`,
`harness/internal/localapi/channel_client.go:163-179`).

Every Create retry generates a new Channel ID, grant ID and bearer secret.
Every Invite retry generates a new token and rotates the current grant
(`harness/internal/node/channel_service.go:56-84,144-195,231-338`).

If a caller receives an ambiguous error after SQLite commits and manually
retries the operation—the current client does not automatically retry:

- retrying Create creates a second Channel and consumes another capacity slot;
- retrying Invite closes/replaces the first grant and returns a different
  secret;
- the caller has no stable identity with which to retrieve the committed
  result.

Store tests prove replay only when the caller supplies the same already-created
objects; the public API cannot recreate those objects after response loss.
This review did not dynamically inject the commit/response gap; the failure
mode follows statically from the missing request identity and fresh randomness
on every manual retry.

**Simpler design:** require operation identity plus canonical request digest on
durable mutating Channel routes. Persist a bounded result receipt. The receipt
alone cannot recover bearer material: either derive that material
deterministically from protected node key material and operation identity or
retain it encrypted until the result is acknowledged; do not put the plaintext
secret in an ordinary durable receipt.

### R5-006 — Channel Join holds global locks across network I/O

`ChannelJoin` holds the global `ChannelManager.mu` while it calls
`EnrollChannel` (`harness/internal/node/channel_service.go:87-135`).
`EnrollChannel` then holds the global `MeshRuntime.mu` while it connects, opens
a stream, performs up to two exchanges, reloads authority and reconciles the
mesh (`harness/internal/peer/mesh_enrollment.go:36-80,146-157`).

The owner-side enrollment handlers also need `ChannelManager.mu`. Reciprocal
joins can therefore leave both nodes holding their local manager lock while
waiting on an inbound handler that needs the peer's manager lock. Each exchange
has a timeout and there are at most two attempts, so this is a time-bounded lock
inversion/mutual timeout rather than a permanent deadlock. Unrelated Channel
status/membership operations remain blocked behind the global mutex during
that interval.

**Simpler design:** under lock, validate and freeze a small enrollment
reservation plus authority digest. Perform all network I/O without the manager
or mesh-global lock. Reacquire a Channel-scoped gate and commit only if the
reservation/digest fence still matches.

### R5-007 — Shutdown does not have a terminal time boundary

The supervisor deliberately removes caller cancellation with
`context.WithoutCancel` so cleanup can continue after caller cancellation, but
it does not replace it with a bounded cleanup deadline. It calls each
component's Shutdown and then waits unconditionally for `runtime.done`
(`harness/internal/node/supervisor.go:241-253`).

The HTTP controller has a five-second `server.Shutdown` timeout but still waits
without a bound for handler drain afterward
(`harness/internal/node/controller.go:324-341`). Other worker drains similarly
depend on every goroutine cooperating with cancellation.

**Impact:** absent external process termination, one stuck handler, callback or
worker can make graceful/in-process shutdown wait indefinitely, retaining the
SQLite writer and preventing a clean restart. This conflicts with the
engineering standard's combined requirements for owned cancellation, bounded
work and an observable wait/deadline path.

**Simpler design:** define one process shutdown budget and propagate its
deadline to components. Every drain/join waits with that deadline. Go cannot
safely kill an arbitrary goroutine, so after listeners/connections receive a
bounded close attempt, budget exhaustion should be escalated to the executable
or outer supervisor for process-fatal termination rather than handled with an
unbounded “graceful” wait or an internal component-level `os.Exit`.

### R5-008 — Durable retry and scoped wakeup semantics are inconsistent

Permanent Channel leave failures are classified separately, but the durable
row remains `queued`/`sent`, is selected again, and receives another attempt.
Backoff caps at ten seconds; the only attempt ceiling is the maximum SQLite
integer
(`harness/internal/node/channel_member_reconciler_state.go:12-67`,
`harness/internal/node/channel_member_reconciler_leave.go:13-53,72-85`,
`harness/internal/store/channel_leave_retry.go:68-159`). The schema already has
an unused terminal `rejected` state.

Separately, channel/peer-scoped repair wakeups discard their scope and invoke a
global trigger. The worker then clears every target's schedule, including
unrelated backoff and permanent suppression
(`harness/internal/node/channel_member_reconciler.go:92-110,149-205`).

**Impact:** a permanent leave error can produce indefinite network traffic and
leave the Channel stuck in `leaving`; one noisy Channel can reactivate work for
all other Channels.

**Simpler design:** persist a fenced terminal rejected state plus diagnostic
after a real attempt/deadline budget; without an owner signature it should not
be called a receipt. Define what happens to the Channel itself—restore active,
settle from a newer roster, or enter an explicit failed/abandon-required
state—so it does not remain silently stuck in `leaving`. Keep a bounded
dirty-key set for scoped wakeups, and reserve a separate explicit
global-authority trigger for the rare cases that really invalidate every
target.

### R5-009 — Normative clauses are absent from clean tracked checkouts and entry points conflict

`.gitignore:21` ignores `.mnemon-dev/`. The autonomous goal explicitly says
that the ignored R5 directory is normative, must not be committed, and cannot
be recovered by a fresh clone
(`.mnemon-dev/architecture/r5/autonomous-implementation-goal.md:35-41`).
The tracked requirement test acknowledges that fresh CI may not contain the
normative source and treats the tracked catalog as authority instead
(`harness/test/contracts/requirements_test.go:86-89`).

This prevents a reviewer or clean CI checkout from reading the clause text
whose digest is being accepted. A clean checkout can validate the tracked
catalog's internal digest shape, but cannot independently recompute the
clause-text-to-digest binding from tracked sources alone. A development
workspace that still has the ignored documents does perform that recomputation.

There are also conflicting entry points:

- `.mnemon-dev/architecture/README.md:8-16` says D4 is not written and
  implementation is not authorized;
- `.mnemon-dev/architecture/r5/README.md:1-9,150-175` says D0-D4 are frozen and
  implementation is authorized;
- the two R5 product briefs create messaging ambiguity around Codex-first
  positioning versus Codex plus Claude, although the frozen ND-14 requirement
  itself clearly requires both projections.

**Impact:** clause digests cannot be independently recomputed from the tracked
repository alone, and project authorization state cannot be determined from
one canonical tracked entry point.

**Simpler design:** commit one concise normative R5 contract and make the R5
README the only status authority. Keep extensive rationale/local notebooks
ignored if desired, but never make an ignored document the only source of a
release requirement. Mark superseded briefs explicitly.

### R5-010 — Artifact and pin assertions are not supported by their evidence

`network_paths_artifact_origin_ok` checks only that a direct Artifact source is
the publication origin and that the semantic outcome is not ignored
(`harness/test/e2e/runner/run_case.sh:2161-2168`).

The same boolean is then used to pass AR-01, producer pin lifetime, closure
authorization, explicit pin and Work-scope assertions
(`run_case.sh:2371-2380,2499-2517`). But
`harness/test/e2e/schemas/network-paths.schema.json:66-105` contains no
Artifact root, produced/referenced role, producer Event/Run/operation, pin
state, receipt time or authorization fields.

**Impact:** the current evidence can prove a source relationship, not the
retention/provenance properties claimed by the report.

**Simpler design:** emit one bounded public Artifact receipt with root digest,
producer Event/Run, Work, source, pin reason and pin state. Compare before/after
receipts and add two focused negative cases: unauthorized pull and premature
cleanup. Do not make the generic network-path record prove the full Artifact
lifecycle.

### R5-011 — Exact DAG and cursor claims use weak or same-source oracles

The scenario schema freezes an exact path, but the system checker largely
checks a minimum hop count and that child identity differs from the cause; it
does not compare the ordered Node/Channel path
(`harness/test/e2e/runner/run_case.sh:2096-2120`).

The “one Channel sequence cannot fill another Channel's gap” assertion checks
that one Event key does not occur in two Channels, rather than inspecting
cursor/gap movement (`run_case.sh:2123-2129,2460-2468`).

For reviewer identity and causality, the scripted task application writes the
expected reviewer/causality/pass fields, and the hidden oracle reads those same
fields (`harness/test/e2e/runner/scripted_task_apply.sh:245-254`,
`harness/test/e2e/scenarios/overlapping-channels/oracle/oracle.sh:23-31`).
The hidden code tests remain useful and independent; the provenance judgment
does not.

**Simpler design:** derive an ordered
`(source Event, node, Channel, WorkRef, result root)` DAG from public action
receipts and compare it exactly with the manifest. Give the hidden oracle a
read-only provenance document and make it cross-check the workspace result
instead of trusting a scripted `"pass"` field.

### R5-012 — Performance and Hook-transcript contracts are not evidence gates

The contract requires at least 100 Event samples, direct/relay/repair
p50/p95/p99, Event-to-Inbox, Inbox-to-Runtime, reconnect-to-repair and runner
resources
(`.mnemon-dev/architecture/r5/docker-live-acceptance.md:358-379`).

The report schema contains only setup, Channel join and Channel ready arrays
(`harness/test/e2e/schemas/report.schema.json:85-93`). The runner and validator
generate/check only those fields
(`harness/test/e2e/runner/run_case.sh:3070-3083`,
`harness/test/e2e/runner/validate_evidence.sh:145-174`).

The same contract requires a transcript for Hook invocation, exit, fixed cue,
current/action/resolve receipts and context lifecycle
(`docker-live-acceptance.md:432-435`). Live stdout is intentionally discarded,
the report has no managed-turn record, and the validator checks integrity only
for files that happen to exist (`run_case.sh:2633-2641`,
`validate_evidence.sh:33-95,185-212`).

**Simpler design:** move performance out of the five narrative scenarios into
one fixed 100-Event micro-suite with receipt timestamps and resource summary.
For managed turns, emit one small public transcript record per turn and require
an exact count/linkage in the case validator.

### R5-013 — Audience authorization is not content confidentiality

The product brief promises that each node keeps its own data and shares only an
explicit description and selected artifacts
(`.mnemon-dev/r5-product-brief.md:50-57`).

The architecture instead says every active Channel member may receive, verify,
persist and relay complete publication bytes; `audience` only controls semantic
application and Artifact access
(`.mnemon-dev/architecture/r5/problem-statement.md:141-145`).
The publication embeds the entire Event
(`harness/internal/model/publication.go:41-73`), whose fields include summary,
payload and Artifact references
(`harness/internal/model/event.go:104-117`).

**Impact:** a non-audience Channel member receives the work description and
payload even though it records an `ignored` outcome. That is authorization for
effects, not confidentiality.

**Simpler choices:**

1. explicitly document each Channel as a full-content trust domain; or
2. use direct audience-only push/pull; or
3. encrypt payloads for the audience.

Option 1 is smallest but weakens the product wording. Option 2 aligns naturally
with a smaller known-member T0. Option 3 preserves Gossip relay but adds more
cryptographic/key-distribution complexity and is not the simplification path.

### R5-014 — An apparently E2E-driven probe expanded the candidate control surface

The frozen CLI/local API contract does not list `channel replay-probe`, but the
experimental Harness's non-test CLI and server expose it
(`harness/internal/cli/channel.go:85-131`,
`harness/internal/localapi/channel_routes.go:8-35`).

The peer implementation does not send an adversarial message through the real
Gossip network. It constructs a `pubsub.Message` in the daemon and directly
calls its own validator
(`harness/internal/peer/topic_replay_probe.go:19-56`).

**Impact:** the candidate CLI, API, Node and peer surfaces were enlarged to let
Docker invoke an in-process validator mechanic. The probe does use a real
signed publication, target session and durable before/after counts, so it
proves validator rejection and mutation suppression. It does not prove
transport-level wrong-topic routing.

**Simpler design:** keep wrong-topic behavior as a peer parser/validator unit or
process test. If a Docker-level adversarial test is required, inject through a
separate external peer/container using the real protocol. Do not keep a
candidate self-test route solely to make an E2E assertion possible.

### R5-015 — CI is weaker than the documented merge gate

The R5 acceptance document says `make verify` is the merge gate and includes
layout, unit/race, process and the full Hermetic Docker suite
(`.mnemon-dev/architecture/r5/docker-live-acceptance.md:445-460`).

CI instead runs build, `go test ./...`, root E2E and `make harness-verify`
(`.github/workflows/ci.yml:27-39`). The recursive Go tests include the process
test package, but `harness-verify` does not run the dedicated race, Docker or
complete evidence target (`Makefile:70-79,144-147`).

**Impact:** a PR can merge without the gate the design calls mandatory. The
scripted bundle may be run manually, but it is not protected by this workflow.

**Simpler design:** make the required distinction explicit:

- fast required PR gate;
- required or separately protected Hermetic gate;
- Live release gate.

If Docker cost makes it unsuitable for every PR, use a required queued workflow
or protected candidate branch rather than naming an unenforced command as the
merge gate.

### R5-016 — The quality gate is a debt ratchet, not a quality proof

`go_quality_baseline.json` accepts 905 existing threshold violations:

| Rule | Baseline entries |
|---|---:|
| cyclomatic complexity | 291 |
| cognitive complexity | 224 |
| function statements | 178 |
| function logical lines | 91 |
| production file length | 62 |
| normalized duplicates | 28 |
| paired test file length | 21 |
| control-flow nesting | 10 |

Examples include:

- `NewAgentRun`: cyclomatic complexity 87;
- `verifyOwnedChannelEnrollmentLedger`: 72;
- `peer_inbox_artifact.go`: 1,761 lines;
- `codex_adapter.go`: 1,520 lines;
- `artifact_receiver.go`: 1,473 lines;
- `channel_frame.go`: 1,447 lines.

The Go engineering standard sets the cyclomatic new-code violation threshold at
20, a hard cyclomatic ceiling of 30, and a production-file target of 400 lines;
other rules have their own thresholds
(`docs/development/go-engineering-standard.md:248-260`).

The ratchet is useful because it prevents silent growth or baseline laundering.
It does not establish that the current code meets the standard. The six-entry
architecture debt file similarly records duplicate closed sets, dependency
direction, transaction-shell duplication and an unexpected `internal/cli`
package, but does not force their scheduled removal.

**Simpler process:** keep historical lineage and non-increase checks, but call
the result a debt ratchet. After architectural consolidation, retain a small
reviewed exception list rather than one record for every oversized
function/file. Add positive and negative fixture tests for critical evidence
predicates; a green ratchet cannot compensate for a false oracle.

### R5-017 — Artifact GC exceeds the T0 retention requirement

The T0 contract retains accepted roots/provenance and requires automatic cleanup
only for unaccepted temporary, failed or orphan staging data
(`.mnemon-dev/architecture/r5/operation-and-evidence-contract.md:511-522`).

The implementation nevertheless has a large crash-safe accepted-Artifact GC
system:

- `harness/internal/artifact/cas.go`: 1,320 lines;
- `harness/internal/artifact/gc.go`: 1,158 lines;
- `harness/internal/store/artifact_gc.go`: 1,190 lines;
- multiple scan, queue, prepare, completion and guard tables plus triggers.

This machinery adds cursors, tombstones, receipts, retries, fencing and many
cross-table invariants for a behavior the T0 product need not perform.

**Simpler T0:** never automatically evict accepted objects. Keep staging in
operation-scoped directories and run a bounded startup/background TTL sweep
only for unreferenced staging after a grace period. Provide explicit
eject/abandon cleanup. Add accepted-byte reclamation only after real storage
pressure and retention policy exist.

### R5-018 — Durable lifecycle and authority are repeated across layers

Several internal same-daemon workers have separate renew/source/transition
receipt tables and replay paths. Some are justified: a source receipt can be
immutable externally observable provenance, and a renew result can carry a new
fence. Others may be recoverable by rereading the one SQLite authority row
under an exact `(id, attempt, owner, lease_until)` fence. That simplification
must be demonstrated receipt by receipt; same-process execution alone does not
remove crash-after-commit ambiguity.

The architecture debt manifest already records:

- repeated closed-set authorities;
- Store transaction lifecycle duplication;
- unexpected `internal/cli`;
- dependency-direction debt
  (`harness/test/contracts/go_architecture_debt.json:5-64`).

The model and schema also encode related invariants at different lifecycle
stages. For example, the `PeerInbox` model constructor permits some terminal
combinations that stricter SQLite triggers reject
(`harness/internal/model/inbox.go:19-177`,
`harness/internal/store/schema.sql:1986-2115`). This may be a legitimate
snapshot-versus-transition-history distinction and has not been shown to cause
a candidate-path failure, but the distinction is implicit and increases review
cost.

Two callback designs add unnecessary state combinations:

- signer callbacks execute inside Store transactions while the Store has one
  SQLite connection; today's signer is in-memory Ed25519, but the interface
  permits blocking/reentrant implementations;
- `ReconcileWithCommit` accepts an arbitrary callback while holding several
  mesh locks, yet every non-test caller currently passes a no-op; only tests
  exercise real callback behavior.

**Simpler design:**

- reserve exact durable replay receipts for external operations where the
  response can truly be lost;
- for each internal receipt, first prove that current authority plus request
  digest is sufficient to recover every crash outcome, and consolidate only
  those that pass that test;
- document snapshot validation versus durable transition-history validation
  where both are intentionally different;
- make signer behavior explicitly local/nonblocking or sign outside a
  transaction and commit under a digest fence;
- remove general callbacks that have no non-test use.

This should not hide transaction/fence checks behind a generic framework. The
goal is fewer authorities, not fewer explicit invariants.

### R5-019 — Isolation and lifecycle boundaries are only partial

Root release packages do not import Harness, which is good. But Harness and root
still share one Go module. R5 added libp2p and a large indirect dependency set
to root `go.mod`; `go test ./...`, `go mod tidy`, dependency review and CI now
couple the experimental layer to the release repository even when root runtime
does not load those packages.

The Store accepts only exact schema v1 and has no migration
(`harness/internal/store/store.go:25-28`,
`harness/internal/store/schema.go:71-105`). That matches the frozen T0 contract,
but a continuity product cannot preserve long-running work through its first
schema change. It is acceptable only while state is explicitly disposable.

Untrusted parser coverage is also thin. There is one Go fuzz target in the
Harness, while network frame parsers and recursive canonical JSON consume
untrusted bytes, for example:

- `harness/internal/peer/channel_frame.go:204-239`;
- `harness/internal/peer/event_frame.go:127-158`;
- `harness/internal/peer/artifact_frame.go:197-228`;
- `harness/internal/model/canonical.go:61-84,149-216`.

The engineering standard recommends fuzzing codec/parser boundaries
(`docs/development/go-engineering-standard.md:233-235`).

**Simpler improvements:**

- place Harness in its own `harness/go.mod` and use a workspace only for local
  development;
- clearly label schema-v1 state disposable until forward migrations exist;
- add small fuzz corpora at external frame/canonical JSON boundaries rather
  than more broad scenario machinery.

### R5-020 — Test layout and dual-Host scope add avoidable T0 cost

The contract requires every handwritten `foo.go` to map to exactly one
same-directory `foo_test.go`
(`.mnemon-dev/architecture/r5/requirements-and-gates.md:153-164`), enforced by
`harness/scripts/check_test_pairs.sh:56-83`. This likely encouraged artificial
production-file fragmentation while forcing large test files; 21 paired test
files already exceed 800 lines.

The runtime reference calls T0 Codex-only
(`.mnemon-dev/architecture/r5/runtime-reference-codex.md:481-492`), while ND-14
and G-SETUP require both Codex and Claude projection. Carrying both Hosts before
the real Codex acceptance path closes is scope expansion and reflects the
document-authority conflict in R5-009.

**Simpler T0:** organize tests at package/topic level with explicit ownership,
not a basename bijection, and defer Claude until one Codex vertical slice has
real evidence. This requires changing the frozen CUT-04/ND-14 contract; it
cannot be done honestly as a mere refactor.

## 6. Overengineering assessment

### 6.1 Where complexity is justified

Complexity is proportional to risk in these areas:

- signed Channel roster and origin binding;
- canonical Event/publication identity;
- operation key plus request digest;
- atomic Event/Work/Handling/publication commits;
- claim lease, owner fence and terminal receipt;
- content-addressed Artifact verification;
- bounded network frames, queues and worker ownership;
- durable Inbox separating delivery from Agent availability;
- independent task oracle and isolated Node state.

Removing these would make the implementation shorter by weakening the actual
continuity and authority guarantees.

### 6.2 Where complexity is not earning its cost

The clearest overengineering falls into four groups.

#### A. The T0 contract is too broad

One preview simultaneously attempts:

- Codex and Claude projection;
- seven semantic actions;
- six nodes and three overlapping Channels in every canonical case;
- Gossip direct, Gossip relay and origin Pull;
- detailed Artifact provenance plus accepted-object GC;
- 31 fault-matrix rows;
- five Live narrative scenarios and a rolling ten-run rate;
- exact test-layout and custom quality-ratchet policy.

Each feature is defensible alone. Their simultaneous inclusion prevents a
small, falsifiable product slice.

#### B. Internal reliability is modeled as if every boundary were remote

External CLI mutations need exact operation receipts because the client can
lose the response. For some internal worker transitions, current authority plus
request digest may already distinguish every crash outcome; those do not need
a separate receipt family. Other receipts carry a new fence or externally
observable provenance and should remain. The current design does not make this
distinction easy to audit.

#### C. Observability is carrying full history

Status reads complete publication evidence, and general network-path evidence
is asked to prove Artifact retention, provenance, DAG and cursor properties.
This makes the observation surface both too large and less truthful. Small
purpose-specific projections would be simpler.

#### D. The proof system is larger than its independent oracles

The E2E runner, five manifests, scripted Runtime and evidence schemas are
extensive, yet:

- a no-work `none/none` probe proves a crash race;
- Live needs scripted-only files;
- 103 pending requirements pass;
- performance fields are absent;
- provenance assertions lack provenance fields.

More evidence files do not improve confidence when the oracle is not bound to
the claimed event.

## 7. Simpler architecture options

### Option A — Keep the frozen R5 contract

This is the lowest product-risk path from the current branch. It does not
materially reduce feature scope, but it can reduce implementation and proof
complexity.

1. Split operational status from evidence export; use bounded aggregates and
   paginated diagnostics.
2. Make Channel mutation APIs response-loss-safe with the same narrow operation
   identity pattern already used for Teamwork.
3. Move network I/O and signing callbacks outside global locks/transactions;
   commit with explicit digest/fence CAS.
4. Remove or merge an internal receipt only after a crash-outcome matrix proves
   current authority plus request digest is sufficient; keep domain-specific
   fences and provenance explicit.
5. Remove accepted-object automatic GC from T0 while retaining staging cleanup.
6. Delete candidate self-test routes such as `replay-probe`.
7. Split the E2E runner by phase/fault and use declarative precondition/action/
   postcondition records.
8. Derive release status from evidence; do not hand-maintain `pending` versus
   `verified`.
9. Separate the Harness Go module from root.
10. Preserve cryptographic, fence, bound, CAS and independent-oracle checks.

This option can simplify code substantially, but GossipSub, origin Pull, two
Hosts, seven actions, overlapping Channels and the full Live matrix remain
because they are normative MUSTs.

### Option B — Reopen the contract around the product goal

If the goal is the smallest useful “collaboration continuity and independent
review” product, a smaller R5 could be:

1. one local daemon and SQLite single writer per workspace;
2. one signed Node identity and owner-signed Channel roster;
3. signed immutable Event/Work and a durable Inbox;
4. direct best-effort push to known audience members;
5. periodic/on-demand Pull repair from the origin;
6. content-addressed immutable Artifact transfer with no accepted-object
   automatic GC;
7. Codex-only managed Host projection;
8. the four actions needed for the first workflow: offer, review/rework,
   deliver and resolve;
9. one real two- or three-node payment-review vertical slice;
10. a focused process fault suite for commit loss, claim race, restart and
    Artifact interruption.

With at most eight known members, direct fan-out avoids GossipSub topics,
message-ID policy, validator/relay queues and full-content relay visibility.
Pull still provides deterministic repair. The tradeoff is that a receiver
which missed an origin publication must wait for the origin to return; cached
Channel relay availability is deferred.

Add overlapping Channels, team expansion, relay delivery, Claude and the full
five-case matrix only after the first Live slice proves user value and reveals
which availability properties are actually needed.

This option conflicts with the current frozen GossipSub, Host, action, Docker
and verification requirements. It must be a documented architecture decision,
not an implementation shortcut.

### Recommendation between the options

Do not choose based on sunk code volume. Choose based on authority:

- If the 103 clauses are the product, use Option A and accept a large R5.
- If the product brief is the product and the clauses are an implementation
  hypothesis, use Option B.

Frozen contracts and an experimental Harness can legitimately coexist. The
actual ambiguity is that the normative clauses are untracked, entry-point
status documents disagree, and supersession between briefs/contracts is not
explicit. That governance gap made it difficult to reduce scope when the T0
implementation expanded.

## 8. Recommended resolution order

No new feature work should be added before the following review decisions are
closed:

1. **Restore truthful control/evidence paths**
   - fix the 64-publication status design;
   - reject post-completion/no-op fault evidence;
   - separate Live from scripted-private evidence;
   - make the release gate require all MUST verified.
2. **Resolve concrete runtime risks**
   - response-loss identity for Channel mutations;
   - no network I/O under global Channel/mesh locks;
   - bounded shutdown;
   - terminal durable leave failure;
   - scoped reconciliation wakeups.
3. **Choose one normative authority**
   - track the canonical contract;
   - reconcile architecture status and Host scope;
   - decide Option A versus Option B.
4. **Repair evidence semantics**
   - purpose-specific Artifact receipts;
   - exact public DAG/cursor evidence;
   - Hook-turn transcript;
   - one 100-Event performance suite;
   - machine-readable 31-row fault coverage ledger mapping each row to
     unit/process/Docker evidence.
5. **Then simplify structure**
   - staging-only GC;
   - fewer internal receipt authorities;
   - remove no-op callbacks and candidate self-test probes;
   - separate Go module;
   - relax basename test policy if the contract is reopened;
   - reduce the quality debt baseline through responsibility-level splits.
6. **Only then claim completion**
   - all 101 MUST derived as verified;
   - P0/P1 findings closed or explicitly re-scoped;
   - CI protects the real merge gate;
   - one current-image Live vertical slice passes;
   - eventually the chosen Live/fault/performance release policy passes.

## 9. Final assessment

### Is the basic capability implemented?

**Mechanically, much of it is.** The repository contains coherent Node,
Channel, Event, Work, Artifact, durable Inbox, managed Runtime and mesh
implementations. Current Hermetic evidence demonstrates many happy-path
mechanics and isolation properties.

**As a proven product capability, not yet.** Long-lived status fails, several
fault claims are not grounded, Live evidence is absent and currently
inconsistent, and the requirement registry has zero verified entries.

### Is it overengineered?

**Yes.** The overengineering is primarily in the frozen T0 breadth, accepted
Artifact lifecycle, duplicated durable authorities and proof apparatus—not in
the existence of signatures, fences or durable state.

### Can R5 be materially simpler?

**Yes, but only with an explicit choice.** Keeping every current MUST allows
meaningful internal simplification but not a small system. A genuinely smaller
R5 requires reopening the network, Host, action and verification scope around a
single Live product slice.

### Does it interfere with the root path?

**Not at runtime import level.** Root release packages do not import Harness.
**Not fully at repository/tooling level.** One root Go module, expanded
dependencies, `go test ./...`, CI and large shared diffs couple the experimental
Harness to the main repository. A separate Harness module would make the
non-interference boundary structural rather than conventional.
