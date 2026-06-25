# GitHub Decentralized Mesh Implementation Plan

> 日期:2026-06-26
> 类型:实现计划 / 可转换为 `/goal` 的执行设计
> 架构依据:[github-remote-workspace-backend.md](github-remote-workspace-backend.md)
> 默认 live repo:`mnemon-dev/mnemon-teamwork-example`
> 状态:D1-D7 已讨论并同意,本文件按已同意边界重新收束

## 0. 已锁定决策

本计划不再把以下内容作为开放分歧处理:

```text
D1 GitHub direct first; no GitHub App in first implementation.
D2 Default topology is one shared repo + one branch per mnemond.
D3 Data model can express multiple publish targets, but MVP permits only one active publish target.
D4 Validation repo is evidence surface only, never mnemond governance input.
D5 Real GitHub adapter must be validated promptly; milestone cannot finish with fake store only.
D6 Branch identity uses mnemond_id, not hostagent display name.
D7 GitHub backend is repo-mediated publication, not P2P networking.
```

Implication:

```text
GitHub mesh means repo-mediated publication streams.
It does not mean mnemond-to-mnemond networking.
```

## 1. 目标与完成定义

实现一个可验证的 **GitHub-backed decentralized Remote Workspace foundation**:

```text
5 hostagents
5 mnemond
1 shared GitHub repo: mnemon-dev/mnemon-teamwork-example
5 per-mnemond publication branches
0 shared governed.db
0 central active mnemon-hub
```

核心证明:

```text
accepted synced event envelopes can propagate through repo-mediated publication streams;
each receiving mnemond validates and imports through Event Intake -> Tick -> Materializer;
teamwork can continue through assignment, nested decomposition, join, leave, reassignment, aggregation, and next act.
```

Foundation done means all of the following are true:

- Existing HTTP `mnemon-hub` sync behavior remains compatible.
- `RemoteEntry` supports backend and direction without breaking old remotes.
- GitHub concepts do not enter `runtime`, `state`, `materializer`, `presentation`, or `hostagent`.
- Every connected appserver has its own `mnemond`, own local store, and isolated runtime workspace.
- Pull-side diagnostics from invalid remote publication entries become durable local diagnostics.
- GitHub backend is implemented behind the `exchange.RemoteWorkspace` ABI.
- Fake-store unit tests cover deterministic publication behavior.
- A gated live GitHub case passes against `mnemon-dev/mnemon-teamwork-example`.
- Deterministic 5-mnemond local acceptance passes without a central active `mnemon-hub`.
- Real Codex appserver acceptance has a runnable script, evidence format, and natural task suite.
- Acceptance includes the 5-node teamwork case plus at least 2-3 natural user-task scenarios.
- Each real task starts from one or more connected PoC agents receiving ordinary user messages, not from a global orchestration prompt.
- At least one real task demonstrates multiple Teamwork-ReAct rounds: output review -> replanning -> reassignment -> execution -> aggregation -> next act.

### 1.1 Implementation checkpoint - 2026-06-26

Current branch progress:

- `exchange.RemoteWorkspace` seam exists and both HTTP and GitHub backends use it.
- `RemoteEntry` now carries `backend`, `direction`, `repo`, and `branch`; legacy HTTP defaults remain compatible.
- Directional `publish` / `subscribe` remote plans are implemented.
- Pull-side remote diagnostics are imported as durable governed diagnostics.
- `PublicationStore` seam and deterministic memory store exist.
- GitHub publication backend is fake-store tested.
- Real GitHub `PublicationStore` adapter exists.
- GitHub Remote Workspace normalizes opaque GitHub branch-head cursors before writing local mnemond pull state.
- Gated live publish/pull/import test exists and is skipped unless live credentials are provided.
- Gated live publish/pull/import passed against `mnemon-dev/mnemon-teamwork-example` on 2026-06-26 after initializing default publication branches `mnemon/agent-a` through `mnemon/agent-e`.
- Deterministic local GitHub mesh tests cover:
  - five isolated mnemond runtimes;
  - one branch per mnemond;
  - no central active `mnemon-hub`;
  - two later joining nodes;
  - one paused node;
  - active-node reassignment;
  - paused-node catch-up through publication branches.
- Real Codex appserver acceptance runner exists as:

```bash
mnemon-harness acceptance r1-github-mesh-task-suite \
  --agents 5 \
  --agent-turns \
  --github-repo mnemon-dev/mnemon-teamwork-example \
  --github-token-file <token-file> \
  --sync-interval 30s
```

When `--github-branch-prefix` is omitted, the runner must create run-scoped publication branches:

```text
mnemon/acceptance/<run-id>/agent-a
mnemon/acceptance/<run-id>/agent-b
...
```

Fixed branches such as `mnemon/agent-a` through `mnemon/agent-e` are valid for manual operator smoke tests, but not the default real appserver acceptance path because historical publication entries can pollute a fresh run.

Real runner lifecycle now treats join as delayed activation of preconfigured publication streams:

```text
configure 5 workspaces/remotes/branches up front
start 3 mnemond/appservers for the first natural task round
start the remaining 2 mnemond/appservers during the task
publish fresh joined agent_profile events
verify profiles converge through configured publication branches
pause/restart one already joined local mnemond during the task
```

This is intentionally not branch discovery, P2P node discovery, or dynamic networking. The branch list and `remotes.json` remain configured bootstrap inventory; the lifecycle evidence proves delayed availability and backlog import against those configured streams.

Known open evidence:

- The real Codex appserver GitHub mesh suite is runnable, but still requires a GitHub token file and a usable Codex appserver environment.
- Until the real appserver run passes, this goal is not closed.

## 2. Non-goals and invariants

Non-goals:

- Do not implement GitHub Issue/PR/Actions collaboration.
- Do not let GitHub become canonical governed state.
- Do not implement strong cross-trust-domain security in this milestone.
- Do not implement direct mnemond-to-mnemond transport.
- Do not implement P2P networking, gossip, DHT, peer routing, NAT traversal, or overlay network.
- Do not call branch enumeration a node discovery protocol.
- Do not claim multi-publish reliability before per-target sync ledger exists.

Hard invariants:

```text
Remote Workspace transports accepted synced envelopes.
Local mnemond alone imports through Event Intake -> Tick -> Materializer.
GitHub backend talks only to configured GitHub Remote Workspace.
Branch presence is publication inventory, not liveness or authority.
Validation reports are evidence, not runtime input.
```

Package boundary invariant:

```text
runtime/state/materializer/presentation/hostagent
  must not import GitHub backend packages.

GitHub code must stay below exchange/backend boundary.
```

## 3. Current baseline

Already implemented first cut:

```text
exchange.RemoteWorkspace interface
  SyncPush(contract.SyncPushRequest) (contract.SyncPushResponse, error)
  SyncPull(contract.SyncPullRequest) (contract.SyncPullResponse, error)
  SyncStatus() (contract.SyncStatusResponse, error)
```

Current HTTP `access.Client` can satisfy the interface. `remotes.json` has a `backend` field start:

```text
empty backend -> http
unknown backend -> fail closed
```

Known unrelated validation caveat:

```text
go test ./harness/cmd/mnemon-harness
```

may still hit the macOS `/var` vs `/private/var` acceptance run-root issue until fixed separately.

## 4. Target topology

Runtime topology:

```text
                 configured GitHub Remote Workspace
              repo: mnemon-dev/mnemon-teamwork-example
        +------------------------------------------------+
        | branch mnemon/<mnemond-a>  accepted log        |
        | branch mnemon/<mnemond-b>  accepted log        |
        | branch mnemon/<mnemond-c>  accepted log        |
        | branch mnemon/<mnemond-d>  accepted log        |
        | branch mnemon/<mnemond-e>  accepted log        |
        +------------------------------------------------+
             ^                 ^                 ^
             |                 |                 |
       push own stream   pull subscribed   pull subscribed
             |                 |                 |
        +----+----+       +----+----+       +----+----+
        | mnemond |       | mnemond |       | mnemond |
        | local   |       | local   |       | local   |
        | store   |       | store   |       | store   |
        +----+----+       +----+----+       +----+----+
             ^                 ^                 ^
             |                 |                 |
        hostagent         hostagent         hostagent
```

There is no:

```text
mnemond -> mnemond socket
gossip channel
routing table
overlay network
GitHub Issue/PR assignment queue
```

Data flow:

```text
hostagent emits event
  -> local mnemond accepts event
  -> local sync material marks accepted synced envelope
  -> GitHub backend publishes to own branch
  -> subscribed mnemond pull publication streams
  -> pull validates digest/scope/phase/idempotency
  -> valid envelopes enter local Event Intake
  -> invalid envelopes create durable diagnostics
  -> Tick -> Materializer derives local views/cues
```

## 5. Remote model contract

`RemoteEntry` target model:

```text
RemoteEntry
  id
  backend: http | github
  direction: bidirectional | publish | subscribe
  endpoint        # http only
  repo            # github only, e.g. mnemon-dev/mnemon-teamwork-example
  branch          # github only, e.g. mnemon/<mnemond-id>
  credential_ref
  ca_file         # http only
  scopes
```

Direction semantics:

```text
bidirectional -> push target + pull source
publish       -> push target only
subscribe     -> pull source only
```

Compatibility:

```text
empty backend   -> http
empty direction -> bidirectional
legacy HTTP remotes preserve current behavior
```

MVP restriction:

```text
At most one active publish target.
Multiple subscribe sources are allowed if cursor/import idempotency is proven.
```

Reason:the current sync ledger mostly tracks synced/pending/conflict globally. Multiple push targets need a per-target ledger before reliability can be claimed.

## 6. GitHub repo contract

Default repository:

```text
mnemon-dev/mnemon-teamwork-example
```

Branch namespace:

```text
mnemon/team
mnemon/<mnemond-id>
```

Recommended layout:

```text
mnemon/team
  .mnemon/team.json
  .mnemon/scenarios/<scenario>.json
  .mnemon/reports/<run-id>/summary.json

mnemon/<mnemond-id>
  mnemon-publications/v1/manifest.json
  mnemon-publications/v1/events/<origin_mnemond>/<resource_kind>/<resource_id>/<local_ingest_seq>-<local_decision_id>.json
```

Contract:

- `mnemon/team` stores bootstrap metadata and reports only.
- `mnemon/<mnemond-id>` stores accepted-event publication entries only.
- A `mnemond` writes only its own branch.
- A `mnemond` reads configured/subscribed publication branches.
- Branch enumeration is publication stream enumeration inside a configured Remote Workspace.
- Branch presence is not liveness, membership authority, permission authority, or scheduling input.
- Reports are never imported as governed events.
- The validation repo may be deleted/recreated without changing local governance semantics.

Team manifest shape:

```json
{
  "schema_version": 1,
  "team_id": "mnemon-teamwork-example",
  "members": [
    {
      "mnemond_id": "agent-a",
      "branch": "mnemon/agent-a",
      "principal": "codex-a@project"
    }
  ]
}
```

Interpretation:

```text
members = configured publication stream inventory
members != canonical team state
members != permission grant
members != online status
```

## 7. PublicationStore contract

The GitHub backend must be testable without real GitHub. Introduce a storage seam below `exchange.RemoteWorkspace`:

```go
type PublicationStore interface {
    PutEvent(ctx context.Context, branch string, path string, body []byte) (PutResult, error)
    ListEvents(ctx context.Context, branch string, prefix string, cursor string) (ListResult, error)
    ReadFile(ctx context.Context, branch string, path string) ([]byte, error)
    WriteFile(ctx context.Context, branch string, path string, body []byte) error
}

type PutResult struct {
    Created bool
    ExistsSame bool
    Conflict bool
}

type StoredEvent struct {
    Path string
    Body []byte
    Cursor string
}
```

Event path:

```text
mnemon-publications/v1/events/<origin_mnemond>/<resource_kind>/<resource_id>/<local_ingest_seq>-<local_decision_id>.json
```

`local_ingest_seq` is zero-padded to 12 digits. The path is intentionally visible and maintainable in GitHub, for example:

```text
mnemon-publications/v1/events/agent-a/progress_digest/project/000000000007-dec-a.json
```

Put semantics:

```text
same path + same body      -> idempotent success
same path + different body -> conflict diagnostic
new path                   -> created
unsupported branch/path    -> fail closed
```

List/cursor semantics for MVP:

```text
Fake store:
  deterministic ordered cursor.

GitHub store:
  cursor may encode last fully scanned branch head.
  if branch head is unchanged, return no new entries.
  if branch head changed, list current event tree and let local import idempotency skip duplicates.
```

This is intentionally conservative. It avoids claiming perfect append-only incremental listing before a per-branch/per-target ledger exists.

## 8. Phase dependency graph

```text
P0 guardrails
  -> P1 RemotePlan
  -> P2 diagnostics
  -> P3 PublicationStore
  -> P4 fake-tested GitHub backend
  -> P5 repo contract + operator config
  -> P6 real GitHub adapter + live case
  -> P7 deterministic 5-mnemond acceptance
  -> P8 real Codex appserver acceptance
  -> P9 docs + goal closure
```

P5 repo contract is listed before P6 because the live case must know exactly which repo, branches, paths, and reports are valid.

## 9. P0: Concept guardrails

Purpose:make concept boundaries executable.

Scope:

- Add/extend coreguard tests.
- Forbid GitHub backend imports from `runtime`, `state`, `materializer`, `presentation`, `hostagent`.
- Forbid GitHub Issue/PR/Action as teamwork semantic names.
- Forbid P2P discovery/gossip/routing/overlay terms in backend implementation names.
- Enforce `publication stream enumeration` terminology for branch listing.

Outputs:

- Coreguard test file or extension.
- Package allowlist for GitHub backend.
- Naming denylist for GitHub-native teamwork concepts.

Validation:

```text
go test ./harness/internal/coreguard
```

Done when:

- Core dependency graph contains no GitHub backend import.
- Failing examples catch at least one forbidden import/name case.

## 10. P1: Directional RemotePlan

Purpose:separate push targets from pull sources.

Scope:

- Add `direction` to `RemoteEntry`.
- Add `RemotePlan{PushTargets, PullSources}`.
- Preserve legacy HTTP behavior.
- Worker/manual sync use RemotePlan instead of single remote.
- Enforce MVP one active publish target.

Tests:

- legacy remote -> one push target and one pull source.
- `direction=publish` -> push only.
- `direction=subscribe` -> pull only.
- unknown direction fail-closed.
- multiple publish targets fail-closed or return explicit unsupported error.
- worker idle/no remote behavior unchanged.

Validation:

```text
go test ./harness/internal/mnemonhub/exchange ./harness/internal/app ./harness/cmd/mnemon-harness -run 'TestSync|TestRemote|TestLoadRemote'
go build -o /tmp/mnemon-harness-check ./harness/cmd/mnemon-harness
```

Done when:

- Existing HTTP connect/push/pull tests pass.
- Config output includes backend/direction without breaking old remotes.

## 11. P2: Pull diagnostics ingestion

Purpose:make pull-side rejection visible and durable.

Why now:GitHub direct has no server-side push clamp. Invalid entries must not silently disappear.

Scope:

- Consume `SyncPullResponse.Diagnostics`.
- Add `sync.remote_diagnostic.observed` or equivalent durable event.
- Ensure diagnostic import is idempotent.
- Keep HTTP no-diagnostics path unchanged.

Payload shape:

```json
{
  "remote_id": "...",
  "origin_mnemond": "...",
  "event_id": "...",
  "subject": "...",
  "status": "rejected|conflict|invalid",
  "diagnostic": "..."
}
```

Tests:

- fake remote returns diagnostics.
- local event log contains durable diagnostic.
- repeated pull does not duplicate diagnostic.
- valid event import remains unchanged.

Validation:

```text
go test ./harness/internal/app -run 'TestSync|TestDiagnostic'
```

Done when:

- Invalid remote publication entries produce visible local diagnostics.
- Diagnostics are not treated as accepted remote events.

## 12. P3: PublicationStore fake seam

Purpose:isolate GitHub storage mechanics from exchange semantics.

Scope:

- Add `PublicationStore` interface.
- Add deterministic in-memory fake store.
- Add path normalization and branch validation helpers.
- Add cursor behavior tests.

Tests:

- publication path deterministic and human-reviewable.
- idempotent put.
- conflict put.
- list after cursor.
- unsupported branch/path fail closed.
- same event body across repeated publish does not create conflicts.

Validation:

```text
go test ./harness/internal/mnemonhub/exchange -run 'TestPublicationStore|TestGitHubBackendFake'
```

Done when:

- Fake store can run the full publish/pull/import logic without network.
- No GitHub API package is needed for unit tests.

## 13. P4: GitHub backend over fake store

Purpose:implement `exchange.RemoteWorkspace` semantics before wiring real GitHub.

Remote config:

```text
backend: github
direction: publish|subscribe
repo: mnemon-dev/mnemon-teamwork-example
branch: mnemon/<mnemond-id>
credential_ref: ...
scope: ...
```

Push flow:

```text
SyncPush(req)
  for each synced envelope:
    require phase=synced
    materialize SyncedEventMaterial
    derive visible publication path
    write mnemon-publications/v1/events/<origin>/<kind>/<resource>/<seq>-<decision>.json
    created/exists-same -> accepted
    exists-different -> conflict diagnostic
```

Pull flow:

```text
SyncPull(req)
  list subscribed publication branch entries
  skip own origin
  validate envelope digest/phase/scope
  return valid Events
  return invalid/out-of-scope/conflict as Diagnostics
```

Tests:

- publish writes only synced envelopes.
- subscribe returns valid subscribed events.
- own origin excluded.
- invalid phase -> diagnostic.
- out-of-scope -> diagnostic.
- digest mismatch -> diagnostic.
- same key different body -> diagnostic.
- cursor unchanged -> no new events in fake store.

Validation:

```text
go test ./harness/internal/mnemonhub/exchange
go test ./harness/internal/app -run 'TestSync'
```

Done when:

- GitHub backend behavior is proven over fake store.
- Worker can use backend through `exchange.RemoteWorkspace`, not a GitHub-specific type.

## 14. P5: Repo contract and operator config

Purpose:freeze the real repo shape before the live GitHub adapter.

Scope:

- Document `mnemon-dev/mnemon-teamwork-example` as the default live repo.
- Define branch names for the live case.
- Define `team.json` and report output.
- Add CLI/config examples.
- Add validation that repo/branch/path shapes are accepted or rejected deterministically.

Live branch set:

```text
mnemon/team
mnemon/agent-a
mnemon/agent-b
mnemon/agent-c
mnemon/agent-d
mnemon/agent-e
```

Operator UX:

```bash
mnemon-harness sync connect self \
  --backend github \
  --direction publish \
  --github-repo mnemon-dev/mnemon-teamwork-example \
  --github-branch mnemon/agent-a \
  --token-file ...

mnemon-harness sync connect agent-b-stream \
  --backend github \
  --direction subscribe \
  --github-repo mnemon-dev/mnemon-teamwork-example \
  --github-branch mnemon/agent-b \
  --token-file ...
```

Done when:

- A new operator can identify the repo, branches, token file, and expected report path.
- Validation repo is clearly marked evidence surface only.

## 15. P6: Real GitHub adapter and live case

Purpose:prove the backend works on real GitHub, not just fake storage.

Official docs anchors to verify at implementation time:

- Repository Contents API: https://docs.github.com/en/rest/repos/contents
- Git Trees API: https://docs.github.com/en/rest/git/trees
- Git References API: https://docs.github.com/en/rest/git/refs
- Fine-grained token permissions: https://docs.github.com/en/rest/authentication/permissions-required-for-fine-grained-personal-access-tokens
- REST API rate limits: https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api
- REST API best practices: https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api

Plan:

```text
P6A docs verification and API spike
P6B GitHub PublicationStore implementation
P6C gated live publish/pull/import case
```

P6A must verify:

- create/update file behavior and conflict status.
- branch reference behavior and fast-forward constraints if Git Database API is used.
- required fine-grained token permissions for contents read/write.
- rate-limit headers and secondary rate-limit behavior.
- serial mutative request policy.

Default API strategy:

```text
Use Contents API first for MVP PutEvent if one-file-per-event commits are acceptable.
Use Git Trees/Commits/Refs if batching or branch-head control becomes necessary.
Use Git Trees or equivalent tree listing for event discovery when directory listing limits matter.
```

The implementation must not hard-code an API choice that prevents later switching behind `PublicationStore`.

Security:

- Token comes from `credential_ref` or token file.
- Token never appears in logs, diagnostics, reports, or test failure output.
- Use authenticated requests.
- Use explicit timeout.
- Serialize mutative requests per branch.
- Treat `403`, `404`, `409`, `422`, and rate-limit responses as structured errors.

Gated live case:

```text
repo: mnemon-dev/mnemon-teamwork-example
publish branch: mnemon/agent-a
subscribe branch: mnemon/agent-b

agent-a:
  local accepted synced envelope
  -> publish mnemon-publications/v1/events/agent-a/progress_digest/project/000000000001-dec-a.json

agent-b:
  pull mnemon/agent-a
  -> validate envelope
  -> import through Event Intake path
  -> persist cursor/status

repeat pull:
  -> no duplicate import
```

Suggested gated command shape:

```bash
MNEMON_GITHUB_LIVE=1 \
MNEMON_GITHUB_REPO=mnemon-dev/mnemon-teamwork-example \
MNEMON_GITHUB_TOKEN_FILE=/path/to/token \
MNEMON_GITHUB_BRANCH_A=mnemon/agent-a \
MNEMON_GITHUB_BRANCH_B=mnemon/agent-b \
go test ./harness/internal/app -run TestGitHubLivePublishPullImport -count=1 -v
```

Default unit tests must skip this when `MNEMON_GITHUB_LIVE` is not set.

Done when:

- Fake unit tests pass.
- Gated live case has passed at least once against `mnemon-dev/mnemon-teamwork-example`.
- Live case evidence records repo, branches, commit refs or publication cursors, and import result.

## 16. P7: Deterministic local 5-mnemond acceptance

Purpose:prove mesh semantics without real Codex appservers.

Topology:

```text
5 local stores
5 mnemond runtime instances
1 fake or real publication store
5 publication branches
0 central active mnemon-hub
0 shared governed.db
```

Scenario:

1. Agent A publishes `teamwork_signal` and assignments.
2. B/C/D/E subscribe/import.
3. B emits nested assignment.
4. Two new `mnemond` instances join.
5. One `mnemond` stops publishing progress/profile.
6. TTL-derived cue leads another actor to emit reassignment.
7. Progress digests aggregate.
8. Another act is emitted after aggregation.
9. Final completion evidence is emitted.

Assertions:

- no shared governed.db.
- no central active mnemon-hub endpoint.
- every cross-mnemond event came from a publication branch.
- imported resources exist only after Event Intake -> Tick -> Materializer.
- injected bad entries produce diagnostics.
- repeated pulls are idempotent.

Done when:

- Deterministic acceptance script/test passes locally.
- Report contains event chain, branch/cursor evidence, diagnostics, and no-hub/no-shared-db proof.

Current implemented deterministic coverage:

```bash
go test ./harness/internal/app -run 'TestSyncGitHubFake' -count=1 -v
```

It covers five isolated mnemond runtimes, one publication branch per mnemond, fake GitHub publication store, later join by two nodes, paused node, active-node reassignment event, and paused-node catch-up. It does not by itself prove real Codex appserver natural planning; that remains P8.

## 17. P8: Real Codex appserver acceptance

Purpose:prove the workflow with real hostagent behavior.

Topology:

```text
5 codex appservers
5 mnemond
5 isolated runtime workspaces
5 isolated local mnemond stores
1 shared GitHub repo: mnemon-dev/mnemon-teamwork-example
5 run-scoped publication branches
0 central active mnemon-hub
0 shared governed.db
```

Isolation requirements:

- Each appserver connects to exactly one dedicated `mnemond`.
- Each `mnemond` has its own local store path.
- Each appserver has its own runtime workspace path.
- No appserver reads or writes another appserver's local Mnemon workspace.
- Cross-agent visibility happens only after publication branch pull/import.
- The report must include local store paths and prove they are distinct.
- The default branch prefix is run-scoped; the runner initializes missing branches from `main` before local sync starts.
- Reusing long-lived branches is allowed only when explicitly requested with `--github-branch-prefix`.
- The default real GitHub sync interval is 30 seconds per local `mnemond`; fake/local tests may use shorter intervals, but real GitHub acceptance must not use 100ms polling.

Baseline 5-node Teamwork-ReAct scenario:

1. Configure 5 isolated appserver/mnemond workspaces, remotes, and run-scoped publication branches.
2. Start the initial online subset of appservers/mnemond; the current runner uses 3 online and 2 delayed nodes for the natural task round.
3. Install generic hook/GUIDE/skill for started appservers.
4. Publish fresh `agent_profile` from the initial online nodes.
5. Send an ordinary user message to one connected PoC agent to start the teamwork task.
6. Verify assignment propagation.
7. Verify nested decomposition.
8. Verify first round outputs are published as progress digests.
9. Verify a PoC-like agent reviews outputs and emits a second-round plan.
10. Verify second-round reassignment or refinement is published and executed.
11. Start 2 delayed `mnemond`/appserver pairs mid-run from the already configured `remotes.json`.
12. Verify delayed nodes import backlog and publish fresh `agent_profile` events.
13. Stop/restart 1 `mnemond` mid-run.
14. Verify reassignment or renewed progress through governed events rather than direct scheduling.
15. Verify aggregation.
16. Verify another act can be emitted after aggregation if the result is incomplete.
17. Verify final completion evidence.

Natural task suite:

- Run the baseline 5-node case.
- Run at least 2 of the natural task scenarios in section 18.
- Prefer all 3 scenarios before declaring the milestone robust.
- At least one scenario must use multiple PoC agents.
- At least one scenario must create overlapping tasks where progress on task B can complete or materially advance task A.
- At least one scenario must verify profile update and profile freshness during the run.
- At least one scenario must show multiple output-driven replanning/reassignment rounds.
- Each scenario must be team-shaped:success should require useful work from more than one agent, not just one agent doing everything while others observe.

Report evidence:

```text
run_id
participants
entry_poc_agents
natural_user_messages
runtime_workspace_paths
local_mnemond_store_paths
publication branches
events published per branch
events imported per mnemond
diagnostics per mnemond
assignment/progress chain
replanning_rounds
reassignment_rounds
mnemond join/leave timeline
profile refresh/update timeline
cross_task_reuse_or_completion evidence
proof no central mnemon-hub endpoint was used
proof no shared governed.db was used
proof each appserver used its own mnemond/local store/runtime workspace
```

The acceptance report exposes the publication/import summary as machine-readable fields under `sync`:

```text
sync.published_events_by_branch
sync.imported_events_by_mnemond
sync.diagnostics_by_mnemond
sync.profile_events_by_mnemond
sync.lifecycle[].action = delayed_join_start | delayed_join_ready | pause_local_mnemond | restart_local_mnemond
sync.lifecycle[].ledger = local ledger counts captured at lifecycle boundaries
```

Done when:

- Script is runnable.
- A successful run produces the report above.
- Each task was initiated from connected PoC agent conversation, not global harness prompt.
- At least one successful scenario contains two or more output-driven replanning rounds.
- At least one successful scenario demonstrates isolated per-agent `mnemond` and local store paths.
- Failure mode leaves enough diagnostics to distinguish API/auth/config errors from Mnemon import/admission errors.
- GitHub API rate-limit failures must preserve retry/rate-limit headers when GitHub returns them, so an operator can distinguish temporary cooldown from auth or repo misconfiguration.

## 18. Natural acceptance task suite

Purpose:test whether teamwork behaves like normal agent usage, not like a scripted benchmark.

Harness rules:

- The harness may start/stop appservers, start/stop `mnemond`, configure remotes, and collect evidence.
- The harness may send ordinary user messages to one or more chosen PoC agents.
- The harness must not send a global prompt describing the expected internal delegation graph.
- The harness must not directly tell agents which member should receive which assignment, except where a normal user would reasonably mention a person/role.
- The harness must not inject hidden "expected answer" scaffolding into agent contexts.
- The harness observes events, profiles, diagnostics, assignments, and progress digests after the fact.

Natural prompt shape:

```text
User -> connected PoC agent:
  "Can you help me get X done? Pull in help if useful."
```

Avoid prompt shape:

```text
Global harness -> all agents:
  "Agent A must create assignments for B/C/D, then B must split work, then C must..."
```

Required observation dimensions:

- PoC initiation:which connected agent received the user message and emitted the first teamwork signal.
- Isolation:each connected agent uses its own `mnemond`, local store, and runtime workspace.
- Profile freshness:agents publish fresh `agent_profile` before and during work.
- Profile adaptation:agents update profile/posture when availability, recent success, specialization, or load changes.
- Multi-PoC behavior:two PoC agents can independently start work without corrupting each other's streams.
- Multi-task behavior:an agent can hold multiple tasks and make useful progress without losing context.
- Cross-task reuse:work done for task B can complete, unblock, or materially advance task A.
- Teamwork-ReAct loop:agents use round outputs to replan, reassign, execute again, and aggregate again.
- Team-shaped work:the final result contains meaningful contributions from multiple agents.
- Natural escalation:agents ask the user only when ambiguity/risk warrants it.
- No forced choreography:assignments and decomposition emerge from agent reasoning and governed events.

### Scenario A: Repository onboarding synthesis

Entry:

```text
PoC agent-a receives:
"帮我快速理解这个仓库现在的 GitHub Remote Workspace 改造方向,整理一份新成员能读懂的上手说明。你可以让其他成员帮忙核对架构、测试和风险。如果第一轮信息不够,请根据大家的反馈再拆一轮补齐。"
```

Expected natural behavior:

- `agent-a` acts as PoC and emits a teamwork signal.
- Other agents may inspect architecture docs, current harness sync code, tests, and risk areas.
- At least one agent publishes a profile/posture update showing documentation/review availability or recent context.
- First-round output reveals gaps; PoC emits a second-round refinement or fact-check assignment.
- Aggregation produces a concise onboarding artifact or report.

What this tests:

- Single-PoC kickoff.
- Natural decomposition without explicit worker list.
- Profile freshness and role fit.
- Aggregation from multiple publication branches.
- Output-driven replanning after first-round findings.

### Scenario B: Sync issue plus opportunistic docs/test completion

Entry:

```text
PoC agent-b receives:
"同步这块我担心还有隐藏问题。你帮我检查一下 GitHub Remote Workspace 相关的配置、诊断和测试设计;如果发现顺手能补的文档或测试缺口,一起处理。第一轮先找风险,再根据结果安排第二轮验证。"
```

Optional earlier/open task:

```text
PoC agent-a previously received:
"你先帮我整理一个这次 GitHub mesh 改造的文档和测试 readiness checklist,后面我们实现完要靠它验收。"
```

Expected natural behavior:

- `agent-b` starts from implementation/test concern, not from a scripted assignment plan.
- First-round risk findings drive a second-round verification or docs/test patch assignment.
- While checking sync diagnostics or RemotePlan tests, an agent may produce evidence that also satisfies the earlier docs/test readiness task.
- The system records that progress on task B completed or materially advanced task A.
- Agents update profile/posture if they become loaded, blocked, or demonstrate sync/testing expertise.

What this tests:

- Multiple active tasks in continuous context.
- Cross-task reuse and opportunistic completion.
- Multi-round review -> replan -> verify loop.
- Diagnostics visibility.
- Avoiding duplicate work when two tasks overlap.

### Scenario C: Multi-PoC live-readiness and operator safety

Entry:

```text
PoC agent-a receives:
"请你推进一次 GitHub live case 的准备,目标是能在 mnemon-dev/mnemon-teamwork-example 上证明 publish/pull/import 成立。先让大家找出缺口,再根据第一轮结果安排第二轮补齐。"

PoC agent-c, in a separate normal conversation, receives:
"我主要担心这个 GitHub 方案的操作者安全和失败诊断。你帮我从 token、repo、branch、报错可读性这几个角度检查一下。"
```

Expected natural behavior:

- Two PoC agents independently emit teamwork signals.
- The team converges on compatible work instead of creating conflicting branch/repo assumptions.
- Security/operator findings may update the live-case runbook.
- Live-case preparation may satisfy part of the operator safety task.
- First-round live-readiness/security output leads to second-round fixes or validation assignments.
- Profiles reflect current availability and specialization, for example API/live-case, docs, security, or test evidence.

What this tests:

- Multi-PoC initiation.
- Concurrent teamwork streams over the same repo-mediated workspace.
- Conflict avoidance and aggregation.
- Natural task overlap and reuse.
- Multi-PoC multi-round replanning.

## 19. P9: Documentation, UX, and goal closure

Scope:

- Update harness README for GitHub-backed decentralized Remote Workspace.
- Document repo-mediated publication mesh, not P2P networking.
- Document branch enumeration as publication stream enumeration.
- Document honest-client/single-trust-domain boundary.
- Document live GitHub test prerequisites and skip behavior.
- Document cleanup/reset behavior for `mnemon-dev/mnemon-teamwork-example`.
- Document the natural acceptance task suite and PoC initiation rule.
- Summarize validation commands and known caveats.

Done when:

- Operator path is reproducible from docs.
- Candidate goal can be closed with unit, live-case, deterministic, and natural appserver acceptance evidence.

## 20. Validation matrix

Minimum per phase:

```text
P0: go test ./harness/internal/coreguard
P1: go test ./harness/internal/mnemonhub/exchange ./harness/internal/app ./harness/cmd/mnemon-harness -run 'TestSync|TestRemote|TestLoadRemote'
P2: go test ./harness/internal/app -run 'TestSync|TestDiagnostic'
P3: go test ./harness/internal/mnemonhub/exchange -run 'TestPublicationStore'
P4: go test ./harness/internal/mnemonhub/exchange ./harness/internal/app -run 'TestGitHub|TestSync'
P5: config/repo contract tests and CLI help tests
P6: fake unit tests by default + gated live GitHub publish/pull/import case
P7: deterministic local 5-mnemond acceptance
P8: real Codex appserver acceptance + natural task suite
P9: docs/UX review against actual commands
```

Always run after code changes:

```text
gofmt
go build -o /tmp/mnemon-harness-check ./harness/cmd/mnemon-harness
```

When changing harness module assets:

```text
make harness-validate
```

Full E2E remains:

```text
bash scripts/e2e_test.sh
make test
```

## 21. Risk ledger

| Risk | Mitigation |
| --- | --- |
| GitHub concepts leak into Mnemon governance | P0 guardrails and naming denylist |
| Branch enumeration becomes node discovery | D7 invariant and publication stream terminology |
| Fake-store proof hides GitHub API issues | P6 gated live case is required for completion |
| Contents API one-commit-per-event becomes too slow | Keep `PublicationStore` API-independent; switch to Git Trees/Commits/Refs behind seam |
| Directory listing/cursor is incomplete at scale | Conservative branch-head cursor + idempotent import; later per-branch ledger |
| Multiple publish targets overclaim reliability | MVP one active publish target |
| Token leakage in diagnostics | explicit redaction tests and no-token reports |
| GitHub rate/secondary limits | authenticated requests, serialized mutative writes, backoff/error handling |
| Validation repo becomes control plane | D4 invariant and report-only contract |
| Acceptance is over-scripted and proves only the harness | PoC-only natural user entry rule; harness observes instead of choreographing |
| Appservers accidentally share local governance state | per-agent mnemond/store/runtime workspace isolation proof required |
| Agents fail to refresh useful profiles | profile freshness/update assertions in P8 reports |
| Multiple tasks duplicate work or miss cross-task completion | cross-task reuse/completion evidence required in natural task suite |
| Teamwork stops after one round and does not prove protocol value | multi-round Teamwork-ReAct evidence required: output review -> replan -> reassign -> execute -> aggregate |

## 22. Candidate `/goal`

```text
Implement the GitHub-backed decentralized Remote Workspace foundation for mnemon harness.

Scope:
- Preserve the existing HTTP mnemon-hub behavior.
- Add directional RemotePlan with publish/subscribe/bidirectional semantics.
- Add pull diagnostics ingestion so invalid remote publication entries become durable local diagnostics.
- Add a fake-tested GitHub publication backend implementing exchange.RemoteWorkspace over a PublicationStore seam.
- Add repo/branch contract for mnemon-dev/mnemon-teamwork-example.
- Add a real GitHub adapter and gated live GitHub publish/pull/import validation case.
- Add deterministic local 5-mnemond mesh acceptance.
- Add real Codex appserver acceptance harness/report shape and natural task suite.
- Ensure each connected agent has its own mnemond, local store, and isolated runtime workspace in acceptance.
- Do not let GitHub concepts enter runtime/state/materializer/hostagent/presentation.
- Do not use GitHub Issues/PR/Actions as teamwork semantics.
- Do not implement P2P networking; GitHub mesh means repo-mediated publication streams only.

Validation:
- Relevant go tests for exchange/app/cmd sync paths.
- go build ./harness/cmd/mnemon-harness.
- Gated live GitHub publish/pull/import case against mnemon-dev/mnemon-teamwork-example when credentials are provided.
- Deterministic local decentralized mesh acceptance.
- Real Codex appserver acceptance script/report is runnable.
- Natural PoC-initiated task suite covers baseline 5-node case, per-agent mnemond/store/workspace isolation, multi-PoC kickoff, profile updates, cross-task reuse/completion, and multi-round Teamwork-ReAct replanning/reassignment.
```
