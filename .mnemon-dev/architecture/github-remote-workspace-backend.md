# GitHub Remote Workspace Backend:去中心化 publication mesh 架构

> 日期:2026-06-26
> 类型:详细架构文档 / Remote Workspace backend 方向
> 第一性参照:[positioning-thesis.md](positioning-thesis.md)、[event-substrate-concept-normalization.md](event-substrate-concept-normalization.md)、[multi-machine-and-hub-redesign.md](multi-machine-and-hub-redesign.md)、[r1-production-like-per-hostagent-mnemond-experiment.md](r1-production-like-per-hostagent-mnemond-experiment.md)

## 0. 结论

GitHub 用在这里是合理的,但它合理的原因不是"GitHub 能存文件",而是它可以作为 **GitHub-backed decentralized Remote Workspace** 的 bootstrap backend。

准确表述:

```text
Remote Workspace is a protocol role.
mnemon-hub is the first-party HTTP implementation.
GitHub is a bootstrap backend for decentralized publication mesh.
```

GitHub backend 不替代 `mnemonhub` 这个协议角色,也不替代本地 `mnemond` 的治理语义。它只替代/补充 `mnemon-hub` 这个后端实现形态,用于让用户无需部署中心 hub 也能启动多 `mnemond` teamwork。

核心形态:

```text
shared GitHub team repo
  branch mnemon/<mnemond-a>  <- only mnemond-a writes
  branch mnemon/<mnemond-b>  <- only mnemond-b writes
  branch mnemon/<mnemond-c>  <- only mnemond-c writes

each mnemond publishes its own accepted-event log;
other mnemond subscribe to those branches and validate locally.
```

这不是 P2P 网络通信,因为所有 `mnemond` 仍只连接已配置的 GitHub Remote Workspace;但它是 **repo-mediated decentralized publication topology**:没有一个全局 active `mnemon-hub` 服务拥有整个 teamwork exchange。

### 0.1 当前实现状态 - 2026-06-26

当前代码已经落地的边界:

- GitHub backend 只存在于 `harness/internal/mnemonhub/exchange/backend/github`。
- `runtime` / `state` / `materializer` / `presentation` / `hostagent` 不依赖 GitHub backend。
- 每个 `mnemond` 通过 `remotes.json` 配置一个 `publish` branch 和多个 `subscribe` branches。
- GitHub Contents/Refs API 的 branch head cursor 不直接写入本地 mnemond cursor;backend 会把 opaque cursor 收束为本地可持久化数字 cursor,重复列表依赖 Event Intake 幂等性去重。
- acceptance report 会显式输出:
  - `transport_model=repo-mediated-publication`
  - `roster_source=configured-remotes-json`
  - `network_discovery=none`
  - per-workspace `remotes.json`
  - per-agent publication branch
  - per-agent local mnemond store path

当前仍需外部证据的边界:

- gated live GitHub publish/pull/import 已在 2026-06-26 通过真实 GitHub 访问验证;验证仓库缺失默认 publication branches 时先失败,随后初始化 `mnemon/agent-a` 到 `mnemon/agent-e` 后通过。
- real Codex appserver GitHub mesh suite 需要 token + Codex appserver 环境后实际运行。
- 在 real Codex appserver GitHub mesh suite 通过前,不能把 GitHub mesh 目标标记为 complete。

## 1. 非目标与红线

GitHub backend 必须服务 Mnemon 的事件治理模型,不能反过来把 Mnemon 拉回 GitHub 协作模型。

非目标:

- 不用 GitHub Issues / PR / Projects 表达 `assignment`、`progress_digest` 或 teamwork 状态。
- 不用 GitHub Actions 作为 scheduler、runtime loop 或治理执行器。
- 不让 GitHub backend 直接写 governed resources。
- 不让 GitHub backend 影响 render/cue/presentation。
- 不引入 agent-to-agent message inbox。
- 不把 branch/commit ordering 当成治理 ordering。
- 不实现 P2P 网络通信、gossip、DHT、peer routing、NAT traversal 或 overlay network。
- 不把 branch 枚举实现成独立的 P2P 节点发现协议。

硬红线:

```text
remote backend may transport accepted synced envelopes;
only local mnemond may import them through Event Intake -> Tick -> Materializer.
github backend may talk to configured GitHub Remote Workspace only;
it must not open mnemond-to-mnemond network links.
```

## 1.5 概念收束与防污染规则

这条路线最大的风险不是 GitHub API 复杂,而是概念污染:把 GitHub repo/branch/Issue/PR 误提升为 Mnemon 的协作主语,从而偏离 `hostagent / mnemond / mnemonhub / event` 主轴。

因此后续文档、代码、CLI、测试命名必须遵守以下收束规则。

### 1.5.1 系统主语

允许作为系统主语的概念:

```text
hostagent
mnemond
mnemonhub
event / event envelope
Remote Workspace
publication backend
RemotePlan
```

其中:

- `mnemond` 是去中心运行单元。
- `event envelope` 是跨节点交换材料。
- `Remote Workspace` 是协议角色。
- `publication backend` 是 Remote Workspace 的传输/存储实现。
- `GitHub` 只是一个 publication backend。

不允许把以下对象提升为治理主语:

```text
GitHub repo
GitHub branch
GitHub issue
GitHub PR
GitHub Action
validation repo
team repo
```

这些对象只能作为 substrate / transport namespace / evidence surface。

### 1.5.2 命名约束

推荐命名:

```text
GitHub-backed publication mesh
repo-mediated publication mesh
publication branch
publication stream enumeration
team rendezvous repo
validation repo
RemotePlan publish/subscribe
```

避免命名:

```text
GitHub team state
GitHub assignment
GitHub scheduler
GitHub agent inbox
GitHub source of truth
branch state
PR-based teamwork
issue-based assignment
P2P discovery protocol
mesh networking layer
peer routing
```

原因:这些命名会暗示 GitHub 承担 governed state 或 teamwork 语义,与 Mnemon 架构相冲突。

### 1.5.3 语义边界

GitHub repo/branch 的语义是:

```text
append-only publication substrate
transport cursor/provenance
team rendezvous and bootstrap
publication stream enumeration
acceptance evidence storage
```

GitHub repo/branch 的语义不是:

```text
canonical governed state
assignment database
scheduler queue
agent inbox
truth source for availability
arbiter of completion
network peer discovery authority
node routing table
```

Canonical state 只在本地 `mnemond` store 中产生;跨节点可见性来自 accepted synced envelopes 被订阅方 `mnemond` 本地导入后的结果。

### 1.5.4 验证仓库边界

如果使用一个专门仓库做 teamwork 验证,该仓库的角色应定义为:

```text
validation repo = GitHub-backed publication substrate + run evidence surface
```

它可以包含:

```text
team manifest
per-mnemond publication branches
scenario fixtures
run reports
acceptance evidence
```

它不能包含或承担:

```text
canonical governed.db
central scheduler
authoritative assignment state
agent-to-agent messages
GitHub Issue/PR workflow as teamwork semantics
```

验证通过的证据应证明:

```text
5 appservers
5 mnemond stores
0 shared governed.db
0 central active mnemon-hub
accepted events propagate through publication branches
every imported event enters through local Event Intake -> Tick -> Materializer
```

### 1.5.5 网络通信边界

`decentralized`、`mesh`、`P2P-like` 这些词只允许描述 publication ownership / event propagation topology,不能描述 GitHub backend 的网络实现。

GitHub backend 的通信模型是:

```text
mnemond -> configured GitHub Remote Workspace
```

不允许引入:

```text
mnemond -> mnemond direct socket
gossip
DHT
peer routing
NAT traversal
overlay network
dynamic P2P topology maintenance
```

因此 branch 枚举的准确含义是:

```text
publication branch enumeration
= list publication streams inside an already configured Remote Workspace
!= P2P node discovery
!= team membership authority
!= liveness authority
!= permission authority
```

branch 枚举可以提供 roster bootstrap 的输入材料,但是否接受某个 stream、是否把它视为可用 agent、是否允许其 event 进入本地治理,都必须由本地 `mnemond` 的 admission/import policy 决定。

## 2. 术语裁决:decentralized / federated / P2P

推荐术语:

```text
Decentralized Remote Workspace
GitHub-backed publication mesh
Repo-mediated publication mesh
Mnemond-published accepted-event logs
Federated mnemond sync
Publication stream enumeration
```

术语边界:

- **decentralized**:适合。没有单个中心 `mnemon-hub` 进程承载所有交换;每个 `mnemond` 拥有自己的 publication stream。
- **federated**:适合。每个 `mnemond`/信任域保留本地治理,通过订阅授权接收其他 publication streams 中的 accepted events。
- **P2P**:只可作为拓扑类比,不可作为网络实现承诺。当前 GitHub backend 不是 peer-to-peer socket transport,而是 **repo-mediated publication topology**。除非另起独立设计实现 direct mnemond-to-mnemond transport,否则不应把 GitHub backend 命名为 P2P networking。

## 3. 当前中心 hub 流程

当前 production-like 目标是:

```text
5 hostagents + 5 mnemond + 1 mnemonhub + 0 shared governed.db
```

中心 hub 流程:

```text
+-------------+        +-------------+        +-------------+
| hostagent A | -----> | mnemond A   | -----> | mnemon-hub |
+-------------+ event  +-------------+ sync   +-------------+
                                                  |
                                                  | pull
                                                  v
                                           +-------------+
                                           | mnemond B   |
                                           +-------------+
                                                  |
                                                  v
                                           +-------------+
                                           | hostagent B |
                                           +-------------+
```

数据流:

```text
agent-a emits teamwork_signal / assignment
  -> mnemond-a accepts event
  -> mnemond-a records pending synced envelope
  -> sync worker pushes to mnemon-hub
  -> mnemon-hub validates grant/scope/digest/idempotency
  -> mnemond-b/c/d/e pull from mnemon-hub
  -> each imports through Event Intake -> Tick -> Materializer
  -> render/skill surfaces cues
  -> assigned agents act and emit progress/assignment/signal
```

中心 hub 特性:

- 一个公共 exchange point。
- push 前 server-side validation。
- hub-side cursor/status。
- 清晰但需要部署/运行 hub。

## 3.5 诚实定位:GitHub 是 bootstrap backend,不是最终去中心 substrate

如果目标是"真正去中心运行",GitHub 不是最优雅的最终核心。GitHub mesh 的结构是:

```text
mnemond is decentralized as publisher;
GitHub remains centralized rendezvous/storage substrate.
```

真正去中心的核心抽象应是:

```text
mnemond publishes an append-only accepted-event feed;
other mnemond subscribe to that feed;
all imports still go through local governance.
```

因此最稳的架构表达是:

```text
Mnemon's decentralized unit is mnemond, not GitHub.
GitHub is a convenient publication substrate.
```

当前 GitHub backend 的通信边界:

```text
+-------------+                           +-------------+
| mnemond A   |                           | mnemond B   |
| local state |                           | local state |
+------+------+                           +------+------+
       |                                         |
       | push/pull own and subscribed streams    | push/pull own and subscribed streams
       v                                         v
       +------------- configured GitHub Remote Workspace -------------+
       | shared repo                                                   |
       | branch mnemon/a                                               |
       | branch mnemon/b                                               |
       +---------------------------------------------------------------+
```

Publication backend 应保持可插拔:

```text
publication backend:
  github branch
  static HTTPS directory
  object storage
  git remote
  future mnemond-native publication endpoint
```

direct P2P/libp2p-like transport 属于另一个未来研究方向,不是 GitHub backend 的实现内容,也不是当前 goal 的网络层假设。

GitHub 的价值是现实工程上的 bootstrap:

- repo 权限和 token 心智现成;
- storage/history/audit trail 现成;
- branch namespace 可表达 per-mnemond publication stream;
- 不需要用户部署中心 `mnemon-hub`;
- 离线节点的历史 feed 可由 GitHub 保留;
- 很适合证明 `5 appserver + 5 mnemond + 0 central active hub` 可跑通。

但 GitHub 不应变成架构中心:

- 不应让 GitHub API 渗入 `runtime` / `state` / `presentation` / `hostagent`;
- 不应把 GitHub branch/commit ordering 当作治理 ordering;
- 不应把 GitHub Issue/PR/Actions 当作 teamwork 语义层;
- 不应把 "GitHub-backed mesh" 宣传成纯 P2P。

实现策略:

```text
architecture core:
  decentralized publication mesh

first backend:
  GitHub-backed publication branch

future backend:
  mnemond-native publication endpoint
```

这保证第一版可以借 GitHub 快速启动,但最终形态仍然是 `mnemond-native publication mesh`。

## 4. GitHub Mesh 目标流程

GitHub mesh 改为 shared repo + per-agent branch:

```text
repo: mnemon-dev/mnemon-teamwork-example

branches:
  mnemon/agent-a
  mnemon/agent-b
  mnemon/agent-c
  mnemon/agent-d
  mnemon/agent-e
```

拓扑:

```text
                 shared GitHub repo
        +----------------------------------+
        | branch mnemon/agent-a            |
        | branch mnemon/agent-b            |
        | branch mnemon/agent-c            |
        | branch mnemon/agent-d            |
        | branch mnemon/agent-e            |
        +----------------------------------+
             ^       ^       ^       ^
             |       |       |       |
          publish  read    read    read
             |
        mnemond-a
```

每个 `mnemond`:

- 只写自己的 branch。
- 读取自己订阅的 publication branches。
- pull 后本地验证 scope/digest/idempotency。
- 只通过本地 Event Intake 导入。

GitHub mesh 数据流:

```text
agent-a emits teamwork_signal / assignment
  -> mnemond-a accepts event locally
  -> mnemond-a publishes synced envelope to branch mnemon/agent-a
  -> mnemond-b/c/d/e read branch mnemon/agent-a
  -> each local mnemond validates and imports
  -> assigned agents act
  -> each publishes its own accepted events on its own branch
```

这不是 agent-to-agent messaging。它是:

```text
mnemond-to-mnemond accepted-event publication mesh
```

## 5. 为什么是 shared repo + per-agent branch

### 5.1 优于 per-agent repo

```text
per-agent repo:
  + ownership clean
  - team bootstrap inventory = N repos
  - onboarding heavy
  - permissions/config spread across repos

shared repo + per-agent branch:
  + one team rendezvous
  + one permission namespace
  + publication streams enumerable by branch/manifest
  + still preserves one-writer-per-publication-stream
```

### 5.2 优于 one shared branch

```text
shared branch:
  - all mnemond write same head
  - commit races likely
  - ownership boundary fuzzy
  - one bad writer can corrupt shared stream

per-agent branch:
  + one writer per branch
  + no cross-agent branch-head contention
  + readers merge by local governance import
  + branch is transport namespace, not governed truth
```

### 5.3 推荐默认

默认 GitHub direct backend 应使用:

```text
one shared team repo
one publication branch per mnemond
```

不是每个 agent 一个 repo,也不是所有 agent 写一个 branch。

## 6. 组件架构

目标组件图:

```text
+----------------------------- HostAgent -----------------------------+
| Codex / Claude Code                                                  |
| thin hook + managed GUIDE + mnemon-observe skill                     |
| observe / pull / render                                              |
+-------------------------------+--------------------------------------+
                                |
                                v
+----------------------------- mnemond -------------------------------+
| Local governance domain                                             |
|                                                                      |
| Event Intake -> Admission Rules -> Bridge -> Materializer            |
|        |             |              |              |                 |
|        v             v              v              v                 |
|   observed log   diagnostics   proposed events   resources           |
|                                                                      |
| Store: events / resources / decisions / sync_events / cursors        |
+-------------------------------+--------------------------------------+
                                |
                                v
+------------------------ Sync Orchestrator --------------------------+
| Reads RemotePlan                                                     |
|                                                                      |
| Push lane:                                                           |
|   local accepted synced events -> push targets                       |
|                                                                      |
| Pull lane:                                                           |
|   pull sources -> validate/diagnose -> importPulledEvents            |
+-------------------------------+--------------------------------------+
                                |
                                v
+----------------------- Remote Workspace ABI ------------------------+
| interface:                                                           |
|   SyncPush(req)   -> SyncPushResponse                                |
|   SyncPull(req)   -> SyncPullResponse                                |
|   SyncStatus()    -> SyncStatusResponse                              |
+-------------------+-------------------------------+------------------+
                    |                               |
                    v                               v
        +----------------------+        +------------------------------+
        | HTTP backend          |        | GitHub backend               |
        | mnemon-hub            |        | publication mesh             |
        | central sync service  |        | shared repo/per-agent branch |
        +----------------------+        +------------------------------+
```

Boundary:

- `runtime` / `state` / `presentation` / `hostagent` must not know GitHub.
- `app` owns orchestration and import.
- `mnemonhub/exchange` owns remote exchange seams, cursors, local sync ledger helpers, and backend adapters.
- `mnemonhub` remains the central HTTP reference backend.

## 7. RemotePlan:从双向 remote 到 directional plan

当前 remote 是隐含双向:

```text
remote hub:
  push local events
  pull remote events
```

GitHub mesh 需要方向分离:

```text
publish target:
  where this mnemond writes its accepted-event stream

subscribe source:
  where this mnemond reads subscribed accepted-event streams
```

目标 model:

```text
RemoteEntry
  id
  backend: http | github
  direction: bidirectional | publish | subscribe
  credential_ref
  scope
  backend-specific config

RemotePlan
  PushTargets []RemoteEntry
  PullSources []RemoteEntry
```

兼容规则:

```text
empty backend   -> http
empty direction -> bidirectional
old remotes.json -> behavior unchanged
```

Mapping:

```text
http + bidirectional:
  PushTargets += entry
  PullSources += entry

github + publish:
  PushTargets += entry

github + subscribe:
  PullSources += entry
```

ASCII:

```text
+------------------------- RemotePlan Loader --------------------------+
| remotes.json                                                         |
|                                                                      |
| backend: http     direction: bidirectional -> push + pull             |
| backend: github   direction: publish       -> push only               |
| backend: github   direction: subscribe     -> pull only               |
+-------------------------------+--------------------------------------+
                                |
                                v
+------------------------- Sync Orchestrator --------------------------+
| for each PushTarget:                                                  |
|   ReadPushBatch -> RemoteWorkspace.SyncPush -> ApplyPushResponse      |
|                                                                      |
| for each PullSource:                                                  |
|   ReadPullState -> RemoteWorkspace.SyncPull -> import events          |
|                                      \-> ingest diagnostics           |
+----------------------------------------------------------------------+
```

## 8. GitHub repo layout

Team repo:

```text
mnemon-dev/mnemon-teamwork-example
```

Team coordination branch:

```text
branch: mnemon/team

.mnemon/team.json
```

Per-mnemond publication branches:

```text
branch: mnemon/<mnemond-id>

mnemon-publications/v1/
  manifest.json
  events/
    <origin_mnemond>/
      <resource_kind>/
        <resource_id>/
          <local_ingest_seq>-<local_decision_id>.json
```

`team.json` sketch:

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

`manifest.json` sketch:

```json
{
  "schema_version": 1,
  "backend": "github",
  "mode": "publication",
  "origin_mnemond": "agent-a",
  "published_at": "2026-06-26T00:00:00Z",
  "published_scopes": [
    { "kind": "assignment", "id": "project" },
    { "kind": "progress_digest", "id": "project" }
  ]
}
```

Event file:

```text
mnemon-publications/v1/events/<origin_mnemond>/<resource_kind>/<resource_id>/<local_ingest_seq>-<local_decision_id>.json
```

`local_ingest_seq` is zero-padded to 12 digits. This keeps GitHub paths human-reviewable:

```text
mnemon-publications/v1/events/agent-a/progress_digest/project/000000000007-dec-a.json
```

Rules:

- File body is a single `event.EventEnvelope` with `phase=synced`.
- Same path + same body = idempotent.
- Same path + different body = conflict diagnostic.
- The path is a visible publication address, not a hash key.
- Git commit SHA is transport cursor/provenance only.
- Governance ordering remains local ingest seq after import.

## 9. Data flows

### 9.1 Publish

```text
local Materializer accepts decision
  -> state records accepted envelope
  -> state records pending sync event
  -> sync orchestrator reads PushTargets
  -> GitHub backend derives visible publication path
  -> writes event file to own publication branch
  -> records per-target ack locally
```

ASCII:

```text
+----------+     +---------+     +-------------+     +------------------+
| decision | --> | synced  | --> | sync worker | --> | GitHub branch    |
| accepted |     | event   |     | push lane   |     | mnemon/agent-a   |
+----------+     +---------+     +-------------+     +------------------+
```

### 9.2 Subscribe / import

```text
sync orchestrator reads PullSources
  -> GitHub backend lists subscribed branch after cursor
  -> validates event envelope
  -> returns SyncPullResponse.Events
  -> app.importPulledEvents
  -> IngestTrusted(sync@local)
  -> Tick
  -> RemoteImportRule
  -> Materializer.Apply
```

ASCII:

```text
+------------------+     +-------------+     +--------------+     +----------+
| GitHub branch    | --> | pull lane   | --> | Event Intake | --> | local    |
| mnemon/agent-b   |     | validate    |     | + Tick       |     | resource |
+------------------+     +-------------+     +--------------+     +----------+
```

Forbidden:

```text
GitHub event -> direct Store.Resource write
```

Required:

```text
GitHub event -> Event Intake -> Tick -> Materializer
```

### 9.3 Diagnostics

GitHub direct lacks server-side push clamp, so pull must surface problems:

```text
invalid phase
invalid schema
digest mismatch
out-of-subscription scope
same publication path different body
unknown importable kind
```

All must become local visible diagnostics, never silent drops.

Target flow:

```text
GitHub backend detects invalid entry
  -> SyncPullResponse.Diagnostics
  -> sync orchestrator ingests diagnostic observation
  -> durable sync.diagnostic appears in local event log
```

This extends the existing skipped-kind path:

```text
sync.import_skipped.observed -> SyncImportSkippedRule -> sync.diagnostic
```

## 10. Teamwork scenario flow

Target acceptance scenario:

```text
5 codex appservers
5 mnemond
1 shared GitHub team repo
5 per-agent publication branches
0 shared governed.db
0 central active mnemon-hub
```

### 10.1 Bootstrap

```text
agent-a appserver starts
  -> mnemond-a starts
  -> publish branch mnemon/agent-a exists
  -> agent-a emits agent_profile
  -> profile is published
```

Team publication enumeration / roster bootstrap:

```text
team.json / branch list says a publication stream exists
agent_profile says whether agent-a is currently useful/available
assignment TTL/progress says whether work is healthy
```

Branch presence is not enough to assign work and is not a network-level online signal.

### 10.2 First act

```text
agent-a receives user teamwork task
  -> reads governed context
  -> emits teamwork_signal
  -> emits assignment(s)
  -> mnemond-a accepts
  -> mnemond-a publishes to mnemon/agent-a
```

Subscribed mnemond:

```text
mnemond-b/c/d/e subscribe to mnemon/agent-a
  -> import signal/assignment
  -> render work cues
  -> assigned agents act
```

### 10.3 Nested decomposition

```text
agent-b receives assignment
  -> decides it should split
  -> emits new teamwork_signal / assignment
  -> mnemond-b accepts
  -> mnemond-b publishes to mnemon/agent-b
  -> subscribed mnemond pull/import
```

This is Teamwork-ReAct-like:

```text
Sense -> Select -> Work -> Feedback
          ^                     |
          +------ next Act -----+
```

But the "act" is event-governed, not direct message dispatch.

### 10.4 Mnemond join

Two new `mnemond` instances join during work:

```text
agent-f/g appservers start
  -> create branches mnemon/agent-f/g
  -> publish fresh agent_profile
  -> subscribe to existing branches
  -> pull backlog according to cursors
```

Existing mnemond can import them only after the next configured repo pull and:

```text
team manifest/branch enumeration includes the publication stream
+ fresh agent_profile is imported
```

### 10.5 Mnemond stale and reassignment

One `mnemond` stops publishing fresh evidence:

```text
agent-c stops publishing progress/profile refresh
```

No scheduler should directly reassign. Instead:

```text
assignment TTL expires without progress
  -> presentation derives expired/stalled cue
  -> another agent emits teamwork_signal or assignment
  -> mnemond accepts reassignment event
  -> event propagates through mesh
```

This preserves R1 rule:

```text
assignment_expired is derived, not durable state.
assignment_status is deferred.
```

### 10.6 Aggregation and next act

Agents publish `progress_digest`.

Aggregator-like agent sees:

```text
progress_digest from branches b/d/e/f
missing/expired assignment from c
```

It may:

- emit final `progress_digest`;
- emit new `teamwork_signal`;
- emit new `assignment` for another act;
- ask user only if ambiguity/risk requires escalation.

Loop continues until the task is truly complete.

## 11. Publication sensing model

Do not conflate publication stream inventory with teamwork availability.

Three layers:

```text
1. Configured workspace inventory
   shared repo / team.json / branch enumeration
   tells mnemond which publication streams can be subscribed

2. Governed agent presence
   agent_profile with ttl/freshness/availability
   tells agents who appears available and suited

3. Assignment health
   assignment TTL + progress_digest
   tells agents whether a commitment is stale, blocked, or complete enough
```

Publication candidate active:

```text
publication branch exists
+ fresh agent_profile
+ suitable scope/context advantages
=> candidate for assignment
```

This does not mean a direct network counterpart is online. It only means the configured workspace currently contains an acceptable publication stream with fresh governed evidence.

Publication candidate stale:

```text
profile stale
or assignment TTL expired without progress
=> derived cue; another agent may reassign
```

## 12. 权限与安全

Two-layer permission model:

```text
GitHub permission:
  who can read/write branches or repo

Mnemon permission:
  which origin/resource_ref/event material this mnemond accepts
```

Shared repo + per-agent branch 模式必须诚实承认一个边界:GitHub 本身不能可靠地替 Mnemon 表达每个 branch / 每个 resource_ref / 每个 event 的治理权限。实际安全模型应理解为:

```text
GitHub repo permission = transport access
mnemond import policy  = governance permission
```

也就是说:

- GitHub 控制谁能接触这个 repo。
- Branch 命名和 branch ownership 是约定/配置,不是完整的 Mnemon 权限系统。
- 一个订阅方能读到某个 branch,不等于本地 `mnemond` 会接受里面的 event。
- 一个 writer 能把内容写进 GitHub,不等于其他 `mnemond` 会导入这些内容。
- 真正的接受/拒绝发生在订阅方本地 pull/import 时。

Pull-side acceptance chain:

```text
GitHub event
  -> backend validation
  -> subscription/origin policy
  -> resource_ref scope check
  -> digest/schema/idempotency check
  -> Event Intake
  -> Tick
  -> Materializer
```

如果任何一步失败,结果必须是 diagnostic,不是静默丢弃,也不是直接写资源。

GitHub direct:

- Good for quick start and single trust domain.
- Branch protection/rulesets can reduce accidental branch damage.
- Fine-grained tokens/deploy keys can limit repo access.
- Still cannot express Mnemon resource-level scope by itself.
- Cannot prevent an already-authorized repo writer from publishing malformed or out-of-scope material.
- Relies on each subscribing `mnemond` to fail-closed on import.

Therefore GitHub direct is appropriate for:

```text
single trust domain
honest clients
quick-start decentralized mesh
```

It is not enough for:

```text
strong cross-trust-domain boundary
server-side push clamp
strict pre-append authorization
```

GitHub App:

```text
Local mnemond -> GitHub App backend -> GitHub repo
```

App backend can validate before writing:

- identity
- scope clamp
- digest
- idempotency
- branch ownership

This is closer to `mnemon-hub` server-side semantics, but requires running an App backend. Use it when "bad material should not be appendable to the remote at all" is a requirement.

mnemon-hub:

- Strongest first-party reference backend.
- Server-side validation.
- Good for stronger cross-domain boundary.
- Requires service deployment.

Positioning:

```text
GitHub direct = bootstrap decentralized mesh
mnemon-hub    = strong reference hub
GitHub App    = GitHub-hosted strong validation variant
```

## 13. State model pressure point:per-remote sync status

Current local sync ledger mostly treats a synced event as pending/synced/conflict globally. GitHub mesh may need per-target status:

```text
event E published to:
  github-self: synced
  http-hub: pending
  archive: conflict
```

If one local accepted event must publish to multiple push targets, marking it globally synced after the first successful target would starve other targets.

Likely requirement:

```text
sync_event_targets
  publication_path
  remote_id
  status
  diagnostic
  updated_at
```

MVP escape hatch:

- Start with one publish target for GitHub direct.
- Do not claim multi-publish reliability until per-target ledger exists.

## 14. Implementation stages

### Stage 0: Backend seam

Status: initial version implemented in code.

```text
exchange.RemoteWorkspace
  SyncPush
  SyncPull
  SyncStatus
```

HTTP `access.Client` satisfies this ABI.

### Stage 1: Directional RemotePlan

Add:

```text
backend
direction
RemotePlan{PushTargets, PullSources}
```

Tests:

- legacy config remains bidirectional HTTP.
- unknown backend fail-closed.
- unknown direction fail-closed.
- publish-only does not pull.
- subscribe-only does not push.

### Stage 2: Pull diagnostics consumption

Consume `SyncPullResponse.Diagnostics`.

Tests:

- fake remote returns diagnostic.
- local event log gets durable `sync.diagnostic`.
- repeated pull is idempotent.

### Stage 3: Publication store interface

Before real GitHub API, define fake-testable storage:

```go
type PublicationStore interface {
    PutEvent(path string, body []byte) (created bool, conflict bool, err error)
    ListEvents(prefix string, cursor string) (events []StoredEvent, nextCursor string, err error)
}
```

Tests use memory/fake store.

### Stage 4: GitHub backend skeleton

Implement `RemoteWorkspace` over `PublicationStore`.

Push:

- read local batch
- derive visible publication path
- write to own branch store
- return accepted/conflict

Pull:

- list subscribed branch store
- validate envelope
- return valid events + diagnostics

### Stage 5: Repo contract and operator config

Freeze the validation repo and operator-facing config before the live adapter:

```text
repo: mnemon-dev/mnemon-teamwork-example
team metadata branch: mnemon/team
publication branch: mnemon/<mnemond-id>
```

This stage defines:

- branch namespace;
- event/report paths;
- `team.json` shape;
- CLI examples;
- validation rules for repo/branch/path shape.

CLI options:

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

Later, if clearer:

```bash
mnemon-harness sync publish connect ...
mnemon-harness sync subscribe add ...
```

### Stage 6: Real GitHub adapter and live case

Add GitHub API implementation behind `PublicationStore`. Detailed execution is governed by [github-decentralized-mesh-implementation-plan.md](github-decentralized-mesh-implementation-plan.md).

MVP choices:

- Contents API: simpler for one-file-per-event writes.
- Git Trees/Commits/Refs: better for batching and branch-head control.

Use fake store for default unit behavior, but do not stop at fake-store proof. Stage 6 is complete only after a gated real GitHub case proves the flow on `mnemon-dev/mnemon-teamwork-example`.

Required gated live case:

```text
configured GitHub repo: mnemon-dev/mnemon-teamwork-example
branch mnemon/agent-a
branch mnemon/agent-b

agent-a push:
  accepted synced envelope -> GitHub publication branch

agent-b pull:
  enumerate subscribed publication branch
  read publication entry
  validate envelope
  import through Event Intake
  persist cursor/status

repeat pull:
  no duplicate import
```

Do not add real network tests as default unit tests. The real GitHub case should run only when credentials/repo are explicitly provided, but the milestone cannot be called done until that case has passed at least once.

### Stage 6.5: Validation report contract

Before the full 5-appserver acceptance, define the dedicated evidence report contract so the test harness does not drift into GitHub-native teamwork.

Validation repo layout:

```text
mnemon/team
  .mnemon/team.json
  .mnemon/scenarios/<scenario>.json
  .mnemon/reports/<run-id>/summary.json

mnemon/<mnemond-a>
  mnemon-publications/v1/manifest.json
  mnemon-publications/v1/events/<origin>/<resource-kind>/<resource-id>/<local-seq>-<decision-id>.json

mnemon/<mnemond-b>
mnemon/<mnemond-c>
...
```

Contract:

- `mnemon/team` holds bootstrap metadata and reports only.
- `mnemon/<mnemond-id>` branches hold accepted-event publication logs only.
- No Issue/PR/Action is used as teamwork state.
- Reports summarize evidence; they do not become input to `mnemond` governance.
- The validation repo may be deleted and recreated without changing local governance semantics.

Required report evidence:

```text
run_id
participants
publication branches
events published per branch
events imported per mnemond
diagnostics per mnemond
assignment/progress chain
mnemond join/leave timeline
proof no central mnemon-hub endpoint was used
proof no shared governed.db was used
```

### Stage 7: Acceptance scenarios

All real appserver scenarios must start from one or more connected PoC agents receiving ordinary user messages. The harness may start/stop nodes and collect evidence, but it must not use a global choreography prompt to tell every agent what to do.

Deterministic acceptance:

```text
5 local mnemond/runtime instances
1 fake or real publication store
5 publication branches
0 central mnemon-hub
0 shared governed.db
```

Real appserver acceptance:

```text
5 real codex appservers
5 mnemond
5 isolated runtime workspaces
5 isolated local mnemond stores
1 shared GitHub repo: mnemon-dev/mnemon-teamwork-example
5 run-scoped publication branches
0 central mnemon-hub
0 shared governed.db
```

Isolation requirements:

- each appserver is attached to exactly one dedicated `mnemond`;
- each `mnemond` has its own local store;
- each appserver has its own runtime workspace;
- cross-agent visibility only happens through publication branch pull/import.
- default real acceptance branches are `mnemon/acceptance/<run-id>/agent-*` and are initialized from `main` before local sync starts.
- long-lived branches such as `mnemon/agent-a` are explicit operator smoke-test inputs, not the default acceptance isolation model.

Scenario:

1. Start 5 appservers/mnemond.
2. Publish fresh profiles.
3. Send an ordinary user message to one connected PoC agent to start teamwork.
4. Verify assignment propagation.
5. Verify nested decomposition.
6. Verify first-round outputs cause a second-round plan.
7. Verify second-round reassignment/refinement is executed.
8. Add 2 `mnemond` instances mid-run.
9. Stop 1 `mnemond` mid-run.
10. Verify stale/TTL cue causes governed reassignment.
11. Verify progress aggregation.
12. Verify another act can be emitted after aggregation.
13. Verify completion proof uses accepted events, not GitHub issue/PR state.

Additional natural task scenarios should cover:

- single-PoC repository onboarding synthesis;
- implementation/test investigation where work on task B can complete or advance task A;
- multi-PoC live-readiness/operator-safety work over the same repo;
- profile freshness and profile/posture updates during the run.
- multiple output-driven Teamwork-ReAct rounds: review outputs, replan, reassign, execute, aggregate again.

## 15. Remaining open questions

- Should public read repos be supported with signed envelopes, or require private repos for MVP?
- Should branch protection be recommended or automated?
- Do we need external signatures before claiming cross-trust-domain safety?
- How aggressive should compaction/checkpoint be for long-running teams?
