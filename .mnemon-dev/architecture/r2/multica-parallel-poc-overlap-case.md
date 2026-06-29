# Multica Parallel PoC Overlap Case

- Status: R2 acceptance scenario design
- Date: 2026-06-30
- Related:
  - `multica-hub-backend-architecture.md`
  - `multica-runtime-adapter.md`
  - `teamwork-event-driven-cue-model.md`
  - `../teamwork-protocol-sense-select-work-feedback.md`

## 0. Purpose

这个 case 用来验证 Multica 作为 Mnemon hub backend 时,是否能承载接近真实业务协作的复杂度:

- 一个 root issue 作为 session mailbox。
- 三个相关 PoC 同时启动,每个 PoC 有独立目录和交付物。
- PoC 之间共享上下文,并要求反馈显式引用共享上下文。
- 角色有重叠,同一个 agent 会把一个 PoC 的局部上下文带到另一个 PoC。
- 第一轮反馈之后必须产生至少一个 follow-up assignment,触发第二轮 teamwork。
- 最终 integrator 需要把多轮反馈合成一个 operator-facing decision。

本 case 不专门测试 GitHub mesh。GitHub mesh 只需要在设计上保持 mirror-only / no second activation path 的逻辑合理性。当前验收重点是 `mnemonhub` 与 Multica hub path,其中 Multica 是可见产品面和 assignment activation path。

## 1. Scenario

Case id:

```text
parallel-poc-overlap
```

Root issue title:

```text
Parallel PoC overlap drill <HHMMSS>
```

Recommended command:

```text
mnemon-acceptance multica-runtime-prod-sim \
  --task-case parallel-poc-overlap \
  --require-hub-flow \
  --min-participants 5 \
  --min-active-agents 5
```

Directory plan created under the acceptance run root:

```text
<run-root>/
  acceptance-report.json
  taskcase/
    parallel-poc-overlap/
      evidence/
      shared-context/
        session-map/
        mailbox-contract/
        risk-register/
        evidence-ledger/
      workstreams/
        poc-runtime-routing/
        poc-operator-runbook/
        poc-release-risk/
      roles/
        planner/
        researcher/
        implementer/
        reviewer/
        integrator/
```

The root issue body should include the same plan in human-readable Markdown. Deterministic protocol data still belongs in Multica metadata or stable comment markers, not visible prose.

## 2. Collaboration Topology

```text
                              +-------------------------------+
                              | Multica root issue             |
                              | session mailbox                |
                              +---------------+---------------+
                                              |
                                              v
                                  planner@team creates
                                  first-round assignments
                                              |
          +-----------------------------------+-----------------------------------+
          |                                   |                                   |
          v                                   v                                   v
+-----------------------+         +-----------------------+         +-----------------------+
| poc-runtime-routing   |         | poc-operator-runbook  |         | poc-release-risk      |
| assignment mailbox A  |         | assignment mailbox B  |         | assignment mailbox C  |
| researcher/implementer|         | implementer/reviewer  |         | researcher/reviewer/  |
|                       |         |                       |         | integrator            |
+-----------+-----------+         +-----------+-----------+         +-----------+-----------+
            |                                 |                                 |
            +---------------+-----------------+-----------------+---------------+
                            | shared contexts and feedback       |
                            v                                    |
                  +---------------------+                        |
                  | follow-up mailbox   |<-----------------------+
                  | chosen from gap or  |
                  | disagreement        |
                  +----------+----------+
                             |
                             v
                    +-------------------+
                    | integrator@team   |
                    | final decision    |
                    +-------------------+
```

## 3. Shared Contexts

```text
+------------------+---------------------------+-----------------------------+
| Shared context   | Used by                   | Purpose                     |
+------------------+---------------------------+-----------------------------+
| session-map      | runtime-routing, release  | Map root, child, run, agent |
| mailbox-contract | runtime-routing, runbook  | Visible text and metadata   |
| risk-register    | runbook, release          | Risks, owners, mitigations  |
| evidence-ledger  | all PoCs                  | Issue/run/comment evidence  |
+------------------+---------------------------+-----------------------------+
```

Expected reuse:

- `researcher@team` carries `session-map` from runtime routing into release risk.
- `implementer@team` carries `mailbox-contract` from runtime routing into runbook review.
- `reviewer@team` carries rollback and status-projection concerns from runbook review into release risk.
- `integrator@team` consumes all contexts and must wait for follow-up feedback before closing.

## 4. Role Matrix

```text
+------------------+-----------------------+-------------------------------+
| Principal        | Primary PoC           | Overlap                       |
+------------------+-----------------------+-------------------------------+
| planner@team     | poc-runtime-routing   | poc-release-risk              |
| researcher@team  | poc-runtime-routing   | poc-release-risk              |
| implementer@team | poc-operator-runbook  | poc-runtime-routing           |
| reviewer@team    | poc-release-risk      | poc-operator-runbook          |
| integrator@team  | poc-release-risk      | all PoCs for final synthesis  |
+------------------+-----------------------+-------------------------------+
```

Roles are cues, not permanent identities. The protocol-level facts remain accepted events and hub metadata. An agent may be PoC-like when emitting a signal or assignment, and IC-like when working an assignment and producing feedback.

## 5. Expected ReAct Progression

```text
Round 1: Observe
  root issue -> planner runtime run
  planner emits three assignments
  Multica projects three child issue mailboxes
  runtime-routing, runbook, and release PoCs run in parallel
  each feedback comment names:
    - shared context consumed
    - evidence artifact produced
    - issue/run/comment/status refs

Round 2: Act
  planner or integrator reads first-round feedback
  highest disagreement or missing evidence is selected
  a follow-up assignment is created
  follow-up owner reuses at least two shared contexts
  feedback cites prior child comments before adding new evidence

Round 3: Reflect
  integrator consumes all PoC outputs and follow-up feedback
  final root comment records:
    - observed facts
    - actions taken
    - context reused across PoCs
    - residual risk
    - ship/hold/follow-up decision
```

## 6. Protocol And Multica Boundaries

```text
+----------------------+------------------------------+------------------------------+
| Layer                | Carries                      | Must not carry               |
+----------------------+------------------------------+------------------------------+
| Multica visible text | task, context, evidence      | session ids, assignment ids  |
| Multica metadata     | routing, dedupe, correlation | LLM prompt instructions      |
| Multica comments     | human feedback, event refs   | sole canonical truth         |
| Multica runs         | activation evidence          | proof of task completion     |
| Mnemon event store   | canonical accepted events    | product-only display state   |
| mnemond render       | LLM-facing cue               | raw Multica issue as prompt  |
+----------------------+------------------------------+------------------------------+
```

Standard Multica-visible references should be used for humans only: issue mentions, assigned agents, and normal comments. Machine correlation should use `mnemon.*` metadata keys and stable event/comment markers.

## 7. Expected Acceptance Signals

Minimum successful run:

```text
active_agents        >= 5
child_mailboxes      >= 4
feedback_comments    >= 4
teamwork_rounds      >= observe + act + reflect
root final status    terminal
child final statuses result or blocker
```

Report evidence should show:

- root metadata has `hub_backend=multica` and `kind=session_mailbox`.
- child issues carry assignment mailbox metadata.
- child visible text uses structured sections: Assignment, Context, Feedback.
- child visible text does not expose session ids, assignment ids, fingerprints, or projection owner keys.
- run evidence covers planner plus at least four additional participants.
- comments include context reuse evidence, not only generic completion text.

## 8. Failure Signals

```text
+------------------------------------+---------------------------------------+
| Failure                            | Meaning                               |
+------------------------------------+---------------------------------------+
| only one child mailbox             | planner did not split teamwork         |
| no follow-up mailbox               | second ReAct round did not happen      |
| no context names in feedback       | context reuse is not observable        |
| duplicated child mailbox           | hub dedupe or assignment identity risk |
| protocol fields in visible text    | metadata/visible boundary regression   |
| active agent count below expected  | Multica activation did not fan out     |
| final root lacks decision          | integration loop did not close         |
+------------------------------------+---------------------------------------+
```

This case is intentionally stronger than a routing smoke test. It should make weak assignment routing, missing feedback projection, stale context selection, and accidental protocol leakage visible in one Multica session.
