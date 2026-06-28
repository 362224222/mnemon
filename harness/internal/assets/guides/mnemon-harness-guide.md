# Mnemon Harness Guide

Mnemon is the governed event layer for durable agent context. Use it when the current work depends on prior governed state, or when this turn changes durable state that another agent or a future turn should see.

## Rhythm

- Before substantive work, decide whether governed context should be read.
- After substantive work, decide whether durable state should be recorded.
- Before context compaction, preserve important continuity through Mnemon when needed.
- `[mnemon:wake]` is only a local wake signal.

## Read

Use the generic skill or CLI to inspect current state before acting when the prompt references prior decisions, active coordination, delegated work, project intent, or another agent's state.

Useful commands:

```bash
. .mnemon/harness/local/env.sh
"${MNEMON_HARNESS_BIN:-mnemon-harness}" control pull
"${MNEMON_HARNESS_BIN:-mnemon-harness}" control render --intent context.packet
"${MNEMON_HARNESS_BIN:-mnemon-harness}" control render --intent teamwork.events
"${MNEMON_HARNESS_BIN:-mnemon-harness}" loop packages
"${MNEMON_HARNESS_BIN:-mnemon-harness}" loop schema --type <kind>
```

## Record

Emit governed events through Mnemon. Do not write `.mnemon` state files directly.

Prefer the short teamwork/profile commands when they fit the event you need:

```bash
. .mnemon/harness/local/env.sh
"${MNEMON_HARNESS_BIN:-mnemon-harness}" control teamwork signal \
  --scope <scope> \
  --statement "<collaboration need>" \
  --why-teamwork "<why another agent is useful>" \
  --evidence "<evidence ref>" \
  --external-id <unique-id>

"${MNEMON_HARNESS_BIN:-mnemon-harness}" control teamwork assign \
  --assignee <agent-principal> \
  --scope <scope> \
  --work "<expected work>" \
  --evidence "<evidence ref>" \
  --external-id <unique-id>

"${MNEMON_HARNESS_BIN:-mnemon-harness}" control teamwork progress \
  --assignment-ref <assignment-id> \
  --summary "<progress, result, or blocker>" \
  --external-id <unique-id>

"${MNEMON_HARNESS_BIN:-mnemon-harness}" control profile update \
  --focus "<current focus>" \
  --advantage "<context advantage>" \
  --summary "<profile summary>" \
  --external-id <unique-id>
```

Use the low-level observe API only when you need fields the short commands do not expose:

```bash
. .mnemon/harness/local/env.sh
"${MNEMON_HARNESS_BIN:-mnemon-harness}" control observe \
  --type <kind>.write_candidate.observed \
  --payload '{"rule":{...},"narrative":{...},"refs":{...}}' \
  --external-id <unique-id>
```

Check `"${MNEMON_HARNESS_BIN:-mnemon-harness}" loop schema --type <kind>` before guessing payload fields.

## Teamwork Events

- Emit `agent_profile` when your role, focus, availability, constraints, or context advantages materially change.
- Emit `project_intent` when the durable project direction, goal, or framing changes.
- Emit `teamwork_signal` when collaboration is needed and the need should be visible to other agents.
- Emit `assignment` when concrete work is delegated or self-assigned with expected feedback.
- Emit `progress_digest` when a result, blocker, or important context change should be returned.

## Guardrails

- Do not record secrets, credentials, tokens, or transient scratch state.
- Do not invent `assignment_status` or `assignment_expired`; expiration is derived by presentation.
- Include evidence for mid-risk coordination events when required by schema.
- Prefer one governed event per durable fact or commitment.
