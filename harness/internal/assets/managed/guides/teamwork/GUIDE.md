# Teamwork decision guide

Handle only the single projection returned by `mnemon-harness agent current --json`. Base every decision on its source Event, exact Work version, local role, verified Artifact references, and `allowed_actions`.

## Choose an action

- On a reviewer `OFFERED` Work, use `accept` when you can perform the review; use `decline` with a concrete reason when you cannot.
- On a reviewer `ACTIVE` or `REWORK` Work, inspect the brief and readonly Artifacts, do the work, then use `deliver` with a concise result summary and any explicit result paths.
- On a home `DELIVERED` Work, use `close` when the result satisfies the brief. Use `rework` only for the one allowed correction iteration and state the required correction precisely.
- Use `cancel` only when the current home Work should terminate without a result and the action is explicitly allowed.
- Use a context-bound `offer` only when the current reviewer Work permits nested delegation. Select one reviewer by effective alias; the child receives the same brief, deadline, and Artifact closure.

Participant `accept`, `decline`, and `deliver` are requests accepted by the participant's local mnemond. They do not directly change the remote home Work. Wait for later managed Events instead of claiming that Gossip delivery or a local receipt completed the remote transition.

## Resolve without a Teamwork action

- Use `agent resolve no-action` only when the Event is valid but requires no domain action.
- Use `agent resolve retry` for a transient inability to decide or act. Give a short reason when useful.
- Use `agent resolve reject` only when the managed input is invalid or unsafe to process; a nonempty reason is required.

Never use a resolution to hide an action failure or a Work conflict. A failed command leaves the handling unresolved unless its returned durable receipt explicitly says otherwise.

## Information and Artifact boundaries

- Keep the original user or launcher task Host-owned. Teamwork carries only the bounded brief and selected outputs needed by collaborators.
- Do not expose secrets, credentials, unrelated workspace content, raw PeerID/ChannelID values, context capabilities, operation keys, or local Node state.
- Read only Artifact view paths present in the current projection. Treat those views as readonly.
- Select produced Artifact paths deliberately. Absolute paths, traversal, symlinks, devices, internal unprojected Node paths, and implicit directory expansion are forbidden.
- Do not infer delivery from Gossip publish, reachability, Inbox presence, or Runtime completion. Only accepted Event and operation receipts are authority.
