---
name: mnemon-harness
description: Process one mnemond-managed Teamwork event after a [mnemon:wake] cue, or explicitly delegate review work through the Mnemon Harness CLI. Use for Channel-scoped review offers, acceptance, delivery, rework, close, cancellation, or a fenced handling resolution.
---

# Mnemon Harness Teamwork

Use `mnemon-harness` as the only Agent interface to managed Teamwork. Never write Event JSON, choose IDs or authority fields, read a context file, or call mnemond directly.

## Process one pending Event

1. Run `mnemon-harness agent current --json` once after a `[mnemon:wake]` cue.
2. If `status` is `actionable`, inspect that single projection and its `allowed_actions`.
3. Read [guides/teamwork/GUIDE.md](guides/teamwork/GUIDE.md) before choosing the outcome.
4. Invoke exactly one allowed `teamwork` action or `agent resolve` with the returned `context_file` path.
5. Treat only a validated `accepted` or `resolved` receipt as completion. On failure, keep the Event unresolved and report the stable error.

`none`, `busy`, and `waiting` are honest no-work states. Do not loop, drain a backlog, or invent work when one is returned.

## Start Teamwork explicitly

For an ordinary Host-owned task, use Teamwork only when an independent review or bounded delegation materially helps. Run `agent current --json` to obtain bounded Channel aliases, then submit one contextless `teamwork offer`. Do not turn the original task into an Event automatically.

## Closed commands

```text
mnemon-harness agent current --json
mnemon-harness teamwork offer|accept|decline|deliver|rework|close|cancel
mnemon-harness agent resolve no-action|retry|reject --context <context-file>
```

- Pass natural-language input only through `--content-file <workspace-relative-path|->`; use stdin for `-`.
- Pass short selectors only with `--channel`, `--to`, and `--deadline` on `offer`.
- Pass selected workspace outputs with repeated `--artifact` flags. Do not assign Artifact roles.
- Pass the exact returned path with `--context`; never open, copy, print, move, or reuse its contents.
- Request `--json` and preserve the receipt when reporting a managed outcome.

There is no MCP, generic submit command, capability registry, Memory, or Evolution surface in R5.
