# Domain Operations Trace Adapter

This adapter turns stopped R7 authority stores and a sanitized runner report
into the protocol-neutral `mnemon.test.trace` format. It never reads a prompt,
provider stream, transcript, reasoning record, or semantic payload.

## Scenario identity

The trace header's `scenario.digest` is content addressed. It binds:

- every regular file under the domain-operations fixture tree, including the
  mission, five domain projections, Compose world, tools, tests, and fixtures;
- the paid runner, Agent Dockerfile, and load/world entry points that determine
  the attention schedule, Runtime image, and external oracle;
- the exact `domainctl`, `mnemon-harness`, `mnemond`, and bounded Pi delegate
  asset digests observed in the Agent image.

Rounds, timestamps, model output, and successful outcomes are deliberately not
part of this identity. Changing the physical case or candidate binaries changes
the digest; merely rerunning the same case does not.

The runner integration is intentionally small. Preserve the existing
`sha256sum` output as a mode-0600 regular file and invoke:

```text
go run ./test/r7/domainops/trace \
  --report /absolute/sanitized-report.json \
  --authority /absolute/stopped-authority-root \
  --scenario-root /absolute/harness \
  --candidate-binaries /absolute/candidate-binaries.sha256 \
  --output /absolute/result.trace
```

The candidate manifest must contain exactly the required absolute runtime
paths. The adapter independently hashes every fixture input and rejects missing,
unbound, duplicate, symlinked, oversized, or malformed inputs.

## Authority summary

For every accepted Event, the trace may expose only bounded protocol metadata:

- semantic kind;
- source Principal;
- target aliases or local Principal IDs and target count;
- Artifact digest/count;
- semantic payload byte length, never payload bytes;
- Handling or Reference effects;
- peer Delivery, re-admission, and Receipt lineage.

These fields come from canonical stopped authority state. Runtime counters never
invent a causal edge to an Event.

`turns[].delegate_calls` counts completed child-Pi effects, not tool attempts.
The Runtime may return a closed `slot_used` result for a repeated attempt, but
the report still requires at most one completed delegate per parent turn.
The `submit_*`, `intent_submits`, and `*_receipts` turn counters describe paired
Pi Bash envelopes that visibly contained submit traffic; they do not count
shell processes or canonical Effects. Sequential corrections inside one
envelope collapse by precedence to accepted, rejected, control denial, or
invocation failure. `turns[].submit_invocation_failures` therefore counts
envelopes that exposed no Receipt or admission diagnostic. These Runtime
observations never prove or contribute evidence of an Effect; only stopped
authority state does that. `turns[].submit_control_diagnostics` retains only the
bounded, deduplicated control code and safe CLI message needed to diagnose an
Agent-surface failure; it contains no submitted Intent or provider prose.

The passed runner report is `mnemon.r7.domain-ops.live-report` version 2. It
contains two ordered service-world episodes and a bounded authority boundary
between them. A runner-attested sequence captured after the external recovery
oracle starts the consolidation interval; the adapter independently verifies
that every reported boundary head was accepted after that sequence, exists in
the stopped Reference lineage, and is no newer than the end boundary. Every
reported later use must be an exact causation or supersede/retract edge from a
post-boundary accepted Event. It does not inspect Artifact bytes, semantic kinds, or remediation
choices. Version 1 passed reports are intentionally not accepted as two-episode
evidence; failed reports retain their independent version-1 shape below.

## Failed-run input

After authority state exists, a failed live run should still stop and copy all
five stores, then write a sanitized input with this closed shape:

```json
{
  "schema": "mnemon.r7.domain-ops.failure-report",
  "version": 1,
  "status": "failed",
  "model": "bounded-model-token",
  "run": {
    "id": "bounded-run-token",
    "started_at": "canonical UTC RFC3339Nano",
    "finished_at": "canonical UTC RFC3339Nano",
    "candidate_digest": "sha256:..."
  },
  "failure": {
    "code": "bounded.machine-readable-code",
    "observed_at": "canonical UTC RFC3339Nano"
  },
  "turns": [],
  "raw_provider_streams_retained": false
}
```

`turns` may contain only the same completed, sanitized counter records used by
the passed report. Partial text, tool input/output, prompts, and provider errors
do not belong in this file. Invoke the adapter with `--failure-report` instead
of `--report` and the same three evidence paths.

A failure trace preserves whatever accepted Event, target, Artifact, Handling,
Receipt, and peer Delivery chain exists in the stopped stores. Its terminal
result is always `failed`; the scenario gate is `fail`, unevaluated R7 gates are
`unknown`, and no failed run can be rendered as a passed scenario merely because
its authority databases are internally valid.
