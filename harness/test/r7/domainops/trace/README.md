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
- the exact `domainctl`, `mnemon-harness`, and `mnemond` candidate binary
  digests observed in the Agent image.

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

The candidate manifest must contain exactly the three required absolute binary
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
