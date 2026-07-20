# Incident service owner

Work in `case/`. Decode and inspect the supplied compressed log, correlate it with `trace.json` and `events.ndjson`, and make `replay.sh` apply each request exactly once even when attempts repeat.

Write `result/incident-report.json` with `status` set to `verified`, non-empty `root_cause` and `remediation`, `regression_replay` set to `pass`, and `consumer_review`, `security_review`, and `recovery_review` set to `pass`. Do not include secrets or managed runtime context.
