# Recovery checklist

- Repeated attempts for one request have one committed effect.
- The fixed replay output is deterministic across two executions.
- Restart does not require a manual daemon, wake, pull, or sync command.
- The report names a rollback trigger and an observable duplicate-effect metric.
