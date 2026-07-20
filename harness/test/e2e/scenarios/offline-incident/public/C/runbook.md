# Consumer incident runbook

1. Separate request identity from transport attempt identity.
2. Treat response timeout as unknown outcome, not permission to create a second effect.
3. Repair a missing publication from its signed origin.
4. Verify the full Artifact digest before using logs or trace data.
5. Replay the regression twice and require byte-identical results.
