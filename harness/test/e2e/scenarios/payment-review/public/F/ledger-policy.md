# Ledger policy

Each accepted idempotency key maps to exactly one immutable charge. Reconciliation must be reproducible from committed charge identities, and a replay must have no second semantic effect.
