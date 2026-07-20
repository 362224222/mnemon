# Payment security policy

Idempotency keys are confidential request credentials. A key binds exactly one immutable charge request. Logs, error text, review summaries, and Artifact names must not contain the key. Conflicting content must fail without changing the ledger.
