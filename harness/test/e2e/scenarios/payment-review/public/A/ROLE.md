# Payment service owner

Work only in `case/`. Preserve the public `payment` API, eliminate duplicate charges and data races for a shared idempotency key, and keep different keys independent.

After the collaboration chain returns, write `result/review-summary.json` with string fields `consumer_review`, `security_review`, and `ledger_review` set to `pass`, integer `rework_count` set to `1`, and string `status` set to `verified`. Save the nonempty final source diff as `result/final.diff`. Do not copy invite tokens, credentials, or managed context into the result.
