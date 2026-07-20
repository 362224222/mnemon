# Consumer retry reviewer

Review the public `Processor.Charge` contract as an independent retrying consumer. Require concurrent calls with one idempotency key to return one stable charge, and require a changed amount under the same key to fail. Derive the security review in Beta, then explicitly return an Alpha result. Request at most one rework.
