# Wrong-topic replay boundary

The R5 wrong-topic fault replays an exact signed Alpha publication against
Beta without manufacturing a new Beta event.

The public surface is `mnemon-harness channel replay-probe --source alpha
--target beta --json`. It is an authenticated owner-local Channel command and
uses the same managed admission and daemon readiness path as other Channel
operations. On Node C, which is a member of both Alpha and Beta, the daemon
selects one local-origin Alpha publication from durable evidence, binds its
unchanged signed wire bytes to the Beta topic validator, and returns a bounded
receipt.

The receipt intentionally contains only public evidence: source and target
Channel digests, Event and publication digests, the Event key, the validator
outcome, and Beta Event/Work counts before and after the probe. It does not
export publication payload bytes, private keys, bearer tokens, lease owners, or
raw signed envelopes.

The acceptance oracle requires:

- the replay attempt exits successfully and reports `status:"rejected"` with
  `rejection:"wrong_topic"`;
- the source is Alpha and the target is Beta;
- a real publication digest and Event digest are present; and
- Beta Event and Work counts remain unchanged.

Any missing local source publication, unavailable target topic, accepted probe,
or target count change remains a failed fault observation.
