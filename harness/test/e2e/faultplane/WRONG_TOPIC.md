# Wrong-topic replay boundary

The R5 contract asks the fixed six-node Docker topology to replay a publication
captured on channel Alpha onto channel Beta. That fault cannot currently be
created through the production-like public surface without weakening another
acceptance boundary.

All six nodes A-F are ordinary product participants in the prescribed topology.
The public commands can create and join channels and publish new domain events,
but they do not expose an operation that emits a captured raw publication frame
on a caller-selected transport topic. The authenticated and encrypted libp2p
transport also prevents a host-side byte proxy from changing the topic of an
opaque captured frame. Creating a new Beta event is not replaying the captured
Alpha publication.

The apparent workarounds violate the contract:

- importing Harness internals or mutating node state would introduce a test
  backdoor;
- parsing and rewriting protocol messages in the fault plane would cease to be
  an external, domain-opaque fault;
- adding a malicious seventh publisher would violate the fixed six-node
  topology;
- treating a newly derived Beta event as the replay would weaken the required
  semantic oracle.

Accordingly, this fault plane deliberately has no wrong-topic primitive. The
`wrong-topic-replay` claim must remain neither injected nor observed until a
production-like public adversarial surface can perform the exact replay, or the
canonical topology contract is explicitly changed. The response-loss and Docker
network receipts likewise assert only their exact external action; they do not
claim a public fault observation.
