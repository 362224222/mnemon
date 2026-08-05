# Federated Domain Operations Case

This fixture exercises R7 federation and the View-driven evolution loop against
a running checkout system. The optional R8 binary selector is verified by its
independent deletion-safe suite and is deliberately not forced into this
non-binary incident. This is a real service world, not a transcript fixture:
requests cross HTTP service
boundaries, state changes in the services, and an independent probe judges the
result.

```text
                         checkout traffic
                                |
                                v
                         +-------------+
                         |   gateway   |       edge domain
                         +------+------+
                                |
                         selected region
                         +------+------+
                         |             |
                         v             v
                   +-----------+ +-----------+
                   | payment E | | payment W |   payment domain
                   +-----+-----+ +-----+-----+
                         |             |
                         v             v
                   +-----------+ +-----------+
                   | callback E| | callback W|   platform domain
                   +-----+-----+ +-----+-----+
                         |             |
                         +------+------+
                                v
                          +-----------+
                          |  ledger   |          data domain
                          +-----------+

                     +----------------+
                     | SLO monitor    |          lead domain
                     +----------------+
```

## Why the domains are separate

The five workspaces model teams that already exist for ordinary operational
reasons. Each has different local knowledge, credentials, and tools:

| Domain | Knows and owns |
| --- | --- |
| `lead` | End-to-end symptoms and the incident outcome; no service mutation |
| `edge` | Gateway routing, counters, and a bounded read-only request/receipt history |
| `payment` | Payment behavior and bounded payment configuration |
| `platform` | Callback delivery behavior and bounded callback configuration |
| `data` | Ledger captures and explicitly audited void operations |

Service administration is available only on the owning domain network. A
shared mnemond network carries governed collaboration Events; it does not grant
service credentials or merge the five Runtime contexts.

## What is deliberately not scripted

The service faults are created outside the Agent workspaces. Episode 1 starts
with the East path affected. After independent recovery checks pass, the runner
records a pre-consolidation sequence, offers one neutral attention opportunity,
then captures only References created or updated in that interval. It restores
the East path to a healthy baseline and injects the same fault family into the
West path. The second fault is not projected into any Agent workspace. The domain
documents describe stable architecture and authority only. They do not reveal
either active incident, prescribe a diagnosis, name an Event choreography,
choose which expert to contact first, or require one repair path.

Agents receive normal attention opportunities and may inspect their own domain,
change state within their own authority, or use whatever opaque Event labels
and collaboration structure fit the evidence. A remote Event is still a
candidate at the receiving mnemond; it is never remote authority.

Each episode passes on the same external outcomes: historical customer receipts
still point to one active capture, extra captures remain as explicit void audit
records, and two fresh evaluation batches complete without duplicate active
captures. Accepted collaboration effects retain their R7 causality and receipt.
Pi runs with a fresh process and no session for every attention opportunity,
while the five mnemond authority stores and their References survive both
episodes. Before Episode 2, all five Runtime containers and writable workspaces
are replaced; only the immutable domain projection and captured mnemond
authority/CAS are restored. After Episode 1 passes its external outcome oracle, every domain gets
one additional neutral attention opportunity. It receives no diagnosis or
instruction to publish; it can only re-observe its own now-updated service and
decide whether anything is worth retaining before the authority boundary is
captured.

Every Runtime prompt also states the generic attention contract: one
opportunity does not own the whole workflow, may commit at most one accepted
contribution, and should stop so later turns can continue. This is a resource
boundary, not case choreography or a completion condition.

The evolution oracle is structural rather than semantic. After Episode 1's
external recovery oracle passes, at least one node must create or update an
active Reference during the neutral post-outcome attention interval. After
Episode 2, at least one accepted Event must explicitly cite that exact head or
supersede or retract it. The oracle does not inspect Reference bytes, Event
kind, diagnosis wording, peer order, or repair configuration. It proves that a
post-outcome Reference was explicitly linked to a later accepted Event; it does
not claim that the Reference improved the later diagnosis or recovery.

The gateway retains only its 32 most recent completed request observations.
This edge-owned surface records the business ID, selected route, outcome, and
the capture ID actually returned to the caller. It does not expose downstream
attempts, infer a root cause, prescribe a repair, or grant ledger authority.

## Fixture boundary

Files under `domains/` are projected into the corresponding Agent workspaces.
They may teach the Agent how to observe and safely operate its own domain. They
must remain independent of the incident seed. Removing these instructions must
not change mnemond Core, Event physics, peer delivery, or R8 selection logic.
