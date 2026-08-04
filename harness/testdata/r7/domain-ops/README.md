# Federated Domain Operations Case

This fixture exercises R7 and R8 against a running checkout system. It is a
real service world, not a transcript fixture: requests cross HTTP service
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
| `edge` | Gateway routing and gateway observations |
| `payment` | Payment behavior and bounded payment configuration |
| `platform` | Callback delivery behavior and bounded callback configuration |
| `data` | Ledger captures and explicitly audited void operations |

Service administration is available only on the owning domain network. A
shared mnemond network carries governed collaboration Events; it does not grant
service credentials or merge the five Runtime contexts.

## What is deliberately not scripted

The initial service state is created outside the Agent workspaces. The domain
documents describe stable architecture and authority only. They do not reveal
the active incident, prescribe a diagnosis, name an Event choreography, choose
which expert to contact first, or require one repair path.

Agents receive normal attention opportunities and may inspect their own domain,
change state within their own authority, or use whatever opaque Event labels
and collaboration structure fit the evidence. A remote Event is still a
candidate at the receiving mnemond; it is never remote authority.

The case passes on external outcomes: historical customer receipts still point
to one active capture, extra captures remain as explicit void audit records,
two fresh evaluation batches complete without duplicate active captures, and
accepted collaboration effects retain their R7 causality and receipt. The
oracle does not inspect the wording of a diagnosis or demand a particular
configuration.

## Fixture boundary

Files under `domains/` are projected into the corresponding Agent workspaces.
They may teach the Agent how to observe and safely operate its own domain. They
must remain independent of the incident seed. Removing these instructions must
not change mnemond Core, Event physics, peer delivery, or R8 selection logic.
