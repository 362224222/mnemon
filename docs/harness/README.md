# Mnemon Harness R7

`mnemon-harness` is an experimental, source-built interface to mnemond. R7 is
Pi-first and has no compatibility promise with earlier Harness revisions.
Stable Mnemon remains the separate Memory CLI.

## Product model

mnemond does not plan work or interpret open-ended Event kinds. It gives an
Agent a bounded world and owns whether a proposed effect becomes durable:

```text
View -> Intent -> Receipt -> View'
```

The local Core has only two mutable domain states:

- a **Handling** records responsibility that still needs action;
- an **Active Reference** records which Artifact-backed description is locally
  in force.

An accepted Intent creates one immutable Event. Artifact bytes live in the CAS;
Events carry verified references. A final answer, process exit, idle signal,
provider success, or transport acknowledgement is never completion.

## Product surface

The ordinary user runs one command per workspace:

```sh
mnemon-harness setup --runtime pi --project-root .
```

Setup provisions the local node, ensures one mnemond, and installs the
project-local Pi Hook and `mnemond` guide. Normal Pi work then uses that Hook;
the user does not manually operate governance commands.

Peer federation is optional. Operators explicitly prepare a listening node,
exchange public Peer Cards, and enroll stable aliases. A remote delivery is a
candidate at the receiving node, not imported truth.

## Boundaries

R7 is not an Agent Runtime, scheduler, workflow engine, Channel service,
Teamwork registry, semantic schema loader, or global truth store. Event `kind`
and first-publish Reference keys are bounded open labels; machine consequences
remain a closed set enforced by local admission.

The root `mnemon` binary, `mnemon setup`, and Legacy Memory do not depend on
Harness. Pi provider and model configuration, including credentials, remain
Pi-owned and must not enter Harness state, Events, logs, or evidence.

Start with [QUICKSTART.md](QUICKSTART.md), use [USAGE.md](USAGE.md) as the
command reference, and treat the [R7 Core contract](r7-core-contract.md) as the
sole active Harness authority.
