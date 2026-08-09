# mnemond Agency R7

`mnemond` is Mnemon's project-local Agency daemon and protocol role. It is not a
second executable: users and Runtime projections reach it through
`mnemon agency ...`. R7 is Pi-first and owns one durable authority below
`.mnemon/agency`. Existing `mnemon` Memory commands remain at the root with
unchanged behavior.

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
mnemon agency setup --runtime pi --project-root .
```

Setup provisions the local node, ensures one mnemond daemon role, and installs
the project-local Pi Hook and `mnemond` guide. Normal Pi work then uses that
Hook; the user does not manually operate governance commands.

Peer federation is optional. Operators explicitly prepare a listening node,
exchange public Peer Cards, and enroll stable aliases. A remote delivery is a
candidate at the receiving node, not imported truth.

## Boundaries

R7 is not an Agent Runtime, scheduler, workflow engine, Channel service,
Teamwork registry, semantic schema loader, or global truth store. Event `kind`
and first-publish Reference keys are bounded open labels; machine consequences
remain a closed set enforced by local admission.

The single executable does not merge the two authorities: `mnemon setup` and
Legacy Memory remain independent from Agency state and lifecycle. Pi provider
and model configuration, including credentials, remain Pi-owned and must not
enter Agency state, Events, logs, or evidence.

Start with [QUICKSTART.md](QUICKSTART.md), use [USAGE.md](USAGE.md) as the
command reference, and treat the [R7 Core contract](core-contract.md) as the
sole active Agency protocol authority.
