# Mnemon Harness R7 Usage

Build from the repository root:

```sh
go -C harness build -o ../mnemon-harness ./cmd/mnemon-harness
go -C harness build -o ../mnemond ./cmd/mnemond
```

## Setup

Pi is the only T0 Runtime adapter:

```sh
mnemon-harness setup --runtime pi --project-root /absolute/project
```

`--project-root` may be omitted when the current directory is the project.
Setup is convergent: it provisions one local node, ensures mnemond, and
installs the exact Pi Hook and guide revision. Runtime model/provider settings
and secrets remain outside Mnemon Harness.

## Peer setup

Prepare a durable transport identity and listening configuration:

```sh
mnemon-harness peer prepare \
  --listen 0.0.0.0:7447 \
  --advertise agent-a.example:7447 \
  --project-root /absolute/project > agent-a.card.json
```

The card is public setup material. Enroll a card received through an
owner-chosen channel under a local alias:

```sh
mnemon-harness peer enroll \
  --alias agent-b \
  --project-root /absolute/project < agent-b.card.json
```

Peer discovery, transitive trust, global membership, and automatic convergence
are not part of R7. Every pair is enrolled explicitly. The advertised address
must be reachable from the peer; `0.0.0.0` is suitable for listening, not for
advertising.

## Agent terminal

These commands are used by the installed Pi Hook and guide. They are hidden
from ordinary help because the normal user workflow does not invoke them:

```sh
mnemon-harness hook attach --json
mnemon-harness agent current --json
mnemon-harness artifact capture --json < artifact.txt
mnemon-harness artifact read <view-offered-handle>
mnemon-harness agent submit --json < intent.json
```

The sequence is strict:

1. the Hook establishes one eligible attachment;
2. `agent current` returns a bounded View and privately binds its authority;
3. the Agent may capture bounded Artifact candidates and submit one offered
   structural Intent;
4. the returned Receipt says `accepted` or `rejected`; an exact retry may be
   marked `replayed` without a second effect.

Opaque handles, operation keys, attachment material, Principal identity,
fences, digests, and accepted state are machine-owned. The Agent must not guess
or carry them between Views. An Event `kind` is an open bounded semantic label;
it does not select code or register a workflow.

## Daemon

Setup normally owns daemon readiness. Supervisors may serve an already
provisioned node directly:

```sh
mnemond serve --state-dir /absolute/project/.mnemon/harness/node
```

`mnemond` never provisions a blank node. One daemon owns the local SQLite
writer, CAS, control socket, and optional peer workers.

## Completion and trust

Only an explicit accepted `handling.resolve.completed` Intent with at least one
locally available, hash-verified Artifact projects completion. `declined` and
`unresolved` close responsibility without claiming success. Final text,
process exit, Runtime idle, provider success, and network acknowledgements do
not complete work.

R7 protects the authority boundary; it is not an OS sandbox. T0 owner-private
files exclude other OS users but do not distinguish the installed Hook from
arbitrary same-UID code with local shell and file access.

## Development gates

```sh
make harness-build
make test
```

For race, process, and Docker boundaries:

```sh
make test-integration
```

Paid Pi/DeepSeek evaluation uses `make test-live`. See
[r7-core-contract.md](r7-core-contract.md) for normative behavior and
[r7-module-layout.md](r7-module-layout.md) for the enforced package boundary.
