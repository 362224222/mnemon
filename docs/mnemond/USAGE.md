# mnemond Agency R7 Usage

Build from the repository root:

```sh
go build -o mnemon .
```

## Setup

Pi is the only T0 Runtime adapter:

```sh
mnemon agency setup --runtime pi --project-root /absolute/project
```

`--project-root` may be omitted when the current directory is the project.
Setup is convergent: it provisions one local node, ensures mnemond, and
installs the exact Pi Hook and guide revision. Runtime model/provider settings
and secrets remain outside Mnemon Agency state.

## Peer setup

Prepare a durable transport identity and listening configuration:

```sh
mnemon agency peer prepare \
  --listen 0.0.0.0:7447 \
  --advertise agent-a.example:7447 \
  --project-root /absolute/project > agent-a.card.json
```

The card is public setup material. Enroll a card received through an
owner-chosen channel under a local alias:

```sh
mnemon agency peer enroll \
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
mnemon agency hook attach --json
mnemon agency agent current --json
mnemon agency artifact capture --json < artifact.txt
mnemon agency artifact read <view-offered-handle>
mnemon agency agent submit --json < intent.json
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
mnemon agency serve --state-dir /absolute/project/.mnemon/agency
```

The `mnemond` daemon role never provisions a blank node. One daemon owns the
local SQLite writer, CAS, control socket, and optional peer workers. It is
started by the same physical `mnemon` executable in `agency serve` mode.

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
make build
make test
```

For race, process, and Docker boundaries:

```sh
make test-integration
```

Regular CI runs only `make test`; the integration command is an explicit local
or manually dispatched boundary check. Paid Pi/DeepSeek evaluation uses
`make test-live`. Product boundary suites live under `test/mnemond`, with
data-only fixtures under `testdata/mnemond`. See
[core-contract.md](core-contract.md) for normative behavior.
