# Mnemon Agency

**English** | [中文](zh/AGENCY.md)

Mnemon Agency gives an existing agent durable, project-local responsibility and
effect admission. It records what the agent is responsible for, admits only
valid proposed changes, and returns a durable result for each proposal.

Agency is part of the single `mnemon` executable:

```sh
mnemon agency --version
```

It is available on macOS and Linux and is currently Pi-first. `mnemond` names
the local daemon and protocol role; it is not a second executable or a command
users need to start during normal use.

## Where Agency fits

| Component | Responsibility | User surface |
|---|---|---|
| Memory | Persistent knowledge, linking, and recall across sessions | Root commands such as `mnemon remember` and `mnemon recall` |
| Agency | Project-local responsibility, admitted effects, receipts, and optional peer exchange | `mnemon agency ...` |
| Agent Runtime | Model execution, planning, tools, provider configuration, and credentials | Pi |

These boundaries are deliberate. Agency does not replace Memory, run the model,
plan work, or choose providers. Memory and Agency also keep separate state and
lifecycle: Agency state lives under the project's `.mnemon/agency` directory.

## Set up one project

[Install Mnemon](../README.md#install), enter the physical project directory,
and run setup once. If you plan to connect peers, complete
[peer preparation and enrollment](#add-optional-peers) before this command.

```sh
cd /path/to/project
mnemon agency setup --runtime pi --project-root .
```

`--project-root` may be omitted when the current directory is the project.
Setup provisions the local Agency state, ensures the daemon role, and installs
the project-local Pi integration.

Then start or reload Pi and use it normally. The installed integration presents
Agency state at eligible turn boundaries and teaches the agent how to respond.
There is no per-task Agency command for the user, and Pi continues to own model,
provider, and credential configuration.

## Add optional peers

Peer exchange is explicit and pairwise. Configure it before the final
`mnemon agency setup`, while each node is offline.

On each node, prepare a stable identity and network address. The command writes
a public Peer Card:

```sh
# Node A
mnemon agency peer prepare \
  --listen 0.0.0.0:7447 \
  --advertise node-a.example:7447 \
  --project-root /work/a > node-a.card.json

# Node B
mnemon agency peer prepare \
  --listen 0.0.0.0:7447 \
  --advertise node-b.example:7447 \
  --project-root /work/b > node-b.card.json
```

Exchange the cards through a channel you trust, then register a stable local
alias on each node:

```sh
mnemon agency peer enroll \
  --alias node-b --project-root /work/a < node-b.card.json

mnemon agency peer enroll \
  --alias node-a --project-root /work/b < node-a.card.json
```

Finish setup on both projects:

```sh
mnemon agency setup --runtime pi --project-root /work/a
mnemon agency setup --runtime pi --project-root /work/b
```

The advertised address must be reachable by the other node; `0.0.0.0` is only
suitable for listening. Agency has no anonymous discovery or transitive trust:
each peer is enrolled explicitly. The two nodes retain separate authority, so
received work is authenticated, verified, and admitted locally rather than
imported as truth.

## View → Intent → Receipt

The normative architecture is described in the
[mnemond protocol](mnemond/protocol.md). The protocol is intentionally smaller
than any built-in collaboration capability.

The installed Pi integration follows one small loop:

```text
View -> Intent -> Receipt -> View'
```

- **View** is a bounded snapshot of current responsibilities, available
  references, peer targets, and allowed changes.
- **Intent** is the agent's proposed structural change, formed only from the
  current View and any artifacts captured in that turn.
- **Receipt** records whether Agency accepted or rejected the proposal. An exact
  retry may return a replayed receipt without applying the effect twice.
- **View'** is a later View derived from admitted durable state.

Users normally do not write Intent JSON or invoke the hidden agent-facing
commands. Opaque handles and allowed changes belong to one View; the agent must
not guess them or carry them into another View.

## Safety and completion

Agency uses bounded inputs, local admission, durable receipts, authenticated
peer exchange, and hash-verified artifacts. Remote text and artifacts are still
untrusted content. Do not place provider credentials or other secrets in Agency
payloads or artifacts.

An accepted or replayed receipt is evidence of the recorded Agency effect; a
rejected receipt changes no authority. Peer delivery also does not transfer
authority: the receiving node decides what it admits.

Completion is intentionally strict. Only an accepted
`handling.resolve.completed` Intent backed by at least one locally available,
hash-verified artifact records successful completion. `declined` and
`unresolved` close responsibility without claiming success. Final text, process
exit, runtime idle, provider success, and network acknowledgement do not mark
work complete.

Agency protects its protocol and persistence boundary; it is not an operating
system sandbox. Project state is owner-private, but code running as the same OS
user may still access local files.
