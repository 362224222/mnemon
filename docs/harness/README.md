# Mnemon Harness Public Beta

`mnemon-harness` is an experimental beta for installing host-agent integration
assets and connecting them to a local Mnemon service.

Stable Mnemon remains the memory CLI. The harness is source-build only, has no
compatibility guarantee, and is currently scoped to Agent Integration, Local
Mnemon, standard event packages, and Remote Workspace sync.

## 1. Product Surface

The user-facing command surface is intentionally small:

- `setup`: install Agent Integration shim assets.
- `local`: run or inspect Local Mnemon.
- `status`: show Agent Integration, Local Mnemon, and Remote Workspace state.
- `sync`: connect Local Mnemon to a Remote Workspace. `mnemon-hub` is the
  first-party backend; GitHub publication branches are available as an
  experimental repo-mediated backend.

Other implementation commands are internal and are not part of the beta product
contract.

## 2. Current Scope

The R5 Core beta supports only the Codex projection. Projected host directories
such as `.codex/` and `.agents/` are generated surfaces. Local state lives under
`.mnemon/harness/`.

The current beta does not promise production readiness, automatic apply,
multi-agent governance, broad organization scope, or a general evaluation
runtime.

The GitHub Remote Workspace backend is experimental. It uses explicitly
configured publication branches and does not implement P2P discovery, GitHub
Issues, GitHub PRs, or GitHub Actions as teamwork semantics.

## 3. Separation From Stable Mnemon

`mnemon-harness` is built from `./harness/cmd/mnemon-harness`.

Stable `mnemon` behavior is unchanged unless a user explicitly opts into harness
event emission or runs `mnemon-harness` directly.

## 4. Try It

Build both binaries:

```sh
go build -o mnemon .
go build -o mnemon-harness ./harness/cmd/mnemon-harness
```

Install Agent Integration for a project:

```sh
./mnemon-harness setup --host codex --project-root .
./mnemon-harness local run
./mnemon-harness status
```

See [USAGE.md](USAGE.md) for command examples.

The first complete collaboration slice is governed by the tracked
[R5 Core contract](r5-core-contract.md).

## 5. Release Boundary

This beta intentionally ships minimal public documentation. Internal planning,
experimental command surfaces, generated site HTML, and future governance
experiments are not part of the product contract.
