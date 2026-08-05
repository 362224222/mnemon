# Mnemon Harness

Mnemon Harness is the experimental R7 implementation of mnemond: a local-first
authority for durable Agent events. It is intentionally isolated from the
released root `mnemon` command and its Memory behavior.

The Agent-facing model is deliberately small:

```text
View -> Intent -> Receipt -> View'
```

- `mnemon-harness` sets up the Pi integration and acts as the private Agent
  terminal.
- `mnemond` owns local admission, Events, Handlings, References, Artifacts, and
  Receipts.
- an optional peer link moves authenticated candidates between independently
  authoritative mnemond nodes; every receiver re-admits them locally.

Collaboration patterns are data-only descriptions and fixtures. The Core does
not contain a Channel model, Teamwork registry, workflow engine, or semantic
dispatch by Event kind.

Build and verify the Harness with:

```sh
make harness-build
make harness-validate
make harness-quality
make harness-verify
```

See [the Harness documentation](../docs/harness/README.md), the
[quickstart](../docs/harness/QUICKSTART.md), and the active
[R7 Core contract](../docs/harness/r7-core-contract.md).

Harness changes follow the repository
[Go Engineering Standard](../docs/development/go-engineering-standard.md).
The root `mnemon` release path must not import or depend on this directory.
