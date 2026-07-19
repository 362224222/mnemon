# Mnemon Harness

Mnemon Harness is the experimental R5 implementation of mnemond-managed
Teamwork. It is intentionally isolated from the released root `mnemon` command
and its Memory behavior.

R5 is a clean-cut implementation delivered in independently verified phases.
Its durable control, managed Agent, peer-to-peer, setup, and acceptance layers
remain isolated from the released root product throughout that work.

The product binaries are:

- `mnemon-harness`: the project-local user and managed-Agent client.
- `mnemond`: the sole local Node, Event, Work, Handling, and Artifact
  authority.

Build and validate the current Harness layer with:

```sh
make harness-build
make harness-validate
make harness-quality
make harness-verify
```

Harness changes follow the repository
[Go Engineering Standard](../docs/development/go-engineering-standard.md).
`make harness-validate` currently checks test pairing and managed assets; it is
the focused asset/layout gate. `make harness-quality` applies the tracked R5
quality ratchet, while `make harness-verify` composes the full local Harness
build, layout, quality, vet, and unit-test gate.

The root `mnemon` release path must not import or depend on this directory.
