# Mnemon Harness

Mnemon Harness is the experimental R5 implementation of mnemond-managed
Teamwork. It is intentionally isolated from the released root `mnemon` command
and its Memory behavior.

R5 is a clean-cut implementation. The source tree currently establishes the
two product binaries and their package boundaries; later commits add the
durable control, managed Agent, and peer-to-peer behavior behind those
boundaries.

The product binaries are:

- `mnemon-harness`: the project-local user and managed-Agent client.
- `mnemond`: the sole local Node, Event, Work, Handling, and Artifact
  authority.

Build and validate the current Harness layer with:

```sh
make harness-build
make harness-validate
go test ./harness/...
```

The root `mnemon` release path must not import or depend on this directory.
