# Mnemon Harness Usage

These commands assume you built:

```sh
go build -o mnemon .
go build -o mnemon-harness ./harness/cmd/mnemon-harness
```

## 1. Install Agent Integration

Install Agent Integration into the current project:

```sh
./mnemon-harness setup --host codex --project-root .
```

Use `--dry-run` to preview file changes:

```sh
./mnemon-harness setup --host codex --project-root . --dry-run
```

## 2. Run Local Mnemon

Start the local service used by the host integration:

```sh
./mnemon-harness local run
```

Inspect local state:

```sh
./mnemon-harness local status
./mnemon-harness status
```

## 3. Remote Workspace Sync

Connect a Remote Workspace:

```sh
./mnemon-harness sync connect my-workspace
```

Run one push or pull:

```sh
./mnemon-harness sync push --once
./mnemon-harness sync pull --once
```

Run background sync:

```sh
./mnemon-harness sync run --background
```

## 4. Validate Declarations

Repository maintainers can validate harness loop, host, and binding manifests:

```sh
make harness-validate
```

This is a development check, not part of the normal user workflow.

## Trust model — a governance contract, not a sandbox

The local boundary is enforced by protocol and engineering gates (identity stamping, scope
clamping, fail-closed config, durable audit), **not** by OS-level isolation: a malicious process
running as the same user can read the local files. What each tier actually promises:

- **T0 (always):** the governance contract — the wire admits only observations, the kernel is the
  sole writer, every decision is attributable.
- **T1 (current):** local hardening — the private state tree (`.mnemon/harness`, its `local`/
  `channel` dirs and both credentials dirs) is owner-only (0700, corrected on every setup rerun);
  tokens are 0600; `local run` refuses non-loopback listen addresses unless you pass
  `--allow-nonloopback` explicitly; `mnemon-harness token rotate --principal <p>` force-rotates a
  bearer token (revocation = rotation — tokens load at boot, so restart `local run` to apply).
- **T2 (remote phase):** authn/authz, transport encryption and audit are admission conditions for
  the remote coordination plane, not afterthoughts.
- **T3 (ecosystem phase):** signature chains and sandboxed rules.

OS/process-level isolation is explicitly **outside** the T0/T1 promise.
