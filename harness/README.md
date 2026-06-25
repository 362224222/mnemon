# Mnemon Harness

`mnemon-harness` is an experimental Agent Integration layer for connecting host
agents to Local Mnemon.

The current product surface is intentionally small:

- `setup` installs Agent Integration shim assets into Codex or Claude Code.
- `local run` starts the project-local Mnemon service.
- `status` reports Agent Integration, Local Mnemon, and sync status.
- `sync` connects Local Mnemon to a Remote Workspace and pushes/pulls governed
  commits with attribution preserved. The first-party backend is `mnemon-hub`;
  the experimental GitHub backend uses repo-mediated publication branches.
- `loop validate` remains hidden and is used by `make harness-validate`.

Host directories such as `.codex` and `.claude` are projection surfaces. Runtime
state is under `.mnemon/harness/`, and release-path Mnemon behavior stays under
`cmd/` and `internal/`.

## Build

From the repository root:

```sh
go build -o mnemon .
go build -o mnemon-harness ./harness/cmd/mnemon-harness
```

Validate harness declarations:

```sh
make harness-validate
```

## Try The Harness

Install Agent Integration for a host:

```sh
./mnemon-harness setup --host codex --project-root .
./mnemon-harness local run
./mnemon-harness status
```

Remove projected assets for a principal:

```sh
./mnemon-harness setup uninstall --host codex --principal codex@project --project-root .
```

More command examples are in `docs/harness/USAGE.md`.

## Experimental GitHub Remote Workspace

GitHub can be used as a bootstrap Remote Workspace backend for a decentralized
publication mesh. Each Local Mnemon publishes accepted synced events to its own
configured branch and subscribes to explicitly configured peer branches.

This is not P2P networking or GitHub Issues/PR-based teamwork. GitHub is only the
publication substrate; every imported event still enters through the receiving
Local Mnemon's Event Intake.

Example shape:

```sh
./mnemon-harness sync connect self \
  --backend github \
  --direction publish \
  --github-repo mnemon-dev/mnemon-teamwork-example \
  --github-branch mnemon/agent-a \
  --token-file ~/.config/mnemon/github.token

./mnemon-harness sync connect agent-b \
  --backend github \
  --direction subscribe \
  --github-repo mnemon-dev/mnemon-teamwork-example \
  --github-branch mnemon/agent-b \
  --token-file ~/.config/mnemon/github.token
```

Live validation is opt-in:

```sh
MNEMON_GITHUB_LIVE=1 \
MNEMON_GITHUB_REPO=mnemon-dev/mnemon-teamwork-example \
MNEMON_GITHUB_TOKEN_FILE=~/.config/mnemon/github.token \
go test ./harness/internal/app -run TestGitHubLivePublishPullImport -count=1 -v
```
