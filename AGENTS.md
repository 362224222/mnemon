# Mnemon Agent Guidelines

## Development

- Build with `go build -o mnemon .`.
- Run `make test` for deterministic unit, architecture, fixture, and CLI tests.
- Run `make test-integration` for Harness race, process, and Docker boundaries.
- Run `make test-live` only when explicitly validating the paid Pi/DeepSeek
  scenarios.
- Treat `harness/` as an experimental, not-yet-released harness layer. Do not
  use it as an implementation dependency for release-path commands such as
  `mnemon setup`; formal integrations belong under `cmd/` and `internal/`.
- Treat `.claude/`, `.codex/`, `.openclaw/`, and similar host directories as
  local projection surfaces, not canonical project state.

## Go Engineering

- Read and follow [the Go engineering standard](docs/development/go-engineering-standard.md)
  before changing Go architecture, concurrency, durable state, or shared
  infrastructure.
- Use patterns, channels, generics, callbacks, and registries only when they
  reduce sources of truth, state combinations, or modification points. They are
  not usage quotas, and reducing `if` statements or total lines is not a goal.
- Keep authority, digest, fence, bounds, CAS cardinality, and fail-closed checks
  explicit. Every goroutine must have an owner, cancellation, bounded work, and
  a wait path.
- Preserve independent replay, crash, authorization, and race oracles while
  compressing fixtures and shared setup.

## Commit Discipline

- Prefer small, logical commits. Split unrelated work instead of committing a
  broad mixed diff.
- Keep tightly coupled changes together when splitting would leave either commit
  misleading or incomplete.
- Use the project style already present in history: a concise Conventional
  Commit title plus one or two focused body paragraphs, with bullets only when
  they improve scanning.
- Choose the commit type by the primary project effect:
  - `feat` for new developer-facing or harness capabilities.
  - `fix` for correctness repairs.
  - `test` for tests, eval scenarios, or fixtures that do not add a new
    reusable capability.
  - `docs` for documentation-only changes.
  - `refactor` for structure changes without intended behavior changes.
  - `chore` for repository hygiene and maintenance.
- Mention validation in the body when tests, evals, or manual checks are part of
  the work.
- Do not include agent attribution or co-author lines unless explicitly asked.
