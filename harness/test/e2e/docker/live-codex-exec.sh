#!/bin/sh
set -eu

# The root bootstrap copied the read-only Compose secret into this container's
# private tmpfs Codex home and dropped privileges before any product action.
# Prove Codex recognizes it without emitting provider or account material.
test "${CODEX_HOME:-}" = /run/r5-auth/codex-home
test -r "$CODEX_HOME/auth.json"
test "$(stat -c '%u:%g:%a' "$CODEX_HOME/auth.json")" = 10001:10001:600
codex login status >/dev/null 2>&1
case "${R5_CODEX_MODEL:-}" in
    ''|*[!A-Za-z0-9._-]*) exit 2 ;;
esac
case "${R5_CODEX_REASONING_EFFORT:-}" in
    low|medium|high|xhigh) ;;
    *) exit 2 ;;
esac
exec codex exec --ephemeral --sandbox workspace-write --ask-for-approval never \
  --model "$R5_CODEX_MODEL" \
  --config "model_reasoning_effort=\"$R5_CODEX_REASONING_EFFORT\"" -
