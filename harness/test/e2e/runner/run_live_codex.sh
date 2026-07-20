#!/bin/sh
set -eu
[ "${LIVE_CODEX:-}" = 1 ] || {
    printf '%s\n' 'r5-e2e: LIVE_CODEX=1 is required' >&2
    exit 2
}
exec "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)/run_suite.sh" --runtime codex "$@"
