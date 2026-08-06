#!/bin/sh

set -eu

test "$#" = 3
test "$1" = agent
test "$2" = current
test "$3" = --json
test -z "$(cat)"

if test "${MNEMON_CURRENT_RPC_MODE:-projected}" = failed; then
  exit 1
fi

printf '%s\n' '{"schema":"mnemon.agent.view","version":6,"view":"view:rpc-current","outstanding":{"open_total":0,"related_total":0,"related_projected":0,"truncated":false},"allowed_intents":[]}'
