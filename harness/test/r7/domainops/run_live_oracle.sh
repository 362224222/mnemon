#!/usr/bin/env bash

# Deterministic, zero-provider regression oracle for run_live.sh's Runtime
# boundary. It exercises only stream filtering, terminal validation, pipeline
# error propagation, and complete local process-group timeout.

set -euo pipefail
umask 077

oracle_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
# shellcheck source=run_live.sh
source "$oracle_dir/run_live.sh"

scratch=$(mktemp -d /tmp/mnr7-runtime-oracle.XXXXXX)
cleanup_oracle() {
  chmod -R u+w "$scratch" >/dev/null 2>&1 || true
  rm -rf -- "$scratch"
}
trap cleanup_oracle EXIT

stream_mode=valid
pi_process() {
  case "$stream_mode" in
    valid)
      printf '%s\n' \
        '{"type":"message_update","message":{"role":"assistant","content":"transient"}}' \
        '{"type":"tool_execution_update","toolCallId":"tool-1","progress":"transient"}' \
        '{"type":"message_end","message":{"role":"assistant","stopReason":"stop"}}' \
        '{"type":"agent_end"}'
      ;;
    malformed) printf '%s\n' '{not-json' ;;
    upstream)
      printf '%s\n' '{"type":"agent_end"}'
      return 17
      ;;
    *) return 18 ;;
  esac
}

stream_mode=valid
(bounded_pi_process unused oracle >"$scratch/filtered.jsonl")
test "$(wc -l <"$scratch/filtered.jsonl" | tr -d '[:space:]')" = 2
jq -s -e '
  length == 2 and
  .[0].type == "message_end" and .[0].message.stopReason == "stop" and
  .[1].type == "agent_end" and
  all(.[]; .type != "message_update" and .type != "tool_execution_update")
' "$scratch/filtered.jsonl" >/dev/null

stream_mode=malformed
if (bounded_pi_process unused oracle >"$scratch/malformed.jsonl" 2>/dev/null); then
  printf 'runtime oracle: malformed provider record was accepted\n' >&2
  exit 1
fi
stream_mode=upstream
if (bounded_pi_process unused oracle >"$scratch/upstream.jsonl" 2>/dev/null); then
  printf 'runtime oracle: upstream provider failure was hidden by jq\n' >&2
  exit 1
fi

write_sanitizer_stream() {
  local reason=$1 destination=$2
  jq -nc '{type:"message_start",message:{role:"custom",customType:"mnemond"}}' \
    >"$destination"
  jq -nc --arg reason "$reason" \
    '{type:"message_end",message:{role:"assistant",stopReason:$reason}}' \
    >>"$destination"
  jq -nc '{type:"agent_end"}' >>"$destination"
}

write_sanitizer_stream stop "$scratch/stop.jsonl"
sanitize_turn lead oracle "$scratch/stop.jsonl" "$scratch/stop.json"
for terminal_reason in error aborted; do
  write_sanitizer_stream "$terminal_reason" "$scratch/$terminal_reason.jsonl"
  if sanitize_turn lead oracle "$scratch/$terminal_reason.jsonl" \
      "$scratch/$terminal_reason.json"; then
    printf 'runtime oracle: terminal %s was accepted\n' "$terminal_reason" >&2
    exit 1
  fi
done

late_pipeline() {
  sh -c 'sleep 3; printf '\''{"type":"agent_end"}\n'\''; : >"$1"' late "$scratch/late" |
    jq -c .
}
if with_deadline 1 "$scratch/timeout" late_pipeline >"$scratch/timeout.jsonl" 2>/dev/null; then
  printf 'runtime oracle: timed pipeline returned success\n' >&2
  exit 1
else
  deadline_status=$?
fi
test "$deadline_status" = 124
test -f "$scratch/timeout"
sleep 3
test ! -e "$scratch/late"

printf 'r7 domain ops Runtime boundary oracle: PASS\n'
