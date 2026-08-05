#!/usr/bin/env bash

# Deterministic, zero-provider regression oracle for run_live.sh's Runtime
# boundary. It exercises the controlled role projection, stream filtering,
# terminal validation, pipeline error propagation, and complete local
# process-group timeout.

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

projection_policy() {
  local file=$1
  local protocol_pattern hidden_answer_pattern choreography_pattern
  protocol_pattern='("(kind|consequence|successors|alias|subject_handling|correlation_handle|reply_target)"[[:space:]]*:|handling\.(create|advance|resolve)|reference\.(publish|supersede|retract)|review\.|contract-net\.|blackboard\.)'
  hidden_answer_pattern='("route"[[:space:]]*:[[:space:]]*"east"[[:space:]]*}|--latency[[:space:]]+300ms|--timeout[[:space:]]+100ms|--stable-keys=false|callback-east|payment-east|incident-[0-9]|evaluation-[0-9]|stability-[0-9]|root[ -]?cause[[:space:]]+(is|=)|remediation[[:space:]]+(is|=)|fix[[:space:]]+by)'
  choreography_pattern='(first[[:space:]]+(ask|contact|send)|then[[:space:]]+(ask|contact|send)|send[[:space:]].*[[:space:]]to[[:space:]]+(lead|edge|payment|platform|data))'

  ! grep -Ein -- "$protocol_pattern|$hidden_answer_pattern|$choreography_pattern" "$file" \
    >/dev/null
}

assert_domain_projection_boundary() {
  local pi_source role source projected mode
  pi_source=$(declare -f pi_process)
  if printf '%s\n' "$pi_source" | grep -F -- '--no-context-files' >/dev/null; then
    printf 'runtime oracle: domainops Pi disables its role context projection\n' >&2
    exit 1
  fi
  printf '%s\n' "$pi_source" | grep -F -- 'docker exec -w /workspace' >/dev/null || {
    printf 'runtime oracle: domainops Pi does not start in the controlled workspace\n' >&2
    exit 1
  }

  runtime_root="$scratch/projection-runtime"
  mkdir -p "$runtime_root/workspaces"
  for role in $roles; do
    source="$case_root/domains/$role/AGENTS.md"
    projection_policy "$source" || {
      printf 'runtime oracle: %s role projection contains a task answer or Event choreography\n' \
        "$role" >&2
      exit 1
    }
    prepare_workspace "$role"
    projected="$runtime_root/workspaces/$role/AGENTS.md"
    test -f "$projected" && test ! -L "$projected" && cmp -s "$source" "$projected" || {
      printf 'runtime oracle: %s role projection is not the exact controlled input\n' "$role" >&2
      exit 1
    }
    if mode=$(stat -c '%a' "$projected" 2>/dev/null); then :; else
      mode=$(stat -f '%Lp' "$projected")
    fi
    test "$mode" = 444 || {
      printf 'runtime oracle: %s role projection mode = %s, want 444\n' "$role" "$mode" >&2
      exit 1
    }
  done
  test "$(find "$runtime_root/workspaces" -type f -print | wc -l | tr -d '[:space:]')" = 5 || {
    printf 'runtime oracle: controlled workspaces contain an unexpected projected file\n' >&2
    exit 1
  }
  projection_policy "$mission_file" || {
    printf 'runtime oracle: mission contains a task answer or Event choreography\n' >&2
    exit 1
  }

  printf '%s\n' '{"kind":"repair.force","consequence":"handling.create","successors":[{"alias":"data"}]}' \
    >"$scratch/forbidden-projection.md"
  if projection_policy "$scratch/forbidden-projection.md"; then
    printf 'runtime oracle: projection policy accepted an Event choreography fixture\n' >&2
    exit 1
  fi
  runtime_root=
}

assert_domain_projection_boundary

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

write_submit_stream() {
  local destination=$1
  shift
  local index=0 result is_error
  jq -nc '{type:"message_start",message:{role:"custom",customType:"mnemond"}}' \
    >"$destination"
  for result in "$@"; do
    index=$((index + 1))
    is_error=false
    case "$result" in
      *'"status":"error"'*)
        is_error=true
        result="$result"$'\n\nCommand exited with code 3'
        ;;
    esac
    jq -nc --arg id "submit-$index" \
      '{type:"tool_execution_start",toolCallId:$id,toolName:"bash",
        args:{command:"mnemon-harness agent submit --json"}}' >>"$destination"
    jq -nc --arg id "submit-$index" --arg text "$result" --argjson is_error "$is_error" \
      '{type:"tool_execution_end",toolCallId:$id,toolName:"bash",isError:$is_error,
        result:{content:[{type:"text",text:$text}]}}' >>"$destination"
  done
  jq -nc '{type:"message_end",message:{role:"assistant",stopReason:"stop"}}' \
    >>"$destination"
  jq -nc '{type:"agent_end"}' >>"$destination"
}

write_sanitizer_stream stop "$scratch/stop.jsonl"
sanitize_turn lead oracle "$scratch/stop.jsonl" "$scratch/stop.json"
partial=$(summarize_partial_turn "$scratch/stop.jsonl")
jq -e '
  .record_types.message_start == 1 and
  .record_types.message_end == 1 and
  .record_types.agent_end == 1 and
  .message_boundaries == [
    {"type":"message_start","role":"custom","custom_type":"mnemond"},
    {"type":"message_end","role":"assistant","custom_type":""}
  ] and
  .assistant_stop_reasons == ["stop"]
' <<<"$partial" >/dev/null
printf '%s' 'HTTP 503 provider unavailable' >"$scratch/provider.err"
provider_error=$(summarize_provider_stderr "$scratch/provider.err")
jq -e '
  .bytes == 29 and .unavailable == true and
  (.auth or .rate_limited or .balance or .invalid_request or .network | not)
' <<<"$provider_error" >/dev/null
for terminal_reason in error aborted; do
  write_sanitizer_stream "$terminal_reason" "$scratch/$terminal_reason.jsonl"
  if sanitize_turn lead oracle "$scratch/$terminal_reason.jsonl" \
      "$scratch/$terminal_reason.json"; then
    printf 'runtime oracle: terminal %s was accepted\n' "$terminal_reason" >&2
    exit 1
  fi
done

accepted_receipt='{"schema":"mnemon.agent.receipt","version":1,"outcome":"accepted","replayed":false}'
closed_denial='{"code":"context_required","message":"a bounded View is required","operation_id":null,"replayed":false,"retryable":false,"schema_version":1,"status":"error"}'
write_submit_stream "$scratch/accounted.jsonl" "$accepted_receipt" "$closed_denial"
sanitize_turn lead oracle-accounted "$scratch/accounted.jsonl" "$scratch/accounted.json"
jq -e '
  .submit_attempts == 2 and .intent_submits == 1 and
  .accepted_receipts == 1 and .rejected_receipts == 0 and
  .submit_denials == 1 and .post_accept_denials == 1
' "$scratch/accounted.json" >/dev/null

contained=("$accepted_receipt")
for _ in $(seq 1 13); do contained+=("$closed_denial"); done
write_submit_stream "$scratch/contained.jsonl" "${contained[@]}"
sanitize_turn lead oracle-contained "$scratch/contained.jsonl" "$scratch/contained.json"
jq -e '
  .submit_attempts == 14 and .intent_submits == 1 and
  .accepted_receipts == 1 and .rejected_receipts == 0 and
  .submit_denials == 13 and .post_accept_denials == 13
' "$scratch/contained.json" >/dev/null

write_submit_stream "$scratch/unaccounted.jsonl" 'not a closed protocol result'
if sanitize_turn lead oracle-unaccounted "$scratch/unaccounted.jsonl" \
    "$scratch/unaccounted.json"; then
  printf 'runtime oracle: unaccounted submit attempt was accepted\n' >&2
  exit 1
fi

write_submit_stream "$scratch/two-effects.jsonl" "$accepted_receipt" "$accepted_receipt"
if sanitize_turn lead oracle-two-effects "$scratch/two-effects.jsonl" \
    "$scratch/two-effects.json"; then
  printf 'runtime oracle: two accepted Effects in one turn were accepted\n' >&2
  exit 1
fi

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
