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
  hidden_answer_pattern='("route"[[:space:]]*:[[:space:]]*"east"[[:space:]]*}|--latency[[:space:]]+300ms|--timeout[[:space:]]+100ms|--stable-keys=false|incident-[ab0-9]|evaluation-[ab0-9]|stability-[ab0-9]|root[ -]?cause[[:space:]]+(is|=)|remediation[[:space:]]+(is|=)|fix[[:space:]]+by)'
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
  printf '%s\n' "$pi_source" | grep -F -- \
    '--extension /opt/mnemon/pi-delegate/delegate.ts' >/dev/null || {
    printf 'runtime oracle: domainops Pi lacks the bounded delegate extension\n' >&2
    exit 1
  }
  printf '%s\n' "$pi_source" | grep -F -- '--tools bash,delegate' >/dev/null || {
    printf 'runtime oracle: domainops Pi does not expose the exact bounded tool surface\n' >&2
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
  printf '%s\n' "$outcome_attention" >"$scratch/outcome-attention.md"
  projection_policy "$scratch/outcome-attention.md" || {
    printf 'runtime oracle: outcome attention contains a task answer or Event choreography\n' >&2
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

assert_generic_evolution_oracle() {
  local role database reference_digest event_json
  runtime_root="$scratch/evolution-runtime"
  authority_captured=1
  mkdir -p "$runtime_root/authority" "$runtime_root/evolution-boundary"
  reference_digest="sha256:$(printf 'a%.0s' {1..64})"
  for role in $roles; do
    mkdir -p "$runtime_root/authority/$role"
    database="$runtime_root/authority/$role/agency.db"
    sqlite3 "$database" 'CREATE TABLE events(origin_sequence INTEGER, canonical_json BLOB);'
    jq -n --arg role "$role" \
      '{role:$role,consolidation_after_sequence:0,max_origin_sequence:0,active_heads:[]}' \
      >"$runtime_root/evolution-boundary/$role.json"
  done
  jq -n --arg digest "$reference_digest" '
    {role:"lead",consolidation_after_sequence:0,max_origin_sequence:1,active_heads:[
      {event_id:"event:fixture-reference",event_digest:$digest}]}
  ' >"$runtime_root/evolution-boundary/lead.json"
  event_json=$(jq -cn --arg digest "$reference_digest" '
    {machine:{event_id:"event:fixture-use"},evidence:{causation:[
      {id:"event:fixture-reference",digest:$digest}]}}
  ')
  sqlite3 "$runtime_root/authority/lead/agency.db" <<SQL
.parameter init
.parameter set @event '$event_json'
INSERT INTO events(origin_sequence,canonical_json) VALUES(2,@event);
SQL
  assert_evolution
  test "$(cat "$runtime_root/evolution-effects.total")" = 1 || {
    printf 'runtime oracle: exact later Reference use was not counted\n' >&2
    exit 1
  }

  jq '.active_heads[0].event_digest =
    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"' \
    "$runtime_root/evolution-boundary/lead.json" \
    >"$runtime_root/evolution-boundary/lead-tampered.json"
  mv "$runtime_root/evolution-boundary/lead-tampered.json" \
    "$runtime_root/evolution-boundary/lead.json"
  if (assert_evolution >/dev/null 2>&1); then
    printf 'runtime oracle: non-exact later Reference use passed\n' >&2
    exit 1
  fi
  runtime_root=
  authority_captured=0
}

assert_generic_evolution_oracle

assert_failure_world_boundary() {
  local destination
  runtime_root="$scratch/failure-world-runtime"
  mkdir -p "$runtime_root"
  cat >"$runtime_root/episode-1-incident-after.json" <<'JSON'
{"role":"data","result":{"charges":8,"active_charges":8,"voided_charges":0,"unique_businesses":4,"duplicate_businesses":4,"ignored":"not retained"}}
JSON
  destination="$runtime_root/world.json"
  collect_failure_world "$destination"
  jq -e '
    . == [{episode:"episode-1",charges:8,active_charges:8,voided_charges:0,
      unique_businesses:4,duplicate_businesses:4}]
  ' "$destination" >/dev/null
  if grep -F 'ignored' "$destination" >/dev/null; then
    printf 'runtime oracle: bounded failure world retained an unapproved field\n' >&2
    exit 1
  fi

  cat >"$runtime_root/episode-2-incident-after.json" <<'JSON'
{"role":"data","result":{"charges":8,"active_charges":7,"voided_charges":0,"unique_businesses":4,"duplicate_businesses":1}}
JSON
  if collect_failure_world "$runtime_root/invalid-world.json" >/dev/null 2>&1; then
    printf 'runtime oracle: inconsistent failure world counts were accepted\n' >&2
    exit 1
  fi
  runtime_root=
}

assert_failure_world_boundary

assert_exclusive_turn_window() {
  runtime_root="$scratch/turn-window-runtime"
  mkdir -p "$runtime_root/turn-locks"
  claim_turn_window lead
  if claim_turn_window lead >/dev/null 2>&1; then
    printf 'runtime oracle: concurrent turns acquired the same node window\n' >&2
    exit 1
  fi
  release_turn_window lead
  claim_turn_window lead
  release_turn_window lead
  runtime_root=
}

assert_exclusive_turn_window

write_attention_snapshot() {
  local output=$1 data_unseen=$2 platform_unseen=$3 active=${4:-0} role unseen
  : >"$output.jsonl"
  for role in $roles; do
    unseen=0
    test "$role" != data || unseen=$data_unseen
    test "$role" != platform || unseen=$platform_unseen
    jq -cn --arg role "$role" --argjson unseen "$unseen" --argjson active "$active" \
      '{role:$role,unseen_open:$unseen,active_claims:$active}' >>"$output.jsonl"
  done
  jq -s '.' "$output.jsonl" >"$output"
}

assert_first_attention_boundary() {
  local snapshot counter wave
  runtime_root="$scratch/attention-runtime"
  mkdir -p "$runtime_root/turns"
  snapshot="$runtime_root/targeting.json"
  write_attention_snapshot "$snapshot" 2 1

  run_turn() {
    local role=$1 prompt=$2 tag=$3
    test "$prompt" = "$neutral_attention"
    : >"$runtime_root/turns/$tag"
  }
  wait_for_peer_delivery_quiescence() {
    printf '%s\n' "$1" >>"$runtime_root/barriers"
  }
  run_first_attention_wave episode-test 1 "$snapshot"
  test -f "$runtime_root/turns/episode-test-attention-debt-1-data"
  test -f "$runtime_root/turns/episode-test-attention-debt-1-platform"
  test "$(find "$runtime_root/turns" -type f | wc -l | tr -d '[:space:]')" = 2
  test "$(cat "$runtime_root/barriers")" = episode-test-attention-debt-1

  rm -rf -- "$runtime_root/turns"
  mkdir -p "$runtime_root/turns"
  : >"$runtime_root/barriers"
  counter="$runtime_root/snapshot-counter"
  printf '0\n' >"$counter"
  snapshot_first_attention_debt() {
    local episode=$1 requested_wave=$2 index output
    index=$(cat "$counter")
    index=$((index + 1))
    printf '%s\n' "$index" >"$counter"
    output="$runtime_root/generated-$episode-$requested_wave.json"
    case "$index" in
      1) write_attention_snapshot "$output" 1 0 ;;
      2) write_attention_snapshot "$output" 0 1 ;;
      *) write_attention_snapshot "$output" 0 0 ;;
    esac
    printf '%s\n' "$output"
  }
  first_attention_turn_limit=16
  settle_first_attention_debt episode-test
  jq -e '
    .episode == "episode-test" and .status == "settled" and
    .turn_limit == 16 and .turns_used == 2 and
    [.waves[].wave] == [1,2] and
    all(.final_nodes[]; .unseen_open == 0 and .active_claims == 0)
  ' "$runtime_root/first-attention/episode-test-settlement.json" >/dev/null
  test -f "$runtime_root/turns/episode-test-attention-debt-1-data"
  test -f "$runtime_root/turns/episode-test-attention-debt-2-platform"

  printf '0\n' >"$counter"
  first_attention_turn_limit=1
  snapshot_first_attention_debt() {
    local output="$runtime_root/exhausted-$2.json"
    write_attention_snapshot "$output" 1 1
    printf '%s\n' "$output"
  }
  if settle_first_attention_debt episode-budget >/dev/null 2>&1; then
    printf 'runtime oracle: unbounded first-attention debt was accepted\n' >&2
    exit 1
  fi
  test "$failure_stage" = scenario.episode-budget.attention-budget-exhausted
  jq -e '
    .episode == "episode-budget" and .status == "budget_exhausted" and
    .turn_limit == 1 and .turns_used == 0 and (.waves | length) == 0 and
    ([.final_nodes[] | select(.unseen_open > 0)] | length) == 2 and
    all(.final_nodes[]; .active_claims == 0)
  ' "$runtime_root/first-attention/episode-budget-budget-exhausted.json" >/dev/null

  first_attention_turn_limit=16
  snapshot_first_attention_debt() {
    local output="$runtime_root/occupied-$2.json"
    write_attention_snapshot "$output" 0 0 1
    printf '%s\n' "$output"
  }
  if settle_first_attention_debt episode-occupied >/dev/null 2>&1; then
    printf 'runtime oracle: a live claim was hidden by attention settlement\n' >&2
    exit 1
  fi
  test "$failure_stage" = scenario.episode-occupied.attention-boundary-open
  runtime_root=
}

(assert_first_attention_boundary)

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

write_domain_observation_stream() {
  local destination=$1
  jq -nc '{type:"message_start",message:{role:"custom",customType:"mnemond"}}' \
    >"$destination"
  jq -nc '{type:"tool_execution_start",toolCallId:"domain-status",toolName:"bash",
    args:{command:"domainctl --endpoint http://secret-endpoint-sentinel status"}}' >>"$destination"
  jq -nc '{type:"tool_execution_end",toolCallId:"domain-status",toolName:"bash",
    isError:false,result:{content:[{type:"text",
      text:"{\"role\":\"lead\",\"result\":{\"secret-response-sentinel\":true}}"}]}}' \
    >>"$destination"
  jq -nc '{type:"tool_execution_start",toolCallId:"domain-read",toolName:"bash",
    args:{command:"domainctl read /secret-path-sentinel"}}' >>"$destination"
  jq -nc '{type:"tool_execution_end",toolCallId:"domain-read",toolName:"bash",
    isError:true,result:{content:[{type:"text",text:"secret-read-error-sentinel"}]}}' \
    >>"$destination"
  jq -nc '{type:"tool_execution_start",toolCallId:"domain-probe",toolName:"bash",
    args:{command:"domainctl probe"}}' >>"$destination"
  jq -nc '{type:"tool_execution_end",toolCallId:"domain-probe",toolName:"bash",
    isError:false,result:{content:[{type:"text",
      text:"{\"role\":\"lead\",\"result\":{\"secret-probe-sentinel\":true}}"}]}}' \
    >>"$destination"
  jq -nc '{type:"tool_execution_start",toolCallId:"domain-wrong-role",toolName:"bash",
    args:{command:"domainctl probe"}}' >>"$destination"
  jq -nc '{type:"tool_execution_end",toolCallId:"domain-wrong-role",toolName:"bash",
    isError:false,result:{content:[{type:"text",
      text:"{\"role\":\"other\",\"result\":{\"wrong-role-sentinel\":true}}"}]}}' \
    >>"$destination"
  jq -nc '{type:"tool_execution_start",toolCallId:"domain-missing-error",toolName:"bash",
    args:{command:"domainctl probe"}}' >>"$destination"
  jq -nc '{type:"tool_execution_end",toolCallId:"domain-missing-error",toolName:"bash",
    result:{content:[{type:"text",
      text:"{\"role\":\"lead\",\"result\":{\"secret-untyped-sentinel\":true}}"}]}}' \
    >>"$destination"
  jq -nc '{type:"tool_execution_start",toolCallId:"domain-action",toolName:"bash",
    args:{command:"domainctl --endpoint=http://secret-endpoint-sentinel action /secret-action-sentinel '\''{\"secret-payload-sentinel\":true}'\''"}}' \
    >>"$destination"
  jq -nc '{type:"tool_execution_end",toolCallId:"domain-action",toolName:"bash",
    isError:false,result:{content:[{type:"text",
      text:"{\"role\":\"lead\",\"result\":{\"secret-action-result-sentinel\":true}}"}]}}' \
    >>"$destination"
  jq -nc '{type:"tool_execution_start",toolCallId:"domain-ambiguous",toolName:"bash",
    args:{command:"domainctl read /first; domainctl action /second '\''{}'\''"}}' \
    >>"$destination"
  jq -nc '{type:"tool_execution_end",toolCallId:"domain-ambiguous",toolName:"bash",
    isError:false,result:{content:[{type:"text",
      text:"{\"role\":\"lead\",\"result\":{\"ambiguous-result-sentinel\":true}}"}]}}' \
    >>"$destination"
  jq -nc '{type:"tool_execution_start",toolCallId:"domain-masked",toolName:"bash",
    args:{command:"domainctl read /masked-failure-sentinel || true"}}' >>"$destination"
  jq -nc '{type:"tool_execution_end",toolCallId:"domain-masked",toolName:"bash",
    isError:false,result:{content:[{type:"text",text:""}]}}' \
    >>"$destination"
  jq -nc '{type:"message_end",message:{role:"assistant",stopReason:"stop"}}' \
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
        result:{content:[{type:"text",text:$text}],details:{output:$text}}}' \
      >>"$destination"
  done
  jq -nc '{type:"message_end",message:{role:"assistant",stopReason:"stop"}}' \
    >>"$destination"
  jq -nc '{type:"agent_end"}' >>"$destination"
}

write_sequential_submit_stream() {
  local destination=$1 count=$2
  shift 2
  local command= index result combined= is_error=false
  for index in $(seq 1 "$count"); do
    command="${command:+$command; }mnemon-harness agent submit --json"
  done
  for result in "$@"; do
    combined="${combined:+$combined
}$result"
    case "$result" in *'"status":"error"'*) is_error=true ;; esac
  done
  test "$is_error" = false || combined="$combined"$'\n\nCommand exited with code 3'
  jq -nc '{type:"message_start",message:{role:"custom",customType:"mnemond"}}' \
    >"$destination"
  jq -nc --arg command "$command" \
    '{type:"tool_execution_start",toolCallId:"submit-batch",toolName:"bash",
      args:{command:$command}}' >>"$destination"
  jq -nc --arg text "$combined" --argjson is_error "$is_error" \
    '{type:"tool_execution_end",toolCallId:"submit-batch",toolName:"bash",
      isError:$is_error,result:{content:[{type:"text",text:$text}],details:{output:$text}}}' \
    >>"$destination"
  jq -nc '{type:"message_end",message:{role:"assistant",stopReason:"stop"}}' \
    >>"$destination"
  jq -nc '{type:"agent_end"}' >>"$destination"
}

write_sanitizer_stream stop "$scratch/stop.jsonl"
sanitize_turn lead oracle "$scratch/stop.jsonl" "$scratch/stop.json"
test "$(jq '.delegate_calls' "$scratch/stop.json")" = 0
write_domain_observation_stream "$scratch/domain-observations.jsonl"
sanitize_turn lead oracle-domain-observations "$scratch/domain-observations.jsonl" \
  "$scratch/domain-observations.json"
jq -e '
  .domain_operations == {
    read:{attempts:4,successes:1,tool_errors:1,invalid_results:1,
      batched_unattributed:1},
    probe:{attempts:3,successes:1,tool_errors:0,invalid_results:2,
      batched_unattributed:0},
    mutation:{attempts:2,successes:1,tool_errors:0,invalid_results:0,
      batched_unattributed:1}
  }
' "$scratch/domain-observations.json" >/dev/null
printf '%s\n' '[{"id":"event:before","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]' \
  >"$scratch/events-before.json"
printf '%s\n' '[{"id":"event:before","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},{"id":"event:new","digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}]' \
  >"$scratch/events-after.json"
printf '%s\n' '{"accepted_receipts":1}' >"$scratch/event-binding.json"
bind_turn_events "$scratch/events-before.json" "$scratch/events-after.json" \
  "$scratch/event-binding.json"
jq -e '.accepted_events == [{id:"event:new",digest:"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}]' \
  "$scratch/event-binding.json" >/dev/null
printf '%s\n' '{"accepted_receipts":0}' >"$scratch/event-binding-mismatch.json"
bind_turn_events "$scratch/events-before.json" "$scratch/events-after.json" \
  "$scratch/event-binding-mismatch.json"
jq -e '.accepted_receipts == 0 and (.accepted_events | length) == 1' \
  "$scratch/event-binding-mismatch.json" >/dev/null
printf '%s\n' '{"accepted_receipts":1}' >"$scratch/event-replay.json"
bind_turn_events "$scratch/events-before.json" "$scratch/events-before.json" \
  "$scratch/event-replay.json"
jq -e '.accepted_receipts == 1 and .accepted_events == []' \
  "$scratch/event-replay.json" >/dev/null
printf '%s\n' '[{"id":"event:before","digest":"sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"},{"id":"event:new","digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}]' \
  >"$scratch/events-drifted.json"
if bind_turn_events "$scratch/events-before.json" "$scratch/events-drifted.json" \
    "$scratch/event-binding-mismatch.json"; then
  printf 'runtime oracle: an accepted Event changed across a turn boundary\n' >&2
  exit 1
fi
printf '%s\n' '[{"id":"event:before","digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},{"id":"event:new","digest":"sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},{"id":"event:second","digest":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}]' \
  >"$scratch/events-too-many.json"
if bind_turn_events "$scratch/events-before.json" "$scratch/events-too-many.json" \
    "$scratch/event-binding-mismatch.json"; then
  printf 'runtime oracle: two accepted Events were attributed to one turn\n' >&2
  exit 1
fi
if grep -E 'secret-|ambiguous-result|wrong-role' "$scratch/domain-observations.json" >/dev/null; then
  printf 'runtime oracle: sanitized domain observation retained command or result content\n' >&2
  exit 1
fi
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

write_delegate_stream() {
  local destination=$1 status index=0 is_error
  shift
  jq -nc '{type:"message_start",message:{role:"custom",customType:"mnemond"}}' \
    >"$destination"
  for status in "$@"; do
    index=$((index + 1))
    is_error=true
    test "$status" != completed || is_error=false
    jq -nc --arg id "delegate-$index" \
      '{type:"tool_execution_start",toolCallId:$id,toolName:"delegate",
        args:{task:"bounded independent analysis"}}' >>"$destination"
    jq -nc --arg id "delegate-$index" --arg status "$status" \
      --argjson is_error "$is_error" \
      '{type:"tool_execution_end",toolCallId:$id,toolName:"delegate",isError:$is_error,
        result:{content:[{type:"text",text:"bounded observation"}],
          details:{schema:"mnemon.pi.delegate",version:1,status:$status}}}' \
      >>"$destination"
  done
  jq -nc '{type:"message_end",message:{role:"assistant",stopReason:"stop"}}' \
    >>"$destination"
  jq -nc '{type:"agent_end"}' >>"$destination"
}

write_delegate_stream "$scratch/delegate.jsonl" completed
sanitize_turn lead oracle-delegate "$scratch/delegate.jsonl" "$scratch/delegate.json"
test "$(jq '.delegate_calls' "$scratch/delegate.json")" = 1
write_delegate_stream "$scratch/contained-delegate.jsonl" completed slot_used
sanitize_turn lead oracle-contained-delegate "$scratch/contained-delegate.jsonl" \
  "$scratch/contained-delegate.json"
test "$(jq '.delegate_calls' "$scratch/contained-delegate.json")" = 1
write_delegate_stream "$scratch/two-delegates.jsonl" completed completed
if sanitize_turn lead oracle-two-delegates "$scratch/two-delegates.jsonl" \
    "$scratch/two-delegates.json"; then
  printf 'runtime oracle: two delegate calls in one turn were accepted\n' >&2
  exit 1
fi

accepted_receipt='{"schema":"mnemon.agent.receipt","version":1,"outcome":"accepted","replayed":false}'
rejected_receipt='{"schema":"mnemon.agent.receipt","version":1,"outcome":"rejected","replayed":false,"diagnostic":"bounded correction required"}'
closed_denial='{"code":"context_required","message":"a bounded View is required","operation_id":null,"replayed":false,"retryable":false,"schema_version":1,"status":"error"}'
write_submit_stream "$scratch/accounted.jsonl" "$accepted_receipt" "$closed_denial"
sanitize_turn lead oracle-accounted "$scratch/accounted.jsonl" "$scratch/accounted.json"
jq -e '
  .submit_attempts == 2 and .intent_submits == 1 and
  .accepted_receipts == 1 and .rejected_receipts == 0 and
  .submit_denials == 1 and .post_accept_denials == 1 and
  .submit_control_denials == [{code:"context_required",count:1}]
' "$scratch/accounted.json" >/dev/null
write_submit_stream "$scratch/multiline-submit.jsonl" "$accepted_receipt"
jq -c 'if .type == "tool_execution_start" then
  .args.command = "mnemon-harness artifact capture --json \u003c evidence.json\nmnemon-harness agent submit --json"
  else . end' "$scratch/multiline-submit.jsonl" >"$scratch/multiline-submit.tmp"
mv "$scratch/multiline-submit.tmp" "$scratch/multiline-submit.jsonl"
sanitize_turn lead oracle-multiline-submit "$scratch/multiline-submit.jsonl" \
  "$scratch/multiline-submit.json"
jq -e '.submit_attempts == 1 and .accepted_receipts == 1' \
  "$scratch/multiline-submit.json" >/dev/null
if grep -F 'a bounded View is required' "$scratch/accounted.json" >/dev/null; then
  printf 'runtime oracle: sanitized CLI denial retained diagnostic text\n' >&2
  exit 1
fi

write_submit_stream "$scratch/duplicate-rendering.jsonl" \
  "$closed_denial"$'\n'"$closed_denial"
sanitize_turn lead oracle-duplicate-rendering "$scratch/duplicate-rendering.jsonl" \
  "$scratch/duplicate-rendering.json"
jq -e '
  .submit_attempts == 1 and .intent_submits == 0 and .submit_denials == 1 and
  .submit_control_denials == [{code:"context_required",count:1}]
' "$scratch/duplicate-rendering.json" >/dev/null

write_sequential_submit_stream "$scratch/sequential-denials.jsonl" 3 \
  "$closed_denial" "$closed_denial" "$closed_denial"
sanitize_turn lead oracle-sequential-denials "$scratch/sequential-denials.jsonl" \
  "$scratch/sequential-denials.json"
jq -e '
  .bash_calls == 1 and .submit_attempts == 1 and .intent_submits == 0 and
  .submit_denials == 1 and .submit_invocation_failures == 0
' "$scratch/sequential-denials.json" >/dev/null

write_sequential_submit_stream "$scratch/sequential-rejections.jsonl" 3 \
  "$rejected_receipt" "$rejected_receipt" "$rejected_receipt"
sanitize_turn lead oracle-sequential-rejections "$scratch/sequential-rejections.jsonl" \
  "$scratch/sequential-rejections.json"
jq -e '
  .bash_calls == 1 and .submit_attempts == 1 and .intent_submits == 1 and
  .rejected_receipts == 1 and .submit_denials == 0 and
  .submit_invocation_failures == 0
' "$scratch/sequential-rejections.json" >/dev/null

write_sequential_submit_stream "$scratch/sequential-repair.jsonl" 3 \
  "$closed_denial" "$closed_denial" "$accepted_receipt"
sanitize_turn lead oracle-sequential-repair "$scratch/sequential-repair.jsonl" \
  "$scratch/sequential-repair.json"
jq -e '
  .bash_calls == 1 and .submit_attempts == 1 and .intent_submits == 1 and
  .accepted_receipts == 1 and .submit_denials == 0 and
  .submit_invocation_failures == 0
' "$scratch/sequential-repair.json" >/dev/null

write_submit_stream "$scratch/repaired-operation.jsonl" \
  "$closed_denial"$'\n'"$accepted_receipt"
sanitize_turn lead oracle-repaired-operation "$scratch/repaired-operation.jsonl" \
  "$scratch/repaired-operation.json"
jq -e '
  .submit_attempts == 1 and .intent_submits == 1 and
  .accepted_receipts == 1 and .rejected_receipts == 0 and .submit_denials == 0
' "$scratch/repaired-operation.json" >/dev/null

contained=("$accepted_receipt")
for _ in $(seq 1 13); do contained+=("$closed_denial"); done
write_submit_stream "$scratch/contained.jsonl" "${contained[@]}"
sanitize_turn lead oracle-contained "$scratch/contained.jsonl" "$scratch/contained.json"
jq -e '
  .submit_attempts == 14 and .intent_submits == 1 and
  .accepted_receipts == 1 and .rejected_receipts == 0 and
  .submit_denials == 13 and .post_accept_denials == 13 and
  .submit_control_denials == [{code:"context_required",count:13}]
' "$scratch/contained.json" >/dev/null

write_submit_stream "$scratch/unaccounted.jsonl" 'not a closed protocol result'
if sanitize_turn lead oracle-unaccounted "$scratch/unaccounted.jsonl" \
    "$scratch/unaccounted.json"; then
  printf 'runtime oracle: unaccounted submit attempt was accepted\n' >&2
  exit 1
fi

write_submit_stream "$scratch/local-failure.jsonl" '{"status":"error"}'
sanitize_turn lead oracle-local-failure "$scratch/local-failure.jsonl" \
  "$scratch/local-failure.json"
jq -e '
  .submit_attempts == 1 and .intent_submits == 0 and
  .submit_denials == 0 and .submit_invocation_failures == 1
' "$scratch/local-failure.json" >/dev/null

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
