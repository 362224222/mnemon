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

test "$(grep -Fxc -- "  \"$attention_exhausted_reason\";" \
  "$harness_root/internal/attach/assets/pi/mnemond.ts")" = 1 || {
  printf 'runtime oracle: Host attention disposition drifted from the observer\n' >&2
  exit 1
}
test "$(grep -Fxc -- "const CURRENT_FAILED_TEXT = \"$current_failed_reason\";" \
  "$harness_root/internal/attach/assets/pi/mnemond-current.ts")" = 1 || {
  printf 'runtime oracle: native Current failure disposition drifted from the observer\n' >&2
  exit 1
}
grep -Eq "^[[:space:]]*MonitorProbeLimit[[:space:]]*=[[:space:]]*$monitor_probe_limit$" \
  "$case_root/world/monitor.go" || {
  printf 'runtime oracle: shell and monitor probe bounds diverged\n' >&2
  exit 1
}
grep -Eq "^[[:space:]]*MonitorProbeChargeLimit[[:space:]]*=[[:space:]]*$monitor_probe_charge_limit$" \
  "$case_root/world/monitor.go" || {
  printf 'runtime oracle: shell and monitor per-probe charge bounds diverged\n' >&2
  exit 1
}
grep -Eq "^[[:space:]]*GatewayHistoryLimit[[:space:]]*=[[:space:]]*$gateway_history_limit$" \
  "$case_root/world/gateway.go" || {
  printf 'runtime oracle: shell and gateway history bounds diverged\n' >&2
  exit 1
}
grep -Fqx -- "const maxControlBytes = $domain_control_max_kib << 10" \
  "$case_root/cmd/domainctl/main.go" || {
  printf 'runtime oracle: shell and domainctl response bounds diverged\n' >&2
  exit 1
}
test "$synthetic_charge_limit" = $((monitor_probe_limit * monitor_probe_charge_limit)) || {
  printf 'runtime oracle: synthetic charge envelope is not derived from probe bounds\n' >&2
  exit 1
}
test "$max_agent_probe_count" = 35 && test "$max_goal_probe_count" = 34 &&
    test "$max_agent_probe_count" = $((scenario_episode_count *
      (open_attention_turn_limit + 1) * agent_probe_per_turn_limit +
      agent_probe_per_turn_limit)) &&
    test "$monitor_probe_limit" -ge $((max_agent_probe_count + max_goal_probe_count)) || {
  printf 'runtime oracle: probe budget does not cover the bounded attention schedule\n' >&2
  exit 1
}
test "$scenario_customer_receipt_limit" = 32 &&
    test "$gateway_history_limit" -ge \
      $((monitor_probe_limit + scenario_customer_receipt_limit)) || {
  printf 'runtime oracle: gateway history cannot retain scenario and probe receipts\n' >&2
  exit 1
}

write_trace_source=$(declare -f write_trace)
for required in '--consolidation-authority' '--boundary-authority'; do
  printf '%s\n' "$write_trace_source" | grep -F -- "$required" >/dev/null || {
    printf 'runtime oracle: trace adapter omits %s\n' "$required" >&2
    exit 1
  }
done
consolidation_source=$(declare -f capture_consolidation_start)
printf '%s\n' "$consolidation_source" |
  grep -F -- 'chmod -R a-w "$staging"' >/dev/null || {
  printf 'runtime oracle: consolidation does not freeze independent authority\n' >&2
  exit 1
}
if printf '%s\n' "$consolidation_source" |
    sed -n '/chmod -R a-w "$staging"/,$p' |
    grep -F -- 'rm -rf' >/dev/null; then
  printf 'runtime oracle: consolidation deletes its frozen authority\n' >&2
  exit 1
fi
restart_source=$(declare -f restart_agent_runtimes)
if printf '%s\n' "$restart_source" |
    grep -F -- 'rm -rf -- "$runtime_root/runtime-restart-state"' >/dev/null; then
  printf 'runtime oracle: restart deletes independent boundary authority\n' >&2
  exit 1
fi
printf '%s\n' "$restart_source" |
  grep -F -- 'chmod -R a-w "$runtime_root/runtime-restart-state"' >/dev/null || {
  printf 'runtime oracle: restart does not freeze independent boundary authority\n' >&2
  exit 1
}

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
  printf '%s\n' "$pi_source" | grep -F -- \
    '--tools bash,delegate,mnemond_current,mnemond_submit' >/dev/null || {
    printf 'runtime oracle: domainops Pi does not expose the exact bounded exploration and settlement surface\n' >&2
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
  assert_evolution
  if test "$(cat "$runtime_root/evolution-effects.total")" != 0; then
    printf 'runtime oracle: non-exact later Reference use was counted\n' >&2
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
  local output=$1 data_unclaimed=$2 platform_unclaimed=$3 occupied_role=${4:-}
  local occupied_value=${5:-0} role unclaimed occupied
  : >"$output.jsonl"
  for role in $roles; do
    unclaimed=0
    occupied=0
    test "$role" != data || unclaimed=$data_unclaimed
    test "$role" != platform || unclaimed=$platform_unclaimed
    test "$role" != "$occupied_role" || occupied=$occupied_value
    jq -cn --arg role "$role" --argjson unclaimed "$unclaimed" \
      --argjson occupied "$occupied" \
      '{role:$role,open_unclaimed:$unclaimed,occupied_claims:$occupied}' >>"$output.jsonl"
  done
  jq -s '.' "$output.jsonl" >"$output"
}

assert_open_attention_boundary() {
  local snapshot counter counts unclaimed occupied query_source wave_source driver_source
  local goal_source agents_source post_outcome_source
  runtime_root="$scratch/attention-runtime"
  mkdir -p "$runtime_root/turns"

  sqlite3 "$runtime_root/attention.db" '
    CREATE TABLE handlings(state TEXT NOT NULL, claim_fence INTEGER NOT NULL,
      claim_attachment_id TEXT);
    INSERT INTO handlings VALUES('\''open'\'',0,NULL);
    INSERT INTO handlings VALUES('\''open'\'',7,NULL);
    INSERT INTO handlings VALUES('\''open'\'',3,'\''attachment:occupied'\'');
    INSERT INTO handlings VALUES('\''terminal'\'',9,NULL);'
  counts=$(read_open_attention_counts "$runtime_root/attention.db")
  IFS='|' read -r unclaimed occupied <<EOF
$counts
EOF
  test "$unclaimed" = 2 && test "$occupied" = 1 || {
    printf 'runtime oracle: open attention omitted a previously claimed open Handling\n' >&2
    exit 1
  }
  query_source=$(declare -f read_open_attention_counts)
  printf '%s\n' "$query_source" | grep -F -- \
    "state = '\\''open'\\'' AND claim_attachment_id IS NULL" >/dev/null || {
    printf 'runtime oracle: open attention does not derive open-unclaimed work from authority occupancy\n' >&2
    exit 1
  }
  if printf '%s\n' "$query_source" | grep -Ei -- \
      'claim_fence|semantic|kind|payload|artifact|canonical_json|domainctl' >/dev/null; then
    printf 'runtime oracle: open attention inspects non-occupancy semantics\n' >&2
    exit 1
  fi
  wave_source=$(declare -f run_open_attention_wave)
  printf '%s\n' "$wave_source" | grep -F -- '.open_unclaimed > 0' >/dev/null || {
    printf 'runtime oracle: open attention wave does not use the open-unclaimed projection\n' >&2
    exit 1
  }
  if printf '%s\n' "$wave_source" | grep -Ei -- \
      'semantic|kind|payload|artifact|canonical_json|domainctl|gateway|ledger|payment' \
      >/dev/null; then
    printf 'runtime oracle: open attention wave inspects scenario semantics\n' >&2
    exit 1
  fi
  driver_source=$(declare -f drive_attention_until_outcome)
  if printf '%s\n' "$driver_source" | grep -Ei -- \
      'claim_fence|semantic|kind|payload|artifact|canonical_json|domainctl|gateway|ledger|payment' \
      >/dev/null; then
    printf 'runtime oracle: bounded attention driver inspects scenario semantics\n' >&2
    exit 1
  fi
  goal_source=$(declare -f observe_episode_goal)
  printf '%s\n' "$goal_source" | grep -F -- \
    'data-tool status "$incident_prefix"' >/dev/null || {
    printf 'runtime oracle: episode goal does not observe historical ledger integrity\n' >&2
    exit 1
  }
  printf '%s\n' "$goal_source" | grep -F -- 'lead-tool probe' >/dev/null || {
    printf 'runtime oracle: episode goal omits its bounded real canary\n' >&2
    exit 1
  }
  if printf '%s\n' "$goal_source" | grep -Ei -- \
      'domainctl|[[:space:]]action[[:space:]]|handling|reference|event|repair|remediat|latency|timeout|config' \
      >/dev/null; then
    printf 'runtime oracle: episode goal depends on Agent choreography or remediation\n' >&2
    exit 1
  fi
  agents_source=$(declare -f run_agents)
  test "$(printf '%s\n' "$agents_source" | grep -Fc -- 'run_turn lead')" = 1 &&
    ! printf '%s\n' "$agents_source" | grep -E -- 'for role|while .*round|run_open_attention_wave' \
      >/dev/null || {
    printf 'runtime oracle: initial Agent entry still contains fixed all-node rounds\n' >&2
    exit 1
  }
  post_outcome_source=$(declare -f run_post_outcome_attention)
  test "$(printf '%s\n' "$post_outcome_source" | grep -Fc -- 'run_turn lead')" = 1 &&
    ! printf '%s\n' "$post_outcome_source" | grep -E -- 'for role|run_open_attention_wave' \
      >/dev/null || {
    printf 'runtime oracle: post-outcome attention is not one lead opportunity\n' >&2
    exit 1
  }

  printf '0\n' >"$runtime_root/canary-calls"
  compose() {
    case " $* " in
      *' data-tool status '*) cat "$runtime_root/history-source.json" ;;
      *' lead-tool probe '*)
        local calls
        calls=$(cat "$runtime_root/canary-calls")
        printf '%s\n' $((calls + 1)) >"$runtime_root/canary-calls"
        cat "$runtime_root/canary-source.json"
        ;;
      *) return 1 ;;
    esac
  }

  # Historical failure is a complete false observation and never spends a
  # real canary from the shared bounded service.
  cat >"$runtime_root/history-source.json" <<'JSON'
{"role":"data","result":{"charges":8,"active_charges":8,"voided_charges":0,"unique_businesses":4,"duplicate_businesses":4}}
JSON
  : >"$runtime_root/canary-source.json"
  observe_episode_goal episode-goal 1 "$runtime_root/goal-history-false.json" incident-fixture
  jq -e '
    (keys | sort) == ["canary","episode","observed","satisfied","schema","version"] and
    .schema == "mnemon.r7.domain-ops.goal" and .version == 2 and
    .episode == "episode-goal" and .satisfied == false and .canary == null and
    .observed == {charges:8,active_charges:8,voided_charges:0,
      unique_businesses:4,duplicate_businesses:4}
  ' "$runtime_root/goal-history-false.json" >/dev/null
  test "$(cat "$runtime_root/canary-calls")" = 0
  test ! -e "$runtime_root/episode-goal-incident-after.json"

  # A repaired history is insufficient while the bounded real canary still
  # observes an unsafe customer path.
  cat >"$runtime_root/history-source.json" <<'JSON'
{"role":"data","result":{"charges":8,"active_charges":4,"voided_charges":4,"unique_businesses":4,"duplicate_businesses":0}}
JSON
  cat >"$runtime_root/canary-source.json" <<'JSON'
{"role":"lead","result":{"receipt":{"request_id":1,"business_id":"synthetic-001","capture_id":0,"route":"east","status":"failed"},"observed":{"charges":1,"active_charges":1,"voided_charges":0,"unique_businesses":1,"duplicate_businesses":0},"ledger":{"charges":1,"active_charges":0,"voided_charges":1,"unique_businesses":0,"duplicate_businesses":0}}}
JSON
  observe_episode_goal episode-goal 2 "$runtime_root/goal-canary-false.json" incident-fixture
  jq -e '
    .satisfied == false and .observed.active_charges == 4 and
    .canary == {receipt_status:"failed",capture_id_present:false,
      observed:{charges:1,active_charges:1,voided_charges:0,
        unique_businesses:1,duplicate_businesses:0},
      settled:{charges:1,active_charges:0,voided_charges:1,
        unique_businesses:0,duplicate_businesses:0}}
  ' "$runtime_root/goal-canary-false.json" >/dev/null
  test "$(cat "$runtime_root/canary-calls")" = 1

  # Only repaired history plus one clean real checkout closes the mission goal.
  cat >"$runtime_root/canary-source.json" <<'JSON'
{"role":"lead","result":{"receipt":{"request_id":2,"business_id":"synthetic-002","capture_id":9,"route":"west","status":"succeeded"},"observed":{"charges":1,"active_charges":1,"voided_charges":0,"unique_businesses":1,"duplicate_businesses":0},"ledger":{"charges":1,"active_charges":1,"voided_charges":0,"unique_businesses":1,"duplicate_businesses":0}}}
JSON
  observe_episode_goal episode-goal 3 "$runtime_root/goal-true.json" incident-fixture
  jq -e '
    .satisfied == true and .canary.receipt_status == "succeeded" and
    .canary.capture_id_present == true and
    .canary.observed == .canary.settled and
    .canary.settled == {charges:1,active_charges:1,voided_charges:0,
      unique_businesses:1,duplicate_businesses:0}
  ' "$runtime_root/goal-true.json" >/dev/null
  test "$(cat "$runtime_root/canary-calls")" = 2

  # The driver and final adapter share a closed predicate: an asserted true
  # value that contradicts its observation is rejected immediately.
  jq '.satisfied = false' "$runtime_root/goal-true.json" \
    >"$runtime_root/goal-invalid-projection.json"
  if validate_episode_goal episode-goal "$runtime_root/goal-invalid-projection.json" \
      >/dev/null 2>&1; then
    printf 'runtime oracle: contradictory goal projection was accepted\n' >&2
    exit 1
  fi
  cat >"$runtime_root/history-source.json" <<'JSON'
{"role":"data","result":{"charges":8,"active_charges":7,"voided_charges":0,"unique_businesses":4,"duplicate_businesses":1}}
JSON
  printf '%s\n' '{"sealed":"existing-final-incident-evidence"}' \
    >"$runtime_root/episode-goal-incident-after.json"
  cp "$runtime_root/episode-goal-incident-after.json" \
    "$runtime_root/episode-goal-incident-after.expected.json"
  if observe_episode_goal episode-goal 4 "$runtime_root/goal-invalid.json" \
      incident-fixture >/dev/null 2>&1; then
    printf 'runtime oracle: inconsistent historical counts were accepted\n' >&2
    exit 1
  fi
  test ! -e "$runtime_root/goal-invalid.json"
  cmp -s "$runtime_root/episode-goal-incident-after.expected.json" \
    "$runtime_root/episode-goal-incident-after.json" || {
    printf 'runtime oracle: invalid goal observation overwrote final incident evidence\n' >&2
    exit 1
  }
  test "$(find "$runtime_root" -maxdepth 1 -name '.episode-goal-goal-*' | wc -l | tr -d '[:space:]')" = 0

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
  run_open_attention_wave episode-test 1 "$snapshot"
  test -f "$runtime_root/turns/episode-test-open-attention-1-data"
  test -f "$runtime_root/turns/episode-test-open-attention-1-platform"
  test "$(find "$runtime_root/turns" -type f | wc -l | tr -d '[:space:]')" = 2
  test "$(cat "$runtime_root/barriers")" = episode-test-open-attention-1

  reset_attention_fixture() {
    rm -rf -- "$runtime_root/turns" "$runtime_root/open-attention"
    mkdir -p "$runtime_root/turns" "$runtime_root/open-attention"
    : >"$runtime_root/barriers"
  }
  write_goal_result() {
    local destination=$1 episode=$2 mode=$3
    case "$mode" in
      satisfied)
        jq -n --arg episode "$episode" '
          {schema:"mnemon.r7.domain-ops.goal",version:2,episode:$episode,
           satisfied:true,
           observed:{charges:8,active_charges:4,voided_charges:4,
             unique_businesses:4,duplicate_businesses:0},
           canary:{receipt_status:"succeeded",capture_id_present:true,
             observed:{charges:1,active_charges:1,voided_charges:0,
               unique_businesses:1,duplicate_businesses:0},
             settled:{charges:1,active_charges:1,voided_charges:0,
               unique_businesses:1,duplicate_businesses:0}}}
        ' >"$destination"
        ;;
      historical_failure)
        jq -n --arg episode "$episode" '
          {schema:"mnemon.r7.domain-ops.goal",version:2,episode:$episode,
           satisfied:false,
           observed:{charges:8,active_charges:8,voided_charges:0,
             unique_businesses:4,duplicate_businesses:4},canary:null}
        ' >"$destination"
        ;;
      canary_failure)
        jq -n --arg episode "$episode" '
          {schema:"mnemon.r7.domain-ops.goal",version:2,episode:$episode,
           satisfied:false,
           observed:{charges:8,active_charges:4,voided_charges:4,
             unique_businesses:4,duplicate_businesses:0},
           canary:{receipt_status:"failed",capture_id_present:false,
             observed:{charges:1,active_charges:1,voided_charges:0,
               unique_businesses:1,duplicate_businesses:0},
             settled:{charges:1,active_charges:0,voided_charges:1,
               unique_businesses:0,duplicate_businesses:0}}}
        ' >"$destination"
        ;;
      *) return 1 ;;
    esac
  }

  # A satisfied external goal ends attention immediately even when durable
  # responsibilities remain open.
  reset_attention_fixture
  snapshot_open_attention() {
    local output="$runtime_root/goal-first-$2.json"
    write_attention_snapshot "$output" 2 1
    printf '%s\n' "$output"
  }
  goal_probe() {
    write_goal_result "$3" "$1" satisfied
  }
  open_attention_turn_limit=16
  drive_attention_until_outcome episode-goal-first goal_probe
  jq -e '
    .episode == "episode-goal-first" and .status == "outcome_observed" and
    .turn_limit == 16 and .turns_used == 0 and (.waves | length) == 0 and
    .goal.satisfied == true and
    ([.final_nodes[] | select(.open_unclaimed > 0)] | length) == 2 and
    all(.final_nodes[]; .occupied_claims == 0)
  ' "$runtime_root/open-attention/episode-goal-first-settlement.json" >/dev/null
  test "$(find "$runtime_root/turns" -type f | wc -l | tr -d '[:space:]')" = 0
  test ! -s "$runtime_root/barriers"

  # An unsatisfied goal receives one eligible wave. A later satisfied goal
  # stops even if that collaboration produced more residual responsibilities.
  reset_attention_fixture
  counter="$runtime_root/goal-after-wave-counter"
  printf '0\n' >"$counter"
  snapshot_open_attention() {
    local index output="$runtime_root/goal-after-wave-$2.json"
    index=$(cat "$counter")
    if test "$index" = 0; then write_attention_snapshot "$output" 1 0
    else write_attention_snapshot "$output" 2 1; fi
    printf '%s\n' "$output"
  }
  goal_probe() {
    local index
    index=$(cat "$counter")
    index=$((index + 1))
    printf '%s\n' "$index" >"$counter"
    if test "$index" = 1; then write_goal_result "$3" "$1" historical_failure
    else write_goal_result "$3" "$1" satisfied; fi
  }
  drive_attention_until_outcome episode-goal-after-wave goal_probe
  jq -e '
    .status == "outcome_observed" and .turns_used == 1 and
    [.waves[].wave] == [1] and .goal.satisfied == true and
    ([.final_nodes[] | select(.open_unclaimed > 0)] | length) == 2
  ' "$runtime_root/open-attention/episode-goal-after-wave-settlement.json" >/dev/null
  test -f "$runtime_root/turns/episode-goal-after-wave-open-attention-1-data"
  test "$(cat "$runtime_root/barriers")" = episode-goal-after-wave-open-attention-1

  # No eligible attention cannot be mistaken for a successful outcome.
  reset_attention_fixture
  snapshot_open_attention() {
    local output="$runtime_root/no-eligible-$2.json"
    write_attention_snapshot "$output" 0 0
    printf '%s\n' "$output"
  }
  goal_probe() { write_goal_result "$3" "$1" historical_failure; }
  if drive_attention_until_outcome episode-no-eligible goal_probe >/dev/null 2>&1; then
    printf 'runtime oracle: goal-free quiescence was accepted as success\n' >&2
    exit 1
  fi
  test "$failure_stage" = scenario.episode-no-eligible.attention-quiescent-without-outcome
  jq -e '
    .status == "quiescent_without_outcome" and .turns_used == 0 and
    .goal.satisfied == false and all(.final_nodes[];
      .open_unclaimed == 0 and .occupied_claims == 0)
  ' "$runtime_root/open-attention/episode-no-eligible-quiescent-without-outcome.json" \
    >/dev/null

  # A false goal remains false after one bounded turn, then the resource
  # envelope fails closed before a second turn is issued.
  reset_attention_fixture
  open_attention_turn_limit=1
  snapshot_open_attention() {
    local output="$runtime_root/exhausted-$2.json"
    write_attention_snapshot "$output" 1 0
    printf '%s\n' "$output"
  }
  goal_probe() { write_goal_result "$3" "$1" historical_failure; }
  if drive_attention_until_outcome episode-budget goal_probe >/dev/null 2>&1; then
    printf 'runtime oracle: unbounded open attention was accepted\n' >&2
    exit 1
  fi
  test "$failure_stage" = scenario.episode-budget.attention-budget-exhausted-before-outcome
  jq -e '
    .episode == "episode-budget" and .status == "budget_exhausted_before_outcome" and
    .turn_limit == 1 and .turns_used == 1 and [.waves[].wave] == [1] and
    .goal.satisfied == false and
    ([.final_nodes[] | select(.open_unclaimed > 0)] | length) == 1 and
    all(.final_nodes[]; .occupied_claims == 0)
  ' "$runtime_root/open-attention/episode-budget-budget-exhausted-before-outcome.json" \
    >/dev/null
  test -f "$runtime_root/turns/episode-budget-open-attention-1-data"
  test "$(cat "$runtime_root/barriers")" = episode-budget-open-attention-1

  # Claim occupancy is a protocol safety failure and is recorded before any
  # external goal I/O can hide it.
  reset_attention_fixture
  open_attention_turn_limit=16
  goal_calls=0
  snapshot_open_attention() {
    local output="$runtime_root/occupied-$2.json"
    write_attention_snapshot "$output" 0 0 data 1
    printf '%s\n' "$output"
  }
  goal_probe() {
    goal_calls=$((goal_calls + 1))
    return 1
  }
  if drive_attention_until_outcome episode-occupied goal_probe >/dev/null 2>&1; then
    printf 'runtime oracle: an occupied claim was hidden by a satisfied goal\n' >&2
    exit 1
  fi
  test "$failure_stage" = scenario.episode-occupied.attention-claim-occupied
  jq -e '
    .episode == "episode-occupied" and .status == "claim_occupied" and
    .turn_limit == 16 and .turns_used == 0 and (.waves | length) == 0 and
    .goal == null and
    ([.final_nodes[] | select(.occupied_claims > 0)] | map(.role)) == ["data"] and
    all(.final_nodes[]; .open_unclaimed == 0)
  ' "$runtime_root/open-attention/episode-occupied-claim-occupied.json" >/dev/null
  test "$goal_calls" = 0
  test "$(find "$runtime_root/turns" -type f | wc -l | tr -d '[:space:]')" = 0
  test ! -s "$runtime_root/barriers"
  runtime_root=
}

(assert_open_attention_boundary)

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

write_current_stream() {
  local destination=$1
  shift
  write_current_stream_command "$destination" \
    "mnemon-harness agent current --json" "$@"
}

write_current_stream_command() {
  local destination=$1 command=$2
  shift 2
  local index=0 view
  jq -nc '{type:"message_start",message:{role:"custom",customType:"mnemond"}}' \
    >"$destination"
  for view in "$@"; do
    index=$((index + 1))
    jq -nc --arg id "current-$index" --arg command "$command" \
      '{type:"tool_execution_start",toolCallId:$id,toolName:"bash",
        args:{command:$command}}' >>"$destination"
    jq -nc --arg id "current-$index" --arg text "$view" \
      '{type:"tool_execution_end",toolCallId:$id,toolName:"bash",isError:false,
        result:{content:[{type:"text",text:$text}],details:{output:$text}}}' \
      >>"$destination"
  done
  jq -nc '{type:"message_end",message:{role:"assistant",stopReason:"stop"}}' \
    >>"$destination"
  jq -nc '{type:"agent_end"}' >>"$destination"
}

write_native_current_stream() {
  local destination=$1
  shift
  local index=0 view
  jq -nc '{type:"message_start",message:{role:"custom",customType:"mnemond"}}' \
    >"$destination"
  for view in "$@"; do
    index=$((index + 1))
    jq -nc --arg id "native-current-$index" \
      '{type:"tool_execution_start",toolCallId:$id,toolName:"mnemond_current",args:{}}' \
      >>"$destination"
    jq -nc --arg id "native-current-$index" --arg text "$view" \
      '{type:"tool_execution_end",toolCallId:$id,toolName:"mnemond_current",isError:false,
        result:{content:[{type:"text",text:$text}],
          details:{schema:"mnemon.pi.current",version:1,status:"projected"}}}' \
      >>"$destination"
  done
  jq -nc '{type:"message_end",message:{role:"assistant",stopReason:"stop"}}' \
    >>"$destination"
  jq -nc '{type:"agent_end"}' >>"$destination"
}

assert_native_current_stream_rejected() {
  local name=$1 source=$2 expected=$3 output partial
  output="$scratch/$name.json"
  if sanitize_turn lead "oracle-$name" "$source" "$output"; then
    printf 'runtime oracle: malformed native Current stream %s was accepted\n' "$name" >&2
    exit 1
  fi
  partial=$(summarize_partial_turn "$source")
  jq -e --arg expected "$expected" '
    .current_boundary.native_protocol_valid == false and
    any(.current_boundary.native_violations[];
      .class == $expected and .count >= 1)
  ' <<<"$partial" >/dev/null || {
    printf 'runtime oracle: malformed native Current stream %s lacked bounded diagnostics\n' \
      "$name" >&2
    exit 1
  }
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

write_probe_overflow_stream() {
  local destination=$1 index
  jq -nc '{type:"message_start",message:{role:"custom",customType:"mnemond"}}' \
    >"$destination"
  for index in 1 2; do
    jq -nc --arg id "domain-probe-$index" \
      '{type:"tool_execution_start",toolCallId:$id,toolName:"bash",
        args:{command:"domainctl probe"}}' >>"$destination"
    jq -nc --arg id "domain-probe-$index" \
      '{type:"tool_execution_end",toolCallId:$id,toolName:"bash",isError:false,
        result:{content:[{type:"text",text:
          "{\"role\":\"lead\",\"result\":{\"bounded\":true}}"}]}}' \
      >>"$destination"
  done
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
  jq -nc '{type:"tool_execution_start",toolCallId:"submit-current",toolName:"bash",
    args:{command:"mnemon-harness agent current --json"}}' >>"$destination"
  jq -nc --arg text "$root_view" \
    '{type:"tool_execution_end",toolCallId:"submit-current",toolName:"bash",isError:false,
      result:{content:[{type:"text",text:$text}],details:{output:$text}}}' \
    >>"$destination"
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

write_native_submit_stream() {
  local destination=$1 result=$2
  jq -nc '{type:"message_start",message:{role:"custom",customType:"mnemond"}}' \
    >"$destination"
  jq -nc '{type:"tool_execution_start",toolCallId:"native-submit-current",
    toolName:"mnemond_current",args:{}}' >>"$destination"
  jq -nc --arg text "$root_view" \
    '{type:"tool_execution_end",toolCallId:"native-submit-current",
      toolName:"mnemond_current",isError:false,
      result:{content:[{type:"text",text:$text}],
        details:{schema:"mnemon.pi.current",version:1,status:"projected"}}}' \
    >>"$destination"
  jq -nc '{type:"tool_execution_start",toolCallId:"native-submit",toolName:"mnemond_submit",
    args:{intent:{kind:"opaque",payload:"bounded",consequence:"handling.advance"}}}' \
    >>"$destination"
  jq -nc --arg text "$result" \
    '{type:"tool_execution_end",toolCallId:"native-submit",toolName:"mnemond_submit",
      isError:false,result:{content:[{type:"text",text:$text}],
        details:{schema:"mnemon.pi.effect",version:1,status:"settled"}}}' \
    >>"$destination"
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
  jq -nc '{type:"tool_execution_start",toolCallId:"submit-current",toolName:"bash",
    args:{command:"mnemon-harness agent current --json"}}' >>"$destination"
  jq -nc --arg text "$root_view" \
    '{type:"tool_execution_end",toolCallId:"submit-current",toolName:"bash",isError:false,
      result:{content:[{type:"text",text:$text}],details:{output:$text}}}' \
    >>"$destination"
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
root_view='{"schema":"mnemon.agent.view","version":6,"view":"view:root-secret","outstanding":{"open_total":0,"related_total":0,"related_projected":0,"truncated":false},"allowed_intents":[]}'
current_view='{"schema":"mnemon.agent.view","version":6,"view":"view:current-secret","current":{"facts":{"handle":"handling:secret","reply_to":"event:secret","reply_required":true,"reply_target":"peer-secret"},"semantic":{"kind":"secret.kind","payload":"secret payload"}},"related":[{"facts":{"event":"event:related-secret","relation":"correlation"},"semantic":{"kind":"secret.related","payload":"related secret"}}],"outstanding":{"open_total":3,"related_total":2,"related_projected":1,"truncated":true},"allowed_intents":[]}'
full_view=$(jq -nc --arg digest \
  'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' '
  {
    schema:"mnemon.agent.view",version:6,view:"view:full-secret",
    current:{
      facts:{handle:"handling:full-secret",reply_to:"event:full-secret",
        reply_required:false,artifacts:[{digest:$digest,handle:"artifact:current-secret"}]},
      semantic:{kind:"secret.current",payload:"current secret"}
    },
    related:[{
      facts:{event:"event:related-full-secret",relation:"terminal_reply",outcome:"completed",
        artifacts:[{digest:$digest,handle:"artifact:related-secret"}]},
      semantic:{kind:"secret.related",payload:"related secret"}
    }],
    outstanding:{open_total:1,related_total:2,related_projected:1,truncated:true},
    references:[
      {facts:{key:"playbook-secret",head:"event:active-secret",state:"active",
        artifact:{digest:$digest,handle:"artifact:reference-secret"},
        terminal_outcomes:{completed:1,declined:2,unresolved:3}}},
      {facts:{key:"retired-secret",head:"event:retracted-secret",state:"retracted",
        terminal_outcomes:{completed:0,declined:0,unresolved:0}}}
    ],
    targets:["peer-secret"],
    allowed_intents:[
      {artifacts:"zero_or_one",consequence:"handling.advance",subject:"current"},
      {artifacts:"exactly_one",consequence:"reference.supersede",subject:"none",
        reference:"offered_head",successors:"none"}
    ],
    provenance_handles:["event:full-secret","event:related-full-secret"]
  }')
write_current_stream "$scratch/root-view.jsonl" "$root_view"
sanitize_turn lead oracle-root-view "$scratch/root-view.jsonl" "$scratch/root-view.json"
jq -e '
  .current_reads == 1 and .view == {
    has_current:false,open_total:0,related_total:0,related_projected:0,truncated:false
  }
' "$scratch/root-view.json" >/dev/null
jq -s -c '.[0], .[2], .[1], .[3:][]' "$scratch/root-view.jsonl" \
  >"$scratch/shell-current-end-before-start.jsonl"
if sanitize_turn lead oracle-shell-current-end-before-start \
    "$scratch/shell-current-end-before-start.jsonl" \
    "$scratch/shell-current-end-before-start.json"; then
  printf 'runtime oracle: a shell Current end preceding its start was accepted\n' >&2
  exit 1
fi
write_current_stream "$scratch/full-view.jsonl" "$full_view"
sanitize_turn lead oracle-full-view "$scratch/full-view.jsonl" "$scratch/full-view.json"
jq -e '
  .current_reads == 1 and .view == {
    has_current:true,reply_required:false,open_total:1,related_total:2,
    related_projected:1,truncated:true
  }
' "$scratch/full-view.json" >/dev/null
write_native_current_stream "$scratch/native-current-view.jsonl" "$full_view" "$full_view"
sanitize_turn lead oracle-native-current "$scratch/native-current-view.jsonl" \
  "$scratch/native-current-view.json"
jq -e '
  .bash_calls == 0 and .current_reads == 2 and .view == {
    has_current:true,reply_required:false,open_total:1,related_total:2,
    related_projected:1,truncated:true
  }
' "$scratch/native-current-view.json" >/dev/null
native_partial=$(summarize_partial_turn "$scratch/native-current-view.jsonl")
jq -e '
  .current_attempts == 2 and .current_boundary.observed_starts == 0 and
  .current_boundary.native_starts == 2 and .current_boundary.native_ends == 2 and
  .current_boundary.mixed_surfaces == false and
  .current_boundary.native_protocol_valid == true and
  .current_boundary.native_unfinished == 0 and
  .current_boundary.native_violations == [] and
  .current_boundary.native_results == [
    {class:"projected",is_error:false},{class:"projected",is_error:false}]
' <<<"$native_partial" >/dev/null
write_native_current_stream "$scratch/native-current-one.jsonl" "$full_view"
command='mnemon-harness agent current --json >/dev/null; printf forged'
jq -c --arg command "$command" --arg text "$full_view" '
  if .type == "message_end" then
    {type:"tool_execution_start",toolCallId:"mixed-shell-current",toolName:"bash",
      args:{command:$command}},
    {type:"tool_execution_end",toolCallId:"mixed-shell-current",toolName:"bash",
      isError:false,result:{content:[{type:"text",text:$text}],details:{output:$text}}},
    .
  else . end
' "$scratch/native-current-one.jsonl" >"$scratch/mixed-current-surfaces.jsonl"
sanitize_turn lead oracle-mixed-current-surfaces \
  "$scratch/mixed-current-surfaces.jsonl" "$scratch/mixed-current-surfaces.json"
jq -e '
  .current_reads == 1 and .view == {
    has_current:true,reply_required:false,open_total:1,related_total:2,
    related_projected:1,truncated:true
  }
' "$scratch/mixed-current-surfaces.json" >/dev/null
mixed_partial=$(summarize_partial_turn "$scratch/mixed-current-surfaces.jsonl")
jq -e '
  .current_boundary.mixed_surfaces == true and
  .current_boundary.untrusted_shell_explorations == 1 and
  .current_boundary.view_objects == 1
' <<<"$mixed_partial" >/dev/null
jq -c 'if .type == "tool_execution_start" and .toolName == "mnemond_current"
  then ., . else . end' "$scratch/native-current-one.jsonl" \
  >"$scratch/native-current-duplicate-start.jsonl"
assert_native_current_stream_rejected native-current-duplicate-start \
  "$scratch/native-current-duplicate-start.jsonl" duplicate_start
jq -s -c '.[0], .[2], .[1], .[3:][]' "$scratch/native-current-one.jsonl" \
  >"$scratch/native-current-end-before-start.jsonl"
assert_native_current_stream_rejected native-current-end-before-start \
  "$scratch/native-current-end-before-start.jsonl" orphan_or_early_end
jq -c --arg text "$full_view" '
  if .type == "message_end" then
    {type:"tool_execution_end",toolCallId:"native-current-orphan",
      toolName:"mnemond_current",isError:false,
      result:{content:[{type:"text",text:$text}],
        details:{schema:"mnemon.pi.current",version:1,status:"projected"}}}, .
  else . end
' "$scratch/native-current-one.jsonl" >"$scratch/native-current-orphan-end.jsonl"
assert_native_current_stream_rejected native-current-orphan-end \
  "$scratch/native-current-orphan-end.jsonl" orphan_or_early_end
jq -c 'if .type == "tool_execution_start" and .toolName == "mnemond_current"
  then .args = {unexpected:true} else . end' "$scratch/native-current-one.jsonl" \
  >"$scratch/native-current-nonempty-args.jsonl"
assert_native_current_stream_rejected native-current-nonempty-args \
  "$scratch/native-current-nonempty-args.jsonl" invalid_start_args
printf '%s\n' "$current_failed_reason" | jq -Rs '{type:"tool_execution_end",
  toolCallId:"native-current-failed",toolName:"mnemond_current",isError:true,
  result:{content:[{type:"text",text:(.[:-1])}],
    details:{schema:"mnemon.pi.current",version:1,status:"failed"}}}' \
  >"$scratch/native-current-failed-end.json"
jq -nc '{type:"message_start",message:{role:"custom",customType:"mnemond"}},
  {type:"tool_execution_start",toolCallId:"native-current-failed",
    toolName:"mnemond_current",args:{}},
  input,{type:"message_end",message:{role:"assistant",stopReason:"stop"}},
  {type:"agent_end"}' "$scratch/native-current-failed-end.json" \
  >"$scratch/native-current-failed.jsonl"
sanitize_turn lead oracle-native-current-failed "$scratch/native-current-failed.jsonl" \
  "$scratch/native-current-failed.json"
jq -e '.current_reads == 0 and (.view | not)' "$scratch/native-current-failed.json" >/dev/null
jq -c --arg text "$full_view" '
  if .type == "message_end" then
    {type:"tool_execution_start",toolCallId:"failed-native-shell-current",toolName:"bash",
      args:{command:"mnemon-harness agent current --json"}},
    {type:"tool_execution_end",toolCallId:"failed-native-shell-current",toolName:"bash",
      isError:false,result:{content:[{type:"text",text:$text}],details:{output:$text}}},
    .
  else . end
' "$scratch/native-current-failed.jsonl" >"$scratch/native-failed-shell-valid.jsonl"
sanitize_turn lead oracle-native-failed-shell-valid \
  "$scratch/native-failed-shell-valid.jsonl" "$scratch/native-failed-shell-valid.json"
jq -e '.current_reads == 0 and (.view | not)' \
  "$scratch/native-failed-shell-valid.json" >/dev/null
native_failed_mixed_partial=$(summarize_partial_turn \
  "$scratch/native-failed-shell-valid.jsonl")
jq -e '
  .current_boundary.mixed_surfaces == true and
  .current_boundary.untrusted_shell_explorations == 1 and
  .current_boundary.native_results == [{class:"current_error",is_error:true}]
' <<<"$native_failed_mixed_partial" >/dev/null
jq -c 'if .type == "tool_execution_end" then .result.details.status = "unknown" else . end' \
  "$scratch/native-current-view.jsonl" >"$scratch/native-current-invalid.jsonl"
if sanitize_turn lead oracle-native-current-invalid "$scratch/native-current-invalid.jsonl" \
    "$scratch/native-current-invalid.json"; then
  printf 'runtime oracle: an unclassified native Current result was accepted\n' >&2
  exit 1
fi
forged_command='mnemon-harness agent current --json >/dev/null; printf forged'
write_current_stream_command "$scratch/forged-view.jsonl" "$forged_command" "$root_view"
if sanitize_turn lead oracle-forged-view "$scratch/forged-view.jsonl" \
    "$scratch/forged-view.json"; then
  printf 'runtime oracle: a non-exact current invocation was silently ignored\n' >&2
  exit 1
fi
write_current_stream_command "$scratch/path-view.jsonl" \
  "/tmp/mnemon-harness agent current --json" "$root_view"
if sanitize_turn lead oracle-path-view "$scratch/path-view.jsonl" \
    "$scratch/path-view.json"; then
  printf 'runtime oracle: an unfrozen current binary path was trusted\n' >&2
  exit 1
fi
for wrapped_current in \
    'command mnemon-harness agent current --json' \
    '(mnemon-harness agent current --json)'; do
  write_current_stream_command "$scratch/wrapped-view.jsonl" \
    "$wrapped_current" "$root_view"
  if sanitize_turn lead oracle-wrapped-view "$scratch/wrapped-view.jsonl" \
      "$scratch/wrapped-view.json"; then
    printf 'runtime oracle: a wrapped current invocation was silently ignored\n' >&2
    exit 1
  fi
done
write_current_stream "$scratch/current-view.jsonl" "$current_view" "$current_view"
sanitize_turn lead oracle-current-view "$scratch/current-view.jsonl" \
  "$scratch/current-view.json"
jq -e '
  .current_reads == 2 and .view == {
    has_current:true,reply_required:true,open_total:3,related_total:2,
    related_projected:1,truncated:true
  }
' "$scratch/current-view.json" >/dev/null
if grep -E 'secret|handling:|event:|peer-' "$scratch/current-view.json" >/dev/null; then
  printf 'runtime oracle: sanitized Agent View retained semantic or authority content\n' >&2
  exit 1
fi
inconsistent_view=$(printf '%s' "$current_view" | jq -c '.outstanding.open_total = 4')
write_current_stream "$scratch/inconsistent-view.jsonl" "$current_view" "$inconsistent_view"
if sanitize_turn lead oracle-inconsistent-view "$scratch/inconsistent-view.jsonl" \
    "$scratch/inconsistent-view.json"; then
  printf 'runtime oracle: inconsistent Agent Views were silently combined\n' >&2
  exit 1
fi
malformed_view=$(printf '%s' "$root_view" | jq -c '.private_authority = "secret"')
write_current_stream "$scratch/malformed-view.jsonl" "$malformed_view"
if sanitize_turn lead oracle-malformed-view "$scratch/malformed-view.jsonl" \
    "$scratch/malformed-view.json"; then
  printf 'runtime oracle: a non-exact Agent View was accepted\n' >&2
  exit 1
fi
write_domain_observation_stream "$scratch/domain-observations.jsonl"
sanitize_turn lead oracle-domain-observations "$scratch/domain-observations.jsonl" \
  "$scratch/domain-observations.json"
jq -e '
  .domain_operations == {
    read:{attempts:4,successes:1,tool_errors:1,invalid_results:1,
      batched_unattributed:1},
    probe:{attempts:1,successes:1,tool_errors:0,invalid_results:0,
      batched_unattributed:0},
    mutation:{attempts:2,successes:1,tool_errors:0,invalid_results:0,
      batched_unattributed:1}
  }
' "$scratch/domain-observations.json" >/dev/null
write_probe_overflow_stream "$scratch/domain-probe-overflow.jsonl"
if sanitize_turn lead oracle-domain-probe-overflow \
    "$scratch/domain-probe-overflow.jsonl" "$scratch/domain-probe-overflow.json"; then
  printf 'runtime oracle: one Agent turn exceeded its real probe budget\n' >&2
  exit 1
fi
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

write_host_delegate_disposition_stream() {
  local destination=$1 reason=$2
  jq -nc '{type:"message_start",message:{role:"custom",customType:"mnemond"}}' \
    >"$destination"
  jq -nc '{type:"tool_execution_start",toolCallId:"delegate-host",toolName:"delegate",
    args:{task:"bounded independent analysis"}}' >>"$destination"
  jq -nc --arg reason "$reason" \
    '{type:"tool_execution_end",toolCallId:"delegate-host",toolName:"delegate",
      isError:true,result:{content:[{type:"text",text:$reason}],details:{}}}' \
    >>"$destination"
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
write_host_delegate_disposition_stream "$scratch/host-delegate.jsonl" \
  "$attention_exhausted_reason"
sanitize_turn lead oracle-host-delegate "$scratch/host-delegate.jsonl" \
  "$scratch/host-delegate.json"
test "$(jq '.delegate_calls' "$scratch/host-delegate.json")" = 0
host_delegate_partial=$(summarize_partial_turn "$scratch/host-delegate.jsonl")
jq -e '
  .delegate_attempts == 1 and .delegate_effects == 0 and
  .delegate_results == [{class:"host_attention_disposition",is_error:true}]
' <<<"$host_delegate_partial" >/dev/null
write_host_delegate_disposition_stream "$scratch/unclassified-delegate.jsonl" \
  'unclassified delegate failure'
if sanitize_turn lead oracle-unclassified-delegate \
    "$scratch/unclassified-delegate.jsonl" "$scratch/unclassified-delegate.json"; then
  printf 'runtime oracle: an unclassified delegate error was accepted\n' >&2
  exit 1
fi
jq -c 'if .type == "tool_execution_end" then .result.extra = true else . end' \
  "$scratch/host-delegate.jsonl" >"$scratch/malformed-host-delegate.jsonl"
if sanitize_turn lead oracle-malformed-host-delegate \
    "$scratch/malformed-host-delegate.jsonl" "$scratch/malformed-host-delegate.json"; then
  printf 'runtime oracle: a malformed Host attention disposition was accepted\n' >&2
  exit 1
fi
write_delegate_stream "$scratch/two-delegates.jsonl" completed completed
if sanitize_turn lead oracle-two-delegates "$scratch/two-delegates.jsonl" \
    "$scratch/two-delegates.json"; then
  printf 'runtime oracle: two delegate calls in one turn were accepted\n' >&2
  exit 1
fi

accepted_receipt='{"schema":"mnemon.agent.receipt","version":1,"outcome":"accepted","replayed":false}'
rejected_receipt='{"schema":"mnemon.agent.receipt","version":1,"outcome":"rejected","replayed":false,"diagnostic":"bounded correction required"}'
closed_denial='{"code":"context_required","message":"a bounded View is required","operation_id":null,"replayed":false,"retryable":false,"schema_version":1,"status":"error"}'
write_native_submit_stream "$scratch/native-submit.jsonl" "$accepted_receipt"
sanitize_turn lead oracle-native-submit "$scratch/native-submit.jsonl" \
  "$scratch/native-submit.json"
jq -e '
  .bash_calls == 0 and .submit_attempts == 1 and .intent_submits == 1 and
  .accepted_receipts == 1 and .rejected_receipts == 0
' "$scratch/native-submit.json" >/dev/null
native_partial=$(summarize_partial_turn "$scratch/native-submit.jsonl")
jq -e '
  .submit_command_occurrences == 1 and .submit_ends == 1 and
  .submit_command_cardinality == {"1":1} and .accepted_receipts == 1
' <<<"$native_partial" >/dev/null
jq -s -c '.[0], .[1], .[2], .[4], .[3], .[5:][]' \
  "$scratch/native-submit.jsonl" >"$scratch/native-receipt-before-submit.jsonl"
if sanitize_turn lead oracle-native-receipt-before-submit \
    "$scratch/native-receipt-before-submit.jsonl" \
    "$scratch/native-receipt-before-submit.json"; then
  printf 'runtime oracle: a receipt preceding its submit start was accepted\n' >&2
  exit 1
fi
jq -c 'if .type == "tool_execution_end" and .toolCallId == "native-submit" then
  .toolName = "bash" else . end' "$scratch/native-submit.jsonl" \
  >"$scratch/native-submit-mismatched-surface.jsonl"
if sanitize_turn lead oracle-native-submit-mismatched-surface \
    "$scratch/native-submit-mismatched-surface.jsonl" \
    "$scratch/native-submit-mismatched-surface.json"; then
  printf 'runtime oracle: a submit end from another tool surface was accepted\n' >&2
  exit 1
fi
jq -c --arg text "$root_view" '
  if .type == "tool_execution_start" and .toolCallId == "native-submit" then
    {type:"tool_execution_start",toolCallId:"native-then-shell-current",toolName:"bash",
      args:{command:"mnemon-harness agent current --json >/dev/null; printf ignored"}},
    {type:"tool_execution_end",toolCallId:"native-then-shell-current",toolName:"bash",
      isError:false,result:{content:[{type:"text",text:$text}],details:{output:$text}}},
    .
  else . end
' "$scratch/native-submit.jsonl" >"$scratch/native-then-shell-submit.jsonl"
if sanitize_turn lead oracle-native-then-shell-submit \
    "$scratch/native-then-shell-submit.jsonl" \
    "$scratch/native-then-shell-submit.json"; then
  printf 'runtime oracle: an Effect used a stale native View after shell Current\n' >&2
  exit 1
fi
jq -c --arg reason "$current_failed_reason" '
  if .type == "tool_execution_start" and .toolCallId == "native-submit" then
    {type:"tool_execution_start",toolCallId:"native-current-after-view",
      toolName:"mnemond_current",args:{}},
    {type:"tool_execution_end",toolCallId:"native-current-after-view",
      toolName:"mnemond_current",isError:true,
      result:{content:[{type:"text",text:$reason}],
        details:{schema:"mnemon.pi.current",version:1,status:"failed"}}},
    .
  else . end
' "$scratch/native-submit.jsonl" >"$scratch/native-failed-before-submit.jsonl"
if sanitize_turn lead oracle-native-failed-before-submit \
    "$scratch/native-failed-before-submit.jsonl" \
    "$scratch/native-failed-before-submit.json"; then
  printf 'runtime oracle: an Effect used a View preceding a failed Current\n' >&2
  exit 1
fi
jq -s -c '.[0], .[3], .[4], .[1], .[2], .[5:][]' \
  "$scratch/native-submit.jsonl" >"$scratch/native-submit-before-current.jsonl"
if sanitize_turn lead oracle-native-submit-before-current \
    "$scratch/native-submit-before-current.jsonl" \
    "$scratch/native-submit-before-current.json"; then
  printf 'runtime oracle: an accepted Effect preceding its trusted View was accepted\n' >&2
  exit 1
fi
write_submit_stream "$scratch/accounted.jsonl" "$accepted_receipt" "$closed_denial"
sanitize_turn lead oracle-accounted "$scratch/accounted.jsonl" "$scratch/accounted.json"
jq -e '
  .submit_attempts == 2 and .intent_submits == 1 and
  .accepted_receipts == 1 and .rejected_receipts == 0 and
  .submit_denials == 1 and .post_accept_denials == 1 and
  .submit_control_denials == [{code:"context_required",count:1}]
' "$scratch/accounted.json" >/dev/null
jq -s -c '.[0], .[3], .[4], .[1], .[2], .[5:][]' \
  "$scratch/accounted.jsonl" >"$scratch/shell-submit-before-current.jsonl"
if sanitize_turn lead oracle-shell-submit-before-current \
    "$scratch/shell-submit-before-current.jsonl" \
    "$scratch/shell-submit-before-current.json"; then
  printf 'runtime oracle: a shell Effect preceding its trusted View was accepted\n' >&2
  exit 1
fi
write_submit_stream "$scratch/multiline-submit.jsonl" "$accepted_receipt"
jq -c 'if .type == "tool_execution_start" and .toolCallId == "submit-1" then
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
  .bash_calls == 2 and .submit_attempts == 1 and .intent_submits == 0 and
  .submit_denials == 1 and .submit_invocation_failures == 0
' "$scratch/sequential-denials.json" >/dev/null

write_sequential_submit_stream "$scratch/sequential-rejections.jsonl" 3 \
  "$rejected_receipt" "$rejected_receipt" "$rejected_receipt"
sanitize_turn lead oracle-sequential-rejections "$scratch/sequential-rejections.jsonl" \
  "$scratch/sequential-rejections.json"
jq -e '
  .bash_calls == 2 and .submit_attempts == 1 and .intent_submits == 1 and
  .rejected_receipts == 1 and .submit_denials == 0 and
  .submit_invocation_failures == 0
' "$scratch/sequential-rejections.json" >/dev/null

write_sequential_submit_stream "$scratch/sequential-repair.jsonl" 3 \
  "$closed_denial" "$closed_denial" "$accepted_receipt"
sanitize_turn lead oracle-sequential-repair "$scratch/sequential-repair.jsonl" \
  "$scratch/sequential-repair.json"
jq -e '
  .bash_calls == 2 and .submit_attempts == 1 and .intent_submits == 1 and
  .accepted_receipts == 1 and .submit_denials == 0 and
  .submit_invocation_failures == 0
' "$scratch/sequential-repair.json" >/dev/null

write_sequential_submit_stream "$scratch/duplicate-accepted.jsonl" 2 \
  "$accepted_receipt" "$accepted_receipt"
if sanitize_turn lead oracle-duplicate-accepted "$scratch/duplicate-accepted.jsonl" \
    "$scratch/duplicate-accepted.json"; then
  printf 'runtime oracle: two accepted Receipts in one tool result were accepted\n' >&2
  exit 1
fi

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
