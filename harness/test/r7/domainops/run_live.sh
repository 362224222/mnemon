#!/usr/bin/env bash

# Opt-in, paid, real-provider proof for federated domain operations. The
# runner fixes only the physical world and bounded attention opportunities.
# Diagnosis, Event vocabulary, peer choice, and remediation remain Agent-owned.

set -euo pipefail
umask 077

# Consume the caller's exported credential before even path discovery starts a
# child process. The private copy is a shell variable only; it is streamed once
# over stdin to each container-local FIFO and never enters argv or env.
provider_key=${DEEPSEEK_API_KEY:-}
unset DEEPSEEK_API_KEY
export -n provider_key

runner_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
harness_root=$(cd "$runner_dir/../../.." && pwd -P)
repository_root=$(cd "$harness_root/.." && pwd -P)
case_root="$harness_root/testdata/r7/domain-ops"
compose_file="$case_root/compose.yaml"
mission_file="$case_root/mission.md"

pi_version=0.83.0
pi_model=${DOMAIN_OPS_PI_MODEL:-deepseek-v4-flash}
turn_seconds=${DOMAIN_OPS_TURN_SECONDS:-150}
rounds=${DOMAIN_OPS_ROUNDS:-3}
raw_stream_max_bytes=$((8 * 1024 * 1024))
raw_stream_max_blocks=$((raw_stream_max_bytes / 1024))
peer_quiescence_seconds=30
report_path=${DOMAIN_OPS_REPORT:-$repository_root/.testdata/r7-domain-ops-live/last-report.json}
trace_path=${DOMAIN_OPS_TRACE:-$repository_root/.testdata/r7-domain-ops-live/last.trace}
failure_report_path=${DOMAIN_OPS_FAILURE_REPORT:-$repository_root/.testdata/r7-domain-ops-live/last-failure.json}
failure_trace_path=${DOMAIN_OPS_FAILURE_TRACE:-$repository_root/.testdata/r7-domain-ops-live/last-failure.trace}
roles='lead edge payment platform data'
neutral_attention='Continue the work available in this workspace. Use current evidence and your local authority, preserve uncertainty, and stop when no useful bounded action remains.'
initial_mission=

runtime_root=
project=
control_network=
agent_image=
agent_image_id=
agent_binary_digests=
agent_containers=
turn_pids=()
run_started_at=
run_finished_at=
failure_stage=runner.pre-authority
authority_started=0
authority_captured=0

fail() {
  printf 'r7 domain ops live: %s\n' "$*" >&2
  return 1
}

validate_integer() {
  local name=$1 value=$2 minimum=$3 maximum=$4
  case "$value" in
    ''|*[!0-9]*) fail "$name must be an integer" ;;
  esac
  test "$value" -ge "$minimum" && test "$value" -le "$maximum" ||
    fail "$name must be between $minimum and $maximum"
}

container_for() {
  printf '%s-agent-%s\n' "$project" "$1"
}

endpoint_for() {
  case "$1" in
    lead) printf 'http://monitor:8080\n' ;;
    edge) printf 'http://gateway:8080\n' ;;
    payment) printf 'http://payment-east:8080\n' ;;
    platform) printf 'http://callback-east:8080\n' ;;
    data) printf 'http://ledger:8080\n' ;;
    *) fail "unknown domain role $1" ;;
  esac
}

compose() {
  docker compose -p "$project" -f "$compose_file" "$@"
}

cleanup() {
  local pid container
  provider_key=
  for pid in "${turn_pids[@]:-}"; do
    test -n "$pid" || continue
    kill -TERM "$pid" >/dev/null 2>&1 || true
    wait "$pid" >/dev/null 2>&1 || true
  done
  for container in $agent_containers; do
    docker rm -f "$container" >/dev/null 2>&1 || true
  done
  if test -n "$control_network"; then
    docker network rm "$control_network" >/dev/null 2>&1 || true
  fi
  if test -n "$project"; then
    compose down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  if test -n "$agent_image"; then
    docker image rm "$agent_image" >/dev/null 2>&1 || true
  fi
  if test -n "${DOMAIN_OPS_IMAGE_TAG:-}"; then
    docker image rm "mnemon-domain-ops-world:$DOMAIN_OPS_IMAGE_TAG" >/dev/null 2>&1 || true
  fi
  if test -n "$runtime_root" && test -d "$runtime_root" && test ! -L "$runtime_root"; then
    case "$runtime_root" in
      /tmp/mnr7-domain-live.??????|/private/tmp/mnr7-domain-live.??????)
        chmod -R u+w "$runtime_root" >/dev/null 2>&1 || true
        rm -rf -- "$runtime_root"
        ;;
      *) printf 'r7 domain ops live: refusing to remove unexpected temporary path\n' >&2 ;;
    esac
  fi
}

on_exit() {
  local original_status=$?
  trap - EXIT HUP INT TERM
  set +e
  if test "$original_status" -ne 0; then
    finalize_failure_evidence "$failure_stage"
  fi
  cleanup
  exit "$original_status"
}

require_prerequisites() {
  test "${LIVE_DOMAIN_OPS:-}" = 1 ||
    fail 'set LIVE_DOMAIN_OPS=1 to authorize paid real-provider turns'
  test -n "$provider_key" || fail 'DEEPSEEK_API_KEY is required'
  validate_integer DOMAIN_OPS_ROUNDS "$rounds" 1 8
  validate_integer DOMAIN_OPS_TURN_SECONDS "$turn_seconds" 30 300
  case "$pi_model" in
    ''|*[!a-zA-Z0-9._-]*) fail 'DOMAIN_OPS_PI_MODEL is invalid' ;;
  esac
  command -v docker >/dev/null 2>&1 || fail 'docker is required'
  command -v jq >/dev/null 2>&1 || fail 'jq is required'
  command -v sqlite3 >/dev/null 2>&1 ||
    fail 'sqlite3 is required for the authority-state oracle'
  docker info >/dev/null 2>&1 || fail 'Docker Engine is unavailable'
  docker compose version >/dev/null 2>&1 || fail 'Docker Compose is required'
  test -f "$compose_file" || fail 'domain-ops compose fixture is missing'
  test -f "$runner_dir/Dockerfile" || fail 'domain-ops Dockerfile is missing'
  test -s "$mission_file" || fail 'domain-ops mission fixture is missing or empty'
  test "$(wc -c <"$mission_file" | tr -d ' ')" -le 1024 ||
    fail 'domain-ops mission exceeds its 1 KiB prompt bound'
  initial_mission=$(<"$mission_file")
  test -n "$initial_mission" || fail 'domain-ops mission is empty after reading'
}

build_and_start_world() {
  export DOMAIN_OPS_IMAGE_TAG="live-$$"
  compose build --quiet
  docker build --quiet --target agent -f "$runner_dir/Dockerfile" \
    -t "$agent_image" "$harness_root" >/dev/null
  agent_image_id=$(docker image inspect --format '{{.Id}}' "$agent_image")
  agent_binary_digests=$(docker run --rm --entrypoint sha256sum "$agent_image" \
    /usr/local/bin/mnemon-harness /usr/local/bin/mnemond /usr/local/bin/domainctl)
  test -n "$agent_image_id" && test -n "$agent_binary_digests" ||
    fail 'candidate Agent image identity is unavailable'
  printf '%s\n' "$agent_binary_digests" >"$runtime_root/candidate-binaries.sha256"
  chmod 0600 "$runtime_root/candidate-binaries.sha256"
  compose up -d --wait ledger callback-east callback-west payment-east payment-west \
    gateway monitor
}

run_load() {
  local prefix=$1 count=$2 output=$3
  compose --profile tools run --rm --no-deps load \
    --gateway-url http://gateway:8080 --monitor-url http://monitor:8080 \
    --prefix "$prefix" --count "$count" --settle 1s >"$output"
}

seed_incident() {
  run_load "$1" 4 "$runtime_root/baseline.json"
  jq -e '
    .sent == 4 and .accepted == 4 and .failed == 0 and
    (.receipts | length) == 4 and
    ([.receipts[].business_id] | unique | length) == 4 and
    all(.receipts[]; .capture_id > 0) and
    .observed.ledger.charges == 8 and
    .observed.ledger.active_charges == 8 and
    .observed.ledger.unique_businesses == 4 and
    .observed.ledger.duplicate_businesses == 4
  ' "$runtime_root/baseline.json" >/dev/null ||
    fail 'the hidden production incident was not established'
}

prepare_workspace() {
  local role=$1
  test -s "$case_root/domains/$role/AGENTS.md" ||
    fail "$role domain projection is missing or empty"
}

start_agent_container() {
  local role=$1 container endpoint
  container=$(container_for "$role")
  endpoint=$(endpoint_for "$role")
  docker run -d --name "$container" --hostname "$role" \
    --network "$control_network" --network-alias "$role" \
    --memory 1g --memory-swap 1g --cpus 1 --pids-limit 256 \
    --security-opt no-new-privileges:true --cap-drop ALL \
    --env "DOMAIN_ROLE=$role" --env "DOMAIN_ENDPOINT=$endpoint" \
    "$agent_image" >/dev/null
  # Root creates only the non-secret parent. It is writable just long enough
  # for the unprivileged Runtime to create its owned 0700 state below; setup
  # tightens the parent again before any Agent turn.
  docker exec -u 0 "$container" sh -c \
    'test ! -e /runtime || test -d /runtime; mkdir -p /runtime; chmod 0733 /runtime'
  docker cp "$case_root/domains/$role/AGENTS.md" "$container:/workspace/AGENTS.md"
  docker exec -u 0 "$container" chmod 0444 /workspace/AGENTS.md
  docker network connect "${project}_${role}-ops" "$container"
  test "$(docker inspect --format '{{.Image}}' "$container")" = "$agent_image_id" ||
    fail "$role does not run the candidate Agent image"
  test "$(docker exec "$container" sha256sum /usr/local/bin/mnemon-harness \
    /usr/local/bin/mnemond /usr/local/bin/domainctl)" = "$agent_binary_digests" ||
    fail "$role does not run the candidate Agent binaries"
  agent_containers="$agent_containers $container"
}

assert_agent_boundary() {
  local role=$1 container expected actual leaked
  container=$(container_for "$role")
  expected=$(printf '%s\n%s\n' "$control_network" "${project}_${role}-ops" | sort |
    tr '\n' ',' | sed 's/,$//')
  actual=$(docker inspect --format \
    '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' "$container" |
    sed '/^$/d' | sort | tr '\n' ',' | sed 's/,$//')
  test "$actual" = "$expected" ||
    fail "$role Agent networks = $actual, want $expected"
  docker inspect "$container" | jq -e '
    length == 1 and
    (.[0].Mounts | length) == 0 and
    .[0].HostConfig.Memory == 1073741824 and
    .[0].HostConfig.MemorySwap == 1073741824 and
    .[0].HostConfig.NanoCpus == 1000000000 and
    .[0].HostConfig.PidsLimit == 256 and
    (.[0].HostConfig.CapDrop | index("ALL")) != null and
    (.[0].HostConfig.SecurityOpt | index("no-new-privileges:true")) != null
  ' >/dev/null || fail "$role Agent received an unexpected mount or resource/security profile"
  docker exec "$container" sh -c '
    test "$(stat -c %u /workspace)" = "$(id -u)" &&
    test "$(stat -c %u /workspace/AGENTS.md)" = 0 &&
    test "$(stat -c %a /workspace/AGENTS.md)" = 444
  ' || fail "$role Agent does not own an isolated workspace projection"
  leaked=$(docker inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$container" |
    grep -E 'DEEPSEEK|API_KEY' || true)
  test -z "$leaked" || fail "$role Agent container received a provider credential"
}

prepare_agents() {
  local role remote container reported_version state_dir=/workspace/.mnemon/harness/node
  docker network create "$control_network" >/dev/null
  for role in $roles; do
    prepare_workspace "$role"
    start_agent_container "$role"
  done
  for role in $roles; do
    assert_agent_boundary "$role"
    container=$(container_for "$role")
    reported_version=$(docker exec "$container" pi --version)
    test "$reported_version" = "$pi_version" ||
      fail "$role Pi version = $reported_version, want $pi_version"
    docker exec -w /workspace "$container" mnemon-harness peer prepare \
      --listen 0.0.0.0:7447 --advertise "$role:7447" --project-root /workspace \
      >"$runtime_root/cards/$role.json"
  done
  for role in $roles; do
    container=$(container_for "$role")
    for remote in $roles; do
      test "$role" = "$remote" && continue
      docker exec -i -w /workspace "$container" mnemon-harness peer enroll \
        --alias "$remote" --project-root /workspace \
        <"$runtime_root/cards/$remote.json" >/dev/null
    done
    docker exec -w /workspace "$container" mnemon-harness setup \
      --runtime pi --project-root /workspace >"$runtime_root/setup-$role.json"
    jq -e '.schema == "mnemon.setup" and .version == 1 and .status == "ready"' \
      "$runtime_root/setup-$role.json" >/dev/null || fail "$role setup was not ready"
    docker exec "$container" sh -c \
      'umask 077; mkdir -p /runtime/pi-state /workspace/.mnemon/live && chmod 700 /runtime/pi-state /workspace/.mnemon/live'
    docker exec -u 0 "$container" chmod 0711 /runtime
    docker exec -d "$container" sh -c \
      "exec mnemond serve --state-dir $state_dir >/workspace/.mnemon/live/mnemond.log 2>&1"
  done
  authority_started=1
  for role in $roles; do
    container=$(container_for "$role")
    local ready=0 attempt=0
    while test "$attempt" -lt 50; do
      if docker exec "$container" test -S "$state_dir/control.sock"; then
        ready=1
        break
      fi
      sleep 0.1
      attempt=$((attempt + 1))
    done
    test "$ready" = 1 || fail "$role mnemond did not become ready"
  done
}

with_deadline() {
  local seconds=$1 marker=$2 pid watchdog status elapsed
  shift 2
  rm -f -- "$marker"
  "$@" &
  pid=$!
  (
    elapsed=0
    while test "$elapsed" -lt "$seconds"; do
      sleep 1
      kill -0 "$pid" 2>/dev/null || exit 0
      elapsed=$((elapsed + 1))
    done
    : >"$marker"
    kill -TERM "$pid" >/dev/null 2>&1 || exit 0
    sleep 5
    kill -KILL "$pid" >/dev/null 2>&1 || true
  ) &
  watchdog=$!
  if wait "$pid"; then status=0; else status=$?; fi
  kill -TERM "$watchdog" >/dev/null 2>&1 || true
  wait "$watchdog" >/dev/null 2>&1 || true
  test ! -f "$marker" || return 124
  return "$status"
}

pi_process() {
  local container=$1
  docker exec -w /workspace "$container" env \
    PI_CODING_AGENT_DIR=/runtime/pi-state PI_SKIP_VERSION_CHECK=1 PI_TELEMETRY=0 \
    pi --mode json --print --no-session --approve --no-prompt-templates --no-themes \
      --provider deepseek --model "$pi_model" --thinking off --tools bash \
      @/workspace/.mnemon/live/turn-prompt.md
}

bounded_pi_process() {
  local container=$1
  # Bash defines -f in 1024-byte blocks on both the pinned macOS shell and
  # Linux runner. The limit is inherited by docker, the process writing both
  # redirected provider streams. A post-run byte check below turns SIGXFSZ
  # into an explicit boundary failure and deletes both streams.
  ulimit -f "$raw_stream_max_blocks"
  pi_process "$container"
}

write_key_once() {
  local container=$1
  printf '%s' "$provider_key" | docker exec -i "$container" sh -c \
    'trap '\''rm -f /runtime/pi-state/provider-key.pipe /runtime/pi-state/auth.json'\'' EXIT HUP INT TERM; cat > /runtime/pi-state/provider-key.pipe'
}

sanitize_turn() {
  local role=$1 tag=$2 raw=$3 output=$4
  jq -s -e --arg role "$role" --arg turn "$tag" \
    --arg captured_at "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" '
    def command: (.args.command // "");
    def invocation_pattern($verb):
      (("(^|[|;&][[:space:]]*)([^[:space:];|&]*/)?mnemon-harness" +
        "[[:space:]]+agent[[:space:]]+" + $verb + "([[:space:];|&]|$)"));
    def invocation_count($verb): [command | scan(invocation_pattern($verb))] | length;
    def invokes($verb):
      (invocation_count($verb) > 0);
    def result_objects:
      [(.result | .. | strings | fromjson? | select(type == "object"))];
    def belongs($ids):
      (.toolCallId // null) as $id |
      $id != null and (($ids | index($id)) != null);
    . as $stream |
    ([$stream[] | select(.type == "tool_execution_start" and .toolName == "bash" and
      invokes("current")) | .toolCallId] | unique) as $current_calls |
    ([$stream[] | select(.type == "tool_execution_start" and .toolName == "bash" and
      invokes("submit"))]) as $submit_starts |
    ([$submit_starts[].toolCallId] | unique) as $submit_calls |
    {
      role: $role,
      turn: $turn,
      captured_at: $captured_at,
      hook_cues: ([$stream[] | select(
        (.type == "message_start" or .type == "message_end") and
        .message.role == "custom" and .message.customType == "mnemond")] | length),
      bash_calls: ([$stream[] | select(
        .type == "tool_execution_start" and .toolName == "bash")] | length),
      current_reads: ([$stream[] | select(
        .type == "tool_execution_end" and .toolName == "bash" and .isError == false and
        belongs($current_calls) and
        any(result_objects[]; .schema == "mnemon.agent.view" and .version == 4 and
          (.view | type == "string" and length > 0)))] | length),
      submit_attempts: ([$submit_starts[] | invocation_count("submit")] | add // 0),
      intent_submits: ([$stream[] | select(
        .type == "tool_execution_end" and .toolName == "bash" and belongs($submit_calls) and
        ([result_objects[] | select(.schema == "mnemon.agent.receipt" and .version == 1 and
          (.outcome == "accepted" or .outcome == "rejected"))] | length) == 1)] | length),
      accepted_receipts: ([$stream[] | select(
        .type == "tool_execution_end" and .toolName == "bash" and .isError == false and
        belongs($submit_calls) and
        any(result_objects[]; .schema == "mnemon.agent.receipt" and .version == 1 and
          .outcome == "accepted"))] | length),
      rejected_receipts: ([$stream[] | select(
        .type == "tool_execution_end" and .toolName == "bash" and belongs($submit_calls) and
        any(result_objects[]; .schema == "mnemon.agent.receipt" and .version == 1 and
          .outcome == "rejected"))] | length),
      private_binding_probes: ([$stream[] | select(.type == "tool_execution_start" and
        .toolName == "bash" and
        (command | test("DEEPSEEK|API_KEY|printenv|auth\\.json|provider-key")))] | length),
      agent_end: any($stream[]; .type == "agent_end")
    }
    | select(
        .hook_cues >= 1 and
        .private_binding_probes == 0 and
        .agent_end == true and
        .submit_attempts <= 1 and
        .intent_submits == .submit_attempts and
        (.accepted_receipts + .rejected_receipts) == .submit_attempts)
  ' "$raw" >"$output" || return 1
  jq -s -e '
    all(.[] | select(.type == "tool_execution_start" and .toolName == "bash");
      ((.args.command // "") | contains("mnemon-harness hook attach") | not))
  ' "$raw" >/dev/null
}

summarize_partial_turn() {
  local raw=$1
  jq -s -c '
    def command: (.args.command // "");
    def result_strings: (.result | .. | strings);
    {
      stream_records:length,
      message_starts:([.[] | select(.type == "message_start")] | length),
      tool_starts:([.[] | select(.type == "tool_execution_start")] | length),
      tool_ends:([.[] | select(.type == "tool_execution_end")] | length),
      tool_errors:([.[] | select(.type == "tool_execution_end" and .isError == true)] | length),
      domain_calls:([.[] | select(.type == "tool_execution_start" and
        .toolName == "bash" and (command | contains("domainctl")))] | length),
      current_attempts:([.[] | select(.type == "tool_execution_start" and
        .toolName == "bash" and (command | test("mnemon-harness[[:space:]]+agent[[:space:]]+current")))] | length),
      submit_attempts:([.[] | select(.type == "tool_execution_start" and
        .toolName == "bash" and (command | test("mnemon-harness[[:space:]]+agent[[:space:]]+submit")))] | length),
      accepted_receipts:([.[] | select(.type == "tool_execution_end" and
        .toolName == "bash" and any(result_strings;
          contains("\"schema\":\"mnemon.agent.receipt\"") and
          contains("\"outcome\":\"accepted\"")))] | length),
      rejected_receipts:([.[] | select(.type == "tool_execution_end" and
        .toolName == "bash" and any(result_strings;
          contains("\"schema\":\"mnemon.agent.receipt\"") and
          contains("\"outcome\":\"rejected\"")))] | length),
      hook_cues:([.[] | select(
        (.type == "message_start" or .type == "message_end") and
        .message.role == "custom" and .message.customType == "mnemond")] | length),
      forbidden_hook_attach:([.[] | select(.type == "tool_execution_start" and
        .toolName == "bash" and (command | contains("mnemon-harness hook attach")))] | length),
      forbidden_secret_probe:([.[] | select(.type == "tool_execution_start" and
        .toolName == "bash" and
        (command | test("DEEPSEEK|API_KEY|printenv|auth\\.json|provider-key")))] | length),
      agent_end:any(.[]; .type == "agent_end")
    }
  ' "$raw" 2>/dev/null || printf '{"stream_records":0,"parseable":false}'
}

run_turn() {
  local role=$1 prompt=$2 tag=$3 container raw errors sanitized marker writer status
  local raw_bytes error_bytes
  container=$(container_for "$role")
  raw="$runtime_root/raw/$tag.jsonl"
  errors="$runtime_root/raw/$tag.err"
  sanitized="$runtime_root/sanitized/$tag.json"
  marker="$runtime_root/raw/$tag.timeout"
  printf '%s\n' "$prompt" | docker exec -i "$container" sh -c \
    'umask 077; cat > /workspace/.mnemon/live/turn-prompt.md'
  docker exec "$container" sh -c \
    'rm -f /runtime/pi-state/provider-key.pipe /runtime/pi-state/auth.json && mkfifo /runtime/pi-state/provider-key.pipe && chmod 600 /runtime/pi-state/provider-key.pipe && jq -cn --arg command "!cat /runtime/pi-state/provider-key.pipe" '\''{deepseek:{type:"api_key",key:$command}}'\'' > /runtime/pi-state/auth.json && chmod 600 /runtime/pi-state/auth.json'
  write_key_once "$container" &
  writer=$!
  if with_deadline "$turn_seconds" "$marker" bounded_pi_process "$container" \
      >"$raw" 2>"$errors"; then
    status=0
  else
    status=$?
  fi
  if kill -0 "$writer" 2>/dev/null; then
    kill -TERM "$writer" >/dev/null 2>&1 || true
  fi
  if wait "$writer"; then :; else
    rm -f -- "$raw" "$errors" "$marker"
    fail "$role turn $tag did not consume its one-shot credential"
  fi
  docker exec "$container" sh -c \
    'test ! -e /runtime/pi-state/provider-key.pipe && test ! -e /runtime/pi-state/auth.json' || {
    rm -f -- "$raw" "$errors" "$marker"
    fail "$role turn $tag left its credential pipe behind"
  }
  docker exec "$container" rm -f /workspace/.mnemon/live/turn-prompt.md
  raw_bytes=$(wc -c <"$raw" | tr -d '[:space:]')
  error_bytes=$(wc -c <"$errors" | tr -d '[:space:]')
  if test "$raw_bytes" -ge "$raw_stream_max_bytes" ||
      test "$error_bytes" -ge "$raw_stream_max_bytes"; then
    rm -f -- "$raw" "$errors" "$marker"
    fail "$role turn $tag exceeded the ${raw_stream_max_bytes}-byte provider-stream bound; raw provider output was deleted"
  fi
  if test "$status" -ne 0; then
    local reason='provider turn failed' partial
    test "$status" -ne 124 || reason="provider turn exceeded ${turn_seconds}s"
    if grep -Eqi 'auth|api[ -]?key|unauthori[sz]ed|http 401' "$errors"; then
      reason='DeepSeek rejected the one-shot credential'
    elif grep -Eqi 'rate.?limit|http 429' "$errors"; then
      reason='DeepSeek rate-limited the live case'
    fi
    partial=$(summarize_partial_turn "$raw")
    rm -f -- "$raw" "$errors" "$marker"
    fail "$role turn $tag: $reason; partial=$partial; raw provider output was deleted"
  fi
  jq -e . "$raw" >/dev/null 2>&1 || {
    rm -f -- "$raw" "$errors" "$marker"
    fail "$role turn $tag did not emit a canonical JSON stream"
  }
  sanitize_turn "$role" "$tag" "$raw" "$sanitized" || {
    local partial
    partial=$(summarize_partial_turn "$raw")
    rm -f -- "$raw" "$errors" "$marker"
    fail "$role turn $tag violated the Hook/submit/terminal boundary; partial=$partial"
  }
  rm -f -- "$raw" "$errors" "$marker"
}

run_attention_round() {
  local round=$1 role pid failed=0
  local round_pids=()
  for role in $roles; do
    run_turn "$role" "$neutral_attention" "round-$round-$role" &
    pid=$!
    round_pids+=("$pid")
    turn_pids+=("$pid")
  done
  for pid in "${round_pids[@]}"; do
    if ! wait "$pid"; then failed=1; fi
  done
  test "$failed" = 0 || fail "attention round $round did not finish cleanly"
  turn_pids=()
  wait_for_peer_delivery_quiescence "round-$round"
}

run_agents() {
  local round
  run_turn lead "$initial_mission" initial-lead
  wait_for_peer_delivery_quiescence initial-lead
  round=1
  while test "$round" -le "$rounds"; do
    run_attention_round "$round"
    round=$((round + 1))
  done
}

pause_agent_containers() {
  local container paused=
  for container in $agent_containers; do
    if ! docker pause "$container" >/dev/null; then
      for container in $paused; do
        docker unpause "$container" >/dev/null 2>&1 || true
      done
      return 1
    fi
    paused="$paused $container"
  done
}

unpause_agent_containers() {
  local container failed=0
  for container in $agent_containers; do
    if ! docker unpause "$container" >/dev/null; then failed=1; fi
  done
  test "$failed" = 0
}

snapshot_peer_delivery_occupancy() {
  local attempt=$1 snapshot role container database values pending staged failed=0 total=0
  snapshot="$runtime_root/quiescence/snapshot-$attempt"
  rm -rf -- "$snapshot"
  mkdir -p "$snapshot"
  : >"$runtime_root/quiescence/counts.jsonl"

  pause_agent_containers || return 1
  for role in $roles; do
    container=$(container_for "$role")
    mkdir -p "$snapshot/$role"
    if ! docker cp "$container:/workspace/.mnemon/harness/node/." \
        "$snapshot/$role" >/dev/null; then
      failed=1
      break
    fi
  done
  unpause_agent_containers || failed=1
  if test "$failed" != 0; then
    rm -rf -- "$snapshot"
    return 1
  fi

  for role in $roles; do
    database="$snapshot/$role/agency.db"
    values=$(sqlite3 -readonly -batch -cmd '.timeout 5000' \
      -cmd 'PRAGMA query_only=ON;' "$database" \
      'SELECT (SELECT COUNT(*) FROM peer_outbox WHERE state = '\''pending'\''),
              (SELECT COUNT(*) FROM peer_inbox WHERE state = '\''staged'\'');') || {
      rm -rf -- "$snapshot"
      return 1
    }
    IFS='|' read -r pending staged <<EOF
$values
EOF
    case "$pending" in ''|*[!0-9]*) rm -rf -- "$snapshot"; return 1 ;; esac
    case "$staged" in ''|*[!0-9]*) rm -rf -- "$snapshot"; return 1 ;; esac
    total=$((total + pending + staged))
    jq -cn --arg role "$role" --argjson pending "$pending" --argjson staged "$staged" \
      '{role:$role,pending_outbox:$pending,staged_inbox:$staged}' \
      >>"$runtime_root/quiescence/counts.jsonl"
  done
  rm -rf -- "$snapshot"
  printf '%s\n' "$total"
}

wait_for_peer_delivery_quiescence() {
  local phase=$1
  local started=$SECONDS deadline=$((SECONDS + peer_quiescence_seconds))
  local attempt=0 occupancy
  mkdir -p "$runtime_root/quiescence"
  while test "$SECONDS" -le "$deadline"; do
    attempt=$((attempt + 1))
    occupancy=$(snapshot_peer_delivery_occupancy "$attempt") ||
      fail 'could not inspect protocol-neutral peer delivery occupancy'
    if test "$occupancy" = 0; then
      jq -s --arg phase "$phase" --argjson attempts "$attempt" \
        --argjson elapsed "$((SECONDS - started))" '
        {
          phase:$phase,
          status:"quiescent",
          attempts:$attempts,
          elapsed_seconds:$elapsed,
          pending_delivery_records:([.[] | .pending_outbox + .staged_inbox] | add),
          nodes:.
        }
      ' "$runtime_root/quiescence/counts.jsonl" \
        >>"$runtime_root/peer-quiescence.jsonl"
      return 0
    fi
    test "$SECONDS" -lt "$deadline" || break
    sleep 0.25
  done
  fail "peer delivery did not quiesce after $phase within ${peer_quiescence_seconds}s; pending=$occupancy"
}

assert_receipts() {
  local report=$1 charges=$2 expected_per_business=$3 expected_voids=$4 label=$5
  jq -e --slurpfile report "$report" --argjson expected "$expected_per_business" \
    --argjson voids "$expected_voids" '
      . as $charges |
      .role == "data" and
      all($report[0].receipts[];
        . as $receipt |
        ([$charges.result[] | select(.business_id == $receipt.business_id)] | length) == $expected and
        ([$charges.result[] | select(
          .business_id == $receipt.business_id and .sequence == $receipt.capture_id and
          .state == "active")] | length) == 1 and
        ([$charges.result[] | select(
          .business_id == $receipt.business_id and .state == "voided")] | length) == $voids)
    ' "$charges" >/dev/null || fail "$label receipt integrity oracle failed"
}

assert_fresh_batch() {
  local report=$1 count=$2 label=$3
  jq -e --argjson count "$count" '
    .sent == $count and .accepted == $count and .failed == 0 and
    (.receipts | length) == $count and all(.receipts[]; .capture_id > 0) and
    .observed.ledger.charges == $count and
    .observed.ledger.active_charges == $count and
    .observed.ledger.voided_charges == 0 and
    .observed.ledger.unique_businesses == $count and
    .observed.ledger.duplicate_businesses == 0
  ' "$report" >/dev/null || fail "$label fresh-traffic oracle failed"
}

assert_recovery() {
  local incident_prefix=$1 evaluation_prefix=$2 stability_prefix=$3
  run_load "$evaluation_prefix" 6 "$runtime_root/recovery.json"
  run_load "$stability_prefix" 6 "$runtime_root/stability.json"
  compose --profile tools run --rm --no-deps data-tool status "$incident_prefix" \
    >"$runtime_root/incident-after.json"
  compose --profile tools run --rm --no-deps data-tool \
    read "/charges?prefix=$incident_prefix" >"$runtime_root/incident-charges.json"
  compose --profile tools run --rm --no-deps data-tool \
    read "/charges?prefix=$evaluation_prefix" >"$runtime_root/recovery-charges.json"
  compose --profile tools run --rm --no-deps data-tool \
    read "/charges?prefix=$stability_prefix" >"$runtime_root/stability-charges.json"

  assert_fresh_batch "$runtime_root/recovery.json" 6 recovery
  assert_fresh_batch "$runtime_root/stability.json" 6 stability
  if ! jq -e '
    .role == "data" and
    .result.charges == 8 and
    .result.active_charges == 4 and
    .result.voided_charges == 4 and
    .result.unique_businesses == 4 and
    .result.duplicate_businesses == 0
  ' "$runtime_root/incident-after.json" >/dev/null; then
    local observed
    observed=$(jq -c '.result // {}' "$runtime_root/incident-after.json")
    fail "the independent existing-data reconciliation oracle failed; observed=$observed"
  fi
  assert_receipts "$runtime_root/baseline.json" "$runtime_root/incident-charges.json" 2 1 \
    historical
  assert_receipts "$runtime_root/recovery.json" "$runtime_root/recovery-charges.json" 1 0 \
    recovery
  assert_receipts "$runtime_root/stability.json" "$runtime_root/stability-charges.json" 1 0 \
    stability
}

stop_and_capture_authority() {
  local role container staging="$runtime_root/authority-capture"
  test "$authority_captured" = 0 || return 0
  rm -rf -- "$staging"
  mkdir -p "$staging"
  for role in $roles; do
    container=$(container_for "$role")
    docker stop --time 5 "$container" >/dev/null
    mkdir -p "$staging/$role"
    # Copy the complete stopped state directory so a committed WAL remains
    # part of the read-only oracle rather than being mistaken for lost state.
    docker cp "$container:/workspace/.mnemon/harness/node/." \
      "$staging/$role" >/dev/null
    test -s "$staging/$role/agency.db" || return 1
  done
  rm -rf -- "$runtime_root/authority"
  mv "$staging" "$runtime_root/authority"
  authority_captured=1
}

assert_peer_effect() {
  local role database count total=0
  test "$authority_captured" = 1 || fail 'peer oracle requires captured authority state'
  : >"$runtime_root/peer-effects.jsonl"
  for role in $roles; do
    database="$runtime_root/authority/$role/agency.db"
    count=$(sqlite3 -readonly -batch -cmd '.timeout 5000' -cmd 'PRAGMA query_only=ON;' "$database" \
      'SELECT COUNT(*) FROM peer_inbox WHERE state = '\''settled'\'' AND local_event_id IS NOT NULL;')
    case "$count" in ''|*[!0-9]*) fail "$role peer-effect oracle returned invalid data" ;; esac
    total=$((total + count))
    jq -cn --arg role "$role" --argjson accepted "$count" \
      '{role:$role,accepted_peer_effects:$accepted}' >>"$runtime_root/peer-effects.jsonl"
  done
  test "$total" -ge 1 ||
    fail 'no authenticated cross-peer Event became an accepted local effect'
  printf '%s\n' "$total" >"$runtime_root/peer-effects.total"
}

write_report() {
  local temporary total
  temporary="$runtime_root/report.json"
  total=$(cat "$runtime_root/peer-effects.total")
  run_finished_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  jq -n \
    --arg schema 'mnemon.r7.domain-ops.live-report' \
    --arg model "$pi_model" \
    --arg run_id "$project" \
    --arg started_at "$run_started_at" \
    --arg finished_at "$run_finished_at" \
    --arg candidate_digest "$agent_image_id" \
    --argjson rounds "$rounds" \
    --argjson baseline "$(cat "$runtime_root/baseline.json")" \
    --argjson recovery "$(cat "$runtime_root/recovery.json")" \
    --argjson stability "$(cat "$runtime_root/stability.json")" \
    --argjson incident_after "$(cat "$runtime_root/incident-after.json")" \
    --argjson incident_charges "$(cat "$runtime_root/incident-charges.json")" \
    --argjson recovery_charges "$(cat "$runtime_root/recovery-charges.json")" \
    --argjson stability_charges "$(cat "$runtime_root/stability-charges.json")" \
    --argjson turns "$(jq -s 'sort_by(.turn)' "$runtime_root/sanitized"/*.json)" \
    --argjson delivery_quiescence "$(jq -s '.' "$runtime_root/peer-quiescence.jsonl")" \
    --argjson peer_effects "$(jq -s '.' "$runtime_root/peer-effects.jsonl")" \
    --argjson peer_effect_total "$total" '
      {
        schema:$schema,
        version:1,
        status:"passed",
        model:$model,
        rounds:$rounds,
        run:{id:$run_id,started_at:$started_at,finished_at:$finished_at,
          candidate_digest:$candidate_digest},
        isolation:{passed:true},
        world:{baseline:$baseline,recovery:$recovery,stability:$stability,
          incident_after:$incident_after,incident_charges:$incident_charges,
          recovery_charges:$recovery_charges,stability_charges:$stability_charges},
        protocol:{accepted_peer_effects:$peer_effect_total,by_receiver:$peer_effects,
          delivery_quiescence:$delivery_quiescence},
        turns:$turns,
        raw_provider_streams_retained:false
      }
  ' >"$temporary"
  chmod 0600 "$temporary"
}

write_trace() {
  (
    cd "$harness_root"
    go run ./test/r7/domainops/trace \
      --report "$runtime_root/report.json" \
      --authority "$runtime_root/authority" \
      --scenario-root "$harness_root" \
      --candidate-binaries "$runtime_root/candidate-binaries.sha256" \
      --output "$runtime_root/report.trace"
  )
}

publish_evidence_file() {
  local source=$1 target=$2 directory temporary
  directory=$(dirname "$target")
  mkdir -p "$directory"
  temporary=$(mktemp "$directory/.r7-domain-evidence.XXXXXX")
  cp "$source" "$temporary"
  chmod 0600 "$temporary"
  mv "$temporary" "$target"
}

publish_evidence() {
  # A failed run never touches the last PASS pair. On success, trace is
  # published first so a fresh passed report can never point at an older trace.
  publish_evidence_file "$runtime_root/report.trace" "$trace_path"
  publish_evidence_file "$runtime_root/report.json" "$report_path"
}

finalize_failure_evidence() {
  local code=$1 observed_at completed_turns='[]'
  test "$authority_started" = 1 || return 0
  test -n "$runtime_root" && test -d "$runtime_root" || return 0
  test -s "$runtime_root/candidate-binaries.sha256" || return 0
  stop_and_capture_authority || return 0

  observed_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  if compgen -G "$runtime_root/sanitized/*.json" >/dev/null; then
    completed_turns=$(jq -s 'sort_by(.turn)' "$runtime_root/sanitized"/*.json) || return 0
  fi
  jq -n \
    --arg schema 'mnemon.r7.domain-ops.failure-report' \
    --arg model "$pi_model" \
    --arg run_id "$project" \
    --arg started_at "$run_started_at" \
    --arg finished_at "$observed_at" \
    --arg candidate_digest "$agent_image_id" \
    --arg code "$code" \
    --arg observed_at "$observed_at" \
    --argjson turns "$completed_turns" '
      {
        schema:$schema,
        version:1,
        status:"failed",
        model:$model,
        run:{id:$run_id,started_at:$started_at,finished_at:$finished_at,
          candidate_digest:$candidate_digest},
        failure:{code:$code,observed_at:$observed_at},
        turns:$turns,
        raw_provider_streams_retained:false
      }
    ' >"$runtime_root/failure-report.json" || return 0
  chmod 0600 "$runtime_root/failure-report.json" || return 0
  (
    cd "$harness_root" || exit 1
    go run ./test/r7/domainops/trace \
      --failure-report "$runtime_root/failure-report.json" \
      --authority "$runtime_root/authority" \
      --scenario-root "$harness_root" \
      --candidate-binaries "$runtime_root/candidate-binaries.sha256" \
      --output "$runtime_root/failure-report.trace"
  ) || return 0
  # Trace first: a published failure JSON can never point at a stale trace.
  publish_evidence_file "$runtime_root/failure-report.trace" "$failure_trace_path" || return 0
  publish_evidence_file "$runtime_root/failure-report.json" "$failure_report_path" || return 0
  printf 'failure report: %s\nfailure observer trace: %s\n' \
    "$failure_report_path" "$failure_trace_path" >&2
}

require_prerequisites
rm -f -- "$failure_report_path" "$failure_trace_path"
runtime_root=$(mktemp -d /tmp/mnr7-domain-live.XXXXXX)
runtime_root=$(cd "$runtime_root" && pwd -P)
chmod 0700 "$runtime_root"
project="mnr7-domain-live-$$"
control_network="$project-mnemon-control"
agent_image="mnemon-domain-ops-agent:live-$$"
run_started_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
mkdir -p "$runtime_root/cards" "$runtime_root/workspaces" "$runtime_root/raw" \
  "$runtime_root/sanitized" "$runtime_root/authority"
trap on_exit EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

incident_prefix="incident-$$"
evaluation_prefix="evaluation-$$"
stability_prefix="stability-$$"
failure_stage=runner.world-start
build_and_start_world
failure_stage=scenario.incident-seed
seed_incident "$incident_prefix"
failure_stage=runner.authority-start
prepare_agents
failure_stage=scenario.agent-turns
run_agents
failure_stage=scenario.recovery
assert_recovery "$incident_prefix" "$evaluation_prefix" "$stability_prefix"
failure_stage=runner.authority-capture
stop_and_capture_authority
failure_stage=r7.peer-effect
assert_peer_effect
failure_stage=runner.pass-report
write_report
failure_stage=runner.pass-trace
write_trace
failure_stage=runner.pass-publish
publish_evidence
failure_stage=runner.complete

printf 'r7 domain ops live: PASS (real services, autonomous domain Agents, external recovery oracle, authenticated peer effect)\n'
printf 'sanitized report: %s\n' "$report_path"
printf 'observer trace: %s\n' "$trace_path"
