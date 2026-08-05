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
persisted_evidence_max_bytes=$((8 * 1024 * 1024))
persisted_evidence_max_blocks=$((persisted_evidence_max_bytes / 1024))
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
    /usr/local/bin/mnemon-harness /usr/local/bin/mnemond /usr/local/bin/domainctl \
    /opt/mnemon/pi-delegate/delegate.ts /opt/mnemon/pi-delegate/delegate-runtime.mjs)
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
  local episode=$1 prefix=$2 expected_route=$3
  run_load "$prefix" 4 "$runtime_root/$episode-baseline.json"
  jq -e --arg route "$expected_route" '
    .sent == 4 and .accepted == 4 and .failed == 0 and
    (.receipts | length) == 4 and
    ([.receipts[].business_id] | unique | length) == 4 and
    all(.receipts[]; .capture_id > 0) and
    .observed.ledger.charges == 8 and
    .observed.ledger.active_charges == 8 and
    .observed.ledger.unique_businesses == 4 and
    .observed.ledger.duplicate_businesses == 4 and
    .observed.gateway.route == $route
  ' "$runtime_root/$episode-baseline.json" >/dev/null ||
    fail 'the hidden production incident was not established'
}

inject_second_variant() {
  # This runner-only mutation reverses the affected region while preserving the
  # same externally visible fault family. It is never mounted or prompted into
  # an Agent workspace. Resetting the first region here prevents the second
  # episode from inheriting whichever valid remediation episode 1 happened to
  # choose; the oracle still does not prescribe how episode 2 is recovered.
  compose --profile tools run --rm --no-deps platform-tool \
    --endpoint http://callback-east:8080 action /admin/latency \
    '{"latency_ms":5}' >"$runtime_root/episode-2-reset-platform.json"
  compose --profile tools run --rm --no-deps payment-tool \
    --endpoint http://payment-east:8080 action /admin/config \
    '{"timeout_ms":500,"stable_keys":true,"retries":2}' \
    >"$runtime_root/episode-2-reset-payment.json"
  compose --profile tools run --rm --no-deps platform-tool \
    --endpoint http://callback-west:8080 action /admin/latency \
    '{"latency_ms":300}' >"$runtime_root/episode-2-injected-platform.json"
  compose --profile tools run --rm --no-deps payment-tool \
    --endpoint http://payment-west:8080 action /admin/config \
    '{"timeout_ms":100,"stable_keys":false,"retries":2}' \
    >"$runtime_root/episode-2-injected-payment.json"
  compose --profile tools run --rm --no-deps edge-tool \
    action /admin/route '{"route":"west"}' \
    >"$runtime_root/episode-2-injected-edge.json"
}

prepare_workspace() {
  local role=$1 projection_dir="$runtime_root/workspaces/$1"
  test -s "$case_root/domains/$role/AGENTS.md" ||
    fail "$role domain projection is missing or empty"
  mkdir -p "$projection_dir"
  cp "$case_root/domains/$role/AGENTS.md" "$projection_dir/AGENTS.md"
  chmod 0444 "$projection_dir/AGENTS.md"
}

start_agent_container() {
  local role=$1 container endpoint projection
  container=$(container_for "$role")
  endpoint=$(endpoint_for "$role")
  projection="$runtime_root/workspaces/$role/AGENTS.md"
  # The name is deterministic and unique to this run. Register it before
  # creation so even a partially created container is covered by cleanup.
  agent_containers="$agent_containers $container"
  docker run -d --name "$container" --hostname "$role" \
    --network "$control_network" --network-alias "$role" \
    --memory 1g --memory-swap 1g --cpus 1 --pids-limit 256 \
    --security-opt no-new-privileges:true --cap-drop ALL \
    --mount "type=bind,src=$projection,dst=/workspace/AGENTS.md,readonly" \
    --env "DOMAIN_ROLE=$role" --env "DOMAIN_ENDPOINT=$endpoint" \
    "$agent_image" >/dev/null
  # Root creates only the non-secret parent. It is writable just long enough
  # for the unprivileged Runtime to create its owned 0700 state below; setup
  # tightens the parent again before any Agent turn.
  docker exec -u 0 "$container" sh -c \
    'test ! -e /runtime || test -d /runtime; mkdir -p /runtime; chmod 0733 /runtime'
  docker network connect "${project}_${role}-ops" "$container"
  test "$(docker inspect --format '{{.Image}}' "$container")" = "$agent_image_id" ||
    fail "$role does not run the candidate Agent image"
  test "$(docker exec "$container" sha256sum /usr/local/bin/mnemon-harness \
    /usr/local/bin/mnemond /usr/local/bin/domainctl \
    /opt/mnemon/pi-delegate/delegate.ts \
    /opt/mnemon/pi-delegate/delegate-runtime.mjs)" = "$agent_binary_digests" ||
    fail "$role does not run the candidate Agent binaries"
}

assert_agent_boundary() {
  local role=$1 container expected actual leaked projection
  container=$(container_for "$role")
  projection="$runtime_root/workspaces/$role/AGENTS.md"
  expected=$(printf '%s\n%s\n' "$control_network" "${project}_${role}-ops" | sort |
    tr '\n' ',' | sed 's/,$//')
  actual=$(docker inspect --format \
    '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' "$container" |
    sed '/^$/d' | sort | tr '\n' ',' | sed 's/,$//')
  test "$actual" = "$expected" ||
    fail "$role Agent networks = $actual, want $expected"
  docker inspect "$container" | jq -e --arg projection "$projection" '
    length == 1 and
    (.[0].Mounts | length) == 1 and
    .[0].Mounts[0].Type == "bind" and
    .[0].Mounts[0].Source == $projection and
    .[0].Mounts[0].Destination == "/workspace/AGENTS.md" and
    .[0].Mounts[0].RW == false and
    .[0].HostConfig.Memory == 1073741824 and
    .[0].HostConfig.MemorySwap == 1073741824 and
    .[0].HostConfig.NanoCpus == 1000000000 and
    .[0].HostConfig.PidsLimit == 256 and
    (.[0].HostConfig.CapDrop | index("ALL")) != null and
    (.[0].HostConfig.SecurityOpt | index("no-new-privileges:true")) != null
  ' >/dev/null || fail "$role Agent received an unexpected mount or resource/security profile"
  docker exec "$container" sh -c '
    probe=/workspace/.projection-replace-probe
    test "$(stat -c %u /workspace)" = "$(id -u)" &&
    test "$(stat -c %a /workspace/AGENTS.md)" = 444 &&
    printf probe >"$probe" &&
    ! mv -f "$probe" /workspace/AGENTS.md 2>/dev/null &&
    ! sh -c "printf changed > /workspace/AGENTS.md" 2>/dev/null &&
    rm -f "$probe" &&
    test "$(stat -c %a /workspace/AGENTS.md)" = 444
  ' || fail "$role Agent can replace its immutable workspace projection"
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
  local seconds=$1 marker=$2 pipeline_pid pipeline_status elapsed
  shift 2
  rm -f -- "$marker"
  # Job control gives this asynchronous function and every local pipeline child
  # (docker exec and jq included) one fresh process group on macOS and Linux.
  # The synchronous owner never returns until that entire group is gone.
  set -m
  "$@" &
  pipeline_pid=$!
  elapsed=0
  while kill -0 "$pipeline_pid" 2>/dev/null; do
    if test "$elapsed" -ge "$seconds"; then
      : >"$marker"
      kill -TERM -- "-$pipeline_pid" >/dev/null 2>&1 || true
      local shutdown_elapsed=0
      while kill -0 -- "-$pipeline_pid" 2>/dev/null &&
          test "$shutdown_elapsed" -lt 5; do
        sleep 1
        shutdown_elapsed=$((shutdown_elapsed + 1))
      done
      kill -KILL -- "-$pipeline_pid" >/dev/null 2>&1 || true
      wait "$pipeline_pid" >/dev/null 2>&1 || true
      set +m
      kill -0 -- "-$pipeline_pid" >/dev/null 2>&1 && return 125
      return 124
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  if wait "$pipeline_pid"; then pipeline_status=0; else pipeline_status=$?; fi
  set +m
  return "$pipeline_status"
}

pi_process() {
  local container=$1 tag=$2 pid_file="/workspace/.mnemon/live/pi-$2.pid"
  docker exec -w /workspace "$container" env \
    PI_CODING_AGENT_DIR=/runtime/pi-state PI_SKIP_VERSION_CHECK=1 PI_TELEMETRY=0 \
    sh -c '
      umask 077
      pid_file=$1
      model=$2
      # BusyBox setsid forks and returns immediately when invoked as the
      # docker-exec process-group leader. Starting it as a background child of
      # this non-interactive wrapper lets it become the session leader in
      # place, while the wrapper explicitly owns and joins its lifetime.
      setsid pi --mode json --print --no-session --approve --no-prompt-templates --no-themes \
        --extension /opt/mnemon/pi-delegate/delegate.ts \
        --provider deepseek --model "$model" --thinking off --tools bash,delegate \
        @/workspace/.mnemon/live/turn-prompt.md &
      child=$!
      printf "%s\n" "$child" >"$pid_file"
      if wait "$child"; then status=0; else status=$?; fi
      rm -f "$pid_file"
      exit "$status"
    ' pi-turn-wrapper "$pid_file" "$pi_model"
}

bounded_pi_process() {
  local container=$1 tag=$2
  # Bash defines -f in 1024-byte blocks on both the pinned macOS shell and
  # Linux runner. The limit is inherited by docker, the process writing both
  # redirected persisted evidence streams. Pi's message_update carries a
  # growing full message snapshot, while tool_execution_update is transient
  # progress. Neither participates in an oracle, so retaining them creates
  # quadratic output with no additional evidence. Keep final boundaries and
  # results, and fail if any
  # remaining record is malformed. The pre-filter provider stream is never
  # materialized: the kernel pipe applies backpressure and the turn deadline
  # bounds its lifetime. A separate transient byte meter would add another
  # buffering process without strengthening retained evidence. The 8 MiB file
  # limit below applies only to persisted, filtered evidence and stderr.
  ulimit -f "$persisted_evidence_max_blocks"
  pi_process "$container" "$tag" |
    jq -c 'select(.type != "message_update" and .type != "tool_execution_update")'
}

stop_remote_pi_pipeline() {
  local container=$1 tag=$2 pid_file="/workspace/.mnemon/live/pi-$2.pid"
  docker exec "$container" sh -c '
    pid_file=$1
    test -f "$pid_file" || exit 0
    IFS= read -r pid <"$pid_file"
    case "$pid" in ""|*[!0-9]*) exit 1 ;; esac
    if kill -0 -$pid 2>/dev/null; then
      kill -TERM -$pid 2>/dev/null || true
      elapsed=0
      while kill -0 -$pid 2>/dev/null && test "$elapsed" -lt 50; do
        sleep 0.1
        elapsed=$((elapsed + 1))
      done
      kill -KILL -$pid 2>/dev/null || true
    fi
    rm -f "$pid_file"
    ! kill -0 -$pid 2>/dev/null
  ' pi-turn-cleanup "$pid_file"
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
    def result_texts:
      if (.result | type) == "object" and
          (.result.content? | type) == "array" then
        .result.content[] | select(.type == "text" and (.text | type) == "string") |
          .text
      elif (.result | type) == "string" then .result
      else empty end;
    def result_objects:
      [result_texts | split("\n")[] | fromjson? | select(type == "object")];
    def valid_receipt:
      .schema == "mnemon.agent.receipt" and .version == 1 and
      (.replayed | type == "boolean") and
      ((.outcome == "accepted" and
        (keys | sort) == ["outcome", "replayed", "schema", "version"]) or
       (.outcome == "rejected" and
        (keys | sort) == ["diagnostic", "outcome", "replayed", "schema", "version"] and
        (.diagnostic | type == "string" and utf8bytelength > 0 and
          utf8bytelength <= 512)));
    def valid_control_error:
      (keys | sort) == ["code", "message", "operation_id", "replayed", "retryable",
        "schema_version", "status"] and
      .schema_version == 1 and .status == "error" and .operation_id == null and
      .replayed == false and
      (.message | type == "string" and utf8bytelength > 0 and utf8bytelength <= 512) and
      (.code as $code | [
        "invalid_argument", "content_required", "content_too_large", "artifact_invalid",
        "artifact_too_large", "authentication_failed", "context_required", "context_stale",
        "asset_revision_mismatch", "action_not_allowed", "operation_mismatch",
        "operation_pending", "mnemond_unavailable", "internal"
      ] | index($code) != null) and
      (.retryable == (.code == "operation_pending" or .code == "mnemond_unavailable"));
    def belongs($ids):
      (.toolCallId // null) as $id |
      $id != null and (($ids | index($id)) != null);
    . as $stream |
    ([$stream[] | select(.type == "message_end" and .message.role == "assistant")]) as
      $assistant_ends |
    ([$stream[] | select(.type == "tool_execution_start" and .toolName == "bash" and
      invokes("current")) | .toolCallId] | unique) as $current_calls |
    ([$stream[] | select(.type == "tool_execution_start" and .toolName == "bash" and
      invokes("submit"))]) as $submit_starts |
    ([$submit_starts[].toolCallId] | unique) as $submit_calls |
    ([$stream[] | select(.type == "tool_execution_start" and
      .toolName == "delegate") | .toolCallId] | unique) as $delegate_calls |
    (reduce $stream[] as $record (
      {accepted_seen:false, denials:0};
      if ($record.type == "tool_execution_end" and $record.toolName == "bash" and
          ($record | belongs($submit_calls))) then
        ($record | [result_objects[]]) as $objects |
        .denials += (if .accepted_seen then
          ([$objects[] | select(valid_control_error)] | length)
        else 0 end) |
        .accepted_seen = (.accepted_seen or
          any($objects[]; valid_receipt and .outcome == "accepted"))
      else . end
    )) as $post_accept |
    {
      role: $role,
      turn: $turn,
      captured_at: $captured_at,
      hook_cues: ([$stream[] | select(
        (.type == "message_start" or .type == "message_end") and
        .message.role == "custom" and .message.customType == "mnemond")] | length),
      bash_calls: ([$stream[] | select(
        .type == "tool_execution_start" and .toolName == "bash")] | length),
      delegate_calls: ($delegate_calls | length),
      current_reads: ([$stream[] | select(
        .type == "tool_execution_end" and .toolName == "bash" and .isError == false and
        belongs($current_calls) and
        any(result_objects[]; .schema == "mnemon.agent.view" and .version == 4 and
          (.view | type == "string" and length > 0)))] | length),
      submit_attempts: ([$submit_starts[] | invocation_count("submit")] | add // 0),
      intent_submits: ([$stream[] | select(
        .type == "tool_execution_end" and .toolName == "bash" and belongs($submit_calls)) |
        result_objects[] | select(valid_receipt)] | length),
      accepted_receipts: ([$stream[] | select(
        .type == "tool_execution_end" and .toolName == "bash" and belongs($submit_calls)) |
        result_objects[] | select(valid_receipt and .outcome == "accepted")] | length),
      rejected_receipts: ([$stream[] | select(
        .type == "tool_execution_end" and .toolName == "bash" and belongs($submit_calls)) |
        result_objects[] | select(valid_receipt and .outcome == "rejected")] | length),
      submit_denials: ([$stream[] | select(
        .type == "tool_execution_end" and .toolName == "bash" and belongs($submit_calls)) |
        result_objects[] | select(valid_control_error)] | length),
      post_accept_denials: $post_accept.denials,
      private_binding_probes: ([$stream[] | select(.type == "tool_execution_start" and
        .toolName == "bash" and
        (command | test("DEEPSEEK|API_KEY|printenv|auth\\.json|provider-key")))] | length),
      agent_end: any($stream[]; .type == "agent_end")
    }
    | select(
        .hook_cues >= 1 and
        ($assistant_ends | length) >= 1 and
        (($assistant_ends[-1].message.stopReason // "") != "error") and
        (($assistant_ends[-1].message.stopReason // "") != "aborted") and
        .private_binding_probes == 0 and
        .agent_end == true and
        ([.hook_cues, .bash_calls, .delegate_calls, .current_reads, .submit_attempts, .intent_submits,
          .accepted_receipts, .rejected_receipts, .submit_denials,
          .post_accept_denials, .private_binding_probes] | all(. >= 0 and . <= 256)) and
        .delegate_calls <= 1 and
        ([$stream[] | select(.type == "tool_execution_end" and .toolName == "delegate" and
          belongs($delegate_calls))] | length) == .delegate_calls and
        .accepted_receipts <= 1 and
        .post_accept_denials <= .submit_denials and
        (.post_accept_denials == 0 or .accepted_receipts == 1) and
        (.accepted_receipts + .rejected_receipts) == .intent_submits and
        (.intent_submits + .submit_denials) == .submit_attempts)
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
    def result_strings:
      if (.result | type) == "object" and
          (.result.content? | type) == "array" then
        .result.content[] | select(.type == "text" and (.text | type) == "string") |
          .text
      elif (.result | type) == "string" then .result
      else empty end;
    def result_objects:
      [result_strings | split("\n")[] | fromjson? | select(type == "object")];
    def valid_control_error:
      (keys | sort) == ["code", "message", "operation_id", "replayed", "retryable",
        "schema_version", "status"] and
      .schema_version == 1 and .status == "error" and .operation_id == null and
      .replayed == false and
      (.message | type == "string" and utf8bytelength > 0 and utf8bytelength <= 512) and
      (.code as $code | [
        "invalid_argument", "content_required", "content_too_large", "artifact_invalid",
        "artifact_too_large", "authentication_failed", "context_required", "context_stale",
        "asset_revision_mismatch", "action_not_allowed", "operation_mismatch",
        "operation_pending", "mnemond_unavailable", "internal"
      ] | index($code) != null) and
      (.retryable == (.code == "operation_pending" or .code == "mnemond_unavailable"));
    {
      stream_records:length,
      record_types:(reduce .[] as $record ({};
        ($record.type // "unknown") as $type | .[$type] = ((.[$type] // 0) + 1))),
      message_starts:([.[] | select(.type == "message_start")] | length),
      message_boundaries:([.[] | select(
        .type == "message_start" or .type == "message_end") |
        {type,role:(.message.role // "missing"),
          custom_type:(.message.customType // "")}]),
      assistant_stop_reasons:([.[] | select(
        .type == "message_end" and .message.role == "assistant") |
        (.message.stopReason // "missing")]),
      tool_starts:([.[] | select(.type == "tool_execution_start")] | length),
      tool_ends:([.[] | select(.type == "tool_execution_end")] | length),
      tool_errors:([.[] | select(.type == "tool_execution_end" and .isError == true)] | length),
      domain_calls:([.[] | select(.type == "tool_execution_start" and
        .toolName == "bash" and (command | contains("domainctl")))] | length),
      delegate_calls:([.[] | select(.type == "tool_execution_start" and
        .toolName == "delegate")] | length),
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
      submit_denials:([.[] | select(.type == "tool_execution_end" and
        .toolName == "bash") | result_objects[] | select(valid_control_error)] | length),
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

summarize_provider_stderr() {
  local errors=$1
  jq -n -c --rawfile value "$errors" '
    def matches($pattern): ($value | test($pattern; "i"));
    {
      bytes:($value | utf8bytelength),
      auth:matches("auth|api[ -]?key|unauthori[sz]ed|http[^0-9]*401"),
      rate_limited:matches("rate.?limit|http[^0-9]*429"),
      balance:matches("insufficient|balance|payment|required|http[^0-9]*402"),
      invalid_request:matches("bad request|invalid request|http[^0-9]*400"),
      unavailable:matches("overload|unavailable|http[^0-9]*50[0234]"),
      network:matches("timed out|timeout|connection|dns|socket|tls")
    }
  '
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
  if with_deadline "$turn_seconds" "$marker" bounded_pi_process "$container" "$tag" \
      >"$raw" 2>"$errors"; then
    status=0
  else
    status=$?
  fi
  if ! stop_remote_pi_pipeline "$container" "$tag"; then
    rm -f -- "$raw" "$errors" "$marker"
    fail "$role turn $tag did not terminate its complete Pi process group"
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
  if test "$raw_bytes" -ge "$persisted_evidence_max_bytes" ||
      test "$error_bytes" -ge "$persisted_evidence_max_bytes"; then
    rm -f -- "$raw" "$errors" "$marker"
    fail "$role turn $tag exceeded the ${persisted_evidence_max_bytes}-byte persisted-evidence bound; retained output was deleted"
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
    local partial provider_error
    partial=$(summarize_partial_turn "$raw")
    provider_error=$(summarize_provider_stderr "$errors")
    rm -f -- "$raw" "$errors" "$marker"
    fail "$role turn $tag violated the Hook/submit/terminal boundary; partial=$partial; provider_error=$provider_error"
  }
  rm -f -- "$raw" "$errors" "$marker"
}

run_attention_round() {
  local episode=$1 round=$2 role pid failed=0
  local round_pids=()
  for role in $roles; do
    run_turn "$role" "$neutral_attention" "$episode-round-$round-$role" &
    pid=$!
    round_pids+=("$pid")
    turn_pids+=("$pid")
  done
  for pid in "${round_pids[@]}"; do
    if ! wait "$pid"; then failed=1; fi
  done
  test "$failed" = 0 || fail "$episode attention round $round did not finish cleanly"
  turn_pids=()
  wait_for_peer_delivery_quiescence "$episode-round-$round"
}

run_agents() {
  local episode=$1 round
  run_turn lead "$initial_mission" "$episode-initial-lead"
  wait_for_peer_delivery_quiescence "$episode-initial-lead"
  round=1
  while test "$round" -le "$rounds"; do
    run_attention_round "$episode" "$round"
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
      (.result | length) == (($report[0].receipts | length) * $expected) and
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

revalidate_episode_state() {
  local episode=$1 incident_prefix=$2 evaluation_prefix=$3 stability_prefix=$4
  local suffix=post-attention
  compose --profile tools run --rm --no-deps data-tool status "$incident_prefix" \
    >"$runtime_root/$episode-incident-after-$suffix.json"
  compose --profile tools run --rm --no-deps data-tool \
    read "/charges?prefix=$incident_prefix" \
    >"$runtime_root/$episode-incident-charges-$suffix.json"
  compose --profile tools run --rm --no-deps data-tool \
    read "/charges?prefix=$evaluation_prefix" \
    >"$runtime_root/$episode-recovery-charges-$suffix.json"
  compose --profile tools run --rm --no-deps data-tool \
    read "/charges?prefix=$stability_prefix" \
    >"$runtime_root/$episode-stability-charges-$suffix.json"

  jq -e '
    .role == "data" and
    .result.charges == 8 and .result.active_charges == 4 and
    .result.voided_charges == 4 and .result.unique_businesses == 4 and
    .result.duplicate_businesses == 0
  ' "$runtime_root/$episode-incident-after-$suffix.json" >/dev/null ||
    fail "$episode changed after its external outcome was accepted"
  assert_receipts "$runtime_root/$episode-baseline.json" \
    "$runtime_root/$episode-incident-charges-$suffix.json" 2 1 \
    "$episode post-attention historical"
  assert_receipts "$runtime_root/$episode-recovery.json" \
    "$runtime_root/$episode-recovery-charges-$suffix.json" 1 0 \
    "$episode post-attention recovery"
  assert_receipts "$runtime_root/$episode-stability.json" \
    "$runtime_root/$episode-stability-charges-$suffix.json" 1 0 \
    "$episode post-attention stability"

  mv "$runtime_root/$episode-incident-after-$suffix.json" \
    "$runtime_root/$episode-incident-after.json"
  mv "$runtime_root/$episode-incident-charges-$suffix.json" \
    "$runtime_root/$episode-incident-charges.json"
  mv "$runtime_root/$episode-recovery-charges-$suffix.json" \
    "$runtime_root/$episode-recovery-charges.json"
  mv "$runtime_root/$episode-stability-charges-$suffix.json" \
    "$runtime_root/$episode-stability-charges.json"
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
  local episode=$1 incident_prefix=$2 evaluation_prefix=$3 stability_prefix=$4
  run_load "$evaluation_prefix" 6 "$runtime_root/$episode-recovery.json"
  run_load "$stability_prefix" 6 "$runtime_root/$episode-stability.json"
  compose --profile tools run --rm --no-deps data-tool status "$incident_prefix" \
    >"$runtime_root/$episode-incident-after.json"
  compose --profile tools run --rm --no-deps data-tool \
    read "/charges?prefix=$incident_prefix" >"$runtime_root/$episode-incident-charges.json"
  compose --profile tools run --rm --no-deps data-tool \
    read "/charges?prefix=$evaluation_prefix" >"$runtime_root/$episode-recovery-charges.json"
  compose --profile tools run --rm --no-deps data-tool \
    read "/charges?prefix=$stability_prefix" >"$runtime_root/$episode-stability-charges.json"

  assert_fresh_batch "$runtime_root/$episode-recovery.json" 6 "$episode recovery"
  assert_fresh_batch "$runtime_root/$episode-stability.json" 6 "$episode stability"
  if ! jq -e '
    .role == "data" and
    .result.charges == 8 and
    .result.active_charges == 4 and
    .result.voided_charges == 4 and
    .result.unique_businesses == 4 and
    .result.duplicate_businesses == 0
  ' "$runtime_root/$episode-incident-after.json" >/dev/null; then
    local observed
    observed=$(jq -c '.result // {}' "$runtime_root/$episode-incident-after.json")
    fail "$episode independent existing-data reconciliation oracle failed; observed=$observed"
  fi
  assert_receipts "$runtime_root/$episode-baseline.json" \
    "$runtime_root/$episode-incident-charges.json" 2 1 "$episode historical"
  assert_receipts "$runtime_root/$episode-recovery.json" \
    "$runtime_root/$episode-recovery-charges.json" 1 0 "$episode recovery"
  assert_receipts "$runtime_root/$episode-stability.json" \
    "$runtime_root/$episode-stability-charges.json" 1 0 "$episode stability"
}

capture_consolidation_start() {
  local staging="$runtime_root/evolution-consolidation-state"
  local role container database sequence failed=0
  rm -rf -- "$staging" "$runtime_root/evolution-consolidation-start"
  mkdir -p "$staging" "$runtime_root/evolution-consolidation-start"

  pause_agent_containers || fail 'could not pause nodes before result consolidation'
  for role in $roles; do
    container=$(container_for "$role")
    mkdir -p "$staging/$role"
    if ! docker cp "$container:/workspace/.mnemon/harness/node/." \
        "$staging/$role" >/dev/null; then
      failed=1
      break
    fi
  done
  unpause_agent_containers || failed=1
  test "$failed" = 0 || fail 'could not capture the pre-consolidation authority boundary'

  for role in $roles; do
    database="$staging/$role/agency.db"
    sequence=$(sqlite3 -readonly -batch -cmd '.timeout 5000' \
      -cmd 'PRAGMA query_only=ON;' "$database" \
      'SELECT COALESCE(MAX(origin_sequence), 0) FROM events;')
    case "$sequence" in ''|*[!0-9]*) fail "$role consolidation sequence is invalid" ;; esac
    jq -n --arg role "$role" --argjson sequence "$sequence" \
      '{role:$role,max_origin_sequence:$sequence}' \
      >"$runtime_root/evolution-consolidation-start/$role.json"
  done
  rm -rf -- "$staging"
}

capture_evolution_boundary() {
  local staging="$runtime_root/evolution-boundary-state"
  local role container database start_sequence sequence heads total=0 failed=0
  rm -rf -- "$staging" "$runtime_root/evolution-boundary"
  mkdir -p "$staging" "$runtime_root/evolution-boundary"

  pause_agent_containers || fail 'could not pause nodes at the episode boundary'
  for role in $roles; do
    container=$(container_for "$role")
    mkdir -p "$staging/$role"
    if ! docker cp "$container:/workspace/.mnemon/harness/node/." \
        "$staging/$role" >/dev/null; then
      failed=1
      break
    fi
  done
  unpause_agent_containers || failed=1
  test "$failed" = 0 || fail 'could not capture the episode authority boundary'

  for role in $roles; do
    database="$staging/$role/agency.db"
    start_sequence=$(jq -er '.max_origin_sequence' \
      "$runtime_root/evolution-consolidation-start/$role.json")
    sequence=$(sqlite3 -readonly -batch -cmd '.timeout 5000' \
      -cmd 'PRAGMA query_only=ON;' "$database" \
      'SELECT COALESCE(MAX(origin_sequence), 0) FROM events;')
    case "$sequence" in ''|*[!0-9]*) fail "$role episode boundary sequence is invalid" ;; esac
    test "$sequence" -ge "$start_sequence" ||
      fail "$role authority sequence regressed across result consolidation"
    heads=$(sqlite3 -readonly -batch -json -cmd '.timeout 5000' \
      -cmd 'PRAGMA query_only=ON;' "$database" "
      SELECT r.head_event_id AS event_id,
             e.event_digest AS event_digest
      FROM active_references AS r
      JOIN events AS e ON e.event_id = r.head_event_id
      WHERE r.state = 'active'
        AND e.origin_sequence > $start_sequence
      ORDER BY r.head_event_id;")
    test -n "$heads" || heads='[]'
    jq -e 'type == "array" and all(.[];
      (.event_id | type == "string" and length > 0) and
      (.event_digest | type == "string" and length > 0))' <<<"$heads" >/dev/null ||
      fail "$role episode boundary Reference snapshot is invalid"
    total=$((total + $(jq 'length' <<<"$heads")))
    jq -n --arg role "$role" --argjson start "$start_sequence" \
      --argjson sequence "$sequence" --argjson heads "$heads" \
      '{role:$role,consolidation_after_sequence:$start,
        max_origin_sequence:$sequence,active_heads:$heads}' \
      >"$runtime_root/evolution-boundary/$role.json"
  done
  rm -rf -- "$runtime_root/runtime-restart-state"
  mv "$staging" "$runtime_root/runtime-restart-state"
  if test "$total" -lt 1; then
    fail 'post-outcome attention produced no active Reference for a future independent Runtime'
    return 1
  fi
  jq -s '{nodes:.,active_head_count:([.[].active_heads | length] | add)}' \
    "$runtime_root/evolution-boundary"/*.json >"$runtime_root/evolution-boundary.json"
}

restart_agent_runtimes() {
  local role container snapshot state_dir=/workspace/.mnemon/harness/node
  for role in $roles; do
    container=$(container_for "$role")
    docker stop --time 5 "$container" >/dev/null
    docker rm "$container" >/dev/null
  done
  agent_containers=

  for role in $roles; do
    snapshot="$runtime_root/runtime-restart-state/$role"
    test -s "$snapshot/agency.db" || fail "$role restart snapshot lacks authority"
    rm -f -- "$snapshot/control.sock"
    start_agent_container "$role"
    container=$(container_for "$role")
    docker exec -u 0 "$container" sh -c \
      'mkdir -p /workspace/.mnemon/harness/node && chown -R 10001:10001 /workspace/.mnemon'
    docker cp "$snapshot/." "$container:$state_dir" >/dev/null
    docker exec -u 0 "$container" chown -R 10001:10001 /workspace/.mnemon
    assert_agent_boundary "$role"
    docker exec -w /workspace "$container" mnemon-harness setup \
      --runtime pi --project-root /workspace >"$runtime_root/restart-setup-$role.json"
    jq -e '.schema == "mnemon.setup" and .version == 1 and .status == "ready"' \
      "$runtime_root/restart-setup-$role.json" >/dev/null ||
      fail "$role fresh Runtime setup was not ready"
    docker exec "$container" sh -c \
      'umask 077; mkdir -p /runtime/pi-state /workspace/.mnemon/live && chmod 700 /runtime/pi-state /workspace/.mnemon/live'
    docker exec -u 0 "$container" chmod 0711 /runtime
    docker exec -d "$container" sh -c \
      "exec mnemond serve --state-dir $state_dir >/workspace/.mnemon/live/mnemond.log 2>&1"
  done

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
    test "$ready" = 1 || fail "$role fresh Runtime mnemond did not become ready"
  done
  rm -rf -- "$runtime_root/runtime-restart-state"
}

assert_evolution() {
  local role database boundary sequence events summary total=0
  test "$authority_captured" = 1 || fail 'evolution oracle requires captured authority state'
  : >"$runtime_root/evolution-effects.jsonl"
  for role in $roles; do
    database="$runtime_root/authority/$role/agency.db"
    boundary="$runtime_root/evolution-boundary/$role.json"
    sequence=$(jq -er '.max_origin_sequence' "$boundary")
    events=$(sqlite3 -readonly -batch -json -cmd '.timeout 5000' \
      -cmd 'PRAGMA query_only=ON;' "$database" "
      SELECT origin_sequence,
             CAST(canonical_json AS TEXT) AS canonical_json
      FROM events
      WHERE origin_sequence > $sequence
      ORDER BY origin_sequence;")
    test -n "$events" || events='[]'
    summary=$(jq -n --arg role "$role" --slurpfile boundary "$boundary" \
      --argjson events "$events" '
      def exact($left; $right):
        $left != null and $left.id == $right.event_id and
        $left.digest == $right.event_digest;
      ($events | map(.canonical_json | fromjson)) as $accepted |
      ($boundary[0].active_heads) as $heads |
      [ $accepted[] as $event |
        $heads[] as $head |
        select(
          any($event.evidence.causation[]?; exact(.; $head)) or
          exact($event.machine.expected_reference.head?; $head)
        ) |
        {event_id:$event.machine.event_id,
         reference_event_id:$head.event_id,reference_digest:$head.event_digest}
      ] | unique_by(.event_id + "\u0000" + .reference_event_id) as $matches |
      {role:$role,boundary_sequence:$boundary[0].max_origin_sequence,
       active_head_count:($heads|length),accepted_reference_uses:($matches|length),
       matches:$matches}')
    jq -e '.accepted_reference_uses >= 0 and (.matches | type == "array")' \
      <<<"$summary" >/dev/null || fail "$role evolution evidence is invalid"
    total=$((total + $(jq '.accepted_reference_uses' <<<"$summary")))
    printf '%s\n' "$summary" >>"$runtime_root/evolution-effects.jsonl"
  done
  if test "$total" -lt 1; then
    fail 'episode 2 did not explicitly use or replace an episode-1 Reference head'
    return 1
  fi
  printf '%s\n' "$total" >"$runtime_root/evolution-effects.total"
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

episode_report_json() {
  local episode=$1
  jq -n \
    --arg id "$episode" \
    --argjson baseline "$(cat "$runtime_root/$episode-baseline.json")" \
    --argjson recovery "$(cat "$runtime_root/$episode-recovery.json")" \
    --argjson stability "$(cat "$runtime_root/$episode-stability.json")" \
    --argjson incident_after "$(cat "$runtime_root/$episode-incident-after.json")" \
    --argjson incident_charges "$(cat "$runtime_root/$episode-incident-charges.json")" \
    --argjson recovery_charges "$(cat "$runtime_root/$episode-recovery-charges.json")" \
    --argjson stability_charges "$(cat "$runtime_root/$episode-stability-charges.json")" '
      {id:$id,baseline:$baseline,recovery:$recovery,stability:$stability,
       incident_after:$incident_after,incident_charges:$incident_charges,
       recovery_charges:$recovery_charges,stability_charges:$stability_charges}
    '
}

write_report() {
  local temporary total episodes evolution_total
  temporary="$runtime_root/report.json"
  total=$(cat "$runtime_root/peer-effects.total")
  evolution_total=$(cat "$runtime_root/evolution-effects.total")
  episodes=$(jq -n --argjson first "$(episode_report_json episode-1)" \
    --argjson second "$(episode_report_json episode-2)" '[$first,$second]')
  run_finished_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  jq -n \
    --arg schema 'mnemon.r7.domain-ops.live-report' \
    --arg model "$pi_model" \
    --arg run_id "$project" \
    --arg started_at "$run_started_at" \
    --arg finished_at "$run_finished_at" \
    --arg candidate_digest "$agent_image_id" \
    --argjson rounds "$rounds" \
    --argjson episodes "$episodes" \
    --argjson turns "$(jq -s 'sort_by(.turn)' "$runtime_root/sanitized"/*.json)" \
    --argjson delivery_quiescence "$(jq -s '.' "$runtime_root/peer-quiescence.jsonl")" \
    --argjson peer_effects "$(jq -s '.' "$runtime_root/peer-effects.jsonl")" \
    --argjson evolution_boundary "$(cat "$runtime_root/evolution-boundary.json")" \
    --argjson evolution_effects "$(jq -s '.' "$runtime_root/evolution-effects.jsonl")" \
    --argjson evolution_total "$evolution_total" \
    --argjson peer_effect_total "$total" '
      {
        schema:$schema,
        version:2,
        status:"passed",
        model:$model,
        rounds:$rounds,
        run:{id:$run_id,started_at:$started_at,finished_at:$finished_at,
          candidate_digest:$candidate_digest},
        isolation:{passed:true,fresh_runtime_between_episodes:true},
        world:{episodes:$episodes},
        protocol:{accepted_peer_effects:$peer_effect_total,by_receiver:$peer_effects,
          delivery_quiescence:$delivery_quiescence,
          evolution:{boundary:$evolution_boundary,effects:$evolution_effects,
            accepted_reference_uses:$evolution_total}},
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

main() {
  require_prerequisites
  rm -f -- "$failure_report_path" "$failure_trace_path"
  runtime_root=$(mktemp -d /tmp/mnr7-domain-live.XXXXXX)
  trap on_exit EXIT
  trap 'exit 129' HUP
  trap 'exit 130' INT
  trap 'exit 143' TERM
  runtime_root=$(cd "$runtime_root" && pwd -P)
  chmod 0700 "$runtime_root"
  project="mnr7-domain-live-$$"
  control_network="$project-mnemon-control"
  agent_image="mnemon-domain-ops-agent:live-$$"
  run_started_at=$(date -u '+%Y-%m-%dT%H:%M:%SZ')
  mkdir -p "$runtime_root/cards" "$runtime_root/workspaces" "$runtime_root/raw" \
    "$runtime_root/sanitized" "$runtime_root/authority"

  local first_incident_prefix="incident-a-$$"
  local first_evaluation_prefix="evaluation-a-$$"
  local first_stability_prefix="stability-a-$$"
  local second_incident_prefix="incident-b-$$"
  local second_evaluation_prefix="evaluation-b-$$"
  local second_stability_prefix="stability-b-$$"
  failure_stage=runner.world-start
  build_and_start_world
  failure_stage=scenario.episode-1-incident-seed
  seed_incident episode-1 "$first_incident_prefix" east
  failure_stage=runner.authority-start
  prepare_agents
  failure_stage=scenario.episode-1-agent-turns
  run_agents episode-1
  failure_stage=scenario.episode-1-recovery
  assert_recovery episode-1 "$first_incident_prefix" "$first_evaluation_prefix" \
    "$first_stability_prefix"
  failure_stage=scenario.episode-1-consolidation-start
  capture_consolidation_start
  failure_stage=scenario.episode-1-post-outcome-attention
  run_attention_round episode-1 post-outcome
  failure_stage=scenario.evolution-boundary
  capture_evolution_boundary
  failure_stage=scenario.episode-1-post-outcome-revalidation
  revalidate_episode_state episode-1 "$first_incident_prefix" \
    "$first_evaluation_prefix" "$first_stability_prefix"
  failure_stage=runner.runtime-restart
  restart_agent_runtimes
  failure_stage=scenario.episode-2-injection
  inject_second_variant
  seed_incident episode-2 "$second_incident_prefix" west
  failure_stage=scenario.episode-2-agent-turns
  run_agents episode-2
  failure_stage=scenario.episode-2-recovery
  assert_recovery episode-2 "$second_incident_prefix" "$second_evaluation_prefix" \
    "$second_stability_prefix"
  failure_stage=runner.authority-capture
  stop_and_capture_authority
  failure_stage=r7.peer-effect
  assert_peer_effect
  failure_stage=scenario.evolution
  assert_evolution
  failure_stage=runner.pass-report
  write_report
  failure_stage=runner.pass-trace
  write_trace
  failure_stage=runner.pass-publish
  publish_evidence
  failure_stage=runner.complete

  printf 'r7 domain ops live: PASS (two real incidents, fresh Pi turns, retained authority, external recovery and evolution oracles)\n'
  printf 'sanitized report: %s\n' "$report_path"
  printf 'observer trace: %s\n' "$trace_path"
}

if test "${BASH_SOURCE[0]}" = "$0"; then
  main "$@"
fi
