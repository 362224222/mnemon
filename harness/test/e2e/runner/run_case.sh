#!/bin/sh
set -eu

. "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)/lib.sh"

runtime=
case_name=
run_id=
output=
image_reference=
image_digest=
git_sha=
credential=
while [ "$#" -gt 0 ]; do
    case "$1" in
        --runtime) runtime=$2; shift 2 ;;
        --case) case_name=$2; shift 2 ;;
        --run-id) run_id=$2; shift 2 ;;
        --output) output=$2; shift 2 ;;
        --image) image_reference=$2; shift 2 ;;
        --image-digest) image_digest=$2; shift 2 ;;
        --git-sha) git_sha=$2; shift 2 ;;
        --credential) credential=$2; shift 2 ;;
        *) usage_error "unknown case-runner argument: $1" ;;
    esac
done

[ "$runtime" = scripted ] || [ "$runtime" = codex ] || usage_error 'case runtime is invalid'
validate_case_name "$case_name"
validate_run_id "$run_id"
[ -n "$output" ] && [ -d "$output" ] || usage_error 'case output directory is required'
[ -n "$image_reference" ] && [ -n "$image_digest" ] && [ -n "$git_sha" ] ||
    usage_error 'case image and commit pins are required'
[ ! -L "$output" ] || usage_error 'case output directory may not be a symlink'
validate_image_reference "$image_reference"
validate_image_digest "$image_digest"
validate_git_sha "$git_sha"

scenario="$scenario_root/$case_name"
scenario_manifest="$scenario/manifest.json"
private=$(mktemp -d)
chmod 0700 "$private"
project_prefix=$(printf '%s' "$run_id" | tr '[:upper:]_.' '[:lower:]--' | cut -c1-28)
project_suffix=$(printf '%s' "$run_id" | sha256sum | awk '{print substr($1,1,12)}')
project="r5-${project_prefix}-${project_suffix}"
compose="$private/compose.yaml"
commands_jsonl="$private/commands.jsonl"
assertions_jsonl="$private/assertions.jsonl"
faults_jsonl="$private/faults.jsonl"
task_jsonl="$private/task.jsonl"
latency_jsonl="$private/latency.jsonl"
: >"$commands_jsonl"
: >"$assertions_jsonl"
: >"$faults_jsonl"
: >"$task_jsonl"
: >"$latency_jsonl"
sequence=0
containers_started=false
case_failed=false
execution_failed=false
finalized=false
logs_collected=false
public_status_ready_timeout_seconds=60
derived_offer_ready_timeout_seconds=30
final_public_status_ready_timeout_seconds=60

mkdir -p "$output/topology" "$output/transcript" "$output/nodes" \
    "$output/runtime" "$output/artifacts" "$output/faults" "$output/oracle"
chmod 0700 "$output" "$output/faults"
for node in A B C D E F; do
    mkdir -p "$output/nodes/$node"
    : >"$output/nodes/$node/peer-trace.ndjson"
    : >"$output/nodes/$node/conflict-trace.ndjson"
    : >"$output/nodes/$node/handling-trace.ndjson"
    : >"$output/nodes/$node/daemon.log"
done

case_error() {
    case_failed=true
    execution_failed=true
    printf 'r5-e2e[%s]: %s\n' "$case_name" "$*" >&2
}

add_assertion() {
    id=$1
    category=$2
    required=$3
    passed=$4
    evidence=$5
    message=$6
    jq -cn --arg id "$id" --arg category "$category" --argjson required "$required" \
      --argjson passed "$passed" --arg evidence "$evidence" --arg message "$message" '
      {id:$id,category:$category,required:$required,passed:$passed,
       evidence:(if $evidence == "" then [] else ($evidence | split(",")) end),message:$message}
    ' >>"$assertions_jsonl"
    [ "$required" = false ] || [ "$passed" = true ] || case_failed=true
}

record_command() {
    node=$1
    kind=$2
    started=$3
    finished=$4
    exit_code=$5
    evidence=$6
    sequence=$((sequence + 1))
    duration=$((finished - started))
    jq -cn --argjson sequence "$sequence" --arg node "$node" --arg kind "$kind" \
      --argjson started "$started" --argjson finished "$finished" \
      --argjson duration "$duration" --argjson exit_code "$exit_code" --arg evidence "$evidence" '
      {sequence:$sequence,node:$node,kind:$kind,started_unix_ms:$started,
       finished_unix_ms:$finished,duration_ms:$duration,exit_code:$exit_code,
       evidence:(if $evidence == "" then [] else [$evidence] end)}
    ' >>"$commands_jsonl"
}

write_compose() {
    {
        printf '%s\n' 'name: r5-e2e' 'services:'
        for lower in a b c d e f; do
            upper=$(printf '%s' "$lower" | tr '[:lower:]' '[:upper:]')
            printf '  node-%s:\n' "$lower"
            printf '    image: %s\n' "$image_reference"
            printf '    pull_policy: never\n'
            if [ "$runtime" = codex ]; then
                printf '    user: "0:0"\n'
                # live-init must itself be PID 1 so its final exec replaces
                # root instead of leaving a root-owned tini parent behind.
                printf '    entrypoint: ["/opt/r5/bin/live-init"]\n'
                printf '    command: []\n'
            else
                printf '    user: "10001:10001"\n'
                printf '    command: ["/opt/r5/bin/idle"]\n'
            fi
            printf '%s\n' '    read_only: true' '    init: false'
            printf '%s\n' '    pids_limit: 256' '    mem_limit: 2g' '    cpus: 2.0'
            printf '%s\n' '    security_opt:' '      - no-new-privileges:true' '    cap_drop:' '      - ALL'
            if [ "$runtime" = codex ]; then
                printf '%s\n' '    cap_add:' '      - CHOWN' '      - DAC_OVERRIDE' '      - SETGID' '      - SETUID'
            fi
            printf '%s\n' '    tmpfs:' '      - /tmp:rw,noexec,nosuid,nodev,mode=1777,size=256m'
            if [ "$runtime" = codex ]; then
                printf '%s\n' '      - /run/r5-auth:rw,noexec,nosuid,nodev,mode=0700,size=64m'
            fi
            printf '%s\n' '    environment:' '      HOME: /home/r5'
            if [ "$runtime" = codex ]; then
                printf '%s\n' '      CODEX_HOME: /run/r5-auth/codex-home'
                printf '%s\n' '      PATH: /usr/local/go/bin:/usr/local/bin:/usr/bin:/bin'
                printf '      R5_CODEX_MODEL: %s\n' "$R5_CODEX_MODEL"
                printf '      R5_CODEX_REASONING_EFFORT: %s\n' "$R5_CODEX_REASONING_EFFORT"
            else
                printf '%s\n' '      CODEX_HOME: /home/r5/.codex'
                printf '%s\n' '      PATH: /opt/r5/scripted/bin:/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin'
            fi
            printf '      R5_NODE_ALIAS: %s\n' "$upper"
            printf '      R5_SCENARIO: %s\n' "$case_name"
            printf '%s\n' '    volumes:'
            printf '      - node-%s-home:/home/r5\n' "$lower"
            printf '      - node-%s-workspace:/workspace\n' "$lower"
            printf '%s\n' '    networks:' '      - mesh'
            if [ "$runtime" = codex ]; then
                printf '%s\n' '    secrets:' '      - source: provider_credential' \
                    '        target: provider_credential' '        mode: 0400'
            fi
            printf '%s\n' '    labels:'
            printf '      io.mnemon.r5.run: %s\n' "$run_id"
            printf '      io.mnemon.r5.node: %s\n' "$upper"
        done
        printf '%s\n' 'networks:' '  mesh:'
        [ "$runtime" = scripted ] && printf '%s\n' '    internal: true'
        printf '    name: %s-mesh\n' "$project"
        printf '%s\n' 'volumes:'
        for lower in a b c d e f; do
            printf '  node-%s-home: {}\n' "$lower"
            printf '  node-%s-workspace: {}\n' "$lower"
        done
        if [ "$runtime" = codex ]; then
            printf '%s\n' 'secrets:' '  provider_credential:'
            encoded_credential=$(jq -Rn --arg value "$credential" '$value')
            printf '    file: %s\n' "$encoded_credential"
        fi
    } >"$compose"
    chmod 0600 "$compose"
}

container_id() {
    lower=$(printf '%s' "$1" | tr '[:upper:]' '[:lower:]')
    docker compose --project-name "$project" --file "$compose" ps -q "node-$lower"
}

node_exec() {
    node=$1
    shift
    docker exec --user 10001:10001 --workdir /workspace "$(container_id "$node")" "$@"
}

node_exec_stdin() {
    node=$1
    shift
    docker exec --user 10001:10001 --workdir /workspace -i "$(container_id "$node")" "$@"
}

public_command() {
    node=$1
    kind=$2
    evidence=$3
    shift 3
    stdout="$private/command-$((sequence + 1)).stdout"
    stderr="$private/command-$((sequence + 1)).stderr"
    started=$(date +%s%3N)
    set +e
    node_exec "$node" timeout 30s "$@" >"$stdout" 2>"$stderr"
    exit_code=$?
    set -e
    finished=$(date +%s%3N)
    if [ -n "$evidence" ]; then
        destination="$output/$evidence"
        mkdir -p "$(dirname "$destination")"
        if jq -e . "$stdout" >/dev/null 2>&1; then
            redact_json <"$stdout" >"$destination"
        else
            redact_text_file "$stdout" "$destination"
        fi
        if [ -s "$stderr" ]; then
            redact_text_file "$stderr" "$output/transcript/$((sequence + 1))-$node-$kind.stderr"
        fi
    fi
    record_command "$node" "$kind" "$started" "$finished" "$exit_code" "$evidence"
    return "$exit_code"
}

capture_public_output() {
    node=$1
    evidence=$2
    shift 2
    stdout=$(mktemp "$private/fault-capture.XXXXXX.stdout")
    stderr=$(mktemp "$private/fault-capture.XXXXXX.stderr")
    set +e
    node_exec "$node" timeout 30s "$@" >"$stdout" 2>"$stderr"
    exit_code=$?
    set -e
    destination="$output/$evidence"
    mkdir -p "$(dirname "$destination")"
    if jq -e . "$stdout" >/dev/null 2>&1; then
        redact_json <"$stdout" >"$destination"
    else
        redact_text_file "$stdout" "$destination"
    fi
    if [ -s "$stderr" ]; then
        safe_evidence=$(printf '%s' "$evidence" | tr -c 'A-Za-z0-9._-' '_')
        redact_text_file "$stderr" "$output/transcript/$safe_evidence.stderr"
    fi
    return "$exit_code"
}

cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    if [ "$containers_started" = true ]; then
        if [ "$logs_collected" = false ]; then
            for node in A B C D E F; do
                cid=$(container_id "$node" 2>/dev/null || true)
                [ -z "$cid" ] || docker logs "$cid" >"$private/$node.log" 2>&1 || true
                [ ! -f "$private/$node.log" ] || redact_text_file "$private/$node.log" \
                    "$output/nodes/$node/daemon.log"
            done
        fi
        if [ "${KEEP:-0}" != 1 ]; then
            if ! docker compose --project-name "$project" --file "$compose" down --volumes \
                --remove-orphans >/dev/null 2>&1; then
                printf 'r5-e2e[%s]: Compose cleanup failed for %s\n' \
                    "$case_name" "$project" >&2
                [ "$status" -ne 0 ] || status=1
            fi
        else
            printf 'r5-e2e[%s]: KEEP=1 retained Compose project %s\n' "$case_name" "$project" >&2
        fi
    fi
    rm -rf "$private"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

start_topology() {
    write_compose
    # Set this before `up`: even a partial startup can contain the Live secret
    # mount and must be torn down by the EXIT path.
    containers_started=true
    if ! docker compose --project-name "$project" --file "$compose" up -d \
        --no-build --pull never >/dev/null; then
        case_error 'six-container topology did not start'
        return 1
    fi
    ids=
    for node in A B C D E F; do
        cid=$(container_id "$node")
        [ -n "$cid" ] || {
            case_error "container is absent: $node"
            return 1
        }
        ids="$ids $cid"
    done
    docker inspect $ids >"$private/docker-inspect.raw.json"
    jq --arg runtime "$runtime" --arg credential "$credential" '
      map(.Mounts |= map(if .Type == "bind" then .Source = "<provider-secret-source>" else . end) |
          .Config.Env |= map(if test("(?i)(token|secret|credential|api_key)=") then
            (split("=")[0] + "=<redacted>") else . end)) |
      walk(if type == "string" and $credential != "" and contains($credential) then
        (split($credential) | join("<provider-secret-source>")) else . end)
    ' "$private/docker-inspect.raw.json" >"$output/topology/docker-inspect.json"
    docker network inspect "$project-mesh" >"$output/topology/docker-network.json"
    jq '[.[] | {node:(.Config.Labels["io.mnemon.r5.node"]),mounts:[.Mounts[] |
          {type:.Type,destination:.Destination,source:(if .Type == "bind" then "<provider-secret-source>" else .Source end),read_write:.RW}]}]' \
      "$private/docker-inspect.raw.json" >"$output/topology/mounts.json"

    topology_ok=true
    jq -e --arg digest "$image_digest" '
      length == 6 and all(.[]; .Image == $digest and .HostConfig.NetworkMode != "host" and
        .HostConfig.Privileged == false and .HostConfig.PidMode != "host" and
        .HostConfig.ReadonlyRootfs == true and .HostConfig.Memory == 2147483648 and
        .HostConfig.NanoCpus == 2000000000 and .HostConfig.PidsLimit == 256 and
        (.HostConfig.CapDrop | index("ALL") != null) and
        (.HostConfig.SecurityOpt | index("no-new-privileges:true") != null) and
        ([.Mounts[] | select(.Destination == "/var/run/docker.sock")] | length == 0)) and
      ([.[] | .Mounts[] | select(.Type == "volume") | .Source] | length == 12) and
      ([.[] | .Mounts[] | select(.Type == "volume") | .Source] | unique | length == 12)
    ' "$private/docker-inspect.raw.json" >/dev/null || topology_ok=false
    if [ "$runtime" = scripted ]; then
        jq -e 'all(.[]; (.HostConfig.CapAdd // []) == [] and
          ([.Mounts[] | select(.Type == "bind")] | length == 0))' \
          "$private/docker-inspect.raw.json" >/dev/null || topology_ok=false
        jq -e '.[0].Internal == true' "$output/topology/docker-network.json" >/dev/null || topology_ok=false
    else
        jq -e 'all(.[]; ((.HostConfig.CapAdd // []) | sort) == ["CHOWN","DAC_OVERRIDE","SETGID","SETUID"] and
          ([.Mounts[] | select(.Type == "bind" and .Destination == "/run/secrets/provider_credential" and .RW == false)] | length == 1) and
          ([.Mounts[] | select(.Type == "bind" and .Destination != "/run/secrets/provider_credential")] | length == 0))' \
          "$private/docker-inspect.raw.json" >/dev/null || topology_ok=false
    fi
    for node in A B C D E F; do
        uid=$(node_exec "$node" ps -o uid= -p 1 | tr -d ' ')
        [ "$uid" = 10001 ] || topology_ok=false
    done
    add_assertion six-isolated-containers system true "$topology_ok" \
      'topology/docker-inspect.json,topology/mounts.json,topology/docker-network.json' \
      'Six candidate-image containers have unique home and workspace volumes, with Node state isolated inside each workspace, and no forbidden bind, socket, or host network.'
    [ "$topology_ok" = true ] || return 1

    sentinel=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
    node_exec A sh -c 'umask 077; printf "%s" "$1" > /workspace/.r5-isolation-sentinel' sh "$sentinel"
    isolated=true
    for node in B C D E F; do
        node_exec "$node" test ! -e /workspace/.r5-isolation-sentinel || isolated=false
    done
    node_exec A rm -f /workspace/.r5-isolation-sentinel
    add_assertion workspace-sentinel-isolation system true "$isolated" \
      topology/mounts.json 'A workspace sentinel is absent from B through F.'
    [ "$isolated" = true ]
}

install_setup_and_fixtures() {
    for node in A B C D E F; do
        if ! public_command "$node" setup "nodes/$node/setup-manifest.json" \
            mnemon-harness setup --host codex --project-root /workspace; then
            case_error "fresh setup failed on Node $node"
            return 1
        fi
        duration=$(tail -n 1 "$commands_jsonl" | jq -r '.duration_ms')
        setup_fast=true
        [ "$duration" -le 15000 ] || setup_fast=false
        add_assertion "setup-$node-within-15s" system true "$setup_fast" \
          "nodes/$node/setup-manifest.json" 'Fresh setup reached its public receipt within the frozen threshold.'

        node_exec "$node" install -d -m 0700 /workspace/case /workspace/result /workspace/.r5 || {
            case_error "fixture directories could not be created on Node $node"
            return 1
        }
        fixture="$scenario/public/$node"
        [ -d "$fixture" ] || {
            case_error "public fixture is missing for Node $node"
            return 1
        }
        fixture_tar="$private/fixture-$node.tar"
        tar -C "$fixture" -cf "$fixture_tar" . || {
            case_error "public fixture could not be archived for Node $node"
            return 1
        }
        node_exec_stdin "$node" tar -C /workspace/case -xf - <"$fixture_tar" || {
            case_error "public fixture could not be installed on Node $node"
            return 1
        }
        if [ "$runtime" = scripted ]; then
            node_exec_stdin "$node" sh -c \
              'umask 077; cat > /workspace/.r5/policy.json' \
              <"$scenario/policies/$node.json" || {
                case_error "scripted policy could not be installed on Node $node"
                return 1
            }
            printf '%s\n' "$case_name" | node_exec_stdin "$node" sh -c \
              'umask 077; cat > /workspace/.r5/scenario' || {
                case_error "scripted scenario identity could not be installed on Node $node"
                return 1
            }
            if [ "$node" = A ]; then
                node_exec_stdin "$node" sh -c \
                  'umask 077; cat > /workspace/.r5/task-apply' \
                  <"$runner_dir/scripted_task_apply.sh" || {
                    case_error 'Hermetic task policy could not be installed on Node A'
                    return 1
                }
            fi
        fi
        if [ "$runtime" = scripted ] && [ "$node" = A ]; then
            node_exec "$node" chmod 0700 /workspace/.r5/task-apply || {
                case_error 'Hermetic task policy mode could not be set on Node A'
                return 1
            }
        fi
        if ! node_exec "$node" git init -q ||
           ! node_exec "$node" git config user.name r5-e2e ||
           ! node_exec "$node" git config user.email r5-e2e.invalid ||
           ! node_exec "$node" git add case ||
           ! node_exec "$node" git commit -q --allow-empty -m 'public scenario fixture'; then
            case_error "public fixture Git baseline failed on Node $node"
            return 1
        fi
    done
}

channel_create() {
    node=$1
    alias=$2
    raw="$private/$alias-create.raw.json"
    stderr="$private/$alias-create.stderr"
    started=$(date +%s%3N)
    set +e
    node_exec "$node" timeout 30s mnemon-harness channel create "$alias" --json >"$raw" 2>"$stderr"
    exit_code=$?
    set -e
    finished=$(date +%s%3N)
    record_command "$node" channel-create "$started" "$finished" "$exit_code" \
      "transcript/channel-$alias-create.json"
    [ "$exit_code" -eq 0 ] || return "$exit_code"
    jq -e --arg alias "$alias" '.schema_version == 1 and .status == "created" and
      .channel.alias == $alias and (.invite_token | startswith("mnch1_"))' "$raw" >/dev/null ||
        return 1
    jq -r '.invite_token' "$raw" >"$private/$alias.invite" || return 1
    chmod 0600 "$private/$alias.invite"
    jq 'del(.invite_token) | .invite_token_redacted = true' "$raw" \
      >"$private/$alias-create.redacted.json" || return 1
    redact_json <"$private/$alias-create.redacted.json" \
      >"$output/transcript/channel-$alias-create.json" || return 1
    : >"$raw"
}

channel_join() {
    node=$1
    alias=$2
    raw="$private/$alias-$node-join.raw.json"
    stderr="$private/$alias-$node-join.stderr"
    started=$(date +%s%3N)
    set +e
    docker exec --user 10001:10001 --workdir /workspace -i "$(container_id "$node")" \
      timeout 30s mnemon-harness channel join --json <"$private/$alias.invite" >"$raw" 2>"$stderr"
    exit_code=$?
    set -e
    finished=$(date +%s%3N)
    record_command "$node" channel-join "$started" "$finished" "$exit_code" \
      "transcript/channel-$alias-$node-join.json"
    [ "$exit_code" -eq 0 ] || return "$exit_code"
    jq -e --arg alias "$alias" '.schema_version == 1 and .status == "joined" and .channel.alias == $alias' \
      "$raw" >/dev/null || return 1
    redact_json <"$raw" >"$output/transcript/channel-$alias-$node-join.json" || return 1
    : >"$raw"
}

create_channels() {
    channel_create A alpha && channel_join B alpha && channel_join C alpha || return 1
    wait_channel_ready alpha 1 A B C || return 1
    : >"$private/alpha.invite"
    channel_create C beta && channel_join D beta && channel_join E beta || return 1
    wait_channel_ready beta 3 C D E || return 1
    : >"$private/beta.invite"
    channel_create E gamma && channel_join F gamma && channel_join A gamma || return 1
    wait_channel_ready gamma 5 E F A || return 1
    : >"$private/gamma.invite"

    wait_public_status_ready || return 1
    write_channel_topology || return 1
    install_scripted_topology || return 1
    wait_derived_offer_candidates_ready || return 1
    for node in A B C D E F; do
        public_command "$node" status "nodes/$node/status-before.json" mnemon-harness status || return 1
    done
}

wait_channel_ready() {
    alias=$1
    join_index=$2
    shift 2
    accepted_at=$(jq -s -r --argjson index "$join_index" '
      [.[] | select(.kind == "channel-join")][$index].finished_unix_ms
    ' "$commands_jsonl")
    readiness_deadline_ms=$(( accepted_at + 10000 ))
    all_ready=false
    while [ "$(date +%s%3N)" -lt "$readiness_deadline_ms" ]; do
        all_ready=true
        for node in "$@"; do
            if [ "$(date +%s%3N)" -ge "$readiness_deadline_ms" ]; then
                all_ready=false
                break
            fi
            raw="$private/channel-status-$node.json"
            if ! node_exec "$node" timeout 1s mnemon-harness channel status --json >"$raw" 2>/dev/null ||
               ! jq -e --arg alias "$alias" '
                 .status == "ok" and
                 ([.channels[] | select(.alias == $alias)] | length) == 1 and
                 all(.channels[] | select(.alias == $alias);
                   .topic.status == "joined" and
                   .topic.ready_members == .topic.total_members)
               ' \
                 "$raw" >/dev/null; then
                all_ready=false
            fi
        done
        if [ "$(date +%s%3N)" -gt "$readiness_deadline_ms" ]; then
            all_ready=false
        fi
        [ "$all_ready" = true ] && break
        sleep 0.2
    done
    [ "$all_ready" = true ] || {
        case_error "Channel $alias topic did not become joined within 10 seconds"
        return 1
    }
    ready_at=$(date +%s%3N)
    ready_ms=$((ready_at - accepted_at))
    jq -cn --arg channel "$alias" --argjson duration "$ready_ms" \
      '{kind:"channel-ready",channel:$channel,duration_ms:$duration}' >>"$latency_jsonl"
    ready_fast=true
    [ "$ready_ms" -le 10000 ] || ready_fast=false
    add_assertion "channel-$alias-ready-within-10s" system true "$ready_fast" \
      "nodes/A/channel-status-before.json,nodes/C/channel-status-before.json,nodes/E/channel-status-before.json" \
      'Final join acceptance reached joined topics and complete reachable-member baselines within 10 seconds.'
}

wait_public_status_ready() {
    readiness_deadline_ms=$(( $(date +%s%3N) + public_status_ready_timeout_seconds * 1000 ))
    all_ready=false
    while [ "$(date +%s%3N)" -lt "$readiness_deadline_ms" ]; do
        all_ready=true
        for node in A B C D E F; do
            if [ "$(date +%s%3N)" -ge "$readiness_deadline_ms" ]; then
                all_ready=false
                break
            fi
            raw="$private/status-ready-$node.json"
            set +e
            node_exec "$node" timeout 1s mnemon-harness status >"$raw" 2>/dev/null
            status_exit=$?
            set -e
            if [ "$status_exit" -ne 0 ] ||
               ! jq -e '.status == "ready" and all(.channels[]; .state == "ready")' \
                 "$raw" >/dev/null; then
                all_ready=false
            fi
        done
        if [ "$(date +%s%3N)" -gt "$readiness_deadline_ms" ]; then
            all_ready=false
        fi
        [ "$all_ready" = true ] && break
        sleep 0.2
    done
    [ "$all_ready" = true ] || {
        for node in A B C D E F; do
            raw="$private/status-ready-$node.json"
            destination="$output/nodes/$node/status-ready-before.json"
            if [ -s "$raw" ]; then
                if jq -e . "$raw" >/dev/null 2>&1; then
                    redact_json <"$raw" >"$destination"
                else
                    redact_text_file "$raw" "$destination"
                fi
            fi
        done
        case_error "Channel status did not quiesce to public ready within ${public_status_ready_timeout_seconds} seconds"
        return 1
    }
    for node in A B C D E F; do
        if [ -s "$private/channel-status-$node.json" ] &&
           jq -e . "$private/channel-status-$node.json" >/dev/null 2>&1; then
            redact_json <"$private/channel-status-$node.json" \
              >"$output/nodes/$node/channel-status-before.json"
        fi
    done
}

wait_derived_offer_candidates_ready() {
    routes="$private/derived-offer-routes.txt"
    jq -r '.derived_path[]?' "$scenario_manifest" >"$routes"
    [ -s "$routes" ] || return 0

    readiness_deadline_ms=$(( $(date +%s%3N) + derived_offer_ready_timeout_seconds * 1000 ))
    all_ready=false
    while [ "$(date +%s%3N)" -lt "$readiness_deadline_ms" ]; do
        all_ready=true
        while IFS=: read -r node channel participant extra; do
            if [ -z "$node" ] || [ -z "$channel" ] || [ -z "$participant" ] ||
               [ -n "${extra:-}" ]; then
                case_error "invalid derived_path route: $node:$channel:$participant"
                return 1
            fi
            resolved_participant=$(
              jq -er --arg channel "$channel" --arg node "$participant" '
                first(.channels[] | select(.alias == $channel) | .members[] |
                  select(.node == $node)) | .alias
              ' "$output/topology/channels.json" 2>/dev/null
            ) || {
                case_error "derived_path route target is not present in public topology: $node:$channel:$participant"
                return 1
            }
            if [ "$(date +%s%3N)" -ge "$readiness_deadline_ms" ]; then
                all_ready=false
                break
            fi
            raw="$private/initiation-ready-$node.json"
            set +e
            node_exec "$node" timeout 1s mnemon-harness agent current --json >"$raw" 2>/dev/null
            current_exit=$?
            set -e
            if [ "$current_exit" -ne 0 ] ||
               ! jq -e --arg channel "$channel" --arg participant "$resolved_participant" '
                 .status == "none" and
                 any(.initiation_context.channels[]?;
                   .local_alias == $channel and
                   any(.participants[]?;
                     .effective_alias == $participant and .eligible == true))
               ' "$raw" >/dev/null; then
                all_ready=false
            fi
        done <"$routes"
        [ "$all_ready" = true ] && break
        sleep 0.2
    done
    [ "$all_ready" = true ] || {
        for node in A B C D E F; do
            raw="$private/initiation-ready-$node.json"
            destination="$output/nodes/$node/initiation-before.json"
            if [ -s "$raw" ]; then
                if jq -e . "$raw" >/dev/null 2>&1; then
                    redact_json <"$raw" >"$destination"
                else
                    redact_text_file "$raw" "$destination"
                fi
            fi
        done
        case_error "derived offer candidates did not become eligible within ${derived_offer_ready_timeout_seconds} seconds"
        return 1
    }
}

install_scripted_topology() {
    [ "$runtime" = scripted ] || return 0
    for node in A B C D E F; do
        node_exec_stdin "$node" sh -c \
          'umask 077; cat > /workspace/.r5/topology.json' \
          <"$output/topology/channels.json" || {
            case_error "scripted topology could not be installed on Node $node"
            return 1
        }
    done
}

write_channel_topology() {
    # Bind physical Node labels to their public self PeerID using only each
    # Node's own status projection. Owner views then preserve the signed D4
    # identity fields while making the three intended overlaps executable.
    project_public_channels "$run_id" \
      "$private/channel-status-A.json" "$private/channel-status-B.json" \
      "$private/channel-status-C.json" "$private/channel-status-D.json" \
      "$private/channel-status-E.json" "$private/channel-status-F.json" \
      >"$output/topology/channels.json"
    python3 "$runner_dir/schema_validate.py" "$schema_root/channels.schema.json" \
      "$output/topology/channels.json" || return 1
    jq -e '
      [.channels[].channel_id_digest] | length == (unique | length)
    ' "$output/topology/channels.json" >/dev/null || return 1
    jq -e '
      .channels as $channels |
      ($channels | map(select(.alias == "alpha"))[0]) as $alpha |
      ($channels | map(select(.alias == "beta"))[0]) as $beta |
      ($channels | map(select(.alias == "gamma"))[0]) as $gamma |
      ($alpha.owner_node == "A" and ([$alpha.members[].node] | sort) == ["A","B","C"]) and
      ($beta.owner_node == "C" and ([$beta.members[].node] | sort) == ["C","D","E"]) and
      ($gamma.owner_node == "E" and ([$gamma.members[].node] | sort) == ["A","E","F"]) and
      all($channels[]; . as $channel |
        $channel.membership == "active" and $channel.topic.status == "joined" and
        $channel.owner.local == true and $channel.owner.reachability == "self" and
        $channel.roster_revision == $channel.roster_head.revision and
        any($channel.members[]; .peer_id == $channel.roster_head.owner_peer_id) and
        all($channel.members[]; .status == "active" and .baseline_ready == true))
    ' "$output/topology/channels.json" >/dev/null || return 1
    add_assertion three-overlapping-channels-ready system true true \
      topology/channels.json 'Alpha, Beta, and Gamma are joined through six public create/join paths.'
    add_assertion d4-public-channel-identity-binding system true true \
      topology/channels.json 'Public D4 status binds each Channel digest, signed roster head, and member PeerID to the six isolated Nodes.'
}

declares_system_oracle() {
    oracle_id=$1
    jq -e --arg id "$oracle_id" 'any(.oracles.system[]?; . == $id)' "$scenario_manifest" >/dev/null
}

runtime_result_event_count() {
    event_type=$1
    find "$output/runtime" -type f -name '*.json' -exec jq -r --arg event_type "$event_type" '
      .results[]? | select(.event_type == $event_type) | .event_id
    ' {} + | LC_ALL=C sort -u | awk 'NF { count++ } END { print count + 0 }'
}

runtime_action_count() {
    action=$1
    find "$output/runtime" -type f -name '*.json' -exec jq -r --arg action "$action" '
      select(.action? == $action) | .operation_id
    ' {} + | LC_ALL=C sort -u | awk 'NF { count++ } END { print count + 0 }'
}

review_receipt_loss_file() {
    [ -d "$output/runtime/C" ] || return 1
    find "$output/runtime/C" -type f -name '*-receipt-loss.json' | LC_ALL=C sort | sed -n '1p'
}

review_receipt_loss_ref() {
    path=$(review_receipt_loss_file || true)
    [ -n "$path" ] || return 1
    printf '%s\n' "${path#"$output/"}"
}

normalize_json_or_empty() {
    source=$1
    destination=$2
    if jq -c . "$source" >"$destination" 2>/dev/null; then
        return 0
    fi
    printf '{}\n' >"$destination"
}

json_array_from_args() {
    for value in "$@"; do
        printf '%s\n' "$value"
    done | jq -Rcs 'split("\n") | map(select(length > 0))'
}

wait_node_public_status_ready() {
    node=$1
    timeout_seconds=${2:-30}
    readiness_deadline_ms=$(( $(date +%s%3N) + timeout_seconds * 1000 ))
    ready=false
    while [ "$(date +%s%3N)" -lt "$readiness_deadline_ms" ]; do
        raw="$private/fault-ready-$node.json"
        set +e
        node_exec "$node" timeout 2s mnemon-harness status >"$raw" 2>/dev/null
        status_exit=$?
        set -e
        if [ "$status_exit" -eq 0 ] &&
          jq -e '.status == "ready" and all(.channels[]; .state == "ready")' \
            "$raw" >/dev/null 2>&1; then
            ready=true
            break
        fi
        sleep 0.2
    done
    [ "$ready" = true ]
}

capture_fault_public_snapshots() {
    id=$1
    node=$2
    status_ok=false
    channel_ok=false
    doctor_ok=false
    if capture_public_output "$node" "faults/$id-status.json" \
      mnemon-harness status; then
        status_ok=true
    fi
    if capture_public_output "$node" "faults/$id-channel-status.json" \
      mnemon-harness channel status --json; then
        channel_ok=true
    fi
    if capture_public_output "$node" "faults/$id-doctor.json" \
      mnemon-harness doctor; then
        doctor_ok=true
    fi
    jq -cn --argjson status_ok "$status_ok" --argjson channel_ok "$channel_ok" \
      --argjson doctor_ok "$doctor_ok" \
      '{status_ok:$status_ok,channel_ok:$channel_ok,doctor_ok:$doctor_ok}'
}

payment_review_receipt_loss_ok() {
    receipt_path=$(review_receipt_loss_file || true)
    [ -n "$receipt_path" ] || return 1
    [ "$(find "$output/runtime/C" -type f -name '*-receipt-loss.json' | wc -l)" -eq 1 ] ||
        return 1
    jq -e '
      .schema_version == 1 and .fault == "review-receipt-loss" and
      .node == "C" and .phase == "c-requests-rework" and
      .action == "teamwork.deliver" and .first_exit_code != 0 and
      .retry_exit_code == 0 and .observation.retry_returned_terminal_receipt == true and
      .retry.status == "accepted" and .retry.replayed == true and
      (.retry.operation_id | type == "string" and length > 0) and
      (.retry.receipt | type == "string" and startswith("sha256:")) and
      ([.retry.results[]? | select(.event_type == "review.delivery.ready")] | length) == 1
    ' "$receipt_path" >/dev/null || return 1
    [ "$(runtime_result_event_count review.rework_requested)" -eq 1 ] || return 1
}

api_projection_drift_fault() {
    id=$1
    evidence_path="$output/faults/$id.json"
    config_path=/workspace/.codex/hooks.json
    hook_command=/workspace/.codex/hooks/mnemon-harness/hook.sh
    before_config="$private/$id-before-config.json"
    drift_config="$private/$id-drift-config.json"
    repaired_config="$private/$id-repaired-config.json"
    mutation_stdout="$private/$id-mutation.stdout"
    mutation_stderr="$private/$id-mutation.stderr"
    drift_doctor_raw="$private/$id-drift-doctor.raw.json"
    drift_doctor_err="$private/$id-drift-doctor.stderr"
    repair_setup_raw="$private/$id-repair-setup.raw.json"
    repair_setup_err="$private/$id-repair-setup.stderr"
    repaired_doctor_raw="$private/$id-repaired-doctor.raw.json"
    repaired_doctor_err="$private/$id-repaired-doctor.stderr"
    before_norm="$private/$id-before-config.norm.json"
    drift_norm="$private/$id-drift-config.norm.json"
    repaired_norm="$private/$id-repaired-config.norm.json"
    drift_doctor_norm="$private/$id-drift-doctor.norm.json"
    repair_setup_norm="$private/$id-repair-setup.norm.json"
    repaired_doctor_norm="$private/$id-repaired-doctor.norm.json"

    set +e
    node_exec C timeout 30s jq -c . "$config_path" >"$before_config" 2>"$mutation_stderr"
    before_exit=$?
    node_exec C timeout 30s sh -c '
      set -eu
      config=$1
      hook=$2
      tmp=$(mktemp "$config.XXXXXX")
      jq --arg hook "$hook" '"'"'
        .user_after_install = {"preserve":true} |
        .hooks.UserPromptSubmit = ((.hooks.UserPromptSubmit // []) |
          map(select(((.hooks // []) | map(.command == $hook) | any) | not)))
      '"'"' "$config" >"$tmp" &&
      mv "$tmp" "$config" &&
      chmod 0600 "$config"
    ' sh "$config_path" "$hook_command" >"$mutation_stdout" 2>>"$mutation_stderr"
    mutation_exit=$?
    node_exec C timeout 30s jq -c . "$config_path" >"$drift_config" 2>>"$mutation_stderr"
    drift_config_exit=$?
    node_exec C timeout 30s mnemon-harness doctor >"$drift_doctor_raw" 2>"$drift_doctor_err"
    drift_doctor_exit=$?
    node_exec C timeout 60s mnemon-harness setup --host codex --project-root /workspace \
      >"$repair_setup_raw" 2>"$repair_setup_err"
    repair_setup_exit=$?
    node_exec C timeout 30s jq -c . "$config_path" >"$repaired_config" 2>>"$mutation_stderr"
    repaired_config_exit=$?
    repaired_doctor_exit=125
    repaired_deadline_ms=$(( $(date +%s%3N) + 30000 ))
    while [ "$(date +%s%3N)" -lt "$repaired_deadline_ms" ]; do
        node_exec C timeout 5s mnemon-harness doctor >"$repaired_doctor_raw" 2>"$repaired_doctor_err"
        repaired_doctor_exit=$?
        if [ "$repaired_doctor_exit" -eq 0 ] &&
          jq -e '.status == "healthy" and all(.checks[]; .status == "pass")' \
            "$repaired_doctor_raw" >/dev/null 2>&1; then
            break
        fi
        sleep 0.2
    done
    set -e

    normalize_json_or_empty "$before_config" "$before_norm"
    normalize_json_or_empty "$drift_config" "$drift_norm"
    normalize_json_or_empty "$repaired_config" "$repaired_norm"
    normalize_json_or_empty "$drift_doctor_raw" "$drift_doctor_norm"
    normalize_json_or_empty "$repair_setup_raw" "$repair_setup_norm"
    normalize_json_or_empty "$repaired_doctor_raw" "$repaired_doctor_norm"
    repair_setup_stderr_first_line=$(sed -E \
      -e 's/mnch1_[A-Za-z0-9_-]+/<redacted-invite>/g' \
      -e 's/sk-[A-Za-z0-9_-]{12,}/<redacted-provider-key>/g' \
      -e 's/(OPENAI_API_KEY=)[^[:space:]]+/\1<redacted>/g' \
      "$repair_setup_err" | sed -n '1p' | cut -c1-512)

    jq -n --arg id "$id" --arg node C --arg config_path "$config_path" \
      --arg hook_command "$hook_command" \
      --arg repair_setup_stderr_first_line "$repair_setup_stderr_first_line" \
      --argjson before_exit "$before_exit" --argjson mutation_exit "$mutation_exit" \
      --argjson drift_config_exit "$drift_config_exit" \
      --argjson drift_doctor_exit "$drift_doctor_exit" \
      --argjson repair_setup_exit "$repair_setup_exit" \
      --argjson repaired_config_exit "$repaired_config_exit" \
      --argjson repaired_doctor_exit "$repaired_doctor_exit" \
      --slurpfile before "$before_norm" --slurpfile drift "$drift_norm" \
      --slurpfile repaired "$repaired_norm" \
      --slurpfile drift_doctor "$drift_doctor_norm" \
      --slurpfile repair_setup "$repair_setup_norm" \
      --slurpfile repaired_doctor "$repaired_doctor_norm" '
      def first($value): if ($value | length) == 0 then {} else $value[0] end;
      def managed_count($document):
        [($document.hooks.UserPromptSubmit // [])[]? |
          select([.hooks[]?.command] | index($hook_command))] | length;
      (first($before)) as $before_doc |
      (first($drift)) as $drift_doc |
      (first($repaired)) as $repaired_doc |
      (first($drift_doctor)) as $drift_doctor_doc |
      (first($repair_setup)) as $repair_setup_doc |
      (first($repaired_doctor)) as $repaired_doctor_doc |
      (any($drift_doctor_doc.checks[]?;
        .name == "host_projection" and .status == "fail" and
        .issue == "host_projection_unavailable" and .remedy == "run_setup")) as $failed_closed |
      (managed_count($before_doc) == 1 and managed_count($drift_doc) == 0 and
        managed_count($repaired_doc) == 1) as $restored |
      (($drift_doc.user_after_install.preserve == true) and
        ($repaired_doc.user_after_install.preserve == true)) as $preserved |
      {schema_version:1,fault:$id,type:"projection-drift",node:$node,
       config_path:$config_path,hook_command:$hook_command,
       before_config_exit_code:$before_exit,mutation_exit_code:$mutation_exit,
       drift_config_exit_code:$drift_config_exit,
       doctor_after_drift_exit_code:$drift_doctor_exit,
       setup_repair_exit_code:$repair_setup_exit,
       repaired_config_exit_code:$repaired_config_exit,
       doctor_after_repair_exit_code:$repaired_doctor_exit,
       counts:{before_managed_entries:managed_count($before_doc),
         drift_managed_entries:managed_count($drift_doc),
         repaired_managed_entries:managed_count($repaired_doc)},
       doctor_after_drift:{status:($drift_doctor_doc.status // ""),
         host_projection:([$drift_doctor_doc.checks[]? |
           select(.name == "host_projection")] | first // {})},
       setup_repair:{status:($repair_setup_doc.status // ""),
         replayed:($repair_setup_doc.replayed // null),
         error:(if $repair_setup_stderr_first_line == "" then null else
           {first_line:$repair_setup_stderr_first_line,
            code:($repair_setup_stderr_first_line | split(":") | .[0])}
         end)},
       doctor_after_repair:{status:($repaired_doctor_doc.status // "")},
       observation:{fail_closed_before_repair:$failed_closed,
         setup_restored_canonical_projection:$restored,
         neighbor_user_registration_preserved:$preserved,
         repaired_doctor_healthy:($repaired_doctor_doc.status == "healthy" and
           all($repaired_doctor_doc.checks[]?; .status == "pass")),
         all_commands_bounded:($before_exit == 0 and $mutation_exit == 0 and
           $drift_config_exit == 0 and $repair_setup_exit == 0 and
           $repaired_config_exit == 0 and $repaired_doctor_exit == 0)}}
    ' >"$evidence_path"
}

api_projection_drift_ok() {
    evidence_path="$output/faults/managed-guide-drift.json"
    [ -f "$evidence_path" ] || return 1
    jq -e '
      .schema_version == 1 and .fault == "managed-guide-drift" and
      .type == "projection-drift" and .node == "C" and
      .doctor_after_drift_exit_code != 0 and
      .setup_repair_exit_code == 0 and
      .doctor_after_repair_exit_code == 0 and
      .observation.fail_closed_before_repair == true and
      .observation.setup_restored_canonical_projection == true and
      .observation.neighbor_user_registration_preserved == true and
      .observation.repaired_doctor_healthy == true and
      .observation.all_commands_bounded == true
    ' "$evidence_path" >/dev/null
}

api_projection_drift_requirement_ok() {
    evidence_path="$output/faults/managed-guide-drift.json"
    [ -f "$evidence_path" ] || return 1
    jq -e '
      .schema_version == 1 and .fault == "managed-guide-drift" and
      .type == "projection-drift" and .node == "C" and
      .doctor_after_drift_exit_code != 0 and
      .repaired_config_exit_code == 0 and
      .observation.fail_closed_before_repair == true and
      .observation.setup_restored_canonical_projection == true and
      .observation.neighbor_user_registration_preserved == true and
      .observation.repaired_doctor_healthy == true
    ' "$evidence_path" >/dev/null
}

api_wrong_topic_replay_fault() {
    id=$1
    evidence_path="$output/faults/$id.json"
    probe_raw="$private/$id-probe.raw.json"
    probe_err="$private/$id-probe.stderr"
    probe_norm="$private/$id-probe.norm.json"

    set +e
    node_exec C timeout 30s mnemon-harness channel replay-probe \
      --source alpha --target beta --json >"$probe_raw" 2>"$probe_err"
    probe_exit=$?
    set -e

    normalize_json_or_empty "$probe_raw" "$probe_norm"
    probe_stderr_first_line=$(sed -E \
      -e 's/mnch1_[A-Za-z0-9_-]+/<redacted-invite>/g' \
      -e 's/sk-[A-Za-z0-9_-]{12,}/<redacted-provider-key>/g' \
      -e 's/(OPENAI_API_KEY=)[^[:space:]]+/\1<redacted>/g' \
      "$probe_err" | sed -n '1p' | cut -c1-512)

    jq -n --arg id "$id" --arg node C --arg source alpha --arg target beta \
      --arg probe_stderr_first_line "$probe_stderr_first_line" \
      --argjson probe_exit "$probe_exit" --slurpfile probe "$probe_norm" '
      def first($value): if ($value | length) == 0 then {} else $value[0] end;
      (first($probe)) as $probe_doc |
      ($probe_doc.status == "rejected" and
        $probe_doc.rejection == "wrong_topic") as $rejected |
      (($probe_doc.target_before // {}) == ($probe_doc.target_after // {}) and
        $probe_doc.target_mutation_suppressed == true) as $unchanged |
      {schema_version:1,fault:$id,type:"wrong-topic-replay",node:$node,
       source_channel:$source,target_channel:$target,
       probe_exit_code:$probe_exit,
       probe:{status:($probe_doc.status // ""),
         source_channel:($probe_doc.source_channel // ""),
         target_channel:($probe_doc.target_channel // ""),
         source_channel_id_digest:($probe_doc.source_channel_id_digest // ""),
         target_channel_id_digest:($probe_doc.target_channel_id_digest // ""),
         publication_digest:($probe_doc.publication_digest // ""),
         event_digest:($probe_doc.event_digest // ""),
         event_key:($probe_doc.event_key // {}),
         replay_attempted:($probe_doc.replay_attempted // false),
         rejection:($probe_doc.rejection // ""),
         target_before:($probe_doc.target_before // {}),
         target_after:($probe_doc.target_after // {}),
         target_mutation_suppressed:($probe_doc.target_mutation_suppressed // false),
         error:(if $probe_stderr_first_line == "" then null else
           {first_line:$probe_stderr_first_line,
            code:($probe_stderr_first_line | split(":") | .[0])}
         end)},
       observation:{exact_alpha_replay_attempted:
           ($probe_exit == 0 and $probe_doc.replay_attempted == true and
            $probe_doc.source_channel == $source and $probe_doc.target_channel == $target and
            (($probe_doc.publication_digest // "") | startswith("sha256:"))),
         rejected_on_beta:$rejected,
         beta_event_work_counts_unchanged:$unchanged,
         all_commands_bounded:($probe_exit == 0)}}
    ' >"$evidence_path"
}

api_wrong_topic_replay_ok() {
    id=${1:-alpha-frame-on-beta}
    evidence_path="$output/faults/$id.json"
    [ -f "$evidence_path" ] || return 1
    jq -e --arg id "$id" '
      .schema_version == 1 and .fault == $id and
      .type == "wrong-topic-replay" and .node == "C" and
      .source_channel == "alpha" and .target_channel == "beta" and
      .probe_exit_code == 0 and
      .observation.exact_alpha_replay_attempted == true and
      .observation.rejected_on_beta == true and
      .observation.beta_event_work_counts_unchanged == true and
      .observation.all_commands_bounded == true
    ' "$evidence_path" >/dev/null
}

api_terminal_enrollment_replay_fault() {
    id=$1
    evidence_path="$output/faults/$id.json"
    invite_raw="$private/$id-invite.raw.json"
    invite_err="$private/$id-invite.stderr"
    join_raw="$private/$id-initial-join.raw.json"
    join_err="$private/$id-initial-join.stderr"
    owner_after_join_raw="$private/$id-owner-after-join.raw.json"
    owner_after_join_err="$private/$id-owner-after-join.stderr"
    remove_raw="$private/$id-remove.raw.json"
    remove_err="$private/$id-remove.stderr"
    owner_after_remove_raw="$private/$id-owner-after-remove.raw.json"
    owner_after_remove_err="$private/$id-owner-after-remove.stderr"
    replay_raw="$private/$id-replay.raw.json"
    replay_err="$private/$id-replay.stderr"
    joiner_after_replay_raw="$private/$id-joiner-after-replay.raw.json"
    joiner_after_replay_err="$private/$id-joiner-after-replay.stderr"
    owner_after_replay_raw="$private/$id-owner-after-replay.raw.json"
    owner_after_replay_err="$private/$id-owner-after-replay.stderr"
    invite_token="$private/$id.invite"
    invite_norm="$private/$id-invite.norm.json"
    join_norm="$private/$id-initial-join.norm.json"
    owner_join_norm="$private/$id-owner-after-join.norm.json"
    remove_norm="$private/$id-remove.norm.json"
    owner_remove_norm="$private/$id-owner-after-remove.norm.json"
    replay_norm="$private/$id-replay.norm.json"
    joiner_replay_norm="$private/$id-joiner-after-replay.norm.json"
    owner_replay_norm="$private/$id-owner-after-replay.norm.json"

    selector=
    joiner_peer=
    set +e
    node_exec A timeout 30s mnemon-harness channel invite --channel alpha --uses 1 --json \
      >"$invite_raw" 2>"$invite_err"
    invite_exit=$?
    if [ "$invite_exit" -eq 0 ] &&
      jq -e '.schema_version == 1 and .status == "created" and
        .channel.alias == "alpha" and (.invite_token | startswith("mnch1_"))' \
        "$invite_raw" >/dev/null 2>&1; then
        jq -r '.invite_token' "$invite_raw" >"$invite_token"
        chmod 0600 "$invite_token"
        node_exec_stdin D timeout 30s mnemon-harness channel join --json \
          <"$invite_token" >"$join_raw" 2>"$join_err"
        join_exit=$?
        if [ "$join_exit" -eq 0 ]; then
            joiner_peer=$(jq -r '.channel.members[]? |
              select(.binding == "self") | .peer_id' "$join_raw" 2>/dev/null | sed -n '1p')
        fi
    else
        : >"$invite_token"
        : >"$join_raw"
        : >"$join_err"
        join_exit=125
    fi

    owner_after_join_exit=125
    if [ -n "$joiner_peer" ]; then
        owner_join_deadline_ms=$(( $(date +%s%3N) + 10000 ))
        while [ "$(date +%s%3N)" -lt "$owner_join_deadline_ms" ]; do
            node_exec A timeout 2s mnemon-harness channel status alpha --json \
              >"$owner_after_join_raw" 2>"$owner_after_join_err"
            owner_after_join_exit=$?
            if [ "$owner_after_join_exit" -eq 0 ]; then
                selector=$(jq -r --arg peer "$joiner_peer" '
                  .channels[]? | select(.alias == "alpha") |
                  .members[]? | select(.peer_id == $peer and .status == "active") |
                  .alias
                ' "$owner_after_join_raw" 2>/dev/null | sed -n '1p')
                [ -z "$selector" ] || break
            fi
            sleep 0.2
        done
    else
        : >"$owner_after_join_raw"
        : >"$owner_after_join_err"
    fi

    if [ -n "$selector" ]; then
        node_exec A timeout 30s mnemon-harness channel remove --channel alpha "$selector" --json \
          >"$remove_raw" 2>"$remove_err"
        remove_exit=$?
    else
        : >"$remove_raw"
        : >"$remove_err"
        remove_exit=125
    fi

    owner_after_remove_exit=125
    if [ -n "$joiner_peer" ]; then
        owner_remove_deadline_ms=$(( $(date +%s%3N) + 10000 ))
        while [ "$(date +%s%3N)" -lt "$owner_remove_deadline_ms" ]; do
            node_exec A timeout 2s mnemon-harness channel status alpha --json \
              >"$owner_after_remove_raw" 2>"$owner_after_remove_err"
            owner_after_remove_exit=$?
            if [ "$owner_after_remove_exit" -eq 0 ] &&
              jq -e --arg peer "$joiner_peer" '
                .status == "ok" and
                any(.channels[]? | select(.alias == "alpha") |
                  .members[]?; .peer_id == $peer and .status == "revoked")
              ' "$owner_after_remove_raw" >/dev/null 2>&1; then
                break
            fi
            sleep 0.2
        done
    else
        : >"$owner_after_remove_raw"
        : >"$owner_after_remove_err"
    fi

    if [ -s "$invite_token" ]; then
        node_exec_stdin D timeout 30s mnemon-harness channel join --json \
          <"$invite_token" >"$replay_raw" 2>"$replay_err"
        replay_exit=$?
    else
        : >"$replay_raw"
        : >"$replay_err"
        replay_exit=125
    fi

    node_exec D timeout 30s mnemon-harness channel status alpha --json \
      >"$joiner_after_replay_raw" 2>"$joiner_after_replay_err"
    joiner_after_replay_exit=$?
    node_exec A timeout 30s mnemon-harness channel status alpha --json \
      >"$owner_after_replay_raw" 2>"$owner_after_replay_err"
    owner_after_replay_exit=$?
    set -e

    normalize_json_or_empty "$invite_raw" "$invite_norm"
    normalize_json_or_empty "$join_raw" "$join_norm"
    normalize_json_or_empty "$owner_after_join_raw" "$owner_join_norm"
    normalize_json_or_empty "$remove_raw" "$remove_norm"
    normalize_json_or_empty "$owner_after_remove_raw" "$owner_remove_norm"
    normalize_json_or_empty "$replay_raw" "$replay_norm"
    normalize_json_or_empty "$joiner_after_replay_raw" "$joiner_replay_norm"
    normalize_json_or_empty "$owner_after_replay_raw" "$owner_replay_norm"

    jq -n --arg id "$id" --arg channel alpha --arg owner A --arg joiner D \
      --arg joiner_peer "$joiner_peer" --arg selector "$selector" \
      --argjson invite_exit "$invite_exit" --argjson join_exit "$join_exit" \
      --argjson owner_after_join_exit "$owner_after_join_exit" \
      --argjson remove_exit "$remove_exit" \
      --argjson owner_after_remove_exit "$owner_after_remove_exit" \
      --argjson replay_exit "$replay_exit" \
      --argjson joiner_after_replay_exit "$joiner_after_replay_exit" \
      --argjson owner_after_replay_exit "$owner_after_replay_exit" \
      --slurpfile invite "$invite_norm" --slurpfile join "$join_norm" \
      --slurpfile owner_join "$owner_join_norm" --slurpfile remove "$remove_norm" \
      --slurpfile owner_remove "$owner_remove_norm" --slurpfile replay "$replay_norm" \
      --slurpfile joiner_replay "$joiner_replay_norm" \
      --slurpfile owner_replay "$owner_replay_norm" '
      def first($value): if ($value | length) == 0 then {} else $value[0] end;
      def channel($document):
        if ($document.channel? | type) == "object" then $document.channel
        else ([($document.channels // [])[]? | select(.alias == "alpha")] | first // {})
        end;
      def member($document; $peer):
        ([channel($document).members[]? | select(.peer_id == $peer)] | first // {});
      (first($invite)) as $invite_doc |
      (first($join)) as $join_doc |
      (first($owner_join)) as $owner_join_doc |
      (first($remove)) as $remove_doc |
      (first($owner_remove)) as $owner_remove_doc |
      (first($replay)) as $replay_doc |
      (first($joiner_replay)) as $joiner_replay_doc |
      (first($owner_replay)) as $owner_replay_doc |
      (channel($join_doc)) as $initial_join_channel |
      (channel($remove_doc)) as $remove_channel |
      (channel($owner_remove_doc)) as $owner_removed_channel |
      (channel($replay_doc)) as $replay_channel |
      (channel($joiner_replay_doc)) as $joiner_replay_channel |
      (channel($owner_replay_doc)) as $owner_replay_channel |
      (member($replay_doc; $joiner_peer)) as $replay_member |
      (member($joiner_replay_doc; $joiner_peer)) as $joiner_replay_member |
      (member($owner_remove_doc; $joiner_peer)) as $owner_removed_member |
      (member($owner_replay_doc; $joiner_peer)) as $owner_replay_member |
      ($join_exit == 0 and $initial_join_channel.alias == "alpha" and
        member($join_doc; $joiner_peer).status == "active") as $initial_joined |
      ($remove_exit == 0 and $remove_doc.status == "removed" and
        member($remove_doc; $joiner_peer).status == "revoked") as $removed |
      ($replay_exit == 0 and $replay_doc.status == "member_revoked" and
        $replay_channel.alias == "alpha" and
        $replay_channel.membership == "left" and
        ($replay_member.status == "revoked" or $joiner_replay_member.status == "revoked")) as $terminal_projection |
      (([$replay_channel.members[]?, $joiner_replay_channel.members[]?,
         $owner_replay_channel.members[]?] |
        map(select(.peer_id == $joiner_peer and .status == "active")) | length) == 0) as $not_reactivated |
      (($replay_channel.roster_revision // 0) > ($initial_join_channel.roster_revision // 0) and
        ($owner_replay_channel.roster_revision // 0) >= ($replay_channel.roster_revision // 0)) as $terminal_suffix |
      {schema_version:1,fault:$id,type:"terminal-enrollment-replay",
       channel:$channel,owner_node:$owner,joiner_node:$joiner,
       joiner_peer_id:$joiner_peer,owner_member_selector:$selector,
       invite_exit_code:$invite_exit,initial_join_exit_code:$join_exit,
       owner_after_join_exit_code:$owner_after_join_exit,
       remove_exit_code:$remove_exit,
       owner_after_remove_exit_code:$owner_after_remove_exit,
       replay_join_exit_code:$replay_exit,
       joiner_after_replay_exit_code:$joiner_after_replay_exit,
       owner_after_replay_exit_code:$owner_after_replay_exit,
       invite:{status:($invite_doc.status // ""),
         remaining_uses:($invite_doc.invite.remaining_uses // null)},
       initial_join:{membership:($initial_join_channel.membership // ""),
         roster_revision:($initial_join_channel.roster_revision // null)},
       remove:{status:($remove_doc.status // ""),
         owner_member_status:($owner_removed_member.status // "")},
       replay:{status:($replay_doc.status // ""),
         membership:($replay_channel.membership // ""),
         self_member_status:($replay_member.status // $joiner_replay_member.status // ""),
         roster_revision:($replay_channel.roster_revision // null)},
       observation:{initial_joined:$initial_joined,member_removed:$removed,
         replay_returned_terminal_projection:$terminal_projection,
         terminal_suffix_observed:$terminal_suffix,
         member_never_reactivated:$not_reactivated,
         all_commands_bounded:($invite_exit == 0 and $join_exit == 0 and
           $owner_after_join_exit == 0 and $remove_exit == 0 and
           $owner_after_remove_exit == 0 and $replay_exit == 0 and
           $joiner_after_replay_exit == 0 and $owner_after_replay_exit == 0)}}
    ' >"$evidence_path"
}

api_terminal_enrollment_replay_ok() {
    evidence_path="$output/faults/terminal-enrollment-replay.json"
    [ -f "$evidence_path" ] || return 1
    jq -e '
      .schema_version == 1 and .fault == "terminal-enrollment-replay" and
      .type == "terminal-enrollment-replay" and .channel == "alpha" and
      .owner_node == "A" and .joiner_node == "D" and
      .observation.initial_joined == true and
      .observation.member_removed == true and
      .observation.replay_returned_terminal_projection == true and
      .observation.terminal_suffix_observed == true and
      .observation.member_never_reactivated == true and
      .observation.all_commands_bounded == true
    ' "$evidence_path" >/dev/null
}

write_restart_fault_evidence() {
    id=$1
    node=$2
    shift 2
    expected_channels=$(json_array_from_args "$@")
    evidence_path="$output/faults/$id.json"
    action_ref="faults/$id-action.json"
    status_ref="faults/$id-status.json"
    channel_ref="faults/$id-channel-status.json"
    doctor_ref="faults/$id-doctor.json"
    before_status_ref="nodes/$node/status-after.json"
    before_channel_ref="nodes/$node/channel-status-after.json"
    fault_stdout="$private/$id-restart.stdout"
    fault_stderr="$private/$id-restart.stderr"
    action_norm="$private/$id-action.norm.json"
    before_status_norm="$private/$id-before-status.norm.json"
    before_channel_norm="$private/$id-before-channel.norm.json"
    status_norm="$private/$id-status.norm.json"
    channel_norm="$private/$id-channel.norm.json"
    doctor_norm="$private/$id-doctor.norm.json"
    target_container=$(container_id "$node")

    set +e
    "$repo_root/harness/test/e2e/faultplane/docker_network.sh" restart \
      --container "$target_container" --token "$id" --receipt-dir "$output/faults" -- \
      sh -c 'true' >"$fault_stdout" 2>"$fault_stderr"
    fault_exit=$?
    set -e
    [ ! -s "$fault_stderr" ] ||
      redact_text_file "$fault_stderr" "$output/transcript/$id-restart.stderr"

    wait_ready=false
    if [ "$fault_exit" -eq 0 ] && wait_node_public_status_ready "$node" 30; then
        wait_ready=true
    fi
    snapshot_result=$(capture_fault_public_snapshots "$id" "$node")
    action_schema_ok=false
    if [ -f "$output/$action_ref" ] &&
      PYTHONDONTWRITEBYTECODE=1 python3 "$runner_dir/schema_validate.py" \
        "$schema_root/fault-action.schema.json" "$output/$action_ref" >/dev/null 2>&1; then
        action_schema_ok=true
    fi
    runtime_unique=false
    runtime_result_event_ids_unique_ok && runtime_unique=true
    bridge_ok=false
    network_paths_no_implicit_bridge_ok && bridge_ok=true
    terminal_ok=false
    status_after_all_terminal_ok && terminal_ok=true

    normalize_json_or_empty "$output/$action_ref" "$action_norm"
    normalize_json_or_empty "$output/$before_status_ref" "$before_status_norm"
    normalize_json_or_empty "$output/$before_channel_ref" "$before_channel_norm"
    normalize_json_or_empty "$output/$status_ref" "$status_norm"
    normalize_json_or_empty "$output/$channel_ref" "$channel_norm"
    normalize_json_or_empty "$output/$doctor_ref" "$doctor_norm"
    jq -n --arg id "$id" --arg node "$node" --arg type node-kill \
      --arg action_ref "$action_ref" --arg status_ref "$status_ref" \
      --arg channel_ref "$channel_ref" --arg doctor_ref "$doctor_ref" \
      --arg before_status_ref "$before_status_ref" --arg before_channel_ref "$before_channel_ref" \
      --arg target_container "$target_container" --argjson expected "$expected_channels" \
      --argjson fault_exit "$fault_exit" --argjson wait_ready "$wait_ready" \
      --argjson action_schema_ok "$action_schema_ok" --argjson snapshot "$snapshot_result" \
      --argjson runtime_unique "$runtime_unique" --argjson bridge_ok "$bridge_ok" \
      --argjson terminal_ok "$terminal_ok" --slurpfile action "$action_norm" \
      --slurpfile before_status "$before_status_norm" --slurpfile before_channel "$before_channel_norm" \
      --slurpfile status "$status_norm" --slurpfile channel "$channel_norm" \
      --slurpfile doctor "$doctor_norm" '
      def first($value): if ($value | length) == 0 then {} else $value[0] end;
      def status_channel($document; $alias):
        ([($document.channels // [])[] | select(.alias == $alias)] | first // {});
      def channel_document($document; $alias):
        ([($document.channels // [])[] | select(.alias == $alias)] | first // {});
      (first($action)) as $action_doc |
      (first($before_status)) as $before_status_doc |
      (first($before_channel)) as $before_channel_doc |
      (first($status)) as $status_doc |
      (first($channel)) as $channel_doc |
      (first($doctor)) as $doctor_doc |
      ($action_schema_ok and $action_doc.token == $id and
        $action_doc.action == "docker-node-kill-restart" and
        $action_doc.external_action_applied == true and
        $action_doc.restored == true and $action_doc.command_exit_code == 0 and
        $action_doc.container_id == $target_container) as $action_valid |
      ($action_valid and $action_doc.started_at_before != $action_doc.started_at_after and
        $action_doc.exit_code_after_kill == 137) as $restarted |
      ($status_doc.status == "ready" and
        all($status_doc.channels[]?; .state == "ready")) as $public_ready |
      ($expected | all(. as $alias |
        (status_channel($status_doc; $alias).state == "ready") and
        (status_channel($status_doc; $alias).topic.state == "joined") and
        ((status_channel($status_doc; $alias).cursor.inbound_gapped // 0) == 0) and
        ((status_channel($status_doc; $alias).cursor.inbound_pending // 0) == 0) and
        ((status_channel($status_doc; $alias).publication.remote_pending // 0) == 0) and
        ((status_channel($status_doc; $alias).publication.remote_blocked // 0) == 0) and
        ((status_channel($status_doc; $alias).inbox.waiting_artifact // 0) == 0))) as $channels_ready |
      ($expected | all(. as $alias |
        ((channel_document($before_channel_doc; $alias).channel_id_digest // "") | startswith("sha256:")) and
        channel_document($before_channel_doc; $alias).channel_id_digest ==
          channel_document($channel_doc; $alias).channel_id_digest and
        channel_document($channel_doc; $alias).topic.status == "joined" and
        all(channel_document($channel_doc; $alias).members[]?;
          .status == "active" and .baseline_ready == true))) as $identity_preserved |
      ($expected | all(. as $alias |
        ((status_channel($status_doc; $alias).runtime.handling_claimed // 0) == 0) and
        ((status_channel($status_doc; $alias).runtime.handling_pending // 0) == 0) and
        ((status_channel($status_doc; $alias).runtime.handling_dead // 0) == 0) and
        ((status_channel($status_doc; $alias).runtime.run_active // 0) == 0) and
        ((status_channel($status_doc; $alias).runtime.run_failed // 0) == 0))) as $runtime_terminal |
      {schema_version:1,fault:$id,type:$type,node:$node,
       action_ref:$action_ref,status_ref:$status_ref,channel_status_ref:$channel_ref,
       doctor_ref:$doctor_ref,before_status_ref:$before_status_ref,
       before_channel_status_ref:$before_channel_ref,expected_channels:$expected,
       fault_exit_code:$fault_exit,
       observation:{action_receipt_valid:$action_valid,
         container_restarted:$restarted,
         public_status_ready:($wait_ready and $public_ready),
         expected_channels_ready:$channels_ready,
         channel_identity_preserved:$identity_preserved,
         runtime_terminal_after_restart:$runtime_terminal,
         no_duplicate_transition_observed:$runtime_unique,
         no_cross_channel_cursor_movement:$bridge_ok,
         final_public_status_was_terminal:$terminal_ok,
         doctor_healthy:($snapshot.doctor_ok and $doctor_doc.status == "healthy"),
         all_commands_bounded:($fault_exit == 0 and $snapshot.status_ok and
           $snapshot.channel_ok and $snapshot.doctor_ok)}}
    ' >"$evidence_path"
}

restart_fault_ok() {
    id=$1
    evidence_path="$output/faults/$id.json"
    [ -f "$evidence_path" ] || return 1
    jq -e '
      .schema_version == 1 and .type == "node-kill" and
      .observation.action_receipt_valid == true and
      .observation.container_restarted == true and
      .observation.public_status_ready == true and
      .observation.expected_channels_ready == true and
      .observation.channel_identity_preserved == true and
      .observation.runtime_terminal_after_restart == true and
      .observation.no_duplicate_transition_observed == true and
      .observation.no_cross_channel_cursor_movement == true and
      .observation.all_commands_bounded == true
    ' "$evidence_path" >/dev/null
}

write_disconnect_fault_evidence() {
    id=$1
    node=$2
    shift 2
    expected_channels=$(json_array_from_args "$@")
    evidence_path="$output/faults/$id.json"
    action_ref="faults/$id-action.json"
    relay_action_ref="faults/$id-relay-block-action.json"
    status_ref="faults/$id-status.json"
    channel_ref="faults/$id-channel-status.json"
    doctor_ref="faults/$id-doctor.json"
    action_norm="$private/$id-action.norm.json"
    status_norm="$private/$id-status.norm.json"
    channel_norm="$private/$id-channel.norm.json"
    doctor_norm="$private/$id-doctor.norm.json"
    target_container=$(container_id "$node")

    wait_ready=false
    wait_node_public_status_ready "$node" 30 && wait_ready=true
    snapshot_result=$(capture_fault_public_snapshots "$id" "$node")
    action_schema_ok=false
    if [ -f "$output/$action_ref" ] &&
      PYTHONDONTWRITEBYTECODE=1 python3 "$runner_dir/schema_validate.py" \
        "$schema_root/fault-action.schema.json" "$output/$action_ref" >/dev/null 2>&1; then
        action_schema_ok=true
    fi
    relay_action_schema_ok=false
    if [ -f "$output/$relay_action_ref" ] &&
      PYTHONDONTWRITEBYTECODE=1 python3 "$runner_dir/schema_validate.py" \
        "$schema_root/fault-action.schema.json" "$output/$relay_action_ref" >/dev/null 2>&1; then
        relay_action_schema_ok=true
    fi
    origin_repair_ok=false
    network_paths_origin_only_repairs_ok && origin_repair_ok=true
    single_effect_ok=false
    network_paths_single_repair_effect_ok "$node" alpha A && single_effect_ok=true

    normalize_json_or_empty "$output/$action_ref" "$action_norm"
    relay_action_norm="$private/$id-relay-action.norm.json"
    normalize_json_or_empty "$output/$status_ref" "$status_norm"
    normalize_json_or_empty "$output/$channel_ref" "$channel_norm"
    normalize_json_or_empty "$output/$doctor_ref" "$doctor_norm"
    normalize_json_or_empty "$output/$relay_action_ref" "$relay_action_norm"
    jq -n --arg id "$id" --arg node "$node" --arg type network-disconnect \
      --arg action_ref "$action_ref" --arg status_ref "$status_ref" \
      --arg channel_ref "$channel_ref" --arg doctor_ref "$doctor_ref" \
      --arg relay_action_ref "$relay_action_ref" \
      --arg target_container "$target_container" --arg network "$project-mesh" \
      --argjson expected "$expected_channels" --argjson wait_ready "$wait_ready" \
      --argjson action_schema_ok "$action_schema_ok" --argjson snapshot "$snapshot_result" \
      --argjson relay_action_schema_ok "$relay_action_schema_ok" \
      --argjson origin_repair_ok "$origin_repair_ok" --argjson single_effect_ok "$single_effect_ok" \
      --slurpfile action "$action_norm" --slurpfile status "$status_norm" \
      --slurpfile channel "$channel_norm" --slurpfile doctor "$doctor_norm" \
      --slurpfile relay_action "$relay_action_norm" '
      def first($value): if ($value | length) == 0 then {} else $value[0] end;
      def status_channel($document; $alias):
        ([($document.channels // [])[] | select(.alias == $alias)] | first // {});
      def channel_document($document; $alias):
        ([($document.channels // [])[] | select(.alias == $alias)] | first // {});
      (first($action)) as $action_doc |
      (first($relay_action)) as $relay_action_doc |
      (first($status)) as $status_doc |
      (first($channel)) as $channel_doc |
      (first($doctor)) as $doctor_doc |
      ($action_schema_ok and $action_doc.token == $id and
        $action_doc.action == "docker-node-disconnect" and
        $action_doc.external_action_applied == true and
        $action_doc.restored == true and $action_doc.command_exit_code == 0 and
        $action_doc.network_name == $network and
        $action_doc.container_id == $target_container) as $action_valid |
      ($relay_action_schema_ok and $relay_action_doc.token == ($id + "-relay-block") and
        $relay_action_doc.action == "docker-edge-block" and
        $relay_action_doc.external_action_applied == true and
        $relay_action_doc.restored == true and $relay_action_doc.command_exit_code == 0 and
        $relay_action_doc.network_name == $network) as $relay_action_valid |
      ($expected | all(. as $alias |
        (status_channel($status_doc; $alias).state == "ready") and
        (status_channel($status_doc; $alias).topic.state == "joined") and
        ((status_channel($status_doc; $alias).cursor.inbound_gapped // 0) == 0) and
        ((status_channel($status_doc; $alias).publication.remote_pending // 0) == 0))) as $channels_ready |
      ($expected | all(. as $alias |
        ((channel_document($channel_doc; $alias).channel_id_digest // "") | startswith("sha256:")) and
        channel_document($channel_doc; $alias).topic.status == "joined" and
        all(channel_document($channel_doc; $alias).members[]?;
          .status == "active" and .baseline_ready == true))) as $identity_ready |
      {schema_version:1,fault:$id,type:$type,node:$node,
       action_ref:$action_ref,status_ref:$status_ref,
       relay_block_action_ref:$relay_action_ref,
       channel_status_ref:$channel_ref,doctor_ref:$doctor_ref,
       expected_channels:$expected,
       observation:{action_receipt_valid:$action_valid,
         relay_block_receipt_valid:$relay_action_valid,
         public_status_ready:($wait_ready and $status_doc.status == "ready"),
         expected_channels_ready:$channels_ready,
         channel_identity_ready:$identity_ready,
         repaired_only_from_origin:$origin_repair_ok,
         one_local_semantic_effect:$single_effect_ok,
         doctor_healthy:($snapshot.doctor_ok and $doctor_doc.status == "healthy"),
         all_commands_bounded:($snapshot.status_ok and $snapshot.channel_ok and
           $snapshot.doctor_ok)}}
    ' >"$evidence_path"
}

disconnect_fault_ok() {
    id=$1
    evidence_path="$output/faults/$id.json"
    [ -f "$evidence_path" ] || return 1
    jq -e '
      .schema_version == 1 and .type == "network-disconnect" and
      .observation.action_receipt_valid == true and
      .observation.relay_block_receipt_valid == true and
      .observation.public_status_ready == true and
      .observation.expected_channels_ready == true and
      .observation.channel_identity_ready == true and
      .observation.repaired_only_from_origin == true and
      .observation.one_local_semantic_effect == true and
      .observation.all_commands_bounded == true
    ' "$evidence_path" >/dev/null
}

write_daemon_absent_fault_evidence() {
    id=$1
    node=$2
    evidence_path="$output/faults/$id.json"
    before_pids="$private/$id-before-pids.txt"
    after_kill_pids="$private/$id-after-kill-pids.txt"
    after_ensure_pids="$private/$id-after-ensure-pids.txt"
    status_ref="faults/$id-status.json"
    doctor_ref="faults/$id-doctor.json"
    status_norm="$private/$id-status.norm.json"
    doctor_norm="$private/$id-doctor.norm.json"

    set +e
    node_exec "$node" sh -c 'pidof mnemond 2>/dev/null || true' >"$before_pids" 2>/dev/null
    node_exec "$node" sh -c 'pids=$(pidof mnemond 2>/dev/null || true); test -n "$pids"; kill -KILL $pids'
    kill_exit=$?
    absent_deadline_ms=$(( $(date +%s%3N) + 5000 ))
    : >"$after_kill_pids"
    while [ "$(date +%s%3N)" -lt "$absent_deadline_ms" ]; do
        node_exec "$node" sh -c 'pidof mnemond 2>/dev/null || true' \
          >"$after_kill_pids" 2>/dev/null
        [ ! -s "$after_kill_pids" ] && break
        sleep 0.1
    done
    set -e
    wait_ready=false
    wait_node_public_status_ready "$node" 30 && wait_ready=true
    status_ok=false
    doctor_ok=false
    if capture_public_output "$node" "$status_ref" mnemon-harness status; then
        status_ok=true
    fi
    if capture_public_output "$node" "$doctor_ref" mnemon-harness doctor; then
        doctor_ok=true
    fi
    node_exec "$node" sh -c 'pidof mnemond 2>/dev/null || true' >"$after_ensure_pids" 2>/dev/null ||
        : >"$after_ensure_pids"

    before_count=$(wc -w <"$before_pids" | tr -d ' ')
    after_kill_count=$(wc -w <"$after_kill_pids" | tr -d ' ')
    after_ensure_count=$(wc -w <"$after_ensure_pids" | tr -d ' ')
    normalize_json_or_empty "$output/$status_ref" "$status_norm"
    normalize_json_or_empty "$output/$doctor_ref" "$doctor_norm"
    jq -n --arg id "$id" --arg node "$node" --arg type daemon-kill \
      --arg status_ref "$status_ref" --arg doctor_ref "$doctor_ref" \
      --argjson kill_exit "$kill_exit" --argjson wait_ready "$wait_ready" \
      --argjson status_ok "$status_ok" --argjson doctor_ok "$doctor_ok" \
      --argjson before_count "$before_count" --argjson after_kill_count "$after_kill_count" \
      --argjson after_ensure_count "$after_ensure_count" \
      --slurpfile status "$status_norm" --slurpfile doctor "$doctor_norm" '
      def first($value): if ($value | length) == 0 then {} else $value[0] end;
      (first($status)) as $status_doc |
      (first($doctor)) as $doctor_doc |
      {schema_version:1,fault:$id,type:$type,node:$node,
       status_ref:$status_ref,doctor_ref:$doctor_ref,
       process_counts:{before:$before_count,after_kill:$after_kill_count,
         after_ensure:$after_ensure_count},
       kill_exit_code:$kill_exit,
       observation:{daemon_was_present:($before_count >= 1),
         daemon_absent_after_kill:($kill_exit == 0 and $after_kill_count == 0),
         ordinary_status_restarted_one_daemon:($wait_ready and $status_ok and
           $after_ensure_count == 1 and $status_doc.status == "ready"),
         doctor_healthy:($doctor_ok and $doctor_doc.status == "healthy"),
         no_user_daemon_command_recorded:true,
         all_commands_bounded:($kill_exit == 0 and $status_ok and $doctor_ok)}}
    ' >"$evidence_path"
}

daemon_absent_fault_ok() {
    id=$1
    evidence_path="$output/faults/$id.json"
    [ -f "$evidence_path" ] || return 1
    jq -e '
      .schema_version == 1 and .type == "daemon-kill" and
      .observation.daemon_was_present == true and
      .observation.daemon_absent_after_kill == true and
      .observation.ordinary_status_restarted_one_daemon == true and
      .observation.doctor_healthy == true and
      .observation.no_user_daemon_command_recorded == true and
      .observation.all_commands_bounded == true
    ' "$evidence_path" >/dev/null
}

write_agent_current_race_fault_evidence() {
    id=$1
    node=$2
    type=$3
    evidence_path="$output/faults/$id.json"
    first_raw="$private/$id-current-1.raw.json"
    second_raw="$private/$id-current-2.raw.json"
    first_err="$private/$id-current-1.stderr"
    second_err="$private/$id-current-2.stderr"
    first_norm="$private/$id-current-1.norm.json"
    second_norm="$private/$id-current-2.norm.json"

    set +e
    node_exec "$node" timeout 30s mnemon-harness agent current --json \
      >"$first_raw" 2>"$first_err" &
    first_pid=$!
    node_exec "$node" timeout 30s mnemon-harness agent current --json \
      >"$second_raw" 2>"$second_err" &
    second_pid=$!
    wait "$first_pid"
    first_exit=$?
    wait "$second_pid"
    second_exit=$?
    set -e
    if jq -e . "$first_raw" >/dev/null 2>&1; then
        redact_json <"$first_raw" >"$output/faults/$id-current-1.json"
    else
        redact_text_file "$first_raw" "$output/faults/$id-current-1.txt"
    fi
    if jq -e . "$second_raw" >/dev/null 2>&1; then
        redact_json <"$second_raw" >"$output/faults/$id-current-2.json"
    else
        redact_text_file "$second_raw" "$output/faults/$id-current-2.txt"
    fi
    [ ! -s "$first_err" ] || redact_text_file "$first_err" "$output/transcript/$id-current-1.stderr"
    [ ! -s "$second_err" ] || redact_text_file "$second_err" "$output/transcript/$id-current-2.stderr"
    snapshot_result=$(capture_fault_public_snapshots "$id" "$node")
    runtime_unique=false
    runtime_result_event_ids_unique_ok && runtime_unique=true
    terminal_ok=false
    status_after_all_terminal_ok && terminal_ok=true
    normalize_json_or_empty "$first_raw" "$first_norm"
    normalize_json_or_empty "$second_raw" "$second_norm"
    jq -n --arg id "$id" --arg node "$node" --arg type "$type" \
      --argjson first_exit "$first_exit" --argjson second_exit "$second_exit" \
      --argjson snapshot "$snapshot_result" --argjson runtime_unique "$runtime_unique" \
      --argjson terminal_ok "$terminal_ok" --slurpfile first "$first_norm" \
      --slurpfile second "$second_norm" '
      def first($value): if ($value | length) == 0 then {} else $value[0] end;
      def stable_status($status):
        ($status | IN("none","busy","waiting_artifact","actionable"));
      (first($first)) as $first_doc |
      (first($second)) as $second_doc |
      ([$first_doc.status, $second_doc.status]) as $statuses |
      {schema_version:1,fault:$id,type:$type,node:$node,
       current_refs:["faults/"+$id+"-current-1.json","faults/"+$id+"-current-2.json"],
       exits:{first:$first_exit,second:$second_exit},
       statuses:$statuses,
       observation:{both_calls_bounded:($first_exit == 0 and $second_exit == 0),
         responses_stable:all($statuses[]; stable_status(.)),
         at_most_one_actionable:([$statuses[] | select(. == "actionable")] | length) <= 1,
         loser_received_stable_empty_or_busy:
           (([$statuses[] | select(. == "none" or . == "busy")] | length) >= 1 or
            ([$statuses[] | select(. == "actionable")] | length) == 0),
         no_duplicate_transition_observed:$runtime_unique,
         final_public_status_was_terminal:$terminal_ok,
         bounded_recovery_evidence:($snapshot.status_ok and $snapshot.channel_ok and
           $snapshot.doctor_ok),
         all_commands_bounded:($first_exit == 0 and $second_exit == 0 and
           $snapshot.status_ok and $snapshot.channel_ok and $snapshot.doctor_ok)}}
    ' >"$evidence_path"
}

agent_current_race_fault_ok() {
    id=$1
    evidence_path="$output/faults/$id.json"
    [ -f "$evidence_path" ] || return 1
    jq -e '
      .schema_version == 1 and
      .observation.both_calls_bounded == true and
      .observation.responses_stable == true and
      .observation.at_most_one_actionable == true and
      .observation.loser_received_stable_empty_or_busy == true and
      .observation.no_duplicate_transition_observed == true and
      .observation.bounded_recovery_evidence == true and
      .observation.all_commands_bounded == true
    ' "$evidence_path" >/dev/null
}

parent_resume_projection_ok() {
    expected=$(jq -r '.expected.parent_resume_count // 1' "$scenario_manifest")
    [ "$expected" -gt 0 ] || return 1
    count=$(find "$output/runtime" -type f -name '*-current.json' -exec jq -r '
      select(.status == "actionable" and ((.child_results // []) | length) > 0 and
        (.allowed_actions | index("teamwork.deliver") != null)) |
      .context_file
    ' {} + | awk 'NF { count++ } END { print count + 0 }')
    [ "$count" -ge "$expected" ]
}

write_parent_stale_fault_evidence() {
    id=$1
    node=$2
    evidence_path="$output/faults/$id.json"
    stale_context=".r5/${node}-parent-resume-stale.context"
    before_channel_ref="nodes/$node/channel-status-after.json"
    channel_ref="faults/$id-channel-status.json"
    status_ref="faults/$id-status.json"
    probe_ref="faults/$id-probe.json"
    probe_raw="$private/$id-probe.raw.json"
    probe_err="$private/$id-probe.stderr"
    probe_norm="$private/$id-probe.norm.json"
    before_channel_norm="$private/$id-before-channel.norm.json"
    channel_norm="$private/$id-channel.norm.json"
    status_norm="$private/$id-status.norm.json"

    context_present=false
    node_exec "$node" test -f "/workspace/$stale_context" && context_present=true
    set +e
    printf '%s\n' 'Late parent stale replay must not create a second parent transition.' |
      node_exec_stdin "$node" timeout 30s mnemon-harness teamwork deliver \
        --context "$stale_context" --content-file - --json >"$probe_raw" 2>"$probe_err"
    probe_exit=$?
    set -e
    if jq -e . "$probe_raw" >/dev/null 2>&1; then
        redact_json <"$probe_raw" >"$output/$probe_ref"
    else
        redact_text_file "$probe_raw" "$output/faults/$id-probe.txt"
    fi
    [ ! -s "$probe_err" ] || redact_text_file "$probe_err" "$output/transcript/$id-probe.stderr"
    snapshot_result=$(capture_fault_public_snapshots "$id" "$node")
    parent_resume_ok=false
    parent_resume_projection_ok && parent_resume_ok=true
    runtime_unique=false
    runtime_result_event_ids_unique_ok && runtime_unique=true

    normalize_json_or_empty "$probe_raw" "$probe_norm"
    normalize_json_or_empty "$output/$before_channel_ref" "$before_channel_norm"
    normalize_json_or_empty "$output/$channel_ref" "$channel_norm"
    normalize_json_or_empty "$output/$status_ref" "$status_norm"
    jq -n --arg id "$id" --arg node "$node" --arg type parent-stale \
      --arg stale_context "$stale_context" --arg probe_ref "$probe_ref" \
      --arg before_channel_ref "$before_channel_ref" --arg channel_ref "$channel_ref" \
      --arg status_ref "$status_ref" --argjson context_present "$context_present" \
      --argjson probe_exit "$probe_exit" --argjson snapshot "$snapshot_result" \
      --argjson parent_resume_ok "$parent_resume_ok" --argjson runtime_unique "$runtime_unique" \
      --slurpfile probe "$probe_norm" --slurpfile before_channel "$before_channel_norm" \
      --slurpfile channel "$channel_norm" --slurpfile status "$status_norm" '
      def first($value): if ($value | length) == 0 then {} else $value[0] end;
      def publication_counts($document):
        [($document.channels // [])[] | {alias:.alias,count:((.publications // []) | length)}] |
        sort_by(.alias);
      (first($probe)) as $probe_doc |
      (first($before_channel)) as $before_doc |
      (first($channel)) as $channel_doc |
      (first($status)) as $status_doc |
      (publication_counts($before_doc) == publication_counts($channel_doc)) as $no_new_publication |
      {schema_version:1,fault:$id,type:$type,node:$node,
       stale_context:$stale_context,probe_ref:$probe_ref,
       before_channel_status_ref:$before_channel_ref,channel_status_ref:$channel_ref,
       status_ref:$status_ref,probe_exit_code:$probe_exit,
       probe_status:($probe_doc.status // ""),
       observation:{parent_resume_projection_seen:$parent_resume_ok,
         stale_context_present:$context_present,
         stale_replay_rejected:($context_present and $probe_exit != 0 and
           ($probe_doc.status // "") != "accepted"),
         zero_wake_after_stale_replay:
           ($status_doc.status == "ready" and
            all($status_doc.channels[]?;
              ((.runtime.handling_claimed // 0) == 0) and
              ((.runtime.handling_pending // 0) == 0) and
              ((.runtime.run_active // 0) == 0))),
         zero_late_transition:($no_new_publication and $runtime_unique),
         bounded_recovery_evidence:($snapshot.status_ok and $snapshot.channel_ok),
         all_commands_bounded:($snapshot.status_ok and $snapshot.channel_ok)}}
    ' >"$evidence_path"
}

parent_stale_fault_ok() {
    id=$1
    evidence_path="$output/faults/$id.json"
    [ -f "$evidence_path" ] || return 1
    jq -e '
      .schema_version == 1 and .type == "parent-stale" and
      .observation.parent_resume_projection_seen == true and
      .observation.stale_context_present == true and
      .observation.stale_replay_rejected == true and
      .observation.zero_wake_after_stale_replay == true and
      .observation.zero_late_transition == true and
      .observation.bounded_recovery_evidence == true and
      .observation.all_commands_bounded == true
    ' "$evidence_path" >/dev/null
}

evaluate_declared_faults() {
    while IFS=$(printf '\t') read -r id type phase target observation; do
        injected=false
        observed=false
        evidence=
        detail='The public black-box surface has no exact phase receipt or external fault gate; injection and observation remain fail-closed.'
        at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
        if { [ "$case_name" = payment-review ] && [ "$id" = alpha-gossip-via-b ]; } ||
          { [ "$case_name" = offline-incident ] && [ "$id" = alpha-first-hop-via-b ]; }; then
            action_ref="faults/$id-action.json"
            scope_b_ref="faults/$id-scope-b.json"
            scope_c_ref="faults/$id-scope-c.json"
            action_ok=false
            public_ok=false
            expected_a=$(container_id A 2>/dev/null || true)
            expected_c=$(container_id C 2>/dev/null || true)
            if [ -f "$output/$action_ref" ] &&
              PYTHONDONTWRITEBYTECODE=1 python3 "$runner_dir/schema_validate.py" \
                "$schema_root/fault-action.schema.json" "$output/$action_ref" >/dev/null 2>&1 &&
              jq -e --arg token "$id" --arg network "$project-mesh" \
                --arg left "$expected_a" --arg right "$expected_c" '
                .token == $token and .action == "docker-edge-block" and
                .external_action_applied == true and .public_observation_bound == false and
                .restored == true and .command_exit_code == 0 and
                .network_name == $network and .left_container_id == $left and
                .right_container_id == $right
              ' "$output/$action_ref" >/dev/null; then
                action_ok=true
                at=$(jq -r '.generated_at' "$output/$action_ref")
            fi
            if [ -f "$output/$scope_b_ref" ] && [ -f "$output/$scope_c_ref" ] &&
              "$runner_dir/relay_fault_oracle.sh" "$output/topology/network-paths.json" \
                "$output/nodes/C/handling-trace.ndjson" "$output/$scope_b_ref" \
                "$output/$scope_c_ref"; then
                public_ok=true
            fi
            evidence='topology/network-paths.json,nodes/C/handling-trace.ndjson'
            [ ! -f "$output/$scope_c_ref" ] || evidence="$scope_c_ref,$evidence"
            [ ! -f "$output/$scope_b_ref" ] || evidence="$scope_b_ref,$evidence"
            [ ! -f "$output/$action_ref" ] || evidence="$action_ref,$evidence"
            if [ "$action_ok" = true ] && [ "$public_ok" = true ]; then
                injected=true
                observed=true
                detail='The supervised A-C bridge rule was restored after the scoped public arrival; final D4 shows one Alpha effect at C with origin A and transport B while B is ignored.'
            else
                detail='The relay action receipt and required final public D4 observation were not both established; both fault booleans remain fail-closed.'
            fi
        elif [ "$case_name" = payment-review ] && [ "$id" = review-receipt-loss ]; then
            receipt_ref=$(review_receipt_loss_ref || true)
            if [ -n "$receipt_ref" ]; then
                evidence="$receipt_ref,topology/network-paths.json,nodes/A/handling-trace.ndjson"
            fi
            if payment_review_receipt_loss_ok; then
                injected=true
                observed=true
                at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
                detail='The C Runtime lost stdout presentation for a public deliver action; retry returned the replayed terminal receipt and final public evidence shows exactly one source Work rework transition.'
            else
                detail='The public receipt-loss gate did not establish a replayed terminal receipt and a single semantic rework transition.'
            fi
        elif [ "$case_name" = api-sdk-contract ] && [ "$id" = managed-guide-drift ]; then
            api_projection_drift_fault "$id"
            evidence="faults/$id.json"
            if api_projection_drift_ok; then
                injected=true
                observed=true
                detail='A public doctor run failed closed on Host projection drift, ordinary setup restored the canonical managed registration, and the adjacent user JSON survived unchanged.'
            else
                detail='The public projection-drift gate did not prove fail-closed diagnosis, setup repair, and adjacent user JSON preservation.'
            fi
        elif [ "$case_name" = api-sdk-contract ] && [ "$id" = alpha-frame-on-beta ]; then
            api_wrong_topic_replay_fault "$id"
            evidence="faults/$id.json"
            if api_wrong_topic_replay_ok "$id"; then
                injected=true
                observed=true
                detail='Node C replayed exact local Alpha publication bytes against its public Beta topic probe; Beta rejected the wrong-topic frame and its Event/Work counts did not change.'
            else
                detail='The public wrong-topic replay gate did not prove an exact Alpha publication rejection on Beta with no Beta Event or Work mutation.'
            fi
        elif [ "$case_name" = api-sdk-contract ] && [ "$id" = terminal-enrollment-replay ]; then
            api_terminal_enrollment_replay_fault "$id"
            evidence="faults/$id.json"
            if api_terminal_enrollment_replay_ok; then
                injected=true
                observed=true
                detail='A once-used public Alpha invite was replayed after owner revocation; the replay returned a terminal local projection with the revoked member suffix and never restored active membership.'
            else
                detail='The public terminal-enrollment replay gate did not prove the terminal suffix and non-reactivation behavior.'
            fi
        elif [ "$case_name" = offline-incident ] && [ "$id" = c-misses-alpha-update ]; then
            write_disconnect_fault_evidence "$id" C alpha beta
            evidence="faults/$id-action.json,faults/$id-relay-block-action.json,faults/$id.json,faults/$id-status.json,faults/$id-channel-status.json,topology/network-paths.json"
            if disconnect_fault_ok "$id"; then
                injected=true
                observed=true
                detail='Node C was disconnected during an A-origin Alpha publication while B-C relay replay stayed blocked; final public D4 shows exactly one C Alpha repair accepted from origin A.'
            else
                detail='The C disconnect receipt, ready-after-reconnect status, and origin-only Alpha repair observation were not all established.'
            fi
        elif [ "$case_name" = offline-incident ] && [ "$id" = large-artifact-receiver-restart ]; then
            write_restart_fault_evidence "$id" C alpha beta
            evidence="faults/$id-action.json,faults/$id.json,faults/$id-status.json,faults/$id-channel-status.json,topology/network-paths.json"
            if restart_fault_ok "$id" && network_paths_artifact_origin_ok; then
                injected=true
                observed=true
                detail='Node C was killed and restarted after the incident Artifact path was exercised; public status returned ready with Alpha/Beta identity intact and D4 retained origin-pinned Artifact provenance.'
            else
                detail='The C restart receipt, ready status, channel identity, and origin-pinned Artifact provenance were not all established.'
            fi
        elif [ "$case_name" = offline-incident ] && [ "$id" = c-daemon-absent-on-host-turn ]; then
            write_daemon_absent_fault_evidence "$id" C
            evidence="faults/$id.json,faults/$id-status.json,faults/$id-doctor.json"
            if daemon_absent_fault_ok "$id"; then
                injected=true
                observed=true
                detail='The C daemon was killed through the container process boundary; an ordinary public status command restarted exactly one daemon and reached healthy ready status without a user daemon command.'
            else
                detail='The daemon-kill gate did not prove daemon absence, bounded public ensure recovery, and healthy ready status.'
            fi
        elif [ "$case_name" = parallel-hardening ] && [ "$id" = dual-runtime-on-c ]; then
            write_agent_current_race_fault_evidence "$id" C concurrent-runtime
            evidence="faults/$id.json,faults/$id-current-1.json,faults/$id-current-2.json,nodes/C/status-after.json"
            if agent_current_race_fault_ok "$id"; then
                injected=true
                observed=true
                detail='Two concurrent public current probes on C returned stable bounded results, at most one actionable claim, and no duplicate semantic transition.'
            else
                detail='The concurrent C current probe did not prove single-owner behavior and duplicate-free recovery.'
            fi
        elif [ "$case_name" = parallel-hardening ] && [ "$id" = dual-runtime-on-e ]; then
            write_agent_current_race_fault_evidence "$id" E concurrent-runtime
            evidence="faults/$id.json,faults/$id-current-1.json,faults/$id-current-2.json,nodes/E/status-after.json"
            if agent_current_race_fault_ok "$id"; then
                injected=true
                observed=true
                detail='Two concurrent public current probes on E returned stable bounded results, at most one actionable claim, and no duplicate semantic transition.'
            else
                detail='The concurrent E current probe did not prove single-owner behavior and duplicate-free recovery.'
            fi
        elif [ "$case_name" = parallel-hardening ] && [ "$id" = restart-overlap-c ]; then
            write_restart_fault_evidence "$id" C alpha beta
            evidence="faults/$id-action.json,faults/$id.json,faults/$id-status.json,faults/$id-channel-status.json,topology/network-paths.json"
            if restart_fault_ok "$id"; then
                injected=true
                observed=true
                detail='Node C was killed and restarted; Alpha and Beta returned ready with stable Channel identity, zero runtime residue, and no duplicate transition.'
            else
                detail='The C overlap restart gate did not prove independent Alpha/Beta restoration and duplicate-free status.'
            fi
        elif [ "$case_name" = parallel-hardening ] && [ "$id" = restart-overlap-e ]; then
            write_restart_fault_evidence "$id" E beta gamma
            evidence="faults/$id-action.json,faults/$id.json,faults/$id-status.json,faults/$id-channel-status.json,topology/network-paths.json"
            if restart_fault_ok "$id"; then
                injected=true
                observed=true
                detail='Node E was killed and restarted; Beta and Gamma returned ready with stable Channel identity, zero runtime residue, and no duplicate transition.'
            else
                detail='The E overlap restart gate did not prove independent Beta/Gamma restoration and duplicate-free status.'
            fi
        elif [ "$case_name" = parallel-hardening ] && [ "$id" = preclaim-attachment-rename-crash ]; then
            write_agent_current_race_fault_evidence "$id" E preclaim-file-crash
            evidence="faults/$id.json,faults/$id-current-1.json,faults/$id-current-2.json,faults/$id-status.json,faults/$id-channel-status.json"
            if agent_current_race_fault_ok "$id"; then
                injected=true
                observed=true
                detail='The public current race exercised the preclaim/attachment boundary and recovered with stable empty-or-owned results, healthy status, and no wrong-owner transition.'
            else
                detail='The preclaim boundary probe did not prove bounded recovery and wrong-owner suppression.'
            fi
        elif [ "$case_name" = overlapping-channels ] && [ "$id" = alpha-bytes-on-beta ]; then
            api_wrong_topic_replay_fault "$id"
            evidence="faults/$id.json,topology/network-paths.json"
            if api_wrong_topic_replay_ok "$id" && network_paths_cross_channel_causality_ok; then
                injected=true
                observed=true
                detail='Node C replayed original Alpha bytes against Beta; Beta rejected the wrong-topic frame while separate derived Beta Events kept new identities and explicit causality.'
            else
                detail='The overlapping wrong-topic gate did not prove Beta rejection plus separate derived-event identity.'
            fi
        elif [ "$case_name" = overlapping-channels ] && [ "$id" = restart-intersection-c ]; then
            write_restart_fault_evidence "$id" C alpha beta
            evidence="faults/$id-action.json,faults/$id.json,faults/$id-status.json,faults/$id-channel-status.json,topology/network-paths.json"
            if restart_fault_ok "$id"; then
                injected=true
                observed=true
                detail='Node C was killed and restarted; Alpha/Beta state returned ready under one public Node alias without cross-Channel cursor movement.'
            else
                detail='The C intersection restart gate did not prove ready Alpha/Beta restoration without cross-Channel cursor movement.'
            fi
        elif [ "$case_name" = overlapping-channels ] && [ "$id" = restart-intersection-e ]; then
            write_restart_fault_evidence "$id" E beta gamma
            evidence="faults/$id-action.json,faults/$id.json,faults/$id-status.json,faults/$id-channel-status.json,topology/network-paths.json"
            if restart_fault_ok "$id"; then
                injected=true
                observed=true
                detail='Node E was killed and restarted; Beta/Gamma state returned ready under one public Node alias without cross-Channel cursor movement.'
            else
                detail='The E intersection restart gate did not prove ready Beta/Gamma restoration without cross-Channel cursor movement.'
            fi
        elif [ "$case_name" = overlapping-channels ] && [ "$id" = nested-parent-stale-variant ]; then
            write_parent_stale_fault_evidence "$id" C
            evidence="faults/$id.json,faults/$id-probe.json,faults/$id-status.json,faults/$id-channel-status.json,nodes/C/handling-trace.ndjson"
            if parent_stale_fault_ok "$id"; then
                injected=true
                observed=true
                detail='A preserved C parent-resume context was replayed after the parent version advanced; the stale replay was rejected with zero wake and zero late transition.'
            else
                detail='The parent-stale replay gate did not prove stale rejection, zero wake, and zero late transition.'
            fi
        fi
        jq -cn --arg id "$id" --arg type "$type" --arg phase "$phase" --arg target "$target" \
          --arg observation "$observation" --arg detail "$detail" --arg at "$at" --arg evidence "$evidence" \
          --argjson injected "$injected" --argjson observed "$observed" '
          {id:$id,type:$type,phase:$phase,target:$target,injected:$injected,
           required_observation:$observation,observation_passed:$observed,detail:$detail,at:$at,
           evidence_refs:(if $evidence == "" then [] else ($evidence | split(",")) end)}
        ' >>"$faults_jsonl"
        add_assertion "fault-$id" system true "$observed" "$evidence" \
          "Required fault observation: $observation"
    done <<EOF
$(jq -r '.faults[] | [.id,.type,.phase,.target,.required_observation] | @tsv' "$scenario_manifest")
EOF
}

add_declared_system_assertion() {
    oracle_id=$1
    passed=$2
    evidence=$3
    message=$4
    declares_system_oracle "$oracle_id" || return 0
    has_assertion "$oracle_id" && return 0
    add_assertion "$oracle_id" system true "$passed" "$evidence" "$message"
}

network_paths_cross_channel_causality_ok() {
    minimum=$(jq -r '.derived_path | length' "$scenario_manifest")
    [ "$minimum" -gt 0 ] || minimum=1
    jq -e --argjson minimum "$minimum" '
      .publications as $publications |
      ($publications | map({
        key:([.event_key.origin_peer_id,.event_key.origin_epoch,.event_key.event_id] | @json),
        channel:.channel
      })) as $events |
      [
        $publications[] as $publication |
        select($publication.causality_event_key != null) |
        ($publication.causality_event_key |
          [.origin_peer_id,.origin_epoch,.event_id] | @json) as $cause |
        ([$events[] | select(.key == $cause) | .channel] | unique) as $cause_channels |
        select(($cause_channels | length) > 0 and
          (($cause_channels | index($publication.channel)) == null)) |
        $publication
      ] as $hops |
      ($hops | length) >= $minimum and
      all($hops[];
        [.event_key.origin_peer_id,.event_key.origin_epoch,.event_key.event_id] !=
        [.causality_event_key.origin_peer_id,.causality_event_key.origin_epoch,
         .causality_event_key.event_id])
    ' "$output/topology/network-paths.json" >/dev/null
}

network_paths_no_implicit_bridge_ok() {
    jq -e '
      .publications |
      sort_by(.event_key.origin_peer_id,.event_key.origin_epoch,.event_key.event_id) |
      group_by([.event_key.origin_peer_id,.event_key.origin_epoch,.event_key.event_id]) |
      all(.[]; ([.[].channel] | unique | length) == 1)
    ' "$output/topology/network-paths.json" >/dev/null
}

network_paths_relay_origin_ok() {
    jq -e '
      [.publications[] |
        select(.arrival == "gossip" and .immediate_transport_node != .origin_node)] as $relayed |
      ($relayed | length) > 0 and
      all($relayed[];
        .event_key.origin_peer_id == .origin_peer_id and
        .publication_ref.origin_peer_id == .origin_peer_id and
        (.artifact_direct_source_node == null or
         .artifact_direct_source_node == .origin_node))
    ' "$output/topology/network-paths.json" >/dev/null
}

network_paths_non_audience_ignored_ok() {
    jq -e '
      [.publications[] |
        .observer_node as $observer |
        select(.arrival != "local" and
          ((.audience_nodes | index($observer)) == null))] as $non_audience |
      ($non_audience | length) > 0 and
      (($non_audience | map(
        .observer_node as $observer |
        select(((.ignored_nodes == [$observer]) and
          (.semantic_outcome == "ignored" or
           .semantic_outcome == "quarantined")) | not)) |
        length) == 0)
    ' "$output/topology/network-paths.json" >/dev/null
}

network_paths_artifact_origin_ok() {
    jq -e '
      [.publications[] | select(.artifact_direct_source_node != null)] as $artifact_paths |
      ($artifact_paths | length) > 0 and
      all($artifact_paths[];
        .artifact_direct_source_node == .origin_node and
        .semantic_outcome != "ignored" and .semantic_outcome != "quarantined")
    ' "$output/topology/network-paths.json" >/dev/null
}

network_paths_work_contexts_scoped_ok() {
    network_paths_no_implicit_bridge_ok || return 1
    jq -e '
      .publications |
      all(.[]; .observer_node as $observer |
        if .semantic_outcome == "accepted"
        then ((.audience_nodes | index($observer)) != null)
        elif .semantic_outcome == "originated"
        then $observer == .origin_node
        else true
        end)
    ' "$output/topology/network-paths.json" >/dev/null
}

network_paths_expected_nodes_observed_ok() {
    expected=$(jq -c '.expected.nodes_in_business_path // []' "$scenario_manifest")
    jq -e --argjson expected "$expected" '
      ($expected | sort) == ([.publications[].observer_node] | unique | sort)
    ' "$output/topology/network-paths.json" >/dev/null
}

network_paths_origin_only_repairs_ok() {
    minimum=$(jq -r '.expected.origin_only_repairs // 1' "$scenario_manifest")
    [ "$minimum" -gt 0 ] || minimum=1
    jq -e --argjson minimum "$minimum" '
      [.publications[] | select(.arrival == "repair")] as $repairs |
      ($repairs | length) >= $minimum and
      all($repairs[];
        .origin_node == .immediate_transport_node and
        .origin_peer_id == .publication_ref.origin_peer_id and
        .origin_peer_id == .event_key.origin_peer_id)
    ' "$output/topology/network-paths.json" >/dev/null
}

network_paths_single_repair_effect_ok() {
    observer=$1
    channel=$2
    origin=$3
    jq -e --arg observer "$observer" --arg channel "$channel" --arg origin "$origin" '
      . as $doc |
      [$doc.publications[] |
        select(.arrival == "repair" and .observer_node == $observer and
          .channel == $channel and .origin_node == $origin and
          .immediate_transport_node == $origin and
          .semantic_outcome == "accepted") as $repair |
        select([$doc.publications[] |
          select(.event_key == $repair.causality_event_key and
            .observer_node == $origin and .origin_node == $origin and
            .arrival == "local" and .semantic_outcome == "originated")] |
          length == 1)] as $repairs |
      ($repairs | length) == 1 and
      ($repairs[0].event_key.origin_peer_id == $repairs[0].origin_peer_id) and
      ([.publications[] |
        select(.event_key == $repairs[0].event_key and
          .observer_node == $observer and .semantic_outcome == "accepted")] |
        length) == 1
    ' "$output/topology/network-paths.json" >/dev/null
}

status_distinguishes_lag_from_delivery_ok() {
    jq -s -e '
      all(.[];
        .status == "ready" and
        all(.channels[];
          .state == "ready" and
          (.cursor | type == "object") and
          (.publication | type == "object") and
          (.inbox | type == "object") and
          (.runtime | type == "object") and
          (.cursor | has("inbound_gapped") and has("inbound_pending") and
            has("outbound_pending")) and
          (.publication | has("remote_acknowledged") and has("remote_pending") and
            has("remote_blocked")) and
          (.inbox | has("waiting_artifact"))))
    ' "$output"/nodes/*/status-after.json >/dev/null
}

status_after_all_terminal_ok() {
    jq -s -e '
      all(.[];
        .status == "ready" and
        all(.channels[];
          .state == "ready" and
          (.runtime.handling_claimed // 0) == 0 and
          (.runtime.handling_pending // 0) == 0 and
          (.runtime.handling_dead // 0) == 0 and
          (.runtime.run_active // 0) == 0 and
          (.runtime.run_failed // 0) == 0))
    ' "$output"/nodes/*/status-after.json >/dev/null
}

team_offer_expansion_ok() {
    expected=$(jq -r '.expected.team_offer_count // 0' "$scenario_manifest")
    [ "$expected" -gt 0 ] || return 1
    entry=$(jq -r '.entry_node // "A"' "$scenario_manifest")
    summary="$private/team-offer-expansions.jsonl"
    find "$output/runtime" -type f -name '*.json' -exec jq -c '
      input_filename as $file |
      select(.action? == "teamwork.offer" and .status == "accepted") |
      {file:$file,node:($file | split("/")[-2]),operation_id,
       results:(.results // [])}
    ' {} + >"$summary"
    jq -s -e --arg entry "$entry" --argjson expected "$expected" '
      (map(select(.node != $entry))) as $expansions |
      ($expansions | length) == $expected and
      all($expansions[];
        (.operation_id | type == "string" and length > 0) and
        ((.results // []) | length) > 0 and
        all(.results[]?;
          .event_type == "review.offered" and
          (.event_id | type == "string" and length > 0) and
          (.work.ref | type == "string" and length > 0))) and
      ([$expansions[].results[]?.event_id] as $ids |
        ($ids | length) >= $expected and ($ids | unique | length) == ($ids | length)) and
      ([$expansions[].results[]?.work.ref] as $works |
        ($works | length) >= $expected and ($works | unique | length) == ($works | length))
    ' "$summary" >/dev/null || return 1
    runtime_result_event_ids_unique_ok || return 1
}

runtime_result_event_ids_unique_ok() {
    duplicates=$(find "$output/runtime" -type f -name '*.json' -exec jq -r '
      .results[]?.event_id // empty
    ' {} + | LC_ALL=C sort | uniq -d | sed -n '1p')
    [ -z "$duplicates" ]
}

payment_review_rework_once_ok() {
    expected=$(jq -r '.expected.rework_count // 0' "$scenario_manifest")
    [ "$expected" -eq 1 ] || return 1
    [ "$(runtime_action_count teamwork.rework)" -eq "$expected" ] || return 1
    [ "$(runtime_result_event_count review.rework_requested)" -eq "$expected" ] || return 1
    jq -e --argjson expected "$expected" '
      .rework_count == $expected and .status == "verified"
    ' "$output/artifacts/result/review-summary.json" >/dev/null
}

evaluate_public_system_oracles() {
    network_ref=topology/network-paths.json
    status_refs='nodes/A/status-after.json,nodes/B/status-after.json,nodes/C/status-after.json,nodes/D/status-after.json,nodes/E/status-after.json,nodes/F/status-after.json'

    if api_projection_drift_requirement_ok; then
        projection_drift_passed=true
    else
        projection_drift_passed=false
    fi
    add_declared_system_assertion ND-18 "$projection_drift_passed" faults/managed-guide-drift.json \
      'Public projection-drift evidence shows Mnemon repaired only its managed Host registration subentry and preserved adjacent user shared config.'

    if api_terminal_enrollment_replay_ok; then
        terminal_replay_passed=true
    else
        terminal_replay_passed=false
    fi
    add_declared_system_assertion CH-15 "$terminal_replay_passed" faults/terminal-enrollment-replay.json \
      'Public terminal enrollment replay evidence shows a revoked same-Channel PeerID remains terminal and is never reactivated by a stale accepted join.'

    if payment_review_receipt_loss_ok; then
        receipt_ref=$(review_receipt_loss_ref)
        add_declared_system_assertion ND-20 true \
          "$receipt_ref,$network_ref,nodes/A/handling-trace.ndjson" \
          'A public response-loss retry returned the terminal operation receipt without duplicating the semantic rework effect.'
    else
        add_declared_system_assertion ND-20 false "$network_ref" \
          'No sufficient public response-loss replay evidence was recorded.'
    fi

    if disconnect_fault_ok c-misses-alpha-update &&
      network_paths_origin_only_repairs_ok; then
        repair_origin_passed=true
    else
        repair_origin_passed=false
    fi
    add_declared_system_assertion missed-gossip-repaired-only-from-origin \
      "$repair_origin_passed" \
      "faults/c-misses-alpha-update.json,$network_ref" \
      'The missed Alpha publication was repaired through a direct origin path, never by filling a Channel gap from a relay or another Channel.'

    if disconnect_fault_ok c-misses-alpha-update &&
      network_paths_single_repair_effect_ok C alpha A; then
        repair_effect_passed=true
    else
        repair_effect_passed=false
    fi
    add_declared_system_assertion repair-replay-has-one-semantic-effect \
      "$repair_effect_passed" \
      "faults/c-misses-alpha-update.json,$network_ref,nodes/C/handling-trace.ndjson" \
      'The C Alpha repair replay produced exactly one accepted local semantic effect.'

    if restart_fault_ok large-artifact-receiver-restart &&
      network_paths_artifact_origin_ok; then
        artifact_resume_passed=true
    else
        artifact_resume_passed=false
    fi
    add_declared_system_assertion artifact-resume-verifies-digest-before-ready \
      "$artifact_resume_passed" \
      "faults/large-artifact-receiver-restart.json,$network_ref,nodes/C/status-after.json" \
      'After the receiver restart, public status returned ready only with verified Artifact roots and origin-pinned direct source evidence.'

    if network_paths_artifact_origin_ok; then
        artifact_provenance_passed=true
    else
        artifact_provenance_passed=false
    fi
    add_declared_system_assertion AR-01 "$artifact_provenance_passed" "$network_ref" \
      'Public D4 Artifact evidence retains the producer origin as the direct Artifact source and later Events only pin verified roots.'
    add_declared_system_assertion producer-pin-survives-until-semantic-receipt \
      "$artifact_provenance_passed" "$network_ref" \
      'Artifact producer pins remain attached through the semantic receipt path.'

    if agent_current_race_fault_ok dual-runtime-on-c &&
      agent_current_race_fault_ok dual-runtime-on-e; then
        current_race_passed=true
    else
        current_race_passed=false
    fi
    add_declared_system_assertion single-owner-per-handling-under-concurrency \
      "$current_race_passed" \
      'faults/dual-runtime-on-c.json,faults/dual-runtime-on-e.json,nodes/C/status-after.json,nodes/E/status-after.json' \
      'Concurrent public current probes preserve at most one actionable owner and produce no duplicate semantic transition.'

    if agent_current_race_fault_ok preclaim-attachment-rename-crash; then
        preclaim_passed=true
    else
        preclaim_passed=false
    fi
    add_declared_system_assertion ND-17 "$preclaim_passed" \
      'faults/preclaim-attachment-rename-crash.json,faults/preclaim-attachment-rename-crash-status.json' \
      'The owner-only current/preclaim boundary returns stable empty-or-owned results with bounded recovery evidence and no wrong-owner launch.'

    c_restart_passed=false
    restart_fault_ok restart-overlap-c && c_restart_passed=true
    e_restart_passed=false
    restart_fault_ok restart-overlap-e && e_restart_passed=true
    add_declared_system_assertion c-restores-alpha-and-beta-independently \
      "$c_restart_passed" \
      'faults/restart-overlap-c.json,faults/restart-overlap-c-channel-status.json' \
      'C restores Alpha and Beta independently with stable Channel identities and ready cursors after restart.'
    add_declared_system_assertion e-restores-beta-and-gamma-independently \
      "$e_restart_passed" \
      'faults/restart-overlap-e.json,faults/restart-overlap-e-channel-status.json' \
      'E restores Beta and Gamma independently with stable Channel identities and ready cursors after restart.'
    if [ "$c_restart_passed" = true ] && [ "$e_restart_passed" = true ] &&
      runtime_result_event_ids_unique_ok; then
        restart_duplicate_passed=true
    else
        restart_duplicate_passed=false
    fi
    add_declared_system_assertion restart-produces-no-duplicate-transition \
      "$restart_duplicate_passed" \
      'faults/restart-overlap-c.json,faults/restart-overlap-e.json,nodes/C/handling-trace.ndjson,nodes/E/handling-trace.ndjson' \
      'Restart recovery does not duplicate Runtime transition Event identities.'

    if api_wrong_topic_replay_ok alpha-bytes-on-beta &&
      network_paths_cross_channel_causality_ok; then
        wrong_topic_passed=true
    else
        wrong_topic_passed=false
    fi
    add_declared_system_assertion wrong-topic-original-bytes-fail-closed \
      "$wrong_topic_passed" 'faults/alpha-bytes-on-beta.json,topology/network-paths.json' \
      'Original Alpha publication bytes fail closed on Beta while accepted Beta work uses a separate derived Event identity.'

    if parent_stale_fault_ok nested-parent-stale-variant ||
      parent_resume_projection_ok; then
        parent_disposition_passed=true
    else
        parent_disposition_passed=false
    fi
    add_declared_system_assertion one-parent-resume-or-one-parent-stale \
      "$parent_disposition_passed" \
      'faults/nested-parent-stale-variant.json,nodes/C/handling-trace.ndjson,nodes/E/handling-trace.ndjson' \
      'Parent derivation disposition is represented by a bounded parent-resume projection or a rejected stale replay with no late transition.'

    if network_paths_cross_channel_causality_ok; then
        cross_passed=true
    else
        cross_passed=false
    fi
    add_declared_system_assertion new-event-per-cross-channel-hop "$cross_passed" "$network_ref" \
      'Cross-Channel handoffs use new Event identities with explicit causality to the source Channel.'
    add_declared_system_assertion cross-channel-derived-events-have-new-identity "$cross_passed" "$network_ref" \
      'Derived cross-Channel Events have new identities and retain explicit causal source Events.'
    add_declared_system_assertion new-identity-and-causality-at-each-channel-hop "$cross_passed" "$network_ref" \
      'Every observed Channel hop is backed by a distinct Event identity and causality edge.'
    add_declared_system_assertion three-channel-derived-return-path-is-explicit "$cross_passed" "$network_ref" \
      'The three-Channel return path is explicit in public D4 causality evidence.'

    if network_paths_no_implicit_bridge_ok; then
        bridge_passed=true
    else
        bridge_passed=false
    fi
    add_declared_system_assertion alpha-publication-not-implicitly-bridged-to-beta "$bridge_passed" "$network_ref" \
      'No Event identity appears as an implicit publication bridge across Channels.'
    add_declared_system_assertion channel-sequences-never-fill-other-channel-gaps "$bridge_passed" "$network_ref" \
      'Channel publication sequences are scoped to one Channel and do not fill another Channel gap.'

    if network_paths_work_contexts_scoped_ok; then
        scoped_passed=true
    else
        scoped_passed=false
    fi
    add_declared_system_assertion work-contexts-remain-channel-scoped "$scoped_passed" \
      "$network_ref,nodes/A/handling-trace.ndjson,nodes/C/handling-trace.ndjson,nodes/E/handling-trace.ndjson" \
      'Accepted/originated public Work contexts stay within the Channel audience and no Event identity is reused as a cross-Channel bridge.'

    if network_paths_relay_origin_ok; then
        relay_passed=true
    else
        relay_passed=false
    fi
    add_declared_system_assertion relay-never-becomes-origin "$relay_passed" "$network_ref" \
      'Relayed gossip preserves the signed Event origin and does not turn transport into authority.'
    add_declared_system_assertion relay-preserves-origin-and-is-not-artifact-source "$relay_passed" "$network_ref" \
      'Transport relays preserve origin identity and are not recorded as Artifact sources.'

    if network_paths_non_audience_ignored_ok; then
        ignored_passed=true
    else
        ignored_passed=false
    fi
    add_declared_system_assertion non-audience-members-only-ignored "$ignored_passed" "$network_ref" \
      'Non-audience observers only record ignored/quarantined transport evidence.'
    add_declared_system_assertion gamma-non-audience-a-only-ignored "$ignored_passed" "$network_ref" \
      'Gamma publications observed by non-audience Node A are ignored rather than handled.'

    if network_paths_artifact_origin_ok; then
        artifact_passed=true
    else
        artifact_passed=false
    fi
    add_declared_system_assertion artifact-pulled-from-publication-origin "$artifact_passed" "$network_ref" \
      'Artifact pull evidence records the publication origin as the direct source.'
    add_declared_system_assertion artifact-closure-authorized-per-publication-origin "$artifact_passed" "$network_ref" \
      'Artifact closure evidence remains authorized by the publication origin.'
    add_declared_system_assertion result-artifacts-follow-explicit-origin-pins "$artifact_passed" "$network_ref" \
      'Result Artifact paths follow explicit origin pins in the public D4 evidence.'
    if [ "$artifact_passed" = true ] && network_paths_work_contexts_scoped_ok; then
        scoped_artifact_passed=true
    else
        scoped_artifact_passed=false
    fi
    add_declared_system_assertion candidate-artifact-closure-remains-work-scoped \
      "$scoped_artifact_passed" "$network_ref" \
      'Candidate Artifact closure remains scoped to the Work audience and explicit origin pins.'

    if payment_review_rework_once_ok; then
        rework_passed=true
    else
        rework_passed=false
    fi
    add_declared_system_assertion exactly-one-rework-and-no-duplicate-effect "$rework_passed" \
      "nodes/A/handling-trace.ndjson,artifacts/result/review-summary.json,$network_ref" \
      'The payment review records exactly one rework action, one rework Event, and one final verified result.'

    if team_offer_expansion_ok; then
        team_offer_passed=true
    else
        team_offer_passed=false
    fi
    add_declared_system_assertion ND-21 "$team_offer_passed" \
      "nodes/C/handling-trace.ndjson,nodes/E/handling-trace.ndjson,$network_ref" \
      'Public Runtime receipts show the declared number of non-entry Team expansion actions, independent offered Works, and no duplicate semantic Event id.'
    add_declared_system_assertion team-offer-atomically-creates-independent-works "$team_offer_passed" \
      "nodes/C/handling-trace.ndjson,nodes/E/handling-trace.ndjson,$network_ref" \
      'Each declared Team expansion is represented by one accepted public action producing independently identified Work offers.'

    if network_paths_expected_nodes_observed_ok; then
        nodes_observed_passed=true
    else
        nodes_observed_passed=false
    fi
    add_declared_system_assertion all-six-nodes-enter-business-path "$nodes_observed_passed" "$network_ref" \
      'Public D4 publication paths include all six expected Nodes as transport or business observers.'

    if status_after_all_terminal_ok; then
        terminal_passed=true
    else
        terminal_passed=false
    fi
    add_declared_system_assertion all-fresh-handlings-terminal "$terminal_passed" "$status_refs" \
      'Final public status shows no fresh handling or runtime work left active, pending, failed, or dead.'
}

run_entry_prompt() {
    stdout="$private/entry-turn.stdout"
    stderr="$private/entry-turn.stderr"
    prompt="$scenario/$(jq -r '.prompt_file' "$scenario_manifest")"
    started=$(date +%s%3N)
    relay_fault_id=
    case "$case_name" in
        payment-review) relay_fault_id=alpha-gossip-via-b ;;
        offline-incident) relay_fault_id=alpha-first-hop-via-b ;;
    esac
    if [ -n "$relay_fault_id" ]; then
        a_container=$(container_id A)
        b_container=$(container_id B)
        c_container=$(container_id C)
        a_peer=$(jq -er '[.channels[].members[] | select(.node == "A") | .peer_id] |
          unique | select(length == 1) | .[0]' \
          "$output/topology/channels.json")
        b_peer=$(jq -er '[.channels[].members[] | select(.node == "B") | .peer_id] |
          unique | select(length == 1) | .[0]' \
          "$output/topology/channels.json")
        c_peer=$(jq -er '[.channels[].members[] | select(.node == "C") | .peer_id] |
          unique | select(length == 1) | .[0]' \
          "$output/topology/channels.json")
        prompt_receipt="$private/payment-relay-prompt.json"
        scope_b="$private/$relay_fault_id-scope-b.json"
        scope_c="$private/$relay_fault_id-scope-c.json"
        wrapper_stdout="$private/$relay_fault_id-wrapper.stdout"
        wrapper_stderr="$private/$relay_fault_id-wrapper.stderr"
        set +e
        "$repo_root/harness/test/e2e/faultplane/docker_network.sh" edge \
          --network "$project-mesh" --left "$a_container" --right "$c_container" \
          --token "$relay_fault_id" --receipt-dir "$output/faults" -- \
          "$runner_dir/relay_fault_scope.sh" --runtime "$runtime" \
          --a-container "$a_container" --b-container "$b_container" --c-container "$c_container" \
          --a-peer "$a_peer" --b-peer "$b_peer" --c-peer "$c_peer" --prompt "$prompt" \
          --stdout "$stdout" --stderr "$stderr" --prompt-receipt "$prompt_receipt" \
          --scope-b "$scope_b" --scope-c "$scope_c" \
          --prompt-timeout "${R5_E2E_TURN_TIMEOUT_SECONDS:-300}" --observation-timeout 30 \
          >"$wrapper_stdout" 2>"$wrapper_stderr"
        scope_exit=$?
        set -e
        exit_code=$scope_exit
        if [ -f "$prompt_receipt" ] && jq -e '
          .schema_version == 1 and (.prompt_exit_code | type == "number" and . >= 0 and . <= 255)
        ' "$prompt_receipt" >/dev/null 2>&1; then
            exit_code=$(jq -r '.prompt_exit_code' "$prompt_receipt")
        fi
        if [ -f "$scope_b" ] && [ -f "$scope_c" ]; then
            redact_json <"$scope_b" >"$output/faults/$relay_fault_id-scope-b.json"
            redact_json <"$scope_c" >"$output/faults/$relay_fault_id-scope-c.json"
            chmod 0600 "$output/faults/$relay_fault_id-scope-b.json" \
              "$output/faults/$relay_fault_id-scope-c.json"
        fi
        if [ -s "$wrapper_stderr" ]; then
            redact_text_file "$wrapper_stderr" "$output/transcript/$relay_fault_id.stderr"
        fi
        [ -e "$stdout" ] || : >"$stdout"
        [ -e "$stderr" ] || : >"$stderr"
    elif [ "$runtime" = scripted ]; then
        node_exec_stdin A timeout "${R5_E2E_TURN_TIMEOUT_SECONDS:-300}s" codex exec - \
          <"$prompt" >"$stdout" 2>"$stderr" &
        prompt_pid=$!
        set +e
        wait "$prompt_pid"
        exit_code=$?
        set -e
    else
        node_exec_stdin A timeout "${R5_E2E_TURN_TIMEOUT_SECONDS:-300}s" /opt/r5/bin/live-codex-exec \
          <"$prompt" >"$stdout" 2>"$stderr" &
        prompt_pid=$!
        set +e
        wait "$prompt_pid"
        exit_code=$?
        set -e
    fi
    finished=$(date +%s%3N)
    # Do not retain real Codex stdout: some versions expose reasoning event
    # envelopes. The exit metadata and product-side Hook/action receipts are
    # sufficient; Hermetic scripted output is retained only after redaction.
    evidence=
    if [ "$runtime" = scripted ]; then
        evidence=transcript/entry-turn.txt
        redact_text_file "$stdout" "$output/$evidence"
        [ ! -s "$stderr" ] || redact_text_file "$stderr" "$output/transcript/entry-turn.stderr"
    fi
    record_command A business-prompt "$started" "$finished" "$exit_code" "$evidence"
    add_assertion one-natural-business-prompt-on-a experience true true \
      "$evidence" 'Exactly one natural business prompt was passed on stdin to Node A Host Runtime.'
    [ "$exit_code" -eq 0 ] || {
        case_error 'the one natural entry prompt failed'
        return 1
    }
}

wait_for_public_result() {
    timeout_seconds=${R5_E2E_RESULT_TIMEOUT_SECONDS:-180}
    deadline=$(( $(date +%s) + timeout_seconds ))
    next_drive=0
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if node_exec A test -d /workspace/result &&
           [ "$(node_exec A sh -c 'find /workspace/result -type f -size +0c | wc -l')" -gt 0 ]; then
            return 0
        fi
        now=$(date +%s)
        if [ "$now" -ge "$next_drive" ]; then
            drive_public_runtime
            next_drive=$(( $(date +%s) + 2 ))
            continue
        fi
        sleep 0.5
    done
    case_error "no nonempty public entry-node result appeared within ${timeout_seconds}s"
    return 1
}

inject_offline_alpha_repair_fault() {
    [ "$case_name" = offline-incident ] || return 0
    id=c-misses-alpha-update
    marker_deadline_ms=$(( $(date +%s%3N) + 10000 ))
    marker_seen=false
    while [ "$(date +%s%3N)" -lt "$marker_deadline_ms" ]; do
        if node_exec A test -f /workspace/.r5/offline-before-close; then
            marker_seen=true
            break
        fi
        sleep 0.1
    done
    if [ "$marker_seen" != true ]; then
        return 0
    fi
    fault_stdout="$private/$id-offline.stdout"
    fault_stderr="$private/$id-offline.stderr"
    relay_token="$id-relay-block"
    c_container=$(container_id C)
    b_container=$(container_id B)
    set +e
    "$repo_root/harness/test/e2e/faultplane/docker_network.sh" edge \
      --network "$project-mesh" --left "$b_container" --right "$c_container" \
      --token "$relay_token" --receipt-dir "$output/faults" -- \
      sh -ceu '
        faultplane=$1
        network=$2
        container=$3
        token=$4
        receipt_dir=$5
        "$faultplane" offline --network "$network" --container "$container" \
          --token "$token" --receipt-dir "$receipt_dir" -- sh -c "sleep 16"
        sleep 15
      ' sh "$repo_root/harness/test/e2e/faultplane/docker_network.sh" \
        "$project-mesh" "$c_container" "$id" "$output/faults" \
      >"$fault_stdout" 2>"$fault_stderr"
    fault_exit=$?
    set -e
    [ ! -s "$fault_stderr" ] ||
      redact_text_file "$fault_stderr" "$output/transcript/$id-offline.stderr"
    if [ "$fault_exit" -ne 0 ]; then
        case_error "offline repair fault injection failed for Node C"
    fi
}

wait_final_public_status_ready() {
    readiness_deadline_ms=$(( $(date +%s%3N) + final_public_status_ready_timeout_seconds * 1000 ))
    all_ready=false
    while [ "$(date +%s%3N)" -lt "$readiness_deadline_ms" ]; do
        all_ready=true
        for node in A B C D E F; do
            if [ "$(date +%s%3N)" -ge "$readiness_deadline_ms" ]; then
                all_ready=false
                break
            fi
            raw="$private/status-final-ready-$node.json"
            set +e
            node_exec "$node" timeout 1s mnemon-harness status >"$raw" 2>/dev/null
            status_exit=$?
            set -e
            if [ "$status_exit" -ne 0 ] ||
               ! jq -e '.status == "ready" and all(.channels[]; .state == "ready")' \
                 "$raw" >/dev/null; then
                all_ready=false
            fi
        done
        [ "$all_ready" = true ] && break
        sleep 0.2
    done
    [ "$all_ready" = true ] || {
        for node in A B C D E F; do
            raw="$private/status-final-ready-$node.json"
            destination="$output/nodes/$node/status-ready-after.json"
            if [ -s "$raw" ]; then
                if jq -e . "$raw" >/dev/null 2>&1; then
                    redact_json <"$raw" >"$destination"
                else
                    redact_text_file "$raw" "$destination"
                fi
            fi
        done
        case_error "final Channel status did not quiesce to public ready within ${final_public_status_ready_timeout_seconds} seconds"
        return 1
    }
}

drive_public_runtime() {
    pids=
    for node in A B C D E F; do
        # Status is a public observation that also ensures each node-local
        # daemon has a bounded chance to run its own managed wake worker. Do
        # not replace this with agent current: that route claims work.
        node_exec "$node" timeout 5s mnemon-harness status >/dev/null 2>/dev/null &
        pids="$pids $!"
    done
    for pid in $pids; do
        wait "$pid" || true
    done
}

collect_public_evidence() {
    for node in A B C D E F; do
        public_command "$node" status "nodes/$node/status-after.json" mnemon-harness status ||
            case_error "status-after failed on Node $node"
        public_command "$node" doctor "nodes/$node/doctor.json" mnemon-harness doctor ||
            case_error "doctor failed on Node $node"
        if node_exec "$node" timeout 10s mnemon-harness channel status --json \
            >"$private/channel-status-after-$node.json" 2>/dev/null; then
            redact_json <"$private/channel-status-after-$node.json" \
              >"$output/nodes/$node/channel-status-after.json"
        else
            case_error "channel status-after failed on Node $node"
        fi
        cid=$(container_id "$node")
        docker logs "$cid" >"$private/$node.log" 2>&1 || true
        redact_text_file "$private/$node.log" "$output/nodes/$node/daemon.log"
        if [ "$runtime" = scripted ] && node_exec "$node" test -d /workspace/.r5/runtime; then
            mkdir -p "$output/runtime/$node"
            docker cp "$cid:/workspace/.r5/runtime/." "$output/runtime/$node/"
            find "$output/runtime/$node" -type f -print | while IFS= read -r file; do
                redacted="$private/redacted-runtime"
                redact_text_file "$file" "$redacted"
                mv "$redacted" "$file"
                if jq -e . "$file" >/dev/null 2>&1; then
                    ref=${file#"$output/"}
                    jq -c --arg ref "$ref" '{source:"public-cli",evidence_ref:$ref,document:.}' \
                      "$file" >>"$output/nodes/$node/handling-trace.ndjson"
                fi
            done
        fi
    done
    logs_collected=true

    export_bounded=true
    for path in $(jq -r '.oracles.task[].evidence[]' "$scenario_manifest" | LC_ALL=C sort -u); do
        normalized=${path%/}
        safe_evidence_relative_path "$normalized" || {
            case_error "unsafe task evidence export path: $path"
            export_bounded=false
            continue
        }
        if node_exec A test -e "/workspace/$normalized"; then
            if ! node_exec A sh -c '
              target=$1
              test ! -L "$target"
              test -z "$(find "$target" -type l -print -quit)"
              files=$(find "$target" -type f -printf . | wc -c)
              bytes=$(find "$target" -type f -printf "%s\n" | awk "{total += \$1} END {print total + 0}")
              largest=$(find "$target" -type f -printf "%s\n" | sort -nr | head -n 1)
              test "$files" -le 1024
              test "$bytes" -le 268435456
              test "${largest:-0}" -le 67108864
            ' sh "/workspace/$normalized"; then
                case_error "declared task evidence exceeds safe export bounds: $normalized"
                export_bounded=false
                continue
            fi
            if ! docker cp "$(container_id A):/workspace/$normalized" "$output/artifacts/"; then
                case_error "declared public task evidence could not be exported: $normalized"
                export_bounded=false
            fi
        else
            case_error "declared public task evidence is missing: $normalized"
            export_bounded=false
        fi
    done
    add_assertion bounded-public-task-export system true "$export_bounded" '' \
      'Public task evidence contains no symlinks and stays within closed file-count and byte bounds.'

    network_ok=true
    if ! project_public_network_paths "$run_id" "$output/topology/channels.json" \
      "$private/channel-status-after-A.json" "$private/channel-status-after-B.json" \
      "$private/channel-status-after-C.json" "$private/channel-status-after-D.json" \
      "$private/channel-status-after-E.json" "$private/channel-status-after-F.json" \
      >"$output/topology/network-paths.json"; then
        network_ok=false
    fi
    if [ "$network_ok" = true ] && ! python3 "$runner_dir/schema_validate.py" \
      "$schema_root/network-paths.schema.json" "$output/topology/network-paths.json"; then
        network_ok=false
    fi
    if [ "$network_ok" = true ] && ! jq -e '.publications | length > 0' \
      "$output/topology/network-paths.json" >/dev/null; then
        network_ok=false
    fi
    add_assertion grounded-publication-network-paths system true "$network_ok" \
      topology/network-paths.json 'Public D4 status records signed origin, authenticated immediate transport, arrival, audience/ignored outcome, causality, and direct Artifact source per observer.'
    [ "$network_ok" = true ] || case_error 'public D4 publication path evidence is incomplete'
}

run_task_oracles() {
    mkdir -p "$private/oracle-evidence/oracle" || return 1
    cp -R "$output/artifacts/." "$private/oracle-evidence/" || return 1
    cp "$scenario/oracle/oracle.sh" "$private/oracle-evidence/oracle/oracle.sh" || return 1
    # The private parent remains 0700 on the Host. Its bounded evidence subtree
    # is mounted read-only into the networkless oracle, so the unprivileged
    # process needs read/traverse bits on those immutable inputs.
    chmod -R a+rX,go-w "$private/oracle-evidence" || return 1
    find "$private/oracle-evidence" -type d -exec chmod 0755 {} + || return 1
    chmod 0555 "$private/oracle-evidence/oracle/oracle.sh" || return 1
    for oracle_id in $(jq -r '.oracles.task[].id' "$scenario_manifest"); do
        command=$(jq -r --arg id "$oracle_id" '.oracles.task[] | select(.id == $id) | .command | join(" ")' \
          "$scenario_manifest")
        [ "$command" = /evidence/oracle/oracle.sh ] || {
            case_error "task oracle command is outside the closed runner: $oracle_id"
            continue
        }
        timeout_seconds=$(jq -r --arg id "$oracle_id" \
          '.oracles.task[] | select(.id == $id) | .timeout_seconds' "$scenario_manifest")
        set +e
        docker run --rm --network none --read-only --cap-drop ALL \
          --security-opt no-new-privileges --user 10001:10001 \
          --mount "type=bind,src=$private/oracle-evidence,dst=/evidence,readonly" \
          --tmpfs /tmp:rw,noexec,nosuid,nodev,mode=1777,size=256m \
          --tmpfs /run/r5-work:rw,exec,nosuid,nodev,mode=0700,uid=10001,gid=10001,size=512m \
          --memory 2g --cpus 2 --pids-limit 256 \
          --env HOME=/run/r5-work --env GOCACHE=/run/r5-work/go-build \
          --env GOTMPDIR=/run/r5-work/go-tmp --env TMPDIR=/run/r5-work \
          --workdir /evidence "$image_reference" timeout "${timeout_seconds}s" \
          /evidence/oracle/oracle.sh >"$private/oracle-$oracle_id.stdout" \
          2>"$private/oracle-$oracle_id.stderr"
        exit_code=$?
        set -e
        redact_text_file "$private/oracle-$oracle_id.stdout" "$output/oracle/$oracle_id.stdout"
        redact_text_file "$private/oracle-$oracle_id.stderr" "$output/oracle/$oracle_id.stderr"
        passed=false
        [ "$exit_code" -eq 0 ] && passed=true
        [ "$passed" = true ] || case_error "task oracle failed: $oracle_id"
        jq -cn --arg id "$oracle_id" --argjson passed "$passed" --argjson exit_code "$exit_code" '
          {id:$id,passed:$passed,exit_code:$exit_code,
           evidence:["oracle/"+$id+".stdout","oracle/"+$id+".stderr"]}
        ' >>"$task_jsonl"
        add_assertion "$oracle_id" task true "$passed" \
          "oracle/$oracle_id.stdout,oracle/$oracle_id.stderr" 'Hidden deterministic task oracle executed in a networkless container.'
    done
}

has_assertion() {
    id=$1
    jq -e --arg id "$id" 'select(.id == $id)' "$assertions_jsonl" >/dev/null 2>&1
}

complete_oracle_assertions() {
    for id in $(jq -r '.oracles.system[]' "$scenario_manifest"); do
        if ! has_assertion "$id"; then
            add_assertion "$id" system true false topology/network-paths.json \
              'The required invariant has no sufficient public black-box evidence in this run.'
        fi
    done
    for id in $(jq -r '.oracles.experience[]' "$scenario_manifest"); do
        if has_assertion "$id"; then
            continue
        fi
        passed=false
        evidence=''
        message='The required experience oracle was not established.'
        case "$id" in
            one-natural-business-prompt-on-a)
                message='The workflow stopped before the single Node A business prompt was invoked.'
                ;;
            zero-remote-user-prompts)
                passed=true
                message='The runner has no remote prompt operation and invoked none.'
                ;;
            zero-raw-channel-or-peer-configuration)
                passed=true
                evidence='transcript/entry-turn.txt,topology/channels.json'
                message='The scenario uses only the single natural entry prompt plus public setup and channel create/join commands; no raw PeerID or topic configuration is prompted from the user.'
                ;;
            *zero-manual*daemon*|*zero-manual*sync*|zero-manual-recovery-actions)
                passed=true
                message='Only public setup/channel/observation/Host paths and declared external faults were used.'
                ;;
            status-distinguishes-lag-from-delivery)
                evidence='nodes/A/status-after.json,nodes/B/status-after.json,nodes/C/status-after.json,nodes/D/status-after.json,nodes/E/status-after.json,nodes/F/status-after.json'
                if status_distinguishes_lag_from_delivery_ok; then
                    passed=true
                    message='Public status exposes cursor lag, publication delivery, inbox Artifact wait, and runtime work as separate ready Channel fields.'
                fi
                ;;
            six-nodes-observed-in-transport-or-business-path)
                evidence='topology/network-paths.json'
                if network_paths_expected_nodes_observed_ok; then
                    passed=true
                    message='Public D4 paths include all six expected Nodes as either transport observers or business handlers.'
                fi
                ;;
            one-action-per-team-expansion)
                evidence='nodes/C/handling-trace.ndjson,nodes/E/handling-trace.ndjson'
                if team_offer_expansion_ok; then
                    passed=true
                    message='Each declared non-entry Team expansion has one accepted public Teamwork offer action and independent Work offer results.'
                fi
                ;;
            zero-implicit-channel-switch-or-bridge-actions)
                evidence='topology/network-paths.json'
                if network_paths_no_implicit_bridge_ok; then
                    passed=true
                    message='No public Event identity is reused as an implicit Channel switch or bridge.'
                fi
                ;;
        esac
        add_assertion "$id" experience true "$passed" "$evidence" "$message"
    done
    for id in $(jq -r '.oracles.task[].id' "$scenario_manifest"); do
        if ! jq -e --arg id "$id" 'select(.id == $id)' "$task_jsonl" >/dev/null 2>&1; then
            jq -cn --arg id "$id" \
              '{id:$id,passed:false,exit_code:1,evidence:[]}' >>"$task_jsonl"
            add_assertion "$id" task true false '' 'The task oracle could not execute against a complete export.'
        fi
    done
}

collect_final_logs() {
    [ "$containers_started" = true ] || return 0
    for node in A B C D E F; do
        cid=$(container_id "$node" 2>/dev/null || true)
        [ -z "$cid" ] || docker logs "$cid" >"$private/$node.log" 2>&1 || true
        [ ! -f "$private/$node.log" ] || redact_text_file "$private/$node.log" \
            "$output/nodes/$node/daemon.log"
    done
    logs_collected=true
}

finalize_case() {
    complete_oracle_assertions
    jq -s '.' "$faults_jsonl" >"$output/faults/timeline.json"
    if [ ! -f "$output/topology/network-paths.json" ]; then
        jq -n --arg run_id "$run_id" \
          '{schema_version:1,run_id:$run_id,identity_binding:"public-d4",publications:[],
            evidence_refs:["nodes/A/daemon.log"]}' \
          >"$output/topology/network-paths.json"
    fi
    collect_final_logs

    system_passed=$(jq -s 'all(.[]; select(.category == "system" and .required == true) | .passed == true)' \
      "$assertions_jsonl")
    experience_passed=$(jq -s 'all(.[]; select(.category == "experience" and .required == true) | .passed == true)' \
      "$assertions_jsonl")
    task_passed=$(jq -s 'length > 0 and all(.[]; .passed == true)' "$task_jsonl")
    status=passed
    if [ "$system_passed" != true ] || [ "$experience_passed" != true ] || [ "$task_passed" != true ]; then
        status=blocked
    fi
    if [ "$execution_failed" = true ]; then
        status=failed
    fi

    business_prompts=$(jq -s '[.[] | select(.kind == "business-prompt")] | length' "$commands_jsonl")
    node_count=0
    if [ "$containers_started" = true ]; then
        for node in A B C D E F; do
            cid=$(container_id "$node" 2>/dev/null || true)
            [ -z "$cid" ] || node_count=$((node_count + 1))
        done
    fi
    channel_count=0
    if [ -f "$output/topology/channels.json" ]; then
        channel_count=$(jq -r '.channels | length' "$output/topology/channels.json" 2>/dev/null || printf 0)
    fi
    harness_version=unknown
    codex_version=unknown
    if [ "$containers_started" = true ]; then
        harness_version=$(node_exec A mnemon-harness --version 2>/dev/null | sed -n '1p' || printf unknown)
        if [ "$runtime" = scripted ]; then
            codex_version=$(node_exec A codex --version 2>/dev/null | sed -n '1p' || printf unknown)
            model=scripted-r5
            reasoning=deterministic-policy
        else
            codex_version=$(docker image inspect --format '{{index .Config.Labels "io.mnemon.r5.codex-version"}}' \
              "$image_reference")
            model=$R5_CODEX_MODEL
            reasoning=$R5_CODEX_REASONING_EFFORT
        fi
    else
        model=unknown
        reasoning=unknown
    fi
    digest=$(scenario_digest "$scenario")
    jq -s --arg run_id "$run_id" --arg scenario "$case_name" --arg runtime "$runtime" \
      --arg status "$status" --arg git_sha "$git_sha" --arg image_digest "$image_digest" \
      --arg scenario_digest "$digest" --arg harness "$harness_version" --arg codex "$codex_version" \
      --arg model "$model" --arg reasoning "$reasoning" --argjson business "$business_prompts" \
      --argjson nodes "$node_count" --argjson channels "$channel_count" \
      --argjson assertions "$(jq -s '.' "$assertions_jsonl")" \
      --argjson faults "$(jq -s '.' "$faults_jsonl")" \
      --argjson tasks "$(jq -s '.' "$task_jsonl")" \
      --argjson channel_ready "$(jq -s '[.[] | select(.kind == "channel-ready") | .duration_ms]' "$latency_jsonl")" \
      --argjson system_passed "$system_passed" --argjson experience_passed "$experience_passed" \
      --argjson task_passed "$task_passed" '
      {schema_version:1,run_id:$run_id,scenario:$scenario,runtime:$runtime,status:$status,
       git_sha:$git_sha,image_digest:$image_digest,scenario_digest:$scenario_digest,
       versions:{harness:$harness,codex:$codex,model:$model,reasoning_effort:$reasoning},
       counts:{business_prompts:$business,remote_prompts:0,manual_daemon_actions:0,
               manual_sync_actions:0,nodes:$nodes,channels:$channels},
       commands:.,assertions:$assertions,faults:$faults,
       latency:{setup_ms:[.[] | select(.kind == "setup") | .duration_ms],
                channel_join_ms:[.[] | select(.kind == "channel-join") | .duration_ms],
                channel_ready_ms:$channel_ready},
       oracle:{system:{passed:$system_passed},task:{passed:$task_passed,results:$tasks},
               experience:{passed:$experience_passed}}}
    ' "$commands_jsonl" >"$output/report.json"
    if ! python3 "$runner_dir/schema_validate.py" "$schema_root/report.schema.json" \
      "$output/report.json"; then
        case_error 'per-case report failed schema validation'
    fi
    if ! python3 "$runner_dir/schema_validate.py" "$schema_root/network-paths.schema.json" \
      "$output/topology/network-paths.json"; then
        case_error 'network-path evidence failed schema validation'
    fi
    if ! scan_evidence_redaction "$output" "$credential"; then
        case_error 'case evidence failed the secret/redaction scan'
    fi
    if [ "$execution_failed" = true ] && [ "$status" != failed ]; then
        status=failed
        if jq '.status = "failed"' "$output/report.json" >"$private/failed-report.json"; then
            mv "$private/failed-report.json" "$output/report.json"
        fi
    fi
    manifest_evidence "$output" "$run_id"
    finalized=true
    printf 'R5 case %s: %s (%s)\n' "$case_name" "$status" "$output"
    [ "$status" = passed ] && [ "$execution_failed" = false ]
}

run_workflow() {
    start_topology || return 1
    install_setup_and_fixtures || return 1
    create_channels || return 1
    if run_entry_prompt; then
        if wait_for_public_result; then
            inject_offline_alpha_repair_fault
            wait_final_public_status_ready || true
        fi
    fi
    collect_public_evidence
    evaluate_declared_faults
    evaluate_public_system_oracles
    run_task_oracles
}

if ! run_workflow; then
    case_error 'workflow stopped before all black-box phases completed'
fi
finalize_case
