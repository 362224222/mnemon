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
    : >"$private/alpha.invite"
    channel_create C beta && channel_join D beta && channel_join E beta || return 1
    : >"$private/beta.invite"
    channel_create E gamma && channel_join F gamma && channel_join A gamma || return 1
    : >"$private/gamma.invite"

    readiness_deadline_ms=$(( $(date +%s%3N) + 10000 ))
    all_ready=false
    while [ "$(date +%s%3N)" -lt "$readiness_deadline_ms" ]; do
        all_ready=true
        for node in A B C D E F; do
            if [ "$(date +%s%3N)" -ge "$readiness_deadline_ms" ]; then
                all_ready=false
                break
            fi
            raw="$private/channel-status-$node.json"
            if ! node_exec "$node" timeout 1s mnemon-harness channel status --json >"$raw" 2>/dev/null ||
               ! jq -e '.status == "ok" and all(.channels[]; .topic.status == "joined")' \
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
    for node in A B C D E F; do
        if [ -s "$private/channel-status-$node.json" ] &&
           jq -e . "$private/channel-status-$node.json" >/dev/null 2>&1; then
            redact_json <"$private/channel-status-$node.json" \
              >"$output/nodes/$node/channel-status-before.json"
        fi
    done
    [ "$all_ready" = true ] || {
        case_error 'Channel topics did not become joined within 10 seconds'
        return 1
    }
    ready_at=$(date +%s%3N)
    while IFS=$(printf '\t') read -r alias join_index; do
        accepted_at=$(jq -s -r --argjson index "$join_index" '
          [.[] | select(.kind == "channel-join")][$index].finished_unix_ms
        ' "$commands_jsonl")
        ready_ms=$((ready_at - accepted_at))
        jq -cn --arg channel "$alias" --argjson duration "$ready_ms" \
          '{kind:"channel-ready",channel:$channel,duration_ms:$duration}' >>"$latency_jsonl"
        ready_fast=true
        [ "$ready_ms" -le 10000 ] || ready_fast=false
        add_assertion "channel-$alias-ready-within-10s" system true "$ready_fast" \
          "nodes/A/channel-status-before.json,nodes/C/channel-status-before.json,nodes/E/channel-status-before.json" \
          'Final join acceptance reached joined topics and complete reachable-member baselines within 10 seconds.'
    done <<EOF
alpha	1
beta	3
gamma	5
EOF
    for node in A B C D E F; do
        public_command "$node" status "nodes/$node/status-before.json" mnemon-harness status || return 1
    done
    write_channel_topology
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

evaluate_declared_faults() {
    while IFS=$(printf '\t') read -r id type phase target observation; do
        injected=false
        observed=false
        evidence=
        detail='The public black-box surface has no exact phase receipt or external fault gate; injection and observation remain fail-closed.'
        at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
        if [ "$case_name" = payment-review ] && [ "$id" = alpha-gossip-via-b ]; then
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

run_entry_prompt() {
    stdout="$private/entry-turn.stdout"
    stderr="$private/entry-turn.stderr"
    prompt="$scenario/$(jq -r '.prompt_file' "$scenario_manifest")"
    started=$(date +%s%3N)
    if [ "$case_name" = payment-review ]; then
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
        if [ "$runtime" = scripted ]; then
            # Scenario policies use stable topology labels, while the public
            # selector accepts only the effective member alias. Bind the one
            # entry target from the already-validated public D4 topology.
            c_alias=$(jq -er '
              first(.channels[] | select(.alias == "alpha") | .members[] |
                select(.node == "C")) | .alias
            ' "$output/topology/channels.json")
            node_exec A sh -c '
              set -eu
              policy=/workspace/.r5/policy.json
              next=/workspace/.r5/policy.next
              trap '\''rm -f "$next"'\'' EXIT HUP INT TERM
              umask 077
              jq --arg to "$1" '\''.entry_to = $to'\'' "$policy" >"$next"
              mv "$next" "$policy"
              trap - EXIT HUP INT TERM
            ' sh "$c_alias"
        fi
        prompt_receipt="$private/payment-relay-prompt.json"
        scope_b="$private/payment-relay-scope-b.json"
        scope_c="$private/payment-relay-scope-c.json"
        wrapper_stdout="$private/payment-relay-wrapper.stdout"
        wrapper_stderr="$private/payment-relay-wrapper.stderr"
        set +e
        "$repo_root/harness/test/e2e/faultplane/docker_network.sh" edge \
          --network "$project-mesh" --left "$a_container" --right "$c_container" \
          --token alpha-gossip-via-b --receipt-dir "$output/faults" -- \
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
            redact_json <"$scope_b" >"$output/faults/alpha-gossip-via-b-scope-b.json"
            redact_json <"$scope_c" >"$output/faults/alpha-gossip-via-b-scope-c.json"
            chmod 0600 "$output/faults/alpha-gossip-via-b-scope-b.json" \
              "$output/faults/alpha-gossip-via-b-scope-c.json"
        fi
        if [ -s "$wrapper_stderr" ]; then
            redact_text_file "$wrapper_stderr" "$output/transcript/payment-relay-fault.stderr"
        fi
        [ -e "$stdout" ] || : >"$stdout"
        [ -e "$stderr" ] || : >"$stderr"
    elif [ "$runtime" = scripted ]; then
        node_exec A timeout "${R5_E2E_TURN_TIMEOUT_SECONDS:-300}s" codex exec - \
          <"$prompt" >"$stdout" 2>"$stderr" &
        prompt_pid=$!
        set +e
        wait "$prompt_pid"
        exit_code=$?
        set -e
    else
        node_exec A timeout "${R5_E2E_TURN_TIMEOUT_SECONDS:-300}s" /opt/r5/bin/live-codex-exec \
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
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if node_exec A test -d /workspace/result &&
           [ "$(node_exec A sh -c 'find /workspace/result -type f -size +0c | wc -l')" -gt 0 ]; then
            return 0
        fi
        sleep 0.5
    done
    case_error "no nonempty public entry-node result appeared within ${timeout_seconds}s"
    return 1
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
            *zero-manual*daemon*|*zero-manual*sync*|zero-manual-recovery-actions)
                passed=true
                message='Only public setup/channel/observation/Host paths and declared external faults were used.'
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
        wait_for_public_result || true
    fi
    collect_public_evidence
    evaluate_declared_faults
    run_task_oracles
}

if ! run_workflow; then
    case_error 'workflow stopped before all black-box phases completed'
fi
finalize_case
