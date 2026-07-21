#!/bin/sh
set -eu

die() {
    printf 'r5-docker-fault: %s\n' "$*" >&2
    exit 1
}

usage() {
    printf '%s\n' \
      'usage: docker_network.sh edge --network NETWORK --left CONTAINER --right CONTAINER' \
      '       --token TOKEN --receipt-dir DIRECTORY [--expect-exit CODE] -- COMMAND [ARG...]' \
      '   or: docker_network.sh offline --network NETWORK --container CONTAINER' \
      '       --token TOKEN --receipt-dir DIRECTORY [--expect-exit CODE] -- COMMAND [ARG...]' \
      '   or: docker_network.sh restart --container CONTAINER --token TOKEN' \
      '       --receipt-dir DIRECTORY [--expect-exit CODE] -- COMMAND [ARG...]' >&2
    exit 2
}

docker_bin=${R5_DOCKER_BIN:-docker}
nft_bin=${R5_NFT_BIN:-nft}
faultplane_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
schema_validator="$faultplane_dir/../runner/schema_validate.py"
schema_root="$faultplane_dir/../schemas"
mode=${1:-}
case "$mode" in
    edge|offline|restart) shift ;;
    *) usage ;;
esac

network=
left=
right=
container=
token=
receipt_dir=
expected_exit=0
while [ "$#" -gt 0 ]; do
    case "$1" in
        --network|--left|--right|--container|--token|--receipt-dir|--expect-exit)
            [ "$#" -ge 2 ] || usage
            value=$2
            case "$1" in
                --network) network=$value ;;
                --left) left=$value ;;
                --right) right=$value ;;
                --container) container=$value ;;
                --token) token=$value ;;
                --receipt-dir) receipt_dir=$value ;;
                --expect-exit) expected_exit=$value ;;
            esac
            shift 2
            ;;
        --)
            shift
            break
            ;;
        *) usage ;;
    esac
done
[ "$#" -gt 0 ] || usage

case "$token" in
    [A-Za-z0-9]* ) ;;
    * ) usage ;;
esac
case "$token" in
    *[!A-Za-z0-9._-]* ) usage ;;
esac
[ "${#token}" -le 80 ] || usage
case "$expected_exit" in
    ''|*[!0-9]*) usage ;;
esac
[ "$expected_exit" -le 255 ] || usage
[ -d "$receipt_dir" ] && [ ! -L "$receipt_dir" ] || usage
[ "$(stat -c '%a' "$receipt_dir")" = 700 ] || die 'receipt directory must have mode 0700'
for reference in "$network" "$left" "$right" "$container"; do
    case "$reference" in
        *[!A-Za-z0-9_.:-]*) usage ;;
    esac
    [ "${#reference}" -le 128 ] || usage
done
case "$mode" in
    edge) [ -n "$network" ] && [ -n "$left" ] && [ -n "$right" ] && [ "$left" != "$right" ] || usage ;;
    offline) [ -n "$network" ] && [ -n "$container" ] || usage ;;
    restart) [ -z "$network" ] && [ -n "$container" ] || usage ;;
esac

command -v "$docker_bin" >/dev/null 2>&1 || die 'Docker command is unavailable'
command -v jq >/dev/null 2>&1 || die 'jq is unavailable'
command -v sha256sum >/dev/null 2>&1 || die 'sha256sum is unavailable'
command -v python3 >/dev/null 2>&1 || die 'python3 is unavailable'
if [ "$mode" = edge ]; then
    [ "$(id -u)" -eq 0 ] || die 'edge shaping requires root access to the bridge firewall'
    command -v "$nft_bin" >/dev/null 2>&1 || die 'nft is unavailable'
    "$nft_bin" --json list ruleset >/dev/null 2>&1 ||
        die 'the nftables bridge firewall is unavailable'
fi

canonical_container() {
    resolved=$("$docker_bin" inspect --format '{{.Id}}' "$1" 2>/dev/null) || return 1
    [ "${#resolved}" -eq 64 ] || return 1
    case "$resolved" in
        *[!a-f0-9]*) return 1 ;;
    esac
    printf '%s\n' "$resolved"
}

network_id=
network_name=
if [ "$mode" != restart ]; then
    network_document=$("$docker_bin" network inspect "$network" 2>/dev/null) ||
        die 'Docker network is unavailable'
    network_id=$(printf '%s\n' "$network_document" | jq -er \
      '.[0] | select(.Driver == "bridge" and .Scope == "local") | .Id') ||
        die 'fault target must be one local Docker bridge network'
    network_name=$(printf '%s\n' "$network_document" | jq -er '.[0].Name')
    [ "${#network_id}" -eq 64 ] || die 'Docker returned a non-canonical network identity'
    case "$network_id" in
        *[!a-f0-9]*) die 'Docker returned a non-canonical network identity' ;;
    esac
    case "$network_name" in
        ''|*[!A-Za-z0-9_.:-]*) die 'Docker returned an unsupported network name' ;;
    esac
    case "$network_name" in
        [A-Za-z0-9]*) ;;
        *) die 'Docker returned an unsupported network name' ;;
    esac
    [ "${#network_name}" -le 128 ] || die 'Docker returned an unsupported network name'
fi
case "$mode" in
    edge)
        left_id=$(canonical_container "$left") || die 'left container is unavailable or non-canonical'
        right_id=$(canonical_container "$right") || die 'right container is unavailable or non-canonical'
        [ "$left_id" != "$right_id" ] || die 'edge endpoints resolve to the same container'
        ;;
    offline|restart)
        container_id=$(canonical_container "$container") || die 'container is unavailable or non-canonical'
        ;;
esac

private=$(mktemp -d)
chmod 0700 "$private"
action_output="$receipt_dir/$token-action.json"
[ ! -e "$action_output" ] || die 'fault action receipt already exists'
fault_active=false
cleanup_failed=false
edge_table=
offline_endpoint=
restart_required=false

endpoint_json() {
    target=$1
    "$docker_bin" inspect "$target" | jq -cer --arg network_id "$network_id" '
      .[0].NetworkSettings.Networks | to_entries[] |
      select(.value.NetworkID == $network_id) | .value
    '
}

restore_edge() {
    restore_ok=true
    # Discover the exact table from kernel state. Deleting a table atomically
    # removes its hooked chain and both rules, including after a signal lands
    # between the nft transaction and the next shell assignment.
    if "$nft_bin" list table bridge "$edge_table" >/dev/null 2>&1; then
        "$nft_bin" delete table bridge "$edge_table" >/dev/null 2>&1 || restore_ok=false
    fi
    if "$nft_bin" list table bridge "$edge_table" >/dev/null 2>&1; then
        restore_ok=false
    fi
    [ "$restore_ok" = true ] || return 1
    fault_active=false
}

restore_offline() {
    current=$(endpoint_json "$container_id" 2>/dev/null || true)
    if [ -z "$current" ]; then
        ipv4=$(jq -r '.IPAddress' "$offline_endpoint")
        ipv6=$(jq -r '.GlobalIPv6Address' "$offline_endpoint")
        set -- network connect
        [ -z "$ipv4" ] || set -- "$@" --ip "$ipv4"
        [ -z "$ipv6" ] || set -- "$@" --ip6 "$ipv6"
        while IFS= read -r alias; do
            [ -z "$alias" ] || set -- "$@" --alias "$alias"
        done <<EOF
$(jq -r '(.Aliases // [])[] | select(test("^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"))' "$offline_endpoint")
EOF
        "$docker_bin" "$@" "$network_id" "$container_id" >/dev/null 2>&1 || return 1
        current=$(endpoint_json "$container_id" 2>/dev/null || true)
    fi
    [ -n "$current" ] || return 1
    expected_ipv4=$(jq -r '.IPAddress' "$offline_endpoint")
    observed_ipv4=$(printf '%s\n' "$current" | jq -r '.IPAddress')
    [ "$expected_ipv4" = "$observed_ipv4" ] || return 1
    expected_ipv6=$(jq -r '.GlobalIPv6Address' "$offline_endpoint")
    observed_ipv6=$(printf '%s\n' "$current" | jq -r '.GlobalIPv6Address')
    [ "$expected_ipv6" = "$observed_ipv6" ] || return 1
    expected_aliases=$(jq -c '(.Aliases // []) | sort | unique' "$offline_endpoint")
    printf '%s\n' "$current" | jq -e --argjson expected "$expected_aliases" '
      (.Aliases // []) as $actual | all($expected[]; . as $alias | ($actual | index($alias)) != null)
    ' >/dev/null || return 1
    fault_active=false
}

restore_restart() {
    running=$($docker_bin inspect --format '{{.State.Running}}' "$container_id" 2>/dev/null || true)
    if [ "$running" != true ]; then
        "$docker_bin" start "$container_id" >/dev/null 2>&1 || return 1
    fi
    [ "$("$docker_bin" inspect --format '{{.State.Running}}' "$container_id")" = true ] || return 1
    restart_required=false
    fault_active=false
}

restore_fault() {
    if [ "$fault_active" != true ]; then
        case "$mode" in
            edge)
                [ -n "$edge_table" ] &&
                  "$nft_bin" list table bridge "$edge_table" >/dev/null 2>&1 && fault_active=true
                ;;
            offline)
                if [ -n "$offline_endpoint" ] && [ -f "$offline_endpoint" ] &&
                  [ -z "$(endpoint_json "$container_id" 2>/dev/null || true)" ]; then
                    fault_active=true
                fi
                ;;
            restart)
                [ "$restart_required" = true ] && fault_active=true
                ;;
        esac
    fi
    [ "$fault_active" = true ] || return 0
    case "$mode" in
        edge) restore_edge ;;
        offline) restore_offline ;;
        restart) restore_restart ;;
    esac
}

cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    if ! restore_fault; then
        cleanup_failed=true
        printf 'r5-docker-fault: external fault restoration failed\n' >&2
    fi
    find "$private" -type f -delete 2>/dev/null || true
    find "$private" -depth -type d -empty -delete 2>/dev/null || true
    if [ "$cleanup_failed" = true ] && [ "$status" -eq 0 ]; then
        status=1
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
case "$mode" in
    edge)
        left_endpoint=$(endpoint_json "$left_id") || die 'left container is not attached to the target network'
        right_endpoint=$(endpoint_json "$right_id") || die 'right container is not attached to the target network'
        left_ip=$(printf '%s\n' "$left_endpoint" | jq -er '.IPAddress | select(length > 0)') ||
            die 'left endpoint has no IPv4 address'
        right_ip=$(printf '%s\n' "$right_endpoint" | jq -er '.IPAddress | select(length > 0)') ||
            die 'right endpoint has no IPv4 address'
        python3 - "$left_ip" "$right_ip" <<'PY' || die 'edge endpoint has an invalid IPv4 address'
import ipaddress
import sys
for value in sys.argv[1:]:
    if ipaddress.ip_address(value).version != 4:
        raise SystemExit(1)
PY
        edge_table="R5E$(printf '%s\n' "$token:$network_id:$left_id:$right_id" | sha256sum | awk '{print substr($1,1,16)}')"
        if "$nft_bin" list table bridge "$edge_table" >/dev/null 2>&1; then
            die 'derived edge table already exists'
        fi
        fault_active=true
        "$nft_bin" -f - <<EOF
add table bridge $edge_table
add chain bridge $edge_table forward { type filter hook forward priority -200; policy accept; }
add rule bridge $edge_table forward ip saddr $left_ip ip daddr $right_ip counter drop
add rule bridge $edge_table forward ip saddr $right_ip ip daddr $left_ip counter drop
EOF
        "$nft_bin" --json list table bridge "$edge_table" | jq -e \
          --arg table "$edge_table" --arg left "$left_ip" --arg right "$right_ip" '
          def drop_rule($source; $destination):
            .family == "bridge" and .table == $table and .chain == "forward" and
            (.expr | length) == 4 and
            .expr[0].match.left.payload == {protocol:"ip",field:"saddr"} and
            .expr[0].match.right == $source and
            .expr[1].match.left.payload == {protocol:"ip",field:"daddr"} and
            .expr[1].match.right == $destination and
            (.expr[2] | has("counter")) and (.expr[3] | has("drop"));
          ([.nftables[].chain? | select(
            .family == "bridge" and .table == $table and .name == "forward" and
            .type == "filter" and .hook == "forward" and .prio == -200 and
            .policy == "accept")] | length) == 1 and
          ([.nftables[].rule? | select(.family == "bridge" and .table == $table and
            .chain == "forward")] as $rules |
            ($rules | length) == 2 and
            ([$rules[] | select(drop_rule($left; $right))] | length) == 1 and
            ([$rules[] | select(drop_rule($right; $left))] | length) == 1)
        ' >/dev/null || die 'edge rules did not verify after application'
        ;;
    offline)
        endpoint_json "$container_id" >"$private/endpoint.json" ||
            die 'container is not attached to the target network'
        offline_endpoint="$private/endpoint.json"
        jq -e '
          (.IPAddress | type == "string" and length > 0) and
          (.GlobalIPv6Address | type == "string") and
          ((.Aliases // []) | type == "array" and length <= 16 and
            all(.[]; type == "string" and test("^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")))
        ' "$offline_endpoint" >/dev/null || die 'network endpoint cannot be restored through the bounded surface'
        offline_ipv4=$(jq -r '.IPAddress' "$offline_endpoint")
        offline_ipv6=$(jq -r '.GlobalIPv6Address' "$offline_endpoint")
        python3 - "$offline_ipv4" "$offline_ipv6" <<'PY' || die 'network endpoint has an invalid IP address'
import ipaddress
import sys
if ipaddress.ip_address(sys.argv[1]).version != 4:
    raise SystemExit(1)
if sys.argv[2] and ipaddress.ip_address(sys.argv[2]).version != 6:
    raise SystemExit(1)
PY
        fault_active=true
        "$docker_bin" network disconnect "$network_id" "$container_id" >/dev/null
        [ -z "$(endpoint_json "$container_id" 2>/dev/null || true)" ] ||
            die 'container remained attached after disconnect'
        ;;
    restart)
        [ "$("$docker_bin" inspect --format '{{.State.Running}}' "$container_id")" = true ] ||
            die 'restart target is not running'
        started_before=$("$docker_bin" inspect --format '{{.State.StartedAt}}' "$container_id")
        restart_required=true
        fault_active=true
        "$docker_bin" kill --signal KILL "$container_id" >/dev/null
        [ "$("$docker_bin" inspect --format '{{.State.Running}}' "$container_id")" = false ] ||
            die 'container remained running after SIGKILL'
        exit_after_kill=$("$docker_bin" inspect --format '{{.State.ExitCode}}' "$container_id")
        [ "$exit_after_kill" -eq 137 ] || die 'container exit did not record SIGKILL status 137'
        "$docker_bin" start "$container_id" >/dev/null
        [ "$("$docker_bin" inspect --format '{{.State.Running}}' "$container_id")" = true ] ||
            die 'container did not restart'
        started_after=$("$docker_bin" inspect --format '{{.State.StartedAt}}' "$container_id")
        [ "$started_after" != "$started_before" ] || die 'container restart timestamp did not advance'
        restart_required=false
        fault_active=false
        ;;
esac

set +e
"$@"
command_exit=$?
set -e
[ "$command_exit" -eq "$expected_exit" ] ||
    die "fault-scope command exit $command_exit differs from expected $expected_exit"

if ! restore_fault; then
    cleanup_failed=true
    die 'external fault restoration failed after scoped command'
fi

case "$mode" in
    edge)
        jq -n --arg token "$token" --arg generated_at "$generated_at" \
          --arg network_id "$network_id" --arg network_name "$network_name" \
          --arg left_container_id "$left_id" --arg right_container_id "$right_id" \
          --arg left_ip "$left_ip" --arg right_ip "$right_ip" --arg table "$edge_table" \
          --argjson command_exit "$command_exit" '
          {schema_version:1,token:$token,action:"docker-edge-block",
           external_action_applied:true,public_observation_bound:false,restored:true,
           network_id:$network_id,network_name:$network_name,
           left_container_id:$left_container_id,right_container_id:$right_container_id,
           left_ipv4:$left_ip,right_ipv4:$right_ip,nft_table:$table,
           command_exit_code:$command_exit,generated_at:$generated_at}
        ' >"$private/action.json"
        ;;
    offline)
        ipv4=$(jq -r '.IPAddress' "$offline_endpoint")
        aliases=$(jq -c '(.Aliases // []) | sort | unique' "$offline_endpoint")
        jq -n --arg token "$token" --arg generated_at "$generated_at" \
          --arg network_id "$network_id" --arg network_name "$network_name" \
          --arg container_id "$container_id" --arg ipv4 "$ipv4" \
          --argjson aliases "$aliases" --argjson command_exit "$command_exit" '
          {schema_version:1,token:$token,action:"docker-node-disconnect",
           external_action_applied:true,public_observation_bound:false,restored:true,
           network_id:$network_id,network_name:$network_name,container_id:$container_id,
           restored_ipv4:$ipv4,restored_aliases:$aliases,
           command_exit_code:$command_exit,generated_at:$generated_at}
        ' >"$private/action.json"
        ;;
    restart)
        jq -n --arg token "$token" --arg generated_at "$generated_at" \
          --arg container_id "$container_id" --arg started_before "$started_before" \
          --arg started_after "$started_after" --argjson command_exit "$command_exit" '
          {schema_version:1,token:$token,action:"docker-node-kill-restart",
           external_action_applied:true,public_observation_bound:false,restored:true,
           container_id:$container_id,signal:"KILL",exit_code_after_kill:137,
           started_at_before:$started_before,started_at_after:$started_after,
           command_exit_code:$command_exit,generated_at:$generated_at}
        ' >"$private/action.json"
        ;;
esac
install -m 0600 "$private/action.json" "$action_output"
PYTHONDONTWRITEBYTECODE=1 python3 "$schema_validator" \
  "$schema_root/fault-action.schema.json" "$action_output" ||
    die 'Docker fault action failed its closed schema'
printf '%s\n' "$action_output"
