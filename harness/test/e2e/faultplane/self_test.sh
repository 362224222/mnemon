#!/bin/sh
set -eu

self_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$self_dir/../../../.." && pwd -P)
docker_bin=${R5_DOCKER_BIN:-docker}

die() {
    printf 'r5-faultplane-selftest: %s\n' "$*" >&2
    exit 1
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command is unavailable: $1"
}

container_http() {
    target=$1
    address=$2
    "$docker_bin" exec "$target" curl --fail --silent --show-error \
      --connect-timeout 1 --max-time 2 --output /dev/null "http://$address:8080/"
}

case "${1:-}" in
    __edge_callback)
        [ "$#" -eq 4 ] || die 'invalid edge callback arguments'
        if container_http "$2" "$3" >/dev/null 2>&1; then
            die 'blocked Docker edge remained reachable'
        fi
        container_http "$2" "$4" || die 'unrelated Docker edge was disrupted'
        exit 0
        ;;
    __offline_callback)
        [ "$#" -eq 5 ] || die 'invalid offline callback arguments'
        "$docker_bin" inspect "$3" | jq -e --arg network_id "$2" '
          [.[0].NetworkSettings.Networks | to_entries[] |
            select(.value.NetworkID == $network_id)] | length == 0
        ' >/dev/null || die 'offline endpoint remained attached'
        if container_http "$4" "$5" >/dev/null 2>&1; then
            die 'offline endpoint remained reachable at its prior address'
        fi
        exit 0
        ;;
    __restart_callback)
        [ "$#" -eq 3 ] || die 'invalid restart callback arguments'
        attempt=0
        while [ "$attempt" -lt 100 ]; do
            if container_http "$2" "$3" >/dev/null 2>&1; then
                exit 0
            fi
            attempt=$((attempt + 1))
            sleep 0.1
        done
        die 'restarted service did not become reachable'
        ;;
    __exit_callback)
        [ "$#" -eq 2 ] || die 'invalid exit callback arguments'
        exit "$2"
        ;;
esac

usage() {
    printf '%s\n' 'usage: self_test.sh --unit|--docker|--all' >&2
    exit 2
}

run_unit() {
    require_command go
    require_command python3
    sh -n "$self_dir/docker_network.sh" "$self_dir/response_loss.sh" "$self_dir/self_test.sh"
    go_version=$(awk '$1 == "go" { print $2; exit }' "$repo_root/harness/go.mod")
    (
        cd "$repo_root"
        env GOTOOLCHAIN="go$go_version" GOFLAGS=-mod=readonly \
          go -C harness test ./test/e2e/faultplane
    )
    for schema in "$self_dir"/../schemas/fault-action.schema.json \
      "$self_dir"/../schemas/fault-proxy-receipt.schema.json; do
        PYTHONDONTWRITEBYTECODE=1 python3 "$self_dir/../runner/schema_validate.py" \
          --check-schema "$schema"
    done
    printf '%s\n' 'R5 fault-plane unit self-tests passed'
}

wait_for_http() {
    target=$1
    address=$2
    attempt=0
    while [ "$attempt" -lt 100 ]; do
        if container_http "$target" "$address" >/dev/null 2>&1; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 0.1
    done
    return 1
}

wait_for_socket() {
    target=$1
    path=$2
    attempt=0
    while [ "$attempt" -lt 100 ]; do
        if "$docker_bin" exec --user 10001:10001 "$target" test -S "$path"; then
            return 0
        fi
        attempt=$((attempt + 1))
        sleep 0.1
    done
    return 1
}

run_docker() {
    require_command "$docker_bin"
    require_command nft
    require_command jq
    require_command python3
    require_command sha256sum
    [ "$(id -u)" -eq 0 ] || die 'Docker edge self-test requires root access'
    nft --json list ruleset >/dev/null 2>&1 ||
        die 'the nftables bridge firewall is unavailable'

    private=$(mktemp -d "${TMPDIR:-/tmp}/r5-faultplane-selftest.XXXXXX")
    chmod 0700 "$private"
    suffix=$(printf '%s:%s:%s\n' "$$" "$PPID" "$(date +%s%N)" |
      sha256sum | awk '{print substr($1,1,12)}')
    image="r5-faultplane-selftest:$suffix"
    network="r5fp-$suffix"
    node_a="r5fp-$suffix-a"
    node_b="r5fp-$suffix-b"
    node_c="r5fp-$suffix-c"
    receipts="$private/receipts"
    mkdir -m 0700 "$receipts"

    cleanup_docker_selftest() {
        status=$?
        trap - EXIT HUP INT TERM
        "$docker_bin" rm --force "$node_a" "$node_b" "$node_c" >/dev/null 2>&1 || true
        "$docker_bin" network rm "$network" >/dev/null 2>&1 || true
        "$docker_bin" image rm "$image" >/dev/null 2>&1 || true
        find "$private" -type f -delete 2>/dev/null || true
        find "$private" -depth -type d -empty -delete 2>/dev/null || true
        exit "$status"
    }
    trap cleanup_docker_selftest EXIT
    trap 'exit 129' HUP
    trap 'exit 130' INT
    trap 'exit 143' TERM

    "$docker_bin" build --file "$self_dir/Dockerfile.selftest" --tag "$image" "$repo_root"
    "$docker_bin" network create --driver bridge --internal "$network" >/dev/null
    "$docker_bin" run --detach --name "$node_a" --network "$network" "$image" >/dev/null
    "$docker_bin" run --detach --name "$node_b" --network "$network" "$image" \
      python3 -m http.server 8080 --bind 0.0.0.0 >/dev/null
    "$docker_bin" run --detach --name "$node_c" --network "$network" "$image" \
      python3 -m http.server 8080 --bind 0.0.0.0 >/dev/null

    node_a_id=$("$docker_bin" inspect --format '{{.Id}}' "$node_a")
    node_b_ip=$("$docker_bin" inspect --format "{{(index .NetworkSettings.Networks \"$network\").IPAddress}}" "$node_b")
    node_c_id=$("$docker_bin" inspect --format '{{.Id}}' "$node_c")
    node_c_ip=$("$docker_bin" inspect --format "{{(index .NetworkSettings.Networks \"$network\").IPAddress}}" "$node_c")
    network_id=$("$docker_bin" network inspect --format '{{.Id}}' "$network")
    wait_for_http "$node_a_id" "$node_b_ip" || die 'control HTTP endpoint B did not become ready'
    wait_for_http "$node_a_id" "$node_c_ip" || die 'target HTTP endpoint C did not become ready'

    edge_token="edge-$suffix"
    "$self_dir/docker_network.sh" edge --network "$network" --left "$node_a_id" \
      --right "$node_c_id" --token "$edge_token" --receipt-dir "$receipts" -- \
      "$self_dir/self_test.sh" __edge_callback "$node_a_id" "$node_c_ip" "$node_b_ip" >/dev/null
    jq -e '.action == "docker-edge-block" and .external_action_applied == true and
      .public_observation_bound == false and .restored == true' \
      "$receipts/$edge_token-action.json" >/dev/null || die 'edge receipt is invalid'
    container_http "$node_a_id" "$node_c_ip" || die 'blocked Docker edge was not restored'

    cleanup_token="edge-cleanup-$suffix"
    set +e
    "$self_dir/docker_network.sh" edge --network "$network" --left "$node_a_id" \
      --right "$node_c_id" --token "$cleanup_token" --receipt-dir "$receipts" -- \
      "$self_dir/self_test.sh" __exit_callback 23 >"$private/cleanup.stdout" \
      2>"$private/cleanup.stderr"
    cleanup_exit=$?
    set -e
    [ "$cleanup_exit" -ne 0 ] || die 'intentional edge callback failure unexpectedly succeeded'
    cleanup_table="R5E$(printf '%s\n' "$cleanup_token:$network_id:$node_a_id:$node_c_id" |
      sha256sum | awk '{print substr($1,1,16)}')"
    ! nft list table bridge "$cleanup_table" >/dev/null 2>&1 ||
        die 'edge callback failure left its derived nftables table installed'
    container_http "$node_a_id" "$node_c_ip" || die 'edge callback failure did not restore connectivity'

    offline_token="offline-$suffix"
    "$self_dir/docker_network.sh" offline --network "$network" --container "$node_c_id" \
      --token "$offline_token" --receipt-dir "$receipts" -- \
      "$self_dir/self_test.sh" __offline_callback "$network_id" "$node_c_id" \
      "$node_a_id" "$node_c_ip" >/dev/null
    restored_ip=$("$docker_bin" inspect --format "{{(index .NetworkSettings.Networks \"$network\").IPAddress}}" "$node_c_id")
    [ "$restored_ip" = "$node_c_ip" ] || die 'offline restore changed the endpoint IPv4 address'
    container_http "$node_a_id" "$node_c_ip" || die 'offline endpoint did not recover'
    jq -e --arg ipv4 "$node_c_ip" '.action == "docker-node-disconnect" and
      .external_action_applied == true and .public_observation_bound == false and
      .restored == true and .restored_ipv4 == $ipv4' \
      "$receipts/$offline_token-action.json" >/dev/null || die 'offline receipt is invalid'

    restart_token="restart-$suffix"
    "$self_dir/docker_network.sh" restart --container "$node_c_id" \
      --token "$restart_token" --receipt-dir "$receipts" -- \
      "$self_dir/self_test.sh" __restart_callback "$node_a_id" "$node_c_ip" >/dev/null
    jq -e '.action == "docker-node-kill-restart" and .signal == "KILL" and
      .exit_code_after_kill == 137 and .external_action_applied == true and
      .public_observation_bound == false and .restored == true and
      .started_at_before != .started_at_after' \
      "$receipts/$restart_token-action.json" >/dev/null || die 'restart receipt is invalid'

    socket_path=/workspace/service.sock
    "$docker_bin" exec --detach --user 10001:10001 "$node_a_id" python3 -c '
import os, socket, sys
path = sys.argv[1]
try:
    os.unlink(path)
except FileNotFoundError:
    pass
listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
listener.bind(path)
listener.listen(2)
for _ in range(2):
    connection, _ = listener.accept()
    try:
        connection.recv(4096)
        try:
            connection.sendall(b"opaque-response")
        except OSError:
            pass
    finally:
        connection.close()
listener.close()
' "$socket_path" >/dev/null
    wait_for_socket "$node_a_id" "$socket_path" || die 'Unix response server did not become ready'

    response_token="response-$suffix"
    "$self_dir/response_loss.sh" --container "$node_a_id" --socket "$socket_path" \
      --token "$response_token" --receipt-dir "$receipts" --timeout 10 \
      --expect-exit 42 -- python3 -c '
import socket, sys
connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
connection.connect(sys.argv[1])
connection.sendall(b"opaque-request")
try:
    response = connection.recv(4096)
except OSError:
    sys.exit(42)
sys.exit(42 if response == b"" else 41)
' "$socket_path" >/dev/null
    jq -e '.outcome == "response_dropped_after_first_byte" and
      .first_response_byte_observed == true and .response_bytes_forwarded == 0 and
      .request_bytes_forwarded > 0' "$receipts/$response_token-proxy.json" >/dev/null ||
        die 'response-loss proxy receipt is invalid'
    jq -e '.action == "unix-response-loss" and .external_action_applied == true and
      .public_observation_bound == false and .response_first_byte_observed == true and
      .response_bytes_forwarded_to_client == 0 and .socket_restored == true' \
      "$receipts/$response_token-action.json" >/dev/null || die 'response-loss action is invalid'
    "$docker_bin" exec --user 10001:10001 "$node_a_id" python3 -c '
import socket, sys
connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
connection.connect(sys.argv[1])
connection.sendall(b"opaque-request-after-restore")
response = connection.recv(4096)
sys.exit(0 if response == b"opaque-response" else 1)
' "$socket_path" || die 'restored Unix server did not return its opaque response'

    slow_socket=/workspace/slow.sock
    "$docker_bin" exec --detach --user 10001:10001 "$node_a_id" python3 -c '
import os, socket, sys, time
path = sys.argv[1]
try:
    os.unlink(path)
except FileNotFoundError:
    pass
listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
listener.bind(path)
listener.listen(2)
for index in range(2):
    connection, _ = listener.accept()
    try:
        connection.recv(4096)
        if index == 0:
            time.sleep(7)
        else:
            connection.sendall(b"restored")
    except OSError:
        pass
    finally:
        connection.close()
listener.close()
' "$slow_socket" >/dev/null
    wait_for_socket "$node_a_id" "$slow_socket" || die 'slow Unix server did not become ready'
    failure_token="response-cleanup-$suffix"
    set +e
    "$self_dir/response_loss.sh" --container "$node_a_id" --socket "$slow_socket" \
      --token "$failure_token" --receipt-dir "$receipts" --timeout 5 \
      --expect-exit 42 -- python3 -c '
import socket, sys
connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
connection.connect(sys.argv[1])
connection.sendall(b"timeout-request")
try:
    connection.recv(4096)
except OSError:
    pass
sys.exit(42)
' "$slow_socket" >"$private/response-cleanup.stdout" 2>"$private/response-cleanup.stderr"
    failure_exit=$?
    set -e
    [ "$failure_exit" -ne 0 ] || die 'response timeout unexpectedly produced a success receipt'
    "$docker_bin" exec --user 10001:10001 "$node_a_id" test -S "$slow_socket" ||
        die 'response timeout did not restore the original Unix socket'
    "$docker_bin" exec --user 10001:10001 "$node_a_id" python3 -c '
import socket, sys
connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
connection.connect(sys.argv[1])
connection.sendall(b"after-timeout")
response = connection.recv(4096)
sys.exit(0 if response == b"restored" else 1)
' "$slow_socket" || die 'response timeout cleanup did not restore service'
    [ ! -e "$receipts/$failure_token-action.json" ] &&
      [ ! -e "$receipts/$failure_token-proxy.json" ] ||
        die 'failed response loss published a success receipt'

    printf '%s\n' 'R5 fault-plane Docker self-tests passed'
}

case "${1:-}" in
    --unit)
        [ "$#" -eq 1 ] || usage
        run_unit
        ;;
    --docker)
        [ "$#" -eq 1 ] || usage
        run_docker
        ;;
    --all)
        [ "$#" -eq 1 ] || usage
        run_unit
        run_docker
        ;;
    *) usage ;;
esac
