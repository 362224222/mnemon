#!/bin/sh
set -eu

die() {
    printf 'r5-response-loss: %s\n' "$*" >&2
    exit 1
}

usage() {
    printf '%s\n' \
      'usage: response_loss.sh --container CONTAINER --socket PATH --token TOKEN' \
      '       --receipt-dir DIRECTORY [--workdir PATH] [--timeout SECONDS]' \
      '       [--expect-exit nonzero|CODE] -- COMMAND [ARG...]' >&2
    exit 2
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command is unavailable: $1"
}

container=
socket=
token=
receipt_dir=
workdir=/workspace
timeout_seconds=30
expected_exit=nonzero
proxy_binary=/opt/r5/bin/r5-response-loss-proxy
docker_bin=${R5_DOCKER_BIN:-docker}
faultplane_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
schema_validator="$faultplane_dir/../runner/schema_validate.py"
schema_root="$faultplane_dir/../schemas"

while [ "$#" -gt 0 ]; do
    case "$1" in
        --container|--socket|--token|--receipt-dir|--workdir|--timeout|--expect-exit)
            [ "$#" -ge 2 ] || usage
            value=$2
            case "$1" in
                --container) container=$value ;;
                --socket) socket=$value ;;
                --token) token=$value ;;
                --receipt-dir) receipt_dir=$value ;;
                --workdir) workdir=$value ;;
                --timeout) timeout_seconds=$value ;;
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

case "$container" in
    ''|*[!A-Za-z0-9_.-]*) usage ;;
esac
[ "${#container}" -le 128 ] || usage
case "$token" in
    [A-Za-z0-9]* ) ;;
    * ) usage ;;
esac
case "$token" in
    *[!A-Za-z0-9._-]* ) usage ;;
esac
[ "${#token}" -le 80 ] || usage
case "$socket" in
    /*) ;;
    *) usage ;;
esac
case "$socket" in
    *[!A-Za-z0-9._/-]*|*'//'|*/../*|*/..|*/./*) usage ;;
esac
[ "${#socket}" -le 100 ] || usage
case "$workdir" in
    /*) ;;
    *) usage ;;
esac
case "$workdir" in
    *[!A-Za-z0-9._/-]*|*'//'|*/../*|*/..|*/./*) usage ;;
esac
case "$timeout_seconds" in
    ''|*[!0-9]*) usage ;;
esac
[ "$timeout_seconds" -ge 1 ] && [ "$timeout_seconds" -le 300 ] || usage
case "$expected_exit" in
    nonzero) ;;
    ''|*[!0-9]*) usage ;;
    *) [ "$expected_exit" -le 255 ] || usage ;;
esac
[ -d "$receipt_dir" ] && [ ! -L "$receipt_dir" ] || usage
[ "$(stat -c '%a' "$receipt_dir")" = 700 ] || die 'receipt directory must have mode 0700'

require_command "$docker_bin"
require_command jq
require_command sha256sum
require_command stat
require_command python3

container_id=$("$docker_bin" inspect --format '{{.Id}}' "$container" 2>/dev/null) ||
    die 'container is unavailable'
case "$container_id" in
    *[!a-f0-9]*) die 'Docker returned a non-canonical container identity' ;;
esac
[ "${#container_id}" -eq 64 ] || die 'Docker returned a non-canonical container identity'
[ "$("$docker_bin" inspect --format '{{.State.Running}}' "$container_id")" = true ] ||
    die 'container is not running'

proxy_output="$receipt_dir/$token-proxy.json"
action_output="$receipt_dir/$token-action.json"
[ ! -e "$proxy_output" ] && [ ! -e "$action_output" ] ||
    die 'fault receipt output already exists'

private=$(mktemp -d)
chmod 0700 "$private"
command_stdout="$private/command.stdout"
command_stderr="$private/command.stderr"
short=$(printf '%s\n' "$token:$container_id:$socket" | sha256sum | awk '{print substr($1,1,16)}')
socket_dir=$(dirname -- "$socket")
upstream="$socket_dir/.r5-$short.sock"
[ "${#upstream}" -le 100 ] || die 'socket directory leaves no bounded relocation path'
started_container="/tmp/r5-$short-started.json"
ready_container="/tmp/r5-$short-ready.json"
receipt_container="/tmp/r5-$short-receipt.json"
socket_moved=false
cleanup_failed=false

container_exec() {
    "$docker_bin" exec --user 10001:10001 "$container_id" "$@"
}

restore_socket() {
    if [ "$socket_moved" != true ]; then
        if container_exec test -S "$upstream" >/dev/null 2>&1; then
            socket_moved=true
        else
            return 0
        fi
    fi
    restore_ok=true
    proxy_identified=false
    # The proxy fsyncs its PID-bound start marker before listen(2). Waiting for
    # that earlier marker closes the detached-exec window in which restoring
    # the daemon socket could race a not-yet-ready proxy bind.
    started_wait_deadline=$(( $(date +%s) + 3 ))
    while ! container_exec test -s "$started_container" >/dev/null 2>&1 &&
      ! container_exec test -S "$socket" >/dev/null 2>&1 &&
      [ "$(date +%s)" -lt "$started_wait_deadline" ]; do
        sleep 0.05
    done
    if container_exec test -s "$started_container" >/dev/null 2>&1; then
        pid=$(container_exec cat "$started_container" 2>/dev/null | jq -er \
          --arg token "$token" 'select(.schema_version == 1 and .token == $token and
            .status == "started" and (.pid | type == "number" and . > 1) and
            (.started_at | type == "string" and length > 0)) | .pid' 2>/dev/null || true)
        if [ -z "$pid" ]; then
            printf 'r5-response-loss: proxy start marker is invalid\n' >&2
            restore_ok=false
        else
            proxy_identified=true
        fi
        if [ -n "$pid" ] && container_exec test -d "/proc/$pid" >/dev/null 2>&1; then
            executable=$(container_exec readlink "/proc/$pid/exe" 2>/dev/null || true)
            if [ "$executable" = "$proxy_binary" ]; then
                if ! container_exec kill -TERM "$pid" >/dev/null 2>&1; then
                    container_exec test ! -d "/proc/$pid" >/dev/null 2>&1 || restore_ok=false
                fi
                wait_deadline=$(( $(date +%s) + 3 ))
                while container_exec test -d "/proc/$pid" >/dev/null 2>&1 &&
                  [ "$(date +%s)" -lt "$wait_deadline" ]; do
                    sleep 0.05
                done
                container_exec test ! -d "/proc/$pid" >/dev/null 2>&1 || restore_ok=false
            else
                printf 'r5-response-loss: refusing to signal unexpected PID %s\n' "$pid" >&2
                restore_ok=false
            fi
        fi
    fi
    if container_exec test -e "$socket" >/dev/null 2>&1; then
        if [ "$proxy_identified" = true ] &&
          container_exec test -S "$socket" >/dev/null 2>&1; then
            container_exec unlink "$socket" >/dev/null 2>&1 || restore_ok=false
        else
            printf 'r5-response-loss: refusing to replace an unidentified listen path\n' >&2
            restore_ok=false
        fi
    fi
    if [ "$restore_ok" = true ] && container_exec test -S "$upstream" >/dev/null 2>&1; then
        container_exec mv -- "$upstream" "$socket" >/dev/null 2>&1 || restore_ok=false
    else
        restore_ok=false
    fi
    container_exec test -S "$socket" >/dev/null 2>&1 || restore_ok=false
    container_exec test ! -e "$upstream" >/dev/null 2>&1 || restore_ok=false
    if [ "$restore_ok" = true ]; then
        container_exec unlink "$started_container" >/dev/null 2>&1 || true
        container_exec unlink "$ready_container" >/dev/null 2>&1 || true
        container_exec unlink "$receipt_container" >/dev/null 2>&1 || true
        socket_moved=false
        return 0
    fi
    return 1
}

cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    if ! restore_socket; then
        cleanup_failed=true
        printf 'r5-response-loss: Unix socket restoration failed\n' >&2
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

container_exec test -x "$proxy_binary" || die 'response-loss proxy is absent from the candidate image'
container_exec test -S "$socket" || die 'target is not a live Unix socket'
container_exec test ! -e "$upstream" || die 'relocated upstream socket path already exists'
container_exec test ! -e "$started_container" || die 'proxy start path already exists'
container_exec test ! -e "$ready_container" || die 'proxy ready path already exists'
container_exec test ! -e "$receipt_container" || die 'proxy receipt path already exists'
container_exec mv -- "$socket" "$upstream"
socket_moved=true

"$docker_bin" exec --detach --user 10001:10001 "$container_id" "$proxy_binary" \
  --listen "$socket" --upstream "$upstream" --started "$started_container" \
  --ready "$ready_container" \
  --receipt "$receipt_container" --token "$token" --timeout "${timeout_seconds}s" >/dev/null

started_deadline=$(( $(date +%s) + 10 ))
while ! container_exec test -s "$started_container" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$started_deadline" ] || die 'proxy did not publish its start marker within 10 seconds'
    sleep 0.05
done
container_exec cat "$started_container" | jq -e --arg token "$token" '
  (keys | sort) == (["schema_version","token","status","pid","started_at"] | sort) and
  .schema_version == 1 and .token == $token and .status == "started" and
  (.pid | type == "number" and . > 1) and
  (.started_at | type == "string" and length > 0)
' >/dev/null || die 'proxy start marker is invalid'

ready_deadline=$(( $(date +%s) + 10 ))
while ! container_exec test -s "$ready_container" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$ready_deadline" ] || die 'proxy did not become ready within 10 seconds'
    sleep 0.05
done
container_exec cat "$ready_container" | jq -e --arg token "$token" '
  .schema_version == 1 and .token == $token and .status == "ready" and
  (.pid | type == "number" and . > 1) and (.ready_at | type == "string" and length > 0)
' >/dev/null || die 'proxy readiness receipt is invalid'

set +e
"$docker_bin" exec --user 10001:10001 --workdir "$workdir" "$container_id" "$@" \
  >"$command_stdout" 2>"$command_stderr"
client_exit=$?
set -e

receipt_deadline=$(( $(date +%s) + timeout_seconds + 2 ))
while ! container_exec test -s "$receipt_container" >/dev/null 2>&1; do
    [ "$(date +%s)" -lt "$receipt_deadline" ] || die 'proxy produced no successful loss receipt'
    sleep 0.05
done
container_exec test -f "$receipt_container" && container_exec test ! -L "$receipt_container" ||
    die 'proxy receipt is not a regular non-symlink file'
"$docker_bin" cp "$container_id:$receipt_container" "$proxy_output" >/dev/null
chmod 0600 "$proxy_output"
PYTHONDONTWRITEBYTECODE=1 python3 "$schema_validator" \
  "$schema_root/fault-proxy-receipt.schema.json" "$proxy_output" ||
    die 'proxy loss receipt failed its closed schema'
jq -e --arg token "$token" '
  (keys | sort) == (["schema_version","token","outcome","started_at","accepted_at",
    "first_response_byte_at","finished_at","request_bytes_forwarded",
    "response_bytes_forwarded","first_response_byte_observed"] | sort) and
  .schema_version == 1 and .token == $token and
  .outcome == "response_dropped_after_first_byte" and
  .first_response_byte_observed == true and .response_bytes_forwarded == 0 and
  (.request_bytes_forwarded | type == "number" and . > 0) and
  all([.started_at,.accepted_at,.first_response_byte_at,.finished_at][];
    type == "string" and length > 0)
' "$proxy_output" >/dev/null || die 'proxy loss receipt is invalid'

case "$expected_exit" in
    nonzero) [ "$client_exit" -ne 0 ] || die 'client reported success after its response was dropped' ;;
    *) [ "$client_exit" -eq "$expected_exit" ] || die "client exit $client_exit differs from expected $expected_exit" ;;
esac

if ! restore_socket; then
    cleanup_failed=true
    die 'Unix socket restoration failed after response loss'
fi
stdout_bytes=$(stat -c '%s' "$command_stdout")
stderr_bytes=$(stat -c '%s' "$command_stderr")
generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
jq -n --arg token "$token" --arg generated_at "$generated_at" \
  --arg proxy_receipt "$(basename "$proxy_output")" --argjson client_exit "$client_exit" \
  --argjson stdout_bytes "$stdout_bytes" --argjson stderr_bytes "$stderr_bytes" '
  {schema_version:1,token:$token,action:"unix-response-loss",
   external_action_applied:true,public_observation_bound:false,
   client_exit_code:$client_exit,client_stdout_bytes:$stdout_bytes,
   client_stderr_bytes:$stderr_bytes,response_first_byte_observed:true,
   response_bytes_forwarded_to_client:0,socket_restored:true,
   proxy_receipt:$proxy_receipt,generated_at:$generated_at}
' >"$private/action.json"
install -m 0600 "$private/action.json" "$action_output"
PYTHONDONTWRITEBYTECODE=1 python3 "$schema_validator" \
  "$schema_root/fault-action.schema.json" "$action_output" ||
    die 'response-loss action failed its closed schema'

printf '%s\n' "$action_output"
