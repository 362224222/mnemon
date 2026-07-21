#!/bin/sh
set -eu

die() {
    printf 'r5-relay-fault-scope: %s\n' "$*" >&2
    exit 1
}

usage() {
    printf '%s\n' \
      'usage: relay_fault_scope.sh --runtime scripted|codex --a-container ID' \
      '       --b-container ID --c-container ID --a-peer PEER --b-peer PEER' \
      '       --c-peer PEER --prompt FILE --stdout FILE --stderr FILE' \
      '       --prompt-receipt FILE --scope-b FILE --scope-c FILE' \
      '       [--prompt-timeout SECONDS] [--observation-timeout SECONDS]' >&2
    exit 2
}

runtime=
a_container=
b_container=
c_container=
a_peer=
b_peer=
c_peer=
prompt=
stdout_file=
stderr_file=
prompt_receipt=
scope_b=
scope_c=
prompt_timeout_seconds=300
observation_timeout_seconds=30
while [ "$#" -gt 0 ]; do
    case "$1" in
        --runtime|--a-container|--b-container|--c-container|--a-peer|--b-peer|--c-peer|--prompt|--stdout|--stderr|--prompt-receipt|--scope-b|--scope-c|--prompt-timeout|--observation-timeout)
            [ "$#" -ge 2 ] || usage
            value=$2
            case "$1" in
                --runtime) runtime=$value ;;
                --a-container) a_container=$value ;;
                --b-container) b_container=$value ;;
                --c-container) c_container=$value ;;
                --a-peer) a_peer=$value ;;
                --b-peer) b_peer=$value ;;
                --c-peer) c_peer=$value ;;
                --prompt) prompt=$value ;;
                --stdout) stdout_file=$value ;;
                --stderr) stderr_file=$value ;;
                --prompt-receipt) prompt_receipt=$value ;;
                --scope-b) scope_b=$value ;;
                --scope-c) scope_c=$value ;;
                --prompt-timeout) prompt_timeout_seconds=$value ;;
                --observation-timeout) observation_timeout_seconds=$value ;;
            esac
            shift 2
            ;;
        *) usage ;;
    esac
done

case "$runtime" in
    scripted|codex) ;;
    *) usage ;;
esac
for container in "$a_container" "$b_container" "$c_container"; do
    [ "${#container}" -eq 64 ] || usage
    case "$container" in
        *[!a-f0-9]*) usage ;;
    esac
done
for peer in "$a_peer" "$b_peer" "$c_peer"; do
    case "$peer" in
        ''|*[!A-Za-z0-9._:-]*) usage ;;
    esac
    [ "${#peer}" -le 256 ] || usage
done
for timeout_seconds in "$prompt_timeout_seconds" "$observation_timeout_seconds"; do
    case "$timeout_seconds" in
        ''|*[!0-9]*) usage ;;
    esac
    [ "$timeout_seconds" -ge 1 ] && [ "$timeout_seconds" -le 600 ] || usage
done
[ -f "$prompt" ] && [ ! -L "$prompt" ] || usage
for output in "$stdout_file" "$stderr_file" "$prompt_receipt" "$scope_b" "$scope_c"; do
    case "$output" in
        /*) ;;
        *) usage ;;
    esac
    [ ! -e "$output" ] || die "output already exists: $output"
    parent=$(dirname -- "$output")
    [ -d "$parent" ] && [ ! -L "$parent" ] || usage
done
command -v docker >/dev/null 2>&1 || die 'Docker is unavailable'
command -v jq >/dev/null 2>&1 || die 'jq is unavailable'

set +e
if [ "$runtime" = scripted ]; then
    docker exec --user 10001:10001 --workdir /workspace -i "$a_container" \
      timeout "${prompt_timeout_seconds}s" codex exec - <"$prompt" >"$stdout_file" 2>"$stderr_file"
else
    docker exec --user 10001:10001 --workdir /workspace -i "$a_container" \
      timeout "${prompt_timeout_seconds}s" /opt/r5/bin/live-codex-exec \
      <"$prompt" >"$stdout_file" 2>"$stderr_file"
fi
prompt_exit=$?
set -e
jq -n --argjson exit_code "$prompt_exit" '{schema_version:1,prompt_exit_code:$exit_code}' \
  >"$prompt_receipt"
chmod 0600 "$stdout_file" "$stderr_file" "$prompt_receipt"
[ "$prompt_exit" -eq 0 ] || exit 0

private=$(mktemp -d)
chmod 0700 "$private"
cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    find "$private" -type f -delete 2>/dev/null || true
    find "$private" -depth -type d -empty -delete 2>/dev/null || true
    exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

deadline=$(( $(date +%s) + observation_timeout_seconds ))
while [ "$(date +%s)" -lt "$deadline" ]; do
    if docker exec --user 10001:10001 --workdir /workspace "$b_container" \
      timeout 2s mnemon-harness channel status --json >"$private/b.json" 2>/dev/null &&
      docker exec --user 10001:10001 --workdir /workspace "$c_container" \
      timeout 2s mnemon-harness channel status --json >"$private/c.json" 2>/dev/null &&
      jq -e --arg a "$a_peer" --arg b "$b_peer" --arg c "$c_peer" \
        --slurpfile cdoc "$private/c.json" '
        def alpha_publications:
          [.channels[] | select(.alias == "alpha") | .publications[]];
        (alpha_publications |
          map(select(.origin_peer_id == $a and .immediate_transport_peer_id == $a and
            (.arrival | IN("gossip","repair")) and
            .publication_ref.channel_sequence == 1 and
            .audience_peer_ids == [$c] and .ignored_peer_ids == [$b] and
            .semantic_outcome == "ignored"))) as $from_b |
        ($cdoc[0] | alpha_publications |
          map(select(.origin_peer_id == $a and .immediate_transport_peer_id == $b and
            .arrival == "gossip" and .publication_ref.channel_sequence == 1 and
            .audience_peer_ids == [$c] and .ignored_peer_ids == [] and
            (.semantic_outcome | IN("stored","waiting_artifact","ready","processing","accepted"))))) as $from_c |
        ($from_b | length) == 1 and ($from_c | length) == 1 and
        $from_b[0].publication_digest == $from_c[0].publication_digest and
        $from_b[0].event_key == $from_c[0].event_key
      ' "$private/b.json" >/dev/null 2>&1; then
        install -m 0600 "$private/b.json" "$scope_b"
        install -m 0600 "$private/c.json" "$scope_c"
        exit 0
    fi
    sleep 0.1
done
printf '%s\n' \
  'r5-relay-fault-scope: public D4 did not expose the forced A-to-B-to-C first publication before the deadline' >&2
exit 0
