#!/bin/sh
set -eu

mode=${1:-}
test "$mode" = --user || test "$mode" = --managed
policy=.r5/policy.json
topology=.r5/topology.json
test -f "$policy"
install -d -m 0700 .r5/runtime result
umask 077

node=$(jq -er '.node' "$policy")
stamp=$(date +%s%N)
receipt=".r5/runtime/${stamp}-${node}.json"
current=".r5/runtime/${stamp}-${node}-current.json"

has_action() {
    jq -e --arg action "$1" '.allowed_actions | index($action) != null' "$current" >/dev/null
}

submit_context_action() {
    action=$1
    context=$2
    content=$3
    artifact=${4:-}
    destination=${5:-$receipt}
    if [ -n "$artifact" ]; then
        if printf '%s\n' "$content" | mnemon-harness teamwork "$action" \
          --context "$context" --content-file - --artifact "$artifact" --json >"$destination"; then
            status=0
        else
            status=$?
        fi
    else
        if printf '%s\n' "$content" | mnemon-harness teamwork "$action" \
          --context "$context" --content-file - --json >"$destination"; then
            status=0
        else
            status=$?
        fi
    fi
    [ "$status" -eq 0 ] || return "$status"
    [ "$destination" != /dev/full ] || return 0
    jq -e '.status == "accepted"' "$destination" >/dev/null
}

scenario_name() {
    if [ -n "${R5_SCENARIO:-}" ]; then
        printf '%s\n' "$R5_SCENARIO"
        return 0
    fi
    if [ -f .r5/scenario ]; then
        sed -n '1p' .r5/scenario
        return 0
    fi
    printf '%s\n' ''
}

submit_context_action_with_receipt_loss() {
    action=$1
    context=$2
    content=$3
    artifact=${4:-}
    phase=${5:-}
    fault_record=".r5/runtime/${stamp}-${node}-receipt-loss.json"
    first_stderr=".r5/runtime/${stamp}-${node}-receipt-loss-first.stderr"

    set +e
    submit_context_action "$action" "$context" "$content" "$artifact" /dev/full 2>"$first_stderr"
    first_exit=$?
    submit_context_action "$action" "$context" "$content" "$artifact" "$receipt"
    retry_exit=$?
    set -e

    test "$first_exit" -ne 0
    test "$retry_exit" -eq 0
    jq -e '.status == "accepted" and .replayed == true' "$receipt" >/dev/null
    jq -n --arg fault review-receipt-loss --arg node "$node" \
      --arg phase "$phase" --arg action "teamwork.$action" \
      --argjson first_exit "$first_exit" --argjson retry_exit "$retry_exit" \
      --slurpfile retry "$receipt" '
      {schema_version:1,fault:$fault,node:$node,phase:$phase,action:$action,
       first_stdout_sink:"/dev/full",first_exit_code:$first_exit,
       retry_exit_code:$retry_exit,retry:$retry[0],
       observation:{
         retry_returned_terminal_receipt:
           ($retry[0].status == "accepted" and $retry[0].replayed == true),
         operation_id:$retry[0].operation_id,
         receipt:$retry[0].receipt,
         event_ids:[$retry[0].results[]?.event_id],
         event_types:[$retry[0].results[]?.event_type]
       }}
    ' >"$fault_record"
}

resolve_participant() {
    channel=$1
    selector=$2
    case "$selector" in
        "" | auto | team)
            printf '%s\n' "$selector"
            return 0
            ;;
    esac
    if [ -f "$topology" ]; then
        resolved=$(
          jq -er --arg channel "$channel" --arg node "$selector" '
            first(.channels[] | select(.alias == $channel) | .members[] |
              select(.node == $node)) | .alias
          ' "$topology" 2>/dev/null
        ) || resolved=
        if [ -n "$resolved" ]; then
            printf '%s\n' "$resolved"
            return 0
        fi
    fi
    printf '%s\n' "$selector"
}

if [ "$mode" = --user ]; then
    mnemon-harness agent current --json >"$current"
    test "$(jq -r '.status' "$current")" = none
    channel=$(jq -er '.entry_channel' "$policy")
    participant=$(resolve_participant "$channel" "$(jq -er '.entry_to' "$policy")")
    content=$(sed -n '1,200p')
    test -n "$content"
    set -- mnemon-harness teamwork offer --channel "$channel" --to "$participant" \
      --deadline 24h --content-file - --json
    if [ -d case ]; then
        set -- "$@" --artifact case
    fi
    printf '%s\n' "$content" | "$@" >"$receipt"
    jq -e '.status == "accepted"' "$receipt" >/dev/null
    exit 0
fi

mnemon-harness agent current --json >"$current"
test "$(jq -r '.status' "$current")" = actionable
context=$(jq -er '.context_file' "$current")

if jq -e '.decline_once == true' "$policy" >/dev/null 2>&1 &&
   [ ! -e .r5/declined-once ] && has_action teamwork.decline; then
    : >.r5/declined-once
    submit_context_action decline "$context" "Independent review declined once by deterministic scenario policy."
    exit 0
fi

if has_action teamwork.accept; then
    submit_context_action accept "$context" "${node} accepted the bounded independent review."
    exit 0
fi

derive_channel=$(jq -r '.derive_channel // empty' "$policy")
if [ -n "$derive_channel" ] && [ ! -e .r5/derived ] && has_action teamwork.offer; then
    derive_to=$(resolve_participant "$derive_channel" "$(jq -er '.derive_to' "$policy")")
    content=$(jq -er '.result_content' "$policy")
    set -- mnemon-harness teamwork offer --context "$context" --channel "$derive_channel" \
      --to "$derive_to" --deadline 24h --content-file - --json
    if [ -d case ]; then
        set -- "$@" --artifact case
    fi
    printf '%s\n' "$content" | "$@" >"$receipt"
    jq -e '.status == "accepted"' "$receipt" >/dev/null
    : >.r5/derived
    exit 0
fi

if [ -n "$derive_channel" ] && [ -e .r5/derived ] && [ ! -e .r5/derived-retry ] &&
   jq -e '.source_event.event_type == "review.declined"' "$current" >/dev/null 2>&1 &&
   has_action teamwork.offer; then
    derive_to=$(resolve_participant "$derive_channel" "$(jq -er '.derive_to' "$policy")")
    content=$(jq -er '.result_content' "$policy")
    printf '%s\n' "$content" | mnemon-harness teamwork offer --context "$context" \
      --channel "$derive_channel" --to "$derive_to" --deadline 24h --content-file - --json \
      >"$receipt"
    jq -e '.status == "accepted"' "$receipt" >/dev/null
    : >.r5/derived-retry
    exit 0
fi

if jq -e '.rework_once == true' "$policy" >/dev/null 2>&1 &&
   [ ! -e .r5/reworked-once ] && has_action teamwork.rework; then
    : >.r5/reworked-once
    submit_context_action rework "$context" "Apply the independent findings and return one corrected revision."
    exit 0
fi

if has_action teamwork.deliver; then
    content=$(jq -er '.result_content' "$policy")
    printf '%s\n' "$content" >"result/${node}.txt"
    if [ "$(scenario_name)" = payment-review ] && [ "$node" = C ] &&
       [ ! -e .r5/review-receipt-loss ] &&
       jq -e '.source_event.event_type == "review.closed" and
         .action_work.local_role == "reviewer"' "$current" >/dev/null; then
        : >.r5/review-receipt-loss
        submit_context_action_with_receipt_loss deliver "$context" "$content" result c-requests-rework
        exit 0
    fi
    submit_context_action deliver "$context" "$content" result
    exit 0
fi

if has_action teamwork.close; then
    if [ "$node" = A ] && [ -x .r5/task-apply ]; then
        .r5/task-apply "$current"
    fi
    submit_context_action close "$context" "Independent findings applied and verified."
    exit 0
fi

if has_action agent.resolve.retry; then
    mnemon-harness agent resolve retry --context "$context" --content-file - --json \
      >"$receipt" <<'EOF'
Retry after deterministic transient scenario branch.
EOF
    jq -e '.status == "resolved"' "$receipt" >/dev/null
    exit 0
fi

printf '%s\n' 'scripted policy had no allowed public action' >&2
exit 1
