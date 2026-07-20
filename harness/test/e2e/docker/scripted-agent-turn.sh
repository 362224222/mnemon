#!/bin/sh
set -eu

mode=${1:-}
test "$mode" = --user || test "$mode" = --managed
policy=.r5/policy.json
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
    if [ -n "$artifact" ]; then
        printf '%s\n' "$content" | mnemon-harness teamwork "$action" \
          --context "$context" --content-file - --artifact "$artifact" --json >"$receipt"
    else
        printf '%s\n' "$content" | mnemon-harness teamwork "$action" \
          --context "$context" --content-file - --json >"$receipt"
    fi
    jq -e '.status == "accepted"' "$receipt" >/dev/null
}

if [ "$mode" = --user ]; then
    mnemon-harness agent current --json >"$current"
    test "$(jq -r '.status' "$current")" = none
    channel=$(jq -er '.entry_channel' "$policy")
    participant=$(jq -er '.entry_to' "$policy")
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
    derive_to=$(jq -er '.derive_to' "$policy")
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
    derive_to=$(jq -er '.derive_to' "$policy")
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
