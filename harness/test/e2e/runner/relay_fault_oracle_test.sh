#!/bin/sh
set -eu

runner_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
private=$(mktemp -d)
chmod 0700 "$private"
cleanup() {
    status=$?
    trap - EXIT HUP INT TERM
    find "$private" -type f -delete 2>/dev/null || true
    find "$private" -depth -type d -empty -delete 2>/dev/null || true
    exit "$status"
}
trap cleanup EXIT HUP INT TERM

event_key='{"origin_peer_id":"peer-a","origin_epoch":"epoch-a","event_id":"event-entry"}'
digest="sha256:$(printf '%064d' 0)"
event_digest="sha256:$(printf '%064d' 1)"
jq -n --arg digest "$digest" --arg event_digest "$event_digest" \
  --argjson event_key "$event_key" '
  {publications:[
    {channel:"alpha",observer_node:"B",observer_peer_id:"peer-b",origin_node:"A",
     origin_peer_id:"peer-a",immediate_transport_node:"A",
     arrival:"gossip",publication_ref:{channel_sequence:1},audience_nodes:["C"],
     audience_peer_ids:["peer-c"],ignored_nodes:["B"],semantic_outcome:"ignored",
     publication_digest:$digest,event_digest:$event_digest,
     event_key:$event_key},
    {channel:"alpha",observer_node:"C",observer_peer_id:"peer-c",origin_node:"A",
     origin_peer_id:"peer-a",immediate_transport_node:"B",
     immediate_transport_peer_id:"peer-b",
     arrival:"gossip",publication_ref:{channel_sequence:1},audience_nodes:["C"],
     audience_peer_ids:["peer-c"],ignored_nodes:[],semantic_outcome:"accepted",
     publication_digest:$digest,event_digest:$event_digest,
     event_key:$event_key}
  ]}
' >"$private/network.json"
jq -cn --arg event_digest "$event_digest" --argjson event_key "$event_key" '
  {source:"public-cli",evidence_ref:"runtime/C/first-current.json",document:{
    status:"actionable",
    action_work:{ref:{home_peer_id:"peer-a",work_id:"work-entry"}},
    source_event:{event_key:$event_key,event_digest:$event_digest}}}
' >"$private/handling.ndjson"
jq -n --arg digest "$digest" --argjson event_key "$event_key" '
  {channels:[{alias:"alpha",members:[{binding:"self",peer_id:"peer-b"}],publications:[
    {observer_peer_id:"peer-b",origin_peer_id:"peer-a",
     immediate_transport_peer_id:"peer-a",arrival:"gossip",
     publication_ref:{channel_sequence:1},audience_peer_ids:["peer-c"],
     ignored_peer_ids:["peer-b"],semantic_outcome:"ignored",
     publication_digest:$digest,event_key:$event_key}
  ]}]}
' >"$private/scope-b.json"
jq -n --arg digest "$digest" --argjson event_key "$event_key" '
  {channels:[{alias:"alpha",members:[{binding:"self",peer_id:"peer-c"}],publications:[
    {observer_peer_id:"peer-c",origin_peer_id:"peer-a",
     immediate_transport_peer_id:"peer-b",arrival:"gossip",
     publication_ref:{channel_sequence:1},audience_peer_ids:["peer-c"],
     ignored_peer_ids:[],semantic_outcome:"waiting_artifact",
     publication_digest:$digest,event_key:$event_key}
  ]}]}
' >"$private/scope-c.json"

"$runner_dir/relay_fault_oracle.sh" "$private/network.json" "$private/handling.ndjson" \
  "$private/scope-b.json" "$private/scope-c.json"

cp "$private/handling.ndjson" "$private/repeated-observation.ndjson"
jq -c '.evidence_ref = "runtime/C/second-current.json" |
  .document.context_file = "/workspace/private-second-projection.context"' \
  "$private/handling.ndjson" >>"$private/repeated-observation.ndjson"
"$runner_dir/relay_fault_oracle.sh" "$private/network.json" \
  "$private/repeated-observation.ndjson" "$private/scope-b.json" "$private/scope-c.json"

jq '(.publications[] | select(.observer_node == "C") | .immediate_transport_node) = "A"' \
  "$private/network.json" >"$private/wrong-transport.json"
if "$runner_dir/relay_fault_oracle.sh" "$private/wrong-transport.json" \
  "$private/handling.ndjson" "$private/scope-b.json" "$private/scope-c.json" \
  >/dev/null 2>&1; then
    printf '%s\n' 'relay fault oracle accepted a direct A-to-C arrival' >&2
    exit 1
fi
cp "$private/handling.ndjson" "$private/duplicate-effect.ndjson"
jq -c '.evidence_ref = "runtime/C/duplicate-current.json" |
  .document.action_work.ref.work_id = "work-duplicate"' \
  "$private/handling.ndjson" >>"$private/duplicate-effect.ndjson"
if "$runner_dir/relay_fault_oracle.sh" "$private/network.json" \
  "$private/duplicate-effect.ndjson" "$private/scope-b.json" "$private/scope-c.json" \
  >/dev/null 2>&1; then
    printf '%s\n' 'relay fault oracle accepted a duplicate semantic handling' >&2
    exit 1
fi
jq '(.channels[0].publications[0].immediate_transport_peer_id) = "peer-a"' \
  "$private/scope-c.json" >"$private/wrong-scope.json"
if "$runner_dir/relay_fault_oracle.sh" "$private/network.json" \
  "$private/handling.ndjson" "$private/scope-b.json" "$private/wrong-scope.json" \
  >/dev/null 2>&1; then
    printf '%s\n' 'relay fault oracle accepted an unbound scope observation' >&2
    exit 1
fi
jq '(.channels[0].members[0].peer_id) = "peer-b"' \
  "$private/scope-c.json" >"$private/wrong-observer.json"
if "$runner_dir/relay_fault_oracle.sh" "$private/network.json" \
  "$private/handling.ndjson" "$private/scope-b.json" "$private/wrong-observer.json" \
  >/dev/null 2>&1; then
    printf '%s\n' 'relay fault oracle accepted a scope from the wrong observer' >&2
    exit 1
fi
