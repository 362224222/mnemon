#!/bin/sh
set -eu

[ "$#" -eq 4 ] || {
    printf '%s\n' \
      'usage: relay_fault_oracle.sh NETWORK_PATHS HANDLING_TRACE SCOPE_B SCOPE_C' >&2
    exit 2
}
network_paths=$1
handling_trace=$2
scope_b=$3
scope_c=$4
[ -f "$network_paths" ] && [ ! -L "$network_paths" ] || exit 2
[ -f "$handling_trace" ] && [ ! -L "$handling_trace" ] || exit 2
[ -f "$scope_b" ] && [ ! -L "$scope_b" ] || exit 2
[ -f "$scope_c" ] && [ ! -L "$scope_c" ] || exit 2

jq -e '
  ([.publications[] | select(
    .channel == "alpha" and .observer_node == "B" and .origin_node == "A" and
    .immediate_transport_node == "A" and .arrival == "gossip" and
    .publication_ref.channel_sequence == 1 and .audience_nodes == ["C"] and
    .ignored_nodes == ["B"] and .semantic_outcome == "ignored")]) as $at_b |
  ([.publications[] | select(
    .channel == "alpha" and .observer_node == "C" and .origin_node == "A" and
    .immediate_transport_node == "B" and .arrival == "gossip" and
    .publication_ref.channel_sequence == 1 and .audience_nodes == ["C"] and
    .ignored_nodes == [] and .semantic_outcome == "accepted")]) as $at_c |
  ($at_b | length) == 1 and ($at_c | length) == 1 and
  $at_b[0].publication_digest == $at_c[0].publication_digest and
  $at_b[0].event_key == $at_c[0].event_key and
  ([.publications[] | select(
    .observer_node == "C" and .event_key == $at_c[0].event_key)] | length) == 1
' "$network_paths" >/dev/null

event=$(jq -c '
  first(.publications[] | select(
    .channel == "alpha" and .observer_node == "C" and .origin_node == "A" and
    .immediate_transport_node == "B" and .publication_ref.channel_sequence == 1 and
    .semantic_outcome == "accepted")) as $at_c |
  first(.publications[] | select(
    .channel == "alpha" and .observer_node == "B" and
    .event_key == $at_c.event_key and .semantic_outcome == "ignored")) as $at_b |
  {event_key:$at_c.event_key,publication_digest:$at_c.publication_digest,
   event_digest:$at_c.event_digest,
   origin_peer_id:$at_c.origin_peer_id,
   immediate_transport_peer_id:$at_c.immediate_transport_peer_id,
   audience_peer_ids:$at_c.audience_peer_ids,
   b_peer_id:$at_b.observer_peer_id,c_peer_id:$at_c.observer_peer_id}
' "$network_paths")
jq -e --argjson event "$event" --slurpfile cdoc "$scope_c" '
  ([.channels[] | select(.alias == "alpha")]) as $bchannels |
  ([$cdoc[0].channels[] | select(.alias == "alpha")]) as $cchannels |
  ($bchannels[0].members | map(select(.binding == "self"))) as $bself |
  ($cchannels[0].members | map(select(.binding == "self"))) as $cself |
  ($bchannels[0].publications | map(select(
    .event_key == $event.event_key and
    .publication_digest == $event.publication_digest and
    .origin_peer_id == $event.origin_peer_id and
    .immediate_transport_peer_id == $event.origin_peer_id and
    .arrival == "gossip" and .publication_ref.channel_sequence == 1 and
    .audience_peer_ids == $event.audience_peer_ids and
    .ignored_peer_ids == [$event.b_peer_id] and
    .semantic_outcome == "ignored"))) as $at_b |
  ($cchannels[0].publications | map(select(
    .event_key == $event.event_key and
    .publication_digest == $event.publication_digest and
    .origin_peer_id == $event.origin_peer_id and
    .immediate_transport_peer_id == $event.immediate_transport_peer_id and
    .arrival == "gossip" and .publication_ref.channel_sequence == 1 and
    .audience_peer_ids == $event.audience_peer_ids and
    .ignored_peer_ids == [] and
    (.semantic_outcome | IN("stored","waiting_artifact","ready","processing","accepted"))))) as $at_c |
  ($bchannels | length) == 1 and ($cchannels | length) == 1 and
  ($bself | length) == 1 and $bself[0].peer_id == $event.b_peer_id and
  ($cself | length) == 1 and $cself[0].peer_id == $event.c_peer_id and
  ($at_b | length) == 1 and ($at_c | length) == 1
' "$scope_b" >/dev/null

jq -s -e --argjson event "$event" '
  [.[] | select(
    .source == "public-cli" and .document.status == "actionable" and
    .document.source_event.event_key == $event.event_key)] as $effects |
  ($effects | length) > 0 and
  all($effects[];
    .document.source_event.event_digest == $event.event_digest and
    (.document.action_work.ref.home_peer_id | type) == "string" and
    (.document.action_work.ref.home_peer_id | length) > 0 and
    (.document.action_work.ref.work_id | type) == "string" and
    (.document.action_work.ref.work_id | length) > 0) and
  ([$effects[] | {
    event_key:.document.source_event.event_key,
    event_digest:.document.source_event.event_digest,
    work_ref:.document.action_work.ref
  }] | unique | length) == 1
' "$handling_trace" >/dev/null
