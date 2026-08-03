#!/usr/bin/env bash

set -euo pipefail

runner_dir=$(cd "$(dirname "$0")" && pwd -P)
harness_root=$(cd "$runner_dir/../../../../" && pwd -P)
image=${R8_NETWORK_IMAGE:-mnemon-r8-network:$$}
keep=${R8_NETWORK_KEEP:-0}
prefix="mnr8-network-$$"
network="$prefix-net"
nodes='peer-a peer-b peer-c peer-d peer-e'
runtime=$(mktemp -d)

fail() {
  printf 'r8 network: %s\n' "$*" >&2
  return 1
}

container() {
  printf '%s-%s\n' "$prefix" "$1"
}

cleanup() {
  if test "$keep" = 1; then
    printf 'r8 network retained: %s\n' "$prefix" >&2
    return
  fi
  for node in $nodes; do
    docker rm -f "$(container "$node")" >/dev/null 2>&1 || true
  done
  docker network rm "$network" >/dev/null 2>&1 || true
  docker image rm "$image" >/dev/null 2>&1 || true
  rm -f "$runtime"/*.json "$runtime"/*.txt
  rmdir "$runtime" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

require_tools() {
  command -v docker >/dev/null 2>&1 || fail 'docker is required'
  command -v jq >/dev/null 2>&1 || fail 'jq is required'
  docker info >/dev/null 2>&1 || fail 'Docker Engine is unavailable'
}

wait_ready() {
  local node=$1 attempt=0
  while test "$attempt" -lt 100; do
    if docker exec "$(container "$node")" r8-peer control \
      --socket /workspace/r8/control.sock status >"$runtime/$node-status.json" 2>/dev/null; then
      return 0
    fi
    attempt=$((attempt + 1))
    sleep 0.1
  done
  fail "$node did not expose the R8 control socket"
}

require_tools
docker build --quiet -f "$harness_root/test/r8/network/docker/Dockerfile" \
  -t "$image" "$harness_root" >/dev/null
image_id=$(docker image inspect --format '{{.Id}}' "$image")
binary_digests=$(docker run --rm --entrypoint sha256sum "$image" \
  /usr/local/bin/mnemon-harness /usr/local/bin/mnemond /usr/local/bin/r8-peer)
test -n "$image_id" && test -n "$binary_digests" || fail 'candidate image identity is unavailable'

docker network create "$network" >/dev/null
for node in $nodes; do
  docker run -d --name "$(container "$node")" --hostname "$node" --network "$network" \
    --label mnemon.r8.network="$prefix" "$image" >/dev/null
  test "$(docker inspect --format '{{.Image}}' "$(container "$node")")" = "$image_id" || \
    fail "$node does not run the candidate image"
  test "$(docker inspect --format '{{len .Mounts}}' "$(container "$node")")" = 0 || \
    fail "$node unexpectedly shares a mounted filesystem"
  test "$(docker exec "$(container "$node")" sha256sum /usr/local/bin/mnemon-harness \
    /usr/local/bin/mnemond /usr/local/bin/r8-peer)" = "$binary_digests" || \
    fail "$node does not run the candidate binaries"
  docker exec "$(container "$node")" r8-peer keygen --state-dir /workspace/r8 \
    >"$runtime/$node-key.json"
done

window=$(docker run --rm --entrypoint r8-peer "$image" window)
peers='[]'
for node in $nodes; do
  participant_id=$(jq -er '.participant_id' "$runtime/$node-key.json")
  public_key=$(jq -er '.public_key' "$runtime/$node-key.json")
  peers=$(printf '%s' "$peers" | jq -c --arg id "$participant_id" --arg address "$node:8448" \
    --arg key "$public_key" '. + [{id:$id,address:$address,public_key:$key}]')
done
peers=$(printf '%s' "$peers" | jq -c 'sort_by(.id)')
config=$(jq -cn --arg created "$(printf '%s' "$window" | jq -er .created_at)" \
  --arg expires "$(printf '%s' "$window" | jq -er .expires_at)" --argjson peers "$peers" \
  '{version:1,
    question_digest:"sha256:1c318e6bd54978a4e40e57a7c974be6b0c10a6450e8f73ddb25c346966c0cfd0",
    candidate_a_digest:"sha256:24187cf22679d81aa9089c595650d6af7c731829c73d45797e43632022b157cd",
    candidate_b_digest:"sha256:4e6e7b6d9b4aecb247c59e8e12e664d4557f773a0235b78ec894eae4946a7487",
    created_at:$created,expires_at:$expires,
    profile:{sample_size:1,alpha:1,threshold:1,max_rounds:2,round_timeout_ms:2000},
    peers:$peers}')

selection_id=
for node in $nodes; do
  participant_id=$(jq -er '.participant_id' "$runtime/$node-key.json")
  printf '%s' "$config" | docker exec -i "$(container "$node")" \
    r8-peer install-config --state-dir /workspace/r8
  docker exec "$(container "$node")" r8-peer init --state-dir /workspace/r8 \
    --project-root /workspace --config /workspace/r8/config.json --id "$participant_id" --preference A \
    >"$runtime/$node-init.json"
  current_selection=$(jq -er '.selection_id' "$runtime/$node-init.json")
  if test -z "$selection_id"; then
    selection_id=$current_selection
  fi
  test "$current_selection" = "$selection_id" || fail 'nodes derived different SelectionIDs'
  docker exec "$(container "$node")" test -f /workspace/.mnemon/harness/node/agency.db || \
    fail "$node did not persist the R7 authority seeded through admission"
done

# A restart crosses the real container/process boundary. The entrypoint starts
# one mnemond on the already-provisioned R7 node and one removable R8 adapter on
# that container's private selector.db.
for node in $nodes; do
  docker restart "$(container "$node")" >/dev/null
done
identities=
for node in $nodes; do
  wait_ready "$node"
  docker exec -w /workspace "$(container "$node")" mnemon-harness hook attach --json >/dev/null
  docker exec -w /workspace "$(container "$node")" mnemon-harness agent current --json \
    >"$runtime/$node-view.json"
  opinion_digest=$(jq -er '.seed_opinion_digest' "$runtime/$node-init.json")
  jq -e --arg descriptor "$selection_id" --arg opinion "$opinion_digest" \
    '.current.semantic.kind == "selection.seed" and
    ((.current.facts.artifacts | length) == 2) and
    ([.current.facts.artifacts[].digest] | sort) == ([$descriptor,$opinion] | sort)' \
    "$runtime/$node-view.json" >/dev/null || fail "$node mnemond did not read the accepted R7 seed Event"
  identity=$(docker exec "$(container "$node")" sha256sum \
    /workspace/.mnemon/harness/node/peer-identity.json | awk '{print $1}')
  printf '%s\n' "$identities" | grep -Fx "$identity" >/dev/null && \
    fail 'two isolated R7 nodes exposed the same durable identity'
  identities=$(printf '%s\n%s' "$identities" "$identity")
  printf '%s\n' "$identity" >"$runtime/$node-r7-identity.txt"
  participant_id=$(jq -er '.participant_id' "$runtime/$node-key.json")
  test "$(jq -er '.self' "$runtime/$node-status.json")" = "$participant_id" || \
    fail "$node opened another peer's selector state"
  docker exec "$(container "$node")" sh -c \
    'test "$(ps -o comm | grep -xc mnemond)" -eq 1' || fail "$node does not run exactly one mnemond"
done

docker exec "$(container peer-a)" r8-peer control --socket /workspace/r8/control.sock round \
  >"$runtime/observation.json"
jq -e '.phase == "observed" and .round == 1 and
  .observation.result == "threshold_reached" and .observation.preference == "A"' \
  "$runtime/observation.json" >/dev/null || fail 'real signed sample did not produce the bounded observation'

peer_a_id=$(jq -er '.participant_id' "$runtime/peer-a-key.json")
peer_b_id=$(jq -er '.participant_id' "$runtime/peer-b-key.json")
docker exec "$(container peer-a)" r8-peer probe --state-dir /workspace/r8 \
  --config /workspace/r8/config.json --id "$peer_a_id" --target "$peer_b_id" --mode no-vote \
  >"$runtime/no-vote.json"
jq -e '.authenticated == true and .no_vote == true and .http_status == 200' \
  "$runtime/no-vote.json" >/dev/null || fail 'unknown SelectionID did not return authenticated no-vote'

docker exec "$(container peer-a)" r8-peer probe --state-dir /workspace/r8 \
  --config /workspace/r8/config.json --id "$peer_a_id" --target "$peer_b_id" --mode identity-mismatch \
  >"$runtime/identity-mismatch.json"
jq -e '.authenticated == false and .http_status == 401' "$runtime/identity-mismatch.json" \
  >/dev/null || fail 'claimed source bypassed independent signature identity'

before=$(jq -c '.observation' "$runtime/observation.json")
before_identity=$(tr -d '\n' <"$runtime/peer-a-r7-identity.txt")
docker restart "$(container peer-a)" >/dev/null
wait_ready peer-a
after=$(jq -c '.observation' "$runtime/peer-a-status.json")
test "$after" = "$before" || fail 'PreferenceObservation changed across container restart'
docker exec -w /workspace "$(container peer-a)" mnemon-harness hook attach --json >/dev/null
docker exec -w /workspace "$(container peer-a)" mnemon-harness agent current --json \
  >"$runtime/peer-a-restarted-view.json"
after_identity=$(docker exec "$(container peer-a)" sha256sum \
  /workspace/.mnemon/harness/node/peer-identity.json | awk '{print $1}')
test "$after_identity" = "$before_identity" || fail 'mnemond did not retain the same durable R7 identity'
jq -e '.current.semantic.kind == "selection.seed"' "$runtime/peer-a-restarted-view.json" >/dev/null || \
  fail 'mnemond did not retain the accepted seed responsibility after restart'
docker exec "$(container peer-a)" sh -c \
  'test "$(ps -o comm | grep -xc mnemond)" -eq 1' || \
  fail 'peer-a did not retain exactly one mnemond after the second restart'
peer_a_opinion=$(jq -er '.seed_opinion_digest' "$runtime/peer-a-init.json")
jq -e --arg descriptor "$selection_id" --arg opinion "$peer_a_opinion" \
  '((.current.facts.artifacts | length) == 2) and
  ([.current.facts.artifacts[].digest] | sort) == ([$descriptor,$opinion] | sort)' \
  "$runtime/peer-a-restarted-view.json" >/dev/null || \
  fail 'mnemond did not retain both accepted seed Artifacts after restart'

printf 'r8 network proof passed: image=%s selection=%s nodes=5 k=1 observation=%s\n' \
  "$image_id" "$selection_id" "$(jq -c '.observation' "$runtime/observation.json")"
