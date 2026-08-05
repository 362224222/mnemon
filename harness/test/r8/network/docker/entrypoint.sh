#!/bin/sh

set -eu

state=/workspace/r8
config=$state/config.json
control=$state/control.sock
participant_id=$state/participant.id

if test -f "$config" && test -f "$state/selector.db" && test -f "$participant_id"; then
  peer_id=$(tr -d '\n' <"$participant_id")
  test -n "$peer_id"
  mnemon-harness setup --runtime pi --project-root /workspace >/dev/null
  test "$(ps -o comm | grep -xc mnemond)" -eq 1
  exec r8-peer serve --state-dir "$state" --config "$config" --id "$peer_id" \
    --listen 0.0.0.0:8448 --control "$control"
fi

trap 'exit 0' TERM INT
while :; do
  sleep 3600 &
  wait $!
done
