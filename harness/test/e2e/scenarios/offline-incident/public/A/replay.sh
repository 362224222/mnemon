#!/bin/sh
set -eu

if [ "$#" -ne 1 ] || [ ! -f "$1" ]; then
  echo "usage: replay.sh EVENTS_NDJSON" >&2
  exit 2
fi

jq -r 'select(.kind == "charge") | [.request, (.amount | tostring)] | @tsv' "$1" |
while IFS="$(printf '\t')" read -r request amount; do
  printf '%s %s\n' "$request" "$amount"
done
