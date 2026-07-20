#!/bin/sh
set -eu

actual=$(./replay.sh events.ndjson)
expected='req-17 1250
req-18 900'
if [ "$actual" != "$expected" ]; then
  echo "unexpected replay output" >&2
  exit 1
fi
