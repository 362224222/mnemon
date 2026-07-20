#!/bin/sh
set -eu

if [ "$#" -ne 1 ] || [ ! -f "$1" ]; then
  echo "usage: deployment-check.sh RELEASE_JSON" >&2
  exit 2
fi
jq -e '.dependency_lock == true and .api_version == "v2"' "$1" >/dev/null
