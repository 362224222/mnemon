#!/usr/bin/env bash

set -euo pipefail

runtime_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
runner_dir=$(cd "$runtime_dir/../../domainops" && pwd -P)
harness_root=$(cd "$runtime_dir/../../../.." && pwd -P)
image="mnemon-pi-delegate-oracle:$$"

cleanup() {
  docker image rm "$image" >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM

command -v docker >/dev/null 2>&1 || {
  printf 'pi delegate oracle: docker is required\n' >&2
  exit 1
}
docker info >/dev/null 2>&1 || {
  printf 'pi delegate oracle: Docker Engine is unavailable\n' >&2
  exit 1
}

docker build --quiet --target agent -f "$runner_dir/Dockerfile" \
  -t "$image" "$harness_root" >/dev/null
smoke=$(printf '%s\n' '{"id":"state","type":"get_state"}' |
  docker run --rm -i --entrypoint pi "$image" \
    --mode rpc --no-session --no-extensions \
    -e /opt/mnemon/pi-delegate/delegate.ts \
    --no-skills --no-prompt-templates --no-themes --no-context-files \
    --no-tools --no-approve)
printf '%s\n' "$smoke" | grep -Eq \
  '"id":"state".*"command":"get_state".*"success":true' || {
  printf 'pi delegate oracle: Pi rejected the extension surface\n' >&2
  exit 1
}
docker run --rm --entrypoint node \
  --mount "type=bind,src=$runtime_dir,dst=/delegate-test,readonly" \
  "$image" --experimental-strip-types /delegate-test/delegate.test.mjs

printf 'pi delegate Runtime oracle: PASS\n'
