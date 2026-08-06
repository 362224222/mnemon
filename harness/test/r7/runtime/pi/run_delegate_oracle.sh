#!/usr/bin/env bash

set -euo pipefail

runtime_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
runner_dir=$(cd "$runtime_dir/../../domainops" && pwd -P)
harness_root=$(cd "$runtime_dir/../../../.." && pwd -P)
attention_extension="$harness_root/internal/attach/assets/pi/mnemond.ts"
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
attention_smoke=$(printf '%s\n' '{"id":"state","type":"get_state"}' |
  docker run --rm -i --entrypoint pi \
    --mount "type=bind,src=$attention_extension,dst=/attention-test/mnemond.ts,readonly" \
    "$image" \
    --mode rpc --no-session --no-extensions \
    -e /attention-test/mnemond.ts \
    --no-skills --no-prompt-templates --no-themes --no-context-files \
    --no-tools --no-approve 2>/dev/null || true)
printf '%s\n' "$attention_smoke" | grep -Eq \
  '"id":"state".*"command":"get_state".*"success":true' || {
  printf 'pi attention oracle: Pi rejected the attachment extension surface\n' >&2
  exit 1
}
combined_smoke=$(printf '%s\n' '{"id":"state","type":"get_state"}' |
  docker run --rm -i --entrypoint pi \
    --mount "type=bind,src=$attention_extension,dst=/attention-test/mnemond.ts,readonly" \
    "$image" \
    --mode rpc --no-session --no-extensions \
    -e /opt/mnemon/pi-delegate/delegate.ts -e /attention-test/mnemond.ts \
    --no-skills --no-prompt-templates --no-themes --no-context-files \
    --tools bash,delegate,mnemond_submit --no-approve 2>/dev/null || true)
printf '%s\n' "$combined_smoke" | grep -Eq \
  '"id":"state".*"command":"get_state".*"success":true' || {
  printf 'pi Runtime oracle: Pi rejected the combined extension load surface\n' >&2
  exit 1
}
docker run --rm --entrypoint node \
  --mount "type=bind,src=$runtime_dir,dst=/delegate-test,readonly" \
  "$image" --experimental-strip-types /delegate-test/delegate.test.mjs
docker run --rm --entrypoint node \
  --env MNEMON_PI_EXTENSION=/attention-test/mnemond.ts \
  --mount "type=bind,src=$runtime_dir,dst=/delegate-test,readonly" \
  --mount "type=bind,src=$attention_extension,dst=/attention-test/mnemond.ts,readonly" \
  "$image" --experimental-strip-types /delegate-test/attention-budget.test.mjs

printf 'pi Runtime oracle: PASS\n'
