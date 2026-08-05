#!/usr/bin/env bash

set -euo pipefail

RUNNER_DIR=$(cd "$(dirname "$0")" && pwd -P)
# shellcheck source=static_lib.sh
source "$RUNNER_DIR/static_lib.sh"
trap r7_static_cleanup EXIT INT TERM

r7_static_repository_copy
candidate="$R7_STATIC_TMP/repository"
test -d "$candidate/harness/internal/selector" || r7_static_fail 'selector is already absent'
rm -rf -- "$candidate/harness/internal/selector"
(
  cd "$candidate/harness"
  go test -count=1 ./...
  go build ./cmd/mnemon-harness ./cmd/mnemond
)

printf 'r7 static oracle passed: deleting selector leaves all R7 Go conformance operational\n'
