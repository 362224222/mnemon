#!/usr/bin/env bash

set -euo pipefail

RUNNER_DIR=$(cd "$(dirname "$0")" && pwd -P)
# shellcheck source=static_lib.sh
source "$RUNNER_DIR/static_lib.sh"
trap r7_static_cleanup EXIT INT TERM

r7_static_candidate_copy
candidate="$R7_STATIC_TMP/harness"
test -d "$candidate/internal/selector" || r7_static_fail 'selector is already absent'
rm -rf -- "$candidate/internal/selector"
r7_static_core_tests "$candidate"

printf 'r7 static oracle passed: deleting selector leaves R7 Core operational\n'
