#!/usr/bin/env bash

set -euo pipefail

RUNNER_DIR=$(cd "$(dirname "$0")" && pwd -P)
# shellcheck source=static_lib.sh
source "$RUNNER_DIR/static_lib.sh"
trap r7_static_cleanup EXIT INT TERM

r7_static_candidate_copy
candidate="$R7_STATIC_TMP/harness"
rm -rf -- "$candidate/testdata/r7/examples" "$candidate/testdata/r7/cases" \
  "$candidate/testdata/r7/domain-ops" "$candidate/test/r7/domainops"
r7_static_core_tests "$candidate"

printf 'r7 static oracle passed: Core is pattern-free after deleting examples and cases\n'
