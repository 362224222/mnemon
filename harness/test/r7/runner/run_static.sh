#!/usr/bin/env bash

set -euo pipefail

RUNNER_DIR=$(cd "$(dirname "$0")" && pwd -P)

"$RUNNER_DIR/run_no_case_kind.sh"
"$RUNNER_DIR/run_no_managed_wake.sh"
"$RUNNER_DIR/run_case_data_only.sh"
"$RUNNER_DIR/run_pattern_free.sh"
"$RUNNER_DIR/run_without_selector.sh"

printf 'r7 static oracles passed\n'
