#!/usr/bin/env bash

set -euo pipefail

RUNNER_DIR=$(cd "$(dirname "$0")" && pwd -P)
# shellcheck source=static_lib.sh
source "$RUNNER_DIR/static_lib.sh"

pattern=$(r7_static_forbidden_case_pattern)
sources=$(r7_static_production_sources "$R7_STATIC_HARNESS_ROOT")
test -n "$sources" || r7_static_fail 'R7 production source set is empty'

if (
  cd "$R7_STATIC_HARNESS_ROOT"
  # The closed gate forbids case-specific semantic kind literals. Ordinary
  # prose is not a dispatch table, so this scans the literal namespaces rather
  # than banning English words from comments and diagnostics.
  r7_static_search_files_i "$pattern" "$sources" | grep -q .
); then
  r7_static_fail 'case-specific semantic kind appears in production R7 Go'
fi

printf 'r7 static oracle passed: no production case-specific kind\n'
