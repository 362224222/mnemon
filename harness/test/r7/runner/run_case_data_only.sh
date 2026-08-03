#!/usr/bin/env bash

set -euo pipefail

RUNNER_DIR=$(cd "$(dirname "$0")" && pwd -P)
# shellcheck source=static_lib.sh
source "$RUNNER_DIR/static_lib.sh"

cases_root="$R7_STATIC_HARNESS_ROOT/testdata/r7/cases"
examples_root="$R7_STATIC_HARNESS_ROOT/testdata/r7/examples"
pattern=$(r7_static_forbidden_case_pattern)

for name in review contract-net blackboard; do
  directory="$cases_root/$name"
  test -d "$directory" || r7_static_fail "case directory is missing: $name"
  test -s "$directory/nodes.txt" || r7_static_fail "$name/nodes.txt is absent or empty"
  test -s "$directory/playbook.md" || r7_static_fail "$name/playbook.md is absent or empty"
  test -x "$directory/oracle.sh" || r7_static_fail "$name/oracle.sh is not executable"
done

if find "$examples_root" -type f -perm -111 -print | grep -q .; then
  r7_static_fail 'an R7 example is executable'
fi
if rg -n -i "$pattern" "$examples_root"; then
  r7_static_fail 'an R7 example contains case behavior'
fi
if rg -n -i 'testdata/r7/examples|examples/' "$RUNNER_DIR/lib.sh" "$RUNNER_DIR/run_cases.sh"; then
  r7_static_fail 'the case runner reads non-authoritative examples'
fi
if rg -n -i '(review|contract-net|blackboard)' "$RUNNER_DIR/lib.sh" "$RUNNER_DIR/run_cases.sh"; then
  r7_static_fail 'the generic case runner contains case-specific behavior'
fi

while IFS= read -r executable; do
  case "$executable" in
    "$cases_root"/*) ;;
    *) r7_static_fail "executable R7 fixture is outside cases/: $executable" ;;
  esac
done < <(find "$R7_STATIC_HARNESS_ROOT/testdata/r7" -type f -perm -111 -print)

"$RUNNER_DIR/run_no_case_kind.sh"
printf 'r7 static oracle passed: case behavior is data-only\n'
