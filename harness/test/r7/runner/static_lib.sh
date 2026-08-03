#!/usr/bin/env bash

# Shared mechanics for R7 structural oracles. These helpers know package and
# filesystem boundaries only; collaboration case semantics stay in testdata.

set -euo pipefail

R7_STATIC_RUNNER_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
R7_STATIC_HARNESS_ROOT=$(cd "$R7_STATIC_RUNNER_DIR/../../.." && pwd -P)
R7_STATIC_REPOSITORY_ROOT=$(cd "$R7_STATIC_HARNESS_ROOT/.." && pwd -P)
R7_STATIC_TMP=

r7_static_fail() {
  printf 'r7 static oracle: %s\n' "$*" >&2
  return 1
}

r7_static_cleanup() {
  if test -n "${R7_STATIC_TMP:-}" && test -d "$R7_STATIC_TMP"; then
    rm -rf -- "$R7_STATIC_TMP"
  fi
  R7_STATIC_TMP=
}

r7_static_candidate_copy() {
  r7_static_cleanup
  R7_STATIC_TMP=$(mktemp -d "${TMPDIR:-/tmp}/mnemon-r7-static.XXXXXX")
  mkdir "$R7_STATIC_TMP/harness"
  cp -R "$R7_STATIC_HARNESS_ROOT/." "$R7_STATIC_TMP/harness"
}

# Copy the minimum repository surface needed by every Go conformance package.
# The private Git copy lets history-bound contract tests inspect the exact
# candidate without observing or mutating the caller's repository metadata.
r7_static_repository_copy() {
  local entry

  r7_static_cleanup
  R7_STATIC_TMP=$(mktemp -d "${TMPDIR:-/tmp}/mnemon-r7-static.XXXXXX")
  mkdir "$R7_STATIC_TMP/repository"
  for entry in .git .gitignore go.mod go.sum main.go cmd internal docs harness; do
    test -e "$R7_STATIC_REPOSITORY_ROOT/$entry" || \
      r7_static_fail "repository candidate input is missing: $entry"
    cp -R "$R7_STATIC_REPOSITORY_ROOT/$entry" "$R7_STATIC_TMP/repository/$entry"
  done
}

r7_static_cli_package() {
  local root=$1
  test -d "$root/internal/cli" || r7_static_fail 'R7 Agent terminal package is missing'
  printf '%s\n' './internal/cli'
}

r7_static_core_tests() {
  local root=$1 cli
  cli=$(r7_static_cli_package "$root")
  (
    cd "$root"
    go test -count=1 \
      ./internal/agency \
      ./internal/authority \
      ./internal/cas \
      ./internal/peerlink \
      ./internal/daemon \
      "$cli" \
      ./internal/attach \
      ./cmd/mnemon-harness \
      ./cmd/mnemond \
      ./test/r7/process
    go test -count=1 ./test/contracts -run '^TestR7InternalPackageSetAllowsSelectorDeletion$'
    go build ./cmd/mnemon-harness ./cmd/mnemond
  )
}

r7_static_production_sources() {
  local root=$1 cli=internal/cli
  local -a roots=(
    internal/agency
    internal/authority
    internal/cas
    internal/peerlink
    internal/daemon
    "$cli"
    internal/attach
    cmd/mnemon-harness
    cmd/mnemond
  )
  if test -d "$root/internal/selector"; then
    roots+=(internal/selector)
  fi
  (
    cd "$root"
    find "${roots[@]}" \
      -type f -name '*.go' ! -name '*_test.go' ! -path '*/testdata/*' -print
  )
}

r7_static_forbidden_case_pattern() {
  printf '%s\n' '(review\.|contract-net\.|blackboard\.|memory\.wiki\.|teamwork\.|channel\.)'
}
