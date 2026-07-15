#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HARNESS_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$HARNESS_ROOT/.." && pwd)"
ALLOWLIST="$SCRIPT_DIR/test_pair_allowlist.txt"
failures=0

fail() {
  printf 'test-pair: %s\n' "$*" >&2
  failures=$((failures + 1))
}

is_allowlisted() {
  grep -Fqx "$1" "$ALLOWLIST"
}

build_constraints() {
  sed -n -e '/^\/\/go:build /p' -e '/^\/\/ +build /p' -e '/^package /q' "$1"
}

if [[ ! -f "$ALLOWLIST" ]]; then
  printf 'test-pair: missing allowlist: %s\n' "$ALLOWLIST" >&2
  exit 1
fi

duplicates="$(sed -e '/^[[:space:]]*#/d' -e '/^[[:space:]]*$/d' "$ALLOWLIST" | sort | uniq -d)"
if [[ -n "$duplicates" ]]; then
  fail "duplicate allowlist entries: $duplicates"
fi

while IFS= read -r entry; do
  [[ -z "$entry" || "$entry" == \#* ]] && continue
  file="$REPO_ROOT/$entry"
  if [[ "$entry" != harness/* || ! -f "$file" ]]; then
    fail "invalid or missing allowlist entry: $entry"
    continue
  fi
  if [[ "$entry" == *_test.go ]]; then
    fail "test files cannot be allowlisted: $entry"
    continue
  fi
  if [[ "$(basename "$entry")" == "doc.go" ]]; then
    grep -Eq '^// Package [[:alnum:]_]+ ' "$file" || fail "doc exception lacks a package comment: $entry"
    if grep -Eq '^import([[:space:]]|\()' "$file"; then
      fail "doc exception imports code: $entry"
    fi
    continue
  fi
  if ! sed -n '1,10p' "$file" | grep -Eq '^// Code generated .* DO NOT EDIT\.$'; then
    fail "exception is neither pure doc.go nor generated: $entry"
  fi
done < "$ALLOWLIST"

while IFS= read -r -d '' file; do
  rel="${file#"$REPO_ROOT/"}"
  if [[ "$file" == *_test.go ]]; then
    production="${file%_test.go}.go"
    if [[ ! -f "$production" ]]; then
      fail "$rel has no same-basename production file"
      continue
    fi
    if [[ "$(build_constraints "$file")" != "$(build_constraints "$production")" ]]; then
      fail "$rel and ${production#"$REPO_ROOT/"} have different build constraints"
    fi
    continue
  fi

  if is_allowlisted "$rel"; then
    continue
  fi
  test_file="${file%.go}_test.go"
  if [[ ! -f "$test_file" ]]; then
    fail "$rel has no same-basename test file"
    continue
  fi
  if [[ "$(build_constraints "$file")" != "$(build_constraints "$test_file")" ]]; then
    fail "$rel and ${test_file#"$REPO_ROOT/"} have different build constraints"
  fi
done < <(find "$HARNESS_ROOT" \
  \( -path "$HARNESS_ROOT/test" -o -path '*/testdata' \) -prune -o \
  -type f -name '*.go' -print0)

if (( failures > 0 )); then
  printf 'test-pair: %d violation(s)\n' "$failures" >&2
  exit 1
fi

printf 'test-pair: all handwritten Go files are paired\n'
