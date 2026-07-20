#!/bin/sh
set -eu

evidence_root=${R5_ORACLE_ROOT:-/evidence}
case_dir=${R5_CASE_DIR:-"$evidence_root/case"}
result_dir=${R5_RESULT_DIR:-"$evidence_root/result"}

fail() {
  printf 'api-sdk-contract oracle: %s\n' "$*" >&2
  exit 1
}

for command in go jq grep cp mktemp node; do
  command -v "$command" >/dev/null 2>&1 || fail "required command is unavailable: $command"
done
export GOCACHE=${GOCACHE:-/tmp/r5-go-cache}
export GOTMPDIR=${GOTMPDIR:-/tmp/r5-go-tmp}
mkdir -p "$GOCACHE" "$GOTMPDIR" || fail "writable Go scratch space is unavailable"
[ -d "$case_dir" ] && [ -d "$result_dir" ] || fail "exported case or result directory is missing"
[ -f "$case_dir/pagination.go" ] && [ -f "$case_dir/openapi.json" ] || fail "API fixture is incomplete"
if find "$case_dir" "$result_dir" -type l -print | grep -q .; then
  fail "exported task evidence contains a symlink"
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM
cp -R "$case_dir"/. "$work"/
cat >"$work/pagination_hidden_test.go" <<'EOF'
package pagination

import (
	"errors"
	"testing"
)

func TestHiddenCursorAuthentication(t *testing.T) {
	token, err := EncodeCursor(Cursor{Offset: 80}, []byte("hidden-signing-key-a"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeCursor(token, []byte("hidden-signing-key-b")); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("wrong key returned %v, want ErrInvalidCursor", err)
	}
	bytes := []byte(token)
	if len(bytes) < 3 {
		t.Fatalf("signed token is implausibly short: %q", token)
	}
	index := len(bytes) / 2
	if bytes[index] == 'A' {
		bytes[index] = 'B'
	} else {
		bytes[index] = 'A'
	}
	if _, err := DecodeCursor(string(bytes), []byte("hidden-signing-key-a")); !errors.Is(err, ErrInvalidCursor) {
		t.Fatalf("tampered token returned %v, want ErrInvalidCursor", err)
	}
}
EOF
(cd "$work" && go test -race ./...) || fail "Go or hidden cursor-authentication tests failed"

jq -e '
  .openapi == "3.1.0" and
  (.paths["/items"].get.parameters | any(.name == "cursor")) and
  (.paths["/items"].get.responses["400"] != null) and
  (.components.schemas.Cursor != null) and
  (.components.schemas.Problem != null)
' "$case_dir/openapi.json" >/dev/null || fail "OpenAPI cursor or problem contract is incomplete"
node - "$case_dir/openapi.json" <<'EOF' || fail "TypeScript-style consumer contract failed"
const fs = require("node:fs");
const assert = require("node:assert/strict");
const schema = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
const cursor = schema.components?.schemas?.Cursor;
const problem = schema.components?.schemas?.Problem;
assert.equal(cursor?.type, "string", "consumer sees the cursor as an opaque string");
assert.ok(problem?.properties?.code, "consumer needs a stable machine-readable problem code");
assert.ok(problem?.properties?.message, "consumer needs a stable problem message");
assert.ok(schema.paths?.["/items"]?.get?.responses?.["400"], "invalid cursor response is documented");
EOF

summary="$result_dir/review-summary.json"
[ -s "$result_dir/release-notes.md" ] && [ -f "$summary" ] || fail "release evidence is incomplete"
jq -e '
  .status == "verified" and .consumer == "pass" and .security == "pass" and
  .documentation == "pass" and .compatibility == "pass"
' "$summary" >/dev/null || fail "independent review summary is incomplete"
if grep -R -E 'mnch1_|OPENAI_API_KEY|sk-[A-Za-z0-9_-]{12,}' "$case_dir" "$result_dir" >/dev/null 2>&1; then
  fail "public evidence contains credential-shaped content"
fi
printf '%s\n' 'api-sdk-contract oracle: pass'
