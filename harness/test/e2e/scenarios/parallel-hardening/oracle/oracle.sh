#!/bin/sh
set -eu

evidence_root=${R5_ORACLE_ROOT:-/evidence}
case_dir=${R5_CASE_DIR:-"$evidence_root/case"}
result_dir=${R5_RESULT_DIR:-"$evidence_root/result"}

fail() {
  printf 'parallel-hardening oracle: %s\n' "$*" >&2
  exit 1
}

for command in go jq grep cp mktemp; do
  command -v "$command" >/dev/null 2>&1 || fail "required command is unavailable: $command"
done
export GOCACHE=${GOCACHE:-/tmp/r5-go-cache}
export GOTMPDIR=${GOTMPDIR:-/tmp/r5-go-tmp}
mkdir -p "$GOCACHE" "$GOTMPDIR" || fail "writable Go scratch space is unavailable"
[ -d "$case_dir" ] && [ -d "$result_dir" ] || fail "exported case or result directory is missing"
[ -f "$case_dir/upload.go" ] || fail "upload source is missing"
if find "$case_dir" "$result_dir" -type l -print | grep -q .; then
  fail "exported task evidence contains a symlink"
fi
if grep -R -E 'io\.ReadAll\s*\(' "$case_dir" >/dev/null 2>&1; then
  fail "upload still buffers the entire body"
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM
cp -R "$case_dir"/. "$work"/
cat >"$work/upload_hidden_test.go" <<'EOF'
package upload

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

type boundedReader struct{ remaining int }

func (reader *boundedReader) Read(buffer []byte) (int, error) {
	if len(buffer) > 64*1024 {
		return 0, errors.New("copy buffer exceeds 64 KiB")
	}
	if reader.remaining == 0 {
		return 0, io.EOF
	}
	count := len(buffer)
	if count > reader.remaining {
		count = reader.remaining
	}
	for index := 0; index < count; index++ {
		buffer[index] = byte(index)
	}
	reader.remaining -= count
	return count, nil
}

func TestHiddenBoundedStreamingAndPathSafety(t *testing.T) {
	root := t.TempDir()
	path, err := Save(root, "large.bin", &boundedReader{remaining: 4 << 20})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Size() != 4<<20 {
		t.Fatalf("large upload: info=%v err=%v", info, err)
	}
	outside := filepath.Join(filepath.Dir(root), "escaped.bin")
	if _, err := Save(root, "../escaped.bin", bytes.NewReader([]byte("escape"))); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("traversal returned %v, want ErrInvalidName", err)
	}
	if _, err := os.Stat(outside); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("outside path exists or stat failed unexpectedly: %v", err)
	}
	if _, err := Save(root, filepath.Join(root, "absolute.bin"), bytes.NewReader(nil)); !errors.Is(err, ErrInvalidName) {
		t.Fatalf("absolute path returned %v, want ErrInvalidName", err)
	}
}
EOF
(cd "$work" && go test -race ./...) || fail "malicious path, resource, or end-to-end upload test failed"

report="$result_dir/hardening-report.json"
[ -f "$report" ] || fail "hardening report is missing"
jq -e '
  .status == "pass" and .consumer == "pass" and .security == "pass" and
  .performance == "pass" and .deployment == "pass" and
  (.max_buffer_bytes | type == "number" and . > 0 and . <= 65536)
' "$report" >/dev/null || fail "hardening report lacks independent passing judgments or resource bound"
if grep -R -E 'mnch1_|OPENAI_API_KEY|sk-[A-Za-z0-9_-]{12,}' "$case_dir" "$result_dir" >/dev/null 2>&1; then
  fail "public evidence contains credential-shaped content"
fi
printf '%s\n' 'parallel-hardening oracle: pass'
