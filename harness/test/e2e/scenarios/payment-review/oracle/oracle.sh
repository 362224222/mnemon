#!/bin/sh
set -eu

evidence_root=${R5_ORACLE_ROOT:-/evidence}
case_dir=${R5_CASE_DIR:-"$evidence_root/case"}
result_dir=${R5_RESULT_DIR:-"$evidence_root/result"}

fail() {
  printf 'payment-review oracle: %s\n' "$*" >&2
  exit 1
}

for command in go jq grep cp mktemp; do
  command -v "$command" >/dev/null 2>&1 || fail "required command is unavailable: $command"
done
export GOCACHE=${GOCACHE:-/tmp/r5-go-cache}
export GOTMPDIR=${GOTMPDIR:-/tmp/r5-go-tmp}
mkdir -p "$GOCACHE" "$GOTMPDIR" || fail "writable Go scratch space is unavailable"
[ -d "$case_dir" ] || fail "exported case directory is missing"
[ -d "$result_dir" ] || fail "exported result directory is missing"
[ -f "$case_dir/go.mod" ] && [ -f "$case_dir/payment.go" ] || fail "payment source is incomplete"
if find "$case_dir" "$result_dir" -type l -print | grep -q .; then
  fail "exported task evidence contains a symlink"
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM
cp -R "$case_dir"/. "$work"/
cat >"$work/payment_hidden_test.go" <<'EOF'
package payment

import (
	"sync"
	"testing"
)

func TestHiddenConcurrentDuplicateCharge(t *testing.T) {
	processor := NewProcessor()
	const workers = 64
	start := make(chan struct{})
	results := make(chan Charge, workers)
	errors := make(chan error, workers)
	var wait sync.WaitGroup
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			charge, err := processor.Charge("hidden-checkout-key", 1250)
			if err != nil {
				errors <- err
				return
			}
			results <- charge
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errors)
	for err := range errors {
		t.Fatalf("concurrent charge failed: %v", err)
	}
	var first Charge
	for charge := range results {
		if first.ID == "" {
			first = charge
		}
		if charge != first {
			t.Fatalf("duplicate semantic result: first=%+v got=%+v", first, charge)
		}
	}
	if processor.Count() != 1 {
		t.Fatalf("charge count=%d, want 1", processor.Count())
	}
}
EOF
(cd "$work" && go test -race ./...) || fail "race or hidden duplicate-charge test failed"

summary="$result_dir/review-summary.json"
[ -f "$summary" ] || fail "review summary is missing"
[ -s "$result_dir/final.diff" ] || fail "final source diff is missing"
jq -e '
  .status == "verified" and
  .consumer_review == "pass" and
  .security_review == "pass" and
  .ledger_review == "pass" and
  .rework_count == 1
' "$summary" >/dev/null || fail "review summary does not contain all independent passing judgments"

if grep -R -E 'mnch1_|OPENAI_API_KEY|sk-[A-Za-z0-9_-]{12,}' "$case_dir" "$result_dir" >/dev/null 2>&1; then
  fail "public evidence contains credential-shaped content"
fi
printf '%s\n' 'payment-review oracle: pass'
