#!/bin/sh
set -eu

evidence_root=${R5_ORACLE_ROOT:-/evidence}
case_dir=${R5_CASE_DIR:-"$evidence_root/case"}
result_dir=${R5_RESULT_DIR:-"$evidence_root/result"}

fail() {
  printf 'overlapping-channels oracle: %s\n' "$*" >&2
  exit 1
}

for command in jq grep; do
  command -v "$command" >/dev/null 2>&1 || fail "required command is unavailable: $command"
done
[ -d "$case_dir" ] && [ -d "$result_dir" ] || fail "exported case or result directory is missing"
[ -x "$case_dir/check-release.sh" ] || fail "public release check is missing or not executable"
if find "$case_dir" "$result_dir" -type l -print | grep -q .; then
  fail "exported task evidence contains a symlink"
fi
(cd "$case_dir" && ./check-release.sh) || fail "public release candidate check failed"

bundle="$result_dir/release-bundle.json"
[ -f "$bundle" ] && [ -s "$result_dir/verification.md" ] || fail "final release evidence is incomplete"
jq -e '
  .status == "ready" and
  .api == "pass" and .consumer == "pass" and .dependency == "pass" and
  .security == "pass" and .deployment == "pass" and
  .reviewers == ["B", "C", "D", "E", "F"] and
  .causality == ["A:alpha:C", "C:beta:E", "E:gamma:F", "E:beta:C", "C:alpha:A"]
' "$bundle" >/dev/null || fail "release bundle cannot reconstruct all independent judgments and explicit causality"
if grep -R -E 'mnch1_|OPENAI_API_KEY|sk-[A-Za-z0-9_-]{12,}' "$case_dir" "$result_dir" >/dev/null 2>&1; then
  fail "public evidence contains credential-shaped content"
fi
printf '%s\n' 'overlapping-channels oracle: pass'
