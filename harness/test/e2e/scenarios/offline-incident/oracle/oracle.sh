#!/bin/sh
set -eu

evidence_root=${R5_ORACLE_ROOT:-/evidence}
case_dir=${R5_CASE_DIR:-"$evidence_root/case"}
result_dir=${R5_RESULT_DIR:-"$evidence_root/result"}

fail() {
  printf 'offline-incident oracle: %s\n' "$*" >&2
  exit 1
}

for command in jq grep base64 gzip mktemp; do
  command -v "$command" >/dev/null 2>&1 || fail "required command is unavailable: $command"
done
[ -d "$case_dir" ] && [ -d "$result_dir" ] || fail "exported case or result directory is missing"
[ -x "$case_dir/replay.sh" ] || fail "replay command is missing or not executable"
[ -f "$case_dir/incident.log.gz.b64" ] && [ -f "$case_dir/trace.json" ] || fail "incident Artifact fixture is incomplete"
if find "$case_dir" "$result_dir" -type l -print | grep -q .; then
  fail "exported task evidence contains a symlink"
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM
base64 -d "$case_dir/incident.log.gz.b64" >"$work/incident.log.gz" || fail "compressed log base64 is invalid"
gzip -t "$work/incident.log.gz" || fail "compressed log is corrupt"
cat >"$work/hidden-events.ndjson" <<'EOF'
{"kind":"charge","request":"req-17","amount":1250,"attempt":1}
{"kind":"charge","request":"req-17","amount":1250,"attempt":2}
{"kind":"charge","request":"req-18","amount":900,"attempt":1}
{"kind":"charge","request":"req-17","amount":1250,"attempt":3}
EOF
first=$("$case_dir/replay.sh" "$work/hidden-events.ndjson") || fail "regression replay failed"
second=$("$case_dir/replay.sh" "$work/hidden-events.ndjson") || fail "second regression replay failed"
expected='req-17 1250
req-18 900'
[ "$first" = "$expected" ] || fail "duplicate attempts produced duplicate semantic effects"
[ "$second" = "$first" ] || fail "regression replay is not deterministic"

report="$result_dir/incident-report.json"
[ -f "$report" ] || fail "incident report is missing"
jq -e '
  .status == "verified" and
  (.root_cause | type == "string" and length > 8) and
  (.remediation | type == "string" and length > 8) and
  .regression_replay == "pass" and
  .consumer_review == "pass" and
  .security_review == "pass" and
  .recovery_review == "pass"
' "$report" >/dev/null || fail "incident report lacks deterministic task evidence"
if grep -R -E 'mnch1_|OPENAI_API_KEY|sk-[A-Za-z0-9_-]{12,}' "$case_dir" "$result_dir" >/dev/null 2>&1; then
  fail "public evidence contains credential-shaped content"
fi
printf '%s\n' 'offline-incident oracle: pass'
