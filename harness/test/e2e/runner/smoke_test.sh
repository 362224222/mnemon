#!/bin/sh
set -eu

. "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)/lib.sh"

for script in "$runner_dir"/*.sh "$repo_root"/harness/test/e2e/docker/*.sh \
    "$repo_root"/harness/test/e2e/faultplane/*.sh \
    "$scenario_root"/*/oracle/oracle.sh; do
    [ -x "$script" ] || die "required E2E script is not executable: $script"
    sh -n "$script"
done

PYTHONDONTWRITEBYTECODE=1 python3 -c \
  'import pathlib,sys; compile(pathlib.Path(sys.argv[1]).read_text(), sys.argv[1], "exec")' \
  "$runner_dir/schema_validate.py"

for schema in "$schema_root"/*.json; do
    PYTHONDONTWRITEBYTECODE=1 python3 "$runner_dir/schema_validate.py" --check-schema "$schema"
done
for case_name in $canonical_cases; do
    PYTHONDONTWRITEBYTECODE=1 python3 "$runner_dir/schema_validate.py" \
      "$schema_root/scenario.schema.json" "$scenario_root/$case_name/manifest.json"
    first=$(scenario_digest "$scenario_root/$case_name")
    second=$(scenario_digest "$scenario_root/$case_name")
    [ "$first" = "$second" ] || die "scenario digest is nondeterministic: $case_name"
done

"$runner_dir/run_docker.sh" --validate-only >/dev/null
LIVE_CODEX=1 "$runner_dir/run_live_codex.sh" --validate-only >/dev/null

if "$runner_dir/run_docker.sh" --validate-only --case not-a-case >/dev/null 2>&1; then
    die 'unknown CASE did not fail closed'
fi
if RUN= "$runner_dir/validate_evidence.sh" >/dev/null 2>&1; then
    die 'missing RUN did not fail closed'
fi
validate_image_reference 'registry.invalid/mnemon-r5:e2e@sha256-deadbeef'
validate_image_digest 'sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef'
validate_git_sha '0123456789abcdef0123456789abcdef01234567'
if (validate_image_reference 'mnemon-r5:e2e;touch-invalid') >/dev/null 2>&1; then
    die 'shell-active IMAGE reference did not fail closed'
fi
if (validate_image_digest 'sha256:0123456789abcdef') >/dev/null 2>&1; then
    die 'short candidate image digest did not fail closed'
fi
if (validate_git_sha '0123456789abcdef0123456789abcdef0123456Z') >/dev/null 2>&1; then
    die 'non-canonical candidate Git SHA did not fail closed'
fi

smoke_private=$(mktemp -d)
smoke_credential=$(mktemp)
trap 'rm -rf "$smoke_private"; rm -f "$smoke_credential"' EXIT HUP INT TERM
printf '%s\n' 'authorization="Bearer sk-examplecredential123"' >"$smoke_private/leak.txt"
if scan_evidence_redaction "$smoke_private" '' >/dev/null 2>&1; then
    die 'secret-shaped evidence did not fail closed'
fi
rm -f "$smoke_private/leak.txt"
printf '%s\n' '{"tokens":{"access_token":"opaque-provider-value-73491"}}' >"$smoke_credential"
chmod 0600 "$smoke_credential"
printf '%s\n' 'opaque-provider-value-73491' >"$smoke_private/leak.txt"
if scan_evidence_redaction "$smoke_private" "$smoke_credential" >/dev/null 2>&1; then
    die 'extracted provider credential value did not fail closed'
fi
rm -f "$smoke_private/leak.txt"
printf '%s\n' '{}' >"$smoke_private/target.json"
ln -s target.json "$smoke_private/link.json"
if (manifest_evidence "$smoke_private" smoke-symlink) >/dev/null 2>&1; then
    die 'evidence symlink did not fail closed'
fi
rm -f "$smoke_private/link.json" "$smoke_private/target.json"
trap - EXIT HUP INT TERM
rm -rf "$smoke_private"
rm -f "$smoke_credential"

if find "$repo_root/harness/test/e2e/docker" "$repo_root/harness/test/e2e/runner" \
  -type f \( -name '*.go' -o -name '*.db' -o -name '*.db-wal' -o -name '*.db-shm' \) \
  -print | grep -q .; then
    die 'E2E assets contain a Go/internal test entrypoint or database fixture'
fi
if grep -R --exclude=smoke_test.sh -n -E \
  'harness/internal/|mnemon-harness[[:space:]]+event|REQUEST\.json|RESOLUTION\.json' \
  "$repo_root/harness/test/e2e/docker" "$repo_root/harness/test/e2e/faultplane" \
  "$repo_root/harness/test/e2e/runner" >/dev/null; then
    die 'E2E runner crosses the public black-box boundary'
fi

printf '%s\n' 'R5 E2E runner smoke checks passed'
