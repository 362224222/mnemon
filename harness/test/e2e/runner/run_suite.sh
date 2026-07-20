#!/bin/sh
set -eu

. "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)/lib.sh"

runtime=
selected_case=
requested_run=${RUN:-}
image_reference=${IMAGE:-${R5_E2E_IMAGE:-}}
validate_only=false
credential=${LIVE_CODEX_CREDENTIAL_FILE:-${CODEX_AUTH_FILE:-}}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --runtime)
            [ "$#" -ge 2 ] || usage_error '--runtime requires one value'
            runtime=$2
            shift 2
            ;;
        --case)
            [ "$#" -ge 2 ] || usage_error '--case requires one value'
            selected_case=$2
            shift 2
            ;;
        --run)
            [ "$#" -ge 2 ] || usage_error '--run requires one value'
            requested_run=$2
            shift 2
            ;;
        --image)
            [ "$#" -ge 2 ] || usage_error '--image requires one value'
            image_reference=$2
            shift 2
            ;;
        --credential)
            [ "$#" -ge 2 ] || usage_error '--credential requires one value'
            credential=$2
            shift 2
            ;;
        --validate-only)
            validate_only=true
            shift
            ;;
        *) usage_error "unknown runner argument: $1" ;;
    esac
done

[ "$runtime" = scripted ] || [ "$runtime" = codex ] || usage_error 'runtime must be scripted or codex'
[ -z "$image_reference" ] || validate_image_reference "$image_reference"
if [ -n "$selected_case" ]; then
    cases=$selected_case
else
    cases=$canonical_cases
fi

require_command jq
require_command sha256sum
require_command python3
require_command tar
for case_name in $cases; do
    validate_scenario "$case_name"
    python3 "$runner_dir/schema_validate.py" "$schema_root/scenario.schema.json" \
        "$scenario_root/$case_name/manifest.json"
done
if [ "$validate_only" = true ]; then
    printf 'validated R5 %s runner inputs: %s\n' "$runtime" "$cases"
    exit 0
fi

require_command docker
docker compose version >/dev/null 2>&1 || die 'Docker Compose v2 is unavailable'
[ -z "$(git -C "$repo_root" status --porcelain --untracked-files=all -- . ':(exclude).testdata')" ] ||
    die 'actual Docker evidence requires a clean tracked source tree'
git_sha=$(git -C "$repo_root" rev-parse HEAD)
source_tree=$(git -C "$repo_root" rev-parse 'HEAD^{tree}')

if [ "$runtime" = codex ]; then
    require_private_credential "$credential"
    case "${R5_CODEX_MODEL:-}" in
        ''|*[!A-Za-z0-9._-]*) usage_error 'R5_CODEX_MODEL must be an explicit bounded model ID' ;;
    esac
    case "${R5_CODEX_REASONING_EFFORT:-}" in
        low|medium|high|xhigh) ;;
        *) usage_error 'R5_CODEX_REASONING_EFFORT must be low, medium, high, or xhigh' ;;
    esac
fi

codex_version=${CODEX_VERSION:-}
codex_integrity=${CODEX_PACKAGE_INTEGRITY:-}
if [ -n "${codex_version}${codex_integrity}" ]; then
    [ -n "$codex_version" ] && [ -n "$codex_integrity" ] ||
        usage_error 'CODEX_VERSION and CODEX_PACKAGE_INTEGRITY must be supplied together'
    validate_codex_version "$codex_version"
    validate_codex_integrity "$codex_integrity"
fi
if [ "$runtime" = codex ]; then
    [ -n "$codex_version" ] && [ -n "$codex_integrity" ] ||
        usage_error 'Live runs require CODEX_VERSION and CODEX_PACKAGE_INTEGRITY'
fi
if [ -z "$image_reference" ]; then
    suffix=scripted
    [ -n "$codex_version" ] && suffix=$(printf '%s' "$codex_version" | tr -c 'A-Za-z0-9_.-' '-')
    image_reference="mnemon-r5-e2e:${git_sha}-${suffix}"
    docker build --file "$repo_root/harness/test/e2e/docker/Dockerfile" \
      --build-arg "GIT_SHA=$git_sha" --build-arg "VERSION=r5-${git_sha}" \
      --build-arg "SOURCE_TREE=$source_tree" \
      --build-arg "CODEX_VERSION=$codex_version" \
      --build-arg "CODEX_PACKAGE_INTEGRITY=$codex_integrity" \
      --tag "$image_reference" "$repo_root"
fi
validate_image_reference "$image_reference"

image_digest=$(docker image inspect --format '{{.Id}}' "$image_reference" 2>/dev/null) ||
    die "candidate image is unavailable: $image_reference"
validate_image_digest "$image_digest"
image_revision=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' \
    "$image_reference")
[ "$image_revision" = "$git_sha" ] || die 'candidate image was not built from the current commit'
image_source_tree=$(docker image inspect --format '{{index .Config.Labels "io.mnemon.r5.source-tree"}}' \
    "$image_reference")
[ "$image_source_tree" = "$source_tree" ] || die 'candidate image source tree differs from the current commit'
image_codex_version=$(docker image inspect --format '{{index .Config.Labels "io.mnemon.r5.codex-version"}}' \
    "$image_reference")
image_codex_integrity=$(docker image inspect --format '{{index .Config.Labels "io.mnemon.r5.codex-integrity"}}' \
    "$image_reference")
if [ "$runtime" = codex ]; then
    [ "$image_codex_version" = "$codex_version" ] &&
      [ "$image_codex_integrity" = "$codex_integrity" ] ||
        die 'Live runner image Codex coordinates differ from the requested immutable pins'
fi

if [ -n "$requested_run" ]; then
    validate_run_id "$requested_run"
    run_id=$requested_run
else
    suite_case=all-five
    [ -n "$selected_case" ] && suite_case=$selected_case
    run_id=$(new_run_id "$runtime" "$suite_case")
fi
bundle="$run_root/$run_id"
[ ! -e "$bundle" ] || die "RUN already exists: $run_id"
umask 077
mkdir -p "$bundle/cases"
chmod 0700 "$bundle"

paired_hermetic=
if [ "$runtime" = codex ]; then
    paired_hermetic=${HERMETIC_RUN:-}
    if [ -z "$paired_hermetic" ]; then
        for candidate in $(find "$run_root" -mindepth 2 -maxdepth 2 -name report.json -type f -print 2>/dev/null | LC_ALL=C sort -r); do
            if jq -e --arg sha "$git_sha" --arg digest "$image_digest" '
              .bundle_kind == "suite" and .runtime == "scripted" and .status == "passed" and
              .git_sha == $sha and .image.digest == $digest and (.cases | length == 5)
            ' "$candidate" >/dev/null 2>&1; then
                candidate_run=$(basename "$(dirname "$candidate")")
                if "$runner_dir/validate_evidence.sh" --run "$candidate_run" >/dev/null 2>&1; then
                    paired_hermetic=$candidate_run
                    break
                fi
            fi
        done
    fi
    [ -n "$paired_hermetic" ] || die 'Live run requires a passed five-case Hermetic bundle from the same image'
    validate_run_id "$paired_hermetic"
    "$runner_dir/validate_evidence.sh" --run "$paired_hermetic" >/dev/null ||
        die 'Live run requires a fully validated Hermetic evidence pair'
    jq -e --arg sha "$git_sha" --arg digest "$image_digest" '
      .runtime == "scripted" and .status == "passed" and
      .git_sha == $sha and .image.digest == $digest
    ' "$run_root/$paired_hermetic/report.json" >/dev/null ||
        die 'Live/Hermetic pair must use the exact candidate commit and image'
fi

case_records="$bundle/.case-records.jsonl"
: >"$case_records"
suite_failed=false
for case_name in $cases; do
    case_run_id="${run_id}-${case_name}"
    case_output="$bundle/cases/$case_name"
    mkdir -p "$case_output"
    if "$runner_dir/run_case.sh" --runtime "$runtime" --case "$case_name" \
      --run-id "$case_run_id" --output "$case_output" --image "$image_reference" \
      --image-digest "$image_digest" --git-sha "$git_sha" --credential "$credential"; then
        case_exit=0
    else
        case_exit=$?
        suite_failed=true
    fi
    status=failed
    task=false
    system=false
    if [ -f "$case_output/report.json" ]; then
        status=$(jq -r '.status' "$case_output/report.json")
        task=$(jq -r '.oracle.task.passed' "$case_output/report.json")
        system=$(jq -r '.oracle.system.passed' "$case_output/report.json")
    fi
    if [ "$case_exit" -ne 0 ] || [ "$status" != passed ] ||
       [ "$task" != true ] || [ "$system" != true ]; then
        suite_failed=true
    fi
    jq -cn --arg name "$case_name" --arg path "cases/$case_name" \
      --arg run_id "$case_run_id" --arg status "$status" --argjson exit_code "$case_exit" \
      --argjson task "$task" --argjson system "$system" \
      '{name:$name,path:$path,run_id:$run_id,status:$status,exit_code:$exit_code,
        task_oracles_passed:$task,system_invariants_passed:$system}' >>"$case_records"
done

# A CASE target is diagnostic by definition. It preserves a valid per-case
# bundle but cannot be mistaken for release evidence.
if [ -n "$selected_case" ]; then
    rm -f "$case_records"
    printf 'R5 diagnostic case evidence: %s\n' "$bundle/cases/$selected_case"
    [ "$suite_failed" = false ]
    exit
fi

suite_status=passed
[ "$suite_failed" = false ] || suite_status=failed
generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
jq -s --arg run_id "$run_id" --arg runtime "$runtime" --arg status "$suite_status" \
  --arg git_sha "$git_sha" --arg generated_at "$generated_at" \
  --arg reference "$image_reference" --arg digest "$image_digest" \
  --arg revision "$image_revision" --arg source_tree "$image_source_tree" \
  --arg codex_version "$image_codex_version" \
  --arg codex_integrity "$image_codex_integrity" --arg paired "$paired_hermetic" '
  {schema_version:1,run_id:$run_id,bundle_kind:"suite",runtime:$runtime,status:$status,
   generated_at:$generated_at,git_sha:$git_sha,
       image:{reference:$reference,digest:$digest,revision:$revision,
          source_tree:$source_tree,
          codex_version:(if $codex_version == "" then null else $codex_version end),
          codex_integrity:(if $codex_integrity == "" then null else $codex_integrity end)},
   case_names:["payment-review","api-sdk-contract","offline-incident","parallel-hardening","overlapping-channels"],
   cases:.,paired_hermetic_run:(if $paired == "" then null else $paired end)}
  ' "$case_records" >"$bundle/report.json"
rm -f "$case_records"
manifest_evidence "$bundle" "$run_id"
python3 "$runner_dir/schema_validate.py" "$schema_root/suite-report.schema.json" "$bundle/report.json"
python3 "$runner_dir/schema_validate.py" "$schema_root/evidence-manifest.schema.json" \
    "$bundle/manifest.json"
scan_evidence_redaction "$bundle" "$credential"

printf 'R5 complete suite evidence: %s\n' "$bundle"
if [ "$suite_status" = passed ]; then
    "$runner_dir/validate_evidence.sh" --run "$run_id"
    mkdir -p "$repo_root/.testdata/r5"
    printf '%s\n' "$run_id" >"$repo_root/.testdata/r5/latest-$runtime-run"
else
    exit 1
fi
