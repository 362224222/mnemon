#!/bin/sh
set -eu

. "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)/lib.sh"

run_id=${RUN:-}
case_path=
ratchet_candidate=${R5_RATCHET_CANDIDATE:-0}
[ "$ratchet_candidate" = 0 ] || [ "$ratchet_candidate" = 1 ] ||
    usage_error 'R5_RATCHET_CANDIDATE must be 0 or 1'
while [ "$#" -gt 0 ]; do
    case "$1" in
        --run)
            [ "$#" -ge 2 ] || usage_error '--run requires one value'
            run_id=$2
            shift 2
            ;;
        --case-path)
            [ "$#" -ge 2 ] || usage_error '--case-path requires one value'
            case_path=$2
            shift 2
            ;;
        *) usage_error "unknown evidence-validator argument: $1" ;;
    esac
done

require_command jq
require_command sha256sum
require_command python3

schema_validate="$runner_dir/schema_validate.py"

verify_manifest() {
    directory=$1
    expected_run_id=$2
    manifest="$directory/manifest.json"
    [ -f "$manifest" ] && [ ! -L "$manifest" ] ||
        die "evidence manifest is missing or is a symlink: $directory"
    if find "$directory" -type l -print | grep -q .; then
        die "evidence tree contains a symlink: $directory"
    fi
    python3 "$schema_validate" "$schema_root/evidence-manifest.schema.json" "$manifest"
    [ "$(jq -r '.run_id' "$manifest")" = "$expected_run_id" ] ||
        die "evidence manifest RUN differs: $directory"
    declared=$(mktemp)
    actual=$(mktemp)
    trap 'rm -f "$declared" "$actual"' EXIT HUP INT TERM
    jq -r '.files[].path' "$manifest" >"$declared"
    LC_ALL=C sort -c "$declared" >/dev/null 2>&1 || die "manifest paths are not sorted: $directory"
    [ "$(sort -u "$declared" | wc -l)" -eq "$(wc -l <"$declared")" ] ||
        die "manifest paths are duplicated: $directory"
    (
        cd "$directory"
        find . -type f ! -path './manifest.json' -print | sed 's#^\./##' | LC_ALL=C sort
    ) >"$actual"
    cmp -s "$declared" "$actual" || die "manifest is not an exact inventory: $directory"
    while IFS=$(printf '\t') read -r path digest bytes; do
        safe_evidence_relative_path "$path" || die "unsafe evidence path in manifest: $path"
        file="$directory/$path"
        [ -f "$file" ] && [ ! -L "$file" ] || die "manifest target is not a regular file: $path"
        [ "sha256:$(sha256sum "$file" | awk '{print $1}')" = "$digest" ] ||
            die "evidence digest mismatch: $path"
        [ "$(stat -c '%s' "$file")" -eq "$bytes" ] || die "evidence size mismatch: $path"
    done <<EOF
$(jq -r '.files[] | [.path,.sha256,(.bytes|tostring)] | @tsv' "$manifest")
EOF
    rm -f "$declared" "$actual"
    trap - EXIT HUP INT TERM
}

verify_evidence_refs() {
    directory=$1
    report=$2
    refs=$(mktemp)
    trap 'rm -f "$refs"' EXIT HUP INT TERM
    jq -r '.commands[]?.evidence[]?, .assertions[]?.evidence[]?,
      .oracle.task.results[]?.evidence[]?, .faults[]?.evidence_refs[]?' \
      "$report" >"$refs"
    jq -r '.evidence_refs[]?, .channels[]?.evidence_refs[]?' \
      "$directory/topology/channels.json" >>"$refs"
    jq -r '.evidence_refs[]?, .publications[]?.evidence_refs[]?' \
      "$directory/topology/network-paths.json" >>"$refs"
    LC_ALL=C sort -u "$refs" -o "$refs"
    while IFS= read -r ref; do
        [ -n "$ref" ] || continue
        safe_evidence_relative_path "$ref" || die "unsafe evidence reference: $ref"
        [ -f "$directory/$ref" ] && [ ! -L "$directory/$ref" ] ||
            die "evidence reference does not resolve: $ref"
        jq -e --arg ref "$ref" 'any(.files[]; .path == $ref)' \
          "$directory/manifest.json" >/dev/null ||
            die "evidence reference is absent from manifest: $ref"
    done <"$refs"
    rm -f "$refs"
    trap - EXIT HUP INT TERM
}

verify_public_projections() {
    directory=$1
    expected_run_id=$2
    expected_channels=$(mktemp)
    expected_paths=$(mktemp)
    trap 'rm -f "$expected_channels" "$expected_paths"' EXIT HUP INT TERM
    project_public_channels "$expected_run_id" \
      "$directory/nodes/A/channel-status-before.json" \
      "$directory/nodes/B/channel-status-before.json" \
      "$directory/nodes/C/channel-status-before.json" \
      "$directory/nodes/D/channel-status-before.json" \
      "$directory/nodes/E/channel-status-before.json" \
      "$directory/nodes/F/channel-status-before.json" >"$expected_channels" ||
        die 'public Channel topology cannot be reproduced from source status evidence'
    expected_digest=$(jq -S -c . "$expected_channels" | sha256sum | awk '{print $1}')
    observed_digest=$(jq -S -c . "$directory/topology/channels.json" | sha256sum | awk '{print $1}')
    [ "$expected_digest" = "$observed_digest" ] ||
        die 'public Channel topology differs from its six source status documents'

    project_public_network_paths "$expected_run_id" "$expected_channels" \
      "$directory/nodes/A/channel-status-after.json" \
      "$directory/nodes/B/channel-status-after.json" \
      "$directory/nodes/C/channel-status-after.json" \
      "$directory/nodes/D/channel-status-after.json" \
      "$directory/nodes/E/channel-status-after.json" \
      "$directory/nodes/F/channel-status-after.json" >"$expected_paths" ||
        die 'public publication paths cannot be reproduced from source status evidence'
    expected_digest=$(jq -S -c . "$expected_paths" | sha256sum | awk '{print $1}')
    observed_digest=$(jq -S -c . "$directory/topology/network-paths.json" | sha256sum | awk '{print $1}')
    [ "$expected_digest" = "$observed_digest" ] ||
        die 'public publication paths differ from their six source status documents'
    rm -f "$expected_channels" "$expected_paths"
    trap - EXIT HUP INT TERM
}

verify_case_semantics() {
    directory=$1
    expected_name=$2
    expected_runtime=$3
    report="$directory/report.json"
    scenario_manifest="$scenario_root/$expected_name/manifest.json"
    [ "$(jq -r '.scenario_digest' "$report")" = \
      "$(scenario_digest "$scenario_root/$expected_name")" ] ||
        die "scenario digest differs from current inputs: $expected_name"
    if [ -n "$expected_runtime" ]; then
        [ "$(jq -r '.runtime' "$report")" = "$expected_runtime" ] ||
            die "case runtime differs from suite runtime: $expected_name"
    fi
    jq -e '
      .commands as $commands |
      [$commands[].sequence] == [range(1; ($commands | length) + 1)] and
      ([$commands[] | [.node,.kind]] == [
        ["A","setup"],["B","setup"],["C","setup"],["D","setup"],["E","setup"],["F","setup"],
        ["A","channel-create"],["B","channel-join"],["C","channel-join"],
        ["C","channel-create"],["D","channel-join"],["E","channel-join"],
        ["E","channel-create"],["F","channel-join"],["A","channel-join"],
        ["A","status"],["B","status"],["C","status"],["D","status"],["E","status"],["F","status"],
        ["A","business-prompt"],
        ["A","status"],["A","doctor"],["B","status"],["B","doctor"],
        ["C","status"],["C","doctor"],["D","status"],["D","doctor"],
        ["E","status"],["E","doctor"],["F","status"],["F","doctor"]
      ]) and
      all($commands[]; .exit_code == 0 and .duration_ms >= 0 and
        (.kind | IN("setup","channel-create","channel-join","status","doctor","business-prompt"))) and
      ([$commands[] | select(.kind == "setup")] | length) == 6 and
      ([$commands[] | select(.kind == "setup") | .node] | sort) == ["A","B","C","D","E","F"] and
      ([$commands[] | select(.kind == "channel-create")] | length) == 3 and
      ([$commands[] | select(.kind == "channel-join")] | length) == 6 and
      ([$commands[] | select(.kind == "business-prompt" and .node == "A")] | length) == 1 and
      .counts.business_prompts == ([$commands[] | select(.kind == "business-prompt")] | length) and
      .counts.remote_prompts == 0 and .counts.manual_daemon_actions == 0 and
      .counts.manual_sync_actions == 0 and .counts.nodes == 6 and .counts.channels == 3 and
      .latency.setup_ms == [$commands[] | select(.kind == "setup") | .duration_ms] and
      .latency.channel_join_ms == [$commands[] | select(.kind == "channel-join") | .duration_ms] and
      (.latency.channel_ready_ms | length == 3 and all(. <= 10000)) and
      ([.assertions[].id] | length) == ([.assertions[].id] | unique | length) and
      ([.oracle.task.results[].id] | length) == ([.oracle.task.results[].id] | unique | length)
    ' "$report" >/dev/null || die "command/count/latency evidence is inconsistent: $expected_name"
    jq -e --slurpfile scenario "$scenario_manifest" '
      ([.faults[] | {id,type,phase,target,required_observation}] | sort_by(.id)) ==
        ([$scenario[0].faults[] | {id,type,phase,target,required_observation}] | sort_by(.id)) and
      all(.faults[]; .injected == true and .observation_passed == true)
    ' "$report" >/dev/null || die "declared faults lack passed observations: $expected_name"
    jq -e '.publications | length > 0' "$directory/topology/network-paths.json" >/dev/null ||
        die "passed case has no grounded publication paths: $expected_name"
    verify_evidence_refs "$directory" "$report"
}

validate_case_bundle() {
    directory=$1
    expected_name=$2
    case_run_id=$3
    expected_runtime=${4:-}
    [ -d "$directory" ] && [ ! -L "$directory" ] ||
        die "case evidence directory is missing or is a symlink: $expected_name"
    [ "$(mode_of "$directory")" = 700 ] ||
        die "case evidence directory must be owner-only mode 0700: $expected_name"
    report="$directory/report.json"
    [ -f "$report" ] || die "case report is missing: $expected_name"
    verify_manifest "$directory" "$case_run_id"
    python3 "$schema_validate" "$schema_root/report.schema.json" "$report"
    python3 "$schema_validate" "$schema_root/channels.schema.json" \
        "$directory/topology/channels.json"
    python3 "$schema_validate" "$schema_root/network-paths.schema.json" \
        "$directory/topology/network-paths.json"
    [ "$(jq -r '.run_id' "$directory/topology/channels.json")" = "$case_run_id" ] &&
      [ "$(jq -r '.run_id' "$directory/topology/network-paths.json")" = "$case_run_id" ] ||
        die "public D4 projection RUN differs: $expected_name"
    verify_public_projections "$directory" "$case_run_id"
    [ "$(jq -r '.scenario' "$report")" = "$expected_name" ] ||
        die "case report scenario differs: $expected_name"
    [ "$(jq -r '.run_id' "$report")" = "$case_run_id" ] ||
        die "case report RUN differs: $expected_name"
    python3 "$schema_validate" "$schema_root/scenario.schema.json" \
        "$scenario_root/$expected_name/manifest.json"
    verify_case_semantics "$directory" "$expected_name" "$expected_runtime"

    scenario_manifest="$scenario_root/$expected_name/manifest.json"
    for category in system experience; do
        for oracle_id in $(jq -r --arg category "$category" '.oracles[$category][]' "$scenario_manifest"); do
            jq -e --arg id "$oracle_id" --arg category "$category" '
              any(.assertions[]; .id == $id and .category == $category and
                  .required == true and .passed == true)
            ' "$report" >/dev/null || die "required $category oracle did not pass: $expected_name/$oracle_id"
        done
    done
    for oracle_id in $(jq -r '.oracles.task[].id' "$scenario_manifest"); do
        if [ "$ratchet_candidate" = 1 ]; then
            jq -e --arg id "$oracle_id" 'any(.oracle.task.results[]; .id == $id)' \
              "$report" >/dev/null || die "required task oracle result is absent: $expected_name/$oracle_id"
        else
            jq -e --arg id "$oracle_id" '
              any(.oracle.task.results[]; .id == $id and .passed == true)
            ' "$report" >/dev/null || die "required task oracle did not pass: $expected_name/$oracle_id"
        fi
    done
    if [ "$ratchet_candidate" = 1 ]; then
        jq -e '
          all(.assertions[]; (.required != true) or .category == "task" or .passed == true) and
          .oracle.system.passed == true and .oracle.experience.passed == true and
          .counts.business_prompts == 1 and .counts.remote_prompts == 0 and
          .counts.manual_daemon_actions == 0 and .counts.manual_sync_actions == 0 and
          .counts.nodes == 6 and .counts.channels == 3
        ' "$report" >/dev/null || die "ratchet case invariants are incomplete or failed: $expected_name"
    else
        jq -e '
          all(.assertions[]; (.required != true) or .passed == true) and
          .oracle.system.passed == true and .oracle.task.passed == true and
          .oracle.experience.passed == true and
          .counts.business_prompts == 1 and .counts.remote_prompts == 0 and
          .counts.manual_daemon_actions == 0 and .counts.manual_sync_actions == 0 and
          .counts.nodes == 6 and .counts.channels == 3 and .status == "passed"
        ' "$report" >/dev/null || die "case verdict is incomplete or failed: $expected_name"
    fi
    jq -e '
      .identity_binding == "public-d4" and
      (.channels | length == 3) and
      ([.channels[].alias] | sort) == ["alpha","beta","gamma"] and
      ([.channels[].channel_id_digest] | length) ==
        ([.channels[].channel_id_digest] | unique | length) and
      ([.channels[].members[] | {node,peer_id}] | group_by(.node) | length) == 6 and
      all([.channels[].members[] | {node,peer_id}] | group_by(.node)[];
        ([.[].peer_id] | unique | length) == 1) and
      all(.channels[]; . as $channel |
        .membership == "active" and .topic.status == "joined" and
        .owner.local == true and .owner.reachability == "self" and
        .roster_revision == .roster_head.revision and
        any(.members[]; .peer_id == $channel.roster_head.owner_peer_id and
          .node == $channel.owner_node) and
        all(.members[]; .status == "active" and .baseline_ready == true)) and
      ((.channels | map(select(.alias == "alpha"))[0]) as $channel |
        $channel.owner_node == "A" and ([$channel.members[].node] | sort) == ["A","B","C"]) and
      ((.channels | map(select(.alias == "beta"))[0]) as $channel |
        $channel.owner_node == "C" and ([$channel.members[].node] | sort) == ["C","D","E"]) and
      ((.channels | map(select(.alias == "gamma"))[0]) as $channel |
        $channel.owner_node == "E" and ([$channel.members[].node] | sort) == ["A","E","F"])
    ' "$directory/topology/channels.json" >/dev/null ||
        die "D4 Channel digest, roster head, or member PeerID evidence is malformed: $expected_name"
    jq -e --slurpfile topology "$directory/topology/channels.json" '
      .identity_binding == "public-d4" and (.publications | length > 0) and
      ($topology[0].channels | map(.members[]) | group_by(.node) |
        map({key:.[0].node,value:(map(.peer_id) | unique)}) |
        if all(.[]; (.value | length) == 1) then map(.value = .value[0]) | from_entries
        else error("inconsistent Node PeerIDs") end) as $by_node |
      ($by_node | to_entries | map({key:.value,value:.key}) | from_entries) as $by_peer |
      ($topology[0].channels | map({key:.alias,value:.channel_id_digest}) | from_entries) as $channel_digest |
      ($topology[0].channels | map({key:.channel_id_digest,value:[.members[].peer_id]}) | from_entries) as $channel_peers |
      ([.publications[] |
        [.observer_node,.channel_id_digest,.publication_ref.origin_peer_id,
         .publication_ref.origin_epoch,.publication_ref.channel_sequence]] | length) ==
        ([.publications[] |
          [.observer_node,.channel_id_digest,.publication_ref.origin_peer_id,
           .publication_ref.origin_epoch,.publication_ref.channel_sequence]] | unique | length) and
      all(.publications[]; . as $publication |
        $channel_digest[.channel] == .channel_id_digest and
        $by_node[.observer_node] == .observer_peer_id and
        $by_peer[.origin_peer_id] == .origin_node and
        $by_peer[.immediate_transport_peer_id] == .immediate_transport_node and
        (.audience_peer_ids | map($by_peer[.])) == .audience_nodes and
        (.ignored_peer_ids | map($by_peer[.])) == .ignored_nodes and
        (($publication.artifact_direct_source_peer_id == null and
          $publication.artifact_direct_source_node == null) or
         ($publication.artifact_direct_source_peer_id == $publication.origin_peer_id and
          $publication.artifact_direct_source_node == $publication.origin_node)) and
        (.publication_ref.origin_peer_id == .origin_peer_id) and
        (.event_key.origin_peer_id == .origin_peer_id) and
        (.event_key.origin_epoch == .publication_ref.origin_epoch) and
        (($channel_peers[$publication.channel_id_digest] | index($publication.origin_peer_id)) != null) and
        (($channel_peers[$publication.channel_id_digest] | index($publication.observer_peer_id)) != null) and
        (($channel_peers[$publication.channel_id_digest] | index($publication.immediate_transport_peer_id)) != null) and
        all(.audience_peer_ids[]; . as $peer |
          ($channel_peers[$publication.channel_id_digest] | index($peer)) != null) and
        if .arrival == "local" then
          .observer_peer_id == .origin_peer_id and
          .immediate_transport_peer_id == .origin_peer_id and
          .semantic_outcome == "originated" and
          .artifact_direct_source_peer_id == null and
          (.ignored_peer_ids | length) == 0
        else
          .observer_peer_id != .origin_peer_id and
          .immediate_transport_peer_id != .observer_peer_id and
          (.semantic_outcome != "originated") and
          (if .arrival == "repair" then .immediate_transport_peer_id == .origin_peer_id else true end) and
          (.observer_peer_id as $observer |
            if (.audience_peer_ids | index($observer)) != null then
              (.ignored_peer_ids | length) == 0 and .semantic_outcome != "ignored"
            else
              .ignored_peer_ids == [$observer] and
              (.semantic_outcome == "ignored" or .semantic_outcome == "quarantined")
            end)
        end) and
      all(.publications | group_by([
        .channel_id_digest,.publication_ref.origin_peer_id,
        .publication_ref.origin_epoch,.publication_ref.channel_sequence])[];
        (map({channel,publication_ref,publication_digest,event_key,event_digest,
          channel_id_digest,origin_peer_id,audience_peer_ids,causality_event_key}) |
          unique | length) == 1)
    ' "$directory/topology/network-paths.json" >/dev/null ||
        die "D4 publication origin/transport/audience/provenance evidence is inconsistent: $expected_name"
    scan_evidence_redaction "$directory" \
        "${LIVE_CODEX_CREDENTIAL_FILE:-${CODEX_AUTH_FILE:-}}"
}

if [ -n "$case_path" ]; then
    [ -d "$case_path" ] || die "case evidence directory is unavailable: $case_path"
    report="$case_path/report.json"
    validate_case_bundle "$case_path" "$(jq -r '.scenario' "$report")" "$(jq -r '.run_id' "$report")"
    printf 'validated R5 case evidence: %s\n' "$case_path"
    exit 0
fi

[ -n "$run_id" ] || usage_error 'RUN is required and must name a complete five-case bundle'
validate_run_id "$run_id"
bundle="$run_root/$run_id"
[ -d "$bundle" ] || die "RUN bundle is unavailable: $run_id"
[ "$(mode_of "$bundle")" = 700 ] || die "RUN bundle must be owner-only mode 0700"
suite_report="$bundle/report.json"
[ -f "$suite_report" ] || die "suite report is missing: $run_id"
verify_manifest "$bundle" "$run_id"
python3 "$schema_validate" "$schema_root/suite-report.schema.json" "$suite_report"

current_sha=$(git -C "$repo_root" rev-parse HEAD)
current_tree=$(git -C "$repo_root" rev-parse 'HEAD^{tree}')
[ "$(jq -r '.git_sha' "$suite_report")" = "$current_sha" ] ||
    die 'evidence commit does not equal the current commit'
expected_cases='["payment-review","api-sdk-contract","offline-incident","parallel-hardening","overlapping-channels"]'
jq -e --argjson expected "$expected_cases" '.case_names == $expected and [.cases[].name] == $expected' \
    "$suite_report" >/dev/null || die 'suite does not contain the five canonical cases exactly once'
jq -e --arg run "$run_id" '
  all(.cases[]; .path == ("cases/" + .name) and .run_id == ($run + "-" + .name))
' "$suite_report" >/dev/null || die 'suite case paths or RUN identities are inconsistent'

image_reference=$(jq -er '.image.reference' "$suite_report")
image_digest=$(jq -er '.image.digest' "$suite_report")
require_command docker
observed_digest=$(docker image inspect --format '{{.Id}}' "$image_reference" 2>/dev/null) ||
    die 'recorded candidate image is unavailable for digest verification'
[ "$observed_digest" = "$image_digest" ] || die 'candidate image digest differs from evidence'
observed_revision=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' \
    "$image_reference")
[ "$observed_revision" = "$current_sha" ] || die 'candidate image revision label differs from current commit'
observed_tree=$(docker image inspect --format '{{index .Config.Labels "io.mnemon.r5.source-tree"}}' \
    "$image_reference")
[ "$observed_tree" = "$current_tree" ] &&
    [ "$(jq -r '.image.source_tree' "$suite_report")" = "$current_tree" ] ||
    die 'candidate image source-tree label differs from current commit'
jq -e --arg sha "$current_sha" --arg tree "$current_tree" '
  .git_sha == $sha and .image.revision == $sha and .image.source_tree == $tree
' "$suite_report" >/dev/null || die 'suite image commit/tree fields are internally inconsistent'
observed_version=$(docker run --rm --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges "$image_reference" mnemon-harness --version 2>/dev/null) ||
    die 'candidate mnemon-harness executable is unavailable'
[ "$observed_version" = "mnemon-harness version r5-$current_sha" ] ||
    die 'candidate mnemon-harness version differs from the current commit'
docker run --rm --network none --read-only --cap-drop ALL --security-opt no-new-privileges \
    --entrypoint test "$image_reference" \
    ! -e /opt/r5/bin/scripted-task-apply >/dev/null 2>&1 ||
    die 'candidate image contains the Hermetic task implementation'
for label in codex-version codex-integrity; do
    observed=$(docker image inspect --format \
      "{{index .Config.Labels \"io.mnemon.r5.$label\"}}" "$image_reference")
    field=$(printf '%s' "$label" | tr '-' '_')
    recorded=$(jq -r --arg field "$field" '.image[$field] // ""' "$suite_report")
    [ "$observed" = "$recorded" ] || die "candidate image $label label differs from evidence"
done
suite_runtime=$(jq -r '.runtime' "$suite_report")
if [ "$suite_runtime" = scripted ]; then
    jq -e '.paired_hermetic_run == null' "$suite_report" >/dev/null ||
        die 'Hermetic suite must not claim a paired Hermetic run'
else
    jq -e '.paired_hermetic_run | type == "string" and length > 0' "$suite_report" >/dev/null ||
        die 'Live suite lacks its paired Hermetic RUN identity'
fi
if [ "$suite_runtime" = codex ]; then
    recorded_codex_version=$(jq -er '.image.codex_version | select(length > 0)' "$suite_report")
    jq -e '.image.codex_integrity | test("^sha512-[A-Za-z0-9+/]{86}==$")' \
      "$suite_report" >/dev/null || die 'Live Codex package integrity is not an immutable sha512 SRI'
    observed_codex_cli=$(docker run --rm --network none --read-only --cap-drop ALL \
      --security-opt no-new-privileges "$image_reference" codex --version 2>/dev/null) ||
        die 'pinned Live Codex executable is unavailable'
    case "$observed_codex_cli" in
        *" $recorded_codex_version"|*"-$recorded_codex_version") ;;
        *) die 'pinned Live Codex executable version differs from its image label' ;;
    esac
fi

for case_name in $canonical_cases; do
    case_run_id=$(jq -er --arg name "$case_name" '.cases[] | select(.name == $name) | .run_id' "$suite_report")
    case_directory="$bundle/cases/$case_name"
    validate_case_bundle "$case_directory" "$case_name" "$case_run_id" \
        "$(jq -r '.runtime' "$suite_report")"
    jq -e --arg name "$case_name" --slurpfile case_report "$case_directory/report.json" '
      (.cases[] | select(.name == $name)) as $record |
      $case_report[0] as $case |
      $record.run_id == $case.run_id and $record.status == $case.status and
      $record.task_oracles_passed == $case.oracle.task.passed and
      $record.system_invariants_passed == $case.oracle.system.passed and
      (($record.exit_code == 0) == ($case.status == "passed"))
    ' "$suite_report" >/dev/null || die "suite summary differs from case evidence: $case_name"
    jq -e --arg name "$case_name" --arg sha "$current_sha" --arg digest "$image_digest" '
      select(.scenario == $name and .git_sha == $sha and .image_digest == $digest)
    ' "$case_directory/report.json" >/dev/null || die "mixed commit/image in case evidence: $case_name"
done

for required in ND-17 ND-18 ND-20 ND-21 CH-15 AR-01; do
    found=false
    for case_name in $canonical_cases; do
        if jq -e --arg id "$required" 'any(.assertions[]; .id == $id and .passed == true)' \
            "$bundle/cases/$case_name/report.json" >/dev/null; then
            found=true
            break
        fi
    done
    [ "$found" = true ] || die "independent scenario invariant is absent: $required"
done

if [ "$ratchet_candidate" = 1 ]; then
    jq -e 'all(.cases[]; .system_invariants_passed == true)' \
      "$suite_report" >/dev/null || die 'ratchet suite has a failed system invariant'
else
    jq -e '.status == "passed" and all(.cases[]; .status == "passed" and .exit_code == 0 and
      .task_oracles_passed == true and .system_invariants_passed == true)' \
      "$suite_report" >/dev/null || die 'suite verdict is incomplete or failed'
fi

runtime=$suite_runtime
if [ "$runtime" = codex ]; then
    paired=$(jq -er '.paired_hermetic_run' "$suite_report")
    validate_run_id "$paired"
    paired_report="$run_root/$paired/report.json"
    [ -f "$paired_report" ] || die 'Live bundle lacks its Hermetic evidence pair'
    R5_RATCHET_CANDIDATE=0 R5_VALIDATION_SKIP_HISTORY=1 \
      "$runner_dir/validate_evidence.sh" --run "$paired" >/dev/null ||
        die 'Live bundle Hermetic pair did not pass full evidence validation'
    jq -e --arg sha "$current_sha" --arg digest "$image_digest" '
      .runtime == "scripted" and .status == "passed" and
      .git_sha == $sha and .image.digest == $digest and
      all(.cases[]; .status == "passed" and .system_invariants_passed == true)
    ' "$paired_report" >/dev/null || die 'Live/Hermetic evidence uses different commit/image or failed invariants'

    if [ "${R5_VALIDATION_SKIP_HISTORY:-0}" != 1 ]; then
        candidates=$(mktemp)
        candidate_task_results=$(mktemp)
        trap 'rm -f "$candidates" "$candidate_task_results"' EXIT HUP INT TERM
        find "$run_root" -mindepth 2 -maxdepth 2 -name report.json -type f -print |
          while IFS= read -r report; do
              jq -er --arg sha "$current_sha" --arg digest "$image_digest" --arg path "$report" '
                select(.bundle_kind == "suite" and .runtime == "codex" and
                  .git_sha == $sha and .image.digest == $digest and (.cases | length == 5)) |
                [.generated_at,$path] | @tsv
              ' "$report" 2>/dev/null || true
          done | LC_ALL=C sort -r -k1,1 -k2,2 | head -n 10 | cut -f2- >"$candidates"
        [ "$(wc -l <"$candidates")" -eq 10 ] ||
            die 'fewer than ten full Live runs exist for the current candidate'
        while IFS= read -r candidate_report; do
            candidate_run=$(basename "$(dirname "$candidate_report")")
            R5_RATCHET_CANDIDATE=1 R5_VALIDATION_SKIP_HISTORY=1 \
              "$runner_dir/validate_evidence.sh" --run "$candidate_run" >/dev/null ||
                die "Live ratchet candidate is invalid: $candidate_run"
            candidate_bundle=$(dirname "$candidate_report")
            for case_name in $canonical_cases; do
                jq -c '.oracle.task.passed' "$candidate_bundle/cases/$case_name/report.json" \
                  >>"$candidate_task_results"
            done
        done <"$candidates"
        jq -s -e 'length == 50 and
          (([.[] | select(. == true)] | length) / length >= 0.8)
        ' "$candidate_task_results" >/dev/null ||
            die 'Live rolling last-10 is below 80% task success or 100% invariants'
        rm -f "$candidates" "$candidate_task_results"
        trap - EXIT HUP INT TERM
    fi
fi

scan_evidence_redaction "$bundle" "${LIVE_CODEX_CREDENTIAL_FILE:-${CODEX_AUTH_FILE:-}}"
printf 'validated complete R5 evidence bundle: %s\n' "$run_id"
