#!/bin/sh
set -eu

runner_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd -P)
repo_root=$(CDPATH= cd -- "$runner_dir/../../../../" && pwd -P)
scenario_root="$repo_root/harness/test/e2e/scenarios"
schema_root="$repo_root/harness/test/e2e/schemas"
run_root="$repo_root/.testdata/r5/runs"
canonical_cases='payment-review api-sdk-contract offline-incident parallel-hardening overlapping-channels'

die() {
    printf 'r5-e2e: %s\n' "$*" >&2
    exit 1
}

usage_error() {
    printf 'r5-e2e: %s\n' "$*" >&2
    exit 2
}

require_command() {
    command -v "$1" >/dev/null 2>&1 || die "required command is unavailable: $1"
}

is_canonical_case() {
    wanted=$1
    for known in $canonical_cases; do
        [ "$known" = "$wanted" ] && return 0
    done
    return 1
}

validate_case_name() {
    case_name=$1
    is_canonical_case "$case_name" || usage_error "unknown CASE: $case_name"
    [ -d "$scenario_root/$case_name" ] || die "scenario directory is unavailable: $case_name"
    [ -f "$scenario_root/$case_name/manifest.json" ] || die "scenario manifest is unavailable: $case_name"
}

validate_run_id() {
    run_id=$1
    printf '%s\n' "$run_id" | grep -Eq '^[A-Za-z0-9][A-Za-z0-9._-]{0,159}$' ||
        usage_error 'RUN contains unsupported characters or exceeds 160 bytes'
}

validate_image_reference() {
    reference=$1
    [ -n "$reference" ] && [ "${#reference}" -le 512 ] ||
        usage_error 'IMAGE reference is empty or exceeds 512 bytes'
    case "$reference" in
        [A-Za-z0-9]* ) ;;
        * ) usage_error 'IMAGE reference has an invalid leading character' ;;
    esac
    case "$reference" in
        *[!A-Za-z0-9._/:@-]* ) usage_error 'IMAGE reference contains unsupported characters' ;;
    esac
}

validate_image_digest() {
    digest=$1
    case "$digest" in
        sha256:*) hex=${digest#sha256:} ;;
        *) usage_error 'candidate image digest is not canonical sha256' ;;
    esac
    [ "${#hex}" -eq 64 ] || usage_error 'candidate image digest is not canonical sha256'
    case "$hex" in
        *[!a-f0-9]*) usage_error 'candidate image digest is not canonical sha256' ;;
    esac
}

validate_git_sha() {
    sha=$1
    [ "${#sha}" -eq 40 ] || usage_error 'candidate git SHA is not canonical'
    case "$sha" in
        *[!a-f0-9]*) usage_error 'candidate git SHA is not canonical' ;;
    esac
}

validate_codex_version() {
    version=$1
    [ -n "$version" ] && [ "${#version}" -le 256 ] ||
        usage_error 'CODEX_VERSION is empty or exceeds 256 bytes'
    case "$version" in
        *[!A-Za-z0-9.+-]*) usage_error 'CODEX_VERSION contains unsupported characters' ;;
    esac
}

validate_codex_integrity() {
    integrity=$1
    case "$integrity" in
        sha512-*'==') payload=${integrity#sha512-}; core=${payload%==} ;;
        *) usage_error 'CODEX_PACKAGE_INTEGRITY must be an exact sha512 SRI value' ;;
    esac
    [ "${#core}" -eq 86 ] ||
        usage_error 'CODEX_PACKAGE_INTEGRITY must be an exact sha512 SRI value'
    case "$core" in
        *[!A-Za-z0-9+/]*)
            usage_error 'CODEX_PACKAGE_INTEGRITY must be an exact sha512 SRI value'
            ;;
    esac
}

scenario_digest() {
    scenario=$1
    (
        cd "$scenario"
        find . -type f -print | LC_ALL=C sort | while IFS= read -r path; do
            test ! -L "$path"
            sha256sum "$path"
        done
    ) | sha256sum | awk '{print "sha256:" $1}'
}

validate_scenario() {
    case_name=$1
    validate_case_name "$case_name"
    manifest="$scenario_root/$case_name/manifest.json"
    jq -e --arg name "$case_name" '
      .schema_version == 1 and .name == $name and .entry_node == "A" and
      .prompt_file == "prompt.txt" and
      ([.nodes[].id] == ["A","B","C","D","E","F"]) and
      ([.channels[] | {alias,owner,members}] == [
        {alias:"alpha",owner:"A",members:["A","B","C"]},
        {alias:"beta",owner:"C",members:["C","D","E"]},
        {alias:"gamma",owner:"E",members:["E","F","A"]}
      ]) and
      .derived_path == ["A:alpha:C","C:beta:E","E:gamma:F","E:beta:C","C:alpha:A"] and
      .expected.business_prompts == 1 and .expected.remote_prompts == 0 and
      .expected.manual_daemon_actions == 0 and .expected.manual_sync_actions == 0 and
      (.oracles.system | type == "array" and length > 0) and
      (.oracles.task | type == "array" and length > 0) and
      (.oracles.experience | type == "array" and length > 0)
    ' "$manifest" >/dev/null || die "scenario manifest violates the frozen topology: $case_name"
    jq -e 'all(.nodes[]; (.policy_file | test("^policies/[A-F]\\.json$")))' "$manifest" >/dev/null ||
        die "scenario policy paths are not closed: $case_name"
    jq -e '
      ([.faults[].id] | length) == ([.faults[].id] | unique | length) and
      ([.oracles.system[],.oracles.experience[],.oracles.task[].id] | length) ==
        ([.oracles.system[],.oracles.experience[],.oracles.task[].id] | unique | length) and
      all(.oracles.task[]; .command == ["/evidence/oracle/oracle.sh"])
    ' "$manifest" >/dev/null || die "scenario fault/oracle identities are not closed: $case_name"
    if find "$scenario_root/$case_name" -type l -print 2>/dev/null | grep -q .; then
        die "scenario inputs may not contain symlinks: $case_name"
    fi
    for node in A B C D E F; do
        policy="$scenario_root/$case_name/policies/$node.json"
        [ -f "$policy" ] || die "scenario policy is unavailable: $case_name/$node"
        jq -e --arg node "$node" '
          .schema_version == 1 and .node == $node and (.offer_to | IN("auto","team")) and
          (.result_content | type == "string" and length > 20 and length <= 1024) and
          (.rework_once | type == "boolean") and (.decline_once | type == "boolean")
        ' "$policy" >/dev/null ||
            die "scenario policy is invalid: $case_name/$node"
        case "$node" in
            A)
                entry_to=C
                [ "$case_name" != overlapping-channels ] || entry_to=team
                jq -e --arg to "$entry_to" '
                  (keys | sort) == (["schema_version","node","entry_channel","entry_to","offer_to","result_content","rework_once","decline_once"] | sort) and
                  .entry_channel == "alpha" and .entry_to == $to and
                  .offer_to == (if $to == "team" then "team" else "auto" end)
                ' "$policy" >/dev/null || die "entry policy is not closed: $case_name/$node"
                ;;
            C)
                derive_to=E
                [ "$case_name" != overlapping-channels ] || derive_to=team
                jq -e --arg to "$derive_to" '
                  (keys | sort) == (["schema_version","node","derive_channel","derive_to","offer_to","result_content","rework_once","decline_once"] | sort) and
                  .derive_channel == "beta" and .derive_to == $to and
                  .offer_to == (if $to == "team" then "team" else "auto" end)
                ' "$policy" >/dev/null || die "derived policy is not closed: $case_name/$node"
                ;;
            E)
                jq -e '
                  (keys | sort) == (["schema_version","node","derive_channel","derive_to","offer_to","result_content","rework_once","decline_once"] | sort) and
                  .derive_channel == "gamma" and .derive_to == "F" and .offer_to == "auto"
                ' "$policy" >/dev/null || die "derived policy is not closed: $case_name/$node"
                ;;
            B|D|F)
                jq -e '
                  (keys | sort) == (["schema_version","node","offer_to","result_content","rework_once","decline_once"] | sort) and
                  .offer_to == "auto"
                ' "$policy" >/dev/null || die "leaf policy is not closed: $case_name/$node"
                ;;
        esac
        [ -d "$scenario_root/$case_name/public/$node" ] ||
            die "public fixture is unavailable: $case_name/$node"
    done
    [ -f "$scenario_root/$case_name/prompt.txt" ] || die "scenario prompt is unavailable: $case_name"
    [ -x "$scenario_root/$case_name/oracle/oracle.sh" ] ||
        die "hidden oracle is unavailable or not executable: $case_name"
    return 0
}

safe_evidence_relative_path() {
    path=$1
    case "$path" in
        ''|/*|.*|*/../*|../*|*/..|*'//'*) return 1 ;;
        .mnemon|.mnemon/*|.codex|.codex/*|.agents|.agents/*|.r5|.r5/*) return 1 ;;
    esac
    printf '%s\n' "$path" | grep -Eq '^[A-Za-z0-9][A-Za-z0-9._/-]{0,239}$'
}

redact_json() {
    jq '
      walk(if type == "object" then
        with_entries(
          if (.key | ascii_downcase | endswith("_redacted")) then .
          elif (.key | ascii_downcase | test("(^|_)(invite_token|token|secret|credential|private_key|claim_secret)($|_)"))
          then .value = "<redacted>" else . end)
      else . end)
    '
}

redact_text_file() {
    source=$1
    destination=$2
    sed -E \
      -e 's/mnch1_[A-Za-z0-9_-]+/<redacted-invite>/g' \
      -e 's/sk-[A-Za-z0-9_-]{12,}/<redacted-provider-key>/g' \
      -e 's/(OPENAI_API_KEY=)[^[:space:]]+/\1<redacted>/g' \
      "$source" >"$destination"
}

new_run_id() {
    runtime=$1
    case_name=$2
    random=$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')
    printf '%s-%s-%s-%s\n' "$(date -u +%Y%m%dT%H%M%SZ)" "$runtime" "$case_name" "$random"
}

mode_of() {
    stat -c '%a' "$1"
}

require_private_credential() {
    credential=$1
    [ -n "$credential" ] || usage_error 'LIVE credential file is required'
    [ -f "$credential" ] && [ ! -L "$credential" ] || die 'provider credential must be a regular non-symlink file'
    [ "$(stat -c '%h' "$credential")" -eq 1 ] || die 'provider credential must have one hard link'
    [ "$(stat -c '%u' "$credential")" -eq "$(id -u)" ] || die 'provider credential must be owned by the invoking user'
    mode=$(mode_of "$credential")
    [ "$mode" = 400 ] || [ "$mode" = 600 ] || die 'provider credential mode must be 0400 or 0600'
    [ -s "$credential" ] || die 'provider credential is empty'
}

project_public_channels() {
    projected_run_id=$1
    shift
    [ "$#" -eq 6 ] || die 'public Channel projection requires six Node status documents'
    jq -s --arg run_id "$projected_run_id" '
      . as $docs |
      def node($index): ["A","B","C","D","E","F"][$index];
      def self_peers($document):
        [$document.channels[].members[] | select(.binding == "self") | .peer_id] | unique;
      if all(range(0;6); (self_peers($docs[.]) | length) == 1) then .
      else error("each Node must expose exactly one stable self PeerID") end |
      ([range(0;6) as $index |
        {key:node($index),value:(self_peers($docs[$index])[0])}] | from_entries) as $by_node |
      ($by_node | to_entries | map({key:.value,value:.key}) | from_entries) as $by_peer |
      if ($by_peer | length) == 6 then . else error("Node PeerIDs must be unique") end |
      def picked($index;$alias): first($docs[$index].channels[] | select(.alias == $alias));
      {schema_version:1,run_id:$run_id,identity_binding:"public-d4",
       channels:[
         (picked(0;"alpha") | del(.invite,.publications) |
           . + {owner_node:$by_peer[.roster_head.owner_peer_id],invite_token_redacted:true,
           evidence_refs:["nodes/A/channel-status-before.json"]}),
         (picked(2;"beta") | del(.invite,.publications) |
           . + {owner_node:$by_peer[.roster_head.owner_peer_id],invite_token_redacted:true,
           evidence_refs:["nodes/C/channel-status-before.json"]}),
         (picked(4;"gamma") | del(.invite,.publications) |
           . + {owner_node:$by_peer[.roster_head.owner_peer_id],invite_token_redacted:true,
           evidence_refs:["nodes/E/channel-status-before.json"]})
       ],evidence_refs:["nodes/A/channel-status-before.json","nodes/B/channel-status-before.json",
         "nodes/C/channel-status-before.json","nodes/D/channel-status-before.json",
         "nodes/E/channel-status-before.json","nodes/F/channel-status-before.json"]}
      | .channels |= map(.members |= map(. + {node:$by_peer[.peer_id]}))
      | if all(.channels[]; .owner_node != null and all(.members[]; .node != null)) then .
        else error("Channel roster contains an unknown Node PeerID") end
    ' "$@"
}

project_public_network_paths() {
    projected_run_id=$1
    topology_file=$2
    shift 2
    [ "$#" -eq 6 ] || die 'public path projection requires six Node status documents'
    jq -s --arg run_id "$projected_run_id" --slurpfile topology "$topology_file" '
      . as $documents |
      def node($index): ["A","B","C","D","E","F"][$index];
      ($topology[0].channels | map(.members[]) |
        group_by(.node) | map({key:.[0].node,value:(map(.peer_id) | unique)}) |
        if all(.[]; (.value | length) == 1) then map(.value = .value[0]) | from_entries
        else error("Node has inconsistent PeerIDs across Channels") end) as $by_node |
      ($by_node | to_entries | map({key:.value,value:.key}) | from_entries) as $by_peer |
      [range(0;6) as $index |
        $documents[$index].channels[] as $channel |
        $channel.publications[] as $publication |
        ($publication.audience_peer_ids | map($by_peer[.])) as $audience_nodes |
        ($publication.ignored_peer_ids | map($by_peer[.])) as $ignored_nodes |
        $publication + {
          channel:$channel.alias,
          observer_node:node($index),observer_peer_id:$by_node[node($index)],
          origin_node:$by_peer[$publication.origin_peer_id],
          immediate_transport_node:$by_peer[$publication.immediate_transport_peer_id],
          audience_nodes:$audience_nodes,ignored_nodes:$ignored_nodes,
          artifact_direct_source_node:(if $publication.artifact_direct_source_peer_id == null then null
            else $by_peer[$publication.artifact_direct_source_peer_id] end),
          evidence_refs:["nodes/" + node($index) + "/channel-status-after.json"]
        }
      ] as $paths |
      if all($paths[];
        .observer_peer_id != null and .origin_node != null and .immediate_transport_node != null and
        all(.audience_nodes[]; . != null) and all(.ignored_nodes[]; . != null) and
        (.artifact_direct_source_peer_id == null or .artifact_direct_source_node != null)) then .
      else error("publication references a PeerID outside the six-Node topology") end |
      {schema_version:1,run_id:$run_id,identity_binding:"public-d4",
       publications:($paths | sort_by(.channel_id_digest,.publication_ref.channel_sequence,
         .publication_ref.origin_peer_id,.publication_ref.origin_epoch,.observer_node)),
       evidence_refs:["nodes/A/channel-status-after.json","nodes/B/channel-status-after.json",
         "nodes/C/channel-status-after.json","nodes/D/channel-status-after.json",
         "nodes/E/channel-status-after.json","nodes/F/channel-status-after.json"]}
    ' "$@"
}

manifest_evidence() {
    directory=$1
    run_id=$2
    if find "$directory" -type l -print | grep -q .; then
        die "evidence contains a symlink: $directory"
    fi
    file_count=$(find "$directory" -type f -printf . | wc -c)
    [ "$file_count" -le 4096 ] || die "evidence exceeds the 4096-file bound: $directory"
    records="$directory/.manifest-records.jsonl"
    : >"$records"
    (
        cd "$directory"
        find . -type f ! -path './manifest.json' ! -name .manifest-records.jsonl -print | LC_ALL=C sort
    ) | while IFS= read -r relative; do
        path=${relative#./}
        safe_evidence_relative_path "$path" || die "unsafe evidence path: $path"
        [ ! -L "$directory/$path" ] || die "evidence contains a symlink: $path"
        digest=$(sha256sum "$directory/$path" | awk '{print "sha256:" $1}')
        bytes=$(stat -c '%s' "$directory/$path")
        jq -cn --arg path "$path" --arg sha256 "$digest" --argjson bytes "$bytes" \
          '{path:$path,sha256:$sha256,bytes:$bytes}' >>"$records"
    done
    jq -s --arg run_id "$run_id" \
      '{schema_version:1,run_id:$run_id,files:sort_by(.path)}' "$records" >"$directory/manifest.json"
    rm -f "$records"
}

scan_evidence_redaction() {
    directory=$1
    credential=${2:-}
    python3 - "$directory" "$credential" <<'PY'
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
credential_path = pathlib.Path(sys.argv[2]) if sys.argv[2] else None
secret = credential_path.read_bytes() if credential_path and credential_path.is_file() else b""
credential_values = set()
if secret:
    try:
        credential_document = json.loads(secret)
    except (UnicodeDecodeError, json.JSONDecodeError):
        credential_document = None

    sensitive_key = re.compile(
        r"(?:auth|token|secret|credential|private|api.?key|authorization|password)", re.I
    )

    def collect_credential_values(value: object, sensitive: bool = False) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                collect_credential_values(child, sensitive or bool(sensitive_key.search(str(key))))
        elif isinstance(value, list):
            for child in value:
                collect_credential_values(child, sensitive)
        elif sensitive and isinstance(value, str) and len(value) >= 8:
            credential_values.add(value.encode())

    if credential_document is not None:
        collect_credential_values(credential_document)
patterns = (
    re.compile(rb"mnch1_[A-Za-z0-9_-]+"),
    re.compile(rb"sk-[A-Za-z0-9_-]{12,}"),
    re.compile(rb"OPENAI_API_KEY\s*[:=]\s*(?!<redacted>)[^\s]+", re.I),
    re.compile(
        rb'''["'](?:invite_token|access_token|refresh_token|id_token|claim_secret|'''
        rb'''private_key|api_key|authorization|password)["']\s*:\s*["'](?!<redacted>)[^"']+''',
        re.I,
    ),
    re.compile(
        rb"(?:access_token|refresh_token|id_token|api_key|authorization|password)\s*=\s*"
        rb"(?!<redacted>)[^\s]+",
        re.I,
    ),
    re.compile(rb"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"),
)
total = 0
for path in root.rglob("*"):
    if path.is_symlink():
        raise SystemExit(f"r5-e2e: evidence contains a symlink: {path}")
    if not path.is_file():
        continue
    size = path.stat().st_size
    total += size
    if size > 64 * 1024 * 1024 or total > 256 * 1024 * 1024:
        raise SystemExit(f"r5-e2e: evidence exceeds bounded redaction scan: {path}")
    raw = path.read_bytes()
    if secret and secret in raw:
        raise SystemExit(f"r5-e2e: provider credential present in evidence: {path}")
    if any(value in raw for value in credential_values):
        raise SystemExit(f"r5-e2e: provider credential value present in evidence: {path}")
    if any(pattern.search(raw) for pattern in patterns):
        raise SystemExit(f"r5-e2e: secret-shaped material present in evidence: {path}")
PY
}
