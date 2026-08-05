#!/usr/bin/env bash

set -euo pipefail

RUNNER_DIR=$(cd "$(dirname "$0")" && pwd -P)
# shellcheck source=static_lib.sh
source "$RUNNER_DIR/static_lib.sh"

count_lines() {
  if test -z "$1"; then
    printf '0\n'
    return
  fi
  printf '%s\n' "$1" | wc -l | tr -d ' '
}

require_one() {
  local matches=$1 description=$2
  if test "$(count_lines "$matches")" != 1; then
    r7_static_fail "$description is not the one frozen interactive surface"
  fi
}

root=$R7_STATIC_HARNESS_ROOT
sources=$(r7_static_production_sources "$root")
test -n "$sources" || r7_static_fail 'R7 production source set is empty'

# Attachment authority has one issuer, and the only production caller is the
# local daemon's interactive attachment service. This checks the issuance API
# rather than banning generic words such as "wake" or "managed" from prose.
issuer_declarations=$(
  cd "$root"
  files=$(find internal/authority -type f -name '*.go' ! -name '*_test.go' -print)
  r7_static_search_files '^func \(s \*Store\) Issue[A-Za-z0-9_]*Attachment\(' "$files"
)
require_one "$issuer_declarations" 'Attachment issuer declaration'
case "$issuer_declarations" in
  internal/authority/attachment_begin.go:*'IssueInteractiveAttachment('*) ;;
  *) r7_static_fail 'Attachment issuer is not IssueInteractiveAttachment' ;;
esac

issuer_calls=$(
  cd "$root"
  r7_static_search_files '\.Issue[A-Za-z0-9_]*Attachment\(' "$sources"
)
require_one "$issuer_calls" 'Attachment issuer call'
case "$issuer_calls" in
  internal/daemon/service.go:*'service.authority.IssueInteractiveAttachment(ctx, service.principal, boundary)'*) ;;
  *) r7_static_fail 'Attachment issuance escaped the interactive daemon service' ;;
esac

# The CLI reaches that service through one exact hook command. Additional
# command aliases or another call to runAttach/client.Attach fail this oracle.
hook_case=$(cd "$root" && r7_static_search_files '^[[:space:]]case "hook\\x00attach":$' internal/cli/app.go)
require_one "$hook_case" 'hook attach command mapping'
attach_returns=$(cd "$root" && r7_static_search_files '^[[:space:]]+return commandAttach$' internal/cli/app.go)
require_one "$attach_returns" 'hook attach command result'
run_attach_calls=$(cd "$root" && files=$(find internal/cli -type f -name '*.go' ! -name '*_test.go' -print) && \
  r7_static_search_files 'return app\.runAttach\(' "$files")
require_one "$run_attach_calls" 'CLI runAttach dispatch'
client_attach_calls=$(cd "$root" && files=$(find internal/cli -type f -name '*.go' ! -name '*_test.go' -print) && \
  r7_static_search_files 'client\.Attach\(ctx, boundary\)' "$files")
require_one "$client_attach_calls" 'CLI attachment request'

# The daemon exposes one attachment route, and that route calls only the
# boundary-digest-bound interactive service. A second route alias or background caller is
# therefore a visible protocol change rather than an implicit wake path.
attach_handlers=$(cd "$root" && files=$(find internal/daemon -type f -name '*.go' ! -name '*_test.go' -print) && \
  r7_static_search_files 'mux\.HandleFunc\([^,]+, server\.handleAttach\)' "$files")
require_one "$attach_handlers" 'daemon attachment route'
case "$attach_handlers" in
  internal/daemon/control.go:*'mux.HandleFunc(routeAttachments, server.handleAttach)'*) ;;
  *) r7_static_fail 'daemon attachment handler is not bound to the frozen route' ;;
esac
service_attach_calls=$(cd "$root" && files=$(find internal/daemon -type f -name '*.go' ! -name '*_test.go' -print) && \
  r7_static_search_files 'server\.service\.attach\(request\.Context\(\), boundary\)' "$files")
require_one "$service_attach_calls" 'daemon interactive attachment call'

attachment_fields() {
  awk '
    /^type attachmentWire struct \{$/ { inside = 1; next }
    inside && /^}$/ { exit }
    inside && match($0, /json:"[^"]+"/) {
      print substr($0, RSTART + 6, RLENGTH - 7)
    }
  ' "$1"
}

expected_fields=$(printf '%s\n' attachment credential expires_at schema version)
for wire in "$root/internal/daemon/control_wire.go" "$root/internal/cli/control_client.go"; do
  if test "$(attachment_fields "$wire")" != "$expected_fields"; then
    r7_static_fail "attachment schema changed outside the frozen interactive surface: $wire"
  fi
done

printf 'r7 static oracle passed: no managed attachment issuance surface\n'
