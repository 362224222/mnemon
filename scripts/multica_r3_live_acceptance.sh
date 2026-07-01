#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${MNEMON_PROJECT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
GOBIN="${GOBIN:-$(go env GOPATH)/bin}"
MULTICA_BIN="${MNEMON_MULTICA_BIN:-$GOBIN/multica}"
PROFILE="${MNEMON_MULTICA_PROFILE:-desktop-api.multica.ai}"
REGISTRY="${MNEMON_MULTICA_REGISTRY:-$PROJECT_ROOT/.mnemon/harness/multica/registry.json}"
WORKSPACE_ID="${MNEMON_MULTICA_WORKSPACE_ID:-}"
RUN_ROOT_BASE="${MNEMON_MULTICA_LIVE_RUN_ROOT:-/tmp/mnemon-r3-multica-live}"
WAIT="${MNEMON_MULTICA_ACCEPTANCE_WAIT:-12m}"
POLL="${MNEMON_MULTICA_ACCEPTANCE_POLL:-10s}"
PREPARE="${MNEMON_MULTICA_ACCEPTANCE_PREPARE:-1}"

if [[ -z "$WORKSPACE_ID" && -f "$REGISTRY" ]]; then
  WORKSPACE_ID="$(jq -r '.workspace_id // ""' "$REGISTRY")"
fi
if [[ -z "$WORKSPACE_ID" ]]; then
  echo "workspace id is required; set MNEMON_MULTICA_WORKSPACE_ID or update $REGISTRY" >&2
  exit 1
fi

if [[ "$PREPARE" == "1" ]]; then
  "$PROJECT_ROOT/scripts/multica_r3_live_prepare.sh"
fi

cases=("$@")
if [[ ${#cases[@]} -eq 0 ]]; then
  cases=(r3-surface-readiness protocol-react-drill parallel-poc-overlap)
fi

mkdir -p "$RUN_ROOT_BASE"
for case_id in "${cases[@]}"; do
  stamp="$(date -u +%Y%m%dT%H%M%SZ)"
  run_root="$RUN_ROOT_BASE/${case_id}-${stamp}"
  echo "running $case_id -> $run_root"
  "$GOBIN/mnemon-acceptance" multica-runtime-prod-sim \
    --multica-bin "$MULTICA_BIN" \
    --multica-profile "$PROFILE" \
    --multica-workspace-id "$WORKSPACE_ID" \
    --registry "$REGISTRY" \
    --task-case "$case_id" \
    --require-surface-flow \
    --min-participants 5 \
    --min-active-agents 3 \
    --wait "$WAIT" \
    --poll "$POLL" \
    --run-root "$run_root"
done

echo "Multica R3 live acceptance complete"
