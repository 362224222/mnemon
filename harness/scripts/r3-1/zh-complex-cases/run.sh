#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"

PHASE="zh-complex-cases"
PHASE_DIR="$(mnemon_r3_prepare_phase "$PHASE")"
export MNEMON_R3_PHASE_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

python3 "$SCRIPT_DIR/run.py" \
  --summary "$PHASE_DIR/summary.json" \
  --out-dir "$PHASE_DIR" \
  --profile "${MNEMON_MULTICA_PROFILE:-desktop-api.multica.ai}" \
  --workspace-id "${MNEMON_MULTICA_WORKSPACE_ID:-}" \
  --run-id "${MNEMON_R3_RUN_ID:-manual}"
