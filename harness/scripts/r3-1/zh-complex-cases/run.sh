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
  --multica-mode "${MNEMON_R3_ZH_MULTICA_MODE:-docker}" \
  --multica-source "${MNEMON_MULTICA_SOURCE_DIR:-/Users/grivn/go/src/github.com/multica-ai/multica/server}" \
  --run-id "${MNEMON_R3_RUN_ID:-manual}"
