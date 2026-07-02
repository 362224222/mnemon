#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"

PHASE="mnemonhub-contract"
PHASE_DIR="$(mnemon_r3_prepare_phase "$PHASE")"
export MNEMON_R3_PHASE_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

LOG="$PHASE_DIR/go-test.log"
cd "$MNEMON_R3_ROOT"
set +e
go test ./harness/internal/event ./harness/internal/contract ./harness/internal/mnemonhub ./harness/internal/mnemonhub/... ./harness/internal/coreguard ./harness/internal/productconfig >"$LOG" 2>&1
STATUS=$?
set -e

python3 "$SCRIPT_DIR/verify.py" \
  --summary "$PHASE_DIR/summary.json" \
  --log "$LOG" \
  --exit-code "$STATUS"
exit "$STATUS"
