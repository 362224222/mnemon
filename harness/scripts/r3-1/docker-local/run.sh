#!/usr/bin/env bash
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
PHASE="docker-local"
PHASE_DIR="$(mnemon_r3_prepare_phase "$PHASE")"
python3 "$SCRIPT_DIR/../lib/report.py" --summary "$PHASE_DIR/summary.json" --phase "$PHASE" --status skipped --skip "Docker R3-1 compose fixture is pending"
