#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
export MNEMON_R3_ROOT="${MNEMON_R3_ROOT:-$ROOT}"
export MNEMON_R3_RUN_ID="${MNEMON_R3_RUN_ID:-$(date -u +"%Y%m%dT%H%M%SZ")}"
export MNEMON_R3_OUT_DIR="${MNEMON_R3_OUT_DIR:-$ROOT/.mnemon-dev/tmp/r3-1-test/$MNEMON_R3_RUN_ID}"

PHASES=(
  preflight
  mnemonhub-contract
  cloudflare-local
  cloudflare-live
  multica-runtime-gate
  multica-writeback
  docker-local
  docker-cloudflare
  live-multica
  zh-complex-cases
)

usage() {
  cat <<'USAGE'
Usage:
  harness/scripts/r3-1/run.sh --phase <name>
  harness/scripts/r3-1/run.sh --smoke
  harness/scripts/r3-1/run.sh --full

Phases:
  preflight
  mnemonhub-contract
  cloudflare-local
  cloudflare-live
  multica-runtime-gate
  multica-writeback
  docker-local
  docker-cloudflare
  live-multica
  zh-complex-cases
USAGE
}

run_phase() {
  local phase="$1"
  local runner="$SCRIPT_DIR/$phase/run.sh"
  if [[ ! -x "$runner" ]]; then
    echo "[r3-1] phase runner missing or not executable: $runner" >&2
    return 2
  fi
  echo "[r3-1] phase=$phase out=$MNEMON_R3_OUT_DIR"
  "$runner"
}

if [[ $# -eq 0 ]]; then
  usage
  exit 2
fi

case "$1" in
  --phase)
    [[ $# -eq 2 ]] || { usage >&2; exit 2; }
    run_phase "$2"
    ;;
  --smoke)
    run_phase preflight
    run_phase mnemonhub-contract
    ;;
  --full)
    for phase in "${PHASES[@]}"; do
      run_phase "$phase"
    done
    ;;
  -h|--help)
    usage
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
