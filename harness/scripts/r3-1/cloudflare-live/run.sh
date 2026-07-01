#!/usr/bin/env bash
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
PHASE="cloudflare-live"
PHASE_DIR="$(mnemon_r3_prepare_phase "$PHASE")"
export MNEMON_R3_PHASE_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

ENV_FILE="${MNEMON_CLOUDFLARE_ENV_FILE:-$HOME/.mnemon/cloudflare-bootstrap.env}"
LOG="$PHASE_DIR/bootstrap.log"
WORKSPACE="$PHASE_DIR/workspace"
mkdir -p "$WORKSPACE"

if [[ ! -f "$ENV_FILE" ]]; then
  python3 "$SCRIPT_DIR/../lib/report.py" --summary "$PHASE_DIR/summary.json" --phase "$PHASE" --status skipped --skip "Cloudflare env file missing: $ENV_FILE"
  exit 0
fi

if [[ "$(stat -f '%Lp' "$ENV_FILE" 2>/dev/null || stat -c '%a' "$ENV_FILE" 2>/dev/null)" != "600" ]]; then
  python3 "$SCRIPT_DIR/../lib/report.py" --summary "$PHASE_DIR/summary.json" --phase "$PHASE" --status failed --failure "Cloudflare env file permissions must be 0600: $ENV_FILE"
  exit 1
fi

if ! command -v wrangler >/dev/null 2>&1 && ! command -v npx >/dev/null 2>&1; then
  python3 "$SCRIPT_DIR/../lib/report.py" --summary "$PHASE_DIR/summary.json" --phase "$PHASE" --status skipped --skip "Neither wrangler nor npx is available for Cloudflare live deploy"
  exit 0
fi

cd "$MNEMON_R3_ROOT"
set +e
go run ./harness/cmd/mnemon-harness hub bootstrap cloudflare \
  --root "$WORKSPACE" \
  --env-file "$ENV_FILE" \
  --principal "cloudflare-live@team" \
  --replica-id "cloudflare-live" \
  --scope "memory/project" \
  --remote "cloudflare-live" \
  --timeout "${MNEMON_CLOUDFLARE_BOOTSTRAP_TIMEOUT:-5m}" \
  >"$LOG" 2>&1
STATUS=$?
set -e

python3 "$SCRIPT_DIR/verify.py" \
  --summary "$PHASE_DIR/summary.json" \
  --log "$LOG" \
  --exit-code "$STATUS" \
  --workspace "$WORKSPACE"
exit "$?"
