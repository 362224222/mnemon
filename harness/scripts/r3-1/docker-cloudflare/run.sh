#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"

PHASE="docker-cloudflare"
PHASE_DIR="$(mnemon_r3_prepare_phase "$PHASE")"
export MNEMON_R3_PHASE_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

ENV_FILE="${MNEMON_CLOUDFLARE_ENV_FILE:-$HOME/.mnemon/cloudflare-bootstrap.env}"
BOOTSTRAP_LOG="$PHASE_DIR/bootstrap.log"
PROBE_LOG="$PHASE_DIR/docker-probe.log"
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

if ! command -v docker >/dev/null 2>&1; then
  python3 "$SCRIPT_DIR/../lib/report.py" --summary "$PHASE_DIR/summary.json" --phase "$PHASE" --status skipped --skip "docker command is not available"
  exit 0
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
  --principal "cloudflare-docker@team" \
  --replica-id "cloudflare-docker" \
  --scope "memory/project" \
  --remote "cloudflare-docker" \
  --timeout "${MNEMON_CLOUDFLARE_BOOTSTRAP_TIMEOUT:-5m}" \
  >"$BOOTSTRAP_LOG" 2>&1
BOOTSTRAP_STATUS=$?
set -e

ENDPOINT="$(awk '/^Endpoint: / {print $2}' "$BOOTSTRAP_LOG" | tail -1)"
TOKEN_FILE="$WORKSPACE/.mnemon/harness/sync/credentials/cloudflare-docker.token"
TOKEN=""
if [[ -f "$TOKEN_FILE" ]]; then
  TOKEN="$(tr -d '\r\n' < "$TOKEN_FILE")"
fi

PROBE_STATUS=1
if [[ "$BOOTSTRAP_STATUS" -eq 0 && -n "$ENDPOINT" && -n "$TOKEN" ]]; then
  DECISION_ID="docker-cloudflare-$(date -u +"%Y%m%d%H%M%S")"
  set +e
  docker run --rm \
    -v "$MNEMON_R3_ROOT:/workspace:ro" \
    -v mnemon-r3-docker-gomodcache:/go/pkg/mod \
    -v mnemon-r3-docker-gocache:/root/.cache/go-build \
    -w /workspace \
    golang:1.24 \
    bash -c "go run ./harness/scripts/r3-1/docker/probe.go --mode push --endpoint '$ENDPOINT' --token '$TOKEN' --replica-id cloudflare-docker --decision-id '$DECISION_ID' --content 'Docker 到 Cloudflare Durable Object 的中文同步事件' && go run ./harness/scripts/r3-1/docker/probe.go --mode status --endpoint '$ENDPOINT' --token '$TOKEN' --replica-id cloudflare-docker" \
    >"$PROBE_LOG" 2>&1
  PROBE_STATUS=$?
  set -e
fi

python3 "$SCRIPT_DIR/verify.py" \
  --summary "$PHASE_DIR/summary.json" \
  --bootstrap-log "$BOOTSTRAP_LOG" \
  --bootstrap-exit "$BOOTSTRAP_STATUS" \
  --probe-log "$PROBE_LOG" \
  --probe-exit "$PROBE_STATUS" \
  --endpoint "$ENDPOINT"
