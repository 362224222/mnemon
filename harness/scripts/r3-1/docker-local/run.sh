#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"

PHASE="docker-local"
PHASE_DIR="$(mnemon_r3_prepare_phase "$PHASE")"
export MNEMON_R3_PHASE_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

STATE_DIR="$PHASE_DIR/state"
REPLICA_DIR="$STATE_DIR/replicas"
mkdir -p "$REPLICA_DIR"
chmod 700 "$STATE_DIR" "$REPLICA_DIR"
printf 'token-a\n' > "$REPLICA_DIR/a.token"
printf 'token-b\n' > "$REPLICA_DIR/b.token"
chmod 600 "$REPLICA_DIR/a.token" "$REPLICA_DIR/b.token"
cat > "$REPLICA_DIR/replicas.json" <<'JSON'
{
  "schema_version": 1,
  "replicas": [
    {
      "principal": "replica-a@docker",
      "credential_ref": "a.token",
      "scopes": [{"kind": "memory", "id": "project"}]
    },
    {
      "principal": "replica-b@docker",
      "credential_ref": "b.token",
      "scopes": [{"kind": "memory", "id": "project"}]
    }
  ]
}
JSON
chmod 600 "$REPLICA_DIR/replicas.json"

COMPOSE="$SCRIPT_DIR/../docker/compose.yml"
PROJECT="mnemon-r3-local-$(printf '%s' "${MNEMON_R3_RUN_ID:-manual}" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_-' '-')"
HUB_LOG="$PHASE_DIR/hub.log"
NODE_A_LOG="$PHASE_DIR/node-a.log"
NODE_B_LOG="$PHASE_DIR/node-b.log"
DOWN_LOG="$PHASE_DIR/compose-down.log"
export MNEMON_R3_DOCKER_WORK="$STATE_DIR"

cd "$MNEMON_R3_ROOT"
set +e
docker compose -p "$PROJECT" -f "$COMPOSE" up -d hub >"$HUB_LOG" 2>&1
UP_STATUS=$?
if [[ "$UP_STATUS" -eq 0 ]]; then
  docker compose -p "$PROJECT" -f "$COMPOSE" run --rm node-a >"$NODE_A_LOG" 2>&1
  NODE_A_STATUS=$?
else
  NODE_A_STATUS=1
fi
if [[ "$UP_STATUS" -eq 0 && "$NODE_A_STATUS" -eq 0 ]]; then
  docker compose -p "$PROJECT" -f "$COMPOSE" run --rm node-b >"$NODE_B_LOG" 2>&1
  NODE_B_STATUS=$?
else
  NODE_B_STATUS=1
fi
docker compose -p "$PROJECT" -f "$COMPOSE" logs hub >>"$HUB_LOG" 2>&1
docker compose -p "$PROJECT" -f "$COMPOSE" down -v >"$DOWN_LOG" 2>&1
DOWN_STATUS=$?
set -e

python3 "$SCRIPT_DIR/verify.py" \
  --summary "$PHASE_DIR/summary.json" \
  --up-exit "$UP_STATUS" \
  --node-a-exit "$NODE_A_STATUS" \
  --node-b-exit "$NODE_B_STATUS" \
  --down-exit "$DOWN_STATUS" \
  --hub-log "$HUB_LOG" \
  --node-a-log "$NODE_A_LOG" \
  --node-b-log "$NODE_B_LOG" \
  --down-log "$DOWN_LOG"
