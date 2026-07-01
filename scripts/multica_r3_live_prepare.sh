#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="${MNEMON_PROJECT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
GOBIN="${GOBIN:-$(go env GOPATH)/bin}"
MULTICA_SOURCE="${MULTICA_SOURCE:-/Users/grivn/go/src/github.com/multica-ai/multica}"
MULTICA_BIN="${MNEMON_MULTICA_BIN:-$GOBIN/multica}"
PROFILE="${MNEMON_MULTICA_PROFILE:-desktop-api.multica.ai}"
REGISTRY="${MNEMON_MULTICA_REGISTRY:-$PROJECT_ROOT/.mnemon/harness/multica/registry.json}"
WORKSPACE_ID="${MNEMON_MULTICA_WORKSPACE_ID:-}"
CONTROL_ADDR="${MNEMON_CONTROL_ADDR:-http://127.0.0.1:8787}"
PROVIDER_RUNTIME="${MNEMON_MULTICA_PROVIDER_RUNTIME:-codex}"
PROVIDER_COMMAND="${MNEMON_MULTICA_PROVIDER_COMMAND:-$PROJECT_ROOT/scripts/multica_r3_live_provider.sh}"
PROVIDER_TIMEOUT="${MNEMON_MULTICA_PROVIDER_TURN_TIMEOUT:-5m}"

if [[ ! -f "$REGISTRY" ]]; then
  echo "missing Multica registry: $REGISTRY" >&2
  exit 1
fi

if [[ -z "$WORKSPACE_ID" ]]; then
  WORKSPACE_ID="$(jq -r '.workspace_id // ""' "$REGISTRY")"
fi
if [[ -z "$WORKSPACE_ID" ]]; then
  echo "workspace id is required; set MNEMON_MULTICA_WORKSPACE_ID or update $REGISTRY" >&2
  exit 1
fi

if [[ ! -x "$MULTICA_BIN" ]]; then
  if [[ ! -d "$MULTICA_SOURCE/server" ]]; then
    echo "Multica CLI not found at $MULTICA_BIN and source not found at $MULTICA_SOURCE" >&2
    exit 1
  fi
  (cd "$MULTICA_SOURCE/server" && go build -o "$MULTICA_BIN" ./cmd/multica)
fi

mkdir -p "$GOBIN"
(cd "$PROJECT_ROOT" && go build -o "$GOBIN/mnemon-acceptance" ./harness/cmd/mnemon-acceptance)
(cd "$PROJECT_ROOT" && go build -o "$GOBIN/mnemon-harness" ./harness/cmd/mnemon-harness)
(cd "$PROJECT_ROOT" && go build -o "$GOBIN/mnemon-multica-runtime" ./harness/cmd/mnemon-multica-runtime)
(cd "$PROJECT_ROOT" && go build -o "$GOBIN/mnemond" ./harness/cmd/mnemond)

HARNESS_BIN="${MNEMON_HARNESS_BIN:-$GOBIN/mnemon-harness}"

"$MULTICA_BIN" --profile "$PROFILE" --workspace-id "$WORKSPACE_ID" daemon status --output json >/dev/null
"$MULTICA_BIN" --profile "$PROFILE" --workspace-id "$WORKSPACE_ID" runtime list --output json | jq -e --arg rid "$(jq -r '.runtime_id // ""' "$REGISTRY")" 'map(select(.id == $rid and .status == "online")) | length > 0' >/dev/null

runtime_id="$(jq -r '.runtime_id // ""' "$REGISTRY")"
if [[ -z "$runtime_id" ]]; then
  echo "registry missing runtime_id: $REGISTRY" >&2
  exit 1
fi

while IFS= read -r participant; do
  principal="$(jq -r '.principal // ""' <<<"$participant")"
  agent_id="$(jq -r '.multica_agent_id // ""' <<<"$participant")"
  agent_name="$(jq -r '.multica_agent_name // ""' <<<"$participant")"
  role="$(jq -r '.role // ""' <<<"$participant")"
  if [[ -z "$principal" || -z "$agent_id" ]]; then
    echo "skipping incomplete participant: $participant" >&2
    continue
  fi
  token_name="${principal//@/-}"
  token_file="$PROJECT_ROOT/.mnemon/harness/channel/credentials/${token_name}.token"
  if [[ ! -f "$token_file" ]]; then
    echo "missing mnemond token file for $principal: $token_file" >&2
    exit 1
  fi
  "$HARNESS_BIN" multica \
    --multica-bin "$MULTICA_BIN" \
    --multica-profile "$PROFILE" \
    --multica-workspace-id "$WORKSPACE_ID" \
    --json \
    participant register \
    --registry "$REGISTRY" \
    --project-root "$PROJECT_ROOT" \
    --agent-id "$agent_id" \
    --agent-name "$agent_name" \
    --principal "$principal" \
    --role "$role" \
    --runtime-id "$runtime_id" \
    --sync-agent \
    --mnemon-control-addr "$CONTROL_ADDR" \
    --mnemon-control-token-file "$token_file" \
    --harness-bin "$HARNESS_BIN" \
    --provider-runtime "$PROVIDER_RUNTIME" \
    --provider-command "$PROVIDER_COMMAND" \
    --provider-workspace "$PROJECT_ROOT" \
    --provider-turn-timeout "$PROVIDER_TIMEOUT" >/dev/null
  echo "prepared $principal -> $agent_name"
done < <(jq -c '.participants[]' "$REGISTRY")

echo "Multica R3 live environment ready"
echo "  workspace: $WORKSPACE_ID"
echo "  registry:  $REGISTRY"
echo "  runtime:   $runtime_id"
echo "  provider:  $PROVIDER_COMMAND"
