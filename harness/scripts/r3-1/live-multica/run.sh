#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"

PHASE="live-multica"
PHASE_DIR="$(mnemon_r3_prepare_phase "$PHASE")"
export MNEMON_R3_PHASE_STARTED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

PROFILE="${MNEMON_MULTICA_PROFILE:-desktop-api.multica.ai}"
WORKSPACE_ID="${MNEMON_MULTICA_WORKSPACE_ID:-}"
DAEMON_LOG="$PHASE_DIR/daemon-status.json"
RUNTIMES_LOG="$PHASE_DIR/runtimes.json"
AGENTS_LOG="$PHASE_DIR/agents.json"
CREATE_LOG="$PHASE_DIR/issue-create.json"
RUNS_LOG="$PHASE_DIR/issue-runs.json"
MESSAGES_LOG="$PHASE_DIR/run-messages.json"
META_LOG="$PHASE_DIR/live-meta.json"

if ! command -v multica >/dev/null 2>&1; then
  python3 "$SCRIPT_DIR/../lib/report.py" --summary "$PHASE_DIR/summary.json" --phase "$PHASE" --status skipped --skip "multica CLI is not available"
  exit 0
fi

set +e
multica --profile "$PROFILE" daemon status --output json >"$DAEMON_LOG" 2>&1
DAEMON_STATUS=$?
set -e
if [[ "$DAEMON_STATUS" -ne 0 ]]; then
  python3 "$SCRIPT_DIR/../lib/report.py" --summary "$PHASE_DIR/summary.json" --phase "$PHASE" --status skipped --skip "Multica daemon/profile is not ready; see $DAEMON_LOG"
  exit 0
fi

if [[ -z "$WORKSPACE_ID" ]]; then
  WORKSPACE_ID="$(python3 - "$DAEMON_LOG" <<'PY'
import json, sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
workspaces = data.get("workspaces") or []
print((workspaces[0] or {}).get("id", "") if workspaces else "")
PY
)"
fi
if [[ -z "$WORKSPACE_ID" ]]; then
  python3 "$SCRIPT_DIR/../lib/report.py" --summary "$PHASE_DIR/summary.json" --phase "$PHASE" --status skipped --skip "Multica daemon did not report a workspace id"
  exit 0
fi

set +e
multica --profile "$PROFILE" --workspace-id "$WORKSPACE_ID" runtime list --output json >"$RUNTIMES_LOG" 2>&1
RUNTIMES_STATUS=$?
multica --profile "$PROFILE" --workspace-id "$WORKSPACE_ID" agent list --output json >"$AGENTS_LOG" 2>&1
AGENTS_STATUS=$?
set -e

RUNTIME_ID=""
AGENT_NAME=""
if [[ "$RUNTIMES_STATUS" -eq 0 && "$AGENTS_STATUS" -eq 0 ]]; then
  RUNTIME_ID="$(python3 - "$RUNTIMES_LOG" <<'PY'
import json, sys
items = json.load(open(sys.argv[1], encoding="utf-8"))
for item in items:
    if item.get("status") == "online" and "mnemon-runtime" in str(item.get("name", "")):
        print(item.get("id", ""))
        break
PY
)"
  AGENT_NAME="$(python3 - "$AGENTS_LOG" "$RUNTIME_ID" <<'PY'
import json, sys
items = json.load(open(sys.argv[1], encoding="utf-8"))
runtime_id = sys.argv[2]
for item in items:
    if item.get("name") == "mnemon-planner" and (not runtime_id or item.get("runtime_id") == runtime_id):
        print(item.get("name", ""))
        break
PY
)"
fi

CREATE_STATUS=1
RUNS_STATUS=1
MESSAGES_STATUS=1
ISSUE_ID=""
TASK_ID=""
if [[ -n "$RUNTIME_ID" && -n "$AGENT_NAME" ]]; then
  TITLE="R3-1 live Multica runtime smoke ${MNEMON_R3_RUN_ID:-manual}"
  DESCRIPTION="中文 live smoke：验证 mnemon-multica-runtime 在 Multica 中以 codex app-server 形态在线，并能通过 issue 分配触发。"
  set +e
  multica --profile "$PROFILE" --workspace-id "$WORKSPACE_ID" issue create \
    --title "$TITLE" \
    --description "$DESCRIPTION" \
    --assignee "$AGENT_NAME" \
    --status todo \
    --priority medium \
    --output json >"$CREATE_LOG" 2>&1
  CREATE_STATUS=$?
  set -e
  if [[ "$CREATE_STATUS" -eq 0 ]]; then
    ISSUE_ID="$(python3 - "$CREATE_LOG" <<'PY'
import json, sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
print(data.get("id", ""))
PY
)"
  fi
fi

if [[ -n "$ISSUE_ID" ]]; then
  for _ in $(seq 1 18); do
    set +e
    multica --profile "$PROFILE" --workspace-id "$WORKSPACE_ID" issue runs "$ISSUE_ID" --output json >"$RUNS_LOG" 2>&1
    RUNS_STATUS=$?
    set -e
    if [[ "$RUNS_STATUS" -eq 0 ]]; then
      TASK_ID="$(python3 - "$RUNS_LOG" <<'PY'
import json, sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
if isinstance(data, dict):
    data = data.get("runs") or data.get("items") or []
if data:
    first = data[0]
    print(first.get("id") or first.get("task_id") or "")
PY
)"
      [[ -n "$TASK_ID" ]] && break
    fi
    sleep 5
  done
fi

if [[ -n "$TASK_ID" ]]; then
  set +e
  multica --profile "$PROFILE" --workspace-id "$WORKSPACE_ID" issue run-messages "$TASK_ID" --output json >"$MESSAGES_LOG" 2>&1
  MESSAGES_STATUS=$?
  set -e
fi

python3 - "$META_LOG" <<PY
import json, os, sys
data = {
    "profile": os.environ.get("PROFILE", "$PROFILE"),
    "workspace_id": "$WORKSPACE_ID",
    "runtime_id": "$RUNTIME_ID",
    "agent_name": "$AGENT_NAME",
    "issue_id": "$ISSUE_ID",
    "task_id": "$TASK_ID",
}
open(sys.argv[1], "w", encoding="utf-8").write(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
PY

python3 "$SCRIPT_DIR/verify.py" \
  --summary "$PHASE_DIR/summary.json" \
  --daemon-log "$DAEMON_LOG" --daemon-exit "$DAEMON_STATUS" \
  --runtimes-log "$RUNTIMES_LOG" --runtimes-exit "$RUNTIMES_STATUS" \
  --agents-log "$AGENTS_LOG" --agents-exit "$AGENTS_STATUS" \
  --create-log "$CREATE_LOG" --create-exit "$CREATE_STATUS" \
  --runs-log "$RUNS_LOG" --runs-exit "$RUNS_STATUS" \
  --messages-log "$MESSAGES_LOG" --messages-exit "$MESSAGES_STATUS" \
  --meta-log "$META_LOG"
