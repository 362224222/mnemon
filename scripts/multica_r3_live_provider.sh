#!/usr/bin/env bash
set -euo pipefail

PROMPT="$(cat)"
ISSUE_ID="${MULTICA_ISSUE_ID:-}"
if [[ -z "$ISSUE_ID" ]]; then
  echo "Mnemon Multica runtime handled issue unknown. Multica surface input: observed. Missing MULTICA_ISSUE_ID." >&2
  exit 1
fi

MULTICA_BIN="${MNEMON_MULTICA_BIN:-multica}"
MULTICA_PROFILE="${MNEMON_MULTICA_PROFILE:-}"
WORKSPACE_ID="${MNEMON_MULTICA_WORKSPACE_ID:-${MULTICA_WORKSPACE_ID:-}}"
HARNESS_BIN="${MNEMON_HARNESS_BIN:-mnemon-harness}"
REGISTRY="${MNEMON_MULTICA_REGISTRY:-.mnemon/harness/multica/registry.json}"
PRINCIPAL="${MNEMON_CONTROL_PRINCIPAL:-${MULTICA_AGENT_NAME:-multica-agent}}"
AGENT_ID="${MULTICA_AGENT_ID:-}"

mcli() {
  local args=()
  if [[ -n "$MULTICA_PROFILE" ]]; then
    args+=(--profile "$MULTICA_PROFILE")
  fi
  if [[ -n "$WORKSPACE_ID" ]]; then
    args+=(--workspace-id "$WORKSPACE_ID")
  fi
  "$MULTICA_BIN" "${args[@]}" "$@"
}

mh_multica() {
  local args=(multica)
  if [[ -n "$MULTICA_BIN" ]]; then
    args+=(--multica-bin "$MULTICA_BIN")
  fi
  if [[ -n "$MULTICA_PROFILE" ]]; then
    args+=(--multica-profile "$MULTICA_PROFILE")
  fi
  if [[ -n "$WORKSPACE_ID" ]]; then
    args+=(--multica-workspace-id "$WORKSPACE_ID")
  fi
  args+=(--json)
  "$HARNESS_BIN" "${args[@]}" "$@"
}

agent_id_for() {
  local principal="$1"
  if [[ ! -f "$REGISTRY" ]]; then
    return 0
  fi
  jq -r --arg principal "$principal" '.participants[]? | select(.principal == $principal) | .multica_agent_id // empty' "$REGISTRY" | head -1
}

safe_segment() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -cs 'a-z0-9._-' '-' | sed 's/^-//; s/-$//'
}

issue_json="$(mcli issue get "$ISSUE_ID" --output json)"
IDENTIFIER="$(jq -r '.identifier // .id // ""' <<<"$issue_json")"
TITLE="$(jq -r '.title // ""' <<<"$issue_json")"
DESCRIPTION="$(jq -r '.description // ""' <<<"$issue_json")"
PARENT_ID="$(jq -r '.parent_issue_id // ""' <<<"$issue_json")"
CASE_ROOT="$(printf '%s\n' "$DESCRIPTION" | sed -n 's/^- Case root: `\(.*\)`/\1/p' | head -1)"
if [[ -z "$CASE_ROOT" ]]; then
  CASE_ROOT="${MNEMON_MULTICA_LIVE_RUN_ROOT:-/tmp/mnemon-r3-multica-live-provider}/${ISSUE_ID}"
fi
mkdir -p "$CASE_ROOT/evidence" "$CASE_ROOT/shared-context"

write_artifact() {
  local slug="$1"
  local body="$2"
  local path="$CASE_ROOT/evidence/${slug}.md"
  printf '%s\n' "$body" > "$path"
  printf '%s' "$path"
}

surface_report() {
  local issue_id="$1"
  local slug="$2"
  local summary="$3"
  local status="$4"
  local body="$5"
  local artifact
  artifact="$(write_artifact "$slug" "$body")"
  printf '%s' "$body" | mh_multica surface-report \
    --issue-id "$issue_id" \
    --title "assignment feedback" \
    --status-label "$status" \
    --summary "$summary" \
    --desired-status "$status" \
    --event-ref "accepted:multica-live/${issue_id}/${slug}" \
    --resource-ref "resource:multica-live/${issue_id}" \
    --surface-ref "surface:multica/${issue_id}" \
    --source-artifact-ref "$artifact" \
    --evidence "ctx:evidence-ledger" \
    --evidence "ctx:provider-contract" \
    --artifact "$artifact" \
    --assignee-agent-id "$AGENT_ID" \
    --assigned-to-provider \
    --content-stdin >/dev/null
}

create_carrier() {
  local title="$1"
  local target_principal="$2"
  local slug="$3"
  local body="$4"
  local target_agent
  target_agent="$(agent_id_for "$target_principal")"
  if [[ -z "$target_agent" ]]; then
    echo "missing target agent for ${target_principal}" >&2
    return 1
  fi
  printf '%s' "$body" | mh_multica activation-carrier \
    --issue-id "$ISSUE_ID" \
    --title "$title" \
    --event-ref "accepted:multica-live/${ISSUE_ID}/${slug}" \
    --resource-ref "assignment:${slug}" \
    --target-agent-id "$target_agent" \
    --content-stdin >/dev/null
}

root_case() {
  local text="$TITLE"$'\n'"$DESCRIPTION"
  if grep -qi "parallel poc" <<<"$text"; then
    echo "parallel"
  elif grep -qi "react" <<<"$text"; then
    echo "react"
  elif grep -qi "surface readiness" <<<"$text"; then
    echo "surface"
  else
    echo "generic"
  fi
}

run_child_issue() {
  local slug
  slug="$(safe_segment "$TITLE")"
  if [[ -z "$slug" ]]; then
    slug="child-feedback"
  fi
  local body
  body="中文反馈：

- 执行者: ${PRINCIPAL}
- Issue: ${IDENTIFIER} ${ISSUE_ID}
- 复用上下文: ctx:evidence-ledger, ctx:provider-contract
- 证据产物: ${CASE_ROOT}/evidence/${slug}.md
- 结论: 该子任务已经通过 Multica 原生 runtime 调度进入 mnemon-multica-runtime，并通过显式 surface-report 写回 OA comment/status。
- 边界: 本 comment 是 display-only 写回，不触发新的 provider 执行；执行需要 activation carrier。

mnemon:event=accepted:multica-live/${ISSUE_ID}/${slug}
"
  surface_report "$ISSUE_ID" "$slug" "子任务 ${slug} 完成，已留下共享上下文和证据。" "done" "$body"
}

run_root_issue() {
  local kind="$1"
  case "$kind" in
    parallel)
      create_carrier "poc-runtime-routing" "researcher@team" "poc-runtime-routing" "验证 provider wrapper routing 与 surface correlation。必须引用 ctx:surface-map、ctx:provider-contract、ctx:evidence-ledger。"
      create_carrier "poc-operator-runbook" "implementer@team" "poc-operator-runbook" "验证 operator runbook 与 rollback readiness。必须引用 ctx:provider-contract、ctx:risk-register、ctx:evidence-ledger。"
      create_carrier "poc-release-risk" "reviewer@team" "poc-release-risk" "验证 release risk 与 status writeback。必须引用 ctx:session-map、ctx:risk-register、ctx:evidence-ledger。"
      create_carrier "follow-up-context-reuse" "integrator@team" "follow-up-context-reuse" "第二轮 follow-up：复用至少两个 shared contexts，综合第一轮 PoC 反馈并给出最终风险判断。"
      ;;
    react)
      create_carrier "observe-surface-metadata" "researcher@team" "observe-surface-metadata" "Round 1 Observe：检查 root surface metadata 与 run correlation。"
      create_carrier "observe-provider-routing" "implementer@team" "observe-provider-routing" "Round 1 Observe：检查 provider wrapper run visibility。"
      create_carrier "act-oa-writeback" "reviewer@team" "act-oa-writeback" "Round 2 Act：验证 OA status/comment writeback 是否不触发 provider。"
      create_carrier "reflect-decision" "integrator@team" "reflect-decision" "Round 3 Reflect：整合 observe/act 证据并给出 residual risk。"
      ;;
    surface|generic)
      create_carrier "surface-metadata-check" "researcher@team" "surface-metadata-check" "检查 root surface metadata，不允许 legacy hub/mailbox keys。"
      create_carrier "provider-run-visibility-check" "reviewer@team" "provider-run-visibility-check" "检查 provider wrapper run messages、comments、status 可见性。"
      create_carrier "activation-carrier-follow-up" "integrator@team" "activation-carrier-follow-up" "验证 activation carrier 携带 event_ref/resource_ref 后才触发执行。"
      ;;
  esac

  local root_body
  root_body="根任务中文汇总：

- Root issue: ${IDENTIFIER} ${ISSUE_ID}
- Case: ${kind}
- 已创建 activation carriers，carrier 才负责触发后续 provider 执行。
- 已通过 surface-report 写回 display-only OA 状态，不把 Multica 当 canonical state。
- 复用上下文: ctx:evidence-ledger, ctx:provider-contract, ctx:risk-register
- Provider wrapper: live deterministic wrapper，用于验证真实 Multica/mnemond/OA 链路；产品路径应继续贴近 Multica 原生 provider backend/app-server。

mnemon:event=accepted:multica-live/${ISSUE_ID}/root
"
  surface_report "$ISSUE_ID" "root-${kind}-summary" "根任务已创建多角色 carrier 并写回 R3 surface 证据。" "done" "$root_body"
}

if [[ -n "$PARENT_ID" && "$PARENT_ID" != "null" ]]; then
  run_child_issue
else
  run_root_issue "$(root_case)"
fi

echo "Mnemon Multica runtime handled issue ${IDENTIFIER:-$ISSUE_ID}. Multica surface input: observed. Live provider wrapper completed ${TITLE} for ${PRINCIPAL}."
