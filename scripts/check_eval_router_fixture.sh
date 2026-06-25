#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-.}"
RUN_ID="df-rgr-0019-router-fixture-$(date -u +%Y%m%dT%H%M%SZ)"
PROPOSAL_RUN_ID="$(printf '%s' "${RUN_ID}" | tr '[:upper:]' '[:lower:]')"
PROPOSAL_ID="eval-memory-memory-router-failed-finding-${PROPOSAL_RUN_ID}"

if ! go run ./harness/cmd/mnemon-harness eval --help >/dev/null 2>&1; then
  echo "eval-router-check: skipped (mnemon-harness eval command unavailable)"
  exit 0
fi

output="$(
  go run ./harness/cmd/mnemon-harness eval --root "${ROOT}" assert \
    --suite router-fixture \
    --scenario memory-router-failed-finding \
    --run-id "${RUN_ID}" 2>&1
)"
echo "${output}"

if [[ "${output}" != *"eval assert: fail"* ]]; then
  echo "expected assertion-only fixture to produce fail outcome" >&2
  exit 1
fi
if [[ "${output}" != *"proposal: ${PROPOSAL_ID} route=memory status=draft"* ]]; then
  echo "expected memory-route proposal draft in output" >&2
  exit 1
fi

report="${ROOT}/.mnemon/harness/reports/runner/${RUN_ID}-codex-app-server-semantic-run.json"
proposal="${ROOT}/.mnemon/harness/proposals/draft/${PROPOSAL_ID}/proposal.json"

if [[ ! -f "${report}" ]]; then
  echo "missing assertion-only report: ${report}" >&2
  exit 1
fi
if [[ ! -f "${proposal}" ]]; then
  echo "missing proposal draft: ${proposal}" >&2
  exit 1
fi
