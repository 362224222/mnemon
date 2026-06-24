package hostsurface

import (
	"fmt"
	"io/fs"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
)

type ThinHookOptions struct {
	Host         string
	Timing       string
	RenderIntent string
}

// RenderThinHook renders the R1 static hook shim. It contains only mechanics for reading host input,
// loading Local Mnemon credentials, calling render, adapting the host dialect, and safe fallback.
func RenderThinHook(fsys fs.FS, opts ThinHookOptions) (string, error) {
	if !markerNamePattern.MatchString(opts.Host) {
		return "", fmt.Errorf("invalid host name %q", opts.Host)
	}
	if !isHookTiming(opts.Timing) {
		return "", fmt.Errorf("unknown hook timing %q (closed set: %s)", opts.Timing, strings.Join(hookTimings, "|"))
	}
	if !isRenderIntent(opts.RenderIntent) {
		return "", fmt.Errorf("unknown render intent %q", opts.RenderIntent)
	}
	rawHost, err := fs.ReadFile(fsys, "hosts/"+opts.Host+"/host.json")
	if err != nil {
		return "", fmt.Errorf("read host.json for host %s: %w", opts.Host, err)
	}
	mech, err := decodeHostMechanics(rawHost)
	if err != nil {
		return "", fmt.Errorf("decode host mechanics for host %s: %w", opts.Host, err)
	}
	stdin := mech.StdinRead.Default
	if stdin == "" {
		stdin = stdinTolerant
	}
	dialect := mech.Dialect.Default
	if dialect == "" {
		dialect = dialectPlain
	}

	var blocks []string
	add := func(lines ...string) { blocks = append(blocks, strings.Join(lines, "\n")) }
	add("#!/usr/bin/env bash", "set -euo pipefail")
	if stdin == stdinStrict {
		add(`INPUT="$(cat)"`)
	} else {
		add(`INPUT="$(cat || true)"`)
	}
	add(sessionIDLine)
	add(
		`HOOK_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"`,
		`CONFIG_DIR="$(cd "${HOOK_DIR}/../.." && pwd)"`,
		`PROJECT_ROOT="$(cd "${CONFIG_DIR}/.." && pwd)"`,
		`LOCAL_ENV="${PROJECT_ROOT}/.mnemon/harness/local/env.sh"`,
		`if [[ -f "${LOCAL_ENV}" ]]; then`,
		`  # shellcheck source=/dev/null`,
		`  source "${LOCAL_ENV}"`,
		`fi`,
	)
	add(
		`HARNESS_BIN="${MNEMON_HARNESS_BIN:-mnemon-harness}"`,
		`CONTROL_ADDR="${MNEMON_CONTROL_ADDR:-http://127.0.0.1:8787}"`,
		`CONTROL_PRINCIPAL="${MNEMON_CONTROL_PRINCIPAL:-}"`,
		`TOKEN_ARGS=()`,
		`if [[ -n "${MNEMON_CONTROL_TOKEN_FILE:-}" ]]; then`,
		`  TOKEN_PATH="${MNEMON_CONTROL_TOKEN_FILE}"`,
		`  if [[ "${TOKEN_PATH}" != /* ]]; then`,
		`    TOKEN_PATH="${PROJECT_ROOT}/${TOKEN_PATH}"`,
		`  fi`,
		`  TOKEN_ARGS=(--token-file "${TOKEN_PATH}")`,
		`fi`,
	)
	add(
		`FALLBACK_BODY="mnemon is temporarily unavailable; continue only with local context, or retry mnemon status."`,
		`if command -v "${HARNESS_BIN}" >/dev/null 2>&1; then`,
		`  if RENDER_BODY="$("${HARNESS_BIN}" control render \`,
		`      --addr "${CONTROL_ADDR}" \`,
		`      --principal "${CONTROL_PRINCIPAL}" \`,
		`      ${TOKEN_ARGS[@]+"${TOKEN_ARGS[@]}"} \`,
		`      --intent "`+opts.RenderIntent+`" \`,
		`      --lifecycle "`+opts.Timing+`" \`,
		`      --surface "hook" 2>/dev/null)"; then`,
		`    if [[ -z "${RENDER_BODY}" ]]; then`,
		`      exit 0`,
		`    fi`,
		`  else`,
		`    RENDER_BODY="${FALLBACK_BODY}"`,
		`  fi`,
		`else`,
		`  RENDER_BODY="${FALLBACK_BODY}"`,
		`fi`,
	)
	switch dialect {
	case dialectPlain:
		add(`printf '%s\n' "${RENDER_BODY}"`)
	case dialectSystemMessageOnly:
		add(jsonEscapeFunction)
		add(
			`SYSTEM_MESSAGE="$(json_escape "${RENDER_BODY}")"`,
			`cat <<JSON`,
			`{`,
			`  "systemMessage": "${SYSTEM_MESSAGE}"`,
			`}`,
			`JSON`,
		)
	case dialectCodexContinue:
		add(jsonEscapeFunction)
		add(
			`SYSTEM_MESSAGE="$(json_escape "${RENDER_BODY}")"`,
			`cat <<JSON`,
			`{`,
			`  "continue": false,`,
			`  "stopReason": "${SYSTEM_MESSAGE}",`,
			`  "systemMessage": "${SYSTEM_MESSAGE}"`,
			`}`,
			`JSON`,
		)
	case dialectClaudeDecision:
		add(jsonEscapeFunction)
		add(
			`REASON="$(json_escape "${RENDER_BODY}")"`,
			`cat <<JSON`,
			`{`,
			`  "decision": "block",`,
			`  "reason": "${REASON}"`,
			`}`,
			`JSON`,
		)
	default:
		return "", fmt.Errorf("%s/%s: unsupported thin hook dialect %q", opts.Host, opts.Timing, dialect)
	}
	return strings.Join(blocks, "\n\n") + "\n", nil
}

func RenderStandardThinHook(host, timing string) (string, error) {
	return RenderThinHook(assets.FS, ThinHookOptions{Host: host, Timing: timing, RenderIntent: presentation.IntentTeamworkEvents})
}

func isRenderIntent(intent string) bool {
	switch intent {
	case presentation.IntentSkillBootstrap, presentation.IntentContextPacket, presentation.IntentProfileEvents, presentation.IntentTeamworkEvents, presentation.IntentPayloadContract:
		return true
	default:
		return false
	}
}
