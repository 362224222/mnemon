package hostagent

import (
	"fmt"
	"io/fs"
	"strings"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

type ThinHookOptions struct {
	Host   string
	Timing string
}

// RenderThinHook renders the generic lifecycle hook. The hook is deliberately business-free: it only
// reads host input, locates the project-local managed guide, emits the generic lifecycle reminder for
// the timing, adapts to the host dialect, and falls back safely.
func RenderThinHook(fsys fs.FS, opts ThinHookOptions) (string, error) {
	if !markerNamePattern.MatchString(opts.Host) {
		return "", fmt.Errorf("invalid host name %q", opts.Host)
	}
	if !isHookTiming(opts.Timing) {
		return "", fmt.Errorf("unknown hook timing %q (closed set: %s)", opts.Timing, strings.Join(hookTimings, "|"))
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
		`GUIDE_PATH="${PROJECT_ROOT}/.mnemon/harness/local/guide.md"`,
		`if [[ -f "${LOCAL_ENV}" ]]; then`,
		`  # shellcheck source=/dev/null`,
		`  source "${LOCAL_ENV}"`,
		`fi`,
	)
	add(hookBodyBlock(opts.Timing))
	switch dialect {
	case dialectPlain:
		add(`printf '%s\n' "${HOOK_BODY}"`)
	case dialectSystemMessageOnly:
		add(jsonEscapeFunction)
		add(
			`SYSTEM_MESSAGE="$(json_escape "${HOOK_BODY}")"`,
			`cat <<JSON`,
			`{`,
			`  "systemMessage": "${SYSTEM_MESSAGE}"`,
			`}`,
			`JSON`,
		)
	case dialectCodexContinue:
		add(jsonEscapeFunction)
		add(
			`SYSTEM_MESSAGE="$(json_escape "${HOOK_BODY}")"`,
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
			`REASON="$(json_escape "${HOOK_BODY}")"`,
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
	return RenderThinHook(assets.FS, ThinHookOptions{Host: host, Timing: timing})
}

func hookBodyBlock(timing string) string {
	switch timing {
	case "prime":
		return strings.Join([]string{
			`if [[ -f "${GUIDE_PATH}" ]]; then`,
			`  HOOK_BODY="$(printf '%s\n\n' "[mnemon] Mnemon integration active. Follow the loaded GUIDE to decide when to read governed context or record durable state."; cat "${GUIDE_PATH}")"`,
			`else`,
			`  HOOK_BODY="[mnemon] Mnemon GUIDE is unavailable; continue only with local context, or retry mnemon setup."`,
			`fi`,
		}, "\n")
	case "remind":
		return `HOOK_BODY="[mnemon] Evaluate whether governed context should be read before responding."`
	case "nudge":
		return `HOOK_BODY="[mnemon] Evaluate whether this turn changed durable state that should be recorded."`
	case "compact":
		return `HOOK_BODY="[mnemon] Before compaction, preserve important durable continuity through mnemon if needed."`
	default:
		return `HOOK_BODY="[mnemon] Evaluate whether governed context or durable state should be handled."`
	}
}
