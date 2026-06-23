package hostsurface

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

var hookTimings = []string{"prime", "remind", "nudge", "compact"}

const (
	stdinTolerant   = "tolerant"
	stdinStrict     = "strict"
	stdinGrepDirect = "grep-direct"
)

const (
	dialectCodexContinue     = "codex-continue"
	dialectClaudeDecision    = "claude-decision"
	dialectSystemMessageOnly = "system-message-only"
	dialectPlain             = "plain"
)

const (
	slotText  = "text"
	slotOver  = "over"
	slotUnder = "under"
)

type HostMechanics struct {
	StdinRead        MechanicSelection                       `json:"stdin_read"`
	Dialect          MechanicSelection                       `json:"dialect"`
	JSONEscape       bool                                    `json:"json_escape"`
	MarkerOverrides  map[string]map[string]bool              `json:"marker_overrides,omitempty"`
	WordingOverrides map[string]map[string]map[string]string `json:"wording_overrides,omitempty"`
}

type MechanicSelection struct {
	Default   string                       `json:"default"`
	Overrides map[string]map[string]string `json:"overrides,omitempty"`
}

func decodeHostMechanics(hostJSON []byte) (HostMechanics, error) {
	var outer map[string]json.RawMessage
	if err := json.Unmarshal(hostJSON, &outer); err != nil {
		return HostMechanics{}, fmt.Errorf("parse host.json: %w", err)
	}
	raw, ok := outer["mechanics"]
	if !ok {
		return HostMechanics{}, fmt.Errorf("host.json has no mechanics section (required to render hooks)")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var mech HostMechanics
	if err := dec.Decode(&mech); err != nil {
		return HostMechanics{}, fmt.Errorf("parse host.json mechanics: %w", err)
	}
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); err != io.EOF {
		return HostMechanics{}, fmt.Errorf("trailing data after host mechanics (want a single JSON object)")
	}
	if err := validateHostMechanics(mech); err != nil {
		return HostMechanics{}, err
	}
	return mech, nil
}

var markerNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)

func isHookTiming(timing string) bool {
	for _, t := range hookTimings {
		if t == timing {
			return true
		}
	}
	return false
}

func validateHostMechanics(mech HostMechanics) error {
	if !mech.JSONEscape {
		return errors.New("host mechanics: json_escape must be true")
	}
	stdinIdioms := map[string]bool{stdinTolerant: true, stdinStrict: true, stdinGrepDirect: true}
	dialects := map[string]bool{dialectCodexContinue: true, dialectClaudeDecision: true, dialectSystemMessageOnly: true, dialectPlain: true}
	if err := validateMechanicSelection("mechanics.stdin_read", mech.StdinRead, stdinIdioms); err != nil {
		return err
	}
	if err := validateMechanicSelection("mechanics.dialect", mech.Dialect, dialects); err != nil {
		return err
	}
	for loop, byTiming := range mech.MarkerOverrides {
		if !markerNamePattern.MatchString(loop) {
			return fmt.Errorf("mechanics.marker_overrides: invalid loop name %q", loop)
		}
		for timing := range byTiming {
			if !isHookTiming(timing) {
				return fmt.Errorf("mechanics.marker_overrides.%s: unknown timing %q", loop, timing)
			}
		}
	}
	for loop, byTiming := range mech.WordingOverrides {
		if !markerNamePattern.MatchString(loop) {
			return fmt.Errorf("mechanics.wording_overrides: invalid loop name %q", loop)
		}
		for timing, slots := range byTiming {
			if !isHookTiming(timing) {
				return fmt.Errorf("mechanics.wording_overrides.%s: unknown timing %q", loop, timing)
			}
			for slot, text := range slots {
				if slot != slotText && slot != slotOver && slot != slotUnder {
					return fmt.Errorf("mechanics.wording_overrides.%s.%s: unknown slot %q", loop, timing, slot)
				}
				if err := validateSlotText(text, fmt.Sprintf("mechanics.wording_overrides.%s.%s.%s", loop, timing, slot)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validateMechanicSelection(where string, sel MechanicSelection, allowed map[string]bool) error {
	if !allowed[sel.Default] {
		return fmt.Errorf("%s.default: unknown value %q", where, sel.Default)
	}
	for loop, byTiming := range sel.Overrides {
		if !markerNamePattern.MatchString(loop) {
			return fmt.Errorf("%s.overrides: invalid loop name %q", where, loop)
		}
		for timing, value := range byTiming {
			if !isHookTiming(timing) {
				return fmt.Errorf("%s.overrides.%s: unknown timing %q", where, loop, timing)
			}
			if !allowed[value] {
				return fmt.Errorf("%s.overrides.%s.%s: unknown value %q", where, loop, timing, value)
			}
		}
	}
	return nil
}

func validateSlotText(text, where string) error {
	if text == "" {
		return fmt.Errorf("%s: empty text slot", where)
	}
	if strings.ContainsAny(text, "\"`\\\n") || strings.Contains(text, "$(") {
		return fmt.Errorf("%s: text slot contains shell-active characters", where)
	}
	return nil
}

const sessionIDLine = `SESSION_ID="$(printf '%s' "${INPUT}" | sed -n 's/.*"session_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -1)"`

const jsonEscapeFunction = `json_escape() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/\\n}"
  printf '%s' "${value}"
}`
