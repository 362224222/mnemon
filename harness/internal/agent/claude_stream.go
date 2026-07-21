package agent

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

const claudeHookOutputMax = 10 << 10

type claudeStreamHeader struct {
	Type      string `json:"type"`
	Subtype   string `json:"subtype"`
	SessionID string `json:"session_id"`
	UUID      string `json:"uuid"`
}

type claudeStreamInit struct {
	Type              string   `json:"type"`
	Subtype           string   `json:"subtype"`
	CWD               string   `json:"cwd"`
	SessionID         string   `json:"session_id"`
	Tools             []string `json:"tools"`
	MCPServers        []any    `json:"mcp_servers"`
	PermissionMode    string   `json:"permissionMode"`
	ClaudeCodeVersion string   `json:"claude_code_version"`
	Skills            []string `json:"skills"`
}

type claudeHookEvent struct {
	Type      string `json:"type"`
	Subtype   string `json:"subtype"`
	HookID    string `json:"hook_id"`
	HookName  string `json:"hook_name"`
	HookEvent string `json:"hook_event"`
	Output    string `json:"output"`
	Stdout    string `json:"stdout"`
	Stderr    string `json:"stderr"`
	ExitCode  *int   `json:"exit_code"`
	Outcome   string `json:"outcome"`
	UUID      string `json:"uuid"`
	SessionID string `json:"session_id"`
}

type claudeStreamResult struct {
	Type          string            `json:"type"`
	Subtype       string            `json:"subtype"`
	IsError       bool              `json:"is_error"`
	DurationAPIMS uint64            `json:"duration_api_ms"`
	NumTurns      uint64            `json:"num_turns"`
	SessionID     string            `json:"session_id"`
	UUID          string            `json:"uuid"`
	Denials       []json.RawMessage `json:"permission_denials"`
	Usage         struct {
		InputTokens  uint64 `json:"input_tokens"`
		OutputTokens uint64 `json:"output_tokens"`
	} `json:"usage"`
}

type claudeObservedHook struct {
	responded bool
	cue       bool
}

type claudeStreamProof struct {
	workspace     string
	sessionID     string
	managedHookID string
	resultUUID    string
	initSeen      bool
	resultSeen    bool
	hooks         map[string]claudeObservedHook
}

func newClaudeStreamProof(workspace string) *claudeStreamProof {
	return &claudeStreamProof{workspace: workspace, hooks: make(map[string]claudeObservedHook)}
}

func (proof *claudeStreamProof) observe(raw []byte) (bool, error) {
	var header claudeStreamHeader
	if err := decodeClaudeStreamJSON(raw, &header); err != nil || header.Type == "" {
		return false, errors.New("Claude stream message is malformed")
	}
	if err := proof.bindMessage(header); err != nil {
		return false, err
	}
	switch {
	case header.Type == "system" && header.Subtype == "hook_started":
		return false, proof.observeHookStart(raw)
	case header.Type == "system" && header.Subtype == "hook_response":
		return proof.observeHookResponse(raw)
	case header.Type == "system" && header.Subtype == "init":
		return false, proof.observeInit(raw)
	case header.Type == "result":
		return false, proof.observeResult(raw)
	default:
		return false, nil
	}
}

func (proof *claudeStreamProof) bindMessage(header claudeStreamHeader) error {
	if !utf8.ValidString(header.Type) || !utf8.ValidString(header.Subtype) ||
		len(header.Type) > 64 || len(header.Subtype) > 128 {
		return errors.New("Claude stream message kind is invalid")
	}
	if header.UUID != "" && !validClaudeUUID(header.UUID) {
		return errors.New("Claude stream UUID is invalid")
	}
	if header.SessionID == "" {
		return nil
	}
	if !validClaudeUUID(header.SessionID) {
		return errors.New("Claude stream session is invalid")
	}
	if proof.sessionID == "" {
		proof.sessionID = header.SessionID
	}
	if proof.sessionID != header.SessionID {
		return errors.New("Claude stream crossed session authority")
	}
	return nil
}

func (proof *claudeStreamProof) observeHookStart(raw []byte) error {
	var event claudeHookEvent
	if err := decodeClaudeStreamJSON(raw, &event); err != nil {
		return errors.New("Claude Hook start is malformed")
	}
	if event.HookEvent != "UserPromptSubmit" {
		return nil
	}
	if !validClaudeHookEnvelope(event) || event.HookID == "" || len(event.HookID) > 128 ||
		len(proof.hooks) >= 64 {
		return errors.New("Claude UserPromptSubmit Hook start is invalid")
	}
	if _, duplicate := proof.hooks[event.HookID]; duplicate {
		return errors.New("Claude UserPromptSubmit Hook start is duplicate")
	}
	proof.hooks[event.HookID] = claudeObservedHook{}
	return nil
}

func (proof *claudeStreamProof) observeHookResponse(raw []byte) (bool, error) {
	var event claudeHookEvent
	if err := decodeClaudeStreamJSON(raw, &event); err != nil {
		return false, errors.New("Claude Hook response is malformed")
	}
	if event.HookEvent != "UserPromptSubmit" {
		return false, nil
	}
	observed, exists := proof.hooks[event.HookID]
	if !exists || observed.responded || !validClaudeHookEnvelope(event) ||
		event.ExitCode == nil || *event.ExitCode != 0 || event.Outcome != "success" ||
		event.Stderr != "" || !boundedClaudeHookText(event) {
		return false, errors.New("Claude UserPromptSubmit Hook response is invalid")
	}
	cue := event.Output == WakeCue+"\n" && event.Stdout == WakeCue+"\n"
	if !cue && (strings.Contains(event.Output, WakeCue) || strings.Contains(event.Stdout, WakeCue)) {
		return false, errors.New("Claude managed Hook cue is ambiguous")
	}
	if cue && proof.managedHookID != "" {
		return false, errors.New("Claude emitted duplicate managed Hook cues")
	}
	observed.responded, observed.cue = true, cue
	proof.hooks[event.HookID] = observed
	if cue {
		proof.managedHookID = event.HookID
	}
	return cue, nil
}

func (proof *claudeStreamProof) observeInit(raw []byte) error {
	if proof.initSeen {
		return errors.New("Claude emitted duplicate initialization")
	}
	var init claudeStreamInit
	if err := decodeClaudeStreamJSON(raw, &init); err != nil ||
		!validClaudeStreamInit(init, proof.workspace) {
		return errors.New("Claude initialization authority differs")
	}
	proof.initSeen = true
	return nil
}

func (proof *claudeStreamProof) observeResult(raw []byte) error {
	if proof.resultSeen {
		return errors.New("Claude emitted duplicate completion")
	}
	var result claudeStreamResult
	if err := decodeClaudeStreamJSON(raw, &result); err != nil ||
		!validClaudeStreamResult(result, proof) {
		return errors.New("Claude completion authority differs")
	}
	proof.resultSeen, proof.resultUUID = true, result.UUID
	return nil
}

func (proof *claudeStreamProof) validateComplete(stderrClean bool) error {
	if proof == nil || !stderrClean || !proof.initSeen || !proof.resultSeen ||
		proof.sessionID == "" || proof.managedHookID == "" || !proof.allHooksResponded() {
		return errors.New("Claude managed turn proof is incomplete")
	}
	return nil
}

func (proof *claudeStreamProof) allHooksResponded() bool {
	if len(proof.hooks) == 0 {
		return false
	}
	for _, hook := range proof.hooks {
		if !hook.responded {
			return false
		}
	}
	return true
}

func (proof *claudeStreamProof) wakeReceipt() (model.JSON, error) {
	if proof == nil || proof.sessionID == "" || proof.managedHookID == "" {
		return model.JSON{}, errors.New("Claude managed Hook proof is incomplete")
	}
	return model.JSONFrom(struct {
		Adapter       string `json:"adapter"`
		Cue           string `json:"cue"`
		EventName     string `json:"event_name"`
		HookID        string `json:"hook_id"`
		HookName      string `json:"hook_name"`
		Outcome       string `json:"outcome"`
		SchemaVersion int    `json:"schema_version"`
		SessionID     string `json:"session_id"`
	}{claudeAdapterName, WakeCue, "UserPromptSubmit", proof.managedHookID,
		"UserPromptSubmit", "success", 1, proof.sessionID})
}

func (proof *claudeStreamProof) completionIDs() (string, string) {
	if proof == nil {
		return "", ""
	}
	return proof.sessionID, proof.managedHookID
}

func validClaudeHookEnvelope(event claudeHookEvent) bool {
	return event.Type == "system" &&
		(event.Subtype == "hook_started" || event.Subtype == "hook_response") &&
		event.HookName == "UserPromptSubmit" && event.HookEvent == "UserPromptSubmit" &&
		validClaudeUUID(event.UUID) && validClaudeUUID(event.SessionID)
}

func boundedClaudeHookText(event claudeHookEvent) bool {
	for _, value := range []string{event.Output, event.Stdout, event.Stderr} {
		if len(value) > claudeHookOutputMax || !utf8.ValidString(value) {
			return false
		}
	}
	return true
}

func validClaudeStreamInit(init claudeStreamInit, workspace string) bool {
	return init.Type == "system" && init.Subtype == "init" && init.CWD == workspace &&
		validClaudeUUID(init.SessionID) && init.PermissionMode == claudePermissionMode &&
		init.ClaudeCodeVersion != "" && len(init.ClaudeCodeVersion) <= 256 &&
		utf8.ValidString(init.ClaudeCodeVersion) && len(init.MCPServers) == 0 &&
		exactClaudeTools(init.Tools) && countClaudeSkill(init.Skills) == 1
}

func exactClaudeTools(tools []string) bool {
	want := strings.Split(claudeToolSurface, ",")
	if len(tools) != len(want) {
		return false
	}
	seen := make(map[string]bool, len(want))
	for _, tool := range tools {
		seen[tool] = true
	}
	for _, tool := range want {
		if !seen[tool] {
			return false
		}
	}
	return true
}

func countClaudeSkill(skills []string) int {
	count := 0
	for _, skill := range skills {
		if skill == "mnemon-harness" {
			count++
		}
	}
	return count
}

func validClaudeStreamResult(result claudeStreamResult, proof *claudeStreamProof) bool {
	return result.Type == "result" && result.Subtype == "success" && !result.IsError &&
		proof.initSeen && proof.managedHookID != "" && proof.allHooksResponded() &&
		result.SessionID == proof.sessionID && validClaudeUUID(result.UUID) &&
		result.DurationAPIMS > 0 && result.NumTurns > 0 && result.NumTurns <= 64 &&
		result.Usage.InputTokens > 0 && result.Usage.OutputTokens > 0 &&
		result.Denials != nil && len(result.Denials) == 0
}

func validClaudeUUID(value string) bool {
	parsed, err := uuid.Parse(value)
	return err == nil && parsed.String() == value
}

func decodeClaudeStreamJSON(raw []byte, target any) error {
	if _, err := model.CanonicalizeJSON(raw); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("Claude stream JSON has trailing data")
	}
	return nil
}
