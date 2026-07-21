package agent

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestClaudeStreamProofRequiresExactManagedHookAndCompletion(t *testing.T) {
	proof := newClaudeStreamProof("/workspace")
	lines := claudeProofLines("/workspace", claudeProofMutation{})
	wakeCount := 0
	for _, line := range lines {
		wake, err := proof.observe(line)
		if err != nil {
			t.Fatalf("observe(%s) = %v", line, err)
		}
		if wake {
			wakeCount++
		}
	}
	if err := proof.validateComplete(true); err != nil || wakeCount != 1 {
		t.Fatalf("complete/wakes = (%v, %d)", err, wakeCount)
	}
	receipt, err := proof.wakeReceipt()
	if err != nil || !strings.Contains(receipt.String(), WakeCue) ||
		!strings.Contains(receipt.String(), `"adapter":"claude-cli"`) {
		t.Fatalf("wakeReceipt() = (%s, %v)", receipt.String(), err)
	}
}

func TestClaudeStreamProofRejectsAuthorityDrift(t *testing.T) {
	tests := []struct {
		name     string
		mutation claudeProofMutation
	}{
		{name: "cue", mutation: claudeProofMutation{wrongCue: true}},
		{name: "session", mutation: claudeProofMutation{wrongSession: true}},
		{name: "tools", mutation: claudeProofMutation{wrongTools: true}},
		{name: "MCP", mutation: claudeProofMutation{unexpectedMCP: true}},
		{name: "permission denial", mutation: claudeProofMutation{permissionDenied: true}},
		{name: "zero provider turn", mutation: claudeProofMutation{zeroTurns: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proof := newClaudeStreamProof("/workspace")
			var observedErr error
			for _, line := range claudeProofLines("/workspace", test.mutation) {
				if _, err := proof.observe(line); err != nil {
					observedErr = err
					break
				}
			}
			if observedErr == nil && proof.validateComplete(true) == nil {
				t.Fatal("authority drift produced a complete proof")
			}
		})
	}
}

type claudeProofMutation struct {
	wrongCue         bool
	wrongSession     bool
	wrongTools       bool
	unexpectedMCP    bool
	permissionDenied bool
	zeroTurns        bool
}

func claudeProofLines(workspace string, mutation claudeProofMutation) [][]byte {
	session := "11111111-1111-4111-8111-111111111111"
	otherSession := session
	if mutation.wrongSession {
		otherSession = "22222222-2222-4222-8222-222222222222"
	}
	hookID := "hook-managed"
	start := map[string]any{"type": "system", "subtype": "hook_started", "hook_id": hookID,
		"hook_name": "UserPromptSubmit", "hook_event": "UserPromptSubmit",
		"uuid": "33333333-3333-4333-8333-333333333333", "session_id": session}
	cue := WakeCue + "\n"
	if mutation.wrongCue {
		cue = "not managed\n"
	}
	response := map[string]any{"type": "system", "subtype": "hook_response", "hook_id": hookID,
		"hook_name": "UserPromptSubmit", "hook_event": "UserPromptSubmit", "output": cue,
		"stdout": cue, "stderr": "", "exit_code": 0, "outcome": "success",
		"uuid": "44444444-4444-4444-8444-444444444444", "session_id": session}
	tools := strings.Split(claudeToolSurface, ",")
	if mutation.wrongTools {
		tools = append(tools, "WebFetch")
	}
	mcp := []any{}
	if mutation.unexpectedMCP {
		mcp = []any{map[string]any{"name": "unexpected"}}
	}
	init := map[string]any{"type": "system", "subtype": "init", "cwd": workspace,
		"session_id": session, "tools": tools, "mcp_servers": mcp,
		"permissionMode": claudePermissionMode, "claude_code_version": "2.1.215",
		"skills": []string{"mnemon-harness"},
		"uuid":   "55555555-5555-4555-8555-555555555555"}
	turns := 1
	if mutation.zeroTurns {
		turns = 0
	}
	denials := []any{}
	if mutation.permissionDenied {
		denials = []any{map[string]any{"tool": "Bash"}}
	}
	result := map[string]any{"type": "result", "subtype": "success", "is_error": false,
		"duration_api_ms": 10, "num_turns": turns, "session_id": otherSession,
		"uuid": "66666666-6666-4666-8666-666666666666", "permission_denials": denials,
		"usage": map[string]any{"input_tokens": 20, "output_tokens": 5}}
	return [][]byte{mustClaudeJSON(start), mustClaudeJSON(response), mustClaudeJSON(init), mustClaudeJSON(result)}
}

func mustClaudeJSON(value any) []byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}
