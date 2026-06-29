package drive

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
)

func TestManagedWakeCandidatesFromRenderCarryAudit(t *testing.T) {
	resp := presentation.Response{
		AuditID:    "render_audit_1",
		BodyDigest: "sha256:render",
		Events: []eventmodel.EventEnvelope{eventmodel.DerivedEnvelope(eventmodel.Event{
			SchemaVersion: eventmodel.SchemaVersion,
			ID:            "derived:assignment.work_available:assignment/asg1:codex-a@project",
			Type:          "assignment.work_available",
			Subject:       "assignment/asg1",
			Actor:         "mnemond@local",
			Audience:      "codex-a@project",
			Payload:       eventmodel.BuildPayload(nil, map[string]any{"body": "Assignment asg1 is yours."}, nil),
		}, "2026-06-24T10:00:00Z", "2026-06-24T10:05:00Z", "work", nil)},
	}
	candidates := ManagedWakeCandidatesFromRender("codex-a@project", resp)
	if len(candidates) != 1 {
		t.Fatalf("candidates = %+v, want one", candidates)
	}
	if candidates[0].RenderAuditID != resp.AuditID || candidates[0].RenderBodyDigest != resp.BodyDigest {
		t.Fatalf("candidate should carry render audit metadata: %+v", candidates[0])
	}
}

func TestFileManagedWakeLedgerPersistsSeen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wake-ledger.jsonl")
	candidate := ManagedWakeCandidate{Principal: "codex-a@project", DerivedEventID: "d1", BodyDigest: "sha256:x"}
	ledger := NewFileManagedWakeLedger(path)
	if ledger.Seen(candidate) {
		t.Fatal("fresh ledger should not have seen candidate")
	}
	if err := ledger.Record(ManagedWakeRecord{Principal: candidate.Principal, DerivedEventID: candidate.DerivedEventID, BodyDigest: candidate.BodyDigest, Status: "completed"}); err != nil {
		t.Fatal(err)
	}
	reopened := NewFileManagedWakeLedger(path)
	if !reopened.Seen(candidate) {
		t.Fatal("reopened ledger should remember candidate")
	}
}

func TestFileManagedWakeLedgerAllowsRetryAfterFailed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wake-ledger.jsonl")
	candidate := ManagedWakeCandidate{Principal: "codex-a@project", DerivedEventID: "d1", BodyDigest: "sha256:x"}
	ledger := NewFileManagedWakeLedger(path)
	if err := ledger.Record(ManagedWakeRecord{
		Principal:      candidate.Principal,
		DerivedEventID: candidate.DerivedEventID,
		BodyDigest:     candidate.BodyDigest,
		Status:         "failed",
		Error:          "timeout",
	}); err != nil {
		t.Fatal(err)
	}
	if ledger.Seen(candidate) {
		t.Fatal("failed wake must remain retryable")
	}
	reopened := NewFileManagedWakeLedger(path)
	if reopened.Seen(candidate) {
		t.Fatal("failed wake must remain retryable after reload")
	}
	if err := reopened.Record(ManagedWakeRecord{
		Principal:      candidate.Principal,
		DerivedEventID: candidate.DerivedEventID,
		BodyDigest:     candidate.BodyDigest,
		Status:         "completed",
	}); err != nil {
		t.Fatal(err)
	}
	if !reopened.Seen(candidate) {
		t.Fatal("completed retry should mark candidate handled")
	}
}

func TestHTTPRenderClientUsesBearerAndDecodesResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token-1" {
			t.Fatalf("Authorization = %q, want bearer token", got)
		}
		_ = json.NewEncoder(w).Encode(presentation.Response{SchemaVersion: 1, Status: presentation.StatusOK, AuditID: "audit-1"})
	}))
	defer server.Close()
	resp, err := (HTTPRenderClient{BaseURL: server.URL, Token: "token-1"}).Render(context.Background(), presentation.Request{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.AuditID != "audit-1" {
		t.Fatalf("render response = %+v", resp)
	}
}

func TestCodexAppServerTurnClientRejectsContextQuery(t *testing.T) {
	client := CodexAppServerTurnClient{Command: "definitely-not-run"}
	if _, err := client.StartTurn(context.Background(), "assignment asg1"); err == nil {
		t.Fatal("codex appserver client must reject non-sentinel queries before starting a process")
	}
}

func TestCodexAppServerTurnClientInjectsHookContextAsDeveloperInstructions(t *testing.T) {
	workspace := t.TempDir()
	requestLog := filepath.Join(workspace, "requests.jsonl")
	fakeCodex := filepath.Join(workspace, "fake-codex")
	if err := os.WriteFile(fakeCodex, []byte(`#!/usr/bin/env bash
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$CODEX_REQUEST_LOG"
  case "$line" in
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":1,"result":{"userAgent":"fake"}}\n'
      ;;
    *'"method":"thread/start"'*)
      printf '{"jsonrpc":"2.0","id":2,"result":{"thread":{"id":"thread-1"}}}\n'
      ;;
    *'"method":"turn/start"'*)
      printf '{"jsonrpc":"2.0","id":3,"result":{"turn":{"id":"turn-1","status":"inProgress"}}}\n'
      printf '{"jsonrpc":"2.0","method":"item/completed","params":{"threadId":"thread-1","turnId":"turn-1","item":{"type":"agentMessage","id":"msg-1","text":"handled wake","phase":"final_answer"}}}\n'
      printf '{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed"}}}\n'
      ;;
  esac
done
`), 0o755); err != nil {
		t.Fatal(err)
	}
	hookDir := filepath.Join(workspace, ".codex", "hooks", "mnemon-r1")
	if err := os.MkdirAll(hookDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hookDir, "prime.sh"), []byte(`#!/usr/bin/env bash
printf '{"systemMessage":"prime guide loaded"}\n'
`), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hookDir, "remind.sh"), []byte(`#!/usr/bin/env bash
INPUT="$(cat || true)"
case "${INPUT}" in
  *"[mnemon:wake]"*) printf '{"systemMessage":"remind rendered governed context"}\n' ;;
  *) printf '{"systemMessage":"remind generic"}\n' ;;
esac
`), 0o755); err != nil {
		t.Fatal(err)
	}

	client := CodexAppServerTurnClient{
		Command:        fakeCodex,
		Workspace:      workspace,
		Env:            append(os.Environ(), "CODEX_REQUEST_LOG="+requestLog),
		TurnTimeout:    5 * time.Second,
		RequestTimeout: 5 * time.Second,
	}
	result, err := client.StartTurn(context.Background(), ManagedWakeQuery)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || !strings.Contains(result.FinalAnswer, "handled wake") {
		t.Fatalf("result = %+v", result)
	}

	raw, err := os.ReadFile(requestLog)
	if err != nil {
		t.Fatal(err)
	}
	var threadStart, turnStart map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var msg map[string]any
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			t.Fatalf("decode request %q: %v", line, err)
		}
		switch msg["method"] {
		case "thread/start":
			threadStart = msg
		case "turn/start":
			turnStart = msg
		}
	}
	if threadStart == nil || turnStart == nil {
		t.Fatalf("missing thread/start or turn/start request:\n%s", raw)
	}
	threadParams := threadStart["params"].(map[string]any)
	instructions, _ := threadParams["developerInstructions"].(string)
	for _, want := range []string{"Mnemon managed wake context", "prime guide loaded", "remind rendered governed context", "assignment or self-assignment may be useful"} {
		if !strings.Contains(instructions, want) {
			t.Fatalf("developerInstructions missing %q:\n%s", want, instructions)
		}
	}
	if _, ok := threadParams["additionalContext"]; ok {
		t.Fatalf("thread/start must not use removed additionalContext field: %+v", threadParams)
	}
	turnParams := turnStart["params"].(map[string]any)
	if _, ok := turnParams["additionalContext"]; ok {
		t.Fatalf("turn/start must not use removed additionalContext field: %+v", turnParams)
	}
	input := turnParams["input"].([]any)
	first := input[0].(map[string]any)
	if first["text"] != ManagedWakeQuery {
		t.Fatalf("turn input = %+v, want sentinel only", first)
	}
}
