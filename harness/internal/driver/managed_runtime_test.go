package driver

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

func TestCodexAppServerTurnClientEmitsActivationTrace(t *testing.T) {
	workspace := t.TempDir()
	fakeCodex := filepath.Join(workspace, "fake-codex")
	if err := os.WriteFile(fakeCodex, []byte(`#!/usr/bin/env bash
while IFS= read -r line; do
  case "$line" in
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":1,"result":{"userAgent":"fake"}}\n'
      ;;
    *'"method":"thread/start"'*)
      printf '{"jsonrpc":"2.0","id":2,"result":{"thread":{"id":"thread-1"}}}\n'
      ;;
    *'"method":"turn/start"'*)
      printf '{"jsonrpc":"2.0","id":3,"result":{"turn":{"id":"turn-1","status":"inProgress"}}}\n'
      printf '{"jsonrpc":"2.0","method":"item/started","params":{"threadId":"thread-1","turnId":"turn-1","item":{"type":"agentMessage","id":"msg-1","text":"","phase":"commentary"}}}\n'
      printf '{"jsonrpc":"2.0","method":"item/agentMessage/delta","params":{"threadId":"thread-1","turnId":"turn-1","itemId":"msg-1","delta":"native trace detail"}}\n'
      printf '{"jsonrpc":"2.0","method":"item/completed","params":{"threadId":"thread-1","turnId":"turn-1","item":{"type":"agentMessage","id":"msg-1","text":"native trace detail","phase":"final_answer"}}}\n'
      printf '{"jsonrpc":"2.0","method":"turn/completed","params":{"threadId":"thread-1","turn":{"id":"turn-1","status":"completed"}}}\n'
      ;;
  esac
done
`), 0o755); err != nil {
		t.Fatal(err)
	}
	var traces []ManagedTurnTraceEvent
	client := CodexAppServerTurnClient{
		Principal:      "planner@team",
		Command:        fakeCodex,
		Workspace:      workspace,
		TurnTimeout:    5 * time.Second,
		RequestTimeout: 5 * time.Second,
	}
	result, err := client.StartTurnWithTrace(context.Background(), ManagedWakeQuery, ManagedTurnTraceSinkFunc(func(event ManagedTurnTraceEvent) {
		traces = append(traces, event)
	}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "completed" || !strings.Contains(result.FinalAnswer, "native trace detail") {
		t.Fatalf("result = %+v", result)
	}
	var sawStart, sawDelta, sawComplete bool
	for _, event := range traces {
		if event.Principal != "planner@team" || event.SourceRuntime != ManagedTurnTraceSourceCodexAppServer {
			t.Fatalf("unexpected trace identity: %+v", event)
		}
		switch event.Method {
		case "item/started":
			sawStart = event.ItemType == "agentMessage"
		case "item/agentMessage/delta":
			sawDelta = event.Text == "native trace detail"
		case "item/completed":
			sawComplete = event.Phase == "final_answer"
		}
	}
	if !sawStart || !sawDelta || !sawComplete {
		t.Fatalf("missing native trace events start=%v delta=%v complete=%v traces=%+v", sawStart, sawDelta, sawComplete, traces)
	}
}

func TestCodexAppServerAdditionalContextRunsStandardHooks(t *testing.T) {
	workspace := t.TempDir()
	hookDir := filepath.Join(workspace, ".codex", "hooks", "mnemon-r1")
	if err := os.MkdirAll(hookDir, 0o755); err != nil {
		t.Fatal(err)
	}
	prime := filepath.Join(hookDir, "prime.sh")
	if err := os.WriteFile(prime, []byte(`#!/usr/bin/env bash
printf '{"systemMessage":"prime guide loaded"}\n'
`), 0o755); err != nil {
		t.Fatal(err)
	}
	remind := filepath.Join(hookDir, "remind.sh")
	if err := os.WriteFile(remind, []byte(`#!/usr/bin/env bash
INPUT="$(cat || true)"
case "${INPUT}" in
  *"[mnemon:wake]"*) printf '{"systemMessage":"remind rendered governed context"}\n' ;;
  *) printf '{"systemMessage":"remind generic"}\n' ;;
esac
`), 0o755); err != nil {
		t.Fatal(err)
	}

	ctx, err := CodexAppServerAdditionalContext(context.Background(), workspace, os.Environ(), ManagedWakeQuery)
	if err != nil {
		t.Fatal(err)
	}
	if len(ctx) != 2 {
		t.Fatalf("additional context keys = %+v, want prime and remind", ctx)
	}
	for key, want := range map[string]string{
		"mnemon.hook.prime":  "prime guide loaded",
		"mnemon.hook.remind": "remind rendered governed context",
	} {
		entry, ok := ctx[key].(map[string]any)
		if !ok {
			t.Fatalf("%s entry = %#v", key, ctx[key])
		}
		if entry["kind"] != "application" || !strings.Contains(entry["value"].(string), want) {
			t.Fatalf("%s entry = %#v, want application context containing %q", key, entry, want)
		}
	}
}

func TestCodexAppServerAdditionalContextSkipsMissingHooks(t *testing.T) {
	ctx, err := CodexAppServerAdditionalContext(context.Background(), t.TempDir(), nil, ManagedWakeQuery)
	if err != nil {
		t.Fatal(err)
	}
	if len(ctx) != 0 {
		t.Fatalf("additional context = %+v, want empty when hooks are absent", ctx)
	}
}
