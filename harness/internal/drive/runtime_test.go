package drive

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

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
