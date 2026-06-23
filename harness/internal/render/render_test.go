package render

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/projection"
)

func TestRenderCueDeterministicDigestAndAudit(t *testing.T) {
	now := mustTime(t, "2026-06-24T10:00:00Z")
	req := Request{Principal: "codex-a@project", Host: "codex", Lifecycle: "remind", RenderIntent: IntentTeamworkCue}
	proj := projection.Projection{Ref: "proj_head", Digest: "proj_digest", Content: []projection.ResourceContent{
		content("agent_profile", "project", []any{map[string]any{"id": "p1", "actor": "codex-a@project", "freshness": "fresh", "summary": "A profile"}}),
		content("teamwork_signal", "project", []any{map[string]any{"id": "sig1", "statement": "Need a render review"}}),
	}}
	sink := &MemoryAuditSink{}
	r := Renderer{Now: func() time.Time { return now }, AuditSink: sink}

	resp1, err := r.RenderCue(context.Background(), req, proj)
	if err != nil {
		t.Fatal(err)
	}
	resp2, err := r.RenderCue(context.Background(), req, proj)
	if err != nil {
		t.Fatal(err)
	}
	if resp1.Status != StatusOK || resp1.BodyDigest == "" || resp1.BodyDigest != resp2.BodyDigest {
		t.Fatalf("body digest must be stable and non-empty: %#v / %#v", resp1, resp2)
	}
	if !strings.Contains(resp1.Body, "[mnemon:signal]") || strings.Contains(resp1.Body, "[mnemon:profile]") {
		t.Fatalf("expected signal cue and no fresh-profile cue:\n%s", resp1.Body)
	}
	if len(sink.Records) != 2 || sink.Records[0].BodyDigest != resp1.BodyDigest || sink.Records[0].ProjectionDigest != "proj_digest" {
		t.Fatalf("audit records must mirror response digest/projection: %+v", sink.Records)
	}
}

func TestRenderCueScopeAndAssignmentState(t *testing.T) {
	now := mustTime(t, "2026-06-24T10:00:00Z")
	reqB := Request{Principal: "codex-b@project", Host: "codex", Lifecycle: "nudge", RenderIntent: IntentTeamworkCue}
	proj := projection.Projection{Ref: "proj_assign", Digest: "digest_assign", Content: []projection.ResourceContent{
		content("assignment", "project", []any{map[string]any{
			"id": "asg1", "actor": "codex-a@project", "assignee": "codex-b@project",
			"scope": "review render cue", "expected_work": "review render cue",
			"ttl": "30m", "created_at": "2026-06-24T09:45:00Z",
		}}),
		content("memory", "private", []any{map[string]any{"id": "m1", "content": "out-of-scope secret"}}),
	}}
	resp, err := (Renderer{Now: func() time.Time { return now }}).RenderCue(context.Background(), reqB, proj)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.Body, "[mnemon:work]") || !strings.Contains(resp.Body, "[mnemon:feedback]") {
		t.Fatalf("assignee should receive work + feedback cues:\n%s", resp.Body)
	}
	if strings.Contains(resp.Body, "out-of-scope secret") {
		t.Fatalf("render leaked unrelated resource content:\n%s", resp.Body)
	}

	proj.Content = append(proj.Content, content("progress_digest", "project", []any{map[string]any{
		"id": "pg1", "actor": "codex-b@project", "assignment_ref": "asg1", "summary": "review done",
	}}))
	resp, err = (Renderer{Now: func() time.Time { return now }}).RenderCue(context.Background(), reqB, proj)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(resp.Body, "[mnemon:work]") || strings.Contains(resp.Body, "[mnemon:feedback]") {
		t.Fatalf("linked progress should remove assignee work/feedback cue:\n%s", resp.Body)
	}
}

func TestRenderCueExpiredOnlyForOriginator(t *testing.T) {
	now := mustTime(t, "2026-06-24T10:00:00Z")
	proj := projection.Projection{Ref: "proj_expired", Digest: "digest_expired", Content: []projection.ResourceContent{
		content("assignment", "project", []any{map[string]any{
			"id": "asg-exp", "actor": "codex-a@project", "assignee": "codex-b@project",
			"scope": "review overdue work", "expected_work": "review overdue work",
			"ttl": "30m", "created_at": "2026-06-24T09:00:00Z",
		}}),
	}}
	respA, err := (Renderer{Now: func() time.Time { return now }}).RenderCue(context.Background(),
		Request{Principal: "codex-a@project", RenderIntent: IntentTeamworkCue}, proj)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(respA.Body, "[mnemon:expired]") {
		t.Fatalf("originator must see expired cue:\n%s", respA.Body)
	}
	respB, err := (Renderer{Now: func() time.Time { return now }}).RenderCue(context.Background(),
		Request{Principal: "codex-b@project", RenderIntent: IntentTeamworkCue}, proj)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(respB.Body, "[mnemon:expired]") {
		t.Fatalf("assignee must not see originator expired cue:\n%s", respB.Body)
	}
}

func TestMinimalFallbackHasNoDynamicCue(t *testing.T) {
	resp := MinimalFallback(Request{Principal: "codex@project"}, mustTime(t, "2026-06-24T10:00:00Z"))
	if resp.Status != StatusFallback || strings.Contains(resp.Body, "[mnemon:work]") || strings.Contains(resp.Body, "assignment") {
		t.Fatalf("fallback must not contain stale dynamic teamwork cue: %#v", resp)
	}
}

func TestJSONLAuditSinkWritesRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit", "render.jsonl")
	sink := &JSONLAuditSink{Path: path}
	rec := AuditRecord{
		SchemaVersion:    1,
		AuditID:          "render_abc",
		Principal:        "codex@project",
		RenderIntent:     IntentTeamworkCue,
		ProjectionDigest: "proj_digest",
		BodyDigest:       "body_digest",
		Status:           StatusOK,
		CreatedAt:        "2026-06-24T10:00:00Z",
	}
	if err := sink.WriteRenderAudit(context.Background(), rec); err != nil {
		t.Fatalf("write audit: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read audit: %v", err)
	}
	var got AuditRecord
	if err := json.Unmarshal(bytesTrimSpace(raw), &got); err != nil {
		t.Fatalf("audit must be one JSON object per line: %v\n%s", err, raw)
	}
	if got.AuditID != rec.AuditID || got.BodyDigest != rec.BodyDigest {
		t.Fatalf("audit record mismatch: got %+v want %+v", got, rec)
	}
}

func TestRenderIntentsAreBounded(t *testing.T) {
	now := mustTime(t, "2026-06-24T10:00:00Z")
	proj := projection.Projection{Ref: "proj_intent", Digest: "digest_intent", Content: []projection.ResourceContent{
		content("agent_profile", "project", []any{map[string]any{
			"id": "profile-a", "actor": "codex-a@project", "freshness": "stale", "summary": "A stale profile",
		}}),
		content("teamwork_signal", "project", []any{map[string]any{"id": "sig1", "statement": "Need a teammate"}}),
		contentWithFields("memory", "project", map[string]any{"entries": []any{map[string]any{
			"id": "mem1", "content": "render memory note", "source": "user", "confidence": "high",
		}}}),
		contentWithFields("skill", "project", map[string]any{"declarations": []any{map[string]any{
			"skill_id": "review-helper", "name": "review helper", "status": "active",
		}}}),
	}}
	r := Renderer{Now: func() time.Time { return now }}

	profile, err := r.RenderCue(context.Background(), Request{Principal: "codex-a@project", RenderIntent: IntentProfileCue}, proj)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(profile.Body, "[mnemon:profile]") || strings.Contains(profile.Body, "[mnemon:signal]") {
		t.Fatalf("profile.cue must render only profile cue:\n%s", profile.Body)
	}

	packet, err := r.RenderCue(context.Background(), Request{Principal: "codex-a@project", RenderIntent: IntentContextPacket}, proj)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(packet.Body, "[mnemon:context]") ||
		!strings.Contains(packet.Body, "teamwork_signal/sig1") ||
		!strings.Contains(packet.Body, "render memory note") ||
		!strings.Contains(packet.Body, "review helper") {
		t.Fatalf("context.packet must summarize scoped projection:\n%s", packet.Body)
	}

	contract, err := r.RenderCue(context.Background(), Request{Principal: "codex-a@project", RenderIntent: IntentPayloadContract}, proj)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(contract.Body, "[mnemon:payload-contract]") || !strings.Contains(contract.Body, "assignment.write_candidate.observed") {
		t.Fatalf("payload.contract must render governed event contract:\n%s", contract.Body)
	}

	unknown, err := r.RenderCue(context.Background(), Request{Principal: "codex-a@project", RenderIntent: "unknown.intent"}, proj)
	if err != nil {
		t.Fatal(err)
	}
	if unknown.Status != StatusEmpty || strings.TrimSpace(unknown.Body) != "" {
		t.Fatalf("unknown intent must not emit dynamic cue: %#v", unknown)
	}
}

func bytesTrimSpace(in []byte) []byte {
	return []byte(strings.TrimSpace(string(in)))
}

func content(kind, id string, items []any) projection.ResourceContent {
	return contentWithFields(kind, id, map[string]any{"items": items})
}

func contentWithFields(kind, id string, fields map[string]any) projection.ResourceContent {
	return projection.ResourceContent{
		Ref:     contract.ResourceRef{Kind: contract.ResourceKind(kind), ID: contract.ResourceID(id)},
		Version: 1,
		Fields:  fields,
	}
}

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	out, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
