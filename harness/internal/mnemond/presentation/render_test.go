package presentation

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
)

func TestRenderPresentationDeterministicDigestAndAudit(t *testing.T) {
	now := mustTime(t, "2026-06-24T10:00:00Z")
	req := Request{Principal: "codex-a@project", Host: "codex", Lifecycle: "remind", RenderIntent: IntentTeamworkEvents}
	proj := view.View{Ref: "proj_head", Digest: "proj_digest", Content: []view.ResourceContent{
		content("agent_profile", "project", []any{map[string]any{"id": "p1", "actor": "codex-a@project", "freshness": "fresh", "summary": "A profile"}}),
		content("teamwork_signal", "project", []any{map[string]any{"id": "sig1", "statement": "Need a render review"}}),
	}}
	sink := &MemoryAuditSink{}
	r := Renderer{Now: func() time.Time { return now }, AuditSink: sink}

	resp1, err := r.RenderPresentation(context.Background(), req, proj)
	if err != nil {
		t.Fatal(err)
	}
	resp2, err := r.RenderPresentation(context.Background(), req, proj)
	if err != nil {
		t.Fatal(err)
	}
	if resp1.Status != StatusOK || resp1.BodyDigest == "" || resp1.BodyDigest != resp2.BodyDigest {
		t.Fatalf("body digest must be stable and non-empty: %#v / %#v", resp1, resp2)
	}
	if !strings.Contains(resp1.Body, "[mnemon:signal]") || strings.Contains(resp1.Body, "[mnemon:profile]") {
		t.Fatalf("expected signal presentation and no fresh-profile presentation:\n%s", resp1.Body)
	}
	if len(resp1.Events) != 1 {
		t.Fatalf("response should expose derived event envelopes for rule consumers: %+v", resp1.Events)
	}
	if err := resp1.Events[0].Validate(); err != nil {
		t.Fatalf("response event envelope must validate: %v", err)
	}
	if _, ok := resp1.Events[0].Event.Payload["body"]; ok {
		t.Fatalf("derived event payload must not keep flat body key: %+v", resp1.Events[0].Event.Payload)
	}
	if body, _ := eventmodel.PayloadNarrative(resp1.Events[0].Event.Payload)["body"].(string); body == "" {
		t.Fatalf("derived event narrative must carry hook-facing body text: %+v", resp1.Events[0].Event.Payload)
	}
	if len(sink.Records) != 2 || sink.Records[0].BodyDigest != resp1.BodyDigest || sink.Records[0].PresentationViewDigest != "proj_digest" {
		t.Fatalf("audit records must mirror response digest/presentation-view: %+v", sink.Records)
	}
	if sink.Records[0].EventCounts[DerivedEventTeamworkSignalOpen] != 1 || sink.Records[0].PresentationCounts["signal"] != 1 {
		t.Fatalf("audit must record derived-event and presentation counts: %+v", sink.Records[0])
	}
}

func TestTeamworkSignalPresentationCarriesContextRefs(t *testing.T) {
	now := mustTime(t, "2026-06-24T10:00:00Z")
	req := Request{Principal: "codex-a@project", Host: "codex", Lifecycle: "remind", RenderIntent: IntentTeamworkEvents}
	proj := view.View{Ref: "proj_signal_refs", Digest: "digest_signal_refs", Content: []view.ResourceContent{
		content("agent_profile", "project", []any{map[string]any{"id": "p1", "actor": "codex-a@project", "freshness": "fresh", "summary": "A profile"}}),
		content("teamwork_signal", "project", []any{map[string]any{
			"id":           "sig1",
			"statement":    "Run the shared readiness drill",
			"context_refs": []any{"multica:issue:issue-123"},
		}}),
	}}
	resp, err := (Renderer{Now: func() time.Time { return now }}).RenderPresentation(context.Background(), req, proj)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.Body, "Context refs: multica:issue:issue-123") {
		t.Fatalf("signal presentation must carry stable context refs for wake matching:\n%s", resp.Body)
	}
	if len(resp.Events) != 1 {
		t.Fatalf("expected one signal event: %+v", resp.Events)
	}
	body, _ := eventmodel.PayloadNarrative(resp.Events[0].Event.Payload)["body"].(string)
	if !strings.Contains(body, "multica:issue:issue-123") {
		t.Fatalf("derived event body must carry stable context refs: %q", body)
	}
}

func TestMulticaTeamworkSignalPresentationKeepsHandoffInMnemonProtocol(t *testing.T) {
	now := mustTime(t, "2026-06-24T10:00:00Z")
	proj := view.View{Ref: "proj_multica_signal", Digest: "digest_multica_signal", Content: []view.ResourceContent{
		content("agent_profile", "project", []any{map[string]any{"id": "p1", "actor": "planner@team", "freshness": "fresh", "summary": "A profile"}}),
		content("teamwork_signal", "project", []any{map[string]any{
			"id":        "sig1",
			"statement": "Validate the Multica hub flow",
		}}),
	}}
	resp, err := (Renderer{Now: func() time.Time { return now }}).RenderPresentation(context.Background(),
		Request{Principal: "planner@team", Host: "multica", Lifecycle: "remind", RenderIntent: IntentTeamworkEvents}, proj)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"For Multica-backed teamwork",
		"Mnemon assignment events",
		"runtime projects Multica assignment mailboxes",
	} {
		if !strings.Contains(resp.Body, want) {
			t.Fatalf("Multica teamwork cue missing %q:\n%s", want, resp.Body)
		}
	}

	plain, err := (Renderer{Now: func() time.Time { return now }}).RenderPresentation(context.Background(),
		Request{Principal: "planner@team", Host: "codex", Lifecycle: "remind", RenderIntent: IntentTeamworkEvents}, proj)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plain.Body, "Multica-backed teamwork") {
		t.Fatalf("non-Multica cue should stay host-neutral:\n%s", plain.Body)
	}
}

func TestMulticaTeamworkPresentationFiltersOtherSessions(t *testing.T) {
	now := mustTime(t, "2026-06-24T10:00:00Z")
	proj := view.View{Ref: "proj_multica_scope", Digest: "digest_multica_scope", Content: []view.ResourceContent{
		content("agent_profile", "project", []any{map[string]any{"id": "p1", "actor": "planner@team", "freshness": "fresh", "summary": "A profile"}}),
		content("teamwork_signal", "project", []any{
			map[string]any{
				"id":            "sig-current",
				"statement":     "Current Multica validation",
				"session_id":    "multica:session:root-current",
				"root_issue_id": "root-current",
			},
		}),
		content("assignment", "project", []any{
			map[string]any{
				"id":            "asg-stale",
				"actor":         "planner@team",
				"assignee":      "planner@team",
				"scope":         "old Multica validation",
				"expected_work": "work on stale child issue",
				"session_id":    "multica:session:root-stale",
				"root_issue_id": "root-stale",
				"ttl":           "30m",
				"created_at":    "2026-06-24T09:45:00Z",
			},
		}),
	}}
	resp, err := (Renderer{Now: func() time.Time { return now }}).RenderPresentation(context.Background(),
		Request{
			Principal:    "planner@team",
			Host:         "multica",
			Lifecycle:    "remind",
			RenderIntent: IntentTeamworkEvents,
			SessionID:    "multica:session:root-current",
			InputDigest:  "root-current",
		}, proj)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.Body, "Current Multica validation") {
		t.Fatalf("current signal should remain visible:\n%s", resp.Body)
	}
	if strings.Contains(resp.Body, "stale child issue") || strings.Contains(resp.Body, "asg-stale") {
		t.Fatalf("Multica render must not leak stale session assignment:\n%s", resp.Body)
	}
}

func TestMulticaAssignmentMailboxPresentationKeepsCurrentAssignment(t *testing.T) {
	now := mustTime(t, "2026-06-24T10:00:00Z")
	proj := view.View{Ref: "proj_multica_assignment_scope", Digest: "digest_multica_assignment_scope", Content: []view.ResourceContent{
		content("assignment", "project", []any{
			map[string]any{
				"id":            "asg-current",
				"actor":         "planner@team",
				"assignee":      "worker@team",
				"scope":         "current mailbox work",
				"expected_work": "inspect current mailbox",
				"ttl":           "30m",
				"created_at":    "2026-06-24T09:45:00Z",
			},
			map[string]any{
				"id":            "asg-stale",
				"actor":         "planner@team",
				"assignee":      "worker@team",
				"scope":         "stale mailbox work",
				"expected_work": "inspect stale mailbox",
				"ttl":           "30m",
				"created_at":    "2026-06-24T09:45:00Z",
			},
		}),
	}}
	resp, err := (Renderer{Now: func() time.Time { return now }}).RenderPresentation(context.Background(),
		Request{
			Principal:    "worker@team",
			Host:         "multica",
			Lifecycle:    "remind",
			RenderIntent: IntentTeamworkEvents,
			SessionID:    "multica:session:root-current",
			InputDigest:  "asg-current",
		}, proj)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.Body, "inspect current mailbox") {
		t.Fatalf("current assignment should remain visible:\n%s", resp.Body)
	}
	if strings.Contains(resp.Body, "inspect stale mailbox") || strings.Contains(resp.Body, "asg-stale") {
		t.Fatalf("Multica assignment render must not leak stale assignment:\n%s", resp.Body)
	}
}

func TestRenderPresentationScopeAndAssignmentState(t *testing.T) {
	now := mustTime(t, "2026-06-24T10:00:00Z")
	reqB := Request{Principal: "codex-b@project", Host: "codex", Lifecycle: "nudge", RenderIntent: IntentTeamworkEvents}
	proj := view.View{Ref: "proj_assign", Digest: "digest_assign", Content: []view.ResourceContent{
		content("assignment", "project", []any{map[string]any{
			"id": "asg1", "actor": "codex-a@project", "assignee": "codex-b@project",
			"scope": "review render presentation", "expected_work": "review render presentation",
			"ttl": "30m", "created_at": "2026-06-24T09:45:00Z",
		}}),
		content("memory", "private", []any{map[string]any{"id": "m1", "content": "out-of-scope secret"}}),
	}}
	resp, err := (Renderer{Now: func() time.Time { return now }}).RenderPresentation(context.Background(), reqB, proj)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resp.Body, "[mnemon:work]") || !strings.Contains(resp.Body, "[mnemon:feedback]") {
		t.Fatalf("assignee should receive work + feedback presentations:\n%s", resp.Body)
	}
	if strings.Contains(resp.Body, "out-of-scope secret") {
		t.Fatalf("render leaked unrelated resource content:\n%s", resp.Body)
	}

	proj.Content = append(proj.Content, content("progress_digest", "project", []any{map[string]any{
		"id": "pg1", "actor": "codex-b@project", "assignment_ref": "asg1", "summary": "review done",
	}}))
	resp, err = (Renderer{Now: func() time.Time { return now }}).RenderPresentation(context.Background(), reqB, proj)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(resp.Body, "[mnemon:work]") || strings.Contains(resp.Body, "[mnemon:feedback]") {
		t.Fatalf("linked progress should remove assignee work/feedback presentation:\n%s", resp.Body)
	}
}

func TestDeriveEventEnvelopesSeparateEventModelFromPresentation(t *testing.T) {
	now := mustTime(t, "2026-06-24T10:00:00Z")
	reqB := Request{Principal: "codex-b@project", Host: "codex", Lifecycle: "nudge", RenderIntent: IntentTeamworkEvents}
	proj := view.View{Ref: "proj_assign", Digest: "digest_assign", Content: []view.ResourceContent{
		content("assignment", "project", []any{map[string]any{
			"id": "asg1", "actor": "codex-a@project", "assignee": "codex-b@project",
			"scope": "review render presentation", "expected_work": "review render presentation",
			"ttl": "30m", "created_at": "2026-06-24T09:45:00Z",
		}}),
	}}

	envelopes := DeriveEventEnvelopes(reqB, proj, now)
	if len(envelopes) != 2 {
		t.Fatalf("expected work/feedback derived envelopes, got %+v", envelopes)
	}
	got := map[string]eventmodel.EventEnvelope{}
	for _, env := range envelopes {
		if env.Phase != "derived" {
			t.Fatalf("read-side events must be derived envelopes, got %+v", env)
		}
		if err := env.Validate(); err != nil {
			t.Fatalf("derived envelope must validate: %v", err)
		}
		if _, ok := env.Event.Payload["body"]; ok {
			t.Fatalf("derived envelope must not contain flat presentation body: %+v", env.Event.Payload)
		}
		body, _ := eventmodel.PayloadNarrative(env.Event.Payload)["body"].(string)
		if body == "" {
			t.Fatalf("derived envelope must carry natural language body in payload.narrative: %+v", env.Event.Payload)
		}
		if strings.Contains(env.Event.Type, "mnemon:") || strings.Contains(body, "[mnemon:") {
			t.Fatalf("derived envelope must not contain presentation labels: %+v", env)
		}
		if env.Meta["presentation_hint"] == "" {
			t.Fatalf("derived envelope must keep presentation hint in meta: %+v", env.Meta)
		}
		got[env.Event.Type] = env
	}

	work := got[DerivedEventAssignmentWorkAvailable]
	if work.Event.Subject != "assignment/asg1" {
		t.Fatalf("work event must point at assignment subject: %+v", work)
	}
	feedback := got[DerivedEventAssignmentFeedbackNeeded]
	suggested := feedback.Meta["suggested_event_types"].([]string)
	if suggested[0] != "progress_digest.write_candidate.observed" {
		t.Fatalf("feedback event should name the next observed event: %+v", feedback)
	}

	body := PresentEventEnvelopes(envelopes)
	if !strings.Contains(body, "[mnemon:work]") || !strings.Contains(body, "[mnemon:feedback]") {
		t.Fatalf("presentation should retain current hook-facing labels:\n%s", body)
	}
}

func TestProfileCuePolicyFollowsLifecycle(t *testing.T) {
	now := mustTime(t, "2026-06-24T10:00:00Z")
	proj := view.View{Ref: "proj_profile", Digest: "digest_profile", Content: []view.ResourceContent{
		content("teamwork_signal", "project", []any{map[string]any{"id": "sig1", "statement": "Need teammate context"}}),
	}}

	prime := DeriveEventEnvelopes(Request{Principal: "codex-a@project", Lifecycle: "prime", RenderIntent: IntentTeamworkEvents}, proj, now)
	if _, ok := eventByType(prime, DerivedEventProfileUpdateRequested); !ok {
		t.Fatalf("prime with missing profile should render a bounded profile cue: %+v", prime)
	}

	remind := DeriveEventEnvelopes(Request{Principal: "codex-a@project", Lifecycle: "remind", RenderIntent: IntentTeamworkEvents}, proj, now)
	if _, ok := eventByType(remind, DerivedEventProfileUpdateRequested); !ok {
		t.Fatalf("remind with open teamwork signal should render a contextual profile cue: %+v", remind)
	}

	workOnly := view.View{Ref: "proj_work", Digest: "digest_work", Content: []view.ResourceContent{
		content("assignment", "project", []any{map[string]any{
			"id": "asg1", "actor": "codex-b@project", "assignee": "codex-a@project",
			"scope": "review render presentation", "expected_work": "review render presentation",
			"ttl": "30m", "created_at": "2026-06-24T09:45:00Z",
		}}),
	}}
	nudge := DeriveEventEnvelopes(Request{Principal: "codex-a@project", Lifecycle: "nudge", RenderIntent: IntentTeamworkEvents}, workOnly, now)
	if _, ok := eventByType(nudge, DerivedEventProfileUpdateRequested); ok {
		t.Fatalf("nudge should not render profile cue merely because profile is missing: %+v", nudge)
	}

	changed := view.View{Ref: "proj_changed", Digest: "digest_changed", Content: []view.ResourceContent{
		content("progress_digest", "project", []any{map[string]any{
			"id": "pg1", "actor": "codex-a@project", "feedback_kind": "progress",
			"changed_context": []any{"learned managed wake constraint"},
		}}),
	}}
	nudgeChanged := DeriveEventEnvelopes(Request{Principal: "codex-a@project", Lifecycle: "nudge", RenderIntent: IntentTeamworkEvents}, changed, now)
	if _, ok := eventByType(nudgeChanged, DerivedEventProfileUpdateRequested); !ok {
		t.Fatalf("nudge with structured changed_context should render profile cue: %+v", nudgeChanged)
	}
}

func TestDerivedCueTextAvoidsForcedActionWording(t *testing.T) {
	now := mustTime(t, "2026-06-24T10:00:00Z")
	proj := view.View{Ref: "proj_cues", Digest: "digest_cues", Content: []view.ResourceContent{
		content("teamwork_signal", "project", []any{map[string]any{"id": "sig1", "statement": "Need teammate context"}}),
		content("assignment", "project", []any{map[string]any{
			"id": "asg-exp", "actor": "codex-a@project", "assignee": "codex-b@project",
			"scope": "review overdue work", "expected_work": "review overdue work",
			"ttl": "30m", "created_at": "2026-06-24T09:00:00Z",
		}}),
	}}
	body := PresentEventEnvelopes(DeriveEventEnvelopes(Request{Principal: "codex-a@project", Lifecycle: "remind", RenderIntent: IntentTeamworkEvents}, proj, now))
	for _, forced := range []string{"Update your agent_profile", "Decide whether", "Start a new act", "emit progress_digest"} {
		if strings.Contains(body, forced) {
			t.Fatalf("derived cue body should not force action with %q:\n%s", forced, body)
		}
	}
}

func TestRenderPresentationExpiredOnlyForOriginator(t *testing.T) {
	now := mustTime(t, "2026-06-24T10:00:00Z")
	proj := view.View{Ref: "proj_expired", Digest: "digest_expired", Content: []view.ResourceContent{
		content("assignment", "project", []any{map[string]any{
			"id": "asg-exp", "actor": "codex-a@project", "assignee": "codex-b@project",
			"scope": "review overdue work", "expected_work": "review overdue work",
			"ttl": "30m", "created_at": "2026-06-24T09:00:00Z",
		}}),
	}}
	respA, err := (Renderer{Now: func() time.Time { return now }}).RenderPresentation(context.Background(),
		Request{Principal: "codex-a@project", RenderIntent: IntentTeamworkEvents}, proj)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(respA.Body, "[mnemon:expired]") {
		t.Fatalf("originator must see expired presentation:\n%s", respA.Body)
	}
	respB, err := (Renderer{Now: func() time.Time { return now }}).RenderPresentation(context.Background(),
		Request{Principal: "codex-b@project", RenderIntent: IntentTeamworkEvents}, proj)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(respB.Body, "[mnemon:expired]") {
		t.Fatalf("assignee must not see originator expired presentation:\n%s", respB.Body)
	}
}

func TestMinimalFallbackHasNoDynamicPresentation(t *testing.T) {
	resp := MinimalFallback(Request{Principal: "codex@project"}, mustTime(t, "2026-06-24T10:00:00Z"))
	if resp.Status != StatusFallback || strings.Contains(resp.Body, "[mnemon:work]") || strings.Contains(resp.Body, "assignment") {
		t.Fatalf("fallback must not contain stale dynamic teamwork presentation: %#v", resp)
	}
}

func TestJSONLAuditSinkWritesRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit", "presentation.jsonl")
	sink := &JSONLAuditSink{Path: path}
	rec := AuditRecord{
		SchemaVersion:          1,
		AuditID:                "render_abc",
		Principal:              "codex@project",
		RenderIntent:           IntentTeamworkEvents,
		PresentationViewDigest: "proj_digest",
		BodyDigest:             "body_digest",
		Status:                 StatusOK,
		CreatedAt:              "2026-06-24T10:00:00Z",
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
	proj := view.View{Ref: "proj_intent", Digest: "digest_intent", Content: []view.ResourceContent{
		content("agent_profile", "project", []any{map[string]any{
			"id": "profile-a", "actor": "codex-a@project", "freshness": "stale", "summary": "A stale profile",
		}}),
		content("teamwork_signal", "project", []any{map[string]any{"id": "sig1", "statement": "Need a teammate"}}),
		contentWithFields("progress_digest", "project", map[string]any{"items": []any{map[string]any{
			"id": "progress1", "summary": "render progress note",
		}}}),
		contentWithFields("fixture_declaration", "project", map[string]any{"declarations": []any{map[string]any{
			"declaration_id": "review-helper", "name": "review helper", "status": "active",
		}}}),
	}}
	r := Renderer{Now: func() time.Time { return now }}

	profile, err := r.RenderPresentation(context.Background(), Request{Principal: "codex-a@project", RenderIntent: IntentProfileEvents}, proj)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(profile.Body, "[mnemon:profile]") || strings.Contains(profile.Body, "[mnemon:signal]") {
		t.Fatalf("profile.events must render only profile presentation:\n%s", profile.Body)
	}

	packet, err := r.RenderPresentation(context.Background(), Request{Principal: "codex-a@project", RenderIntent: IntentContextPacket}, proj)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(packet.Body, "[mnemon:context]") ||
		!strings.Contains(packet.Body, "teamwork_signal/sig1") ||
		!strings.Contains(packet.Body, "render progress note") ||
		!strings.Contains(packet.Body, "review helper") {
		t.Fatalf("context.packet must summarize scoped presentation view:\n%s", packet.Body)
	}

	contract, err := r.RenderPresentation(context.Background(), Request{Principal: "codex-a@project", RenderIntent: IntentPayloadContract}, proj)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(contract.Body, "[mnemon:payload-contract]") || !strings.Contains(contract.Body, "assignment.write_candidate.observed") {
		t.Fatalf("payload.contract must render governed event contract:\n%s", contract.Body)
	}

	unknown, err := r.RenderPresentation(context.Background(), Request{Principal: "codex-a@project", RenderIntent: "unknown.intent"}, proj)
	if err != nil {
		t.Fatal(err)
	}
	if unknown.Status != StatusEmpty || strings.TrimSpace(unknown.Body) != "" {
		t.Fatalf("unknown intent must not emit dynamic presentation: %#v", unknown)
	}
}

func bytesTrimSpace(in []byte) []byte {
	return []byte(strings.TrimSpace(string(in)))
}

func eventByType(events []eventmodel.EventEnvelope, eventType string) (eventmodel.EventEnvelope, bool) {
	for _, event := range events {
		if event.Event.Type == eventType {
			return event, true
		}
	}
	return eventmodel.EventEnvelope{}, false
}

func content(kind, id string, items []any) view.ResourceContent {
	return contentWithFields(kind, id, map[string]any{"items": items})
}

func contentWithFields(kind, id string, fields map[string]any) view.ResourceContent {
	return view.ResourceContent{
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
