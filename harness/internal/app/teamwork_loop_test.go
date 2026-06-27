package app

import (
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

func TestMinimalTeamworkLoopThroughRenderPresentations(t *testing.T) {
	refs := []contract.ResourceRef{
		{Kind: "agent_profile", ID: "project"},
		{Kind: "teamwork_signal", ID: "project"},
		{Kind: "assignment", ID: "project"},
		{Kind: "progress_digest", ID: "project"},
	}
	observed := []string{
		"agent_profile.write_candidate.observed",
		"teamwork_signal.write_candidate.observed",
		"assignment.write_candidate.observed",
		"progress_digest.write_candidate.observed",
	}
	a := access.HostAgentBinding("codex-a@project", "http://127.0.0.1:8787", refs)
	a.AllowedObservedTypes = observed
	b := access.HostAgentBinding("codex-b@project", "http://127.0.0.1:8787", refs)
	b.AllowedObservedTypes = observed
	loaded := access.LoadedBindings{
		Bindings: []access.ChannelBinding{a, b},
		Tokens: map[string]contract.ActorID{
			"tok-a": "codex-a@project",
			"tok-b": "codex-b@project",
		},
	}
	rc, err := LocalRuntimeConfigFromBindings(loaded.Bindings, nil)
	if err != nil {
		t.Fatalf("runtime config: %v", err)
	}
	now := "2026-06-24T10:00:00Z"
	rc.Now = func() string { return now }
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "teamwork-loop.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	bindings, err := access.NewBindingSet(loaded.Bindings...)
	if err != nil {
		t.Fatalf("binding set: %v", err)
	}
	renderNow := mustRenderHTTPTime(t, "2026-06-24T10:05:00Z")
	srv := httptest.NewServer(NewLocalHTTPHandler(rt, access.TokenAuthenticator{Tokens: loaded.Tokens}, bindings, presentation.Renderer{
		Now: func() time.Time { return renderNow },
	}))
	defer srv.Close()
	clientA := access.NewClientWithToken(srv.URL, "tok-a")
	clientB := access.NewClientWithToken(srv.URL, "tok-b")
	observe := func(client *access.Client, extID, typ string, payload map[string]any) {
		t.Helper()
		rec, err := client.IngestObserve("", contract.ObservationEnvelope{
			ExternalID: extID,
			Event:      contract.Event{Type: typ, Payload: payload},
		})
		if err != nil || !rec.Ticked {
			t.Fatalf("observe %s: rec=%+v err=%v", typ, rec, err)
		}
	}

	observe(clientA, "profile-a", "agent_profile.write_candidate.observed",
		r2AgentProfile("codex-a@project", "coordinate R1 render loop", "available", "30m",
			"A can originate and integrate render assignments.", "read R1 event-presentation plan"))
	observe(clientB, "profile-b", "agent_profile.write_candidate.observed",
		r2AgentProfile("codex-b@project", "review R1 render loop", "available", "30m",
			"B can review render assignments.", "fresh context on render endpoint"))
	observe(clientA, "signal-r1", "teamwork_signal.write_candidate.observed",
		r2TeamworkSignal("harness/r1/render", "Need another agent to review the render endpoint.",
			"another profile has endpoint context", "1h", "profile roster"))
	observe(clientA, "assignment-r1", "assignment.write_candidate.observed", r2AssignmentPayload(
		map[string]any{"assignment_id": "asg-r1", "signal_ref": "sig-r1", "assignee": "codex-b@project", "scope": "review render endpoint", "ttl": "30m"},
		map[string]any{"expected_work": "review the render endpoint", "expected_feedback": "progress_digest with result or blocker"},
		map[string]any{"evidence_refs": []any{"signal sig-r1"}},
	))

	work := postRender(t, srv.URL, "tok-b", presentation.Request{RenderIntent: presentation.IntentTeamworkEvents})
	if !strings.Contains(work.Body, "[mnemon:work]") || !strings.Contains(work.Body, "asg-r1") || !strings.Contains(work.Body, "[mnemon:feedback]") {
		t.Fatalf("B must see work + feedback presentation for assignment:\n%s", work.Body)
	}

	observe(clientB, "progress-r1", "progress_digest.write_candidate.observed",
		r2ProgressFor("asg-r1", "harness/r1/render", "review complete; render endpoint is usable", "render endpoint test"))
	integrate := postRender(t, srv.URL, "tok-a", presentation.Request{RenderIntent: presentation.IntentTeamworkEvents})
	if !strings.Contains(integrate.Body, "[mnemon:integrate]") || !strings.Contains(integrate.Body, "review complete") {
		t.Fatalf("A must see integration presentation after B feedback:\n%s", integrate.Body)
	}
	afterFeedback := postRender(t, srv.URL, "tok-b", presentation.Request{RenderIntent: presentation.IntentTeamworkEvents})
	if strings.Contains(afterFeedback.Body, "Assignment asg-r1 is yours") {
		t.Fatalf("linked progress must remove B work presentation:\n%s", afterFeedback.Body)
	}

	now = "2026-06-24T10:10:00Z"
	observe(clientA, "assignment-expired", "assignment.write_candidate.observed", r2AssignmentPayload(
		map[string]any{"assignment_id": "asg-exp", "assignee": "codex-b@project", "scope": "check expired branch", "ttl": "5m"},
		map[string]any{"expected_work": "check expired branch", "expected_feedback": "progress_digest with result or blocker"},
		map[string]any{"evidence_refs": []any{"TTL branch"}},
	))
	renderNow = mustRenderHTTPTime(t, "2026-06-24T10:20:00Z")
	expired := postRender(t, srv.URL, "tok-a", presentation.Request{RenderIntent: presentation.IntentTeamworkEvents})
	if !strings.Contains(expired.Body, "[mnemon:expired]") || !strings.Contains(expired.Body, "asg-exp") {
		t.Fatalf("A must see expired presentation for unreported assignment:\n%s", expired.Body)
	}
	assigneeExpired := postRender(t, srv.URL, "tok-b", presentation.Request{RenderIntent: presentation.IntentTeamworkEvents})
	if strings.Contains(assigneeExpired.Body, "[mnemon:expired]") {
		t.Fatalf("B must not see originator expired presentation:\n%s", assigneeExpired.Body)
	}
}
