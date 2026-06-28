package app

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation/view"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

func TestRenderEndpointUsesAuthenticatedScopedProjection(t *testing.T) {
	ref := contract.ResourceRef{Kind: "assignment", ID: "project"}
	a := access.HostAgentBinding("codex-a@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	a.AllowedObservedTypes = []string{"assignment.write_candidate.observed"}
	b := access.HostAgentBinding("codex-b@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
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
	rc.Now = func() string { return "2026-06-24T10:00:00Z" }
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "presentation.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	bindings, err := access.NewBindingSet(loaded.Bindings...)
	if err != nil {
		t.Fatalf("binding set: %v", err)
	}
	audit := &presentation.MemoryAuditSink{}
	handler := NewLocalHTTPHandler(rt, access.TokenAuthenticator{Tokens: loaded.Tokens}, bindings, presentation.Renderer{
		Now:       func() time.Time { return mustRenderHTTPTime(t, "2026-06-24T10:05:00Z") },
		AuditSink: audit,
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	clientA := access.NewClientWithToken(srv.URL, "tok-a")
	rec, err := clientA.IngestObserve("", contract.ObservationEnvelope{
		ExternalID: "assignment-render-endpoint",
		Event:      contract.Event{Type: "assignment.write_candidate.observed", Payload: r2Assignment("review render endpoint", "30m", "codex-b@project", "review the render endpoint", "short result", "endpoint test")},
	})
	if err != nil || !rec.Ticked {
		t.Fatalf("seed assignment: rec=%+v err=%v", rec, err)
	}

	resp := postRender(t, srv.URL, "tok-b", presentation.Request{RenderIntent: presentation.IntentTeamworkEvents})
	if resp.Status != presentation.StatusOK || !strings.Contains(resp.Body, "[mnemon:work]") {
		t.Fatalf("render endpoint should return assignee work presentation: %#v", resp)
	}
	if strings.Contains(resp.Body, "codex-a private") {
		t.Fatalf("render endpoint leaked out-of-scope content:\n%s", resp.Body)
	}
	if len(audit.Records) != 1 || audit.Records[0].Principal != "codex-b@project" || audit.Records[0].BodyDigest != resp.BodyDigest {
		t.Fatalf("render endpoint must write matching audit record: %+v resp=%+v", audit.Records, resp)
	}
}

func TestRenderEndpointRequiresRenderVerb(t *testing.T) {
	ref := contract.ResourceRef{Kind: "assignment", ID: "project"}
	b := access.HostAgentBinding("codex-b@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	b.AllowedVerbs = []access.Verb{access.VerbPull}
	loaded := access.LoadedBindings{Bindings: []access.ChannelBinding{b}, Tokens: map[string]contract.ActorID{"tok-b": "codex-b@project"}}
	rc, err := LocalRuntimeConfigFromBindings(loaded.Bindings, nil)
	if err != nil {
		t.Fatalf("runtime config: %v", err)
	}
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "render-deny.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	bindings, err := access.NewBindingSet(loaded.Bindings...)
	if err != nil {
		t.Fatalf("binding set: %v", err)
	}
	srv := httptest.NewServer(NewLocalHTTPHandler(rt, access.TokenAuthenticator{Tokens: loaded.Tokens}, bindings, presentation.Renderer{}))
	defer srv.Close()

	body, _ := json.Marshal(presentation.Request{RenderIntent: presentation.IntentTeamworkEvents})
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/render", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer tok-b")
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusForbidden {
		t.Fatalf("render without render verb status = %s, want 403", res.Status)
	}
}

func TestRenderEndpointAppliesBindingBudgetWithoutReducingAuthority(t *testing.T) {
	ref := contract.ResourceRef{Kind: "progress_digest", ID: "project"}
	b := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	b.AllowedObservedTypes = []string{"progress_digest.write_candidate.observed"}
	b.Budget = contract.BudgetDigestOnly
	loaded := access.LoadedBindings{
		Bindings: []access.ChannelBinding{b},
		Tokens:   map[string]contract.ActorID{"tok": "codex@project"},
	}
	rc, err := LocalRuntimeConfigFromBindings(loaded.Bindings, nil)
	if err != nil {
		t.Fatalf("runtime config: %v", err)
	}
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "render-budget.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	bindings, err := access.NewBindingSet(loaded.Bindings...)
	if err != nil {
		t.Fatalf("binding set: %v", err)
	}
	srv := httptest.NewServer(NewLocalHTTPHandler(rt, access.TokenAuthenticator{Tokens: loaded.Tokens}, bindings, presentation.Renderer{
		Now: func() time.Time { return mustRenderHTTPTime(t, "2026-06-24T10:05:00Z") },
	}))
	defer srv.Close()

	client := access.NewClientWithToken(srv.URL, "tok")
	for i := 1; i <= 3; i++ {
		rec, err := client.IngestObserve("", contract.ObservationEnvelope{
			ExternalID: fmt.Sprintf("progress-budget-%d", i),
			Event:      contract.Event{Type: "progress_digest.write_candidate.observed", Payload: r2Progress(fmt.Sprintf("render budget entry %d", i))},
		})
		if err != nil || !rec.Ticked {
			t.Fatalf("seed progress %d: rec=%+v err=%v", i, rec, err)
		}
	}

	packet := postRender(t, srv.URL, "tok", presentation.Request{RenderIntent: presentation.IntentContextPacket})
	if !strings.Contains(packet.Body, "render budget entry 3") {
		t.Fatalf("digest-only render packet must keep newest entry:\n%s", packet.Body)
	}
	for _, dropped := range []string{"render budget entry 1", "render budget entry 2"} {
		if strings.Contains(packet.Body, dropped) {
			t.Fatalf("digest-only render packet leaked older entry %q:\n%s", dropped, packet.Body)
		}
	}

	proj, err := client.PullPresentationView("", contract.Subscription{Actor: "codex@project"})
	if err != nil {
		t.Fatalf("pull authoritative presentation view: %v", err)
	}
	if n := resourceItemCount(proj.Content, ref); n != 3 {
		t.Fatalf("budget must not reduce authority: stored resource has %d items, want 3", n)
	}
}

func TestEventDataflowReachesContextPresenter(t *testing.T) {
	ref := contract.ResourceRef{Kind: "progress_digest", ID: "project"}
	b := access.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	b.AllowedObservedTypes = []string{"progress_digest.write_candidate.observed"}
	loaded := access.LoadedBindings{
		Bindings: []access.ChannelBinding{b},
		Tokens:   map[string]contract.ActorID{"tok": "codex@project"},
	}
	rc, err := LocalRuntimeConfigFromBindings(loaded.Bindings, nil)
	if err != nil {
		t.Fatalf("runtime config: %v", err)
	}
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "event-dataflow.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	bindings, err := access.NewBindingSet(loaded.Bindings...)
	if err != nil {
		t.Fatalf("binding set: %v", err)
	}
	srv := httptest.NewServer(NewLocalHTTPHandler(rt, access.TokenAuthenticator{Tokens: loaded.Tokens}, bindings, presentation.Renderer{
		Now: func() time.Time { return mustRenderHTTPTime(t, "2026-06-24T10:05:00Z") },
	}))
	defer srv.Close()

	client := access.NewClientWithToken(srv.URL, "tok")
	rec, err := client.IngestObserve("", contract.ObservationEnvelope{
		ExternalID: "event-dataflow-1",
		Event:      contract.Event{Type: "progress_digest.write_candidate.observed", Payload: r2Progress("Use the presenter registry as the dataflow boundary.")},
	})
	if err != nil || !rec.Ticked {
		t.Fatalf("observe event: rec=%+v err=%v", rec, err)
	}
	if v, fields, err := rt.Resource(ref); err != nil || v == 0 || !strings.Contains(fmt.Sprint(fields["content"]), "presenter registry") {
		t.Fatalf("event must materialize through mnemond state: v=%d fields=%+v err=%v", v, fields, err)
	}

	packet := postRender(t, srv.URL, "tok", presentation.Request{RenderIntent: presentation.IntentContextPacket})
	if packet.Status != presentation.StatusOK ||
		!strings.Contains(packet.Body, "[mnemon:context]") ||
		!strings.Contains(packet.Body, "presenter registry") {
		t.Fatalf("context presenter must carry admitted event state into the agent packet: %#v", packet)
	}
	for _, teamworkLabel := range []string{"[mnemon:work]", "[mnemon:feedback]", "[mnemon:integrate]", "[mnemon:expired]"} {
		if strings.Contains(packet.Body, teamworkLabel) {
			t.Fatalf("event dataflow must not require teamwork presentation label %q:\n%s", teamworkLabel, packet.Body)
		}
	}
}

func postRender(t *testing.T, baseURL, token string, reqBody presentation.Request) presentation.Response {
	t.Helper()
	body, err := json.Marshal(reqBody)
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/render", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("render status = %s", res.Status)
	}
	var out presentation.Response
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func resourceItemCount(content []view.ResourceContent, ref contract.ResourceRef) int {
	for _, rc := range content {
		if rc.Ref != ref {
			continue
		}
		switch entries := rc.Fields["items"].(type) {
		case []any:
			return len(entries)
		case []map[string]any:
			return len(entries)
		}
	}
	return 0
}

func mustRenderHTTPTime(t *testing.T, s string) time.Time {
	t.Helper()
	out, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
