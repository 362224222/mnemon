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

	"github.com/mnemon-dev/mnemon/harness/internal/channel"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/projection"
	"github.com/mnemon-dev/mnemon/harness/internal/render"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

func TestRenderEndpointUsesAuthenticatedScopedProjection(t *testing.T) {
	ref := contract.ResourceRef{Kind: "assignment", ID: "project"}
	a := channel.HostAgentBinding("codex-a@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	a.AllowedObservedTypes = []string{"assignment.write_candidate.observed"}
	b := channel.HostAgentBinding("codex-b@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	loaded := channel.LoadedBindings{
		Bindings: []channel.ChannelBinding{a, b},
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
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "render.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	bindings, err := channel.NewBindingSet(loaded.Bindings...)
	if err != nil {
		t.Fatalf("binding set: %v", err)
	}
	audit := &render.MemoryAuditSink{}
	handler := NewLocalHTTPHandler(rt, channel.TokenAuthenticator{Tokens: loaded.Tokens}, bindings, render.Renderer{
		Now:       func() time.Time { return mustRenderHTTPTime(t, "2026-06-24T10:05:00Z") },
		AuditSink: audit,
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	clientA := channel.NewClientWithToken(srv.URL, "tok-a")
	rec, err := clientA.IngestObserve("", contract.ObservationEnvelope{
		ExternalID: "assignment-render-endpoint",
		Event: contract.Event{Type: "assignment.write_candidate.observed", Payload: map[string]any{
			"scope": "review render endpoint", "ttl": "30m", "assignee": "codex-b@project",
			"expected_work": "review the render endpoint", "expected_feedback": "short result",
			"evidence": "endpoint test",
		}},
	})
	if err != nil || !rec.Ticked {
		t.Fatalf("seed assignment: rec=%+v err=%v", rec, err)
	}

	resp := postRender(t, srv.URL, "tok-b", render.Request{RenderIntent: render.IntentTeamworkCue})
	if resp.Status != render.StatusOK || !strings.Contains(resp.Body, "[mnemon:work]") {
		t.Fatalf("render endpoint should return assignee work cue: %#v", resp)
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
	b := channel.HostAgentBinding("codex-b@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	b.AllowedVerbs = []channel.Verb{channel.VerbPull}
	loaded := channel.LoadedBindings{Bindings: []channel.ChannelBinding{b}, Tokens: map[string]contract.ActorID{"tok-b": "codex-b@project"}}
	rc, err := LocalRuntimeConfigFromBindings(loaded.Bindings, nil)
	if err != nil {
		t.Fatalf("runtime config: %v", err)
	}
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "render-deny.db"), rc)
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	bindings, err := channel.NewBindingSet(loaded.Bindings...)
	if err != nil {
		t.Fatalf("binding set: %v", err)
	}
	srv := httptest.NewServer(NewLocalHTTPHandler(rt, channel.TokenAuthenticator{Tokens: loaded.Tokens}, bindings, render.Renderer{}))
	defer srv.Close()

	body, _ := json.Marshal(render.Request{RenderIntent: render.IntentTeamworkCue})
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
	ref := contract.ResourceRef{Kind: "memory", ID: "project"}
	b := channel.HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{ref})
	b.AllowedObservedTypes = []string{"memory.write_candidate.observed"}
	b.Budget = contract.BudgetDigestOnly
	loaded := channel.LoadedBindings{
		Bindings: []channel.ChannelBinding{b},
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
	bindings, err := channel.NewBindingSet(loaded.Bindings...)
	if err != nil {
		t.Fatalf("binding set: %v", err)
	}
	srv := httptest.NewServer(NewLocalHTTPHandler(rt, channel.TokenAuthenticator{Tokens: loaded.Tokens}, bindings, render.Renderer{
		Now: func() time.Time { return mustRenderHTTPTime(t, "2026-06-24T10:05:00Z") },
	}))
	defer srv.Close()

	client := channel.NewClientWithToken(srv.URL, "tok")
	for i := 1; i <= 3; i++ {
		rec, err := client.IngestObserve("", contract.ObservationEnvelope{
			ExternalID: fmt.Sprintf("memory-budget-%d", i),
			Event: contract.Event{Type: "memory.write_candidate.observed", Payload: map[string]any{
				"content": fmt.Sprintf("render budget entry %d", i), "source": "user", "confidence": "high",
			}},
		})
		if err != nil || !rec.Ticked {
			t.Fatalf("seed memory %d: rec=%+v err=%v", i, rec, err)
		}
	}

	packet := postRender(t, srv.URL, "tok", render.Request{RenderIntent: render.IntentContextPacket})
	if !strings.Contains(packet.Body, "render budget entry 3") {
		t.Fatalf("digest-only render packet must keep newest entry:\n%s", packet.Body)
	}
	for _, dropped := range []string{"render budget entry 1", "render budget entry 2"} {
		if strings.Contains(packet.Body, dropped) {
			t.Fatalf("digest-only render packet leaked older entry %q:\n%s", dropped, packet.Body)
		}
	}

	proj, err := client.PullProjection("", contract.Subscription{Actor: "codex@project"})
	if err != nil {
		t.Fatalf("pull authoritative projection: %v", err)
	}
	if n := memoryEntryCount(proj.Content); n != 3 {
		t.Fatalf("budget must not reduce authority: stored memory has %d entries, want 3", n)
	}
}

func postRender(t *testing.T, baseURL, token string, reqBody render.Request) render.Response {
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
	var out render.Response
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

func memoryEntryCount(content []projection.ResourceContent) int {
	for _, rc := range content {
		if rc.Ref.Kind != "memory" {
			continue
		}
		switch entries := rc.Fields["entries"].(type) {
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
