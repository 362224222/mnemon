package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/presentation"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

// TestControlTokenFileAuth proves P3.2 `control --token-file`: the channel client reads the bearer
// token from a file (so projected hooks keep it out of prompt-visible command lines), authenticates,
// and surfaces explicit errors for a wrong token or a missing file.
func TestControlTokenFileAuth(t *testing.T) {
	root := t.TempDir()
	ref := contract.ResourceRef{Kind: "progress_digest", ID: "project"}
	rt, err := runtime.OpenRuntime(filepath.Join(root, runtime.DefaultStorePath), runtime.RuntimeConfig{
		Subs:     map[contract.ActorID]contract.Subscription{"codex@project": {Actor: "codex@project", Refs: []contract.ResourceRef{ref}}},
		Bindings: []access.ChannelBinding{access.HostAgentBinding("codex@project", "http://x", []contract.ResourceRef{ref})},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	srv := httptest.NewServer(runtime.NewRuntimeHandler(rt, access.TokenAuthenticator{Tokens: map[string]contract.ActorID{"tok-codex": "codex@project"}}))
	defer srv.Close()

	tokFile := filepath.Join(t.TempDir(), "codex.token")
	if err := os.WriteFile(tokFile, []byte("tok-codex\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	controlAddr = srv.URL
	controlPrincipal = "codex@project"
	controlToken = ""
	controlTokenFile = tokFile
	controlStatusJSON = false
	t.Cleanup(func() {
		controlAddr = "http://127.0.0.1:8787"
		controlPrincipal = ""
		controlToken = ""
		controlTokenFile = ""
	})

	var buf bytes.Buffer
	controlStatusCmd.SetOut(&buf)
	if err := controlStatusCmd.RunE(controlStatusCmd, nil); err != nil {
		t.Fatalf("control status --token-file must succeed: %v", err)
	}
	if !strings.Contains(buf.String(), "codex@project") {
		t.Fatalf("status output must name the token-resolved principal; got %q", buf.String())
	}
	for _, want := range []string{"Local Mnemon: ready", "local accepted, remote pending"} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("status output must include %q; got %q", want, buf.String())
		}
	}
	// P3d: the FIELD section (Control Tower seed) reports the coordination counts; with nothing
	// observed yet they are all zero, but the line is present and names the default-enabled kinds.
	for _, want := range []string{"Field:", "assignment=0", "agent profile=0", "teamwork signal=0"} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("status must include coordination FIELD count %q; got %q", want, buf.String())
		}
	}
	// Channel status has no Remote Workspace data source (no --root, ServerAPI only):
	// it must not assert a connection state it cannot know.
	if strings.Contains(buf.String(), "Remote Workspace") {
		t.Fatalf("control status must not claim a Remote Workspace state; got %q", buf.String())
	}

	// wrong token => authenticated rejection.
	badTok := filepath.Join(t.TempDir(), "bad.token")
	if err := os.WriteFile(badTok, []byte("wrong"), 0o600); err != nil {
		t.Fatal(err)
	}
	controlTokenFile = badTok
	if err := controlStatusCmd.RunE(controlStatusCmd, nil); err == nil {
		t.Fatal("control status with an invalid token must fail")
	}

	// missing token file => explicit read error.
	controlTokenFile = filepath.Join(t.TempDir(), "nonexistent.token")
	if err := controlStatusCmd.RunE(controlStatusCmd, nil); err == nil {
		t.Fatal("control status with a missing --token-file must error")
	}
}

func TestControlPullJSONIncludesScopedContent(t *testing.T) {
	ref := contract.ResourceRef{Kind: "progress_digest", ID: "project"}
	binding := access.HostAgentBinding("codex@project", "http://x", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{"progress_digest.write_candidate.observed"}
	rt, err := app.OpenLocalRuntime(filepath.Join(t.TempDir(), "governed.db"), access.LoadedBindings{Bindings: []access.ChannelBinding{binding}}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	srv := httptest.NewServer(runtime.NewRuntimeHandler(rt, access.HeaderAuthenticator{}))
	defer srv.Close()
	client := access.NewClient(srv.URL, "codex@project")
	if rec, err := client.IngestObserve("codex@project", contract.ObservationEnvelope{
		ExternalID: "progress-json",
		Event: contract.Event{Type: "progress_digest.write_candidate.observed", Payload: map[string]any{
			"summary": "Use Local Mnemon as the event source.",
		}},
	}); err != nil || !rec.Ticked {
		t.Fatalf("seed local progress event: rec=%+v err=%v", rec, err)
	}

	oldAddr := controlAddr
	oldPrincipal := controlPrincipal
	oldToken := controlToken
	oldTokenFile := controlTokenFile
	oldActor := controlActor
	oldPullJSON := controlPullJSON
	t.Cleanup(func() {
		controlAddr = oldAddr
		controlPrincipal = oldPrincipal
		controlToken = oldToken
		controlTokenFile = oldTokenFile
		controlActor = oldActor
		controlPullJSON = oldPullJSON
	})
	controlAddr = srv.URL
	controlPrincipal = "codex@project"
	controlToken = ""
	controlTokenFile = ""
	controlActor = ""
	controlPullJSON = true

	var buf bytes.Buffer
	controlPullCmd.SetOut(&buf)
	if err := controlPullCmd.RunE(controlPullCmd, nil); err != nil {
		t.Fatalf("control pull --json: %v", err)
	}
	var out struct {
		Content []struct {
			Fields map[string]any `json:"fields"`
		} `json:"Content"`
	}
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("pull output must be JSON: %v\n%s", err, buf.String())
	}
	if len(out.Content) != 1 {
		t.Fatalf("pull JSON must include one scoped content item, got %+v", out.Content)
	}
	if content, _ := out.Content[0].Fields["content"].(string); !strings.Contains(content, "Use Local Mnemon") {
		t.Fatalf("pull JSON content missing progress text: %+v", out.Content[0].Fields)
	}
}

func TestControlRenderPrintsDerivedEventPresentationBody(t *testing.T) {
	ref := contract.ResourceRef{Kind: "assignment", ID: "project"}
	a := access.HostAgentBinding("codex-a@project", "http://x", []contract.ResourceRef{ref})
	a.AllowedObservedTypes = []string{"assignment.write_candidate.observed"}
	b := access.HostAgentBinding("codex-b@project", "http://x", []contract.ResourceRef{ref})
	loaded := access.LoadedBindings{
		Bindings: []access.ChannelBinding{a, b},
		Tokens: map[string]contract.ActorID{
			"tok-a": "codex-a@project",
			"tok-b": "codex-b@project",
		},
	}
	rc, err := app.LocalRuntimeConfigFromBindings(loaded.Bindings, nil)
	if err != nil {
		t.Fatal(err)
	}
	rc.Now = func() string { return "2026-06-24T10:00:00Z" }
	rt, err := runtime.OpenRuntime(filepath.Join(t.TempDir(), "presentation.db"), rc)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	bindings, err := access.NewBindingSet(loaded.Bindings...)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(app.NewLocalHTTPHandler(rt, access.TokenAuthenticator{Tokens: loaded.Tokens}, bindings, presentation.Renderer{
		Now: func() time.Time { return mustCmdTime(t, "2026-06-24T10:05:00Z") },
	}))
	defer srv.Close()
	clientA := access.NewClientWithToken(srv.URL, "tok-a")
	if rec, err := clientA.IngestObserve("", contract.ObservationEnvelope{
		ExternalID: "control-render-assignment",
		Event: contract.Event{Type: "assignment.write_candidate.observed", Payload: map[string]any{
			"scope": "review control render", "ttl": "30m", "assignee": "codex-b@project",
			"expected_work": "review control render", "expected_feedback": "short result",
			"evidence": "control render test",
		}},
	}); err != nil || !rec.Ticked {
		t.Fatalf("seed assignment: rec=%+v err=%v", rec, err)
	}

	oldAddr := controlAddr
	oldPrincipal := controlPrincipal
	oldToken := controlToken
	oldTokenFile := controlTokenFile
	oldIntent := controlRenderIntent
	oldLifecycle := controlRenderLifecycle
	oldSurface := controlRenderSurface
	oldMaxChars := controlRenderMaxChars
	oldJSON := controlRenderJSON
	t.Cleanup(func() {
		controlAddr = oldAddr
		controlPrincipal = oldPrincipal
		controlToken = oldToken
		controlTokenFile = oldTokenFile
		controlRenderIntent = oldIntent
		controlRenderLifecycle = oldLifecycle
		controlRenderSurface = oldSurface
		controlRenderMaxChars = oldMaxChars
		controlRenderJSON = oldJSON
	})
	controlAddr = srv.URL
	controlPrincipal = "codex-b@project"
	controlToken = "tok-b"
	controlTokenFile = ""
	controlRenderIntent = presentation.IntentTeamworkEvents
	controlRenderLifecycle = "remind"
	controlRenderSurface = "hook"
	controlRenderMaxChars = 6000
	controlRenderJSON = false

	var buf bytes.Buffer
	controlRenderCmd.SetOut(&buf)
	if err := controlRenderCmd.RunE(controlRenderCmd, nil); err != nil {
		t.Fatalf("control render: %v", err)
	}
	if !strings.Contains(buf.String(), "[mnemon:work]") || strings.Contains(buf.String(), `"body"`) {
		t.Fatalf("control render must print presentation body only, got:\n%s", buf.String())
	}
}

func mustReadCmd(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func mustCmdTime(t *testing.T, s string) time.Time {
	t.Helper()
	out, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
