package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/app"
	"github.com/mnemon-dev/mnemon/harness/internal/capability"
	"github.com/mnemon-dev/mnemon/harness/internal/channel"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

// TestControlTokenFileAuth proves P3.2 `control --token-file`: the channel client reads the bearer
// token from a file (so projected hooks keep it out of prompt-visible command lines), authenticates,
// and surfaces explicit errors for a wrong token or a missing file.
func TestControlTokenFileAuth(t *testing.T) {
	root := t.TempDir()
	ref := contract.ResourceRef{Kind: "memory", ID: "m1"}
	rt, err := runtime.OpenRuntime(filepath.Join(root, runtime.DefaultStorePath), runtime.RuntimeConfig{
		Subs:     map[contract.ActorID]contract.Subscription{"codex@project": {Actor: "codex@project", Refs: []contract.ResourceRef{ref}}},
		Bindings: []channel.ChannelBinding{channel.HostAgentBinding("codex@project", "http://x", []contract.ResourceRef{ref})},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	srv := httptest.NewServer(runtime.NewRuntimeHandler(rt, channel.TokenAuthenticator{Tokens: map[string]contract.ActorID{"tok-codex": "codex@project"}}))
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
	ref := contract.ResourceRef{Kind: "memory", ID: "project"}
	binding := channel.HostAgentBinding("codex@project", "http://x", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{capability.MemoryWriteCandidateObserved}
	rt, err := app.OpenLocalRuntime(filepath.Join(t.TempDir(), "governed.db"), channel.LoadedBindings{Bindings: []channel.ChannelBinding{binding}}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	srv := httptest.NewServer(runtime.NewRuntimeHandler(rt, channel.HeaderAuthenticator{}))
	defer srv.Close()
	client := channel.NewClient(srv.URL, "codex@project")
	if rec, err := client.IngestObserve("codex@project", contract.ObservationEnvelope{
		ExternalID: "memory-json",
		Event: contract.Event{Type: capability.MemoryWriteCandidateObserved, Payload: map[string]any{
			"content": "Use Local Mnemon as the memory source.",
			"source":  "user", "confidence": "high",
		}},
	}); err != nil || !rec.Ticked {
		t.Fatalf("seed local memory: rec=%+v err=%v", rec, err)
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
		t.Fatalf("pull JSON content missing memory text: %+v", out.Content[0].Fields)
	}
}

func TestControlPullMirrorWritesNonAuthoritativeMemoryFile(t *testing.T) {
	ref := contract.ResourceRef{Kind: "memory", ID: "project"}
	binding := channel.HostAgentBinding("codex@project", "http://x", []contract.ResourceRef{ref})
	binding.AllowedObservedTypes = []string{capability.MemoryWriteCandidateObserved}
	rt, err := app.OpenLocalRuntime(filepath.Join(t.TempDir(), "governed.db"), channel.LoadedBindings{Bindings: []channel.ChannelBinding{binding}}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	srv := httptest.NewServer(runtime.NewRuntimeHandler(rt, channel.HeaderAuthenticator{}))
	defer srv.Close()
	client := channel.NewClient(srv.URL, "codex@project")
	if rec, err := client.IngestObserve("codex@project", contract.ObservationEnvelope{
		ExternalID: "memory-mirror",
		Event: contract.Event{Type: capability.MemoryWriteCandidateObserved, Payload: map[string]any{
			"content": "Mirror content comes from Local Mnemon.",
			"source":  "user", "confidence": "high",
		}},
	}); err != nil || !rec.Ticked {
		t.Fatalf("seed local memory: rec=%+v err=%v", rec, err)
	}

	oldAddr := controlAddr
	oldPrincipal := controlPrincipal
	oldToken := controlToken
	oldTokenFile := controlTokenFile
	oldActor := controlActor
	oldPullJSON := controlPullJSON
	oldMirror := controlMirrorPath
	t.Cleanup(func() {
		controlAddr = oldAddr
		controlPrincipal = oldPrincipal
		controlToken = oldToken
		controlTokenFile = oldTokenFile
		controlActor = oldActor
		controlPullJSON = oldPullJSON
		controlMirrorPath = oldMirror
	})
	mirrorPath := filepath.Join(t.TempDir(), "MEMORY.md")
	controlAddr = srv.URL
	controlPrincipal = "codex@project"
	controlToken = ""
	controlTokenFile = ""
	controlActor = ""
	controlPullJSON = false
	controlMirrorPath = mirrorPath

	var buf bytes.Buffer
	controlPullCmd.SetOut(&buf)
	if err := controlPullCmd.RunE(controlPullCmd, nil); err != nil {
		t.Fatalf("control pull --mirror: %v", err)
	}
	mirror := string(mustReadCmd(t, mirrorPath))
	if !strings.Contains(mirror, "Non-authoritative mirror") || !strings.Contains(mirror, "Mirror content comes from Local Mnemon") {
		t.Fatalf("mirror did not render scoped memory:\n%s", mirror)
	}
	if !strings.Contains(buf.String(), "wrote memory mirror") {
		t.Fatalf("control pull should report mirror refresh, got %q", buf.String())
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
