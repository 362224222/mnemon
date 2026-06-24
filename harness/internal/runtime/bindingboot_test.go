package runtime

import (
	"context"
	"io"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
)

// writeProjectBindings writes a one-binding manifest + token file under a fresh project root and
// returns (root, bindingPath).
func writeProjectBindings(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	channelDir := filepath.Join(root, ".mnemon", "harness", "channel")
	if err := os.MkdirAll(filepath.Join(channelDir, "tokens"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(channelDir, "tokens", "codex.token"), []byte("tok-codex\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	js := `{"schema_version":1,"bindings":[{
	  "principal":"codex@project","actor_kind":"host-agent","transport":"http",
	  "endpoint":"http://127.0.0.1:8787","allowed_verbs":["observe","pull","status"],
	  "allowed_observed_types":["session.observed"],
	  "subscription_scope":[{"kind":"memory","id":"m1"}],
	  "idempotency_namespace":"host:codex@project",
	  "credential_ref":".mnemon/harness/channel/tokens/codex.token"}]}`
	bindingPath := filepath.Join(channelDir, "bindings.json")
	if err := os.WriteFile(bindingPath, []byte(js), 0o644); err != nil {
		t.Fatal(err)
	}
	return root, bindingPath
}

// TestBindingFileChannelTokenAuth proves the P3 path end to end at the mnemond access boundary: a loaded
// binding file drives the runtime's bindings + scope + a access.TokenAuthenticator, so a bearer token
// resolves the principal, an in-scope pull/status succeeds, an unknown token is rejected, and a
// cross-scope pull is refused — all without the trusted principal header.
func TestBindingFileChannelTokenAuth(t *testing.T) {
	root, bindingPath := writeProjectBindings(t)
	loaded, err := access.LoadBindingFile(root, bindingPath)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	rt, err := OpenRuntime(filepath.Join(root, DefaultStorePath), RuntimeConfig{
		Bindings: loaded.Bindings,
		Subs:     access.SubsFromBindings(loaded.Bindings),
	})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	srv := httptest.NewServer(NewRuntimeHandler(rt, access.TokenAuthenticator{Tokens: loaded.Tokens}))
	defer srv.Close()

	// valid token resolves the principal from the bearer credential (no X-Mnemon-Principal header).
	good := access.NewClientWithToken(srv.URL, "tok-codex")
	st, err := good.Status("")
	if err != nil {
		t.Fatalf("token-authed status: %v", err)
	}
	if st.Principal != "codex@project" || st.ActorKind != contract.KindHostAgent {
		t.Fatalf("token must resolve to the bound principal/kind; got %+v", st)
	}
	if _, err := good.PullEventView("", contract.Subscription{Actor: "codex@project", Refs: []contract.ResourceRef{{Kind: "memory", ID: "m1"}}}); err != nil {
		t.Fatalf("in-scope pull: %v", err)
	}
	// cross-scope pull refused.
	if _, err := good.PullEventView("", contract.Subscription{Actor: "codex@project", Refs: []contract.ResourceRef{{Kind: "memory", ID: "secret"}}}); err == nil {
		t.Fatal("cross-scope pull must be refused")
	}
	// unknown token rejected.
	if _, err := access.NewClientWithToken(srv.URL, "nope").Status(""); err == nil {
		t.Fatal("unknown bearer token must be rejected")
	}
}

// TestRunHTTPServerWithBindingsBoots is the P3.2 server-boot test: the binding-configured front door
// boots on a real port, a token client round-trips status, and ctx cancel shuts it down.
func TestRunHTTPServerWithBindingsBoots(t *testing.T) {
	root, bindingPath := writeProjectBindings(t)
	loaded, err := access.LoadBindingFile(root, bindingPath)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- RunHTTPServerWithBindings(ctx, addr, filepath.Join(root, DefaultStorePath), loaded, io.Discard)
	}()

	c := access.NewClientWithToken("http://"+addr, "tok-codex")
	var st contract.ChannelStatus
	deadline := time.Now().Add(3 * time.Second)
	for {
		st, err = c.Status("")
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("server did not become ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if st.Principal != "codex@project" {
		t.Fatalf("status principal = %q", st.Principal)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("server exited with error: %v", err)
	}
}
