package runtime

import (
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
)

// TestRuntimeIsSingleStoreOwner pins the P1.3 ownership invariant (S11): while one runtime owns the
// canonical store, a second owner — an embedded per-op opener OR a long-lived server — is rejected,
// so there is exactly ONE store owner / ONE dispatch-cursor driver at a time. After the owner closes,
// the store may be taken (the embedded -> service handoff).
func TestRuntimeIsSingleStoreOwner(t *testing.T) {
	p := filepath.Join(t.TempDir(), "governed.db")
	rt, err := OpenRuntime(p, RuntimeConfig{})
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if _, err := OpenRuntime(p, RuntimeConfig{}); err == nil {
		t.Fatal("a second runtime must not concurrently own the same store (S11 single-writer)")
	}
	if err := rt.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	rt2, err := OpenRuntime(p, RuntimeConfig{})
	if err != nil {
		t.Fatalf("reopen after the owner closes must succeed (embedded->service handoff): %v", err)
	}
	rt2.Close()
}

// TestServiceModeUnreachableErrors pins the P1.3 service-mode contract: a surface that calls a
// configured service it does not own fails EXPLICITLY when the service is unreachable — never a
// silent empty success that would read as "no governed state".
func TestServiceModeUnreachableErrors(t *testing.T) {
	rt, err := OpenRuntime(filepath.Join(t.TempDir(), "s.db"), RuntimeConfig{})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	srv := httptest.NewServer(access.NewHTTPHandler(rt.API()))
	url := srv.URL
	srv.Close() // the configured service is now unreachable

	c := access.NewClient(url, "agent")
	if _, _, err := c.Ingest("agent", contract.ObservationEnvelope{ExternalID: "x", Event: contract.Event{Type: "memory.observed"}}); err == nil {
		t.Fatal("observe against an unreachable service must error explicitly")
	}
	if _, err := c.PullPresentationView("agent", contract.Subscription{Actor: "agent"}); err == nil {
		t.Fatal("pull against an unreachable service must error explicitly")
	}
}
