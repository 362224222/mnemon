package runtime

import (
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
)

// TestChannelStatusEvidence pins P2.3: status is richer than a pull alias — it carries the binding
// actor kind, the runtime/store ref, and the server mode (a pull cannot), while staying consistent
// with the scoped pull digest. It is gated on the binding's access.VerbStatus.
func TestChannelStatusEvidence(t *testing.T) {
	ref := contract.ResourceRef{Kind: "memory", ID: "m1"}
	storePath := filepath.Join(t.TempDir(), "governed.db")
	rt, err := OpenRuntime(storePath, RuntimeConfig{
		Subs:     map[contract.ActorID]contract.Subscription{"codex": {Actor: "codex", Refs: []contract.ResourceRef{ref}}},
		Bindings: []access.ChannelBinding{access.HostAgentBinding("codex", "", []contract.ResourceRef{ref})},
	})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	srv := httptest.NewServer(NewRuntimeHandler(rt, access.HeaderAuthenticator{}))
	defer srv.Close()

	c := access.NewClient(srv.URL, "codex")
	st, err := c.Status("codex")
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	if st.Principal != "codex" {
		t.Fatalf("status principal = %q", st.Principal)
	}
	if st.ActorKind != contract.KindHostAgent {
		t.Fatalf("status must carry the binding actor kind (a pull alias cannot); got %q", st.ActorKind)
	}
	if st.StoreRef == "" {
		t.Fatal("status must carry the runtime/store ref")
	}
	if st.Mode == "" {
		t.Fatal("status must carry the server mode")
	}
	// consistent with the scoped pull digest.
	proj, err := c.PullEventView("codex", contract.Subscription{Actor: "codex"})
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if st.Digest != proj.Digest {
		t.Fatalf("status digest %q must match the scoped pull digest %q", st.Digest, proj.Digest)
	}

	// an unbound principal gets no status.
	if _, err := access.NewClient(srv.URL, "ghost").Status("ghost"); err == nil {
		t.Fatal("an unbound principal must not get channel status")
	}
}
