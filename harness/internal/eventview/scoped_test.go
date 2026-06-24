package eventview

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// S9: a scoped view contains ONLY the subscription's refs — an out-of-scope resource never appears.
func TestScopedViewExcludesOutOfScope(t *testing.T) {
	s, k := newStoreKernel(t)
	createP(t, k, contract.ResourceRef{Kind: "memory", ID: "m1"}, map[string]any{"content": "a"})
	createP(t, k, contract.ResourceRef{Kind: "memory", ID: "m2"}, map[string]any{"content": "b"})
	sub := contract.Subscription{Actor: "user", Refs: []contract.ResourceRef{{Kind: "memory", ID: "m1"}}}
	view := ScopedView(s, sub)
	if len(view.Resources) != 1 || view.Resources[0].Ref.ID != "m1" {
		t.Fatalf("scoped view must contain only m1; got %+v", view.Resources)
	}
}

// S10/D8: the context digest covers field CONTENT. Two stores with the SAME {Kind:ID:Version} but different
// content must produce different digests (a content tamper that preserves the version is still detectable).
func TestDigestCoversContent(t *testing.T) {
	mk := func(content string) string {
		s, k := newStoreKernel(t)
		createP(t, k, contract.ResourceRef{Kind: "memory", ID: "m1"}, map[string]any{"content": content})
		return Build(s, []contract.ResourceRef{{Kind: "memory", ID: "m1"}}, "user").Digest
	}
	if mk("alpha") == mk("beta") {
		t.Fatal("digest must cover field content: same Kind:ID:Version, different content must differ (D8)")
	}
}
