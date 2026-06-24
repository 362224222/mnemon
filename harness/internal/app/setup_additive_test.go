package app

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
)

// Installing skill after memory for the same principal must be ADDITIVE: the binding keeps the memory
// grant (observed types + scope) and gains the skill grant — it does not replace one with the other.
// And the bearer token is idempotent: a rerun must not rotate it (a running Local Mnemon still holds
// the old token in memory, so a rotated token would lock hooks out).
func TestSetupIsAdditiveAndTokenIdempotent(t *testing.T) {
	root := t.TempDir()
	h := New(root)
	var out bytes.Buffer

	r1, err := h.Setup(context.Background(), &out, &out, SetupOptions{
		Host: "codex", Loops: []string{"memory"}, Principal: "codex@project", ProjectRoot: root,
	})
	if err != nil {
		t.Fatalf("setup memory: %v", err)
	}
	tok1, err := os.ReadFile(r1.TokenFile)
	if err != nil {
		t.Fatalf("read token: %v", err)
	}

	if _, err := h.Setup(context.Background(), &out, &out, SetupOptions{
		Host: "codex", Loops: []string{"skill"}, Principal: "codex@project", ProjectRoot: root,
	}); err != nil {
		t.Fatalf("setup skill: %v", err)
	}

	loaded, err := access.LoadBindingFile(root, r1.BindingFile)
	if err != nil {
		t.Fatalf("load bindings: %v", err)
	}
	var b access.ChannelBinding
	for _, x := range loaded.Bindings {
		if x.Principal == "codex@project" {
			b = x
		}
	}
	if !b.AllowsObservedType("memory.write_candidate.observed") {
		t.Fatal("additive setup must keep the memory grant after installing skill")
	}
	if !b.AllowsObservedType("skill.write_candidate.observed") {
		t.Fatal("additive setup must add the skill grant")
	}
	var hasMem, hasSkill bool
	for _, ref := range b.SubscriptionScope {
		if ref.Kind == "memory" {
			hasMem = true
		}
		if ref.Kind == "skill" {
			hasSkill = true
		}
	}
	if !hasMem || !hasSkill {
		t.Fatalf("binding scope must union both kinds; got %+v", b.SubscriptionScope)
	}

	tok2, err := os.ReadFile(r1.TokenFile)
	if err != nil {
		t.Fatalf("read token after rerun: %v", err)
	}
	if !bytes.Equal(tok1, tok2) {
		t.Fatal("the bearer token must be idempotent across reruns (a rerun rotated it)")
	}
}
