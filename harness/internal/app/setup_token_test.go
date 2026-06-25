package app

import (
	"bytes"
	"context"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/access"
)

// Rerunning setup with --token=false must CLEAR the binding's token credential, not keep the old one.
// Otherwise a restarted Local Mnemon still enables the TokenAuthenticator (binding carries a token)
// while the hooks switch to the trusted header (env drops the token file) — and auth breaks.
func TestSetupTokenFalseClearsBindingCredential(t *testing.T) {
	root := t.TempDir()
	h := New(root)
	var out bytes.Buffer

	r1, err := h.Setup(context.Background(), &out, &out, SetupOptions{
		Host: "codex", Principal: "codex@project", ProjectRoot: root,
		UseToken: true, TokenExplicit: true,
	})
	if err != nil {
		t.Fatalf("setup (token on): %v", err)
	}
	loaded, err := access.LoadBindingFile(root, r1.BindingFile)
	if err != nil {
		t.Fatalf("load bindings: %v", err)
	}
	if len(loaded.Tokens) == 0 {
		t.Fatal("token install must record a binding credential")
	}

	if _, err := h.Setup(context.Background(), &out, &out, SetupOptions{
		Host: "codex", Principal: "codex@project", ProjectRoot: root,
		UseToken: false, TokenExplicit: true,
	}); err != nil {
		t.Fatalf("setup (--token=false): %v", err)
	}
	loaded, err = access.LoadBindingFile(root, r1.BindingFile)
	if err != nil {
		t.Fatalf("load bindings after --token=false: %v", err)
	}
	if len(loaded.Tokens) != 0 {
		t.Fatal("--token=false must clear the binding credential so the server matches the header-auth hooks")
	}
}
