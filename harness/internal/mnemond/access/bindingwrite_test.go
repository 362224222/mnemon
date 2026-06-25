package access

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

func readEntries(t *testing.T, path string) []bindingFileEntry {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var doc bindingFileDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if doc.SchemaVersion != 1 {
		t.Fatalf("schema_version = %d", doc.SchemaVersion)
	}
	return doc.Bindings
}

// TestUpsertAndRemoveBindingPreservesOthers proves the P4 binding upsert manages exactly its own
// principal: it creates the file, replaces in place (idempotent), preserves a user-added sibling
// entry, and on remove drops only its own entry.
func TestUpsertAndRemoveBindingPreservesOthers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "channel", "bindings.json")

	// a user-added binding already in the manifest.
	user := HostAgentBinding("user@project", "http://x", []contract.ResourceRef{{Kind: "memory", ID: "u"}})
	if err := UpsertBinding(path, user, ""); err != nil {
		t.Fatalf("upsert user: %v", err)
	}
	// setup's managed binding.
	codex := HostAgentBinding("codex@project", "http://127.0.0.1:8787", []contract.ResourceRef{{Kind: "memory", ID: "project"}})
	if err := UpsertBinding(path, codex, ".mnemon/harness/channel/tokens/codex.token"); err != nil {
		t.Fatalf("upsert codex: %v", err)
	}
	// idempotent re-upsert must not duplicate.
	if err := UpsertBinding(path, codex, ".mnemon/harness/channel/tokens/codex.token"); err != nil {
		t.Fatalf("re-upsert codex: %v", err)
	}
	entries := readEntries(t, path)
	if len(entries) != 2 {
		t.Fatalf("want 2 entries (user + codex), got %d: %+v", len(entries), entries)
	}
	var codexEntry *bindingFileEntry
	for i := range entries {
		if entries[i].Principal == "codex@project" {
			codexEntry = &entries[i]
		}
	}
	if codexEntry == nil || codexEntry.CredentialRef != ".mnemon/harness/channel/tokens/codex.token" || codexEntry.Endpoint != "http://127.0.0.1:8787" {
		t.Fatalf("codex entry wrong: %+v", codexEntry)
	}

	// uninstall removes only codex, preserving the user binding.
	removed, err := RemoveBinding(path, "codex@project")
	if err != nil || !removed {
		t.Fatalf("remove codex: removed=%v err=%v", removed, err)
	}
	entries = readEntries(t, path)
	if len(entries) != 1 || entries[0].Principal != "user@project" {
		t.Fatalf("user binding must survive uninstall; got %+v", entries)
	}
	// removing an absent principal is a no-op.
	if removed, _ := RemoveBinding(path, "ghost@project"); removed {
		t.Fatal("removing an absent principal must report not-removed")
	}
}
