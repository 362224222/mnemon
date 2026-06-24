package exchange

import (
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/store"
)

// ProbeAvailable lets the standalone sync detect a co-hosted Local Mnemon before it tries to open a
// SECOND writer: it succeeds when the store is free, and returns an error when a running server holds
// the single-writer lock (so background sync can refuse cleanly instead of failing per-pass).
func TestProbeAvailable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "governed.db")

	if err := ProbeAvailable(path); err != nil {
		t.Fatalf("a free store must probe available; got %v", err)
	}

	held, err := store.OpenStore(path)
	if err != nil {
		t.Fatalf("hold the store: %v", err)
	}
	defer held.Close()

	if err := ProbeAvailable(path); err == nil {
		t.Fatal("a store held by a running server must probe BUSY (single-writer lock)")
	}

	held.Close()
	if err := ProbeAvailable(path); err != nil {
		t.Fatalf("after the holder releases, the store must probe available again; got %v", err)
	}
}
