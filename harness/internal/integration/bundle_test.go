package integration

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/assets"
)

func TestInstallNodeBundlePublishesAndReplaysOneExactRevision(t *testing.T) {
	nodeState := newBundleNodeState(t)
	bundle, err := assets.Load()
	if err != nil {
		t.Fatal(err)
	}
	first, err := InstallNodeBundle(nodeState, bundle)
	if err != nil || first.Replayed || first.Revision != bundle.Manifest().AssetRevision {
		t.Fatalf("first InstallNodeBundle() = (%#v, %v)", first, err)
	}
	assertInstalledBundle(t, first.Path, bundle)
	second, err := InstallNodeBundle(nodeState, bundle)
	if err != nil || !second.Replayed || second.Path != first.Path || second.Revision != first.Revision {
		t.Fatalf("second InstallNodeBundle() = (%#v, %v)", second, err)
	}
	if err := VerifyNodeBundle(nodeState, bundle); err != nil {
		t.Fatalf("VerifyNodeBundle() error = %v", err)
	}
}

func TestInstallNodeBundleConcurrentPublicationHasOneCanonicalWinner(t *testing.T) {
	nodeState := newBundleNodeState(t)
	bundle, _ := assets.Load()
	start := make(chan struct{})
	results := make(chan NodeBundleReceipt, 2)
	errorsFound := make(chan error, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			receipt, err := InstallNodeBundle(nodeState, bundle)
			results <- receipt
			errorsFound <- err
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatalf("concurrent InstallNodeBundle() error = %v", err)
		}
	}
	var fresh int
	for result := range results {
		if !result.Replayed {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("fresh publication count = %d, want 1", fresh)
	}
	if err := VerifyNodeBundle(nodeState, bundle); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyNodeBundleRejectsDriftWithoutRepairingIt(t *testing.T) {
	tests := []struct {
		name   string
		tamper func(t *testing.T, root string)
	}{
		{name: "content", tamper: func(t *testing.T, root string) {
			t.Helper()
			if err := os.WriteFile(filepath.Join(root, "SKILL.md"), []byte("changed\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "mode", tamper: func(t *testing.T, root string) {
			t.Helper()
			if err := os.Chmod(filepath.Join(root, "SKILL.md"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "extra", tamper: func(t *testing.T, root string) {
			t.Helper()
			if err := os.WriteFile(filepath.Join(root, "extra"), []byte("unknown"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing", tamper: func(t *testing.T, root string) {
			t.Helper()
			if err := os.Remove(filepath.Join(root, "SKILL.md")); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nodeState := newBundleNodeState(t)
			bundle, _ := assets.Load()
			receipt, err := InstallNodeBundle(nodeState, bundle)
			if err != nil {
				t.Fatal(err)
			}
			test.tamper(t, receipt.Path)
			if err := VerifyNodeBundle(nodeState, bundle); !errors.Is(err, ErrProjectionConflict) {
				t.Fatalf("VerifyNodeBundle() error = %v", err)
			}
			if _, err := InstallNodeBundle(nodeState, bundle); !errors.Is(err, ErrProjectionConflict) {
				t.Fatalf("InstallNodeBundle() repaired drift: %v", err)
			}
		})
	}
}

func TestInstallNodeBundleRejectsUnsafeStateAndPreexistingRevision(t *testing.T) {
	bundle, _ := assets.Load()
	t.Run("state mode", func(t *testing.T) {
		nodeState := newBundleNodeState(t)
		if err := os.Chmod(nodeState, 0o755); err != nil {
			t.Fatal(err)
		}
		if _, err := InstallNodeBundle(nodeState, bundle); !errors.Is(err, ErrUnsafeProjection) {
			t.Fatalf("InstallNodeBundle() error = %v", err)
		}
	})
	t.Run("assets symlink", func(t *testing.T) {
		nodeState := newBundleNodeState(t)
		outside := t.TempDir()
		if err := os.Symlink(outside, filepath.Join(nodeState, "assets")); err != nil {
			t.Fatal(err)
		}
		if _, err := InstallNodeBundle(nodeState, bundle); !errors.Is(err, ErrUnsafeProjection) {
			t.Fatalf("InstallNodeBundle() error = %v", err)
		}
	})
	t.Run("unknown revision directory", func(t *testing.T) {
		nodeState := newBundleNodeState(t)
		root := filepath.Join(nodeState, "assets", bundle.Manifest().AssetRevision)
		if err := os.MkdirAll(root, 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := InstallNodeBundle(nodeState, bundle); !errors.Is(err, ErrProjectionConflict) {
			t.Fatalf("InstallNodeBundle() error = %v", err)
		}
	})
}

func newBundleNodeState(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}

func assertInstalledBundle(t *testing.T, root string, bundle assets.Bundle) {
	t.Helper()
	manifest, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil || string(manifest) != string(bundle.ManifestBytes()) {
		t.Fatalf("installed manifest differs: %v", err)
	}
	for _, record := range bundle.Manifest().Files {
		path := filepath.Join(root, filepath.FromSlash(record.Path))
		content, err := os.ReadFile(path)
		want, readErr := bundle.Read(record.Path)
		if err != nil || readErr != nil || string(content) != string(want) {
			t.Fatalf("installed %s differs: %v, %v", record.Path, err, readErr)
		}
	}
}
