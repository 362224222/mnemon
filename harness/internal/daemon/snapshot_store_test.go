package daemon

import (
	"path/filepath"
	"testing"
	"time"
)

func TestFileSnapshotStorePersistsDaemonStatus(t *testing.T) {
	root := t.TempDir()
	path := StatusSnapshotPath(root, "")
	started := time.Unix(100, 0).UTC()
	updated := started.Add(time.Minute)
	store := NewFileSnapshotStore(path)
	if snapshot, ok, err := store.Load(); err != nil || ok || len(snapshot.Workers) != 0 {
		t.Fatalf("initial load = snapshot=%+v ok=%v err=%v", snapshot, ok, err)
	}
	if err := store.Save(Snapshot{
		StartedAt: started,
		Workers: map[string]WorkerSnapshot{
			"multica-watch": {
				Kind:      WorkerInteraction,
				Status:    "idle",
				Message:   "cursor=7",
				StartedAt: started,
				UpdatedAt: updated,
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	resumed := NewFileSnapshotStore(path)
	snapshot, ok, err := resumed.Load()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("saved snapshot should load")
	}
	worker := snapshot.Workers["multica-watch"]
	if !snapshot.StartedAt.Equal(started) || worker.Kind != WorkerInteraction || worker.Status != "idle" || worker.Message != "cursor=7" || !worker.UpdatedAt.Equal(updated) {
		t.Fatalf("loaded snapshot = %+v", snapshot)
	}
}

func TestStatusSnapshotPathHonorsExplicitPath(t *testing.T) {
	explicit := filepath.Join(t.TempDir(), "status.json")
	if got := StatusSnapshotPath("/ignored", explicit); got != explicit {
		t.Fatalf("StatusSnapshotPath explicit = %q, want %q", got, explicit)
	}
}

func TestFileSnapshotStoreRequiresPath(t *testing.T) {
	if _, _, err := NewFileSnapshotStore(" ").Load(); err == nil {
		t.Fatal("expected load to reject empty path")
	}
	if err := NewFileSnapshotStore(" ").Save(Snapshot{}); err == nil {
		t.Fatal("expected save to reject empty path")
	}
}
