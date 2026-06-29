package daemon

import (
	"path/filepath"
	"testing"
	"time"
)

func TestFileCursorStoreResumesCursors(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	path := filepath.Join(t.TempDir(), "daemon", "cursors.json")
	store := NewFileCursorStore(path, func() time.Time { return now })
	if _, ok, err := store.Load("multica-watch"); err != nil || ok {
		t.Fatalf("initial load = ok=%v err=%v", ok, err)
	}
	if err := store.Save("multica-watch", "issue:cursor:7"); err != nil {
		t.Fatal(err)
	}

	resumed := NewFileCursorStore(path, func() time.Time { return now.Add(time.Hour) })
	record, ok, err := resumed.Load("multica-watch")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || record.Cursor != "issue:cursor:7" || !record.UpdatedAt.Equal(now) {
		t.Fatalf("resumed cursor = %+v ok=%v", record, ok)
	}
}

func TestFileCursorStoreValidatesWorkerName(t *testing.T) {
	store := NewFileCursorStore(filepath.Join(t.TempDir(), "cursors.json"), nil)
	if err := store.Save(" ", "cursor"); err == nil {
		t.Fatal("expected save to reject empty worker name")
	}
	if _, _, err := store.Load(" "); err == nil {
		t.Fatal("expected load to reject empty worker name")
	}
}

func TestBackoffPolicyCapsRetries(t *testing.T) {
	policy := BackoffPolicy{Base: time.Second, Max: 5 * time.Second}
	for _, tc := range []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: -1, want: time.Second},
		{attempt: 0, want: time.Second},
		{attempt: 1, want: 2 * time.Second},
		{attempt: 2, want: 4 * time.Second},
		{attempt: 3, want: 5 * time.Second},
		{attempt: 10, want: 5 * time.Second},
	} {
		if got := policy.Duration(tc.attempt); got != tc.want {
			t.Fatalf("Duration(%d) = %s, want %s", tc.attempt, got, tc.want)
		}
	}
}

func TestInFlightGuardSuppressesDuplicateWork(t *testing.T) {
	guard := NewInFlightGuard()
	if guard.TryStart("") {
		t.Fatal("empty key must not start")
	}
	if !guard.TryStart("projection:root-1") {
		t.Fatal("first start should win")
	}
	if guard.TryStart("projection:root-1") {
		t.Fatal("duplicate start should be suppressed")
	}
	guard.Done("projection:root-1")
	if !guard.TryStart("projection:root-1") {
		t.Fatal("completed key should be startable again")
	}
}
