package session

import (
	"path/filepath"
	"testing"
	"time"
)

func TestFileStoreStartsAndLoadsSession(t *testing.T) {
	now := time.Date(2026, 6, 29, 10, 0, 0, 0, time.UTC)
	store := NewFileStore(DefaultDir(t.TempDir(), ""), func() time.Time { return now })
	record, err := store.Start(Record{
		ID:                        "release-readiness",
		Title:                     "Release readiness",
		PrimaryActivationCarrier:  "multica",
		DuplicateActivationPolicy: "suppress",
	})
	if err != nil {
		t.Fatal(err)
	}
	if record.SchemaVersion != SchemaVersion || record.ID != "release-readiness" || !record.CreatedAt.Equal(now) || !record.UpdatedAt.Equal(now) {
		t.Fatalf("started record = %+v", record)
	}

	loaded, err := store.Load("release-readiness")
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Title != "Release readiness" || loaded.PrimaryActivationCarrier != "multica" {
		t.Fatalf("loaded record = %+v", loaded)
	}
}

func TestFileStoreAttachDedupesAndUpdatesPrimaryCarrier(t *testing.T) {
	now := time.Date(2026, 6, 29, 10, 0, 0, 0, time.UTC)
	current := now
	store := NewFileStore(DefaultDir(t.TempDir(), ""), func() time.Time { return current })
	if _, err := store.Start(Record{ID: "session-1", PrimaryActivationCarrier: "local"}); err != nil {
		t.Fatal(err)
	}
	current = now.Add(time.Minute)
	record, err := store.Attach("session-1", Attachment{Surface: "multica", ExternalRef: "issue/root-1", Primary: true})
	if err != nil {
		t.Fatal(err)
	}
	current = now.Add(2 * time.Minute)
	record, err = store.Attach("session-1", Attachment{Surface: "multica", ExternalRef: "issue/root-1", Primary: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Attachments) != 1 {
		t.Fatalf("attachments = %+v, want one deduped attachment", record.Attachments)
	}
	if record.PrimaryActivationCarrier != "multica" || !record.UpdatedAt.Equal(current) {
		t.Fatalf("record after attach = %+v", record)
	}
	if !record.Attachments[0].AttachedAt.Equal(current) {
		t.Fatalf("attachment timestamp = %s, want %s", record.Attachments[0].AttachedAt, current)
	}
}

func TestFileStoreValidatesSessionIDsAndAttachments(t *testing.T) {
	store := NewFileStore(filepath.Join(t.TempDir(), "sessions"), nil)
	if _, err := store.Start(Record{ID: " "}); err == nil {
		t.Fatal("expected start to reject empty id")
	}
	if _, err := store.Start(Record{ID: "bad/id"}); err == nil {
		t.Fatal("expected start to reject path separators")
	}
	if _, err := store.Start(Record{ID: "session-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Attach("session-1", Attachment{Surface: "", ExternalRef: "issue/root-1"}); err == nil {
		t.Fatal("expected attach to reject empty surface")
	}
	if _, err := store.Attach("session-1", Attachment{Surface: "multica", ExternalRef: ""}); err == nil {
		t.Fatal("expected attach to reject empty external ref")
	}
}
