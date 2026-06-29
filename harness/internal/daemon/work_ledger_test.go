package daemon

import (
	"path/filepath"
	"testing"
	"time"
)

func TestFileWorkLedgerPersistsProjectionAndWakeRecords(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	path := filepath.Join(t.TempDir(), "daemon", "work-ledger.json")
	ledger := NewFileWorkLedger(path, func() time.Time { return now })
	if seen, err := ledger.Seen(WorkKindProjection, "multica:comment:root-1"); err != nil || seen {
		t.Fatalf("initial seen = %v err=%v", seen, err)
	}
	if err := ledger.Record(WorkLedgerRecord{
		Kind:    WorkKindProjection,
		Key:     "multica:comment:root-1",
		Status:  "completed",
		Message: "comment=comment-1",
	}); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Record(WorkLedgerRecord{
		Kind:     WorkKindWake,
		Key:      "managed:assignment:asg-1",
		Status:   "failed",
		Attempts: 2,
		Error:    "turn timeout",
	}); err != nil {
		t.Fatal(err)
	}

	resumed := NewFileWorkLedger(path, func() time.Time { return now.Add(time.Hour) })
	projection, ok, err := resumed.Load(WorkKindProjection, "multica:comment:root-1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || projection.Status != "completed" || projection.Message != "comment=comment-1" || !projection.CreatedAt.Equal(now) || !projection.UpdatedAt.Equal(now) {
		t.Fatalf("projection record = %+v ok=%v", projection, ok)
	}
	wake, ok, err := resumed.Load(WorkKindWake, "managed:assignment:asg-1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || wake.Status != "failed" || wake.Attempts != 2 || wake.Error != "turn timeout" {
		t.Fatalf("wake record = %+v ok=%v", wake, ok)
	}
}

func TestFileWorkLedgerDedupesByKindAndKey(t *testing.T) {
	now := time.Unix(100, 0).UTC()
	later := now.Add(time.Hour)
	path := filepath.Join(t.TempDir(), "work-ledger.json")
	clock := now
	ledger := NewFileWorkLedger(path, func() time.Time { return clock })
	if err := ledger.Record(WorkLedgerRecord{Kind: WorkKindProjection, Key: "target-1", Status: "started"}); err != nil {
		t.Fatal(err)
	}
	clock = later
	if err := ledger.Record(WorkLedgerRecord{Kind: WorkKindProjection, Key: "target-1", Status: "completed"}); err != nil {
		t.Fatal(err)
	}
	records, err := NewFileWorkLedger(path, nil).Records(WorkKindProjection)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %+v, want one deduped record", records)
	}
	if records[0].Status != "completed" || !records[0].CreatedAt.Equal(now) || !records[0].UpdatedAt.Equal(later) {
		t.Fatalf("deduped record = %+v", records[0])
	}
}

func TestFileWorkLedgerValidatesKindAndKey(t *testing.T) {
	ledger := NewFileWorkLedger(filepath.Join(t.TempDir(), "work-ledger.json"), nil)
	if err := ledger.Record(WorkLedgerRecord{Kind: " ", Key: "target"}); err == nil {
		t.Fatal("expected record to reject empty kind")
	}
	if err := ledger.Record(WorkLedgerRecord{Kind: WorkKindWake, Key: " "}); err == nil {
		t.Fatal("expected record to reject empty key")
	}
	if _, _, err := ledger.Load(" ", "target"); err == nil {
		t.Fatal("expected load to reject empty kind")
	}
}
