package state

import (
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// Round-2 MED: OpenStore must tolerate a decisions table created by older code (no correlation_id /
// next_action columns). CREATE TABLE IF NOT EXISTS is a no-op on a pre-existing table, so without an
// additive migration, inserts/queries against the new columns fail — no decision can be persisted
// (Invariant #7) and DeferralCount silently returns 0 (defeating Invariant #10).
func TestOpenStoreMigratesOldDecisionsSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "coreplane.db")
	// Simulate the OLD on-disk schema: decisions without correlation_id / next_action.
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := raw.Exec(`CREATE TABLE decisions (decision_id TEXT PRIMARY KEY, op_id TEXT, ingest_seq INTEGER, actor TEXT, status TEXT, payload TEXT NOT NULL);`); err != nil {
		t.Fatalf("seed old schema: %v", err)
	}
	raw.Close()

	// Reopen through OpenStore — it must migrate the added columns.
	s, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore on old-schema db: %v", err)
	}
	defer s.Close()
	d := contract.Decision{DecisionID: "d1", OpID: "o1", IngestSeq: 1, Actor: "a", CorrelationID: "c", Status: contract.Deferred, NextAction: "rebase"}
	if err := s.AppendDecision(d); err != nil {
		t.Fatalf("AppendDecision after migration must succeed, got: %v", err)
	}
	if got := s.DeferralCount("c"); got != 1 {
		t.Fatalf("DeferralCount after migration = %d, want 1", got)
	}
}
