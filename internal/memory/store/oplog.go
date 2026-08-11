package store

import (
	"fmt"
	"os"
	"time"
)

// MaxOplogEntries is the maximum number of oplog entries to retain.
const MaxOplogEntries = 5000

// LogOp records an operation to the oplog and trims old entries beyond MaxOplogEntries.
// Best-effort: failures are logged to stderr but do not propagate.
// No-op when the database is read-only.
func (db *DB) LogOp(operation, insightID, detail string) {
	if db.readOnly {
		return
	}
	if _, err := db.execer().Exec(
		`INSERT INTO oplog (operation, insight_id, detail, created_at) VALUES (?, ?, ?, ?)`,
		operation, insightID, detail, time.Now().UTC().Format(time.RFC3339)); err != nil {
		fmt.Fprintf(os.Stderr, "warning: oplog insert: %v\n", err)
	}

	// Trim old entries: only deletes when count exceeds limit (O(1) in the common case).
	if _, err := db.execer().Exec(
		`DELETE FROM oplog WHERE id <= (SELECT MAX(id) FROM oplog) - ?`,
		MaxOplogEntries); err != nil {
		fmt.Fprintf(os.Stderr, "warning: oplog trim: %v\n", err)
	}
}

// OplogEntry represents a single operation log entry.
type OplogEntry struct {
	ID        int    `json:"id"`
	Operation string `json:"operation"`
	InsightID string `json:"insight_id,omitempty"`
	Detail    string `json:"detail,omitempty"`
	CreatedAt string `json:"created_at"`
}

// GetOplog returns the most recent N oplog entries.
func (db *DB) GetOplog(limit int) ([]OplogEntry, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := db.execer().Query(
		`SELECT id, operation, insight_id, detail, created_at FROM oplog ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	entries := make([]OplogEntry, 0)
	for rows.Next() {
		var e OplogEntry
		if err := rows.Scan(&e.ID, &e.Operation, &e.InsightID, &e.Detail, &e.CreatedAt); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}
