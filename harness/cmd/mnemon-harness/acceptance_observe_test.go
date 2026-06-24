package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

func TestInspectAcceptanceRunReadsMnemondAndHubEvents(t *testing.T) {
	root := t.TempDir()
	sharedDB := filepath.Join(root, "local-workspace", ".mnemon", "harness", "local", "governed.db")
	codexDB := filepath.Join(root, "sync-arm", "workspaces", "codex-02", ".mnemon", "harness", "local", "governed.db")
	hubDB := filepath.Join(root, "sync-arm", "hub", "hub.db")
	writeInspectTestMnemondDB(t, sharedDB, "codex-01@project", false)
	writeInspectTestMnemondDB(t, codexDB, "sync@local", true)
	writeInspectTestHubDB(t, hubDB)
	writeFile(t, filepath.Join(filepath.Dir(codexDB), "render-audit.jsonl"), `{"AuditID":"render_1","Principal":"codex-02@project","RenderIntent":"teamwork.events","Status":"ok","PresentationCounts":{"work":1},"EventCounts":{"assignment.work_available":1},"CreatedAt":"2026-06-24T00:00:00Z"}`+"\n")
	writeFile(t, filepath.Join(root, "sync-arm", "hub", "sync-audit.jsonl"), "2026-06-24T00:00:00Z principal=replica-02@hub verb=sync.pull result=ok\n")

	report, err := inspectAcceptanceRun(root, 5)
	if err != nil {
		t.Fatalf("inspect acceptance run: %v", err)
	}
	if report.Topology.Mode != "mixed" {
		t.Fatalf("topology mode = %q, want mixed", report.Topology.Mode)
	}
	if !report.Topology.SharedMnemond || !report.Topology.PerHostagent {
		t.Fatalf("topology flags = shared:%t per:%t, want both true", report.Topology.SharedMnemond, report.Topology.PerHostagent)
	}
	if len(report.CrossEvents) != 1 {
		t.Fatalf("cross events = %d, want 1", len(report.CrossEvents))
	}
	gotImports := report.CrossEvents[0].ImportedBy
	if len(gotImports) != 1 || gotImports[0] != "codex-02" {
		t.Fatalf("imported_by = %#v, want [codex-02]", gotImports)
	}
	var codexStore *acceptanceStoreInspect
	for i := range report.Stores {
		if report.Stores[i].Name == "codex-02" {
			codexStore = &report.Stores[i]
			break
		}
	}
	if codexStore == nil {
		t.Fatalf("codex-02 store missing from report")
	}
	if codexStore.Counts["imported_accepted"] != 1 {
		t.Fatalf("imported_accepted = %d, want 1", codexStore.Counts["imported_accepted"])
	}
	if codexStore.RenderAudit == nil || codexStore.RenderAudit.Entries != 1 {
		t.Fatalf("render audit = %#v, want one entry", codexStore.RenderAudit)
	}
}

func writeInspectTestMnemondDB(t *testing.T, path, actor string, imported bool) {
	t.Helper()
	db := openInspectTestDB(t, path)
	defer db.Close()
	execInspectTestSQL(t, db, `CREATE TABLE events (ingest_seq INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL);`)
	execInspectTestSQL(t, db, `CREATE TABLE event_envelopes (seq INTEGER PRIMARY KEY AUTOINCREMENT, schema_version INTEGER NOT NULL, phase TEXT NOT NULL, event_id TEXT NOT NULL, event_type TEXT NOT NULL, subject TEXT NOT NULL, actor TEXT NOT NULL, audience TEXT NOT NULL DEFAULT '', correlation_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL DEFAULT '', decision_id TEXT NOT NULL DEFAULT '', envelope TEXT NOT NULL);`)
	execInspectTestSQL(t, db, `CREATE TABLE sync_events (origin_replica_id TEXT NOT NULL, local_decision_id TEXT NOT NULL, local_ingest_seq INTEGER NOT NULL, actor TEXT NOT NULL, correlation_id TEXT NOT NULL DEFAULT '', resource_kind TEXT NOT NULL, resource_id TEXT NOT NULL, resource_version INTEGER NOT NULL, fields_digest TEXT NOT NULL, fields TEXT NOT NULL, decided_at TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'pending', remote_peer_id TEXT NOT NULL DEFAULT '', acked_at TEXT NOT NULL DEFAULT '', diagnostic TEXT NOT NULL DEFAULT '', PRIMARY KEY(origin_replica_id, local_decision_id, resource_kind, resource_id));`)
	eventType := "assignment.write_candidate.observed"
	payload := `{"type":"` + eventType + `","actor":"` + actor + `","correlation_id":"corr-1","ts":"2026-06-24T00:00:00Z"}`
	if imported {
		eventType = "assignment.remote_synced_event.observed"
		payload = `{"type":"` + eventType + `","actor":"` + actor + `","correlation_id":"corr-1","ts":"2026-06-24T00:00:00Z","payload":{"material":{"OriginReplicaID":"local-a","LocalDecisionID":"dec-1"}}}`
	}
	execInspectTestSQL(t, db, `INSERT INTO events (payload) VALUES (?)`, payload)
	execInspectTestSQL(t, db, `INSERT INTO event_envelopes (schema_version, phase, event_id, event_type, subject, actor, correlation_id, created_at, decision_id, envelope) VALUES (1, 'accepted', 'evt-1', 'assignment.accepted', 'assignment/project', ?, 'corr-1', '2026-06-24T00:00:00Z', 'dec-1', '{}')`, actor)
	if !imported {
		execInspectTestSQL(t, db, `INSERT INTO sync_events (origin_replica_id, local_decision_id, local_ingest_seq, actor, resource_kind, resource_id, resource_version, fields_digest, fields, status) VALUES ('local-a', 'dec-1', 1, ?, 'assignment', 'project', 1, 'sha256:test', '{}', 'synced')`, actor)
	}
}

func writeInspectTestHubDB(t *testing.T, path string) {
	t.Helper()
	db := openInspectTestDB(t, path)
	defer db.Close()
	execInspectTestSQL(t, db, `CREATE TABLE sync_remote_events (remote_seq INTEGER PRIMARY KEY AUTOINCREMENT, remote_peer_id TEXT NOT NULL, origin_replica_id TEXT NOT NULL, local_decision_id TEXT NOT NULL, local_ingest_seq INTEGER NOT NULL, actor TEXT NOT NULL, correlation_id TEXT NOT NULL DEFAULT '', resource_kind TEXT NOT NULL, resource_id TEXT NOT NULL, resource_version INTEGER NOT NULL, fields_digest TEXT NOT NULL, fields TEXT NOT NULL, decided_at TEXT NOT NULL DEFAULT '', received_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'accepted', diagnostic TEXT NOT NULL DEFAULT '', UNIQUE(remote_peer_id, origin_replica_id, local_decision_id));`)
	execInspectTestSQL(t, db, `INSERT INTO sync_remote_events (remote_peer_id, origin_replica_id, local_decision_id, local_ingest_seq, actor, resource_kind, resource_id, resource_version, fields_digest, fields, decided_at, received_at, status) VALUES ('replica-01@hub', 'local-a', 'dec-1', 1, 'codex-01@project', 'assignment', 'project', 1, 'sha256:test', '{}', '2026-06-24T00:00:00Z', '2026-06-24T00:00:01Z', 'accepted')`)
}

func openInspectTestDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func execInspectTestSQL(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

func writeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}
