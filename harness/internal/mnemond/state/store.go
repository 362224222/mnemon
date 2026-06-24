package state

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	_ "modernc.org/sqlite"
)

type Store struct {
	db      *sql.DB
	release func() error // single-writer lock release; no-op for :memory: (S11)
}
type Tx struct{ tx *sql.Tx }

// dsnFor builds the connection DSN. File-backed stores pin synchronous(FULL) (durability: every commit
// fsyncs before returning, closing the WAL crash-window) on top of busy_timeout + WAL. :memory: stays bare.
func dsnFor(path string) string {
	if path == ":memory:" {
		return path
	}
	return path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)"
}

func OpenStore(path string) (*Store, error) {
	// S11: single-writer lock + anti-NFS guard (skipped for :memory:). Acquire BEFORE opening the DB so a
	// second writer is rejected before it can touch the WAL.
	release, err := openGuard(path, defaultStatFS)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dsnFor(path))
	if err != nil {
		_ = release()
		return nil, err
	}
	db.SetMaxOpenConns(1) // kernel is the sole serializer (Invariant #2): one conn => no lock races, no per-conn :memory: split
	for _, s := range []string{
		`CREATE TABLE IF NOT EXISTS resources (kind TEXT, id TEXT, version INTEGER NOT NULL, fields TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(kind,id));`,
		`CREATE TABLE IF NOT EXISTS events (ingest_seq INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL);`,
		`CREATE TABLE IF NOT EXISTS decisions (decision_id TEXT PRIMARY KEY, op_id TEXT, ingest_seq INTEGER, actor TEXT, correlation_id TEXT, next_action TEXT, status TEXT, payload TEXT NOT NULL);`,
		`CREATE TABLE IF NOT EXISTS cursors (name TEXT PRIMARY KEY, seq INTEGER NOT NULL);`,
		`CREATE TABLE IF NOT EXISTS inbox_dedupe (source TEXT, external_id TEXT, event_seq INTEGER NOT NULL, PRIMARY KEY(source,external_id));`,
		`CREATE TABLE IF NOT EXISTS event_envelopes (seq INTEGER PRIMARY KEY AUTOINCREMENT, schema_version INTEGER NOT NULL, phase TEXT NOT NULL, event_id TEXT NOT NULL, event_type TEXT NOT NULL, subject TEXT NOT NULL, actor TEXT NOT NULL, audience TEXT NOT NULL DEFAULT '', correlation_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL DEFAULT '', decision_id TEXT NOT NULL DEFAULT '', envelope TEXT NOT NULL);`,
		`CREATE INDEX IF NOT EXISTS event_envelopes_phase_idx ON event_envelopes(phase, seq);`,
		`CREATE INDEX IF NOT EXISTS event_envelopes_type_idx ON event_envelopes(event_type, seq);`,
		`CREATE INDEX IF NOT EXISTS event_envelopes_subject_idx ON event_envelopes(subject, seq);`,
		`CREATE INDEX IF NOT EXISTS event_envelopes_actor_idx ON event_envelopes(actor, seq);`,
		`CREATE INDEX IF NOT EXISTS event_envelopes_audience_idx ON event_envelopes(audience, seq);`,
		`CREATE INDEX IF NOT EXISTS event_envelopes_correlation_idx ON event_envelopes(correlation_id, seq);`,
		`CREATE TABLE IF NOT EXISTS outbox (id TEXT PRIMARY KEY, kind TEXT NOT NULL, event_seq INTEGER NOT NULL DEFAULT 0, target TEXT NOT NULL DEFAULT '', payload TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'pending', idempotency_key TEXT UNIQUE, attempts INTEGER NOT NULL DEFAULT 0, lease_owner TEXT NOT NULL DEFAULT '', lease_until INTEGER NOT NULL DEFAULT 0);`,
		`CREATE TABLE IF NOT EXISTS sync_replica (id INTEGER PRIMARY KEY CHECK (id=1), replica_id TEXT NOT NULL, created_at TEXT NOT NULL);`,
		`CREATE TABLE IF NOT EXISTS sync_events (origin_replica_id TEXT NOT NULL, local_decision_id TEXT NOT NULL, local_ingest_seq INTEGER NOT NULL, actor TEXT NOT NULL, correlation_id TEXT NOT NULL DEFAULT '', resource_kind TEXT NOT NULL, resource_id TEXT NOT NULL, resource_version INTEGER NOT NULL, fields_digest TEXT NOT NULL, fields TEXT NOT NULL, decided_at TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'pending', remote_peer_id TEXT NOT NULL DEFAULT '', acked_at TEXT NOT NULL DEFAULT '', diagnostic TEXT NOT NULL DEFAULT '', PRIMARY KEY(origin_replica_id, local_decision_id, resource_kind, resource_id));`,
		`CREATE TABLE IF NOT EXISTS sync_remote_events (remote_seq INTEGER PRIMARY KEY AUTOINCREMENT, remote_peer_id TEXT NOT NULL, origin_replica_id TEXT NOT NULL, local_decision_id TEXT NOT NULL, local_ingest_seq INTEGER NOT NULL, actor TEXT NOT NULL, correlation_id TEXT NOT NULL DEFAULT '', resource_kind TEXT NOT NULL, resource_id TEXT NOT NULL, resource_version INTEGER NOT NULL, fields_digest TEXT NOT NULL, fields TEXT NOT NULL, decided_at TEXT NOT NULL DEFAULT '', received_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'accepted', diagnostic TEXT NOT NULL DEFAULT '', UNIQUE(remote_peer_id, origin_replica_id, local_decision_id));`,
	} {
		if _, err := db.Exec(s); err != nil {
			db.Close()
			_ = release()
			return nil, err
		}
	}
	// Additive migrations for columns introduced after the initial schema, so OpenStore tolerates a
	// decisions table created by older code (CREATE TABLE IF NOT EXISTS is a no-op on an existing table).
	// ALTER ... ADD COLUMN is idempotent here: a "duplicate column" error (fresh DB already has it) is ignored.
	for _, col := range []string{"correlation_id TEXT", "next_action TEXT"} {
		if _, err := db.Exec(`ALTER TABLE decisions ADD COLUMN ` + col); err != nil && !strings.Contains(err.Error(), "duplicate column") {
			db.Close()
			_ = release()
			return nil, err
		}
	}
	for _, col := range []string{"decision_id TEXT NOT NULL DEFAULT ''"} {
		if _, err := db.Exec(`ALTER TABLE event_envelopes ADD COLUMN ` + col); err != nil && !strings.Contains(err.Error(), "duplicate column") {
			db.Close()
			_ = release()
			return nil, err
		}
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS event_envelopes_decision_idx ON event_envelopes(decision_id, seq);`); err != nil {
		db.Close()
		_ = release()
		return nil, err
	}
	return &Store{db: db, release: release}, nil
}

func (s *Store) ReplicaID() (string, error) {
	var replicaID string
	err := s.WithTx(func(tx *Tx) error {
		id, err := tx.ensureReplicaID()
		if err != nil {
			return err
		}
		replicaID = id
		return nil
	})
	return replicaID, err
}

func (t *Tx) ensureReplicaID() (string, error) {
	var replicaID string
	err := t.tx.QueryRow(`SELECT replica_id FROM sync_replica WHERE id=1`).Scan(&replicaID)
	if err == nil {
		return replicaID, nil
	}
	if err != sql.ErrNoRows {
		return "", err
	}
	replicaID = "local-" + uuid.NewString()
	_, err = t.tx.Exec(`INSERT INTO sync_replica (id, replica_id, created_at) VALUES (1, ?, ?)`, replicaID, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		return "", err
	}
	return replicaID, nil
}
func (s *Store) Close() error {
	err := s.db.Close()
	if s.release != nil {
		if rerr := s.release(); err == nil {
			err = rerr
		}
	}
	return err
}

func (s *Store) WithTx(fn func(*Tx) error) error { // the atomic boundary: check+write are one op (Invariant #3,#5)
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	if err := fn(&Tx{tx: tx}); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}
func (s *Store) GetVersion(ref contract.ResourceRef) (contract.Version, error) {
	var v contract.Version
	err := s.db.QueryRow(`SELECT version FROM resources WHERE kind=? AND id=?`, string(ref.Kind), string(ref.ID)).Scan(&v)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return v, err
}
func (t *Tx) CreateResource(ref contract.ResourceRef, fields map[string]any) error {
	b, err := json.Marshal(fields)
	if err != nil {
		return err
	} // propagate, never write "" silently
	_, err = t.tx.Exec(`INSERT INTO resources (kind,id,version,fields,updated_at) VALUES (?,?,1,?,?)`,
		string(ref.Kind), string(ref.ID), string(b), time.Now().UTC().Format(time.RFC3339))
	return err // PK violation => already exists => caller treats as conflict
}
func (t *Tx) CASUpdate(ref contract.ResourceRef, basedOn contract.Version, fields map[string]any) (bool, error) {
	b, err := json.Marshal(fields)
	if err != nil {
		return false, err
	}
	res, err := t.tx.Exec(`UPDATE resources SET fields=?, version=version+1, updated_at=? WHERE kind=? AND id=? AND version=?`,
		string(b), time.Now().UTC().Format(time.RFC3339), string(ref.Kind), string(ref.ID), int64(basedOn))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}
func (t *Tx) ReadVersion(ref contract.ResourceRef) (contract.Version, error) {
	var v contract.Version
	err := t.tx.QueryRow(`SELECT version FROM resources WHERE kind=? AND id=?`, string(ref.Kind), string(ref.ID)).Scan(&v)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return v, err
}

// GetResource returns a resource's current version AND decoded field content (review #5). The content digest
// (D8/S10), budget reserve (S6), and lease TTL read all need the fields, not just the version. Absent ->
// (0, nil, nil), consistent with GetVersion.
func (s *Store) GetResource(ref contract.ResourceRef) (contract.Version, map[string]any, error) {
	return scanResource(s.db.QueryRow(`SELECT version, fields FROM resources WHERE kind=? AND id=?`, string(ref.Kind), string(ref.ID)))
}

// ReadResource is GetResource inside a caller's txn — required by the read-modify-write lease/budget claims
// (S5/S6), which read the current version+fields and CAS in the same transaction.
func (t *Tx) ReadResource(ref contract.ResourceRef) (contract.Version, map[string]any, error) {
	return scanResource(t.tx.QueryRow(`SELECT version, fields FROM resources WHERE kind=? AND id=?`, string(ref.Kind), string(ref.ID)))
}

// scanResource decodes a (version, fields) row, mapping ErrNoRows to the absent (0, nil, nil) form. The
// rowScanner seam lets both the Store and Tx variants share decode + JSON-unmarshal logic.
func scanResource(row rowScanner) (contract.Version, map[string]any, error) {
	var v contract.Version
	var b string
	if err := row.Scan(&v, &b); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil, nil
		}
		return 0, nil, err
	}
	var fields map[string]any
	if err := json.Unmarshal([]byte(b), &fields); err != nil {
		return 0, nil, err
	}
	return v, fields, nil
}

type rowScanner interface{ Scan(dest ...any) error }

// AppendEventReturningSeq appends an event INSIDE a caller's txn and returns its durable LSN (events.rowid).
// IngestObservation (S1) needs append + LSN-read in one transaction so the dedupe row records the same seq.
func (t *Tx) AppendEventReturningSeq(ev contract.Event) (int64, error) {
	b, err := json.Marshal(ev) // never write a garbage payload silently (mirrors Store.AppendEvent)
	if err != nil {
		return 0, err
	}
	res, err := t.tx.Exec(`INSERT INTO events (payload) VALUES (?)`, string(b))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// IngestObservation appends an observation exactly once per (Source,ExternalID): a retried envelope returns
// the original seq with dup=true and never double-appends (S1). The dedupe lookup, the event append (with its
// LSN via AppendEventReturningSeq, review #6), and the dedupe-row insert are ONE transaction; the single
// writer connection serializes concurrent ingests, so the SELECT-then-INSERT cannot race. The server stamps
// env.Event.Actor from the authenticated principal BEFORE calling (D7) — the store trusts the envelope.
func (s *Store) IngestObservation(env contract.ObservationEnvelope) (int64, bool, error) {
	// An idempotency key is REQUIRED for exactly-once ingest (S1): an empty ExternalID would make every
	// keyless observation from a source collapse onto one dedupe row, silently dropping all but the first.
	if env.ExternalID == "" {
		return 0, false, fmt.Errorf("ingest: empty external_id (an idempotency key is required for exactly-once ingest, S1)")
	}
	var seq int64
	var dup bool
	err := s.WithTx(func(tx *Tx) error {
		existing, found, e := tx.lookupDedupe(env.Source, env.ExternalID)
		if e != nil {
			return e
		}
		if found {
			seq, dup = existing, true
			return nil
		}
		s2, e := tx.AppendEventReturningSeq(env.Event)
		if e != nil {
			return e
		}
		if e := tx.insertDedupe(env.Source, env.ExternalID, s2); e != nil {
			return e
		}
		seq = s2
		return nil
	})
	return seq, dup, err
}

func (t *Tx) lookupDedupe(source contract.ActorID, externalID string) (int64, bool, error) {
	var seq int64
	err := t.tx.QueryRow(`SELECT event_seq FROM inbox_dedupe WHERE source=? AND external_id=?`, string(source), externalID).Scan(&seq)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return seq, true, nil
}
func (t *Tx) insertDedupe(source contract.ActorID, externalID string, seq int64) error {
	_, err := t.tx.Exec(`INSERT INTO inbox_dedupe (source, external_id, event_seq) VALUES (?,?,?)`, string(source), externalID, seq)
	return err
}

type EventEnvelopeRecord struct {
	Seq      int64
	Envelope eventmodel.EventEnvelope
}

type EventEnvelopeQuery struct {
	AfterSeq      int64
	Phase         eventmodel.EventPhase
	Type          string
	Subject       eventmodel.EventSubject
	Actor         string
	Audience      string
	CorrelationID string
	Limit         int
}

func (s *Store) AppendEventEnvelope(env eventmodel.EventEnvelope) (int64, error) {
	var seq int64
	err := s.WithTx(func(tx *Tx) error {
		var err error
		seq, err = tx.AppendEventEnvelopeReturningSeq(env)
		return err
	})
	return seq, err
}

func (t *Tx) AppendEventEnvelopeReturningSeq(env eventmodel.EventEnvelope) (int64, error) {
	if err := env.Validate(); err != nil {
		return 0, err
	}
	b, err := json.Marshal(env)
	if err != nil {
		return 0, err
	}
	res, err := t.tx.Exec(`
INSERT INTO event_envelopes
  (schema_version, phase, event_id, event_type, subject, actor, audience, correlation_id, created_at, decision_id, envelope)
VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		env.SchemaVersion, string(env.Phase), env.Event.ID, env.Event.Type, string(env.Event.Subject),
		env.Event.Actor, env.Event.Audience, env.Event.CorrelationID, env.Event.CreatedAt, envelopeDecisionID(env), string(b))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *Store) EventEnvelopes(q EventEnvelopeQuery) ([]EventEnvelopeRecord, error) {
	where := []string{"seq>?"}
	args := []any{q.AfterSeq}
	if q.Phase != "" {
		where = append(where, "phase=?")
		args = append(args, string(q.Phase))
	}
	if q.Type != "" {
		where = append(where, "event_type=?")
		args = append(args, q.Type)
	}
	if q.Subject != "" {
		where = append(where, "subject=?")
		args = append(args, string(q.Subject))
	}
	if q.Actor != "" {
		where = append(where, "actor=?")
		args = append(args, q.Actor)
	}
	if q.Audience != "" {
		where = append(where, "audience=?")
		args = append(args, q.Audience)
	}
	if q.CorrelationID != "" {
		where = append(where, "correlation_id=?")
		args = append(args, q.CorrelationID)
	}
	limit := q.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	args = append(args, limit)
	rows, err := s.db.Query(`
SELECT seq, envelope
FROM event_envelopes
WHERE `+strings.Join(where, " AND ")+`
ORDER BY seq
LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EventEnvelopeRecord
	for rows.Next() {
		var rec EventEnvelopeRecord
		var payload string
		if err := rows.Scan(&rec.Seq, &payload); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(payload), &rec.Envelope); err != nil {
			return nil, err
		}
		if err := rec.Envelope.Validate(); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

func envelopeDecisionID(env eventmodel.EventEnvelope) string {
	if env.Phase != eventmodel.PhaseAccepted {
		return ""
	}
	decisionID, _ := env.Meta["decision_id"].(string)
	return strings.TrimSpace(decisionID)
}

// OutboxRow is one pending external effect (a projection invalidation, a job to run). The outbox is the
// transactional-outbox substrate (S2: an invalidation is produced from the durable decision log under a sink
// cursor, so it survives a crash between the decision commit and its side-effect — RECOVERABLE, not lost;
// S4: delivery is at-least-once with a per-row lease + an idempotency key, NEVER exactly-once).
type OutboxRow struct {
	ID             string
	Kind           string
	EventSeq       int64
	Target         string
	Payload        string
	Status         string
	IdempotencyKey string
	Attempts       int
	LeaseOwner     string
	LeaseUntil     int64 // unix seconds; 0 = unleased
}

// EnqueueOutbox inserts a pending effect INSIDE a caller's txn so it commits atomically with the decision
// (S2). A duplicate idempotency key is a silent no-op (S4 — at-least-once delivery must never enqueue the
// same effect twice). An empty key is stored as NULL: multiple NULLs are distinct in a UNIQUE index, so
// keyless rows (e.g. invalidations) never collide with each other.
func (t *Tx) EnqueueOutbox(row OutboxRow) error {
	var key any
	if row.IdempotencyKey != "" {
		key = row.IdempotencyKey
	}
	status := row.Status
	if status == "" {
		status = "pending"
	}
	_, err := t.tx.Exec(
		`INSERT INTO outbox (id,kind,event_seq,target,payload,status,idempotency_key,attempts,lease_owner,lease_until)
		 VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(idempotency_key) DO NOTHING`,
		row.ID, row.Kind, row.EventSeq, row.Target, row.Payload, status, key, row.Attempts, row.LeaseOwner, row.LeaseUntil)
	return err
}

// ClaimOutbox leases every currently-claimable row (not acked, and either unleased or with an expired lease)
// to owner for ttl, bumping attempts, and returns them. The single writer connection serializes claims, so
// two workers never both win the same row (S4 delivery lease). Rows are read fully before the UPDATE so the
// single connection is not held by an open cursor during the writes. If kinds is non-empty, ONLY rows of
// those kinds are claimed — a delivery worker must lease only the rows it actually delivers (one kind's
// worker never grabs another kind's rows); empty kinds claims every kind.
func (s *Store) ClaimOutbox(owner string, ttl time.Duration, kinds ...string) ([]OutboxRow, error) {
	now := time.Now().Unix()
	until := now + int64(ttl/time.Second)
	where := `status!='acked' AND (lease_owner='' OR lease_until<=?)`
	args := []any{now}
	if len(kinds) > 0 {
		ph := make([]string, len(kinds))
		for i, k := range kinds {
			ph[i] = "?"
			args = append(args, k)
		}
		where += ` AND kind IN (` + strings.Join(ph, ",") + `)`
	}
	var claimed []OutboxRow
	err := s.WithTx(func(tx *Tx) error {
		rows, err := tx.tx.Query(
			`SELECT id,kind,event_seq,target,payload,COALESCE(idempotency_key,''),attempts FROM outbox
			 WHERE `+where+` ORDER BY id`, args...)
		if err != nil {
			return err
		}
		var batch []OutboxRow
		for rows.Next() {
			var r OutboxRow
			if err := rows.Scan(&r.ID, &r.Kind, &r.EventSeq, &r.Target, &r.Payload, &r.IdempotencyKey, &r.Attempts); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return err
		}
		for i := range batch {
			r := &batch[i]
			if _, err := tx.tx.Exec(`UPDATE outbox SET status='claimed', lease_owner=?, lease_until=?, attempts=attempts+1 WHERE id=?`, owner, until, r.ID); err != nil {
				return err
			}
			r.Status, r.LeaseOwner, r.LeaseUntil, r.Attempts = "claimed", owner, until, r.Attempts+1
		}
		claimed = batch
		return nil
	})
	return claimed, err
}

// AckOutbox marks a delivered effect done. It must be acked by the lease holder (a stale worker whose lease
// expired and was reclaimed cannot ack the new holder's row) — an ack of a row not held by owner is an error.
func (s *Store) AckOutbox(id, owner string) error {
	res, err := s.db.Exec(`UPDATE outbox SET status='acked' WHERE id=? AND lease_owner=?`, id, owner)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("ack outbox %q: not held by %q", id, owner)
	}
	return nil
}

// DeleteAckedOutbox prunes terminally-acked rows of one kind. Acked rows are never re-read
// (ClaimOutbox excludes them), so a consumer that acks without pruning grows the outbox by one dead
// row per accepted decision for the life of the project. Returns how many rows were pruned.
func (s *Store) DeleteAckedOutbox(kind string) (int, error) {
	res, err := s.db.Exec(`DELETE FROM outbox WHERE kind=? AND status='acked'`, kind)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// AppendDecisionTx writes a decision INSIDE a caller's txn (used for accepted ops — crash-safe atomicity, Invariant #7).
func (t *Tx) AppendDecisionTx(d contract.Decision) error {
	b, _ := json.Marshal(d)
	_, err := t.tx.Exec(`INSERT INTO decisions (decision_id,op_id,ingest_seq,actor,correlation_id,next_action,status,payload) VALUES (?,?,?,?,?,?,?,?)`,
		d.DecisionID, d.OpID, d.IngestSeq, string(d.Actor), d.CorrelationID, d.NextAction, string(d.Status), string(b))
	return err
}

func (t *Tx) RecordAcceptedEventEnvelopesTx(d contract.Decision) error {
	if d.Status != contract.Accepted {
		return nil
	}
	if d.IngestSeq <= 0 {
		return nil
	}
	acceptedBy, err := t.ensureReplicaID()
	if err != nil {
		return err
	}
	snapshots := d.NewResources
	if len(snapshots) == 0 {
		snapshots = make([]contract.ResourceSnapshot, 0, len(d.NewVersions))
		for _, rv := range d.NewVersions {
			_, fields, err := t.ReadResource(rv.Ref)
			if err != nil {
				return err
			}
			snapshots = append(snapshots, contract.ResourceSnapshot{Ref: rv.Ref, Version: rv.Version, Fields: fields})
		}
	}
	for _, snap := range snapshots {
		fields := snap.Fields
		if fields == nil {
			fields = map[string]any{}
		}
		ev := eventmodel.Event{
			SchemaVersion: eventmodel.SchemaVersion,
			ID:            fmt.Sprintf("%s:%s/%s", d.DecisionID, snap.Ref.Kind, snap.Ref.ID),
			Type:          string(snap.Ref.Kind) + ".accepted",
			Subject:       eventmodel.Subject(string(snap.Ref.Kind), string(snap.Ref.ID)),
			Actor:         string(d.Actor),
			Payload: map[string]any{
				"resource_version": int64(snap.Version),
				"fields":           fields,
			},
			CorrelationID: d.CorrelationID,
			CreatedAt:     d.AppliedAt,
		}
		env := eventmodel.AcceptedEnvelope(ev, d.DecisionID, d.IngestSeq, d.AppliedAt, acceptedBy)
		if _, err := t.AppendEventEnvelopeReturningSeq(env); err != nil {
			return err
		}
	}
	return nil
}

// Tx-scoped variants for the atomic dispatch transaction.
func (t *Tx) AppendEvent(ev contract.Event) error {
	b, err := json.Marshal(ev) // never write a garbage payload silently (mirrors Store.AppendEvent)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec(`INSERT INTO events (payload) VALUES (?)`, string(b))
	return err
}
func (t *Tx) SetCursor(name string, seq int64) error {
	_, err := t.tx.Exec(`INSERT INTO cursors (name,seq) VALUES (?,?) ON CONFLICT(name) DO UPDATE SET seq=excluded.seq`, name, seq)
	return err
}

// AppendDecision writes a decision in its own txn (used for non-accepted ops — nothing to be atomic with).
func (s *Store) AppendDecision(d contract.Decision) error {
	b, _ := json.Marshal(d)
	_, err := s.db.Exec(`INSERT INTO decisions (decision_id,op_id,ingest_seq,actor,correlation_id,next_action,status,payload) VALUES (?,?,?,?,?,?,?,?)`,
		d.DecisionID, d.OpID, d.IngestSeq, string(d.Actor), d.CorrelationID, d.NextAction, string(d.Status), string(b))
	return err
}
func (s *Store) AppendEvent(ev contract.Event) (int64, error) {
	b, err := json.Marshal(ev) // this is the durable ingest stream — never write a garbage payload silently
	if err != nil {
		return 0, err
	}
	res, err := s.db.Exec(`INSERT INTO events (payload) VALUES (?)`, string(b))
	if err != nil {
		return 0, err
	}
	return res.LastInsertId() // = IngestSeq (Invariant #9)
}
func (s *Store) PendingEvents(afterSeq int64) ([]contract.Event, error) {
	rows, err := s.db.Query(`SELECT ingest_seq, payload FROM events WHERE ingest_seq>? ORDER BY ingest_seq`, afterSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []contract.Event
	for rows.Next() {
		var seq int64
		var p string
		if err := rows.Scan(&seq, &p); err != nil {
			return nil, err
		}
		var ev contract.Event
		if err := json.Unmarshal([]byte(p), &ev); err != nil {
			return nil, err // surface a corrupt payload; never manufacture a zero-value event
		}
		ev.IngestSeq = seq
		out = append(out, ev)
	}
	return out, rows.Err()
}
func (s *Store) DecisionCount() int {
	var n int
	_ = s.db.QueryRow(`SELECT COUNT(*) FROM decisions`).Scan(&n)
	return n
}

// MaxDecidedSeq returns the highest event ingest_seq that already has a decision (0 if none, or if only
// direct non-event ops have been applied — those carry ingest_seq 0). The decision log IS the
// reconciler's durable cursor: a decision row means that event was consumed, so a fresh Reconciler
// resumes from here instead of re-reading from 0 (Invariant #9 — exactly-once over a contiguous prefix).
func (s *Store) MaxDecidedSeq() int64 {
	var n int64
	_ = s.db.QueryRow(`SELECT COALESCE(MAX(ingest_seq), 0) FROM decisions`).Scan(&n)
	return n
}

// GetCursor returns a named durable cursor (0 if unset). The runtime's dispatch position lives here, the
// way the reconciler's decision position is derived from MaxDecidedSeq — both make the loop restart-safe.
func (s *Store) GetCursor(name string) int64 {
	var seq int64
	err := s.db.QueryRow(`SELECT seq FROM cursors WHERE name=?`, name).Scan(&seq)
	if err == sql.ErrNoRows {
		return 0
	}
	return seq
}
func (s *Store) SetCursor(name string, seq int64) error {
	_, err := s.db.Exec(`INSERT INTO cursors (name,seq) VALUES (?,?) ON CONFLICT(name) DO UPDATE SET seq=excluded.seq`, name, seq)
	return err
}

// DispatchTx atomically appends all proposed events produced from ONE observed event AND advances the
// dispatch cursor past it. All-or-nothing (Invariant R6 / finding #1): a crash can never leave appended
// proposals with an un-advanced cursor (which would re-dispatch and duplicate).
func (s *Store) DispatchTx(events []contract.Event, cursorName string, seq int64) error {
	return s.WithTx(func(tx *Tx) error {
		for _, ev := range events {
			if err := tx.AppendEvent(ev); err != nil {
				return err
			}
		}
		return tx.SetCursor(cursorName, seq)
	})
}

// DeferralCount returns how many REBASE deferrals a CorrelationID has accumulated in the durable log.
// It is the liveness-escalation counter (Invariant #10) derived from the decision log rather than held
// in memory, so escalation survives a process restart exactly as the cursor does. It counts ONLY
// next_action='rebase' deferrals, so an unrelated human_review deferral (from a defer_to_human /
// auto_merge_disjoint pass that shares the CorrelationID) does NOT pre-seed the count and trigger
// premature escalation.
//
// Scope note: for the reconciler path — the SOLE production caller of Kernel.Apply — this reproduces the
// removed in-memory map's predicate exactly (rebase-deferrals are only produced inside RunOnce). A direct
// (non-reconciler) Apply sharing a CorrelationID would also contribute to this durable count; that is not
// reachable today and is the intended behaviour for any future direct path (e.g. CLI-inline, Invariant #17).
func (s *Store) DeferralCount(correlationID string) int {
	var n int
	_ = s.db.QueryRow(`SELECT COUNT(*) FROM decisions WHERE correlation_id=? AND status='deferred' AND next_action='rebase'`, correlationID).Scan(&n)
	return n
}

// DecisionRow is a decision plus its durable append order (the implicit rowid). The server's side-effect
// sink advances by Rowid, so it can RE-DERIVE invalidations/diagnostics from the decision log after a crash.
type DecisionRow struct {
	Rowid    int64
	Decision contract.Decision
}

// DecisionsAfter returns decisions appended after rowid, in append order. It lets the server produce a
// decision's side-effects (S2 invalidation / S7 diagnostic) idempotently from the durable log rather than
// only from a single RunOnce return — closing the crash window between the decision commit and its effects.
func (s *Store) DecisionsAfter(rowid int64) ([]DecisionRow, error) {
	rows, err := s.db.Query(`SELECT rowid, payload FROM decisions WHERE rowid>? ORDER BY rowid`, rowid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DecisionRow
	for rows.Next() {
		var rid int64
		var p string
		if err := rows.Scan(&rid, &p); err != nil {
			return nil, err
		}
		var d contract.Decision
		if err := json.Unmarshal([]byte(p), &d); err != nil {
			return nil, err
		}
		out = append(out, DecisionRow{Rowid: rid, Decision: d})
	}
	return out, rows.Err()
}

// DecisionsForActor returns this actor's deferred decisions (the pull-feedback source, Invariant #8).
func (s *Store) DecisionsForActor(actor contract.ActorID) ([]contract.Decision, error) {
	rows, err := s.db.Query(`SELECT payload FROM decisions WHERE actor=? AND status='deferred' ORDER BY ingest_seq, rowid`, string(actor))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []contract.Decision
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, err
		}
		var d contract.Decision
		_ = json.Unmarshal([]byte(p), &d)
		out = append(out, d)
	}
	return out, rows.Err()
}

func (t *Tx) RecordSyncEventsTx(d contract.Decision, syncable map[contract.ResourceKind]bool) error {
	if d.Status != contract.Accepted {
		return nil
	}
	envelopes, err := t.acceptedEventEnvelopesForDecision(d.DecisionID)
	if err != nil {
		return err
	}
	if len(envelopes) == 0 {
		return fmt.Errorf("accepted decision %q has no accepted event envelopes", d.DecisionID)
	}
	for _, env := range envelopes {
		material, err := syncMaterialFromAcceptedEnvelope(env)
		if err != nil {
			return err
		}
		if !syncable[material.Ref.Kind] {
			continue
		}
		fieldsJSON, err := json.Marshal(material.Fields)
		if err != nil {
			return err
		}
		digest := digestFields(material.Fields)
		_, err = t.tx.Exec(`
INSERT OR IGNORE INTO sync_events
  (origin_replica_id, local_decision_id, local_ingest_seq, actor, correlation_id,
   resource_kind, resource_id, resource_version, fields_digest, fields, decided_at, status)
VALUES (?,?,?,?,?,?,?,?,?,?,?,'pending')`,
			material.OriginReplicaID, material.DecisionID, material.IngestSeq, material.Actor, material.CorrelationID,
			string(material.Ref.Kind), string(material.Ref.ID), int64(material.Version), digest, string(fieldsJSON), material.AcceptedAt)
		if err != nil {
			return err
		}
	}
	return nil
}

type acceptedSyncMaterial struct {
	OriginReplicaID string
	DecisionID      string
	IngestSeq       int64
	Actor           string
	CorrelationID   string
	Ref             contract.ResourceRef
	Version         contract.Version
	Fields          map[string]any
	AcceptedAt      string
}

func (t *Tx) acceptedEventEnvelopesForDecision(decisionID string) ([]eventmodel.EventEnvelope, error) {
	rows, err := t.tx.Query(`
SELECT envelope
FROM event_envelopes
WHERE phase=? AND decision_id=?
ORDER BY seq`, string(eventmodel.PhaseAccepted), decisionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []eventmodel.EventEnvelope
	for rows.Next() {
		var payload string
		if err := rows.Scan(&payload); err != nil {
			return nil, err
		}
		var env eventmodel.EventEnvelope
		if err := json.Unmarshal([]byte(payload), &env); err != nil {
			return nil, err
		}
		if err := env.Validate(); err != nil {
			return nil, err
		}
		out = append(out, env)
	}
	return out, rows.Err()
}

func syncMaterialFromAcceptedEnvelope(env eventmodel.EventEnvelope) (acceptedSyncMaterial, error) {
	if env.Phase != eventmodel.PhaseAccepted {
		return acceptedSyncMaterial{}, fmt.Errorf("sync material requires accepted envelope, got %q", env.Phase)
	}
	ref, err := refFromSubject(env.Event.Subject)
	if err != nil {
		return acceptedSyncMaterial{}, err
	}
	version, err := int64FromAny(env.Event.Payload["resource_version"])
	if err != nil {
		return acceptedSyncMaterial{}, fmt.Errorf("accepted envelope %q has invalid resource_version: %w", env.Event.ID, err)
	}
	fields, ok := env.Event.Payload["fields"].(map[string]any)
	if !ok {
		return acceptedSyncMaterial{}, fmt.Errorf("accepted envelope %q payload.fields must be an object", env.Event.ID)
	}
	decisionID, _ := env.Meta["decision_id"].(string)
	acceptedBy, _ := env.Meta["accepted_by"].(string)
	ingestSeq, err := int64FromAny(env.Meta["ingest_seq"])
	if err != nil {
		return acceptedSyncMaterial{}, fmt.Errorf("accepted envelope %q has invalid ingest_seq: %w", env.Event.ID, err)
	}
	acceptedAt, _ := env.Meta["accepted_at"].(string)
	return acceptedSyncMaterial{
		OriginReplicaID: acceptedBy,
		DecisionID:      decisionID,
		IngestSeq:       ingestSeq,
		Actor:           env.Event.Actor,
		CorrelationID:   env.Event.CorrelationID,
		Ref:             ref,
		Version:         contract.Version(version),
		Fields:          fields,
		AcceptedAt:      acceptedAt,
	}, nil
}

func refFromSubject(subject eventmodel.EventSubject) (contract.ResourceRef, error) {
	parts := strings.SplitN(string(subject), "/", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return contract.ResourceRef{}, fmt.Errorf("event subject %q is not kind/id", subject)
	}
	return contract.ResourceRef{Kind: contract.ResourceKind(parts[0]), ID: contract.ResourceID(parts[1])}, nil
}

func int64FromAny(value any) (int64, error) {
	switch v := value.(type) {
	case int:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case uint:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > uint64(^uint64(0)>>1) {
			return 0, fmt.Errorf("integer overflows int64")
		}
		return int64(v), nil
	case float64:
		i := int64(v)
		if v != float64(i) {
			return 0, fmt.Errorf("not an integer")
		}
		return i, nil
	default:
		return 0, fmt.Errorf("unsupported numeric type %T", value)
	}
}

func (s *Store) PendingSyncedEvents() ([]eventmodel.EventEnvelope, error) {
	materials, err := s.syncEventMaterialsByStatus("pending")
	if err != nil {
		return nil, err
	}
	out := make([]eventmodel.EventEnvelope, 0, len(materials))
	for _, material := range materials {
		env, err := contract.SyncedEventEnvelopeFromMaterial(material)
		if err != nil {
			return nil, err
		}
		out = append(out, env)
	}
	return out, nil
}

type SyncEventCounts struct {
	Pending   int
	Synced    int
	Conflicts int
}

func (s *Store) MarkSyncedEventStatus(originMnemond, eventID string, subject eventmodel.EventSubject, status, remotePeerID, at, diagnostic string) error {
	localDecisionID := contract.DecisionIDFromEventID(eventID)
	ref, err := contract.ResourceRefFromEventSubject(subject)
	if err != nil {
		return err
	}
	res, err := s.db.Exec(`
UPDATE sync_events
SET status=?, remote_peer_id=?, acked_at=?, diagnostic=?
WHERE origin_replica_id=? AND local_decision_id=? AND resource_kind=? AND resource_id=?`,
		status, remotePeerID, at, diagnostic, originMnemond, localDecisionID, string(ref.Kind), string(ref.ID))
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("synced event %s/%s %s/%s not found", originMnemond, eventID, ref.Kind, ref.ID)
	}
	return nil
}

func (s *Store) SyncEventCounts() (SyncEventCounts, error) {
	rows, err := s.db.Query(`SELECT status, COUNT(*) FROM sync_events GROUP BY status`)
	if err != nil {
		return SyncEventCounts{}, err
	}
	defer rows.Close()
	var counts SyncEventCounts
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return SyncEventCounts{}, err
		}
		switch status {
		case "pending":
			counts.Pending += n
		case "synced":
			counts.Synced += n
		case "conflict", "rejected":
			counts.Conflicts += n
		}
	}
	return counts, rows.Err()
}

type remoteSyncedEventMaterialRecord struct {
	RemoteSeq  int64
	RemotePeer string
	Material   contract.SyncedEventMaterial
	Status     string
	Diagnostic string
}

type RemoteSyncedEventRecord struct {
	RemoteSeq  int64
	RemotePeer string
	Envelope   eventmodel.EventEnvelope
	Status     string
	Diagnostic string
}

func (s *Store) RecordRemoteSyncedEvent(remotePeerID string, env eventmodel.EventEnvelope, receivedAt string) (RemoteSyncedEventRecord, error) {
	material, err := contract.SyncedEventMaterialFromEnvelope(env)
	if err != nil {
		return RemoteSyncedEventRecord{}, err
	}
	rec, err := s.recordRemoteSyncedEventMaterial(remotePeerID, material, receivedAt)
	if err != nil {
		return RemoteSyncedEventRecord{}, err
	}
	return remoteSyncedEventRecordFromMaterial(rec)
}

func (s *Store) recordRemoteSyncedEventMaterial(remotePeerID string, material contract.SyncedEventMaterial, receivedAt string) (remoteSyncedEventMaterialRecord, error) {
	var out remoteSyncedEventMaterialRecord
	err := s.WithTx(func(tx *Tx) error {
		existing, found, err := tx.readRemoteSyncedEventMaterial(remotePeerID, material.OriginReplicaID, material.LocalDecisionID)
		if err != nil {
			return err
		}
		if found {
			if sameSyncedEventMaterial(existing.Material, material) {
				out = existing
				return nil
			}
			out = remoteSyncedEventMaterialRecord{
				RemotePeer: remotePeerID,
				Material:   material,
				Status:     "conflict",
				Diagnostic: "sync idempotency key reused with different event",
			}
			return nil
		}
		fields := material.Fields
		if fields == nil {
			fields = map[string]any{}
		}
		fieldsJSON, err := json.Marshal(fields)
		if err != nil {
			return err
		}
		res, err := tx.tx.Exec(`
INSERT INTO sync_remote_events
  (remote_peer_id, origin_replica_id, local_decision_id, local_ingest_seq, actor, correlation_id,
   resource_kind, resource_id, resource_version, fields_digest, fields, decided_at, received_at, status)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'accepted')`,
			remotePeerID, material.OriginReplicaID, material.LocalDecisionID, material.LocalIngestSeq, string(material.Actor), material.CorrelationID,
			string(material.ResourceRef.Kind), string(material.ResourceRef.ID), int64(material.ResourceVersion), material.FieldsDigest, string(fieldsJSON), material.DecidedAt, receivedAt)
		if err != nil {
			return err
		}
		seq, err := res.LastInsertId()
		if err != nil {
			return err
		}
		material.Fields = fields
		material.Status = "accepted"
		out = remoteSyncedEventMaterialRecord{RemoteSeq: seq, RemotePeer: remotePeerID, Material: material, Status: "accepted"}
		return nil
	})
	return out, err
}

func remoteSyncedEventRecordFromMaterial(rec remoteSyncedEventMaterialRecord) (RemoteSyncedEventRecord, error) {
	env, err := contract.SyncedEventEnvelopeFromMaterial(rec.Material)
	if err != nil {
		return RemoteSyncedEventRecord{}, err
	}
	if rec.RemoteSeq > 0 {
		env.Meta["cursor"] = fmt.Sprintf("%d", rec.RemoteSeq)
	}
	return RemoteSyncedEventRecord{
		RemoteSeq:  rec.RemoteSeq,
		RemotePeer: rec.RemotePeer,
		Envelope:   env,
		Status:     rec.Status,
		Diagnostic: rec.Diagnostic,
	}, nil
}

func (t *Tx) readRemoteSyncedEventMaterial(remotePeerID, originReplicaID, localDecisionID string) (remoteSyncedEventMaterialRecord, bool, error) {
	row := t.tx.QueryRow(`
SELECT remote_seq, remote_peer_id, origin_replica_id, local_decision_id, local_ingest_seq, actor, correlation_id,
       resource_kind, resource_id, resource_version, fields_digest, fields, decided_at, status, diagnostic
FROM sync_remote_events
WHERE remote_peer_id=? AND origin_replica_id=? AND local_decision_id=?`,
		remotePeerID, originReplicaID, localDecisionID)
	rec, err := scanRemoteSyncedEventMaterial(row)
	if err == sql.ErrNoRows {
		return remoteSyncedEventMaterialRecord{}, false, nil
	}
	if err != nil {
		return remoteSyncedEventMaterialRecord{}, false, err
	}
	return rec, true, nil
}

func (s *Store) remoteSyncedEventMaterialsAfter(afterSeq int64, excludeOriginReplicaID string, scopes []contract.ResourceRef, limit int) ([]remoteSyncedEventMaterialRecord, int64, error) {
	if limit <= 0 {
		limit = 100
	}
	where := `remote_seq>? AND status='accepted'`
	args := []any{afterSeq}
	if excludeOriginReplicaID != "" {
		where += ` AND origin_replica_id<>?`
		args = append(args, excludeOriginReplicaID)
	}
	if len(scopes) > 0 {
		parts := make([]string, 0, len(scopes))
		for _, ref := range scopes {
			parts = append(parts, `(resource_kind=? AND resource_id=?)`)
			args = append(args, string(ref.Kind), string(ref.ID))
		}
		where += ` AND (` + strings.Join(parts, " OR ") + `)`
	}
	args = append(args, limit)
	rows, err := s.db.Query(`
SELECT remote_seq, remote_peer_id, origin_replica_id, local_decision_id, local_ingest_seq, actor, correlation_id,
       resource_kind, resource_id, resource_version, fields_digest, fields, decided_at, status, diagnostic
FROM sync_remote_events
WHERE `+where+`
ORDER BY remote_seq
LIMIT ?`, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []remoteSyncedEventMaterialRecord
	var next int64 = afterSeq
	for rows.Next() {
		rec, err := scanRemoteSyncedEventMaterial(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, rec)
		if rec.RemoteSeq > next {
			next = rec.RemoteSeq
		}
	}
	return out, next, rows.Err()
}

func (s *Store) RemoteSyncedEventsAfter(afterSeq int64, excludeOriginMnemond string, scopes []contract.ResourceRef, limit int) ([]RemoteSyncedEventRecord, int64, error) {
	materials, next, err := s.remoteSyncedEventMaterialsAfter(afterSeq, excludeOriginMnemond, scopes, limit)
	if err != nil {
		return nil, 0, err
	}
	out := make([]RemoteSyncedEventRecord, 0, len(materials))
	for _, material := range materials {
		rec, err := remoteSyncedEventRecordFromMaterial(material)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, rec)
	}
	return out, next, nil
}

// RemoteSyncedEventCount is the hub's "events received" counter: the total synced events accepted
// into the remote event ledger (rejected/conflicted events are never inserted).
func (s *Store) RemoteSyncedEventCount() (int64, error) {
	var n int64
	err := s.db.QueryRow(`SELECT COUNT(*) FROM sync_remote_events`).Scan(&n)
	return n, err
}

// CursorsByPrefix reads every durable cursor whose name starts with prefix, keyed by the name REMAINDER
// (the per-replica suffix). The hub's status surface uses it for the last-served-cursor-per-replica
// counter; it is read-only bookkeeping, never an ordering source.
func (s *Store) CursorsByPrefix(prefix string) (map[string]int64, error) {
	// substr-compare, not LIKE: the sync cursor prefixes contain "_" (a LIKE single-char wildcard).
	rows, err := s.db.Query(`SELECT name, seq FROM cursors WHERE substr(name, 1, ?) = ?`, len(prefix), prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var name string
		var seq int64
		if err := rows.Scan(&name, &seq); err != nil {
			return nil, err
		}
		out[strings.TrimPrefix(name, prefix)] = seq
	}
	return out, rows.Err()
}

func scanRemoteSyncedEventMaterial(row rowScanner) (remoteSyncedEventMaterialRecord, error) {
	var rec remoteSyncedEventMaterialRecord
	var actor, kind, id, fieldsJSON string
	var version int64
	err := row.Scan(&rec.RemoteSeq, &rec.RemotePeer, &rec.Material.OriginReplicaID, &rec.Material.LocalDecisionID, &rec.Material.LocalIngestSeq, &actor, &rec.Material.CorrelationID,
		&kind, &id, &version, &rec.Material.FieldsDigest, &fieldsJSON, &rec.Material.DecidedAt, &rec.Status, &rec.Diagnostic)
	if err != nil {
		return remoteSyncedEventMaterialRecord{}, err
	}
	rec.Material.Actor = contract.ActorID(actor)
	rec.Material.ResourceRef = contract.ResourceRef{Kind: contract.ResourceKind(kind), ID: contract.ResourceID(id)}
	rec.Material.ResourceVersion = contract.Version(version)
	rec.Material.Status = rec.Status
	if err := json.Unmarshal([]byte(fieldsJSON), &rec.Material.Fields); err != nil {
		return remoteSyncedEventMaterialRecord{}, err
	}
	return rec, nil
}

func sameSyncedEventMaterial(a, b contract.SyncedEventMaterial) bool {
	return a.LocalIngestSeq == b.LocalIngestSeq &&
		a.Actor == b.Actor &&
		a.CorrelationID == b.CorrelationID &&
		a.ResourceRef == b.ResourceRef &&
		a.ResourceVersion == b.ResourceVersion &&
		a.FieldsDigest == b.FieldsDigest
}

func (s *Store) syncEventMaterialsByStatus(status string) ([]contract.SyncedEventMaterial, error) {
	rows, err := s.db.Query(`
SELECT origin_replica_id, local_decision_id, local_ingest_seq, actor, correlation_id,
       resource_kind, resource_id, resource_version, fields_digest, fields, decided_at, status
FROM sync_events WHERE status=? ORDER BY local_ingest_seq, local_decision_id, resource_kind, resource_id`, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []contract.SyncedEventMaterial
	for rows.Next() {
		var c contract.SyncedEventMaterial
		var actor, kind, id, fieldsJSON string
		var version int64
		if err := rows.Scan(&c.OriginReplicaID, &c.LocalDecisionID, &c.LocalIngestSeq, &actor, &c.CorrelationID,
			&kind, &id, &version, &c.FieldsDigest, &fieldsJSON, &c.DecidedAt, &c.Status); err != nil {
			return nil, err
		}
		c.Actor = contract.ActorID(actor)
		c.ResourceRef = contract.ResourceRef{Kind: contract.ResourceKind(kind), ID: contract.ResourceID(id)}
		c.ResourceVersion = contract.Version(version)
		if err := json.Unmarshal([]byte(fieldsJSON), &c.Fields); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func digestFields(fields map[string]any) string {
	b, _ := json.Marshal(fields)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
