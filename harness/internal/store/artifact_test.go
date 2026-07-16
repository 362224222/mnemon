package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestVerifiedArtifactRootCheckpointReplayConflictAndRestart(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "node", "node.db")
	st, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	root := verifiedRoot(t, "root-one", `{"entries":[],"kind":"report","total_bytes":0}`, 0)
	first, err := st.CheckpointVerifiedArtifactRoot(context.Background(), root)
	if err != nil || first.Replayed {
		t.Fatalf("first checkpoint = (%#v, %v)", first, err)
	}
	retry := root
	retry.CreatedAt = retry.CreatedAt.Add(time.Hour)
	retry.VerifiedAt = retry.VerifiedAt.Add(time.Hour)
	replayed, err := st.CheckpointVerifiedArtifactRoot(context.Background(), retry)
	if err != nil || !replayed.Replayed || !replayed.Root.CreatedAt.Equal(root.CreatedAt) {
		t.Fatalf("replayed checkpoint = (%#v, %v)", replayed, err)
	}
	changed := root
	changed.TotalBytes = 1
	if _, err := st.CheckpointVerifiedArtifactRoot(context.Background(), changed); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("size conflict error = %v", err)
	}
	changed = root
	changed.Manifest, _ = model.NewJSON([]byte(`{"entries":[{"size_bytes":0}]}`))
	changed.ManifestDigest = model.Sum(changed.Manifest.Bytes())
	if _, err := st.CheckpointVerifiedArtifactRoot(context.Background(), changed); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("manifest conflict error = %v", err)
	}
	changed = root
	changed.ManifestDigest = model.Sum([]byte("wrong"))
	if _, err := st.CheckpointVerifiedArtifactRoot(context.Background(), changed); err == nil ||
		!strings.Contains(err.Error(), "manifest digest mismatch") {
		t.Fatalf("digest mismatch error = %v", err)
	}
	if _, err := st.db.Exec("UPDATE artifact_roots SET total_bytes = 2 WHERE root_digest = ?", root.RootDigest.String()); err == nil || !strings.Contains(err.Error(), "artifact root content is immutable") {
		t.Fatalf("immutable root update error = %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if got, err := st.GetVerifiedArtifactRoot(context.Background(), root.RootDigest); err != nil ||
		!sameArtifactContent(got, root) {
		t.Fatalf("root after restart = (%#v, %v)", got, err)
	}
}

func TestVerifiedArtifactRootRejectsStagedUntilPromotion(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	root := verifiedRoot(t, "root-staged", `{"entries":[],"kind":"diff","total_bytes":0}`, 0)
	if _, err := st.db.Exec(`INSERT INTO artifact_roots(root_digest, manifest_json, manifest_digest,
		total_bytes, state, created_at) VALUES(?, ?, ?, ?, 'staged', ?)`, root.RootDigest.String(),
		root.Manifest.Bytes(), root.ManifestDigest.Bytes(), root.TotalBytes, storeTime(root.CreatedAt)); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetVerifiedArtifactRoot(context.Background(), root.RootDigest); !errors.Is(err, ErrArtifactUnverified) {
		t.Fatalf("staged Get error = %v", err)
	}
	checkpoint, err := st.CheckpointVerifiedArtifactRoot(context.Background(), root)
	if err != nil || checkpoint.Replayed {
		t.Fatalf("staged promotion = (%#v, %v)", checkpoint, err)
	}
	if _, err := st.GetVerifiedArtifactRoot(context.Background(), root.RootDigest); err != nil {
		t.Fatalf("verified Get error = %v", err)
	}
	if _, err := st.db.Exec("UPDATE artifact_roots SET state = 'staged' WHERE root_digest = ?", root.RootDigest.String()); err == nil || !strings.Contains(err.Error(), "cannot regress") {
		t.Fatalf("unverify trigger error = %v", err)
	}
}

func TestVerifiedArtifactRootRejectsPromotionBeforeDurableCreation(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	root := verifiedRoot(t, "root-staged-time", `{"entries":[],"kind":"diff","total_bytes":0}`, 0)
	durableCreatedAt := root.CreatedAt.Add(2 * time.Hour)
	if _, err := st.db.Exec(`INSERT INTO artifact_roots(root_digest, manifest_json, manifest_digest,
		total_bytes, state, created_at) VALUES(?, ?, ?, ?, 'staged', ?)`, root.RootDigest.String(),
		root.Manifest.Bytes(), root.ManifestDigest.Bytes(), root.TotalBytes, storeTime(durableCreatedAt)); err != nil {
		t.Fatal(err)
	}
	root.VerifiedAt = root.CreatedAt.Add(time.Hour)
	if _, err := st.CheckpointVerifiedArtifactRoot(context.Background(), root); !errors.Is(err, ErrArtifactConflict) {
		t.Fatalf("promotion before durable creation error = %v", err)
	}
	if _, err := st.GetVerifiedArtifactRoot(context.Background(), root.RootDigest); !errors.Is(err, ErrArtifactUnverified) {
		t.Fatalf("root after rejected promotion error = %v", err)
	}
}

func TestVerifiedArtifactRootRejectsTimesOutsideStoreEncoding(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	base := verifiedRoot(t, "root-invalid-store-time", `{"entries":[],"kind":"diff","total_bytes":0}`, 0)

	tests := []struct {
		name   string
		mutate func(*VerifiedArtifactRoot)
	}{
		{
			name: "five-digit created year",
			mutate: func(root *VerifiedArtifactRoot) {
				root.CreatedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
				root.VerifiedAt = root.CreatedAt.Add(time.Second)
			},
		},
		{
			name: "verified time outside Unix nanoseconds",
			mutate: func(root *VerifiedArtifactRoot) {
				root.VerifiedAt = time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requested := base
			test.mutate(&requested)
			if _, err := st.CheckpointVerifiedArtifactRoot(context.Background(), requested); err == nil ||
				!strings.Contains(err.Error(), "round-trip through canonical SQLite encoding") {
				t.Fatalf("checkpoint error = %v", err)
			}
		})
	}

	var count int
	if err := st.db.QueryRow("SELECT COUNT(*) FROM artifact_roots").Scan(&count); err != nil || count != 0 {
		t.Fatalf("Artifact roots after rejected checkpoints = %d, err=%v", count, err)
	}
}

func TestSchemaRejectsPoisonArtifactTimes(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	manifest := []byte(`{"entries":[],"kind":"time-test","total_bytes":0}`)
	valid := "2026-07-16T12:00:00.000000000Z"
	poisonTimes := []string{
		"10000-01-01T00:00:00.000000000Z",
		"2500-01-01T00:00:00.000000000Z",
		"2026-02-30T00:00:00.000000000Z",
	}

	for index, poison := range poisonTimes {
		root := model.Sum([]byte(fmt.Sprintf("poison-root-%d", index)))
		if _, err := st.db.Exec(`INSERT INTO artifact_roots(root_digest,manifest_json,manifest_digest,
			total_bytes,state,created_at) VALUES(?,?,?,0,'staged',?)`, root.String(), manifest,
			model.Sum(manifest).Bytes(), poison); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
			t.Fatalf("root created_at %q error = %v", poison, err)
		}

		root = model.Sum([]byte(fmt.Sprintf("poison-verified-root-%d", index)))
		if _, err := st.db.Exec(`INSERT INTO artifact_roots(root_digest,manifest_json,manifest_digest,
			total_bytes,state,created_at,verified_at) VALUES(?,?,?,0,'verified',?,?)`, root.String(),
			manifest, model.Sum(manifest).Bytes(), valid, poison); err == nil ||
			!strings.Contains(err.Error(), "CHECK constraint failed") {
			t.Fatalf("root verified_at %q error = %v", poison, err)
		}

		block := model.Sum([]byte(fmt.Sprintf("poison-block-%d", index)))
		if _, err := st.db.Exec(`INSERT INTO artifact_blocks(block_digest,size_bytes,created_at)
			VALUES(?,0,?)`, block.String(), poison); err == nil ||
			!strings.Contains(err.Error(), "CHECK constraint failed") {
			t.Fatalf("block created_at %q error = %v", poison, err)
		}
	}
}

func TestArtifactPinsAndLocalReplicaProvenance(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	insertNode(t, st.db)
	insertProfile(t, st.db)
	insertChannelAndEvent(t, st.db)
	root := verifiedRoot(t, "root-produced", `{"entries":[],"kind":"review","total_bytes":0}`, 0)
	if _, err := st.CheckpointVerifiedArtifactRoot(context.Background(), root); err != nil {
		t.Fatal(err)
	}
	insertArtifactEvent(t, st.db, "event-produced", "local", "peer-home", "epoch-one", 2, 2,
		1, []byte("record-one"), "principal-one", root.RootDigest, model.ArtifactProduced)
	insertLocalCaptureAuthority(t, st.db, root)
	if _, err := st.db.Exec(`INSERT INTO agent_runs(run_id, profile_id, cause_json, launcher, runtime_kind,
		launcher_diagnostic_json, runtime_ids_json, status, started_at)
		VALUES('run-unrelated', 'teamwork-default', '{}', 'test', 'codex-app-server', '{}', '{}',
		'running', '2026-07-16T12:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	unrelated := localProvenanceWithRun(t, root.RootDigest, "event-produced", "run-unrelated")
	tx, _ := st.db.BeginTx(context.Background(), nil)
	if _, err := insertArtifactProvenance(context.Background(), tx, unrelated); !errors.Is(err, ErrArtifactReference) {
		t.Fatalf("unrelated Run provenance error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	other := verifiedRoot(t, "root-other-capture", `{"entries":[],"kind":"other","total_bytes":0}`, 0)
	if _, err := st.CheckpointVerifiedArtifactRoot(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	otherCapture := artifactCapture(t, other)
	if _, err := st.db.Exec(`INSERT INTO operations(operation_id,profile_id,agent_run_id,client_key_hash,
		kind,request_digest,status,capture_json,result_json,created_at,finished_at)
		VALUES('operation-other-capture','teamwork-default','run-artifact',?,'teamwork.offer',?,
		'committed',?,'{}','2026-07-16T12:00:00Z','2026-07-16T12:00:01Z')`,
		[]byte("key-other-capture"), []byte("request-other-capture"), otherCapture.Bytes()); err != nil {
		t.Fatal(err)
	}
	wrongCapture := localProvenanceWithAuthority(t, root.RootDigest, "event-produced", "run-artifact",
		"operation-other-capture")
	tx, _ = st.db.BeginTx(context.Background(), nil)
	if _, err := insertArtifactProvenance(context.Background(), tx, wrongCapture); !errors.Is(err, ErrArtifactReference) {
		t.Fatalf("unrelated capture provenance error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	local := localProvenance(t, root.RootDigest, "event-produced")
	tx, _ = st.db.BeginTx(context.Background(), nil)
	if replay, err := insertArtifactProvenance(context.Background(), tx, local); err != nil || replay {
		t.Fatalf("local provenance = (%t, %v)", replay, err)
	}
	if replay, err := insertEventArtifactPin(context.Background(), tx, root.RootDigest,
		local.ProducerEvent().EventID(), local.CreatedAt()); err != nil || replay {
		t.Fatalf("produced pin = (%t, %v)", replay, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx, _ = st.db.BeginTx(context.Background(), nil)
	if replay, err := insertArtifactProvenance(context.Background(), tx, local); err != nil || !replay {
		t.Fatalf("local provenance replay = (%t, %v)", replay, err)
	}
	if replay, err := insertEventArtifactPin(context.Background(), tx, root.RootDigest,
		local.ProducerEvent().EventID(), local.CreatedAt().Add(time.Hour)); err != nil || !replay {
		t.Fatalf("pin replay = (%t, %v)", replay, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	insertArtifactEvent(t, st.db, "event-referenced", "local", "peer-home", "epoch-one", 3, 3,
		1, []byte("record-one"), "principal-one", root.RootDigest, model.ArtifactReferenced)
	referencedEvent, _ := model.ParseEventID("event-referenced")
	tx, _ = st.db.BeginTx(context.Background(), nil)
	if err := requireReusableArtifactRoot(context.Background(), tx, root.RootDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := insertEventArtifactPin(context.Background(), tx, root.RootDigest, referencedEvent, local.CreatedAt()); err != nil {
		t.Fatalf("referenced Event pin error = %v", err)
	}
	bad := localProvenance(t, root.RootDigest, "event-referenced")
	if _, err := insertArtifactProvenance(context.Background(), tx, bad); !errors.Is(err, ErrArtifactReference) {
		t.Fatalf("referenced provenance error = %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	staged := verifiedRoot(t, "root-unverified", `{"entries":[],"kind":"staged","total_bytes":0}`, 0)
	if _, err := st.db.Exec(`INSERT INTO artifact_roots(root_digest, manifest_json, manifest_digest,
		total_bytes, state, created_at) VALUES(?, ?, ?, 0, 'staged', ?)`, staged.RootDigest.String(),
		staged.Manifest.Bytes(), staged.ManifestDigest.Bytes(), storeTime(staged.CreatedAt)); err != nil {
		t.Fatal(err)
	}
	insertArtifactEvent(t, st.db, "event-unverified", "local", "peer-home", "epoch-one", 4, 4,
		1, []byte("record-one"), "principal-one", staged.RootDigest, model.ArtifactProduced)
	tx, _ = st.db.BeginTx(context.Background(), nil)
	if _, err := insertArtifactProvenance(context.Background(), tx,
		localProvenance(t, staged.RootDigest, "event-unverified")); !errors.Is(err, ErrArtifactUnverified) {
		t.Fatalf("unverified provenance error = %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	installRemoteBinding(t, st.db)
	insertArtifactEvent(t, st.db, "event-replica", "imported", "peer-remote", "epoch-remote", 1, 1,
		2, []byte("record-two"), "principal-remote", root.RootDigest, model.ArtifactProduced)
	replica := replicaProvenance(t, root.RootDigest)
	tx, _ = st.db.BeginTx(context.Background(), nil)
	if _, err := insertArtifactProvenance(context.Background(), tx, replica); err != nil {
		t.Fatalf("replica provenance error = %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO artifact_provenance(root_digest, producer_event_id,
		producer_origin_peer_id, relation, created_at) VALUES(?, 'event-produced', 'peer-home', 'replica', ?)`,
		root.RootDigest.String(), storeTime(local.CreatedAt())); err == nil ||
		!strings.Contains(err.Error(), "artifact producer event mismatch") {
		t.Fatalf("relation trigger error = %v", err)
	}
	if _, err := st.db.Exec("DELETE FROM artifact_provenance WHERE producer_event_id = 'event-produced'"); err == nil || !strings.Contains(err.Error(), "artifact provenance is immutable") {
		t.Fatalf("provenance delete error = %v", err)
	}
}

func verifiedRoot(t *testing.T, identity, manifest string, total uint64) VerifiedArtifactRoot {
	t.Helper()
	value, err := model.NewJSON([]byte(manifest))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 16, 12, 0, 0, 123, time.UTC)
	return VerifiedArtifactRoot{RootDigest: model.Sum([]byte(identity)), Manifest: value,
		ManifestDigest: model.Sum(value.Bytes()), TotalBytes: total, CreatedAt: now, VerifiedAt: now.Add(time.Second)}
}

type sqlExecer interface {
	Exec(string, ...any) (sql.Result, error)
}

func insertArtifactEvent(t *testing.T, db sqlExecer, id, source, origin, epoch string,
	originSeq, channelSeq, revision int, recordHash []byte, actor string, root model.Digest, role model.ArtifactRole,
) {
	t.Helper()
	ref, err := model.NewArtifactRef(root, role)
	if err != nil {
		t.Fatal(err)
	}
	refs, err := model.JSONFrom([]model.ArtifactRef{ref})
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO events(event_id, schema_version, channel_id, origin_peer_id,
		origin_epoch, origin_seq, channel_seq, origin_member_revision, origin_member_record_hash,
		publication_roster_revision, publication_roster_hash, source, actor_principal, event_type,
		audience_json, resource_json, work_home_peer_id, work_id, summary, payload_json,
		artifact_roots_json, caused_by_json, canonical_event_json, event_digest,
		canonical_publication_json, publication_digest, origin_signature, created_at, accepted_at)
		VALUES(?, 1, 'channel-one', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'review.delivery.ready', ?, ?,
		'peer-home', 'work-artifact', 'artifact', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, id, origin, epoch,
		originSeq, channelSeq, revision, recordHash, revision, recordHash, source, actor, []byte(`["peer-home"]`),
		[]byte(`{"home_peer_id":"peer-home","work_id":"work-artifact"}`), []byte(`{}`), refs.Bytes(),
		[]byte(`[]`), []byte(`{}`), model.Sum([]byte(id+"-event")).Bytes(), []byte(`{}`),
		model.Sum([]byte(id+"-publication")).Bytes(), []byte("signature"),
		"2026-07-16T12:00:00Z", "2026-07-16T12:00:00Z")
	if err != nil {
		t.Fatalf("insert Artifact Event %s: %v", id, err)
	}
}

func insertLocalCaptureAuthority(t *testing.T, db *sql.DB, root VerifiedArtifactRoot) {
	t.Helper()
	capture := artifactCapture(t, root)
	if _, err := db.Exec(`INSERT INTO agent_runs(run_id, profile_id, cause_json, launcher, runtime_kind,
		launcher_diagnostic_json, runtime_ids_json, status, started_at)
		VALUES('run-artifact', 'teamwork-default', '{}', 'test', 'codex-app-server', '{}', '{}',
		'running', '2026-07-16T12:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO operations(operation_id, profile_id, agent_run_id, client_key_hash, kind,
		request_digest, status, capture_json, result_json, created_at, finished_at)
		VALUES('operation-artifact', 'teamwork-default', 'run-artifact', ?, 'teamwork.offer', ?, 'committed', ?, '{}',
		'2026-07-16T12:00:00Z', '2026-07-16T12:00:01Z')`, []byte("key"), []byte("request"),
		capture.Bytes()); err != nil {
		t.Fatal(err)
	}
}

func artifactCapture(t *testing.T, root VerifiedArtifactRoot) model.JSON {
	t.Helper()
	capture, err := model.JSONFrom(struct {
		Roots []struct {
			ManifestDigest model.Digest `json:"manifest_digest"`
			RootDigest     model.Digest `json:"root_digest"`
		} `json:"roots"`
	}{[]struct {
		ManifestDigest model.Digest `json:"manifest_digest"`
		RootDigest     model.Digest `json:"root_digest"`
	}{{root.ManifestDigest, root.RootDigest}}})
	if err != nil {
		t.Fatal(err)
	}
	return capture
}

func localProvenance(t *testing.T, root model.Digest, eventText string) model.ArtifactProvenance {
	t.Helper()
	return localProvenanceWithRun(t, root, eventText, "run-artifact")
}

func localProvenanceWithRun(t *testing.T, root model.Digest, eventText, runText string) model.ArtifactProvenance {
	t.Helper()
	return localProvenanceWithAuthority(t, root, eventText, runText, "operation-artifact")
}

func localProvenanceWithAuthority(t *testing.T, root model.Digest, eventText, runText,
	operationText string,
) model.ArtifactProvenance {
	t.Helper()
	peer, _ := model.ParsePeerID("peer-home")
	epoch, _ := model.ParseOriginEpoch("epoch-one")
	event, _ := model.ParseEventID(eventText)
	key, _ := model.NewEventKey(peer, epoch, event)
	run, _ := model.ParseRunID(runText)
	operation, _ := model.ParseOperationID(operationText)
	value, err := model.NewArtifactProvenance(model.ArtifactProvenanceSpec{RootDigest: root,
		ProducerEvent: key, ProducerOriginPeer: peer, LocalAgentRun: &run, Operation: &operation,
		Relation: model.ProvenanceLocalCapture, CreatedAt: time.Date(2026, 7, 16, 12, 0, 1, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func replicaProvenance(t *testing.T, root model.Digest) model.ArtifactProvenance {
	t.Helper()
	peer, _ := model.ParsePeerID("peer-remote")
	epoch, _ := model.ParseOriginEpoch("epoch-remote")
	event, _ := model.ParseEventID("event-replica")
	key, _ := model.NewEventKey(peer, epoch, event)
	value, err := model.NewArtifactProvenance(model.ArtifactProvenanceSpec{RootDigest: root,
		ProducerEvent: key, ProducerOriginPeer: peer, Relation: model.ProvenanceReplica,
		CreatedAt: time.Date(2026, 7, 16, 12, 0, 2, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func installRemoteBinding(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO channel_members(channel_id, revision, record_hash, previous_hash,
		member_peer_id, origin_epoch, display_label, public_key, multiaddrs_json, status,
		signed_record_json, owner_signature, created_at) VALUES('channel-one', 2, ?, ?, 'peer-remote',
		'epoch-remote', 'remote', 'remote-key', '[]', 'active', '{}', 'signature', '2026-07-16T12:00:00Z')`,
		[]byte("record-two"), []byte("record-one")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE channels SET roster_head_revision = 2, roster_head_hash = ? WHERE channel_id = 'channel-one'",
		[]byte("record-two")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO peer_bindings(channel_id, peer_id, origin_epoch, effective_alias,
		public_key, multiaddrs_json, protocols_json, limits_json, member_revision, member_record_hash,
		state, joined_at) VALUES('channel-one', 'peer-remote', 'epoch-remote', 'remote', 'remote-key',
		'[]', '{}', '{}', 2, ?, 'pending', '2026-07-16T12:00:00Z')`, []byte("record-two")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO peer_cursors(channel_id, origin_peer_id, origin_epoch,
		baseline_channel_seq, contiguous_channel_seq, observed_channel_seq, updated_at)
		VALUES('channel-one', 'peer-remote', 'epoch-remote', 0, 0, 0, '2026-07-16T12:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE peer_bindings SET state = 'active' WHERE channel_id = 'channel-one' AND peer_id = 'peer-remote'"); err != nil {
		t.Fatal(err)
	}
}
