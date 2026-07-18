package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestSchemaV1ObjectSetIsComplete(t *testing.T) {
	st := openTestStore(t)

	expected := map[string][]string{
		"table": strings.Fields(
			"node profiles operations events works work_members work_derivations " +
				"agent_handlings agent_runs artifact_roots artifact_blocks artifact_root_blocks " +
				"artifact_pins artifact_provenance channels channel_members channel_conflicts " +
				"enrollment_grants enrollment_grant_uses enrollment_receipts channel_leave_requests " +
				"peer_bindings gossip_publications peer_deliveries peer_inbox publication_conflicts " +
				"origin_quarantines peer_cursors publication_epochs peer_pull_acks",
		),
		"index": strings.Fields(
			"profiles_one_enabled_teamwork_idx operations_reclaim_idx operations_one_started_context_idx " +
				"works_due_idx agent_handlings_ready_idx agent_handlings_one_claimed_profile_idx " +
				"agent_runs_handling_generation_attempt_idx agent_runs_incomplete_managed_idx enrollment_grants_one_open_idx " +
				"channel_leave_requests_one_open_idx gossip_publications_ready_idx peer_inbox_work_idx",
		),
		"trigger": strings.Fields(
			"node_identity_immutable node_no_delete profiles_identity_immutable profiles_no_delete operations_identity_immutable " +
				"operations_capture_checkpoint_immutable operations_terminal_immutable operations_no_delete " +
				"events_no_update events_no_delete events_local_origin_insert events_publication_member_insert " +
				"works_deadline_immutable works_event_scope_insert works_event_scope_update " +
				"work_members_no_update work_members_no_delete work_derivations_scope_insert " +
				"work_derivations_no_update work_derivations_no_delete agent_handlings_identity_immutable " +
				"agent_runs_claim_snapshot_immutable " +
				"agent_runs_attachment_identity_immutable agent_runs_evidence_once artifact_roots_content_immutable " +
				"artifact_roots_no_unverify artifact_roots_verified_at_immutable artifact_blocks_no_update artifact_root_blocks_no_update " +
				"artifact_root_blocks_verified_insert artifact_root_blocks_verified_delete " +
				"artifact_root_blocks_provenance_delete artifact_provenance_event_insert " +
				"artifact_provenance_no_update artifact_provenance_no_delete channels_nonterminal_limit_insert " +
				"channels_descriptor_immutable channels_no_delete channels_roster_head_monotonic " +
				"channels_terminal_status_update channels_conflicted_status_update channels_leaving_status_update " +
				"channel_members_no_update channel_members_no_delete channel_conflicts_no_update " +
				"channel_conflicts_no_delete channel_conflicts_limit_insert channels_conflicted_requires_evidence channel_members_no_reactivate " +
				"channel_members_capacity_insert enrollment_grants_initial_state_insert enrollment_grants_lifecycle_insert enrollment_grants_identity_immutable enrollment_grants_no_delete enrollment_grants_state_update " +
				"enrollment_grant_uses_validate_insert enrollment_grant_uses_account_insert " +
				"enrollment_grant_uses_no_update enrollment_grant_uses_no_delete " +
				"enrollment_receipts_owner_evidence_insert enrollment_receipts_no_update enrollment_receipts_no_delete " +
				"peer_bindings_no_self_insert peer_bindings_member_state_insert peer_bindings_member_state_update peer_bindings_no_self_update " +
				"peer_bindings_identity_epoch_immutable peer_bindings_revoked_terminal peer_bindings_active_no_pending events_imported_binding_insert " +
				"gossip_publications_event_scope_insert gossip_publications_identity_immutable " +
				"peer_deliveries_event_scope_insert peer_deliveries_identity_immutable " +
				"peer_deliveries_scanned_requires_cursor peer_inbox_event_scope_insert " +
				"peer_inbox_binding_epoch_insert peer_inbox_transport_binding_insert " +
				"peer_inbox_publication_member_insert peer_inbox_identity_immutable peer_inbox_event_scope_update " +
				"peer_inbox_receipt_scope_insert peer_inbox_receipt_scope_update publication_conflicts_no_update " +
				"publication_conflicts_no_delete publication_conflicts_existing_scope_insert " +
				"origin_quarantines_no_update origin_quarantines_no_delete peer_cursors_binding_epoch_insert " +
				"peer_cursors_binding_epoch_update peer_cursors_identity_baseline_immutable " +
				"peer_cursors_monotonic_update peer_cursors_no_delete peer_bindings_no_active_insert " +
				"peer_bindings_activate_requires_cursor publication_epochs_local_origin_insert " +
				"publication_epochs_identity_immutable publication_epochs_monotonic_update " +
				"publication_epochs_no_delete peer_pull_acks_identity_baseline_immutable " +
				"peer_pull_acks_monotonic_update peer_pull_acks_confirmation_immutable peer_pull_acks_no_delete " +
				"peer_deliveries_binding_ready_insert peer_deliveries_binding_ready_update",
		),
	}

	actual := make(map[string][]string)
	rows, err := st.db.Query(
		"SELECT type, name FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var objectType, name string
		if err := rows.Scan(&objectType, &name); err != nil {
			t.Fatal(err)
		}
		actual[objectType] = append(actual[objectType], name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for objectType := range expected {
		sort.Strings(expected[objectType])
		sort.Strings(actual[objectType])
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("schema object set mismatch\nactual: %#v\nexpected: %#v", actual, expected)
	}
	if got := len(actual["table"]) + len(actual["index"]) + len(actual["trigger"]); got != 147 {
		t.Fatalf("explicit object count = %d, want 147", got)
	}
}

func TestSchemaV1KeyConstraintsAndTriggers(t *testing.T) {
	st := openTestStore(t)
	insertNode(t, st.db)

	if _, err := st.db.Exec(
		"UPDATE node SET peer_id = 'peer-other' WHERE singleton = 1",
	); err == nil || !strings.Contains(err.Error(), "node identity is immutable") {
		t.Fatalf("node identity update error = %v, want immutable trigger", err)
	}
	if _, err := st.db.Exec("DELETE FROM node WHERE singleton = 1"); err == nil ||
		!strings.Contains(err.Error(), "node identity cannot be deleted") {
		t.Fatalf("node delete error = %v, want no-delete trigger", err)
	}

	if _, err := st.db.Exec(
		"INSERT INTO profiles(profile_id, principal, workspace_root, host, runtime_kind, credential_hash, active_asset_rev, handling_budget_json, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"wrong-profile",
		"principal-wrong",
		"/workspace",
		"codex",
		"codex-app-server",
		[]byte("credential-wrong"),
		"asset-one",
		[]byte("{}"),
		"2026-01-01T00:00:00Z",
		"2026-01-01T00:00:00Z",
	); err == nil {
		t.Fatal("invalid profile_id was accepted")
	}
	if _, err := st.db.Exec(
		"INSERT INTO profiles(profile_id, principal, workspace_root, host, runtime_kind, credential_hash, active_asset_rev, handling_budget_json, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"teamwork-default",
		"principal-one",
		"/workspace",
		"codex",
		"claude-cli",
		[]byte("credential-one"),
		"asset-one",
		[]byte("{}"),
		"2026-01-01T00:00:00Z",
		"2026-01-01T00:00:00Z",
	); err == nil {
		t.Fatal("invalid host/runtime pair was accepted")
	}
	insertProfile(t, st.db)
	if _, err := st.db.Exec("UPDATE profiles SET principal='principal-forged' WHERE profile_id='teamwork-default'"); err == nil || !strings.Contains(err.Error(), "Profile identity is immutable") {
		t.Fatalf("Profile identity update error = %v, want immutable trigger", err)
	}
	if _, err := st.db.Exec("DELETE FROM profiles WHERE profile_id='teamwork-default'"); err == nil || !strings.Contains(err.Error(), "Profile identity cannot be deleted") {
		t.Fatalf("Profile delete error = %v, want no-delete trigger", err)
	}
	if _, err := st.db.Exec(`INSERT INTO agent_runs(run_id, profile_id, cause_json, launcher, runtime_kind,
		launcher_diagnostic_json, runtime_ids_json, status, started_at)
		VALUES('run-operation-one','teamwork-default','{}','test','codex-app-server','{}','{}',
		'running','2026-01-01T00:00:00.000000000Z')`); err != nil {
		t.Fatalf("insert operation AgentRun: %v", err)
	}

	if _, err := st.db.Exec(
		"INSERT INTO operations(operation_id, profile_id, agent_run_id, client_key_hash, kind, request_digest, status, lease_owner, lease_until, created_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"operation-one",
		"teamwork-default",
		"run-operation-one",
		[]byte("key-one"),
		"teamwork.offer",
		[]byte("request-one"),
		"started",
		"writer-one",
		"2026-01-01T01:00:00Z",
		"2026-01-01T00:00:00.000000000Z",
	); err != nil {
		t.Fatalf("insert operation: %v", err)
	}
	if _, err := st.db.Exec(
		"UPDATE operations SET kind = 'teamwork.cancel' WHERE operation_id = 'operation-one'",
	); err == nil || !strings.Contains(err.Error(), "operation identity is immutable") {
		t.Fatalf("operation identity update error = %v, want immutable trigger", err)
	}
	if _, err := st.db.Exec(
		"UPDATE operations SET agent_run_id = 'run-forged' WHERE operation_id = 'operation-one'",
	); err == nil || !strings.Contains(err.Error(), "operation identity is immutable") {
		t.Fatalf("operation AgentRun update error = %v, want immutable trigger", err)
	}
	if _, err := st.db.Exec(
		"UPDATE operations SET status = 'committed', lease_owner = NULL, lease_until = NULL, result_json = ?, finished_at = '2026-01-01T00:30:00.000000000Z' WHERE operation_id = 'operation-one'",
		[]byte("{}"),
	); err != nil {
		t.Fatalf("commit operation: %v", err)
	}
	if _, err := st.db.Exec(
		"UPDATE operations SET result_json = ? WHERE operation_id = 'operation-one'",
		[]byte("changed"),
	); err == nil || !strings.Contains(err.Error(), "terminal operation is immutable") {
		t.Fatalf("terminal operation update error = %v, want immutable trigger", err)
	}
	if _, err := st.db.Exec(
		"DELETE FROM operations WHERE operation_id = 'operation-one'",
	); err == nil || !strings.Contains(err.Error(), "operations are durable evidence") {
		t.Fatalf("operation delete error = %v, want durable trigger", err)
	}

	insertReviewFixture(t, st.db)
	if _, err := st.db.Exec(
		"UPDATE events SET summary = 'changed' WHERE event_id = 'event-one'",
	); err == nil || !strings.Contains(err.Error(), "events are immutable") {
		t.Fatalf("event update error = %v, want immutable trigger", err)
	}
	if _, err := st.db.Exec(
		"UPDATE works SET deadline_unix_nano = deadline_unix_nano + 1 WHERE home_peer_id = 'peer-home' AND work_id = 'work-one'",
	); err == nil || !strings.Contains(err.Error(), "work deadline is immutable") {
		t.Fatalf("deadline update error = %v, want immutable trigger", err)
	}
}

func TestSchemaChannelAuthorityEvidenceConstraints(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	insertNode(t, st.db)
	insertChannelAuthority(t, st.db, "channel-one", "alpha", "peer-home", "epoch-one", "record-one")
	insertChannelAuthority(t, st.db, "channel-two", "beta", "peer-two", "epoch-two", "record-two")

	if _, err := st.db.Exec(
		"UPDATE channels SET descriptor_json=? WHERE channel_id='channel-one'",
		[]byte("forged-descriptor"),
	); err == nil || !strings.Contains(err.Error(), "channel descriptor evidence is immutable") {
		t.Fatalf("descriptor update error = %v, want immutable trigger", err)
	}
	if _, err := st.db.Exec("DELETE FROM channels WHERE channel_id='channel-one'"); err == nil ||
		!strings.Contains(err.Error(), "channel descriptor evidence is permanent") {
		t.Fatalf("channel delete error = %v, want permanent evidence trigger", err)
	}
	if _, err := st.db.Exec(
		"UPDATE channel_members SET signed_record_json=? WHERE channel_id='channel-one' AND revision=1",
		[]byte("forged-member"),
	); err == nil || !strings.Contains(err.Error(), "channel member records are append-only") {
		t.Fatalf("member evidence update error = %v, want append-only trigger", err)
	}
	if _, err := st.db.Exec(
		"DELETE FROM channel_members WHERE channel_id='channel-one' AND revision=1",
	); err == nil || !strings.Contains(err.Error(), "channel member records are append-only") {
		t.Fatalf("member evidence delete error = %v, want append-only trigger", err)
	}
	if _, err := st.db.Exec(`UPDATE channels SET roster_head_hash=x'ff'
		WHERE channel_id='channel-one'`); err == nil ||
		!strings.Contains(err.Error(), "channel roster head cannot regress or fork in place") {
		t.Fatalf("same-revision roster fork error = %v, want monotonic-head trigger", err)
	}

	orphanTx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orphanTx.Exec(`INSERT INTO channels(channel_id,name,local_alias,owner_peer_id,
		owner_public_key,descriptor_json,descriptor_digest,descriptor_signature,member_limit,
		roster_head_revision,roster_head_hash,status,topic_state,created_at,updated_at)
		VALUES('channel-orphan','Orphan','orphan','peer-orphan',x'01',x'02',x'03',x'04',8,
		1,x'05','active','joined','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`); err != nil {
		_ = orphanTx.Rollback()
		t.Fatalf("insert deferred orphan channel: %v", err)
	}
	commitErr := orphanTx.Commit()
	if commitErr == nil || !strings.Contains(commitErr.Error(), "FOREIGN KEY constraint failed") {
		t.Fatalf("orphan roster head commit error = %v, want foreign-key failure", commitErr)
	}
	// mattn/go-sqlite3 leaves the underlying SQL transaction open after a
	// deferred-FK COMMIT failure even though database/sql marks sql.Tx done.
	if _, err := st.db.Exec("ROLLBACK"); err != nil && !strings.Contains(err.Error(), "no transaction") {
		t.Fatalf("rollback failed deferred-FK transaction: %v", err)
	}

	remoteOneArgs := []any{
		"channel-one", 2, []byte("record-remote"), []byte("record-one"), "peer-remote",
		"epoch-remote", "remote", []byte("remote-key"), []byte(`["/ip4/127.0.0.1/tcp/4002"]`),
		[]byte(`["/mnemon/artifacts/1","/mnemon/channel/1","/mnemon/events/1"]`),
		[]byte(`{"profile":"r5-hermetic-v1"}`), "active", []byte("member-remote"),
		[]byte("member-remote-signature"), "2026-01-01T00:03:00Z",
	}
	remoteTwoArgs := []any{
		"channel-two", 2, []byte("record-joiner-two"), []byte("record-two"), "peer-joiner-two",
		"epoch-joiner-two", "joiner-two", []byte("joiner-two-key"),
		[]byte(`["/ip4/127.0.0.1/tcp/4003"]`),
		[]byte(`["/mnemon/artifacts/1","/mnemon/channel/1","/mnemon/events/1"]`),
		[]byte(`{"profile":"r5-hermetic-v1"}`), "active", []byte("member-joiner-two"),
		[]byte("member-joiner-two-signature"), "2026-01-01T00:03:00Z",
	}
	for _, memberArgs := range [][]any{remoteOneArgs, remoteTwoArgs} {
		if _, err := st.db.Exec(`INSERT INTO channel_members(channel_id,revision,record_hash,
			previous_hash,member_peer_id,origin_epoch,display_label,public_key,multiaddrs_json,
			protocols_json,limits_json,status,signed_record_json,owner_signature,created_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, memberArgs...); err != nil {
			t.Fatalf("insert remote member record: %v", err)
		}
		if _, err := st.db.Exec(`UPDATE channels SET roster_head_revision=2,roster_head_hash=?,
			updated_at='2026-01-01T00:03:00Z' WHERE channel_id=?`, memberArgs[2], memberArgs[0]); err != nil {
			t.Fatalf("advance remote roster head: %v", err)
		}
	}

	if _, err := st.db.Exec(`INSERT INTO enrollment_grants(grant_id,channel_id,verifier,
		expires_at,max_uses,status,created_at) VALUES('grant-one','channel-one',x'11',
		'2026-01-02T00:00:00Z',2,'open','2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("insert enrollment grant: %v", err)
	}
	if _, err := st.db.Exec(`INSERT INTO enrollment_grants(grant_id,channel_id,verifier,
		expires_at,max_uses,status,created_at) VALUES('grant-same-time','channel-one',x'15',
		'2026-01-02T00:00:00Z',1,'open','2026-01-01T00:00:00Z')`); err == nil ||
		!strings.Contains(err.Error(), "enrollment grant lifecycle cannot regress") {
		t.Fatalf("same-time replacement error = %v, want strict lifecycle trigger", err)
	}
	if _, err := st.db.Exec(`INSERT INTO enrollment_grants(grant_id,channel_id,verifier,
		expires_at,max_uses,used_uses,status,created_at,closed_at)
		VALUES('grant-forged-history','channel-two',x'10','2026-01-02T00:00:00Z',2,1,
		'closed','2026-01-01T00:00:00Z','2026-01-01T00:01:00Z')`); err == nil ||
		!strings.Contains(err.Error(), "enrollment grant must begin open and unused") {
		t.Fatalf("forged initial grant state error = %v, want initial-state trigger", err)
	}
	if _, err := st.db.Exec("UPDATE enrollment_grants SET verifier=x'12' WHERE grant_id='grant-one'"); err == nil || !strings.Contains(err.Error(), "enrollment grant identity is immutable") {
		t.Fatalf("grant identity update error = %v, want immutable trigger", err)
	}
	if _, err := st.db.Exec("DELETE FROM enrollment_grants WHERE grant_id='grant-one'"); err == nil ||
		!strings.Contains(err.Error(), "enrollment grant evidence is permanent") {
		t.Fatalf("grant delete error = %v, want permanent evidence trigger", err)
	}
	if _, err := st.db.Exec(`INSERT INTO enrollment_grants(grant_id,channel_id,verifier,
		expires_at,max_uses,status,created_at) VALUES('grant-state','channel-two',x'13',
		'2026-01-02T00:00:00Z',2,'open','2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("insert stateful enrollment grant: %v", err)
	}
	if _, err := st.db.Exec("UPDATE enrollment_grants SET used_uses=2,status='exhausted',closed_at='2026-01-01T00:01:00Z' WHERE grant_id='grant-state'"); err == nil ||
		!strings.Contains(err.Error(), "enrollment grant state is monotonic and terminal") {
		t.Fatalf("grant use jump error = %v, want monotonic-state trigger", err)
	}
	if _, err := st.db.Exec("UPDATE enrollment_grants SET used_uses=1 WHERE grant_id='grant-state'"); err == nil ||
		!strings.Contains(err.Error(), "enrollment grant state is monotonic and terminal") {
		t.Fatalf("grant counter without use ledger error = %v, want ledger consistency", err)
	}
	if _, err := st.db.Exec("UPDATE enrollment_grants SET status='closed',closed_at='2026-01-02T00:00:00Z' WHERE grant_id='grant-state'"); err == nil ||
		!strings.Contains(err.Error(), "enrollment grant state is monotonic and terminal") {
		t.Fatalf("closed grant at expiry error = %v, want terminal-cause fence", err)
	}
	if _, err := st.db.Exec("UPDATE enrollment_grants SET status='expired',closed_at='2026-01-01T00:02:00Z' WHERE grant_id='grant-state'"); err == nil ||
		!strings.Contains(err.Error(), "enrollment grant state is monotonic and terminal") {
		t.Fatalf("early expired grant error = %v, want terminal-cause fence", err)
	}
	if _, err := st.db.Exec("UPDATE enrollment_grants SET status='closed',closed_at='2026-01-01T00:02:00Z' WHERE grant_id='grant-state'"); err != nil {
		t.Fatalf("close enrollment grant: %v", err)
	}
	if _, err := st.db.Exec("UPDATE enrollment_grants SET status='expired' WHERE grant_id='grant-state'"); err == nil ||
		!strings.Contains(err.Error(), "enrollment grant state is monotonic and terminal") {
		t.Fatalf("terminal grant rewrite error = %v, want terminal-state trigger", err)
	}
	if _, err := st.db.Exec(`INSERT INTO enrollment_grants(grant_id,channel_id,verifier,
		expires_at,max_uses,status,created_at) VALUES('grant-retroactive','channel-two',x'16',
		'2026-01-02T00:00:00Z',1,'open','2026-01-01T00:04:00Z')`); err != nil {
		t.Fatalf("insert later enrollment grant: %v", err)
	}
	if _, err := st.db.Exec(`INSERT INTO enrollment_grant_uses(use_id,grant_id,channel_id,
		member_peer_id,join_identity_digest,member_revision,member_record_hash,used_at)
		VALUES('retroactive-use','grant-retroactive','channel-two','peer-joiner-two',x'24',2,?,
		'2026-01-01T00:05:00Z')`, []byte("record-joiner-two")); err == nil ||
		!strings.Contains(err.Error(), "grant use requires open consistent grant") {
		t.Fatalf("retroactive member attribution error = %v, want join transaction fence", err)
	}
	if _, err := st.db.Exec(`INSERT INTO enrollment_grants(grant_id,channel_id,verifier,
		expires_at,max_uses,status,created_at) VALUES('grant-backdated','channel-two',x'14',
		'2026-01-02T00:00:00Z',1,'open','2026-01-01T00:01:59Z')`); err == nil ||
		!strings.Contains(err.Error(), "enrollment grant lifecycle cannot regress") {
		t.Fatalf("backdated replacement error = %v, want lifecycle trigger", err)
	}

	if _, err := st.db.Exec(`INSERT INTO enrollment_grant_uses(use_id,grant_id,channel_id,
		member_peer_id,join_identity_digest,member_revision,member_record_hash,used_at)
		VALUES('cross-channel-use','grant-one','channel-two','peer-joiner-two',x'21',2,?,
		'2026-01-01T00:04:00Z')`, []byte("record-joiner-two")); err == nil ||
		!strings.Contains(err.Error(), "grant use requires open consistent grant") {
		t.Fatalf("cross-channel grant use error = %v, want authority failure", err)
	}
	if _, err := st.db.Exec(`INSERT INTO enrollment_grant_uses(use_id,grant_id,channel_id,
		member_peer_id,join_identity_digest,member_revision,member_record_hash,used_at)
		VALUES('cross-peer-use','grant-one','channel-one','peer-forged',x'22',2,?,
		'2026-01-01T00:04:00Z')`, []byte("record-remote")); err == nil ||
		!strings.Contains(err.Error(), "grant use requires open consistent grant") {
		t.Fatalf("cross-peer grant use error = %v, want authority failure", err)
	}
	if _, err := st.db.Exec(`INSERT INTO enrollment_grant_uses(use_id,grant_id,channel_id,
		member_peer_id,join_identity_digest,member_revision,member_record_hash,used_at)
		VALUES('use-one','grant-one','channel-one','peer-remote',x'23',2,?,
		'2026-01-01T00:04:00Z')`, []byte("record-remote")); err != nil {
		t.Fatalf("insert valid grant use: %v", err)
	}
	var usedUses int
	if err := st.db.QueryRow("SELECT used_uses FROM enrollment_grants WHERE grant_id='grant-one'").
		Scan(&usedUses); err != nil || usedUses != 1 {
		t.Fatalf("grant use accounting = %d, %v, want 1", usedUses, err)
	}
	if _, err := st.db.Exec(`UPDATE enrollment_grants SET status='closed',
		closed_at='2026-01-01T00:03:59Z' WHERE grant_id='grant-one'`); err == nil ||
		!strings.Contains(err.Error(), "enrollment grant state is monotonic and terminal") {
		t.Fatalf("close before durable use error = %v, want lifecycle fence", err)
	}
	if _, err := st.db.Exec(`INSERT INTO enrollment_receipts(receipt_id,owner_use_id,channel_id,
		member_peer_id,roster_head_revision,roster_head_hash,receipt_json,owner_signature,created_at)
		VALUES('cross-receipt','use-one','channel-two','peer-joiner-two',2,?,x'31',x'32',
		'2026-01-01T00:05:00Z')`, []byte("record-joiner-two")); err == nil ||
		!strings.Contains(err.Error(), "owner receipt must match its grant use evidence") {
		t.Fatalf("cross-channel receipt error = %v, want owner-use failure", err)
	}
	if _, err := st.db.Exec(`INSERT INTO enrollment_receipts(receipt_id,owner_use_id,channel_id,
		member_peer_id,roster_head_revision,roster_head_hash,receipt_json,owner_signature,created_at)
		VALUES('early-receipt','use-one','channel-one','peer-remote',2,?,x'31',x'32',
		'2026-01-01T00:03:59Z')`, []byte("record-remote")); err == nil ||
		!strings.Contains(err.Error(), "owner receipt must match its grant use evidence") {
		t.Fatalf("early owner receipt error = %v, want temporal evidence failure", err)
	}
	if _, err := st.db.Exec(`INSERT INTO enrollment_receipts(receipt_id,owner_use_id,channel_id,
		member_peer_id,roster_head_revision,roster_head_hash,receipt_json,owner_signature,created_at)
		VALUES('receipt-one','use-one','channel-one','peer-remote',2,?,x'33',x'34',
		'2026-01-01T00:05:00Z')`, []byte("record-remote")); err != nil {
		t.Fatalf("insert valid enrollment receipt: %v", err)
	}
	if _, err := st.db.Exec(`INSERT INTO enrollment_receipts(receipt_id,owner_use_id,channel_id,
		member_peer_id,roster_head_revision,roster_head_hash,receipt_json,owner_signature,created_at)
		VALUES('receipt-replica',NULL,'channel-two','peer-joiner-two',2,?,x'35',x'36',
		'2026-01-01T00:05:00Z')`, []byte("record-joiner-two")); err != nil {
		t.Fatalf("insert joiner receipt replica without owner grant use: %v", err)
	}
	if _, err := st.db.Exec(`UPDATE enrollment_grants SET status='closed',
		closed_at='2026-01-01T00:05:00Z' WHERE grant_id='grant-one'`); err != nil {
		t.Fatalf("close consumed enrollment grant: %v", err)
	}
	if _, err := st.db.Exec(`INSERT INTO enrollment_grants(grant_id,channel_id,verifier,
		expires_at,max_uses,status,created_at) VALUES('grant-rotated','channel-one',x'41',
		'2026-01-02T00:00:00Z',2,'open','2026-01-01T00:05:00Z')`); err != nil {
		t.Fatalf("insert rotated enrollment grant: %v", err)
	}
	if _, err := st.db.Exec(`INSERT INTO enrollment_grant_uses(use_id,grant_id,channel_id,
		member_peer_id,join_identity_digest,member_revision,member_record_hash,used_at)
		VALUES('use-rotated','grant-rotated','channel-one','peer-remote',x'42',2,?,
		'2026-01-01T00:05:30Z')`, []byte("record-remote")); err == nil ||
		!strings.Contains(err.Error(), "grant use requires open consistent grant") {
		t.Fatalf("rotated grant old-member use error = %v, want same-transaction join fence", err)
	}
	var rotatedUses int
	if err := st.db.QueryRow("SELECT used_uses FROM enrollment_grants WHERE grant_id='grant-rotated'").
		Scan(&rotatedUses); err != nil || rotatedUses != 0 {
		t.Fatalf("rejected rotated grant accounting = %d, %v, want 0", rotatedUses, err)
	}
	if _, err := st.db.Exec("UPDATE enrollment_grant_uses SET used_at='2026-01-01T00:03:00Z' WHERE use_id='use-one'"); err == nil || !strings.Contains(err.Error(), "enrollment grant use evidence is immutable") {
		t.Fatalf("grant use update error = %v, want immutable trigger", err)
	}
	if _, err := st.db.Exec("DELETE FROM enrollment_grant_uses WHERE use_id='use-one'"); err == nil ||
		!strings.Contains(err.Error(), "enrollment grant use evidence is permanent") {
		t.Fatalf("grant use delete error = %v, want permanent evidence trigger", err)
	}
	if _, err := st.db.Exec("UPDATE enrollment_receipts SET receipt_json=x'35' WHERE receipt_id='receipt-one'"); err == nil || !strings.Contains(err.Error(), "enrollment receipt evidence is immutable") {
		t.Fatalf("receipt update error = %v, want immutable trigger", err)
	}
	if _, err := st.db.Exec("DELETE FROM enrollment_receipts WHERE receipt_id='receipt-one'"); err == nil ||
		!strings.Contains(err.Error(), "enrollment receipt evidence is permanent") {
		t.Fatalf("receipt delete error = %v, want permanent evidence trigger", err)
	}

	if _, err := st.db.Exec(`INSERT INTO peer_bindings(channel_id,peer_id,origin_epoch,
		effective_alias,public_key,multiaddrs_json,protocols_json,limits_json,member_revision,
		member_record_hash,state,joined_at) VALUES('channel-one','peer-remote','epoch-remote',
		'remote',x'00',?,?,?,2,?,'pending','2026-01-01T00:03:00Z')`,
		remoteOneArgs[8], remoteOneArgs[9], remoteOneArgs[10], []byte("record-remote")); err == nil ||
		!strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
		t.Fatalf("forged binding key error = %v, want foreign-key failure", err)
	}
	if _, err := st.db.Exec(`INSERT INTO peer_bindings(channel_id,peer_id,origin_epoch,
		effective_alias,public_key,multiaddrs_json,protocols_json,limits_json,member_revision,
		member_record_hash,state,joined_at) VALUES('channel-one','peer-remote','epoch-remote',
		'remote',?,?,?,?,2,?,'pending','2026-01-01T00:03:00Z')`, remoteOneArgs[7],
		remoteOneArgs[8], remoteOneArgs[9], remoteOneArgs[10], []byte("record-remote")); err != nil {
		t.Fatalf("insert valid pending binding: %v", err)
	}
	if _, err := st.db.Exec(`UPDATE peer_bindings SET protocols_json='[]'
		WHERE channel_id='channel-one' AND peer_id='peer-remote'`); err == nil ||
		!strings.Contains(err.Error(), "FOREIGN KEY constraint failed") {
		t.Fatalf("forged binding protocols error = %v, want foreign-key failure", err)
	}
	if _, err := st.db.Exec(`INSERT INTO peer_cursors(channel_id,origin_peer_id,origin_epoch,
		baseline_channel_seq,contiguous_channel_seq,observed_channel_seq,updated_at)
		VALUES('channel-one','peer-remote','epoch-remote',0,0,0,'2026-01-01T00:03:00Z')`); err != nil {
		t.Fatalf("insert binding cursor: %v", err)
	}
	if _, err := st.db.Exec(`UPDATE peer_bindings SET state='active'
		WHERE channel_id='channel-one' AND peer_id='peer-remote'`); err != nil {
		t.Fatalf("activate binding: %v", err)
	}
	if _, err := st.db.Exec(`DELETE FROM peer_cursors
		WHERE channel_id='channel-one' AND origin_peer_id='peer-remote'`); err == nil ||
		!strings.Contains(err.Error(), "peer cursor baseline evidence is durable") {
		t.Fatalf("peer cursor delete error = %v, want durable evidence trigger", err)
	}
	if _, err := st.db.Exec(`INSERT INTO publication_epochs(channel_id,origin_peer_id,origin_epoch,
		source_floor_channel_seq,source_head_channel_seq,updated_at)
		VALUES('channel-one','peer-home','epoch-one',1,0,'2026-01-01T00:03:00Z')`); err != nil {
		t.Fatalf("insert publication epoch for pull ack: %v", err)
	}
	if _, err := st.db.Exec(`INSERT INTO peer_pull_acks(channel_id,target_peer_id,origin_peer_id,
		origin_epoch,baseline_channel_seq,acknowledged_channel_seq,baseline_confirmed_at,updated_at)
		VALUES('channel-one','peer-remote','peer-home','epoch-one',0,0,
		'2026-01-01T00:03:00Z','2026-01-01T00:03:00Z')`); err != nil {
		t.Fatalf("insert pull ack baseline: %v", err)
	}
	if _, err := st.db.Exec(`DELETE FROM peer_pull_acks
		WHERE channel_id='channel-one' AND target_peer_id='peer-remote'`); err == nil ||
		!strings.Contains(err.Error(), "pull ack baseline evidence is durable") {
		t.Fatalf("pull ack delete error = %v, want durable evidence trigger", err)
	}
	if _, err := st.db.Exec(`UPDATE peer_bindings SET state='pending'
		WHERE channel_id='channel-one' AND peer_id='peer-remote'`); err == nil ||
		!strings.Contains(err.Error(), "active binding cannot return to pending") {
		t.Fatalf("active-to-pending error = %v, want monotonic-state trigger", err)
	}
	remoteOneArgs[1] = 3
	remoteOneArgs[2] = []byte("record-remote-revoked")
	remoteOneArgs[3] = []byte("record-remote")
	remoteOneArgs[11] = "revoked"
	remoteOneArgs[12] = []byte("member-remote-revoked")
	remoteOneArgs[13] = []byte("member-remote-revoked-signature")
	remoteOneArgs[14] = "2026-01-01T00:06:00Z"
	if _, err := st.db.Exec(`INSERT INTO channel_members(channel_id,revision,record_hash,
		previous_hash,member_peer_id,origin_epoch,display_label,public_key,multiaddrs_json,
		protocols_json,limits_json,status,signed_record_json,owner_signature,created_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, remoteOneArgs...); err != nil {
		t.Fatalf("insert revoked member record: %v", err)
	}
	if _, err := st.db.Exec(`UPDATE channels SET roster_head_revision=3,roster_head_hash=?,
		updated_at='2026-01-01T00:06:00Z' WHERE channel_id='channel-one'`, remoteOneArgs[2]); err != nil {
		t.Fatalf("advance revoked roster head: %v", err)
	}
	if _, err := st.db.Exec(`UPDATE peer_bindings SET member_revision=3,member_record_hash=?,
		state='revoked' WHERE channel_id='channel-one' AND peer_id='peer-remote'`, remoteOneArgs[2]); err != nil {
		t.Fatalf("revoke binding against terminal member record: %v", err)
	}
	if _, err := st.db.Exec(`UPDATE peer_bindings SET state='pending'
		WHERE channel_id='channel-one' AND peer_id='peer-remote'`); err == nil ||
		!strings.Contains(err.Error(), "revoked binding is terminal") {
		t.Fatalf("revoked binding update error = %v, want terminal trigger", err)
	}
	for index := 0; index <= 8; index++ {
		_, err := st.db.Exec(`INSERT INTO channel_conflicts(conflict_id,channel_id,revision,
			incumbent_record_hash,incumbent_signed_record_json,incumbent_owner_signature,
			challenger_record_hash,challenger_signed_record_json,challenger_owner_signature,
			transport_peer_id,detected_at) VALUES(?,?,3,?,?,?,?,?,?,?,?)`,
			fmt.Sprintf("conflict-limit-%d", index), "channel-one", remoteOneArgs[2], remoteOneArgs[12],
			remoteOneArgs[13], []byte(fmt.Sprintf("challenger-%d", index)), []byte("challenger-record"),
			[]byte("challenger-signature"), "peer-home", "2026-01-01T00:07:00Z")
		if index < 8 && err != nil {
			t.Fatalf("insert bounded conflict evidence %d: %v", index, err)
		}
		if index == 8 && (err == nil || !strings.Contains(err.Error(),
			"channel conflict evidence limit reached")) {
			t.Fatalf("ninth conflict evidence error = %v, want bounded trigger", err)
		}
	}
}

func TestSchemaEnforcesOperationStateShape(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	insertNode(t, st.db)
	insertProfile(t, st.db)
	if _, err := st.db.Exec(`INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
		launcher_diagnostic_json,runtime_ids_json,status,started_at)
		VALUES('run-operation-shape','teamwork-default','{}','test','codex-app-server','{}','{}',
		'running','2026-01-01T00:00:00.000000000Z')`); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		status     string
		leaseOwner any
		leaseUntil any
		result     any
		finishedAt any
	}{
		{
			name: "started with result", status: "started", leaseOwner: "writer",
			leaseUntil: "2026-01-01T01:00:00.000000000Z", result: []byte(`{}`),
		},
		{
			name: "started with finished time", status: "started", leaseOwner: "writer",
			leaseUntil: "2026-01-01T01:00:00.000000000Z", finishedAt: "2026-01-01T00:30:00.000000000Z",
		},
		{
			name: "committed without result", status: "committed",
			finishedAt: "2026-01-01T00:30:00.000000000Z",
		},
		{
			name: "committed without finished time", status: "committed", result: []byte(`{}`),
		},
		{
			name: "rejected without result", status: "rejected",
			finishedAt: "2026-01-01T00:30:00.000000000Z",
		},
		{
			name: "rejected without finished time", status: "rejected", result: []byte(`{}`),
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := st.db.Exec(`INSERT INTO operations(operation_id,profile_id,agent_run_id,
				client_key_hash,kind,request_digest,status,lease_owner,lease_until,result_json,
				created_at,finished_at) VALUES(?,?,?,?,'teamwork.offer',?,?,?,?,?,?,?)`,
				"operation-malformed-"+test.status+"-"+string(rune('a'+index)),
				"teamwork-default", "run-operation-shape", []byte{byte(index + 1)}, []byte{byte(index + 11)},
				test.status, test.leaseOwner, test.leaseUntil, test.result,
				"2026-01-01T00:00:00.000000000Z", test.finishedAt)
			if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
				t.Fatalf("malformed %s operation error = %v, want CHECK failure", test.status, err)
			}
		})
	}

	if _, err := st.db.Exec(`INSERT INTO operations(operation_id,profile_id,agent_run_id,
		client_key_hash,kind,request_digest,status,lease_owner,lease_until,created_at)
		VALUES('operation-valid-committed','teamwork-default','run-operation-shape',x'21',
		'teamwork.offer',x'31','started','writer','2026-01-01T01:00:00.000000000Z',
		'2026-01-01T00:00:00.000000000Z')`); err != nil {
		t.Fatalf("insert valid started operation: %v", err)
	}
	if _, err := st.db.Exec(`UPDATE operations SET status='committed',lease_owner=NULL,
		lease_until=NULL,result_json='{}',finished_at='2026-01-01T00:30:00.000000000Z'
		WHERE operation_id='operation-valid-committed'`); err != nil {
		t.Fatalf("commit valid operation: %v", err)
	}

	var status string
	var leaseOwner, leaseUntil sql.NullString
	var result []byte
	var finishedAt sql.NullString
	if err := st.db.QueryRow(`SELECT status,lease_owner,lease_until,result_json,finished_at
		FROM operations WHERE operation_id='operation-valid-committed'`).
		Scan(&status, &leaseOwner, &leaseUntil, &result, &finishedAt); err != nil {
		t.Fatal(err)
	}
	if status != "committed" || leaseOwner.Valid || leaseUntil.Valid || string(result) != `{}` || !finishedAt.Valid {
		t.Fatalf("valid committed shape = status %q lease (%#v, %#v) result %q finished %#v",
			status, leaseOwner, leaseUntil, result, finishedAt)
	}
}

func TestSchemaEnforcesAgentRunFinishShape(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	insertNode(t, st.db)
	insertProfile(t, st.db)
	started := "2026-01-01T00:00:00.000000000Z"
	finished := "2026-01-01T00:01:00.000000000Z"

	for _, status := range []string{"starting", "running"} {
		_, err := st.db.Exec(`INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
			launcher_diagnostic_json,runtime_ids_json,status,started_at,finished_at)
			VALUES(?, 'teamwork-default','{}','test','codex-app-server','{}','{}',?,?,?)`,
			"run-active-finished-"+status, status, started, finished)
		if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
			t.Fatalf("active AgentRun %q with finished_at error = %v, want CHECK failure", status, err)
		}
	}
	for _, status := range []string{"runtime_finished", "outcome_accepted", "requeued", "rejected", "failed", "dead"} {
		_, err := st.db.Exec(`INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
			launcher_diagnostic_json,runtime_ids_json,status,started_at)
			VALUES(?, 'teamwork-default','{}','test','codex-app-server','{}','{}',?,?)`,
			"run-finished-missing-"+status, status, started)
		if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
			t.Fatalf("finished AgentRun %q without finished_at error = %v, want CHECK failure", status, err)
		}
	}
	if _, err := st.db.Exec(`INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
		launcher_diagnostic_json,runtime_ids_json,status,started_at,finished_at,completion_at,
		completion_receipt_json)
		VALUES('run-runtime-finished-valid','teamwork-default','{}','test','codex-app-server','{}','{}',
		'runtime_finished',?,?,?,'{}')`, started, finished, finished); err != nil {
		t.Fatalf("valid runtime_finished AgentRun error = %v", err)
	}
	if _, err := st.db.Exec(`INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
		launcher_diagnostic_json,runtime_ids_json,status,started_at,finished_at)
		VALUES('run-runtime-finished-without-completion','teamwork-default','{}','test',
		'codex-app-server','{}','{}','runtime_finished',?,?)`, started, finished); err == nil ||
		!strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("runtime_finished missing completion evidence error = %v, want CHECK failure", err)
	}
	for _, test := range []struct {
		name         string
		completionAt any
		receipt      any
	}{
		{name: "time without receipt", completionAt: finished},
		{name: "receipt without time", receipt: []byte(`{}`)},
		{name: "noncanonical time", completionAt: "2026-01-01T00:01:00Z", receipt: []byte(`{}`)},
		{name: "normalized invalid date", completionAt: "2026-02-30T00:01:00.000000000Z", receipt: []byte(`{}`)},
		{name: "below UnixNano range", completionAt: "1677-09-21T00:12:43.145224191Z", receipt: []byte(`{}`)},
		{name: "above UnixNano range", completionAt: "2262-04-11T23:47:16.854775808Z", receipt: []byte(`{}`)},
		{name: "completion before finish", completionAt: started, receipt: []byte(`{}`)},
	} {
		_, err := st.db.Exec(`INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
			launcher_diagnostic_json,runtime_ids_json,status,started_at,finished_at,completion_at,
			completion_receipt_json) VALUES(?, 'teamwork-default','{}','test','codex-app-server',
			'{}','{}','outcome_accepted',?,?,?,?)`, "run-completion-invalid-"+test.name,
			started, finished, test.completionAt, test.receipt)
		if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
			t.Fatalf("%s error = %v, want CHECK failure", test.name, err)
		}
	}
	if _, err := st.db.Exec(`INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
		launcher_diagnostic_json,runtime_ids_json,status,started_at,completion_at,completion_receipt_json)
		VALUES('run-active-completed','teamwork-default','{}','test','codex-app-server','{}','{}',
		'starting',?,?,'{}')`, started, finished); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("active completion evidence error = %v, want CHECK failure", err)
	}
	wakeAt := "2026-01-01T00:00:30.000000000Z"
	for _, test := range []struct {
		name    string
		at      any
		receipt any
	}{
		{name: "time without receipt", at: wakeAt},
		{name: "receipt without time", receipt: []byte(`{"hook_id":"hook-only"}`)},
		{name: "empty receipt", at: wakeAt, receipt: []byte(`{}`)},
	} {
		_, err := st.db.Exec(`INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
			launcher_diagnostic_json,runtime_ids_json,status,wake_delivered_at,wake_receipt_json,
			started_at,finished_at) VALUES(?, 'teamwork-default','{}','test','codex-app-server',
			'{}','{}','outcome_accepted',?,?,?,?)`, "run-wake-invalid-"+test.name,
			test.at, test.receipt, started, finished)
		if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
			t.Fatalf("wake %s error = %v, want CHECK failure", test.name, err)
		}
	}
	if _, err := st.db.Exec(`INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
		launcher_diagnostic_json,runtime_ids_json,status,wake_delivered_at,wake_receipt_json,
		started_at,finished_at) VALUES('run-wake-valid','teamwork-default','{}','test','codex-app-server',
		'{}','{}','outcome_accepted',?,'{"hook_id":"hook-valid"}',?,?)`, wakeAt, started, finished); err != nil {
		t.Fatalf("valid wake receipt error = %v", err)
	}
	wakeAfterFinish := "2026-01-01T00:02:00.000000000Z"
	completion := "2026-01-01T00:03:00.000000000Z"
	if _, err := st.db.Exec(`INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
		launcher_diagnostic_json,runtime_ids_json,status,wake_delivered_at,wake_receipt_json,started_at,finished_at,
		completion_at,completion_receipt_json) VALUES('run-wake-after-finish','teamwork-default','{}',
		'test','codex-app-server','{}','{}','outcome_accepted',?,'{}',?,?,?,'{}')`, wakeAfterFinish,
		started, finished, completion); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("wake after finish error = %v, want CHECK failure", err)
	}
	unequalRuntimeCompletion := "2026-01-01T00:02:00.000000000Z"
	if _, err := st.db.Exec(`INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,runtime_kind,
		launcher_diagnostic_json,runtime_ids_json,status,started_at,finished_at,completion_at,
		completion_receipt_json) VALUES('run-runtime-finished-unequal','teamwork-default','{}','test',
		'codex-app-server','{}','{}','runtime_finished',?,?,?,'{}')`, started, finished,
		unequalRuntimeCompletion); err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("runtime_finished unequal completion error = %v, want CHECK failure", err)
	}
}

func TestSchemaSealsVerifiedArtifactRootBlockMap(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	manifest := []byte(`{"entries":[],"kind":"sealed-test","total_bytes":2}`)
	root := model.Sum([]byte("sealed-artifact-root"))
	blockA := model.Sum([]byte("sealed-block-a"))
	blockB := model.Sum([]byte("sealed-block-b"))
	created := "2026-07-16T13:00:00.000000000Z"
	verified := "2026-07-16T13:00:01.000000000Z"
	if _, err := st.db.Exec(`INSERT INTO artifact_roots(root_digest,manifest_json,manifest_digest,
		total_bytes,state,created_at) VALUES(?,?,?,2,'staged',?)`, root.String(), manifest,
		model.Sum(manifest).Bytes(), created); err != nil {
		t.Fatal(err)
	}
	for _, block := range []model.Digest{blockA, blockB} {
		if _, err := st.db.Exec(`INSERT INTO artifact_blocks(block_digest,size_bytes,created_at)
			VALUES(?,1,?)`, block.String(), created); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.db.Exec(`INSERT INTO artifact_root_blocks(root_digest,ordinal,logical_path,
		offset_bytes,length_bytes,block_digest,mode) VALUES(?,0,'result.txt',0,1,?,420)`,
		root.String(), blockA.String()); err != nil {
		t.Fatalf("staged block map insert: %v", err)
	}
	if _, err := st.db.Exec(`UPDATE artifact_roots SET state='verified',verified_at=? WHERE root_digest=?`,
		verified, root.String()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec(`INSERT INTO artifact_root_blocks(root_digest,ordinal,logical_path,
		offset_bytes,length_bytes,block_digest,mode) VALUES(?,1,'result.txt',1,1,?,420)`,
		root.String(), blockB.String()); err == nil || !strings.Contains(err.Error(), "block map is sealed") {
		t.Fatalf("verified block map append error = %v", err)
	}
	if _, err := st.db.Exec(`DELETE FROM artifact_root_blocks WHERE root_digest=? AND ordinal=0`,
		root.String()); err == nil || !strings.Contains(err.Error(), "block map is sealed") {
		t.Fatalf("verified block map delete error = %v", err)
	}
	if _, err := st.db.Exec(`UPDATE artifact_roots SET verified_at=? WHERE root_digest=?`,
		"2026-07-16T13:00:02.000000000Z", root.String()); err == nil ||
		!strings.Contains(err.Error(), "verification time is immutable") {
		t.Fatalf("verified timestamp rewrite error = %v", err)
	}
	var count int
	if err := st.db.QueryRow("SELECT COUNT(*) FROM artifact_root_blocks WHERE root_digest=?",
		root.String()).Scan(&count); err != nil || count != 1 {
		t.Fatalf("sealed block map count = %d, err=%v", count, err)
	}
	if _, err := st.db.Exec(`DELETE FROM artifact_roots WHERE root_digest=?`, root.String()); err != nil {
		t.Fatalf("whole unprovenanced root cleanup: %v", err)
	}
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM artifact_root_blocks WHERE root_digest=?`,
		root.String()).Scan(&count); err != nil || count != 0 {
		t.Fatalf("block map after whole-root cleanup = %d, err=%v", count, err)
	}
}

func TestSchemaV1RejectsWrongSQLiteTypes(t *testing.T) {
	st := openTestStore(t)
	insertNode(t, st.db)
	insertChannelAndEvent(t, st.db)

	_, err := st.db.Exec(
		"INSERT INTO works(channel_id, home_peer_id, work_id, participant_roster_revision, version, iteration, deadline_unix_nano, state, state_json, updated_by_event, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"channel-one",
		"peer-home",
		"work-one",
		1,
		1,
		1,
		1.25,
		"OFFERED",
		[]byte("{}"),
		"event-one",
		"2026-01-01T00:00:00Z",
	)
	if err == nil || !strings.Contains(err.Error(), "CHECK constraint failed") {
		t.Fatalf("REAL deadline error = %v, want typeof(integer) rejection", err)
	}
}

func TestOpenFailsClosedForWrongOrTamperedSchema(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
	}{
		{
			name:       "future-version",
			statements: []string{"PRAGMA user_version = 2"},
		},
		{
			name:       "old-version-zero-with-objects",
			statements: []string{"CREATE TABLE legacy_state(id INTEGER PRIMARY KEY)"},
		},
		{
			name: "version-one-incomplete",
			statements: []string{
				"CREATE TABLE node(singleton INTEGER PRIMARY KEY)",
				"PRAGMA user_version = 1",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "node", "node.db")
			rawExec(t, path, test.statements...)
			st, err := Open(context.Background(), path)
			if st != nil {
				_ = st.Close()
				t.Fatal("Open() returned Store for unsupported schema")
			}
			if !errors.Is(err, ErrUnsupportedSchema) {
				t.Fatalf("Open() error = %v, want ErrUnsupportedSchema", err)
			}
			if !strings.Contains(err.Error(), "eject/setup") {
				t.Fatalf("Open() error = %q, want recovery guidance", err)
			}
		})
	}
}

func TestOpenRejectsSameNameSchemaTampering(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
	}{
		{
			name:       "table-shape",
			statements: []string{"ALTER TABLE node ADD COLUMN injected TEXT"},
		},
		{
			name: "trigger-body",
			statements: []string{
				"DROP TRIGGER events_no_update",
				"CREATE TRIGGER events_no_update BEFORE UPDATE ON events BEGIN SELECT 1; END",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "node", "node.db")
			st, err := Open(context.Background(), path)
			if err != nil {
				t.Fatalf("initial Open() error = %v", err)
			}
			if err := st.Close(); err != nil {
				t.Fatalf("initial Close() error = %v", err)
			}
			rawExec(t, path, test.statements...)

			st, err = Open(context.Background(), path)
			if st != nil {
				_ = st.Close()
				t.Fatal("Open() returned Store for tampered schema")
			}
			if !errors.Is(err, ErrUnsupportedSchema) {
				t.Fatalf("Open() error = %v, want ErrUnsupportedSchema", err)
			}
			if !strings.Contains(err.Error(), "definition mismatch") {
				t.Fatalf("Open() error = %q, want definition mismatch", err)
			}
		})
	}
}

func TestOpenRejectsForeignKeyCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node", "node.db")
	st, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	rawExec(t, path,
		"PRAGMA foreign_keys = OFF",
		"INSERT INTO operations(operation_id, profile_id, agent_run_id, client_key_hash, kind, request_digest, status, lease_owner, lease_until, created_at) VALUES('orphan-operation', 'teamwork-default', 'run-orphan', x'01', 'teamwork.offer', x'02', 'started', 'writer', '2026-01-01T01:00:00Z', '2026-01-01T00:00:00Z')",
	)
	st, err = Open(context.Background(), path)
	if st != nil {
		_ = st.Close()
		t.Fatal("Open() accepted foreign-key corruption")
	}
	if err == nil || !strings.Contains(err.Error(), "foreign key violation") {
		t.Fatalf("Open() error = %v, want foreign key violation", err)
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "node", "node.db")
	st, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return st
}

func rawExec(t *testing.T, path string, statements ...string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), directoryMode); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func insertNode(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(
		"INSERT INTO node(singleton, peer_id, origin_epoch, active_asset_rev, created_at, updated_at) VALUES(1, ?, ?, ?, ?, ?)",
		"peer-home",
		"epoch-one",
		"asset-one",
		"2026-01-01T00:00:00Z",
		"2026-01-01T00:00:00Z",
	); err != nil {
		t.Fatalf("insert node: %v", err)
	}
}

func insertProfile(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec(
		"INSERT INTO profiles(profile_id, principal, workspace_root, host, runtime_kind, credential_hash, active_asset_rev, handling_budget_json, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"teamwork-default",
		"principal-one",
		"/workspace",
		"codex",
		"codex-app-server",
		[]byte("credential-one"),
		"asset-one",
		[]byte("{}"),
		"2026-01-01T00:00:00Z",
		"2026-01-01T00:00:00Z",
	); err != nil {
		t.Fatalf("insert profile: %v", err)
	}
}

func insertChannelAuthority(
	t *testing.T,
	db *sql.DB,
	channelID string,
	alias string,
	ownerPeer string,
	originEpoch string,
	recordHash string,
) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin channel authority fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`INSERT INTO channels(channel_id,name,local_alias,owner_peer_id,
		owner_public_key,descriptor_json,descriptor_digest,descriptor_signature,member_limit,
		roster_head_revision,roster_head_hash,status,topic_state,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,8,1,?,'active','joined',?,?)`, channelID, strings.ToUpper(alias),
		alias, ownerPeer, []byte("key-"+ownerPeer), []byte("descriptor-"+channelID),
		[]byte("descriptor-digest-"+channelID), []byte("descriptor-signature-"+channelID),
		[]byte(recordHash), "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"); err != nil {
		t.Fatalf("insert channel authority: %v", err)
	}
	if _, err := tx.Exec(`INSERT INTO channel_members(channel_id,revision,record_hash,
		previous_hash,member_peer_id,origin_epoch,display_label,public_key,multiaddrs_json,
		protocols_json,limits_json,status,signed_record_json,owner_signature,created_at)
		VALUES(?,1,?,NULL,?,?,?,?,?,?,?,'active',?,?,?)`, channelID, []byte(recordHash), ownerPeer,
		originEpoch, alias, []byte("key-"+ownerPeer), []byte(`["/ip4/127.0.0.1/tcp/4001"]`),
		[]byte(`["/mnemon/artifacts/1","/mnemon/channel/1","/mnemon/events/1"]`),
		[]byte(`{"profile":"r5-hermetic-v1"}`), []byte("member-"+channelID),
		[]byte("member-signature-"+channelID), "2026-01-01T00:00:00Z"); err != nil {
		t.Fatalf("insert channel owner member: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit channel authority fixture: %v", err)
	}
}

func insertReviewFixture(t *testing.T, db *sql.DB) {
	t.Helper()
	insertChannelAndEvent(t, db)
	if _, err := db.Exec(
		"INSERT INTO works(channel_id, home_peer_id, work_id, participant_roster_revision, version, iteration, deadline_unix_nano, state, state_json, updated_by_event, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"channel-one",
		"peer-home",
		"work-one",
		1,
		1,
		1,
		int64(1_800_000_000_000_000_000),
		"OFFERED",
		[]byte("{}"),
		"event-one",
		"2026-01-01T00:00:00.000000000Z",
	); err != nil {
		t.Fatalf("insert work: %v", err)
	}
}

func insertChannelAndEvent(t *testing.T, db *sql.DB) {
	t.Helper()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin channel fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(
		"INSERT INTO channels(channel_id, name, local_alias, owner_peer_id, owner_public_key, descriptor_json, descriptor_digest, descriptor_signature, member_limit, roster_head_revision, roster_head_hash, status, topic_state, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"channel-one",
		"Alpha",
		"alpha",
		"peer-home",
		[]byte("owner-key"),
		[]byte("descriptor-one"),
		[]byte("descriptor-digest-one"),
		[]byte("descriptor-signature-one"),
		8,
		1,
		[]byte("record-one"),
		"active",
		"joined",
		"2026-01-01T00:00:00Z",
		"2026-01-01T00:00:00Z",
	); err != nil {
		t.Fatalf("insert channel: %v", err)
	}
	if _, err := tx.Exec(
		"INSERT INTO channel_members(channel_id, revision, record_hash, previous_hash, member_peer_id, origin_epoch, display_label, public_key, multiaddrs_json, protocols_json, limits_json, status, signed_record_json, owner_signature, created_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"channel-one",
		1,
		[]byte("record-one"),
		nil,
		"peer-home",
		"epoch-one",
		"home",
		[]byte("owner-key"),
		[]byte("[]"),
		[]byte("[]"),
		[]byte("{}"),
		"active",
		[]byte("{}"),
		[]byte("signature"),
		"2026-01-01T00:00:00Z",
	); err != nil {
		t.Fatalf("insert channel member: %v", err)
	}
	if _, err := tx.Exec(
		"INSERT INTO events(event_id, schema_version, channel_id, origin_peer_id, origin_epoch, origin_seq, channel_seq, origin_member_revision, origin_member_record_hash, publication_roster_revision, publication_roster_hash, source, actor_principal, event_type, audience_json, resource_json, work_home_peer_id, work_id, summary, payload_json, artifact_roots_json, caused_by_json, canonical_event_json, event_digest, canonical_publication_json, publication_digest, origin_signature, created_at, accepted_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"event-one",
		1,
		"channel-one",
		"peer-home",
		"epoch-one",
		1,
		1,
		1,
		[]byte("record-one"),
		1,
		[]byte("record-one"),
		"local",
		"principal-one",
		"review.offered",
		[]byte("[]"),
		[]byte("{}"),
		"peer-home",
		"work-one",
		"review",
		[]byte("{}"),
		[]byte("[]"),
		[]byte("[]"),
		[]byte("{}"),
		[]byte("event-digest"),
		[]byte("{}"),
		[]byte("publication-digest"),
		[]byte("origin-signature"),
		"2026-01-01T00:00:00Z",
		"2026-01-01T00:00:00Z",
	); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit channel fixture: %v", err)
	}
}
