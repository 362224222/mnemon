package store

import (
	"context"
	"database/sql"
	"errors"
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
				"channels_terminal_status_update channels_conflicted_status_update channels_leaving_status_update " +
				"channel_members_no_update channel_members_no_delete channel_conflicts_no_update " +
				"channel_conflicts_no_delete channels_conflicted_requires_evidence channel_members_no_reactivate " +
				"channel_members_capacity_insert peer_bindings_no_self_insert peer_bindings_no_self_update " +
				"peer_bindings_identity_epoch_immutable peer_bindings_revoked_terminal events_imported_binding_insert " +
				"gossip_publications_event_scope_insert gossip_publications_identity_immutable " +
				"peer_deliveries_event_scope_insert peer_deliveries_identity_immutable " +
				"peer_deliveries_scanned_requires_cursor peer_inbox_event_scope_insert " +
				"peer_inbox_binding_epoch_insert peer_inbox_transport_binding_insert " +
				"peer_inbox_publication_member_insert peer_inbox_identity_immutable peer_inbox_event_scope_update " +
				"peer_inbox_receipt_scope_insert peer_inbox_receipt_scope_update publication_conflicts_no_update " +
				"publication_conflicts_no_delete publication_conflicts_existing_scope_insert " +
				"origin_quarantines_no_update origin_quarantines_no_delete peer_cursors_binding_epoch_insert " +
				"peer_cursors_binding_epoch_update peer_cursors_identity_baseline_immutable " +
				"peer_cursors_monotonic_update peer_bindings_no_active_insert " +
				"peer_bindings_activate_requires_cursor publication_epochs_local_origin_insert " +
				"publication_epochs_identity_immutable publication_epochs_monotonic_update " +
				"publication_epochs_no_delete peer_pull_acks_identity_baseline_immutable " +
				"peer_pull_acks_monotonic_update peer_pull_acks_confirmation_immutable " +
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
	if got := len(actual["table"]) + len(actual["index"]) + len(actual["trigger"]); got != 126 {
		t.Fatalf("explicit object count = %d, want 126", got)
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
	if _, err := db.Exec(
		"INSERT INTO channels(channel_id, name, local_alias, owner_peer_id, owner_public_key, member_limit, roster_head_revision, roster_head_hash, status, topic_state, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"channel-one",
		"Alpha",
		"alpha",
		"peer-home",
		[]byte("owner-key"),
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
	if _, err := db.Exec(
		"INSERT INTO channel_members(channel_id, revision, record_hash, previous_hash, member_peer_id, origin_epoch, display_label, public_key, multiaddrs_json, status, signed_record_json, owner_signature, created_at) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"channel-one",
		1,
		[]byte("record-one"),
		nil,
		"peer-home",
		"epoch-one",
		"home",
		[]byte("member-key"),
		[]byte("[]"),
		"active",
		[]byte("{}"),
		[]byte("signature"),
		"2026-01-01T00:00:00Z",
	); err != nil {
		t.Fatalf("insert channel member: %v", err)
	}
	if _, err := db.Exec(
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
}
