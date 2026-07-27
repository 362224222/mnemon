package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestReserveOperationFirstReplayMismatchAndContextGate(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	node, profile := bootstrapValues(t, "peer-operation", "principal-operation", "/workspace/operation")
	_, _ = activateTestNode(t, st, node, profile)
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	insertOperationAgentRun(t, st, profile, "run-operation-one", "running", now)
	insertOperationAgentRun(t, st, profile, "run-operation-two", "running", now)
	contextHash := model.Sum([]byte("context-one"))
	requested := startedOperation(t, "operation-one", "key-one", "request-one", "run-operation-one", "owner-one", now, &contextHash)

	first, err := st.ReserveOperation(context.Background(), requested, now)
	if err != nil || first.Replayed || !first.Acquired {
		t.Fatalf("first ReserveOperation() = (%#v, %v)", first, err)
	}
	replay, err := st.ReserveOperation(context.Background(), requested, now.Add(time.Second))
	if err != nil || !replay.Replayed || !replay.Acquired || replay.Operation.ID() != requested.ID() {
		t.Fatalf("replay ReserveOperation() = (%#v, %v)", replay, err)
	}

	mismatch := startedOperation(t, "operation-one", "key-one", "request-different", "run-operation-one", "owner-one", now, &contextHash)
	if _, err := st.ReserveOperation(context.Background(), mismatch, now); !errors.Is(err, ErrOperationMismatch) {
		t.Fatalf("mismatch error = %v", err)
	}
	crossRun := startedOperation(t, "operation-one", "key-one", "request-one", "run-operation-two", "owner-one", now, &contextHash)
	if _, err := st.ReserveOperation(context.Background(), crossRun, now); !errors.Is(err, ErrOperationMismatch) {
		t.Fatalf("cross-Run started replay error = %v", err)
	}
	other := startedOperation(t, "operation-two", "key-two", "request-two", "run-operation-two", "owner-two", now, &contextHash)
	if _, err := st.ReserveOperation(context.Background(), other, now); !errors.Is(err, ErrOperationPending) {
		t.Fatalf("context collision error = %v", err)
	}
}

func TestReserveOperationLeaseFenceCaptureAndRejectedReplay(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	node, profile := bootstrapValues(t, "peer-fence", "principal-fence", "/workspace/fence")
	_, _ = activateTestNode(t, st, node, profile)
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	insertOperationAgentRun(t, st, profile, "run-fence", "running", now)
	insertOperationAgentRun(t, st, profile, "run-replay", "running", now)
	requested := startedOperation(t, "operation-fence", "key-fence", "request-fence", "run-fence", "owner-one", now, nil)
	if _, err := st.ReserveOperation(context.Background(), requested, now); err != nil {
		t.Fatal(err)
	}
	contender := startedOperation(t, "operation-fence", "key-fence", "request-fence", "run-fence", "owner-two", now, nil)
	if _, err := st.ReserveOperation(context.Background(), contender, now.Add(time.Second)); !errors.Is(err, ErrOperationPending) {
		t.Fatalf("active contender error = %v", err)
	}

	leaseExpiredAt, _ := requested.LeaseUntil()
	reclaimRequest := startedOperation(t, "operation-fence", "key-fence", "request-fence", "run-fence", "owner-two", leaseExpiredAt, nil)
	reclaimed, err := st.ReserveOperation(context.Background(), reclaimRequest, leaseExpiredAt)
	if err != nil || !reclaimed.Acquired || reclaimed.Operation.LeaseOwner() != "owner-two" {
		t.Fatalf("expired reclaim = (%#v, %v)", reclaimed, err)
	}
	capture, _ := model.NewJSON([]byte(`{"roots":[]}`))
	if _, err := st.CheckpointOperationCapture(context.Background(), requested.ID(), "owner-one",
		leaseExpiredAt, capture); !errors.Is(err, ErrOperationFence) {
		t.Fatalf("stale checkpoint owner error = %v", err)
	}
	if replay, err := st.CheckpointOperationCapture(context.Background(), requested.ID(), "owner-two", leaseExpiredAt, capture); err != nil || replay {
		t.Fatalf("first checkpoint = (%t, %v)", replay, err)
	}
	if replay, err := st.CheckpointOperationCapture(context.Background(), requested.ID(), "owner-two", leaseExpiredAt, capture); err != nil || !replay {
		t.Fatalf("checkpoint replay = (%t, %v)", replay, err)
	}
	arrayCapture, _ := model.NewJSON([]byte(`[]`))
	if _, err := st.CheckpointOperationCapture(context.Background(), requested.ID(), "owner-two", leaseExpiredAt, arrayCapture); err == nil {
		t.Fatal("array capture checkpoint was accepted")
	}
	result := mustManagedOperationRejectionReceipt(t, requested.ID(), "invalid_argument",
		"invalid Teamwork action")
	if _, err := st.RejectOperation(context.Background(), requested.ID(), "owner-one", leaseExpiredAt,
		result); !errors.Is(err, ErrOperationFence) {
		t.Fatalf("stale reject owner error = %v", err)
	}
	rejected, err := st.RejectOperation(context.Background(), requested.ID(), "owner-two", leaseExpiredAt, result)
	if err != nil || rejected.Operation.Status() != model.OperationRejected {
		t.Fatalf("RejectOperation() = (%#v, %v)", rejected, err)
	}
	if _, err := st.db.Exec("UPDATE profiles SET enabled=0 WHERE profile_id='teamwork-default'"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.Exec("UPDATE node SET active_asset_rev='asset-drift' WHERE singleton=1"); err != nil {
		t.Fatal(err)
	}
	terminalReplay := startedOperation(t, "operation-fence", "key-fence", "request-fence", "run-replay", "owner-replay", leaseExpiredAt, nil)
	replayed, err := st.ReserveOperation(context.Background(), terminalReplay, leaseExpiredAt.Add(time.Second))
	if err != nil || !replayed.Replayed || replayed.Acquired || replayed.Operation.Status() != model.OperationRejected {
		t.Fatalf("rejected replay = (%#v, %v)", replayed, err)
	}
	if replayed.Operation.AgentRunID() != requested.AgentRunID() {
		t.Fatalf("terminal replay rewrote durable AgentRun: got %s want %s", replayed.Operation.AgentRunID().String(), requested.AgentRunID().String())
	}
}

func TestCheckpointOperationCaptureEmptyProjectionIsDurable(t *testing.T) {
	t.Parallel()
	fixture := newOperationCaptureFixture(t, "projection-empty")
	nonempty := operationCaptureJSON(t, []captureRoot{{
		RootDigest:     model.Sum([]byte("nonempty-root")),
		ManifestDigest: model.Sum([]byte("nonempty-manifest")),
	}})
	if replayed, err := fixture.store.CheckpointOperationCapture(context.Background(),
		fixture.operation.ID(), fixture.operation.LeaseOwner(), fixture.now, nonempty); replayed ||
		!errors.Is(err, ErrCaptureMismatch) {
		t.Fatalf("nonempty checkpoint = (%t,%v)", replayed, err)
	}
	assertOperationArtifactProjection(t, fixture.store, fixture.operation.ID(), nil)
	var captureIsNull int
	if err := fixture.store.db.QueryRow(`SELECT capture_json IS NULL FROM operations
		WHERE operation_id=?`, fixture.operation.ID().String()).Scan(&captureIsNull); err != nil ||
		captureIsNull != 1 {
		t.Fatalf("capture after nonempty checkpoint NULL = (%d,%v)", captureIsNull, err)
	}

	if replayed, err := fixture.store.CheckpointOperationCapture(context.Background(),
		fixture.operation.ID(), fixture.operation.LeaseOwner(), fixture.now, fixture.capture); err != nil || replayed {
		t.Fatalf("empty fresh checkpoint = (%t,%v)", replayed, err)
	}
	assertOperationArtifactProjection(t, fixture.store, fixture.operation.ID(), nil)
	if replayed, err := fixture.store.CheckpointOperationCapture(context.Background(),
		fixture.operation.ID(), fixture.operation.LeaseOwner(), fixture.now.Add(time.Second),
		fixture.capture); err != nil || !replayed {
		t.Fatalf("empty replay checkpoint = (%t,%v)", replayed, err)
	}
}

func TestCheckpointOperationCaptureFenceLeavesNoProjection(t *testing.T) {
	t.Parallel()
	fixture := newOperationCaptureFixture(t, "projection-fence")
	leaseUntil, _ := fixture.operation.LeaseUntil()
	for _, attempt := range []struct {
		name  string
		owner string
		at    time.Time
	}{
		{name: "wrong owner", owner: "projection-intruder", at: fixture.now},
		{name: "expired lease", owner: fixture.operation.LeaseOwner(), at: leaseUntil},
	} {
		if replayed, err := fixture.store.CheckpointOperationCapture(context.Background(),
			fixture.operation.ID(), attempt.owner, attempt.at, fixture.capture); replayed ||
			!errors.Is(err, ErrOperationFence) {
			t.Fatalf("%s checkpoint = (%t,%v)", attempt.name, replayed, err)
		}
		assertOperationArtifactProjection(t, fixture.store, fixture.operation.ID(), nil)
		var captureIsNull int
		if err := fixture.store.db.QueryRow(`SELECT capture_json IS NULL FROM operations
			WHERE operation_id=?`, fixture.operation.ID().String()).Scan(&captureIsNull); err != nil ||
			captureIsNull != 1 {
			t.Fatalf("%s capture NULL = (%d,%v)", attempt.name, captureIsNull, err)
		}
	}
}

func TestCheckpointOperationCaptureUpdateFailureRollsBackProjection(t *testing.T) {
	t.Parallel()
	fixture := newOperationCaptureFixture(t, "projection-rollback")
	mustExec(t, fixture.store, `CREATE TRIGGER test_operation_capture_abort
		BEFORE UPDATE OF capture_json ON operations
		WHEN NEW.operation_id='operation-projection-rollback'
		BEGIN SELECT RAISE(ABORT, 'forced capture update failure'); END`)
	if replayed, err := fixture.store.CheckpointOperationCapture(context.Background(),
		fixture.operation.ID(), fixture.operation.LeaseOwner(), fixture.now, fixture.capture); replayed ||
		err == nil || !strings.Contains(err.Error(), "forced capture update failure") {
		t.Fatalf("forced update failure = (%t,%v)", replayed, err)
	}
	assertOperationArtifactProjection(t, fixture.store, fixture.operation.ID(), nil)
	var captureIsNull int
	if err := fixture.store.db.QueryRow(`SELECT capture_json IS NULL FROM operations
		WHERE operation_id=?`, fixture.operation.ID().String()).Scan(&captureIsNull); err != nil ||
		captureIsNull != 1 {
		t.Fatalf("rolled-back capture NULL = (%d,%v)", captureIsNull, err)
	}
}

func TestCheckpointOperationCaptureConcurrentIdenticalProjection(t *testing.T) {
	t.Parallel()
	fixture := newOperationCaptureFixture(t, "projection-race")
	type result struct {
		replayed bool
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			replayed, err := fixture.store.CheckpointOperationCapture(context.Background(),
				fixture.operation.ID(), fixture.operation.LeaseOwner(), fixture.now, fixture.capture)
			results <- result{replayed: replayed, err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	fresh, replayed := 0, 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent checkpoint error = %v", result.err)
		}
		if result.replayed {
			replayed++
		} else {
			fresh++
		}
	}
	if fresh != 1 || replayed != 1 {
		t.Fatalf("concurrent checkpoint outcomes fresh=%d replayed=%d", fresh, replayed)
	}
	assertOperationArtifactProjection(t, fixture.store, fixture.operation.ID(), nil)
}

func TestReserveOperationRequiresActiveAgentRunAuthority(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	node, profile := bootstrapValues(t, "peer-operation-run", "principal-operation-run", "/workspace/operation-run")
	_, _ = activateTestNode(t, st, node, profile)
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)

	missing := startedOperation(t, "operation-missing-run", "key-missing-run", "request-missing-run", "run-missing", "owner-run", now, nil)
	if _, err := st.ReserveOperation(context.Background(), missing, now); err == nil {
		t.Fatal("operation with missing AgentRun was reserved")
	}

	insertOperationAgentRun(t, st, profile, "run-terminal", "failed", now)
	terminal := startedOperation(t, "operation-terminal-run", "key-terminal-run", "request-terminal-run", "run-terminal", "owner-run", now, nil)
	if _, err := st.ReserveOperation(context.Background(), terminal, now); err == nil {
		t.Fatal("operation with terminal AgentRun was reserved")
	}

	insertOperationAgentRun(t, st, profile, "run-runtime-drift", "running", now)
	mustExec(t, st, `DROP TRIGGER agent_runs_creation_identity_immutable`)
	if _, err := st.db.Exec("UPDATE agent_runs SET runtime_kind='unsupported-runtime' WHERE run_id='run-runtime-drift'"); err != nil {
		t.Fatal(err)
	}
	runtimeDrift := startedOperation(t, "operation-runtime-drift", "key-runtime-drift", "request-runtime-drift", "run-runtime-drift", "owner-run", now, nil)
	if _, err := st.ReserveOperation(context.Background(), runtimeDrift, now); err == nil {
		t.Fatal("operation with cross-runtime AgentRun was reserved")
	}

	conn, err := st.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), "PRAGMA ignore_check_constraints=ON"); err != nil {
		t.Fatal(err)
	}
	_, err = conn.ExecContext(context.Background(), `INSERT INTO profiles(profile_id,principal,workspace_root,
		host,runtime_kind,credential_hash,active_asset_rev,handling_budget_json,enabled,created_at,updated_at)
		VALUES('cross-profile','cross-principal','/cross','codex','codex-app-server',?,?,'{}',0,?,?)`,
		model.Sum([]byte("cross-credential")).Bytes(), profile.ActiveAssetRevision(), storeTime(now), storeTime(now))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(context.Background(), "PRAGMA ignore_check_constraints=OFF"); err != nil {
		t.Fatal(err)
	}
	_, err = conn.ExecContext(context.Background(), `INSERT INTO agent_runs(run_id,profile_id,cause_json,launcher,
		runtime_kind,launcher_diagnostic_json,runtime_ids_json,status,started_at)
		VALUES('run-cross-profile','cross-profile','{}','test','codex-app-server','{}','{}','running',?)`, storeTime(now))
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	crossProfile := startedOperation(t, "operation-cross-profile", "key-cross-profile", "request-cross-profile", "run-cross-profile", "owner-run", now, nil)
	if _, err := st.ReserveOperation(context.Background(), crossProfile, now); err == nil {
		t.Fatal("operation with cross-Profile AgentRun was reserved")
	}
}

func TestReserveOperationRuntimeFinishedAllowsOnlyExistingIdentity(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	node, profile := bootstrapValues(t, "peer-operation-finished", "principal-operation-finished",
		"/workspace/operation-finished")
	_, _ = activateTestNode(t, st, node, profile)
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	insertOperationAgentRun(t, st, profile, "run-operation-finished", "running", now)
	started := startedOperation(t, "operation-before-finish", "key-before-finish", "request-before-finish",
		"run-operation-finished", "owner-before-finish", now, nil)
	if _, err := st.ReserveOperation(context.Background(), started, now); err != nil {
		t.Fatal(err)
	}
	finishedAt := now.Add(time.Second)
	if _, err := st.db.Exec(`UPDATE agent_runs SET status='runtime_finished',finished_at=?,
		completion_at=?,completion_receipt_json='{}' WHERE run_id=?`,
		storeTime(finishedAt), storeTime(finishedAt), started.AgentRunID().String()); err != nil {
		t.Fatal(err)
	}
	if replay, err := st.ReserveOperation(context.Background(), started, finishedAt); err != nil || !replay.Replayed || !replay.Acquired {
		t.Fatalf("existing operation at runtime_finished = (%#v, %v)", replay, err)
	}
	fresh := startedOperation(t, "operation-after-finish", "key-after-finish", "request-after-finish",
		"run-operation-finished", "owner-after-finish", finishedAt, nil)
	if _, err := st.ReserveOperation(context.Background(), fresh, finishedAt); err == nil {
		t.Fatal("fresh generic operation was admitted after runtime_finished")
	}
}

func TestReserveOperationRejectsActiveAssetAuthorityDrift(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	node, profile := bootstrapValues(t, "peer-operation-drift", "principal-operation-drift", "/workspace/operation-drift")
	_, _ = activateTestNode(t, st, node, profile)
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	insertOperationAgentRun(t, st, profile, "run-drift", "running", now)
	if _, err := st.db.Exec("UPDATE node SET active_asset_rev='asset-drift' WHERE singleton=1"); err != nil {
		t.Fatal(err)
	}
	requested := startedOperation(t, "operation-drift", "key-drift", "request-drift", "run-drift", "owner-drift", now, nil)
	if _, err := st.ReserveOperation(context.Background(), requested, now); err == nil {
		t.Fatal("first operation was reserved under Node/Profile asset drift")
	}
}

type operationCaptureFixture struct {
	store     *Store
	operation model.Operation
	now       time.Time
	capture   model.JSON
}

func newOperationCaptureFixture(t *testing.T, suffix string) operationCaptureFixture {
	t.Helper()
	st := openTestStore(t)
	node, profile := bootstrapValues(t, "peer-"+suffix, "principal-"+suffix,
		"/workspace/"+suffix)
	_, _ = activateTestNode(t, st, node, profile)
	now := time.Date(2026, 7, 18, 9, 0, 0, 0, time.UTC)
	runID := "run-" + suffix
	insertOperationAgentRun(t, st, profile, runID, "running", now)
	operation := startedOperation(t, "operation-"+suffix, "key-"+suffix, "request-"+suffix,
		runID, "owner-"+suffix, now, nil)
	if _, err := st.ReserveOperation(context.Background(), operation, now); err != nil {
		t.Fatalf("ReserveOperation(): %v", err)
	}
	capture := operationCaptureJSON(t, nil)
	return operationCaptureFixture{store: st, operation: operation, now: now,
		capture: capture}
}

func operationCaptureJSON(t *testing.T, roots []captureRoot) model.JSON {
	t.Helper()
	var encoded strings.Builder
	encoded.WriteString(`{"roots":[`)
	for index, root := range roots {
		if index != 0 {
			encoded.WriteByte(',')
		}
		_, _ = fmt.Fprintf(&encoded, `{"manifest_digest":"%s","root_digest":"%s"}`,
			root.ManifestDigest.String(), root.RootDigest.String())
	}
	encoded.WriteString(`]}`)
	value, err := model.NewJSON([]byte(encoded.String()))
	if err != nil {
		t.Fatalf("NewJSON(capture): %v", err)
	}
	return value
}

func assertOperationArtifactProjection(t *testing.T, st *Store, operationID model.OperationID,
	want []captureRoot,
) {
	t.Helper()
	rows, err := st.db.Query(`SELECT root_digest,manifest_digest FROM operation_artifact_roots
		WHERE operation_id=? ORDER BY root_digest`, operationID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []captureRoot
	for rows.Next() {
		var rootText string
		var manifestBytes []byte
		if err := rows.Scan(&rootText, &manifestBytes); err != nil {
			t.Fatal(err)
		}
		root, rootErr := model.ParseDigest(rootText)
		manifest, manifestErr := model.DigestFromBytes(manifestBytes)
		if rootErr != nil || manifestErr != nil {
			t.Fatalf("parse projection digest = (%v,%v)", rootErr, manifestErr)
		}
		got = append(got, captureRoot{RootDigest: root, ManifestDigest: manifest})
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("projection roots = %#v, want %#v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("projection root %d = %#v, want %#v", index, got[index], want[index])
		}
	}
}

func startedOperation(t *testing.T, idText, keyText, requestText, runText, owner string, now time.Time,
	contextHash *model.Digest,
) model.Operation {
	t.Helper()
	id, err := model.ParseOperationID(idText)
	if err != nil {
		t.Fatal(err)
	}
	runID, err := model.ParseRunID(runText)
	if err != nil {
		t.Fatal(err)
	}
	leaseUntil := now.Add(time.Minute)
	operation, err := model.NewOperation(model.OperationSpec{ID: id, ProfileID: model.TeamworkProfileID(),
		AgentRunID: runID, ClientKeyHash: model.Sum([]byte(keyText)), ContextHash: contextHash, Kind: model.OperationTeamworkOffer,
		RequestDigest: model.Sum([]byte(requestText)), Status: model.OperationStarted, LeaseOwner: owner,
		LeaseUntil: &leaseUntil, CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

func insertOperationAgentRun(t *testing.T, st *Store, profile model.Profile, runText, status string, startedAt time.Time) {
	t.Helper()
	finishedAt := any(nil)
	if status != "starting" && status != "running" {
		finishedAt = storeTime(startedAt)
	}
	_, err := st.db.Exec(`INSERT INTO agent_runs(run_id, profile_id, cause_json, launcher, runtime_kind,
		launcher_diagnostic_json, runtime_ids_json, status, started_at, finished_at)
		VALUES(?, ?, '{}', 'test', ?, '{}', '{}', ?, ?, ?)`, runText, profile.ID().String(),
		string(profile.Runtime()), status, storeTime(startedAt), finishedAt)
	if err != nil {
		t.Fatal(err)
	}
}
