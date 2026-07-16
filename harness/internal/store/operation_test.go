package store

import (
	"context"
	"errors"
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
	manifest, _ := model.NewJSON([]byte(`{"entries":[]}`))
	rootDigest := model.Sum([]byte("operation-fence-root"))
	manifestDigest := model.Sum(manifest.Bytes())
	if _, err := st.CheckpointVerifiedArtifactRoot(context.Background(), VerifiedArtifactRoot{
		RootDigest: rootDigest, Manifest: manifest, ManifestDigest: manifestDigest,
		CreatedAt: leaseExpiredAt, VerifiedAt: leaseExpiredAt,
	}); err != nil {
		t.Fatal(err)
	}
	capture, _ := model.NewJSON([]byte(`{"roots":[{"manifest_digest":"` + manifestDigest.String() +
		`","root_digest":"` + rootDigest.String() + `"}]}`))
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
	changed, _ := model.NewJSON([]byte(`{"roots":[]}`))
	if _, err := st.CheckpointOperationCapture(context.Background(), requested.ID(), "owner-two", leaseExpiredAt, changed); !errors.Is(err, ErrOperationMismatch) {
		t.Fatalf("changed checkpoint error = %v", err)
	}
	arrayCapture, _ := model.NewJSON([]byte(`[]`))
	if _, err := st.CheckpointOperationCapture(context.Background(), requested.ID(), "owner-two", leaseExpiredAt, arrayCapture); err == nil {
		t.Fatal("array capture checkpoint was accepted")
	}
	result, _ := model.NewJSON([]byte(`{"error":"invalid_action","replayed":false}`))
	if _, err := st.RejectOperation(context.Background(), requested.ID(), "owner-one", leaseExpiredAt,
		result); !errors.Is(err, ErrOperationFence) {
		t.Fatalf("stale reject owner error = %v", err)
	}
	rejected, err := st.RejectOperation(context.Background(), requested.ID(), "owner-two", leaseExpiredAt, result)
	if err != nil || rejected.Status() != model.OperationRejected {
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
	if _, err := st.db.Exec("UPDATE agent_runs SET runtime_kind='claude-cli' WHERE run_id='run-runtime-drift'"); err != nil {
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
