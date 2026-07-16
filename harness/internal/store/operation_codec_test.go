package store

import (
	"context"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestOperationCodecPreservesLeaseAndTerminalIdentity(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	contextHash := model.Sum([]byte("codec-context"))
	started := startedOperation(t, "operation-codec", "key-codec", "request-codec", "run-codec", "owner-one", now, &contextHash)
	newLease := now.Add(2 * time.Minute)
	reclaimed, err := operationWithLease(started, "owner-two", newLease)
	if err != nil {
		t.Fatal(err)
	}
	if reclaimed.LeaseOwner() != "owner-two" || reclaimed.RequestDigest() != started.RequestDigest() {
		t.Fatalf("reclaimed operation lost identity")
	}
	result, _ := model.NewJSON([]byte(`{"ok":true}`))
	terminal, err := operationTerminal(reclaimed, model.OperationCommitted, result, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Status() != model.OperationCommitted || terminal.LeaseOwner() != "" {
		t.Fatalf("terminal operation retained lease")
	}
	if got, ok := terminal.ContextHash(); !ok || got != contextHash {
		t.Fatalf("terminal operation lost context")
	}
	if terminal.AgentRunID() != started.AgentRunID() {
		t.Fatalf("terminal operation lost AgentRun identity")
	}
}

func TestOperationCodecRejectsNoncanonicalDurableJSON(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	node, profile := bootstrapValues(t, "peer-operation-codec", "principal-operation-codec", "/workspace/operation-codec")
	_, _ = activateTestNode(t, st, node, profile)
	now := time.Date(2026, 7, 16, 13, 0, 0, 0, time.UTC)
	insertOperationAgentRun(t, st, profile, "run-codec-capture", "running", now)
	insertOperationAgentRun(t, st, profile, "run-codec-result", "running", now)

	_, err := st.db.Exec(`INSERT INTO operations(operation_id,profile_id,agent_run_id,client_key_hash,
		kind,request_digest,status,lease_owner,lease_until,capture_json,created_at)
		VALUES(?,? ,?,?,?,?, 'started',?,?,?,?)`, "operation-noncanonical-capture", profile.ID().String(),
		"run-codec-capture", model.Sum([]byte("codec-key-capture")).Bytes(), string(model.OperationTeamworkOffer),
		model.Sum([]byte("codec-request-capture")).Bytes(), "owner-codec", storeTime(now.Add(time.Minute)),
		[]byte(`{ "roots":[] }`), storeTime(now))
	if err != nil {
		t.Fatal(err)
	}
	captureID, _ := model.ParseOperationID("operation-noncanonical-capture")
	if _, err := readOperationByID(context.Background(), st.db, captureID); err == nil {
		t.Fatal("noncanonical durable capture JSON was accepted")
	}

	_, err = st.db.Exec(`INSERT INTO operations(operation_id,profile_id,agent_run_id,client_key_hash,
		kind,request_digest,status,result_json,created_at,finished_at)
		VALUES(?,?,?,?,?,?,'rejected',?,?,?)`, "operation-noncanonical-result", profile.ID().String(),
		"run-codec-result", model.Sum([]byte("codec-key-result")).Bytes(), string(model.OperationTeamworkOffer),
		model.Sum([]byte("codec-request-result")).Bytes(), []byte(`{ "error":"rejected" }`),
		storeTime(now), storeTime(now.Add(time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	resultID, _ := model.ParseOperationID("operation-noncanonical-result")
	if _, err := readOperationByID(context.Background(), st.db, resultID); err == nil {
		t.Fatal("noncanonical durable result JSON was accepted")
	}
}
