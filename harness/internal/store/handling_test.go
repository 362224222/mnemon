package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestAgentHandlingTransactionInsertReplayAndConflict(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	insertNode(t, st.db)
	insertProfile(t, st.db)
	insertChannelAndEvent(t, st.db)

	handling := pendingHandling(t, "handling-one", "event-one")
	tx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := insertAgentHandling(context.Background(), tx, handling); err != nil || replay {
		t.Fatalf("first insertAgentHandling() = (%t, %v)", replay, err)
	}
	if replay, err := insertAgentHandling(context.Background(), tx, handling); err != nil || !replay {
		t.Fatalf("replayed insertAgentHandling() = (%t, %v)", replay, err)
	}
	conflict := pendingHandling(t, "handling-other", "event-one")
	if _, err := insertAgentHandling(context.Background(), tx, conflict); !errors.Is(err, ErrHandlingConflict) {
		t.Fatalf("conflicting Profile/Event error = %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	claimToken := model.Sum([]byte("handling-one-claim"))
	claimedAt := handling.CreatedAt().Add(time.Second)
	leaseUntil := claimedAt.Add(time.Minute)
	if _, err := st.db.Exec(`UPDATE agent_handlings SET status='claimed',claim_owner='worker-one',
		claim_token_hash=?,lease_until=?,attempts=1,updated_at=? WHERE handling_id=?`, claimToken.Bytes(),
		storeTime(leaseUntil), storeTime(claimedAt), handling.ID().String()); err != nil {
		t.Fatal(err)
	}
	replayTx, err := st.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := insertAgentHandling(context.Background(), replayTx, handling); err != nil || !replay {
		t.Fatalf("lifecycle replay insertAgentHandling() = (%t, %v)", replay, err)
	}
	if err := replayTx.Commit(); err != nil {
		t.Fatal(err)
	}

	stored, err := st.GetAgentHandling(context.Background(), handling.ID())
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status() != model.HandlingClaimed || stored.ClaimOwner() != "worker-one" ||
		stored.Attempts() != 1 || stored.ProfileID() != model.TeamworkProfileID() {
		t.Fatalf("stored handling = %#v", stored)
	}
	if _, err := st.db.Exec("UPDATE agent_handlings SET priority=priority+1 WHERE handling_id=?",
		handling.ID().String()); err == nil {
		t.Fatal("schema allowed Handling creation identity mutation")
	}
	if _, err := st.db.Exec(`INSERT INTO agent_handlings(
		handling_id, profile_id, event_id, status, available_at, created_at, updated_at
	) VALUES('duplicate', 'teamwork-default', 'event-one', 'pending', ?, ?, ?)`,
		storeTime(handling.AvailableAt()), storeTime(handling.CreatedAt()), storeTime(handling.UpdatedAt())); err == nil {
		t.Fatal("schema accepted a second handling for one Profile/Event")
	}
}

func TestAgentHandlingReadFailsClosedOnInvalidStatusLeaseOrTime(t *testing.T) {
	t.Parallel()
	st := openTestStore(t)
	insertNode(t, st.db)
	insertProfile(t, st.db)
	insertChannelAndEvent(t, st.db)
	handling := pendingHandling(t, "handling-invalid", "event-one")
	tx, _ := st.db.BeginTx(context.Background(), nil)
	if _, err := insertAgentHandling(context.Background(), tx, handling); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err := st.db.Exec("UPDATE agent_handlings SET status = 'unknown' WHERE handling_id = ?", handling.ID().String()); err == nil {
		t.Fatal("schema accepted status outside the closed set")
	}
	if _, err := st.db.Exec("UPDATE agent_handlings SET status = 'claimed' WHERE handling_id = ?", handling.ID().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetAgentHandling(context.Background(), handling.ID()); !errors.Is(err, model.ErrInvariant) {
		t.Fatalf("claim without lease read error = %v, want model invariant", err)
	}
	if _, err := st.db.Exec("UPDATE agent_handlings SET status = 'pending', available_at = '2026-07-16T20:00:00+08:00' WHERE handling_id = ?", handling.ID().String()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetAgentHandling(context.Background(), handling.ID()); err == nil || !strings.Contains(err.Error(), "non-canonical store time") {
		t.Fatalf("noncanonical time read error = %v", err)
	}
}

func TestStoreTimeIsFixedWidthAndLexicallyOrdered(t *testing.T) {
	t.Parallel()
	early := time.Date(2026, 7, 16, 12, 0, 0, 100_000_000, time.UTC)
	later := time.Date(2026, 7, 16, 12, 0, 0, 110_000_000, time.UTC)
	if got := storeTime(early); got != "2026-07-16T12:00:00.100000000Z" {
		t.Fatalf("storeTime() = %q", got)
	}
	if !(storeTime(early) < storeTime(later)) {
		t.Fatalf("fixed store time does not preserve ordering: %q >= %q", storeTime(early), storeTime(later))
	}
	if _, err := parseCanonicalStoreTime("2026-07-16T12:00:00.1Z"); err == nil {
		t.Fatal("variable-width RFC3339 time was accepted for SQLite ordering")
	}
}

func TestAgentHandlingPersistsAcrossRestart(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "node", "node.db")
	st, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	insertNode(t, st.db)
	insertProfile(t, st.db)
	insertChannelAndEvent(t, st.db)
	handling := pendingHandling(t, "handling-restart", "event-one")
	tx, _ := st.db.BeginTx(context.Background(), nil)
	if _, err := insertAgentHandling(context.Background(), tx, handling); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if got, err := st.GetAgentHandling(context.Background(), handling.ID()); err != nil || !sameHandling(got, handling) {
		t.Fatalf("GetAgentHandling() after restart = (%#v, %v)", got, err)
	}
}

func pendingHandling(t *testing.T, idText, eventText string) model.Handling {
	t.Helper()
	id, err := model.ParseHandlingID(idText)
	if err != nil {
		t.Fatal(err)
	}
	event, err := model.ParseEventID(eventText)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 16, 20, 0, 0, 123, time.FixedZone("CST", 8*60*60))
	handling, err := model.NewHandling(model.HandlingSpec{ID: id, ProfileID: model.TeamworkProfileID(),
		EventID: event, Status: model.HandlingPending, Priority: 10, AvailableAt: now,
		CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	return handling
}
