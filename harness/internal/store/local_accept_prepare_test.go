package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/model"
)

func TestApplyLocalAcceptanceTxParticipatesInCallerTransaction(t *testing.T) {
	t.Parallel()
	fixture := newAcceptanceFixture(t, 1)
	operation, authority := fixture.reserveOffer(t, "outer-transaction", nil)
	spec := fixture.offer(t, authority, "outer-transaction", fixture.reviewers, nil, nil)
	committedAt := fixture.now.Add(time.Second)
	receipt := applyAcceptanceInOuterTx(t, fixture, operation, spec, committedAt)

	assertAcceptanceCounts(t, fixture.store, []int{0, 0, 0, 0, 0, 0, 0, 0})
	assertAcceptanceHeads(t, fixture.store, 1, 0)
	assertOperationStatus(t, fixture.store, operation.ID(), model.OperationStarted)

	committed := commitAcceptanceAfterOuterRollback(t, fixture, spec, receipt, committedAt)
	assertTerminalAcceptanceTx(t, fixture, spec, committedAt)
	replay, err := fixture.store.CommitLocalAcceptance(context.Background(),
		LocalAcceptanceSpec{Operation: authority}, time.Time{})
	if err != nil || !replay.Replayed || replay.Receipt.String() != committed.Receipt.String() {
		t.Fatalf("public acceptance replay = (%#v, %v)", replay, err)
	}
}

func applyAcceptanceInOuterTx(t *testing.T, fixture *acceptanceFixture,
	operation model.Operation, spec LocalAcceptanceSpec, committedAt time.Time,
) model.JSON {
	t.Helper()
	tx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	receipt, err := applyLocalAcceptanceTx(context.Background(), tx, spec, committedAt, false)
	if err != nil || receipt.IsZero() {
		t.Fatalf("applyLocalAcceptanceTx() = (%s, %v)", receipt, err)
	}
	assertTransactionLocalAcceptance(t, tx, operation)
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	return receipt
}

func assertTransactionLocalAcceptance(t *testing.T, tx *sql.Tx,
	operation model.Operation,
) {
	t.Helper()
	var events, works, publications, deliveries int
	if err := tx.QueryRow(`SELECT (SELECT COUNT(*) FROM events),(SELECT COUNT(*) FROM works),
		(SELECT COUNT(*) FROM gossip_publications),(SELECT COUNT(*) FROM peer_deliveries)`).
		Scan(&events, &works, &publications, &deliveries); err != nil {
		t.Fatal(err)
	}
	if events != 1 || works != 1 || publications != 1 || deliveries != 1 {
		t.Fatalf("transaction-local evidence = (%d,%d,%d,%d), want all one",
			events, works, publications, deliveries)
	}
	inside, err := readOperationByID(context.Background(), tx, operation.ID())
	if err != nil || inside.Status() != model.OperationCommitted {
		t.Fatalf("transaction-local operation = (%#v, %v)", inside, err)
	}
	var nextOrigin, publicationHead uint64
	if err := tx.QueryRow(`SELECT n.next_origin_seq,p.source_head_channel_seq FROM node n
		JOIN publication_epochs p ON p.origin_peer_id=n.peer_id AND p.origin_epoch=n.origin_epoch`).
		Scan(&nextOrigin, &publicationHead); err != nil {
		t.Fatal(err)
	}
	if nextOrigin != 2 || publicationHead != 1 {
		t.Fatalf("transaction-local heads = (%d,%d), want (2,1)", nextOrigin, publicationHead)
	}
}

func commitAcceptanceAfterOuterRollback(t *testing.T, fixture *acceptanceFixture,
	spec LocalAcceptanceSpec, receipt model.JSON, committedAt time.Time,
) LocalAcceptanceResult {
	t.Helper()
	committed, err := fixture.store.CommitLocalAcceptance(context.Background(), spec, committedAt)
	if err != nil || committed.Replayed || committed.Receipt.String() != receipt.String() {
		t.Fatalf("public acceptance after outer rollback = (%#v, %v), tx receipt %s",
			committed, err, receipt)
	}
	return committed
}

func assertTerminalAcceptanceTx(t *testing.T, fixture *acceptanceFixture,
	spec LocalAcceptanceSpec, committedAt time.Time,
) {
	t.Helper()
	terminalTx, err := fixture.store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := applyLocalAcceptanceTx(context.Background(), terminalTx, spec, committedAt, false); !errors.Is(err, ErrOperationTerminal) {
		_ = terminalTx.Rollback()
		t.Fatalf("direct transaction core terminal error = %v", err)
	}
	if err := terminalTx.Rollback(); err != nil {
		t.Fatal(err)
	}
}
