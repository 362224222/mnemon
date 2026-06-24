package state

import (
	"errors"
	"testing"
	"time"
)

// S2/S4 substrate: the outbox enqueues effects atomically with their producing decision, dedupes by
// idempotency key, and hands rows to a delivery worker under a lease.

func TestOutboxEnqueueRollsBackWithDecision(t *testing.T) {
	s := newTestStore(t)
	boom := errors.New("boom")
	// enqueue inside a tx that then FAILS -> the whole tx rolls back; no row leaks.
	err := s.WithTx(func(tx *Tx) error {
		if e := tx.EnqueueOutbox(OutboxRow{ID: "o1", Kind: "invalidation", Target: "m1", Payload: "{}"}); e != nil {
			return e
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("withtx must surface boom, got %v", err)
	}
	claimed, err := s.ClaimOutbox("w1", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("rolled-back enqueue must leave no outbox row, got %d", len(claimed))
	}
}

func TestOutboxClaimByLease(t *testing.T) {
	s := newTestStore(t)
	if err := s.WithTx(func(tx *Tx) error {
		return tx.EnqueueOutbox(OutboxRow{ID: "o1", Kind: "job", Target: "eval", Payload: "{}", IdempotencyKey: "k1"})
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	claimed, err := s.ClaimOutbox("w1", time.Minute)
	if err != nil {
		t.Fatalf("claim w1: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != "o1" || claimed[0].LeaseOwner != "w1" {
		t.Fatalf("w1 must claim o1; got %+v", claimed)
	}
	// w2 cannot steal an unexpired lease.
	claimed2, err := s.ClaimOutbox("w2", time.Minute)
	if err != nil {
		t.Fatalf("claim w2: %v", err)
	}
	if len(claimed2) != 0 {
		t.Fatalf("w2 must not steal an unexpired lease, got %d", len(claimed2))
	}
	// ack by the holder removes it from future claims.
	if err := s.AckOutbox("o1", "w1"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	claimed3, _ := s.ClaimOutbox("w1", time.Minute)
	if len(claimed3) != 0 {
		t.Fatalf("acked row must not be re-claimed, got %d", len(claimed3))
	}
}

func TestDuplicateIdempotencyKeyIsNoop(t *testing.T) {
	s := newTestStore(t)
	enqueue := func(id string) error {
		return s.WithTx(func(tx *Tx) error {
			return tx.EnqueueOutbox(OutboxRow{ID: id, Kind: "job", Target: "eval", Payload: "{}", IdempotencyKey: "same-key"})
		})
	}
	if err := enqueue("o1"); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	if err := enqueue("o2"); err != nil {
		t.Fatalf("second enqueue (same key) must be a silent no-op, not an error: %v", err)
	}
	claimed, err := s.ClaimOutbox("w1", time.Minute)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("duplicate idempotency key must yield exactly one row, got %d", len(claimed))
	}
}

// DeleteAckedOutbox prunes terminally-acked rows of one kind: acked rows are dead weight (nothing
// re-reads them), and without pruning the outbox grows one row per accepted decision forever.
func TestDeleteAckedOutboxPrunesOnlyAckedOfKind(t *testing.T) {
	s := newTestStore(t)
	if err := s.WithTx(func(tx *Tx) error {
		for _, id := range []string{"i1", "i2"} {
			if err := tx.EnqueueOutbox(OutboxRow{ID: id, Kind: "invalidation"}); err != nil {
				return err
			}
		}
		return tx.EnqueueOutbox(OutboxRow{ID: "j1", Kind: "job"})
	}); err != nil {
		t.Fatal(err)
	}
	rows, err := s.ClaimOutbox("w", time.Minute, "invalidation")
	if err != nil || len(rows) != 2 {
		t.Fatalf("claim: %d rows err=%v", len(rows), err)
	}
	if err := s.AckOutbox("i1", "w"); err != nil {
		t.Fatal(err)
	}
	n, err := s.DeleteAckedOutbox("invalidation")
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("must prune exactly the acked invalidation; got %d", n)
	}
	// the unacked claim + the other kind survive
	if err := s.AckOutbox("i2", "w"); err != nil {
		t.Fatalf("unacked row must survive the prune: %v", err)
	}
	if n, _ := s.DeleteAckedOutbox("job"); n != 0 {
		t.Fatalf("unacked job row must not be pruned; got %d", n)
	}
}
