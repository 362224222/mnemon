package state

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(":memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}
func TestCreateThenCASUpdate(t *testing.T) {
	s := newTestStore(t)
	ref := contract.ResourceRef{Kind: "memory", ID: "m1"}
	if err := s.WithTx(func(tx *Tx) error { return tx.CreateResource(ref, map[string]any{"content": "v1"}) }); err != nil {
		t.Fatalf("create: %v", err)
	}
	if v, _ := s.GetVersion(ref); v != 1 {
		t.Fatalf("want v1, got %d", v)
	}
	// CAS based_on=1 => ok, v2
	_ = s.WithTx(func(tx *Tx) error {
		ok, _ := tx.CASUpdate(ref, 1, map[string]any{"content": "v2"})
		if !ok {
			t.Fatal("expected hit")
		}
		return nil
	})
	// CAS based_on=1 again => MISS (head is 2)
	_ = s.WithTx(func(tx *Tx) error {
		ok, _ := tx.CASUpdate(ref, 1, map[string]any{"content": "v3"})
		if ok {
			t.Fatal("expected miss")
		}
		return nil
	})
	if v, _ := s.GetVersion(ref); v != 2 {
		t.Fatalf("state must stay v2, got %d", v)
	}
}
func TestEventSeqMonotonicDurable(t *testing.T) {
	s := newTestStore(t)
	a, _ := s.AppendEvent(contract.Event{Type: "x.proposed", TS: "2026-06-03T00:00:00Z"})
	b, _ := s.AppendEvent(contract.Event{Type: "y.proposed", TS: "2026-06-03T00:00:00Z"}) // same ts
	if a != 1 || b != 2 {
		t.Fatalf("seq not monotonic: %d %d", a, b)
	}
	evs, _ := s.PendingEvents(0)
	if len(evs) != 2 || evs[0].IngestSeq != 1 || evs[1].IngestSeq != 2 {
		t.Fatalf("not ordered by seq")
	}
}
