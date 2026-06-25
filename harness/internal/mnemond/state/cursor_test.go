package state

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

func TestNamedCursorPersists(t *testing.T) {
	s := newTestStore(t)
	if got := s.GetCursor("dispatch"); got != 0 {
		t.Fatalf("unset cursor must be 0, got %d", got)
	}
	if err := s.SetCursor("dispatch", 7); err != nil {
		t.Fatalf("set: %v", err)
	}
	if got := s.GetCursor("dispatch"); got != 7 {
		t.Fatalf("want 7, got %d", got)
	}
	if err := s.SetCursor("dispatch", 9); err != nil { // upsert
		t.Fatalf("update: %v", err)
	}
	if got := s.GetCursor("dispatch"); got != 9 {
		t.Fatalf("want 9 after upsert, got %d", got)
	}
}

// review finding #1 / crash-window: DispatchTx must be all-or-nothing. A batch containing an
// unmarshalable event fails the whole txn — NO event is appended AND the cursor does NOT advance, so a
// restart correctly re-dispatches (nothing partial leaked).
func TestDispatchTxIsAtomic(t *testing.T) {
	s := newTestStore(t)
	good := contract.Event{Type: "memory.write.proposed", Payload: map[string]any{"k": "v"}}
	bad := contract.Event{Type: "memory.write.proposed", Payload: map[string]any{"bad": make(chan int)}} // unmarshalable
	if err := s.DispatchTx([]contract.Event{good, bad}, "dispatch", 5); err == nil {
		t.Fatal("DispatchTx must fail when an event cannot be marshaled")
	}
	if s.GetCursor("dispatch") != 0 {
		t.Fatal("cursor must NOT advance when DispatchTx rolls back")
	}
	if evs, _ := s.PendingEvents(0); len(evs) != 0 {
		t.Fatalf("no event must be appended when DispatchTx rolls back, got %d", len(evs))
	}
}

func TestDispatchTxCommitsAtomically(t *testing.T) {
	s := newTestStore(t)
	if err := s.DispatchTx([]contract.Event{{Type: "memory.write.proposed", Payload: map[string]any{"k": "v"}}}, "dispatch", 3); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if s.GetCursor("dispatch") != 3 {
		t.Fatal("cursor must advance with the committed append")
	}
	if evs, _ := s.PendingEvents(0); len(evs) != 1 {
		t.Fatalf("the proposed event must be committed, got %d", len(evs))
	}
}
