package state

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// R2#3: AppendEvent is the durable ingest stream — a payload that cannot be marshalled must FAIL, not
// silently write a garbage ("") payload that later decodes to a zero-value event.
func TestAppendEventFailsOnUnserializablePayload(t *testing.T) {
	s := newTestStore(t)
	_, err := s.AppendEvent(contract.Event{Type: "x.proposed", Payload: map[string]any{"bad": make(chan int)}})
	if err == nil {
		t.Fatal("AppendEvent must fail on an unserializable payload, not write a garbage row")
	}
	evs, perr := s.PendingEvents(0)
	if perr != nil {
		t.Fatalf("PendingEvents: %v", perr)
	}
	if len(evs) != 0 {
		t.Fatalf("no event row should be written on marshal failure, got %d", len(evs))
	}
}

// R2#3: PendingEvents must SURFACE a decode error, not manufacture a zero-value event from a corrupt
// payload row.
func TestPendingEventsSurfacesDecodeError(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.db.Exec(`INSERT INTO events (payload) VALUES ('not valid json')`); err != nil {
		t.Fatalf("inject corrupt row: %v", err)
	}
	if _, err := s.PendingEvents(0); err == nil {
		t.Fatal("PendingEvents must surface a decode error, not manufacture a zero-value event")
	}
}
