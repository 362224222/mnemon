package admission

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// #3: a malformed boundary event (no decodable "writes" in the payload) must reconcile to a Rejected
// decision, never an Accepted no-op. opFromEvent must not silently swallow the decode and hand the
// kernel an empty op that gets rubber-stamped Accepted.
func TestMalformedEventYieldsRejected(t *testing.T) {
	s, k := newRecon(t)
	// event carries no "writes" key at all
	ev := contract.Event{ID: "bad1", Type: "memory.write.proposed", Actor: "a1", Payload: map[string]any{"junk": 1}}
	if _, err := s.AppendEvent(ev); err != nil {
		t.Fatalf("append: %v", err)
	}
	ds := NewReconciler(s, k).RunOnce(casModes())
	if len(ds) != 1 {
		t.Fatalf("want 1 decision, got %d", len(ds))
	}
	if ds[0].Status != contract.Rejected {
		t.Fatalf("malformed event must be Rejected (not Accepted no-op), got %s", ds[0].Status)
	}
	if ds[0].IngestSeq != 1 {
		t.Fatalf("rejected decision must still carry the event seq for audit, got %d", ds[0].IngestSeq)
	}
}
