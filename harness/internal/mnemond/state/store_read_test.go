package state

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
)

// review #5: the content digest (D8), budget reserve (S6), and lease TTL read need a resource's FIELD
// CONTENT, not just its version. review #6: IngestObservation must append + get the LSN in one tx (S1).

func TestGetResourceReturnsFields(t *testing.T) {
	s := newTestStore(t)
	ref := contract.ResourceRef{Kind: "memory", ID: "m1"}
	if err := s.WithTx(func(tx *Tx) error {
		return tx.CreateResource(ref, map[string]any{"content": "v1"})
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	v, fields, err := s.GetResource(ref)
	if err != nil {
		t.Fatalf("GetResource: %v", err)
	}
	if v != 1 {
		t.Fatalf("want version 1, got %d", v)
	}
	if fields["content"] != "v1" {
		t.Fatalf("fields[content] = %v, want v1", fields["content"])
	}
	// absent resource -> (0, nil, nil), consistent with GetVersion.
	v0, f0, err := s.GetResource(contract.ResourceRef{Kind: "memory", ID: "absent"})
	if err != nil || v0 != 0 || f0 != nil {
		t.Fatalf("absent must be (0,nil,nil); got (%d,%v,%v)", v0, f0, err)
	}
}

func TestTxReadResource(t *testing.T) {
	s := newTestStore(t)
	ref := contract.ResourceRef{Kind: "memory", ID: "m1"}
	if err := s.WithTx(func(tx *Tx) error { return tx.CreateResource(ref, map[string]any{"content": "x"}) }); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.WithTx(func(tx *Tx) error {
		v, fields, err := tx.ReadResource(ref)
		if err != nil {
			return err
		}
		if v != 1 || fields["content"] != "x" {
			t.Fatalf("ReadResource = (%d,%v), want (1, content=x)", v, fields)
		}
		return nil
	}); err != nil {
		t.Fatalf("withtx: %v", err)
	}
}

func TestTxAppendReturningSeq(t *testing.T) {
	s := newTestStore(t)
	var a, b int64
	err := s.WithTx(func(tx *Tx) error {
		var e error
		if a, e = tx.AppendEventReturningSeq(contract.Event{Type: "x.proposed"}); e != nil {
			return e
		}
		b, e = tx.AppendEventReturningSeq(contract.Event{Type: "y.proposed"})
		return e
	})
	if err != nil {
		t.Fatalf("withtx: %v", err)
	}
	if a != 1 || b != 2 {
		t.Fatalf("AppendEventReturningSeq must return monotonic rowids 1,2; got %d,%d", a, b)
	}
}
