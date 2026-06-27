package state

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

func TestRecordSyncEventsDerivesFromAcceptedEventEnvelopes(t *testing.T) {
	s := newTestStore(t)
	ref := contract.ResourceRef{Kind: "memory", ID: "project"}
	err := s.WithTx(func(tx *Tx) error {
		ev := eventmodel.Event{
			SchemaVersion: eventmodel.SchemaVersion,
			ID:            "dec-1:memory/project",
			Type:          "memory.accepted",
			Subject:       eventmodel.Subject("memory", "project"),
			Actor:         "codex@project",
			Payload: eventmodel.BuildPayload(map[string]any{
				"resource_version": int64(3),
				"fields":           map[string]any{"content": "accepted envelope source"},
			}, nil, nil),
			CorrelationID: "corr-1",
			CreatedAt:     "2026-06-24T00:00:00Z",
		}
		if _, err := tx.AppendEventEnvelopeReturningSeq(eventmodel.AcceptedEnvelope(ev, "dec-1", 9, "2026-06-24T00:01:00Z", "local-replica")); err != nil {
			return err
		}
		return tx.RecordSyncEventsTx(contract.Decision{
			DecisionID:  "dec-1",
			IngestSeq:   9,
			Status:      contract.Accepted,
			Actor:       "ignored-by-sync-material",
			AppliedAt:   "ignored-by-sync-material",
			NewVersions: nil,
			NewResources: []contract.ResourceSnapshot{{
				Ref:     contract.ResourceRef{Kind: "memory", ID: "other"},
				Version: 1,
				Fields:  map[string]any{"content": "must not be used"},
			}},
		}, map[contract.ResourceKind]bool{"memory": true})
	})
	if err != nil {
		t.Fatalf("record sync from accepted envelope: %v", err)
	}
	pending, err := s.PendingSyncedEvents()
	if err != nil {
		t.Fatalf("pending synced events: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("want one pending synced event, got %+v", pending)
	}
	material, err := contract.SyncedEventMaterialFromEnvelope(pending[0])
	if err != nil {
		t.Fatalf("materialize pending synced event: %v", err)
	}
	if material.OriginReplicaID != "local-replica" || material.LocalDecisionID != "dec-1" || material.LocalIngestSeq != 9 {
		t.Fatalf("sync identity should come from accepted envelope meta: %+v", material)
	}
	if material.ResourceRef != ref || material.ResourceVersion != 3 {
		t.Fatalf("sync resource should come from accepted envelope event: %+v", material)
	}
	if material.Actor != "codex@project" || material.CorrelationID != "corr-1" {
		t.Fatalf("sync actor/correlation should come from accepted envelope event: %+v", material)
	}
	if material.Fields["content"] != "accepted envelope source" {
		t.Fatalf("sync fields should come from accepted envelope payload: %+v", material.Fields)
	}
}
