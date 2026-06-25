package app

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
)

func testSyncedEvents(t *testing.T, materials ...contract.SyncedEventMaterial) []eventmodel.EventEnvelope {
	t.Helper()
	events := make([]eventmodel.EventEnvelope, 0, len(materials))
	for _, material := range materials {
		env, err := contract.SyncedEventEnvelopeFromMaterial(material)
		if err != nil {
			t.Fatalf("synced event fixture: %v", err)
		}
		events = append(events, env)
	}
	return events
}
