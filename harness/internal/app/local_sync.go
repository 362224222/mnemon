package app

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	eventmodel "github.com/mnemon-dev/mnemon/harness/internal/event"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemonhub/exchange"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

// ImportLocalSyncPull re-enters pulled synced events through Event Intake (the import runtime), then
// advances the durable pull cursor. It drives Ingest/Tick, so it stays on the app side of the boundary
// (above mnemonhub exchange's pure store helpers) — never bypassing the kernel. It is the OFFLINE path: it
// boots its own import runtime by path, so it must never run inside a serving process (the in-process
// worker drives importPulledEvents over the LIVE runtime instead — flock, v1.1 #2).
func ImportLocalSyncPull(storePath, remoteID, nextCursor string, events []eventmodel.EventEnvelope, catalog policy.Registry) error {
	return ImportLocalSyncPullWithDiagnostics(storePath, remoteID, nextCursor, events, nil, catalog)
}

// ImportLocalSyncPullWithDiagnostics is ImportLocalSyncPull plus pull-side Remote Workspace
// diagnostics. Diagnostics enter as trusted sync.remote_diagnostic.observed observations and are
// converted by policy into durable sync.diagnostic events, exactly like skipped-kind diagnostics.
func ImportLocalSyncPullWithDiagnostics(storePath, remoteID, nextCursor string, events []eventmodel.EventEnvelope, diagnostics []contract.EventExchangeResult, catalog policy.Registry) error {
	if len(events) > 0 || len(diagnostics) > 0 {
		var refs []contract.ResourceRef
		if len(events) > 0 {
			var err error
			refs, err = refsFromSyncedEvents(events)
			if err != nil {
				return err
			}
		}
		rt, err := OpenSyncImportRuntime(storePath, refs, catalog)
		if err != nil {
			return fmt.Errorf("open Local Mnemon import runtime: %w", err)
		}
		if err := importPulledEvents(rt, remoteID, events, catalog); err != nil {
			_ = rt.Close()
			return err
		}
		if err := importRemoteDiagnostics(rt, remoteID, diagnostics); err != nil {
			_ = rt.Close()
			return err
		}
		if err := rt.Close(); err != nil {
			return err
		}
	}
	return exchange.SetSyncPullCursor(storePath, remoteID, nextCursor)
}

// importPulledEvents is the ONE pull-import loop both paths share (offline ImportLocalSyncPull and
// the in-process worker): each synced event re-enters Event Intake under contract.SyncImportActor with the
// six-part pull ExternalID (exactly-once), and a NEW observation is applied by one Tick. An event
// whose kind has no import mapping is no longer silently dropped (v1.1 #4): it ingests
// sync.import_skipped.observed (ExternalID = six-part key + ":skipped") carrying the attribution
// payload, and the sync-import deny rule turns it into a durable sync.diagnostic. The pull cursor
// still advances either way — a skip is visible, never wedging.
func importPulledEvents(rt *runtime.Runtime, remoteID string, events []eventmodel.EventEnvelope, catalog policy.Registry) error {
	catalog = resolveSyncCatalog(catalog)
	pulledAt := time.Now().UTC().Format(time.RFC3339)
	for _, event := range events {
		material, err := contract.SyncedEventMaterialFromEnvelope(event)
		if err != nil {
			return fmt.Errorf("materialize remote synced event: %w", err)
		}
		var env contract.ObservationEnvelope
		if eventType, ok := policy.RemoteSyncedEventType(catalog, material.ResourceRef.Kind); ok {
			env = contract.ObservationEnvelope{
				ExternalID: syncPullExternalID(remoteID, material),
				Event: contract.Event{
					Type: eventType,
					Payload: eventmodel.BuildPayload(map[string]any{
						"material":  material,
						"remote_id": remoteID,
						"pulled_at": pulledAt,
					}, nil, nil),
				},
			}
		} else {
			env = contract.ObservationEnvelope{
				ExternalID: syncPullExternalID(remoteID, material) + ":skipped",
				Event: contract.Event{
					Type: policy.SyncImportSkippedObserved,
					Payload: eventmodel.BuildPayload(map[string]any{
						"kind":              string(material.ResourceRef.Kind),
						"origin_replica_id": material.OriginReplicaID,
						"local_decision_id": material.LocalDecisionID,
						"remote_id":         remoteID,
					}, nil, nil),
				},
			}
		}
		_, dup, err := rt.IngestTrusted(contract.SyncImportActor, env)
		if err != nil {
			return fmt.Errorf("ingest remote synced event: %w", err)
		}
		if !dup {
			if _, err := rt.Tick(); err != nil {
				return fmt.Errorf("apply remote synced event: %w", err)
			}
		}
	}
	return nil
}

func importRemoteDiagnostics(rt *runtime.Runtime, remoteID string, diagnostics []contract.EventExchangeResult) error {
	if len(diagnostics) == 0 {
		return nil
	}
	pulledAt := time.Now().UTC().Format(time.RFC3339)
	for _, item := range diagnostics {
		env := contract.ObservationEnvelope{
			ExternalID: syncRemoteDiagnosticExternalID(remoteID, item),
			Event: contract.Event{
				Type: policy.SyncRemoteDiagnosticObserved,
				Payload: eventmodel.BuildPayload(map[string]any{
					"remote_id":      remoteID,
					"origin_mnemond": item.OriginMnemond,
					"event_id":       item.EventID,
					"subject":        string(item.Subject),
					"status":         item.Status,
					"pulled_at":      pulledAt,
				}, map[string]any{"diagnostic": item.Diagnostic}, nil),
			},
		}
		_, dup, err := rt.IngestTrusted(contract.SyncImportActor, env)
		if err != nil {
			return fmt.Errorf("ingest remote workspace diagnostic: %w", err)
		}
		if !dup {
			if _, err := rt.Tick(); err != nil {
				return fmt.Errorf("apply remote workspace diagnostic: %w", err)
			}
		}
	}
	return nil
}

func refsFromSyncedEvents(events []eventmodel.EventEnvelope) ([]contract.ResourceRef, error) {
	seen := map[contract.ResourceRef]bool{}
	var refs []contract.ResourceRef
	for _, event := range events {
		material, err := contract.SyncedEventMaterialFromEnvelope(event)
		if err != nil {
			return nil, err
		}
		if !seen[material.ResourceRef] {
			seen[material.ResourceRef] = true
			refs = append(refs, material.ResourceRef)
		}
	}
	return refs, nil
}

func syncPullExternalID(remoteID string, material contract.SyncedEventMaterial) string {
	return strings.Join([]string{
		"pull",
		remoteID,
		material.OriginReplicaID,
		material.LocalDecisionID,
		string(material.ResourceRef.Kind),
		string(material.ResourceRef.ID),
	}, ":")
}

func syncRemoteDiagnosticExternalID(remoteID string, item contract.EventExchangeResult) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{
		remoteID,
		item.OriginMnemond,
		item.EventID,
		string(item.Subject),
		item.Status,
		item.Diagnostic,
	}, "\x00")))
	return "pull:" + remoteID + ":diagnostic:" + hex.EncodeToString(sum[:12])
}
