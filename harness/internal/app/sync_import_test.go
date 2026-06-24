package app

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

func TestRemoteProgressImportConflictDiagnosesWithoutOverwrite(t *testing.T) {
	ref := contract.ResourceRef{Kind: "progress_digest", ID: "project"}
	rt, err := OpenSyncImportRuntime(filepath.Join(t.TempDir(), "local.db"), []contract.ResourceRef{ref}, nil)
	if err != nil {
		t.Fatalf("open sync import runtime: %v", err)
	}
	defer rt.Close()

	if err := ingestRemoteMaterialForTest(rt, "first", policy.EmbeddedCatalog()["progress_digest"], remoteProgressMaterialForTest(ref, "shared-entry", "remote content v1")); err != nil {
		t.Fatalf("first import: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("first tick: %v", err)
	}
	_, fields, err := rt.Resource(ref)
	if err != nil {
		t.Fatalf("read progress: %v", err)
	}
	if content, _ := fields["content"].(string); !strings.Contains(content, "remote content v1") {
		t.Fatalf("first import did not write progress: %+v", fields)
	}

	if err := ingestRemoteMaterialForTest(rt, "conflict", policy.EmbeddedCatalog()["progress_digest"], remoteProgressMaterialForTest(ref, "shared-entry", "remote content v2")); err != nil {
		t.Fatalf("conflict import: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("conflict tick: %v", err)
	}
	_, fields, err = rt.Resource(ref)
	if err != nil {
		t.Fatalf("read progress after conflict: %v", err)
	}
	content, _ := fields["content"].(string)
	if strings.Contains(content, "remote content v2") || !strings.Contains(content, "remote content v1") {
		t.Fatalf("conflict import overwrote local progress: %s", content)
	}
	events, err := rt.PendingEvents(0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	byID := make(map[string]contract.Event, len(events))
	for _, ev := range events {
		byID[ev.ID] = ev
	}
	var diag contract.Event
	var diagnosed bool
	for _, ev := range events {
		if ev.Type == "progress_digest.diagnostic" {
			if reason, _ := ev.Payload["reason"].(string); strings.Contains(reason, "remote import conflict") {
				diagnosed = true
				diag = ev
			}
		}
	}
	if !diagnosed {
		t.Fatalf("conflict import must emit a durable diagnostic, events=%+v", events)
	}

	// MED-4 / v1.1: the origin attribution (origin_replica_id + local_decision_id) must be
	// RECOVERABLE from the durable ledger on the B side — not just "a diagnostic fired". Walk the
	// diagnostic's CausedBy to the <kind>.remote_synced_event.observed trigger and recover the identity
	// from its payload.material. (The material round-trips through the event log as a JSON object.)
	if diag.CausedBy == "" {
		t.Fatalf("conflict diagnostic must carry a CausedBy lineage, got %+v", diag)
	}
	trigger, ok := byID[diag.CausedBy]
	if !ok {
		t.Fatalf("diagnostic CausedBy %q must resolve to a durable event", diag.CausedBy)
	}
	if trigger.Type != policy.EmbeddedCatalog()["progress_digest"].RemoteSyncedEventObserved() {
		t.Fatalf("diagnostic must be caused by the remote material observation, got type %q", trigger.Type)
	}
	material, ok := trigger.Payload["material"].(map[string]any)
	if !ok {
		t.Fatalf("commit_observed payload must carry the material, got %+v", trigger.Payload)
	}
	// contract.SyncedEventMaterial carries no JSON tags, so it round-trips with its Go field names.
	origin, _ := material["OriginReplicaID"].(string)
	decision, _ := material["LocalDecisionID"].(string)
	wantDecision := "dec-shared-entry-remote-content-v2" // the conflicting material's decision id
	if origin != "remote-replica" || decision != wantDecision {
		t.Fatalf("origin attribution must be recoverable from the caused-by material: origin=%q decision=%q (want remote-replica / %s)", origin, decision, wantDecision)
	}
}

func TestRemoteAssignmentImportAppendsItemsThroughLocalMnemon(t *testing.T) {
	ref := contract.ResourceRef{Kind: "assignment", ID: "project"}
	rt, err := OpenSyncImportRuntime(filepath.Join(t.TempDir(), "local.db"), []contract.ResourceRef{ref}, nil)
	if err != nil {
		t.Fatalf("open sync import runtime: %v", err)
	}
	defer rt.Close()

	if err := ingestRemoteMaterialForTest(rt, "remote-assignment", policy.EmbeddedCatalog()["assignment"], remoteAssignmentMaterialForTest(ref, "release-review", "active")); err != nil {
		t.Fatalf("remote assignment import: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick remote assignment import: %v", err)
	}
	_, fields, err := rt.Resource(ref)
	if err != nil {
		t.Fatalf("read assignment: %v", err)
	}
	items, ok := fields["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("remote assignment import must write one item, got %+v", fields)
	}
	item, ok := items[0].(map[string]any)
	if !ok || item["scope"] != "release-review" || item["ttl"] != "active" {
		t.Fatalf("unexpected remote assignment item: %+v", items[0])
	}
}

func ingestRemoteMaterialForTest(rt *runtime.Runtime, externalID string, cap policy.Capability, material contract.SyncedEventMaterial) error {
	_, _, err := rt.API().Ingest(contract.SyncImportActor, contract.ObservationEnvelope{
		ExternalID: externalID,
		Event: contract.Event{
			Type: cap.RemoteSyncedEventObserved(),
			Payload: map[string]any{
				"material": material,
			},
		},
	})
	return err
}

func remoteProgressMaterialForTest(ref contract.ResourceRef, itemID, summary string) contract.SyncedEventMaterial {
	return contract.SyncedEventMaterial{
		OriginReplicaID: "remote-replica",
		LocalDecisionID: "dec-" + itemID + "-" + strings.ReplaceAll(summary, " ", "-"),
		LocalIngestSeq:  11,
		Actor:           "codex@remote",
		ResourceRef:     ref,
		ResourceVersion: 1,
		Fields: map[string]any{
			"content": "# Progress\n- " + summary,
			"items": []any{map[string]any{
				"id":         itemID,
				"summary":    summary,
				"actor":      "codex@remote",
				"ingest_seq": float64(11),
			}},
		},
		DecidedAt: "2026-06-06T00:00:00Z",
	}
}

func remoteAssignmentMaterialForTest(ref contract.ResourceRef, scope, ttl string) contract.SyncedEventMaterial {
	return contract.SyncedEventMaterial{
		OriginReplicaID: "remote-replica",
		LocalDecisionID: "dec-" + scope + "-" + ttl,
		LocalIngestSeq:  21,
		Actor:           "codex@remote",
		ResourceRef:     ref,
		ResourceVersion: 1,
		Fields: map[string]any{
			"content": "# Assignments\n- " + scope,
			"items": []any{map[string]any{
				"id":                "remote/" + scope + "/" + ttl,
				"scope":             scope,
				"ttl":               ttl,
				"assignee":          "codex@impl",
				"expected_work":     "complete " + scope,
				"expected_feedback": "summary",
				"evidence":          "remote import fixture",
				"actor":             "codex@remote",
				"ingest_seq":        float64(21),
			}},
			"updated_by": "codex@remote",
		},
		DecidedAt: "2026-06-06T00:00:00Z",
	}
}
