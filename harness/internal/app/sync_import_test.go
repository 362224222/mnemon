package app

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/contract"
	"github.com/mnemon-dev/mnemon/harness/internal/mnemond/policy"
	"github.com/mnemon-dev/mnemon/harness/internal/runtime"
)

func TestRemoteMemoryImportConflictDiagnosesWithoutOverwrite(t *testing.T) {
	ref := contract.ResourceRef{Kind: "memory", ID: "project"}
	rt, err := OpenSyncImportRuntime(filepath.Join(t.TempDir(), "local.db"), []contract.ResourceRef{ref}, nil)
	if err != nil {
		t.Fatalf("open sync import runtime: %v", err)
	}
	defer rt.Close()

	if err := ingestRemoteMemoryForTest(rt, "first", remoteMemoryMaterialForTest(ref, "shared-entry", "remote content v1")); err != nil {
		t.Fatalf("first import: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("first tick: %v", err)
	}
	_, fields, err := rt.Resource(ref)
	if err != nil {
		t.Fatalf("read memory: %v", err)
	}
	if content, _ := fields["content"].(string); !strings.Contains(content, "remote content v1") {
		t.Fatalf("first import did not write memory: %+v", fields)
	}

	if err := ingestRemoteMemoryForTest(rt, "conflict", remoteMemoryMaterialForTest(ref, "shared-entry", "remote content v2")); err != nil {
		t.Fatalf("conflict import: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("conflict tick: %v", err)
	}
	_, fields, err = rt.Resource(ref)
	if err != nil {
		t.Fatalf("read memory after conflict: %v", err)
	}
	content, _ := fields["content"].(string)
	if strings.Contains(content, "remote content v2") || !strings.Contains(content, "remote content v1") {
		t.Fatalf("conflict import overwrote local memory: %s", content)
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
		if ev.Type == "memory.diagnostic" {
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
	// diagnostic's CausedBy to the memory.remote_synced_event.observed trigger and recover the identity
	// from its payload.material. (The material round-trips through the event log as a JSON object.)
	if diag.CausedBy == "" {
		t.Fatalf("conflict diagnostic must carry a CausedBy lineage, got %+v", diag)
	}
	trigger, ok := byID[diag.CausedBy]
	if !ok {
		t.Fatalf("diagnostic CausedBy %q must resolve to a durable event", diag.CausedBy)
	}
	if trigger.Type != policy.EmbeddedCatalog()["memory"].RemoteSyncedEventObserved() {
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

func TestRemoteSkillImportAppendsDeclarationsThroughLocalMnemon(t *testing.T) {
	ref := contract.ResourceRef{Kind: "skill", ID: "project"}
	rt, err := OpenSyncImportRuntime(filepath.Join(t.TempDir(), "local.db"), []contract.ResourceRef{ref}, nil)
	if err != nil {
		t.Fatalf("open sync import runtime: %v", err)
	}
	defer rt.Close()

	if err := ingestRemoteSkillForTest(rt, "remote-skill", remoteSkillMaterialForTest(ref, "release-checklist", "active")); err != nil {
		t.Fatalf("remote skill import: %v", err)
	}
	if _, err := rt.Tick(); err != nil {
		t.Fatalf("tick remote skill import: %v", err)
	}
	_, fields, err := rt.Resource(ref)
	if err != nil {
		t.Fatalf("read skill: %v", err)
	}
	decls, ok := fields["declarations"].([]any)
	if !ok || len(decls) != 1 {
		t.Fatalf("remote skill import must write one declaration, got %+v", fields)
	}
	decl, ok := decls[0].(map[string]any)
	if !ok || decl["skill_id"] != "release-checklist" || decl["status"] != "active" {
		t.Fatalf("unexpected remote skill declaration: %+v", decls[0])
	}
}

func ingestRemoteMemoryForTest(rt *runtime.Runtime, externalID string, material contract.SyncedEventMaterial) error {
	_, _, err := rt.API().Ingest(contract.SyncImportActor, contract.ObservationEnvelope{
		ExternalID: externalID,
		Event: contract.Event{
			Type: policy.EmbeddedCatalog()["memory"].RemoteSyncedEventObserved(),
			Payload: map[string]any{
				"material": material,
			},
		},
	})
	return err
}

func ingestRemoteSkillForTest(rt *runtime.Runtime, externalID string, material contract.SyncedEventMaterial) error {
	_, _, err := rt.API().Ingest(contract.SyncImportActor, contract.ObservationEnvelope{
		ExternalID: externalID,
		Event: contract.Event{
			Type: policy.EmbeddedCatalog()["skill"].RemoteSyncedEventObserved(),
			Payload: map[string]any{
				"material": material,
			},
		},
	})
	return err
}

func remoteMemoryMaterialForTest(ref contract.ResourceRef, entryID, content string) contract.SyncedEventMaterial {
	return contract.SyncedEventMaterial{
		OriginReplicaID: "remote-replica",
		LocalDecisionID: "dec-" + entryID + "-" + strings.ReplaceAll(content, " ", "-"),
		LocalIngestSeq:  11,
		Actor:           "codex@remote",
		ResourceRef:     ref,
		ResourceVersion: 1,
		Fields: map[string]any{
			"content": "# Local Memory\n- " + content,
			"entries": []any{map[string]any{
				"id":         entryID,
				"content":    content,
				"source":     "remote",
				"confidence": "high",
				"actor":      "codex@remote",
				"ingest_seq": float64(11),
			}},
		},
		DecidedAt: "2026-06-06T00:00:00Z",
	}
}

func remoteSkillMaterialForTest(ref contract.ResourceRef, skillID, status string) contract.SyncedEventMaterial {
	return contract.SyncedEventMaterial{
		OriginReplicaID: "remote-replica",
		LocalDecisionID: "dec-" + skillID + "-" + status,
		LocalIngestSeq:  21,
		Actor:           "codex@remote",
		ResourceRef:     ref,
		ResourceVersion: 1,
		Fields: map[string]any{
			"name": "project",
			"declarations": []any{map[string]any{
				"id":         "remote/" + skillID + "/" + status,
				"skill_id":   skillID,
				"name":       skillID,
				"status":     status,
				"content":    "Remote declaration for " + skillID,
				"source":     "remote",
				"confidence": "high",
				"actor":      "codex@remote",
				"ingest_seq": float64(21),
			}},
			"updated_by": "codex@remote",
		},
		DecidedAt: "2026-06-06T00:00:00Z",
	}
}
