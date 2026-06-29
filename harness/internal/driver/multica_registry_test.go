package driver

import (
	"path/filepath"
	"testing"
)

func TestDefaultMulticaParticipantRecords(t *testing.T) {
	got := DefaultMulticaParticipantRecords("mnemon")
	if len(got) != 5 {
		t.Fatalf("participants len = %d", len(got))
	}
	want := map[string]string{
		"planner@team":     "mnemon-planner",
		"researcher@team":  "mnemon-researcher",
		"implementer@team": "mnemon-implementer",
		"reviewer@team":    "mnemon-reviewer",
		"integrator@team":  "mnemon-integrator",
	}
	for _, participant := range got {
		if want[participant.Principal] != participant.AgentName {
			t.Fatalf("unexpected participant: %+v", participant)
		}
	}
}

func TestMulticaRegistryRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "registry.json")
	_, ok, err := LoadMulticaRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("missing registry should return ok=false")
	}
	want := MulticaRegistry{
		WorkspaceID:      "ws-1",
		RuntimeProfileID: "profile-1",
		RuntimeID:        "runtime-1",
		Participants: []MulticaParticipantRecord{{
			Principal: "planner@team",
			AgentName: "mnemon-planner",
			AgentID:   "agent-1",
			Role:      "planner",
		}},
	}
	if err := SaveMulticaRegistry(path, want); err != nil {
		t.Fatal(err)
	}
	got, ok, err := LoadMulticaRegistry(path)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("saved registry should exist")
	}
	if got.SchemaVersion != 1 || got.WorkspaceID != "ws-1" || len(got.Participants) != 1 || got.Participants[0].AgentID != "agent-1" {
		t.Fatalf("registry mismatch: %+v", got)
	}
}
