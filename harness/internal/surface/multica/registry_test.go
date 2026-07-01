package multica

import (
	"path/filepath"
	"testing"
)

func TestDefaultParticipantRecords(t *testing.T) {
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

func TestRegistryRoundTrip(t *testing.T) {
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

func TestMulticaParticipantLookups(t *testing.T) {
	reg := MulticaRegistry{Participants: []MulticaParticipantRecord{
		{
			Principal: "planner@team",
			AgentName: "mnemon-planner",
		},
		{
			Principal: "reviewer@team",
			AgentName: "mnemon-reviewer",
			AgentID:   "agent-reviewer",
		},
	}}
	participant, ok := MulticaParticipantForPrincipal(reg, " planner@team ")
	if !ok || participant.AgentName != "mnemon-planner" {
		t.Fatalf("participant lookup = ok:%v %+v", ok, participant)
	}
	participant, ok = FirstMulticaParticipantWithAgentID(reg)
	if !ok || participant.Principal != "reviewer@team" {
		t.Fatalf("first participant with agent id = ok:%v %+v", ok, participant)
	}
	if got := MulticaPrincipalForAgent(reg, " agent-reviewer ", ""); got != "reviewer@team" {
		t.Fatalf("principal by agent id = %q", got)
	}
	if got := MulticaPrincipalForAgent(reg, "", " mnemon-reviewer "); got != "reviewer@team" {
		t.Fatalf("principal by agent name = %q", got)
	}
}

func TestRuntimeMulticaRegistryPrincipalUsesProviderWorkspace(t *testing.T) {
	tmp := t.TempDir()
	workspace := filepath.Join(tmp, "provider-workspace")
	if err := SaveMulticaRegistry(MulticaRegistryPath(workspace, ""), MulticaRegistry{
		Participants: []MulticaParticipantRecord{{
			Principal: "reviewer@team",
			AgentName: "mnemon-reviewer",
			AgentID:   "agent-reviewer",
		}},
	}); err != nil {
		t.Fatal(err)
	}
	got := RuntimeMulticaRegistryPrincipal([]string{"MNEMON_MULTICA_PROVIDER_WORKSPACE=" + workspace}, tmp, "agent-reviewer", "")
	if got != "reviewer@team" {
		t.Fatalf("runtime registry principal = %q", got)
	}
}
