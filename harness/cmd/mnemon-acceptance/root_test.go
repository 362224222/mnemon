package main

import "testing"

func TestRootExposesAcceptanceScenarioCommands(t *testing.T) {
	commands := map[string]bool{}
	for _, cmd := range rootCmd.Commands() {
		commands[cmd.Name()] = true
	}
	for _, want := range []string{
		"observe",
		"r1-codex",
		"r1-prod-sim",
		"r1-task-sim",
		"r1-cluster-single-entrypoint",
		"multica-provision",
		"multica-runtime-prod-sim",
	} {
		if !commands[want] {
			t.Fatalf("mnemon-acceptance missing command %q; commands=%v", want, commands)
		}
	}
	if commands["acceptance"] {
		t.Fatalf("mnemon-acceptance should expose scenarios directly, not under an acceptance parent")
	}
}
