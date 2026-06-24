package policy

import "testing"

func TestStandardRegistryCarriesFirstPartyEventPackages(t *testing.T) {
	for _, id := range []string{"agent_profile", "teamwork_signal", "project_intent", "assignment", "progress_digest"} {
		pkg, ok := StandardRegistry()[id]
		if !ok {
			t.Fatalf("standard registry must carry %q", id)
		}
		if pkg.Decode == nil || pkg.Header == nil {
			t.Fatalf("standard event package %q must carry compiled decode/header", id)
		}
	}
	// Generic substrate fixtures live in testdata/capabilities and the e2e external-package leg,
	// never in the first-party standard product registry.
	for _, id := range []string{"fixture_record", "fixture_declaration", "note", "decision"} {
		if _, ok := StandardRegistry()[id]; ok {
			t.Fatalf("%q must NOT be first-party standard (demoted to a test/external-package fixture)", id)
		}
	}
	if len(StandardRegistry()) != 5 {
		t.Fatalf("StandardRegistry() must be {agent_profile, teamwork_signal, project_intent, assignment, progress_digest}, got %d entries", len(StandardRegistry()))
	}
}
