package main

import (
	"testing"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
)

func TestValidateGlobalCausationCountsOnlyFederationHops(t *testing.T) {
	root := eventEvidence{ID: "event:root", Digest: agency.Sum([]byte("root")).String()}
	imported := eventEvidence{ID: "event:imported", Digest: agency.Sum([]byte("imported")).String(),
		CausalDepth: 1, Causation: []eventRefWire{{ID: root.ID, Digest: root.Digest}}}
	response := eventEvidence{ID: "event:response", Digest: agency.Sum([]byte("response")).String(),
		CausalDepth: 1, Causation: []eventRefWire{{ID: imported.ID, Digest: imported.Digest}}}
	global := map[string]eventEvidence{root.ID: root, imported.ID: imported, response.ID: response}
	if err := validateGlobalCausation(global); err != nil {
		t.Fatalf("equal-depth local response rejected: %v", err)
	}

	response.CausalDepth = 0
	global[response.ID] = response
	if err := validateGlobalCausation(global); err == nil {
		t.Fatal("causal-depth regression accepted")
	}
}
