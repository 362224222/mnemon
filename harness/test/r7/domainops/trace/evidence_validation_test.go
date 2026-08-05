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

func TestValidateEvolutionEvidenceBindsLaterEventToExactBoundaryHead(t *testing.T) {
	report := validReport()
	referenceDigest := agency.Sum([]byte("retained Reference Event")).String()
	reference := eventEvidence{Node: "lead", ID: "event:reference", Digest: referenceDigest,
		OriginSequence: 2}
	later := eventEvidence{Node: "lead", ID: "event:evolution",
		Digest: agency.Sum([]byte("later Event")).String(), OriginSequence: 3,
		Causation: []eventRefWire{{ID: reference.ID, Digest: reference.Digest}}}
	proof := evidence{Report: report}
	for _, role := range domainRoles {
		node := nodeEvidence{Role: role}
		if role == "lead" {
			node.Events = []eventEvidence{reference, later}
			node.References = []referenceEvidence{{Node: role, EventID: reference.ID,
				State: "active", ArtifactDigest: agency.Sum([]byte("guide")).String()}}
		}
		proof.Nodes = append(proof.Nodes, node)
	}
	if err := validateEvolutionEvidence(proof); err != nil {
		t.Fatalf("validateEvolutionEvidence() error = %v", err)
	}

	proof.Report.Protocol.Evolution.Boundary.Nodes[2].ConsolidationAfterSequence = 2
	if err := validateEvolutionEvidence(proof); err == nil {
		t.Fatal("validateEvolutionEvidence() accepted a Reference published before consolidation")
	}
	proof.Report.Protocol.Evolution.Boundary.Nodes[2].ConsolidationAfterSequence = 1

	proof.Report.Protocol.Evolution.Effects[2].Matches[0].ReferenceDigest =
		agency.Sum([]byte("different head")).String()
	if err := validateEvolutionEvidence(proof); err == nil {
		t.Fatal("validateEvolutionEvidence() accepted a non-exact Reference edge")
	}
}
