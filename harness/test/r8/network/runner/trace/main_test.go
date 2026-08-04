package main

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestWriteTraceCarriesObservedRecolorAndIndependentGates(t *testing.T) {
	proof := validEvidence()
	if err := validateEvidence(proof); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	config := options{runID: "r8-network-test", candidate: digest("c"),
		startedAt: now, finishedAt: now.Add(time.Second)}
	var output bytes.Buffer
	if err := writeTrace(&output, config, proof); err != nil {
		t.Fatal(err)
	}
	trace := output.String()
	for _, required := range []string{
		`"kind":"r8.round.settled"`, `"preference_before":"A"`,
		`"preference_after":"B"`, `"recolored":true`,
		`"id":"r8.identity-binding","status":"pass"`,
		`"record":"result","status":"passed"`,
	} {
		if !strings.Contains(trace, required) {
			t.Fatalf("trace does not contain %s\n%s", required, trace)
		}
	}
}

func TestValidateEvidenceRejectsRecolorInferredOnlyFromFinalState(t *testing.T) {
	proof := validEvidence()
	proof.round.Evidence.Recolored = false
	if err := validateEvidence(proof); err == nil {
		t.Fatal("evidence without an observed recolor was accepted")
	}
}

func validEvidence() evidence {
	selection := digest("8")
	inits := make(map[string]initInput, len(nodes))
	for _, node := range nodes {
		preference := "B"
		if node == "peer-a" {
			preference = "A"
		}
		inits[node] = initInput{snapshotInput: snapshotInput{
			Schema: "mnemon.r8.network.status", Version: 1, SelectionID: selection,
			Self: node + "-identity", Phase: "active", Revision: 2, Preference: preference,
		}, SeedEventID: "event:seed-" + node, SeedEventDigest: digest("d"),
			SeedOpinionDigest: digest("e")}
	}
	preference := "B"
	observation := &observationInput{Margin: -1, Preference: &preference,
		Result: "threshold_reached", Rounds: 1, SelectionID: selection}
	return evidence{inits: inits, round: roundInput{
		snapshotInput: snapshotInput{Schema: "mnemon.r8.network.status", Version: 1,
			SelectionID: selection, Self: inits["peer-a"].Self, Phase: "observed",
			Preference: "B", Round: 1, Observation: observation},
		Evidence: roundEvidenceInput{Round: 1, SampleSize: 1, Alpha: 1, VotesB: 1,
			PreferenceBefore: "A", PreferenceAfter: "B", MarginBefore: 0,
			MarginAfter: -1, Recolored: true}},
		noVote:           probeInput{Mode: "no-vote", HTTPStatus: 200, Authenticated: true, NoVote: true},
		identityMismatch: probeInput{Mode: "identity-mismatch", HTTPStatus: 401},
		restarted: snapshotInput{Schema: "mnemon.r8.network.status", Version: 1,
			SelectionID: selection, Phase: "observed", Preference: "B", Round: 1,
			Observation: observation},
	}
}

func digest(character string) string { return "sha256:" + strings.Repeat(character, 64) }
