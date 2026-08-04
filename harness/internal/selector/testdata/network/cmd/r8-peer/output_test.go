package main

import (
	"testing"
	"time"

	"github.com/mnemon-dev/mnemon/harness/internal/agency"
	"github.com/mnemon-dev/mnemon/harness/internal/selector"
)

func TestProjectRoundEvidenceReportsActualRecolor(t *testing.T) {
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	self := mustParticipant(t, "peer-a")
	peer := mustParticipant(t, "peer-b")
	third := mustParticipant(t, "peer-c")
	profile, err := selector.NewProfile(1, 1, 1, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := selector.NewSelectionDescriptor(
		agency.Sum([]byte("question")), agency.Sum([]byte("candidate-a")),
		agency.Sum([]byte("candidate-b")), []selector.ParticipantID{self, peer, third},
		profile, now.Add(-time.Minute), now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	before, err := selector.NewSelectionState(descriptor.ID(), selector.PreferenceA)
	if err != nil {
		t.Fatal(err)
	}
	nonce := agency.Sum([]byte("round-one"))
	query, err := selector.NewSampleQuery(descriptor.ID(), 1, nonce)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := selector.NewSampleVote(descriptor.ID(), 1, nonce, selector.PreferenceB, peer)
	if err != nil {
		t.Fatal(err)
	}
	vote, err := selector.AuthenticateSampleVote(peer, wire)
	if err != nil {
		t.Fatal(err)
	}
	result, err := selector.ApplyRound(descriptor, before, self, query,
		[]selector.ParticipantID{peer}, []selector.AuthenticatedVote{vote}, now)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := projectRoundEvidence(before, result.State(), 1, 1, 1,
		[]selector.AuthenticatedVote{vote})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.PreferenceBefore != "A" || evidence.PreferenceAfter != "B" ||
		evidence.MarginBefore != 0 || evidence.MarginAfter != -1 || !evidence.Recolored ||
		evidence.VotesA != 0 || evidence.VotesB != 1 {
		t.Fatalf("unexpected round evidence: %+v", evidence)
	}
}

func mustParticipant(t testing.TB, value string) selector.ParticipantID {
	t.Helper()
	participant, err := selector.NewParticipantID(value)
	if err != nil {
		t.Fatal(err)
	}
	return participant
}
