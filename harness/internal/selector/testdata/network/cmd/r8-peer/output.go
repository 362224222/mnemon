package main

import (
	"encoding/json"
	"fmt"

	"github.com/mnemon-dev/mnemon/harness/internal/selector"
)

type snapshotOutput struct {
	Schema      string          `json:"schema"`
	Version     uint32          `json:"version"`
	SelectionID string          `json:"selection_id"`
	Self        string          `json:"self"`
	Phase       string          `json:"phase"`
	Revision    uint64          `json:"revision"`
	Preference  string          `json:"preference,omitempty"`
	Round       uint32          `json:"round,omitempty"`
	Observation json.RawMessage `json:"observation,omitempty"`
}

type initOutput struct {
	snapshotOutput
	SeedOpinionDigest string `json:"seed_opinion_digest"`
	SeedEventID       string `json:"seed_event_id"`
	SeedEventDigest   string `json:"seed_event_digest"`
}

// roundEvidenceOutput is a bounded test projection captured while the
// network adapter still owns the exact pending round and authenticated vote
// set. It is observational evidence only; it does not become selector state.
type roundEvidenceOutput struct {
	Round            uint32 `json:"round"`
	SampleSize       int    `json:"sample_size"`
	Alpha            uint32 `json:"alpha"`
	VotesA           int    `json:"votes_a"`
	VotesB           int    `json:"votes_b"`
	PreferenceBefore string `json:"preference_before"`
	PreferenceAfter  string `json:"preference_after"`
	MarginBefore     int64  `json:"margin_before"`
	MarginAfter      int64  `json:"margin_after"`
	Recolored        bool   `json:"recolored"`
}

type roundOutput struct {
	snapshotOutput
	RoundEvidence roundEvidenceOutput `json:"round_evidence"`
}

func projectSnapshot(snapshot selector.SelectionSnapshot) snapshotOutput {
	output := snapshotOutput{Schema: "mnemon.r8.network.status", Version: 1,
		SelectionID: snapshot.Descriptor().ID().String(), Self: snapshot.Self().String(),
		Phase: string(snapshot.Phase()), Revision: snapshot.Revision()}
	if state, present := snapshot.State(); present {
		output.Preference = state.Preference().String()
		output.Round = state.Round()
	}
	if observation, present := snapshot.Observation(); present {
		output.Observation = json.RawMessage(observation.CanonicalBytes())
	}
	return output
}

func projectInitSnapshot(snapshot selector.SelectionSnapshot,
	seed selector.AcceptedSeedOpinion,
) initOutput {
	return initOutput{snapshotOutput: projectSnapshot(snapshot),
		SeedOpinionDigest: seed.Opinion().Digest().String(),
		SeedEventID:       seed.Event().ID().String(), SeedEventDigest: seed.Event().Digest().String()}
}

func projectRoundExecution(execution roundExecution) (roundOutput, error) {
	before, beforePresent := execution.before.State()
	after, afterPresent := execution.after.State()
	if !beforePresent || !afterPresent || before.SelectionID() != after.SelectionID() ||
		execution.pending.Query().SelectionID() != before.SelectionID() ||
		execution.pending.Query().Round() != after.Round() {
		return roundOutput{}, fmt.Errorf("round evidence has inconsistent selector state")
	}
	profile := execution.after.Descriptor().Profile()
	evidence, err := projectRoundEvidence(before, after, execution.pending.Query().Round(),
		len(execution.pending.Sample()), profile.Alpha(), execution.votes)
	if err != nil {
		return roundOutput{}, err
	}
	return roundOutput{snapshotOutput: projectSnapshot(execution.after),
		RoundEvidence: evidence}, nil
}

func projectRoundEvidence(before, after selector.SelectionState, round uint32,
	sampleSize int, alpha uint32, votes []selector.AuthenticatedVote,
) (roundEvidenceOutput, error) {
	if before.SelectionID().IsZero() || before.SelectionID() != after.SelectionID() ||
		round == 0 || after.Round() != round || before.Round()+1 != round || sampleSize < 1 ||
		alpha == 0 || int(alpha) > sampleSize {
		return roundEvidenceOutput{}, fmt.Errorf("round evidence has inconsistent bounds")
	}
	votesA, votesB := 0, 0
	for _, vote := range votes {
		switch vote.Preference() {
		case selector.PreferenceA:
			votesA++
		case selector.PreferenceB:
			votesB++
		default:
			return roundEvidenceOutput{}, fmt.Errorf("round evidence contains an invalid preference")
		}
	}
	if votesA+votesB > sampleSize {
		return roundEvidenceOutput{}, fmt.Errorf("round evidence exceeds its frozen sample")
	}
	return roundEvidenceOutput{
		Round: round, SampleSize: sampleSize, Alpha: alpha, VotesA: votesA, VotesB: votesB,
		PreferenceBefore: before.Preference().String(), PreferenceAfter: after.Preference().String(),
		MarginBefore: before.Margin(), MarginAfter: after.Margin(),
		Recolored: before.Preference() != after.Preference(),
	}, nil
}
